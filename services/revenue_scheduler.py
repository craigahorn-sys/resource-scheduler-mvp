from __future__ import annotations

from io import BytesIO

import pandas as pd
from sqlalchemy import text

from .db import execute, query_df


# ── Migration helpers (run once on startup) ───────────────────────────────────

def migrate_revenue_columns(engine):
    """Adds billing columns to jobs and creates job_line_items if missing.
    Each ALTER runs in its own transaction so a duplicate-column error
    cannot poison the connection for subsequent statements.
    """
    billing_cols = [
        ("company_man",      "TEXT"),
        ("invoice_number",   "TEXT"),
        ("so_ticket_number", "TEXT"),
        ("day_rate",         "NUMERIC"),
        ("accrue",           "BOOLEAN NOT NULL DEFAULT FALSE"),
        ("ees_supervisor",   "TEXT"),
        ("customer_po",      "TEXT"),
        ("county_state",     "TEXT"),
        ("well_name",        "TEXT"),
        ("well_number",      "TEXT"),
        ("ordered_by",       "TEXT"),
        ("department",       "TEXT"),
        ("job_description",  "TEXT"),
    ]
    for col, col_type in billing_cols:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE jobs ADD COLUMN {col} {col_type}"))
        except Exception:
            pass  # column already exists — safe to ignore

    # CREATE TABLE IF NOT EXISTS is always safe to run unconditionally
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS job_line_items (
                id BIGSERIAL PRIMARY KEY,
                job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                line_number INTEGER,
                description TEXT NOT NULL,
                uom TEXT,
                start_date DATE,
                end_date DATE,
                invoice_qty NUMERIC,
                unit_price NUMERIC,
                line_total NUMERIC,
                notes TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))


# ── Job billing field helpers ─────────────────────────────────────────────────

def update_job_billing(engine, job_id: int, data: dict):
    execute(engine, """
        UPDATE jobs
        SET company_man     = :company_man,
            invoice_number  = :invoice_number,
            so_ticket_number= :so_ticket_number,
            day_rate        = :day_rate,
            accrue          = :accrue,
            ees_supervisor  = :ees_supervisor,
            customer_po     = :customer_po,
            county_state    = :county_state,
            well_name       = :well_name,
            well_number     = :well_number,
            ordered_by      = :ordered_by,
            department      = :department,
            job_description = :job_description
        WHERE id = :job_id
    """, {**data, "job_id": int(job_id)})


# ── Line item CRUD ────────────────────────────────────────────────────────────

def get_line_items_df(engine, job_id: int | None = None) -> pd.DataFrame:
    if job_id is not None:
        return query_df(engine, """
            SELECT li.*, j.job_code, j.job_name, j.customer, j.region_code
            FROM job_line_items li
            JOIN jobs j ON li.job_id = j.id
            WHERE li.job_id = :job_id
            ORDER BY li.line_number NULLS LAST, li.id
        """, {"job_id": int(job_id)})
    return query_df(engine, """
        SELECT li.*, j.job_code, j.job_name, j.customer, j.region_code
        FROM job_line_items li
        JOIN jobs j ON li.job_id = j.id
        ORDER BY li.job_id, li.line_number NULLS LAST, li.id
    """)


def save_line_items(engine, job_id: int, rows: list[dict]):
    """Replace all line items for a job with the provided rows."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM job_line_items WHERE job_id = :job_id"), {"job_id": int(job_id)})
        for i, row in enumerate(rows, start=1):
            conn.execute(text("""
                INSERT INTO job_line_items
                    (job_id, line_number, description, uom, start_date, end_date,
                     invoice_qty, unit_price, line_total, notes)
                VALUES
                    (:job_id, :line_number, :description, :uom, :start_date, :end_date,
                     :invoice_qty, :unit_price, :line_total, :notes)
            """), {
                "job_id":      int(job_id),
                "line_number": i,
                "description": str(row.get("description", "") or ""),
                "uom":         str(row.get("uom", "") or ""),
                "start_date":  row.get("start_date") or None,
                "end_date":    row.get("end_date") or None,
                "invoice_qty": _num(row.get("invoice_qty")),
                "unit_price":  _num(row.get("unit_price")),
                "line_total":  _num(row.get("line_total")),
                "notes":       str(row.get("notes", "") or ""),
            })


def delete_line_item(engine, line_item_id: int):
    execute(engine, "DELETE FROM job_line_items WHERE id = :id", {"id": int(line_item_id)})


# ── Revenue report query ──────────────────────────────────────────────────────

def get_revenue_jobs_df(engine, region_code: str, month: int, year: int) -> pd.DataFrame:
    """
    Returns jobs for the given region whose active window overlaps the month,
    joined with their billing fields and computed dates.
    """
    month_start = f"{year}-{month:02d}-01"
    # last day of month via date arithmetic
    month_end = f"{year}-{month:02d}-01"

    df = query_df(engine, """
        SELECT
            j.id, j.job_code, j.job_name, j.customer, j.customer_color,
            j.location, j.region_code, j.status,
            j.job_start_date, j.job_duration_days,
            j.mob_days_before_job, j.demob_days_after_job,
            j.company_man, j.invoice_number, j.so_ticket_number,
            j.day_rate, j.accrue, j.notes
        FROM jobs j
        WHERE j.region_code = :region_code
          AND j.status NOT IN ('Cancelled', 'Bid')
          AND (j.job_start_date, j.job_start_date + j.job_duration_days * INTERVAL '1 day')
              OVERLAPS (
                  DATE_TRUNC('month', CAST(:month_start AS date)),
                  DATE_TRUNC('month', CAST(:month_start AS date)) + INTERVAL '1 month'
              )
        ORDER BY j.job_start_date, j.id
    """, {"region_code": region_code, "month_start": month_start})
    return df


# ── Excel export ──────────────────────────────────────────────────────────────

def build_revenue_excel(jobs_df: pd.DataFrame, line_items_df: pd.DataFrame,
                        lob_label: str, month_label: str) -> bytes:
    """
    Builds a revenue accrual workbook matching the January 2026 template layout.
    15 columns A-N (K is a narrow spacer).
    Each job block: A/R header, job info row, spacer, line item header, lines, summary row.
    """
    from io import BytesIO
    from datetime import date as _date, datetime as _datetime
    import calendar
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = lob_label[:31]

    # ── Styles ────────────────────────────────────────────────────────────────
    GREEN_HEX   = "3D8840"
    GRAY_HEX    = "EFEFEF"
    GREEN_FILL  = PatternFill("solid", fgColor=GREEN_HEX)
    GRAY_FILL   = PatternFill("solid", fgColor=GRAY_HEX)
    NO_FILL     = PatternFill(fill_type=None)

    HAIR   = Side(style="hair")
    THIN   = Side(style="thin")
    MED    = Side(style="medium")
    NONE_S = Side(style=None)

    def _border(left=None, right=None, top=None, bottom=None):
        return Border(
            left=left   or NONE_S,
            right=right or NONE_S,
            top=top     or NONE_S,
            bottom=bottom or NONE_S,
        )

    hair_bottom = _border(bottom=HAIR)
    thin_bottom = _border(bottom=THIN)
    med_bottom  = _border(bottom=MED)

    def _font(bold=False, size=10, color="000000"):
        return Font(name="Arial", size=size, bold=bold, color=color)

    def _align(h="left", v="center", wrap=False):
        return Alignment(horizontal=h, vertical=v, wrap_text=wrap)

    def _w(ws, col_letter, width):
        ws.column_dimensions[col_letter].width = width

    # ── Column widths (A-N, K is spacer) ──────────────────────────────────────
    for col, w in [("A",9.13),("B",2.38),("C",18.38),("D",43.75),("E",18.0),
                   ("F",20.0),("G",8.75),("H",9.75),("I",9.38),("J",9.0),
                   ("K",2.13),("L",14.0),("M",14.0),("N",14.0)]:
        _w(ws, col, w)

    # ── Helper: write a cell ──────────────────────────────────────────────────
    def _c(row, col, value=None, font=None, fill=None, border=None,
           align=None, num_fmt=None):
        c = ws.cell(row=row, column=col, value=value)
        if font:    c.font   = font
        if fill:    c.fill   = fill
        if border:  c.border = border
        if align:   c.alignment = align
        if num_fmt: c.number_format = num_fmt
        return c

    def _merge(r1, c1, r2, c2):
        ws.merge_cells(start_row=r1, start_column=c1,
                       end_row=r2, end_column=c2)

    MONEY = "$#,##0.00"
    DATE_FMT = "MM/DD/YYYY"
    NUM_FMT  = "0.##"

    # ── Customer rollup panel (rows 2-49, cols A-F) ───────────────────────────
    # Will be filled after processing all jobs
    PANEL_START = 2
    customer_totals: dict[str, float] = {}
    grand_total = 0.0

    # LOB header row
    for col, label, h in [(1,"LOB","right"),(3,"Projection","center"),
                           (5,"Customer","left"),(6,"Total","center")]:
        _c(PANEL_START, col, label,
           font=_font(bold=True), fill=GREEN_FILL,
           border=hair_bottom, align=_align(h=h))

    current_row = PANEL_START + 1   # row 3 — customer rows filled in later
    PANEL_END_ROW = 49              # reserve rows 3-49 for customer list + total
    current_row = PANEL_END_ROW + 2  # start job blocks at row 51

    # ── Job blocks ────────────────────────────────────────────────────────────
    for job_num, (_, job) in enumerate(jobs_df.iterrows(), start=1):
        job_id    = int(job["id"])
        job_items = (
            line_items_df[line_items_df["job_id"] == job_id]
            .sort_values(["line_number", "id"])
            if not line_items_df.empty else pd.DataFrame()
        )

        # ── A/R + Approval header (green) ─────────────────────────────────
        ws.row_dimensions[current_row].height = 15.75
        _c(current_row, 8,  "A/R",
           font=_font(bold=True), fill=GREEN_FILL,
           border=hair_bottom, align=_align(h="center"))
        _merge(current_row, 12, current_row, 14)
        _c(current_row, 12, "Approval - Ops Manager",
           font=_font(bold=True), fill=GREEN_FILL,
           border=hair_bottom, align=_align(h="center"))
        current_row += 1

        # ── Column header row (green) ──────────────────────────────────────
        ws.row_dimensions[current_row].height = 15.75
        hdr_cols = [
            (1,"Job #","center"), (3,"LOB","center"),
            (4,"Lease Name/Number","center"), (5,"Customer","center"),
            (6,"Co. Man","center"), (8,"Initials","center"),
            (9,"Date","center"), (10,"Sent","center"),
            (12,"Initials","center"), (13,"Date","center"),
            (14,"Accrue","center"),
        ]
        for col, label, h in hdr_cols:
            _c(current_row, col, label,
               font=_font(bold=True), fill=GREEN_FILL,
               border=hair_bottom, align=_align(h=h))
        current_row += 1

        # ── Job info row (gray) ────────────────────────────────────────────
        ws.row_dimensions[current_row].height = 15.75
        job_info = [
            (1, job_num,                              "center"),
            (3, lob_label,                            "center"),
            (4, str(job.get("job_name","") or ""),    "left"),
            (5, str(job.get("customer","") or ""),    "center"),
            (6, str(job.get("company_man","") or ""), "center"),
        ]
        for col, val, h in job_info:
            _c(current_row, col, val,
               font=_font(), fill=GRAY_FILL,
               border=med_bottom, align=_align(h=h))
        current_row += 1

        # ── Spacer row ─────────────────────────────────────────────────────
        ws.row_dimensions[current_row].height = 6.75
        current_row += 1

        # ── Line item header (green) ───────────────────────────────────────
        ws.row_dimensions[current_row].height = 15.75
        li_hdrs = [
            (1,"Line #","center"), (4,"Description","center"),
            (5,"UOM","center"),    (8,"Start","center"),
            (9,"End","center"),    (10,"# of Days","center"),
            (12,"Invoice Qty","center"), (13,"Price","center"),
            (14,"Line Total","center"),
        ]
        for col, label, h in li_hdrs:
            _c(current_row, col, label,
               font=_font(bold=True), fill=GREEN_FILL,
               border=hair_bottom, align=_align(h=h))
        # A col has gray fill in template for line # header
        _c(current_row, 1, "Line #",
           font=_font(bold=True), fill=GRAY_FILL,
           border=thin_bottom, align=_align(h="center"))
        current_row += 1

        # ── Line item rows (gray) ──────────────────────────────────────────
        job_total = 0.0
        if job_items.empty:
            ws.row_dimensions[current_row].height = 15.75
            _merge(current_row, 1, current_row, 14)
            _c(current_row, 1, "(No line items entered yet)",
               font=_font(color="999999"), fill=GRAY_FILL,
               align=_align(h="center"))
            current_row += 1
        else:
            for li_num, li in enumerate(job_items.itertuples(), start=1):
                ws.row_dimensions[current_row].height = 15.75

                days = None
                if pd.notna(getattr(li,"start_date",None)) and pd.notna(getattr(li,"end_date",None)):
                    try:
                        days = (pd.to_datetime(li.end_date) -
                                pd.to_datetime(li.start_date)).days + 1
                    except Exception:
                        days = None

                lt = _num(li.line_total)
                qty   = _num(li.invoice_qty)
                price = _num(li.unit_price)
                if lt is None and qty is not None and price is not None:
                    lt = qty * price
                if lt is not None:
                    job_total += lt

                li_data = [
                    (1,  li_num,   "center", None),
                    (4,  str(li.description or ""), "left", None),
                    (5,  str(li.uom or ""),         "center", None),
                    (8,  pd.to_datetime(li.start_date).date()
                         if pd.notna(getattr(li,"start_date",None)) else None,
                         "center", DATE_FMT),
                    (9,  pd.to_datetime(li.end_date).date()
                         if pd.notna(getattr(li,"end_date",None)) else None,
                         "center", DATE_FMT),
                    (10, days,     "center", None),
                    (12, qty,      "center", NUM_FMT),
                    (13, price,    "center", MONEY),
                    (14, lt,       "right",  MONEY),
                ]
                for col, val, h, fmt in li_data:
                    _c(current_row, col, val,
                       font=_font(), fill=GRAY_FILL,
                       border=thin_bottom, align=_align(h=h),
                       num_fmt=fmt)
                current_row += 1

        # ── Summary row ────────────────────────────────────────────────────
        ws.row_dimensions[current_row].height = 15.75
        summary_data = [
            (3,  str(job.get("customer","") or ""),        "left"),
            (4,  str(job.get("county_state","") or ""),    "left"),
            (5,  str(job.get("invoice_number","") or ""),  "center"),
            (6,  str(job.get("so_ticket_number","") or ""),"center"),
            (12, _num(job.get("day_rate")),                "right"),
            (14, job_total,                                "right"),
        ]
        for col, val, h in summary_data:
            fmt = MONEY if col in (12, 14) else None
            _c(current_row, col, val,
               font=_font(bold=True), fill=GRAY_FILL,
               border=med_bottom, align=_align(h=h),
               num_fmt=fmt)
        # Labels above values
        for col, label in [(12,"Day Rate"),(14,"Job Total")]:
            c = ws.cell(row=current_row, column=col)
            # add label as comment-style — put it in the row above summary
            pass
        current_row += 2   # blank gap between jobs

        grand_total += job_total
        cust = str(job.get("customer","") or "Unassigned")
        customer_totals[cust] = customer_totals.get(cust, 0.0) + job_total

    # ── Fill customer rollup panel (rows 3-49) ────────────────────────────────
    panel_row = PANEL_START + 1
    # LOB + projection on row 3
    _c(panel_row, 1, lob_label,
       font=_font(bold=True), fill=GRAY_FILL,
       border=hair_bottom, align=_align(h="right"))
    _c(panel_row, 3, grand_total,
       font=_font(bold=True), fill=GRAY_FILL,
       border=hair_bottom, align=_align(h="center"), num_fmt=MONEY)
    panel_row += 1

    for cust, total in sorted(customer_totals.items()):
        if panel_row > PANEL_END_ROW:
            break
        ws.row_dimensions[panel_row].height = 15.75
        _c(panel_row, 5, cust,
           font=_font(), fill=GRAY_FILL,
           border=hair_bottom, align=_align(h="left"))
        _c(panel_row, 6, total,
           font=_font(), fill=GRAY_FILL,
           border=hair_bottom, align=_align(h="center"), num_fmt=MONEY)
        panel_row += 1

    # Grand total line in panel
    if panel_row <= PANEL_END_ROW:
        _c(panel_row, 5, "TOTAL",
           font=_font(bold=True), fill=GREEN_FILL,
           border=hair_bottom, align=_align(h="left"))
        _c(panel_row, 6, grand_total,
           font=_font(bold=True), fill=GREEN_FILL,
           border=hair_bottom, align=_align(h="center"), num_fmt=MONEY)

    ws.freeze_panes = "A51"

    # ── Summary sheet ─────────────────────────────────────────────────────────
    today = _date.today()
    try:
        from datetime import datetime as _dt
        month_dt = _dt.strptime(month_label, "%B %Y")
        report_month, report_year = month_dt.month, month_dt.year
    except Exception:
        report_month, report_year = today.month, today.year

    days_in_month = calendar.monthrange(report_year, report_month)[1]
    days_elapsed  = today.day if (report_month == today.month and
                                   report_year == today.year) else days_in_month

    ss = wb.create_sheet("Summary")
    for col, w in [("A",28),("B",18),("C",14),("D",18),("E",18),("F",16)]:
        ss.column_dimensions[col].width = w

    # Title
    ss.merge_cells("A1:F1")
    c = ss["A1"]
    c.value = f"{lob_label}  —  Monthly Summary  |  {month_label}"
    c.font  = _font(bold=True, size=12, color="FFFFFF")
    c.fill  = GREEN_FILL
    c.alignment = _align(h="center")
    ss.row_dimensions[1].height = 22

    # Metadata
    ss.merge_cells("A2:F2")
    c = ss["A2"]
    c.value = (f"Days Elapsed: {days_elapsed}  of  {days_in_month}"
               f"  |  Generated: {today.strftime('%m/%d/%Y')}")
    c.font  = _font(color="444444")
    c.alignment = _align(h="center")
    ss.row_dimensions[2].height = 16

    # Column headers
    for col_i, label in enumerate(
        ["Customer","Actual Total","Days Elapsed",
         "Daily Run Rate","Projected Total","Variance"], start=1):
        c = ss.cell(row=3, column=col_i, value=label)
        c.font   = _font(bold=True, color="FFFFFF")
        c.fill   = GREEN_FILL
        c.border = _border(bottom=THIN)
        c.alignment = _align(h="center")
    ss.row_dimensions[3].height = 22

    # Customer rows
    for r_i, (cust, actual) in enumerate(sorted(customer_totals.items()), start=4):
        run_rate  = actual / days_elapsed if days_elapsed > 0 else 0.0
        projected = run_rate * days_in_month
        variance  = projected - actual
        ss.row_dimensions[r_i].height = 15.75
        for col_i, val, fmt, h, bold, color in [
            (1, cust,      None,   "left",  False, "000000"),
            (2, actual,    MONEY,  "right", False, "000000"),
            (3, days_elapsed, "0","center", False, "000000"),
            (4, run_rate,  MONEY,  "right", False, "000000"),
            (5, projected, MONEY,  "right", False, "000000"),
            (6, variance,  MONEY,  "right", True,
             "375623" if variance >= 0 else "9C0006"),
        ]:
            c = ss.cell(row=r_i, column=col_i, value=val)
            c.font   = _font(bold=bold, color=color)
            c.fill   = GRAY_FILL
            c.border = _border(bottom=HAIR)
            c.alignment = _align(h=h)
            if fmt: c.number_format = fmt

    # Grand total
    tr = 4 + len(customer_totals)
    ss.row_dimensions[tr].height = 18
    gr  = grand_total / days_elapsed if days_elapsed > 0 else 0.0
    gp  = gr * days_in_month
    gv  = gp - grand_total
    for col_i, val, fmt, h in [
        (1, "TOTAL",       None,  "left"),
        (2, grand_total,   MONEY, "right"),
        (3, days_elapsed,  "0",   "center"),
        (4, gr,            MONEY, "right"),
        (5, gp,            MONEY, "right"),
        (6, gv,            MONEY, "right"),
    ]:
        c = ss.cell(row=tr, column=col_i, value=val)
        c.font   = _font(bold=True, color="FFFFFF")
        c.fill   = GREEN_FILL
        c.border = _border(bottom=MED)
        c.alignment = _align(h=h)
        if fmt: c.number_format = fmt

    ss.freeze_panes = "A4"

    out = BytesIO()
    wb.save(out)
    return out.getvalue()

def _num(val):
    """Convert to float or return None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except Exception:
        return None


def _fmt(val) -> str:
    n = _num(val)
    return f"{n:,.2f}" if n is not None else "—"


# ── Field Ticket Excel export ─────────────────────────────────────────────────

def build_ticket_excel(job: dict, line_items_df) -> bytes:
    """
    Builds a field ticket Excel matching the Elevate Energy Services template.
    job: dict of job fields (from a pandas Series .to_dict())
    line_items_df: DataFrame of line items for this job
    Returns bytes for st.download_button.
    """
    from io import BytesIO
    from openpyxl import Workbook
    from openpyxl.styles import (Alignment, Border, Font, PatternFill, Side)
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = "field ticket"

    # ── Column widths (match template) ────────────────────────────────────────
    col_widths = [13.71, 10.86, 7.57, 14.57, 16.71, 18.0, 10.43, 14.14]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # ── Row heights ───────────────────────────────────────────────────────────
    row_heights = {1:15, 2:24.75, 3:40.5, 4:17.25, 5:24, 6:18, 7:18, 8:18,
                   9:18, 10:18, 11:52.5, 12:18, 31:42.75, 32:42.75, 33:78.6, 34:24.95}
    for r in range(13, 31):
        row_heights[r] = 18
    for r, h in row_heights.items():
        ws.row_dimensions[r].height = h

    # ── Style helpers ─────────────────────────────────────────────────────────
    THIN = Side(style="thin", color="000000")
    THICK = Side(style="medium", color="000000")
    thin_border = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
    thick_border = Border(left=THICK, right=THICK, top=THICK, bottom=THICK)

    def _f(bold=False, size=10, name="Arial"):
        return Font(name=name, size=size, bold=bold)

    def _a(h="left", v="center", wrap=False):
        return Alignment(horizontal=h, vertical=v, wrap_text=wrap)

    def _set(coord, value, bold=False, size=10, h="left", v="center",
             wrap=False, border=None, num_fmt=None):
        c = ws[coord]
        c.value = value
        c.font = _f(bold=bold, size=size)
        c.alignment = _a(h=h, v=v, wrap=wrap)
        if border:
            c.border = border
        if num_fmt:
            c.number_format = num_fmt
        return c

    def _merge(r1, c1, r2, c2):
        ws.merge_cells(start_row=r1, start_column=c1, end_row=r2, end_column=c2)

    # ── Row 1: Header ─────────────────────────────────────────────────────────
    _merge(1, 6, 1, 7)
    _set("F1", "DELIVERY TICKET", bold=False, size=12, h="center")
    so_val = str(job.get("so_ticket_number", "") or "")
    _set("H1", so_val, bold=True, size=10)

    # ── Rows 2-4: Company info ────────────────────────────────────────────────
    _merge(2, 1, 4, 3)
    c = ws["A2"]
    c.value = "ELEVATE ENERGY SERVICES, LLC\n3696 1ST Avenue Greeley, CO 80631\n970-673-4800"
    c.font = _f(size=10)
    c.alignment = _a(h="center", v="center", wrap=True)

    # ── Row 5: FIELD TICKET title ─────────────────────────────────────────────
    _merge(5, 1, 5, 8)
    _set("A5", "FIELD TICKET", bold=True, size=14, h="center")

    # ── Header fields rows 6-10 ───────────────────────────────────────────────
    def _header_label(coord, text):
        c = ws[coord]
        c.value = text
        c.font = _f(bold=True, size=9)
        c.border = thin_border
        c.alignment = _a(h="left", v="center")

    def _header_value(coord, value):
        c = ws[coord]
        c.value = value
        c.font = _f(bold=False, size=10)
        c.border = thin_border
        c.alignment = _a(h="left", v="center")

    # Left column labels
    _header_label("A6", "CUSTOMER NAME")
    _header_label("A7", "EES SUPERVISOR")
    _header_label("A8", "CUSTOMER PO")
    _header_label("A9", "COUNTY AND STATE")
    _header_label("A10", "LOCATION")

    # Left column values (merged C:D for narrow cols)
    _merge(6, 3, 6, 4); _header_value("C6", str(job.get("customer", "") or ""))
    _merge(7, 3, 7, 4); _header_value("C7", str(job.get("ees_supervisor", "") or ""))
    _merge(8, 3, 8, 4); _header_value("C8", str(job.get("customer_po", "") or ""))
    _merge(9, 3, 9, 4); _header_value("C9", str(job.get("county_state", "") or ""))
    _merge(10, 3, 10, 4); _header_value("C10", str(job.get("location", "") or ""))

    # Right column labels
    _header_label("E6", "DATE OF SERVICE")
    _header_label("E7", "WELL NAME")
    _header_label("E8", "WELL NUMBER")
    _header_label("E9", "ORDERED BY")
    _header_label("E10", "DEPARTMENT")

    # Right column values (merged F:H)
    from datetime import date as dt_date
    svc_date = job.get("job_start_date")
    if svc_date:
        try:
            svc_date = str(svc_date)
        except Exception:
            svc_date = ""
    _merge(6, 6, 6, 8); _header_value("F6", svc_date or "")
    _merge(7, 6, 7, 8); _header_value("F7", str(job.get("well_name", "") or ""))
    _merge(8, 6, 8, 8); _header_value("F8", str(job.get("well_number", "") or ""))
    _merge(9, 6, 9, 8); _header_value("F9", str(job.get("ordered_by", "") or ""))
    _merge(10, 6, 10, 8); _header_value("F10", str(job.get("department", "") or ""))

    # ── Row 11: Job description ───────────────────────────────────────────────
    _header_label("A11", "JOB DESCRIPTION")
    _merge(11, 2, 11, 8)
    c = ws["B11"]
    c.value = str(job.get("job_description", "") or "")
    c.font = _f(bold=True, size=11)
    c.alignment = _a(h="center", v="center", wrap=True)
    c.border = thin_border

    # ── Row 12: Column headers ────────────────────────────────────────────────
    for col, label in [(1,"LENGTH TIME"),(2,"QTY"),(3,"UNIT"),(7,"UNIT PRICE"),(8,"AMOUNT")]:
        c = ws.cell(row=12, column=col, value=label)
        c.font = _f(bold=True, size=9)
        c.border = thin_border
        c.alignment = _a(h="center", v="center")
    _merge(12, 4, 12, 6)
    c = ws["D12"]
    c.value = "DESCRIPTION"
    c.font = _f(bold=True, size=9)
    c.border = thin_border
    c.alignment = _a(h="center", v="center")

    # ── Rows 13-30: Line items ────────────────────────────────────────────────
    MAX_LINES = 18
    items = line_items_df.to_dict("records") if not line_items_df.empty else []

    for i in range(MAX_LINES):
        row = 13 + i
        _merge(row, 4, row, 6)

        if i < len(items):
            li = items[i]
            qty = _num(li.get("invoice_qty"))
            price = _num(li.get("unit_price"))
            total = _num(li.get("line_total"))
            if total is None and qty is not None and price is not None:
                total = qty * price
            desc = str(li.get("description", "") or "")
            uom  = str(li.get("uom", "") or "")

            for col, val, fmt, h in [
                (2, qty,   "0.##",     "center"),
                (3, uom,   None,       "center"),
                (7, price, "#,##0.00", "right"),
                (8, total, "#,##0.00", "right"),
            ]:
                c = ws.cell(row=row, column=col, value=val)
                c.font = _f(); c.border = thin_border
                c.alignment = _a(h=h, v="center")
                if fmt and val is not None: c.number_format = fmt

            c = ws["D" + str(row)]
            c.value = desc
            c.font = _f(); c.border = thin_border
            c.alignment = _a(h="center", v="center")
        else:
            # Empty bordered row
            for col in [2, 3, 8]:
                c = ws.cell(row=row, column=col)
                c.border = thin_border
            c = ws["D" + str(row)]
            c.border = thin_border

        # Apply border to merged D:F cell
        ws.cell(row=row, column=4).border = thin_border

    # ── Row 31: Total ─────────────────────────────────────────────────────────
    _merge(31, 1, 31, 6)
    c = ws["A31"]
    c.value = "ELEVATE ENERGY SERVICES, LLC"
    c.font = _f(bold=True, size=11)
    c.border = thin_border
    c.alignment = _a(h="left", v="center")

    _set("G31", "TOTAL", bold=True, size=10, h="left", border=thin_border)
    # SUM formula for amounts
    c = ws["H31"]
    c.value = f"=SUM(H13:H30)"
    c.font = _f(bold=True, size=11)
    c.border = thin_border
    c.number_format = "#,##0.00"
    c.alignment = _a(h="right", v="center")

    # ── Rows 32-33: Signatures ────────────────────────────────────────────────
    _merge(32, 1, 33, 5)
    c = ws["A32"]
    c.value = "WELL CODING (PLEASE STAMP AND CODE HERE)"
    c.font = _f(bold=True, size=10)
    c.border = thin_border
    c.alignment = _a(h="left", v="top", wrap=True)

    _merge(32, 6, 32, 8)
    _set("F32", "Customer Name Print", bold=True, size=10, h="center", border=thin_border)
    _merge(33, 6, 33, 8)
    _set("F33", "Customer Signature", bold=True, size=10, h="center", border=thin_border)

    # ── Row 34: Tax disclaimer ────────────────────────────────────────────────
    _merge(34, 1, 34, 8)
    c = ws["A34"]
    c.value = "Any applicable taxes will be applied at time of invoicing."
    c.font = _f(bold=True, size=9)
    c.border = thin_border
    c.alignment = _a(h="center", v="center")

    out = BytesIO()
    wb.save(out)
    return out.getvalue()
