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
    Builds a formatted revenue accrual Excel workbook matching the template layout.
    Returns bytes ready for st.download_button.
    """
    from openpyxl import Workbook
    from openpyxl.styles import (Alignment, Border, Font, PatternFill, Side,
                                 numbers)
    from openpyxl.utils import get_column_letter

    wb = Workbook()
    ws = wb.active
    ws.title = lob_label[:31]

    # ── Styles ────────────────────────────────────────────────────────────────
    HEADER_FILL   = PatternFill("solid", fgColor="1F4E79")
    JOB_FILL      = PatternFill("solid", fgColor="D6E4F0")
    SUMMARY_FILL  = PatternFill("solid", fgColor="E2EFDA")
    TOTAL_FILL    = PatternFill("solid", fgColor="BDD7EE")
    THIN          = Side(style="thin", color="AAAAAA")
    THICK         = Side(style="medium", color="444444")
    thin_border   = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
    thick_bottom  = Border(bottom=THICK)

    def hdr_font(size=10, bold=True, color="FFFFFF"):
        return Font(name="Arial", size=size, bold=bold, color=color)

    def body_font(size=10, bold=False, color="000000"):
        return Font(name="Arial", size=size, bold=bold, color=color)

    def money_fmt(cell):
        cell.number_format = '#,##0.00'

    def date_fmt(cell):
        cell.number_format = 'MM/DD/YYYY'

    def center(cell):
        cell.alignment = Alignment(horizontal="center", vertical="center")

    def _set(ws, row, col, value, font=None, fill=None, border=None,
             align=None, num_fmt=None):
        c = ws.cell(row=row, column=col, value=value)
        if font:   c.font   = font
        if fill:   c.fill   = fill
        if border: c.border = border
        if align:  c.alignment = align
        if num_fmt: c.number_format = num_fmt
        return c

    # ── Column widths ─────────────────────────────────────────────────────────
    col_widths = [6, 30, 10, 14, 14, 8, 14, 14, 16]
    col_names  = ["Line #", "Description", "UOM", "Start", "End",
                  "# of Days", "Invoice Qty", "Unit Price", "Line Total"]
    for i, w in enumerate(col_widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    current_row = 1

    # ── LOB header bar ────────────────────────────────────────────────────────
    ws.merge_cells(start_row=current_row, start_column=1,
                   end_row=current_row, end_column=9)
    c = ws.cell(row=current_row, column=1,
                value=f"{lob_label}  —  Revenue Accrual  |  {month_label}")
    c.font = hdr_font(size=12)
    c.fill = HEADER_FILL
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[current_row].height = 20
    current_row += 1

    # ── Column headers ────────────────────────────────────────────────────────
    for col_i, name in enumerate(col_names, start=1):
        c = ws.cell(row=current_row, column=col_i, value=name)
        c.font   = hdr_font()
        c.fill   = HEADER_FILL
        c.border = thin_border
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    ws.row_dimensions[current_row].height = 30
    current_row += 1

    grand_total = 0.0
    customer_totals: dict[str, float] = {}

    for _, job in jobs_df.iterrows():
        job_id = int(job["id"])
        job_items = (
            line_items_df[line_items_df["job_id"] == job_id]
            .sort_values(["line_number", "id"])
            if not line_items_df.empty else pd.DataFrame()
        )

        # ── Job header ────────────────────────────────────────────────────────
        ws.merge_cells(start_row=current_row, start_column=1,
                       end_row=current_row, end_column=9)
        job_header_text = (
            f"Job: {job['job_code']}  |  {job['job_name']}"
            f"  |  Customer: {job.get('customer', '') or '—'}"
            f"  |  Company Man: {job.get('company_man', '') or '—'}"
            f"  |  Invoice: {job.get('invoice_number', '') or '—'}"
            f"  |  SO/Ticket: {job.get('so_ticket_number', '') or '—'}"
        )
        c = ws.cell(row=current_row, column=1, value=job_header_text)
        c.font = body_font(bold=True)
        c.fill = JOB_FILL
        c.border = thick_bottom
        ws.row_dimensions[current_row].height = 16
        current_row += 1

        job_total = 0.0

        if job_items.empty:
            ws.merge_cells(start_row=current_row, start_column=1,
                           end_row=current_row, end_column=9)
            ws.cell(row=current_row, column=1,
                    value="  (No line items entered yet)").font = body_font(color="999999")
            current_row += 1
        else:
            for li_i, li in enumerate(job_items.itertuples(), start=1):
                # Compute days if both dates present
                days = None
                if pd.notna(li.start_date) and pd.notna(li.end_date):
                    try:
                        days = (pd.to_datetime(li.end_date) -
                                pd.to_datetime(li.start_date)).days + 1
                    except Exception:
                        days = None

                # Line total: prefer stored value, else compute
                lt = _num(li.line_total)
                if lt is None:
                    qty = _num(li.invoice_qty)
                    price = _num(li.unit_price)
                    lt = (qty * price) if (qty is not None and price is not None) else None

                row_vals = [
                    li_i,
                    str(li.description or ""),
                    str(li.uom or ""),
                    pd.to_datetime(li.start_date).date() if pd.notna(li.start_date) else None,
                    pd.to_datetime(li.end_date).date()   if pd.notna(li.end_date)   else None,
                    days,
                    _num(li.invoice_qty),
                    _num(li.unit_price),
                    lt,
                ]
                for col_i, val in enumerate(row_vals, start=1):
                    c = ws.cell(row=current_row, column=col_i, value=val)
                    c.font   = body_font()
                    c.border = thin_border
                    c.alignment = Alignment(vertical="center",
                                            horizontal="center" if col_i in (1, 3, 6, 7) else "left")
                    if col_i in (4, 5) and val is not None:
                        c.number_format = "MM/DD/YYYY"
                    if col_i in (8, 9) and val is not None:
                        c.number_format = "#,##0.00"

                if lt is not None:
                    job_total += lt
                current_row += 1

        # ── Job total row ─────────────────────────────────────────────────────
        ws.merge_cells(start_row=current_row, start_column=1,
                       end_row=current_row, end_column=7)
        c = ws.cell(row=current_row, column=1,
                    value=f"Day Rate: ${_fmt(job.get('day_rate'))}     Job Total:")
        c.font = body_font(bold=True)
        c.fill = SUMMARY_FILL
        c.alignment = Alignment(horizontal="right", vertical="center")

        c2 = ws.cell(row=current_row, column=8, value=None)   # spacer
        c3 = ws.cell(row=current_row, column=9, value=job_total)
        c3.font = body_font(bold=True)
        c3.fill = SUMMARY_FILL
        c3.number_format = "#,##0.00"
        c3.alignment = Alignment(horizontal="right", vertical="center")
        ws.row_dimensions[current_row].height = 16
        current_row += 2   # blank gap between jobs

        grand_total += job_total
        cust = str(job.get("customer", "") or "Unassigned")
        customer_totals[cust] = customer_totals.get(cust, 0.0) + job_total

    # ── Grand total bar ───────────────────────────────────────────────────────
    ws.merge_cells(start_row=current_row, start_column=1,
                   end_row=current_row, end_column=8)
    c = ws.cell(row=current_row, column=1,
                value=f"TOTAL  —  {lob_label}  |  {month_label}")
    c.font = hdr_font(size=11)
    c.fill = TOTAL_FILL
    c.alignment = Alignment(horizontal="right", vertical="center")
    c2 = ws.cell(row=current_row, column=9, value=grand_total)
    c2.font = hdr_font(size=11, color="000000")
    c2.fill = TOTAL_FILL
    c2.number_format = "#,##0.00"
    c2.alignment = Alignment(horizontal="right", vertical="center")
    ws.row_dimensions[current_row].height = 20
    current_row += 2

    # ── Customer rollup ───────────────────────────────────────────────────────
    ws.merge_cells(start_row=current_row, start_column=1,
                   end_row=current_row, end_column=9)
    c = ws.cell(row=current_row, column=1, value="Customer Totals")
    c.font = hdr_font()
    c.fill = HEADER_FILL
    c.alignment = Alignment(horizontal="center")
    current_row += 1

    for cust, total in sorted(customer_totals.items()):
        ws.merge_cells(start_row=current_row, start_column=1,
                       end_row=current_row, end_column=8)
        ws.cell(row=current_row, column=1, value=cust).font = body_font()
        ct = ws.cell(row=current_row, column=9, value=total)
        ct.font = body_font(bold=True)
        ct.number_format = "#,##0.00"
        ct.alignment = Alignment(horizontal="right")
        current_row += 1

    # freeze top two rows
    ws.freeze_panes = "A3"

    out = BytesIO()
    wb.save(out)
    return out.getvalue()


# ── Helpers ───────────────────────────────────────────────────────────────────

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
    grand_total = 0.0

    # Merge D:F for all 18 line item rows up front (before writing values)
    for i in range(MAX_LINES):
        _merge(13 + i, 4, 13 + i, 6)

    for i in range(MAX_LINES):
        row = 13 + i

        if i < len(items):
            li = items[i]
            qty   = _num(li.get("invoice_qty"))
            price = _num(li.get("unit_price"))
            stored_total = _num(li.get("line_total"))
            desc  = str(li.get("description", "") or "")
            uom   = str(li.get("uom", "") or "")

            # Compute line total
            if stored_total is not None:
                line_total_val = stored_total
            elif qty is not None and price is not None:
                line_total_val = qty * price
            else:
                line_total_val = None
            if line_total_val is not None:
                grand_total += line_total_val

            # B: Qty
            c = ws.cell(row=row, column=2, value=qty)
            c.font = _f(); c.border = thin_border
            c.alignment = _a(h="center", v="center")

            # C: UOM
            c = ws.cell(row=row, column=3, value=uom)
            c.font = _f(); c.border = thin_border
            c.alignment = _a(h="center", v="center")

            # D: Description (merged D:F — write to top-left cell only)
            c = ws.cell(row=row, column=4, value=desc)
            c.font = _f(); c.border = thin_border
            c.alignment = _a(h="left", v="center")

            # G: Unit Price
            c = ws.cell(row=row, column=7, value=price)
            c.font = _f(); c.border = thin_border
            c.alignment = _a(h="right", v="center")
            c.number_format = "$#,##0.00"

            # H: Amount — store calculated value; Excel formula for live editing
            c = ws.cell(row=row, column=8, value=line_total_val)
            c.font = _f(); c.border = thin_border
            c.alignment = _a(h="right", v="center")
            c.number_format = "$#,##0.00"

        else:
            # Empty row — borders + dollar formatting, no values
            for col, num_fmt in [(2, "General"), (3, "General"),
                                  (4, "General"), (7, "$#,##0.00"),
                                  (8, "$#,##0.00")]:
                c = ws.cell(row=row, column=col, value=None)
                c.font = _f(); c.border = thin_border
                c.number_format = num_fmt

    # ── Row 31: Total ─────────────────────────────────────────────────────────
    _merge(31, 1, 31, 6)
    c = ws["A31"]
    c.value = "ELEVATE ENERGY SERVICES, LLC"
    c.font = _f(bold=True, size=11)
    c.border = thin_border
    c.alignment = _a(h="left", v="center")

    _set("G31", "TOTAL", bold=True, size=10, h="left", border=thin_border)
    # Pre-calculated total — shows without LibreOffice recalc
    c = ws["H31"]
    c.value = grand_total
    c.font = _f(bold=True, size=11)
    c.border = thin_border
    c.number_format = "$#,##0.00"
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
