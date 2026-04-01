
from __future__ import annotations
from datetime import date
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import colorsys
import streamlit as st

from services.db import export_excel, get_engine, init_db, query_df
from services.models import calc_job_dates
from services.scheduler import (
    add_pool_adjustment, allocation_debug_df, create_job, create_requirement, delete_job,
    delete_pool, delete_pool_adjustment, delete_requirement, get_fulfillment_df,
    get_jobs_df, get_pools_df, pool_snapshot_df, recalc_all_requirements,
    requirement_summary_df, update_job, update_requirement, upsert_pool,
    create_rental_requirement, delete_rental_requirement, get_rental_requirements_df,
    delete_manual_owned_allocation, get_manual_owned_allocations_df,
    upsert_manual_owned_allocation_for_job_class, upsert_rental_requirement_for_job_class,
)

st.set_page_config(page_title="Resource Scheduler V2", layout="wide")
st.title("Resource Scheduler V2")

EXCLUDED_CALC_STATUSES = {"Bid", "Awarded"}

st.markdown(
    '''
    <style>
    div[data-testid="stDataEditor"] [data-testid="stDataFrameResizable"] table td:nth-child(2),
    div[data-testid="stDataEditor"] [data-testid="stDataFrameResizable"] table td:nth-child(3),
    div[data-testid="stDataEditor"] [data-testid="stDataFrameResizable"] table td:nth-child(4),
    div[data-testid="stDataEditor"] [data-testid="stDataFrameResizable"] table th:nth-child(2),
    div[data-testid="stDataEditor"] [data-testid="stDataFrameResizable"] table th:nth-child(3),
    div[data-testid="stDataEditor"] [data-testid="stDataFrameResizable"] table th:nth-child(4) {
        text-align: center !important;
    }
    .job-req-summary table {
        width: 100%;
        border-collapse: collapse;
    }
    .job-req-summary th, .job-req-summary td {
        padding: 8px 10px;
        border-bottom: 1px solid #e6e6e6;
    }
    .job-req-summary th {
        border-bottom: 2px solid #d0d0d0;
        text-align: left;
    }
    .job-req-summary .qty {
        text-align: center !important;
    }
    .sticky-job-summary {
        position: sticky;
        top: 4.2rem;
        z-index: 50;
        background: white;
        border: 1px solid #d9d9d9;
        border-left: 5px solid #1f77b4;
        border-radius: 10px;
        padding: 10px 12px;
        margin: 8px 0 12px 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    .sticky-job-summary .title {
        font-size: 0.95rem;
        font-weight: 700;
        margin-bottom: 6px;
    }
    .sticky-job-summary .line {
        font-size: 0.95rem;
        line-height: 1.4;
    }
    </style>
    ''',
    unsafe_allow_html=True,
)

engine = get_engine()
init_db(engine)

try:
    query_df(engine, "SELECT customer_color FROM jobs LIMIT 1")
except Exception:
    try:
        from services.db import execute
        execute(engine, "ALTER TABLE jobs ADD COLUMN customer_color TEXT")
    except Exception:
        pass


if "create_job_start_date" not in st.session_state:
    st.session_state["create_job_start_date"] = date.today()

def format_date_value(value):
    if pd.isna(value):
        return ""
    return pd.to_datetime(value).strftime("%m/%d/%Y")

def format_dates_for_display(df: pd.DataFrame) -> pd.DataFrame:
    formatted = df.copy()
    for col in formatted.columns:
        if any(token in col.lower() for token in ["date", "start", "end", "created_at", "adjustment_date"]):
            try:
                formatted[col] = pd.to_datetime(formatted[col]).dt.strftime("%m/%d/%Y")
            except Exception:
                pass
    return formatted

def load_lookups():
    regions = query_df(engine, "SELECT region_code, region_name FROM regions WHERE active = TRUE ORDER BY region_code")
    resource_classes = query_df(engine, "SELECT id, class_name, category, unit_type, planning_mode FROM resource_classes ORDER BY id")
    jobs = get_jobs_df(engine)
    return regions, resource_classes, jobs

def region_filter(df: pd.DataFrame, active_region: str) -> pd.DataFrame:
    if df.empty or active_region == "Global" or "region_code" not in df.columns:
        return df
    return df.loc[df["region_code"] == active_region].copy()


def filter_active_jobs_for_management(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    working = df.copy()
    if "job_end_date" not in working.columns or "status" not in working.columns:
        return working
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=120)
    end_dates = pd.to_datetime(working["job_end_date"], errors="coerce")
    is_old_completed = (working["status"] == "Complete") & (end_dates < cutoff)
    return working.loc[~is_old_completed].copy()


def filter_by_job_status(df: pd.DataFrame, include_excluded: bool = False) -> pd.DataFrame:
    if df.empty or "status" not in df.columns:
        return df
    statuses = df["status"].astype(str)
    if include_excluded:
        return df.loc[statuses.isin(EXCLUDED_CALC_STATUSES)].copy()
    return df.loc[~statuses.isin(EXCLUDED_CALC_STATUSES)].copy()

def render_pipeline_notice(title: str = "Bid / Awarded Jobs"):
    st.markdown(f"##### {title}")
    st.caption("These jobs are shown for planning visibility only and are excluded from needs and allocation calculations.")

regions_df, resource_classes_df, jobs_df = load_lookups()

def region_format(code: str) -> str:
    match = regions_df.loc[regions_df["region_code"] == code, "region_name"]
    return code if match.empty else f"{code} - {match.iloc[0]}"

def active_region_value(selected: str, widget_value: str) -> str:
    return widget_value if selected == "Global" else selected

def region_default_index(region_codes: list[str], active_region: str) -> int:
    return region_codes.index(active_region) if active_region != "Global" and active_region in region_codes else 0

def region_disabled(active_region: str) -> bool:
    return active_region != "Global"

def quantity_step(unit_type: str, category: str) -> float:
    return 0.125 if category == "Hose" or unit_type == "miles" else 1.0

def quantity_format(unit_type: str, category: str) -> str:
    return "%.3f" if quantity_step(unit_type, category) == 0.125 else "%.0f"

def display_class_name(row: pd.Series) -> str:
    name = str(row.get("class_name", "") or "")
    unit = str(row.get("unit_type", "") or "").lower()
    category = str(row.get("category", "") or "")
    if unit == "miles" or category == "Hose":
        return f"{name} (miles)"
    return name

def resource_options_df(include_rental: bool = False) -> pd.DataFrame:
    rc = resource_classes_df.copy()
    if not include_rental and "category" in rc.columns:
        rc = rc.loc[rc["category"].astype(str) != "Rental"].copy()
    rc["display"] = rc.apply(display_class_name, axis=1)
    return rc

def render_jobs_manage_table(df: pd.DataFrame, active_region: str):
    st.markdown("##### Manage Jobs")
    if df.empty:
        st.info("No jobs yet.")
        return

    widths = [1.3, 1.5, 1.0, 1.0, 1.0, 1.0, 1.0, 0.8]
    headers = ["Customer", "Job Name", "Job Code", "Mob Start", "Job Start", "Job End", "Demob End", "Manage"]

    hdr = st.columns(widths)
    for c, h in zip(hdr, headers):
        c.markdown(f"**{h}**")

    region_codes = regions_df["region_code"].tolist()

    for _, row in df.iterrows():
        cols = st.columns(widths)
        cols[0].write(str(row.get("customer", "") or ""))
        cols[1].write(str(row["job_name"]))
        cols[2].write(str(row["job_code"]))
        cols[3].write(format_date_value(row["mob_start_date"]))
        cols[4].write(format_date_value(row["job_start_date"]))
        cols[5].write(format_date_value(row["job_end_date"]))
        cols[6].write(format_date_value(row["demob_end_date"]))

        with cols[7].popover("Edit/Delete", use_container_width=True):
            region_idx = region_codes.index(row["region_code"])
            edit_customer = st.text_input("Customer", value=str(row.get("customer", "") or ""), key=f"job_customer_{row['id']}")
            edit_customer_color = st.color_picker("Job Color", value=str(row.get("customer_color", "") or "#1f77b4"), key=f"job_customer_color_{row['id']}")
            edit_region = st.selectbox("Region", region_codes, index=region_idx, format_func=region_format, disabled=region_disabled(active_region), key=f"job_region_{row['id']}")
            edit_job_name = st.text_input("Job Name", value=str(row["job_name"]), key=f"job_name_{row['id']}")
            edit_location = st.text_input("Location", value=str(row.get("location", "") or ""), key=f"job_loc_{row['id']}")
            edit_start = st.date_input("Job Start Date", value=pd.to_datetime(row["job_start_date"]).date(), key=f"job_start_{row['id']}")
            edit_duration = st.number_input("Job Duration (days)", min_value=1, value=int(row["job_duration_days"]), step=1, key=f"job_duration_{row['id']}")
            edit_mob = st.number_input("Mobilization Days Before Job", min_value=0, value=int(row["mob_days_before_job"]), step=1, key=f"job_mob_{row['id']}")
            edit_demob = st.number_input("Demobilization Days After Job", min_value=0, value=int(row["demob_days_after_job"]), step=1, key=f"job_demob_{row['id']}")
            statuses = ["Bid", "Awarded", "Planned", "Tentative", "Active", "Billing Pending", "Complete", "Cancelled"]
            status_index = statuses.index(row["status"]) if row["status"] in statuses else 0
            edit_status = st.selectbox("Status", statuses, index=status_index, key=f"job_status_{row['id']}")
            edit_notes = st.text_area("Notes", value=str(row.get("notes", "") or ""), key=f"job_notes_{row['id']}")
            a, b = st.columns(2)
            if a.button("Save", key=f"save_job_{row['id']}"):
                update_job(engine, int(row["id"]), {
                    "job_name": edit_job_name,
                    "region_code": active_region_value(active_region, edit_region),
                    "customer": edit_customer,
                    "customer_color": edit_customer_color,
                    "location": edit_location,
                    "job_start_date": edit_start,
                    "job_duration_days": int(edit_duration),
                    "mob_days_before_job": int(edit_mob),
                    "demob_days_after_job": int(edit_demob),
                    "status": edit_status,
                    "notes": edit_notes,
                })
                st.rerun()
            if b.button("Delete", key=f"delete_job_{row['id']}"):
                delete_job(engine, int(row["id"]))
                st.rerun()


def render_requirements_manage_table(df: pd.DataFrame, key_prefix: str = 'req'):
    st.markdown("##### Manage Requirements")
    if df.empty:
        st.info("No requirements yet.")
        return

    widths = [1.2, 1.4, 1.0, 1.3, 0.9, 0.95, 0.95, 1.1, 0.8]
    headers = ["Customer", "Job Name", "Job Code", "Class", "Quantity", "Assigned EES", "Assigned Rental", "Status", "Manage"]

    hdr = st.columns(widths)
    for c, h in zip(hdr, headers):
        c.markdown(f"**{h}**")

    rc_df = resource_options_df()
    rc_names = rc_df["class_name"].astype(str).tolist()

    for _, row in df.iterrows():
        cols = st.columns(widths)
        cols[0].write(str(row.get("customer", "") or "Unassigned"))
        cols[1].write(str(row.get("job_name", "") or ""))
        cols[2].write(str(row["job_code"]))
        cols[3].write(str(row["class_name"]))
        cols[4].write(format_compact_number(row["quantity_required"]))
        cols[5].write(format_compact_number(row.get("assigned_ees", 0)))
        cols[6].write(format_compact_number(row.get("assigned_rental", 0)))
        cols[7].write(str(row.get("allocation_status", "")))

        with cols[8].popover("Edit/Delete", use_container_width=True):
            legacy_class = str(row["class_name"])
            class_options = rc_names.copy()
            if legacy_class not in class_options:
                class_options = [legacy_class] + class_options
            current_idx = class_options.index(legacy_class)

            edit_class_name = st.selectbox(
                "Resource Class",
                class_options,
                index=current_idx,
                key=f"{key_prefix}_class_{row['id']}",
            )
            edit_rc_match = rc_df.loc[rc_df["class_name"] == edit_class_name]
            edit_rc_id = int(edit_rc_match.iloc[0]["id"]) if not edit_rc_match.empty else int(row["resource_class_id"])
            edit_unit_type = str(edit_rc_match.iloc[0]["unit_type"]) if not edit_rc_match.empty else str(row.get("unit_type", "units"))
            edit_category = str(edit_rc_match.iloc[0]["category"]) if not edit_rc_match.empty else ""
            step = quantity_step(edit_unit_type, edit_category)
            fmt = quantity_format(edit_unit_type, edit_category)

            edit_qty = st.number_input(
                "Quantity Required",
                min_value=0.0,
                value=float(row["quantity_required"]),
                step=step,
                format=fmt,
                key=f"{key_prefix}_qty_{row['id']}",
            )
            edit_assigned_ees = st.number_input(
                "Assigned EES",
                min_value=0.0,
                value=float(row.get("assigned_ees", 0.0)),
                step=step,
                format=fmt,
                key=f"{key_prefix}_assigned_ees_{row['id']}",
            )
            edit_assigned_rental = st.number_input(
                "Assigned Rental",
                min_value=0.0,
                value=float(row.get("assigned_rental", 0.0)),
                step=step,
                format=fmt,
                key=f"{key_prefix}_assigned_rental_{row['id']}",
            )
            edit_vendor = st.text_input(
                "Rental Vendor",
                value=str(row.get("rental_vendor", "") or ""),
                key=f"{key_prefix}_vendor_{row['id']}",
            )
            priorities = ["Low", "Normal", "High", "Critical"]
            p_index = priorities.index(row["priority"]) if row["priority"] in priorities else 1
            edit_priority = st.selectbox(
                "Priority",
                priorities,
                index=p_index,
                key=f"{key_prefix}_priority_{row['id']}",
            )
            edit_before = st.number_input(
                "Days Before Job Start",
                min_value=0,
                value=int(row["days_before_job_start"]),
                step=1,
                key=f"{key_prefix}_before_{row['id']}",
            )
            edit_after = st.number_input(
                "Days After Job End",
                min_value=0,
                value=int(row["days_after_job_end"]),
                step=1,
                key=f"{key_prefix}_after_{row['id']}",
            )
            edit_notes = st.text_area(
                "Notes",
                value=str(row.get("notes", "") or ""),
                key=f"{key_prefix}_notes_{row['id']}",
            )
            a, b = st.columns(2)
            if a.button("Save", key=f"{key_prefix}_save_{row['id']}"):
                update_requirement(
                    engine,
                    int(row["id"]),
                    {
                        "resource_class_id": edit_rc_id,
                        "quantity_required": float(edit_qty),
                        "days_before_job_start": int(edit_before),
                        "days_after_job_end": int(edit_after),
                        "priority": edit_priority,
                        "notes": edit_notes,
                    },
                )
                upsert_manual_owned_allocation_for_job_class(
                    engine,
                    int(row["job_id"]),
                    int(edit_rc_id),
                    float(edit_assigned_ees),
                    int(edit_before),
                    int(edit_after),
                    edit_notes,
                )
                upsert_rental_requirement_for_job_class(
                    engine,
                    int(row["job_id"]),
                    int(edit_rc_id),
                    float(edit_assigned_rental),
                    int(edit_before),
                    int(edit_after),
                    edit_vendor,
                    edit_notes,
                )
                st.rerun()
            if b.button("Delete", key=f"{key_prefix}_delete_{row['id']}"):
                delete_requirement(engine, int(row["id"]))
                st.rerun()


def render_pools_manage_table(df: pd.DataFrame, active_region: str):
    st.markdown("##### Manage Pools")
    if df.empty:
        st.info("No pool rows yet.")
        return
    hdr = st.columns([1.0, 1.3, 0.8, 0.8, 0.8, 1.0, 0.8])
    for c, h in zip(hdr, ["Region", "Class", "Pool Total", "Committed", "Available", "Status", "Manage"]):
        c.markdown(f"**{h}**")
    region_codes = regions_df["region_code"].tolist()
    base_pool_df = get_pools_df(engine)
    rc_df = resource_options_df()
    for _, row in df.iterrows():
        cols = st.columns([1.0, 1.3, 0.8, 0.8, 0.8, 1.0, 0.8])
        cols[0].write(region_format(str(row["region_code"])))
        cols[1].write(str(row["class_name"]))
        cols[2].write(str(row["total_pool"]))
        cols[3].write(str(row["committed_quantity"]))
        cols[4].write(str(row["available_quantity"]))
        cols[5].write(str(row["pool_status"]))
        rc_match = rc_df.loc[rc_df["class_name"] == row["class_name"]].iloc[0]
        step = quantity_step(str(rc_match["unit_type"]), str(rc_match["category"]))
        fmt = quantity_format(str(rc_match["unit_type"]), str(rc_match["category"]))
        with cols[6].popover("Edit/Delete", use_container_width=True):
            region_idx = region_codes.index(row["region_code"])
            edit_region = st.selectbox("Region", region_codes, index=region_idx, format_func=region_format, disabled=region_disabled(active_region), key=f"pool_region_{row['id']}")
            edit_qty = st.number_input("Base Quantity", min_value=0.0, value=float(row["base_quantity"]), step=step, format=fmt, key=f"pool_qty_{row['id']}")
            pool_row = base_pool_df.loc[base_pool_df["id"] == row["id"]].iloc[0]
            edit_notes = st.text_input("Notes", value=str(pool_row.get("notes", "") or ""), key=f"pool_notes_{row['id']}")
            a, b = st.columns(2)
            if a.button("Save", key=f"save_pool_{row['id']}"):
                upsert_pool(engine, active_region_value(active_region, edit_region), int(pool_row["resource_class_id"]), float(edit_qty), edit_notes)
                st.rerun()
            if b.button("Delete", key=f"delete_pool_{row['id']}"):
                delete_pool(engine, int(row["id"]))
                st.rerun()



def format_compact_number(val):
    try:
        f = float(val)
    except Exception:
        return str(val)
    if abs(f - round(f)) < 1e-9:
        return str(int(round(f)))
    s = f"{f:.3f}".rstrip("0").rstrip(".")
    return "0" if s == "-0" else s

def format_editor_quantity(val):
    try:
        f = float(val)
    except Exception:
        return ""
    if abs(f - round(f)) < 1e-9:
        return str(int(round(f)))
    s = f"{f:.3f}".rstrip("0").rstrip(".")
    return "0" if s == "-0" else s

def render_simple_html_table(df: pd.DataFrame, qty_columns: list[str] | None = None):
    qty_columns = qty_columns or []
    headers = list(df.columns)
    html_rows = []
    for _, rec in df.iterrows():
        cells = []
        for col in headers:
            klass = " qty" if col in qty_columns else ""
            cells.append(f"<td class='{klass.strip()}'>{rec[col]}</td>")
        html_rows.append("<tr>" + "".join(cells) + "</tr>")
    header_html = "".join(f"<th class='{'qty' if h in qty_columns else ''}'>{h}</th>" for h in headers)
    st.markdown(
        "<div class='job-req-summary'><table><thead><tr>" + header_html + "</tr></thead><tbody>" + "".join(html_rows) + "</tbody></table></div>",
        unsafe_allow_html=True,
    )


def availability_font_color(val):
    try:
        f = float(val)
    except Exception:
        return "#1b4f9b"
    return "#1f7a1f" if f >= 0 else "#b22222"


def customer_base_color(customer: str) -> str:
    palette = [
        "#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e",
        "#17becf", "#e377c2", "#8c564b", "#bcbd22", "#7f7f7f",
    ]
    idx = abs(hash(customer)) % len(palette)
    return palette[idx]


def shade_hex(hex_color: str, factor: float) -> str:
    hex_color = str(hex_color or "").lstrip("#")
    if len(hex_color) != 6:
        hex_color = customer_base_color("fallback").lstrip("#")
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    l = max(0.28, min(0.78, l * factor))
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return f"#{int(r2*255):02x}{int(g2*255):02x}{int(b2*255):02x}"

def hex_to_rgba(hex_color: str, alpha: float) -> str:
    hex_color = str(hex_color or "").lstrip("#")
    if len(hex_color) != 6:
        hex_color = customer_base_color("fallback").lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    a = max(0.0, min(1.0, float(alpha)))
    return f"rgba({r}, {g}, {b}, {a})"
def week_start_label(ts: pd.Timestamp) -> str:
    week_end = ts + pd.Timedelta(days=6)
    if ts.month == week_end.month:
        return f"{ts.strftime('%b')} {ts.day}-{week_end.day}"
    return f"{ts.strftime('%b')} {ts.day}-{week_end.strftime('%b')} {week_end.day}"


def _overlap_qty(df: pd.DataFrame, seg_start, seg_last_day, qty_col: str) -> float:
    if df.empty:
        return 0.0
    mask = (
        (pd.to_datetime(df["required_start"]) <= pd.to_datetime(seg_last_day))
        & (pd.to_datetime(df["required_end"]) >= pd.to_datetime(seg_start))
    )
    return float(df.loc[mask, qty_col].astype(float).sum()) if mask.any() else 0.0


def compute_planning_segments(req: pd.DataFrame, rental_df: pd.DataFrame, manual_df: pd.DataFrame, x0, num_weeks: int):
    x0 = pd.to_datetime(x0).normalize()
    x_end = x0 + pd.Timedelta(days=7 * num_weeks)

    change_points = {x0, x_end}
    for source_df in [req, rental_df, manual_df]:
        if not source_df.empty:
            for _, row in source_df.iterrows():
                start = pd.to_datetime(row["required_start"]).normalize()
                end_next = pd.to_datetime(row["required_end"]).normalize() + pd.Timedelta(days=1)
                if x0 <= start <= x_end:
                    change_points.add(start)
                if x0 <= end_next <= x_end:
                    change_points.add(end_next)

    weekly_lines = [x0 + pd.Timedelta(days=7 * i) for i in range(num_weeks)] + [x_end]
    filtered_weekly = []
    for ws in weekly_lines:
        too_close = any(abs((ws - cp).days) <= 3 for cp in change_points if cp != ws)
        if not too_close:
            filtered_weekly.append(ws)

    gridlines = sorted(change_points | set(filtered_weekly))
    segments = []
    for i in range(len(gridlines) - 1):
        seg_start = gridlines[i]
        seg_end = gridlines[i + 1]
        if seg_end > seg_start:
            segments.append((seg_start, seg_end))

    tickvals = gridlines
    ticktext = [pd.to_datetime(ts).strftime("%b %-d") for ts in gridlines]

    return gridlines, segments, tickvals, ticktext, x_end


def build_planning_board_data(active_region: str, selected_class: str | None, start_date, num_weeks: int, include_excluded: bool = False):
    req = filter_by_job_status(region_filter(requirement_summary_df(engine), active_region), include_excluded=include_excluded)
    rental_df = filter_by_job_status(region_filter(get_rental_requirements_df(engine), active_region), include_excluded=include_excluded)
    manual_df = filter_by_job_status(region_filter(get_manual_owned_allocations_df(engine), active_region), include_excluded=include_excluded)

    combined_classes = []
    for df in [req, rental_df, manual_df]:
        if not df.empty and "class_name" in df.columns:
            combined_classes.extend(df["class_name"].dropna().astype(str).tolist())
    class_order = resource_classes_df["class_name"].dropna().astype(str).tolist()
    available_classes = list(dict.fromkeys(combined_classes))
    class_options = [c for c in class_order if c in available_classes]
    if not class_options:
        return pd.DataFrame(), pd.DataFrame(), [], [], [], None, ""

    if not selected_class or selected_class not in class_options:
        selected_class = class_options[0]

    req = req.loc[req["class_name"] == selected_class].copy() if not req.empty else pd.DataFrame()
    rental_df = rental_df.loc[rental_df["class_name"] == selected_class].copy() if not rental_df.empty else pd.DataFrame()
    manual_df = manual_df.loc[manual_df["class_name"] == selected_class].copy() if not manual_df.empty else pd.DataFrame()

    jobs_lookup = region_filter(get_jobs_df(engine), active_region)
    for source_name, source_df in [("req", req), ("rental_df", rental_df), ("manual_df", manual_df)]:
        if not source_df.empty and not jobs_lookup.empty and "customer" in jobs_lookup.columns:
            merge_cols = ["job_code", "customer"]
            if "customer_color" in jobs_lookup.columns:
                merge_cols.append("customer_color")
            jobs_lookup_small = jobs_lookup[merge_cols].drop_duplicates()
            source_df = source_df.merge(jobs_lookup_small, on="job_code", how="left", suffixes=("", "_job"))
            if "customer_job" in source_df.columns:
                if "customer" not in source_df.columns:
                    source_df["customer"] = source_df["customer_job"]
                else:
                    source_df["customer"] = source_df["customer"].where(source_df["customer"].fillna("").astype(str).str.strip() != "", source_df["customer_job"])
                source_df = source_df.drop(columns=["customer_job"])
            if "customer_color_job" in source_df.columns:
                if "customer_color" not in source_df.columns:
                    source_df["customer_color"] = source_df["customer_color_job"]
                else:
                    source_df["customer_color"] = source_df["customer_color"].where(source_df["customer_color"].fillna("").astype(str).str.strip() != "", source_df["customer_color_job"])
                source_df = source_df.drop(columns=["customer_color_job"])
        if not source_df.empty:
            source_df["customer"] = source_df.get("customer", "Unassigned")
            source_df["customer"] = source_df["customer"].fillna("").replace("", "Unassigned")
            source_df["customer_color"] = source_df.get("customer_color", "").fillna("")
        if source_name == "req":
            req = source_df
        elif source_name == "rental_df":
            rental_df = source_df
        else:
            manual_df = source_df

    start_ts = pd.to_datetime(start_date).normalize()
    gridlines, segments, tickvals, ticktext, x_end = compute_planning_segments(req, rental_df, manual_df, start_ts, num_weeks)

    key_map = {}
    for source_df in [req, rental_df, manual_df]:
        if source_df.empty:
            continue
        for _, row in source_df.iterrows():
            key = (row["job_code"], row["job_name"], row.get("customer", "Unassigned"), row.get("customer_color", ""), row["unit_type"])
            row_start = pd.to_datetime(row["required_start"])
            row_end = pd.to_datetime(row["required_end"])
            if key not in key_map:
                key_map[key] = {
                    "customer": str(row.get("customer", "") or "Unassigned"),
                    "customer_color": str(row.get("customer_color", "") or ""),
                    "job_code": row["job_code"],
                    "job_name": row["job_name"],
                    "required_start": row_start,
                    "required_end": row_end,
                    "unit_type": row["unit_type"],
                }
            else:
                key_map[key]["required_start"] = min(key_map[key]["required_start"], row_start)
                key_map[key]["required_end"] = max(key_map[key]["required_end"], row_end)

    rows = []
    for _, rec in sorted(key_map.items(), key=lambda kv: (kv[1]["customer"], kv[1]["required_start"], kv[1]["job_name"], kv[1]["job_code"])):
        job_req = req[(req["job_code"] == rec["job_code"])] if not req.empty else pd.DataFrame()
        job_rent = rental_df[(rental_df["job_code"] == rec["job_code"])] if not rental_df.empty else pd.DataFrame()
        job_manual = manual_df[(manual_df["job_code"] == rec["job_code"])] if not manual_df.empty else pd.DataFrame()

        ees_qty = float(job_manual["quantity_assigned"].astype(float).sum()) if not job_manual.empty else float(job_req["quantity_assigned"].astype(float).sum()) if not job_req.empty else 0.0
        rental_qty = float(job_rent["quantity_required"].astype(float).sum()) if not job_rent.empty else 0.0
        vendors = ", ".join(sorted(set(v for v in job_rent.get("vendor_name", pd.Series(dtype=str)).fillna("").tolist() if str(v).strip())))

        total_required = float(job_req["quantity_required"].astype(float).sum()) if not job_req.empty else 0.0
        unit_label = str(rec["unit_type"]).title() if str(rec["unit_type"]).lower() == "miles" else str(rec["unit_type"])

        breakdown_parts = []
        if ees_qty > 0:
            breakdown_parts.append(f"{format_compact_number(ees_qty)} EES")
        if rental_qty > 0:
            rental_text = f"{format_compact_number(rental_qty)} Rental"
            if vendors:
                rental_text += f", {vendors}"
            breakdown_parts.append(rental_text)

        label = f"{rec['job_name']}, {format_compact_number(total_required)} {unit_label}"
        if breakdown_parts:
            label += f", ({' / '.join(breakdown_parts)})"

        rows.append(
            {
                "customer": rec["customer"],
                "customer_color": rec["customer_color"],
                "job_code": rec["job_code"],
                "job_name": rec["job_name"],
                "label": label,
                "required_start": rec["required_start"],
                "required_end": rec["required_end"],
                "unit_type": rec["unit_type"],
            }
        )
    board_df = pd.DataFrame(rows)

    summary_rows = []
    active_req_all = filter_by_job_status(region_filter(requirement_summary_df(engine), active_region), include_excluded=False)
    active_rental_all = filter_by_job_status(region_filter(get_rental_requirements_df(engine), active_region), include_excluded=False)
    if not active_req_all.empty:
        active_req_all = active_req_all.loc[active_req_all["class_name"] == selected_class].copy()
    if not active_rental_all.empty:
        active_rental_all = active_rental_all.loc[active_rental_all["class_name"] == selected_class].copy()

    for seg_start, seg_end in segments:
        seg_last_day = (seg_end - pd.Timedelta(days=1)).normalize()
        need = _overlap_qty(req, seg_start, seg_last_day, "quantity_required")
        rental_active = _overlap_qty(rental_df, seg_start, seg_last_day, "quantity_required")

        snap = region_filter(pool_snapshot_df(engine, as_of_date=seg_start.date()), active_region)
        total_pool = 0.0
        if not snap.empty:
            snap_match = snap.loc[snap["class_name"] == selected_class]
            if not snap_match.empty:
                total_pool = float(snap_match["total_pool"].iloc[0])

        in_region = total_pool + rental_active
        availability = in_region - need
        segment_mid = seg_start + (seg_end - seg_start) / 2

        if include_excluded:
            active_need = _overlap_qty(active_req_all, seg_start, seg_last_day, "quantity_required")
            active_rental = _overlap_qty(active_rental_all, seg_start, seg_last_day, "quantity_required")
            active_in_region = total_pool + active_rental
            unallocated_pool = active_in_region - active_need
            projected_availability = unallocated_pool - need

            summary_rows.extend([
                {"Metric": "Need", "SegmentStart": seg_start, "SegmentEnd": seg_end, "X": segment_mid, "Value": need},
                {"Metric": "Unallocated Pool", "SegmentStart": seg_start, "SegmentEnd": seg_end, "X": segment_mid, "Value": unallocated_pool},
                {"Metric": "Availability", "SegmentStart": seg_start, "SegmentEnd": seg_end, "X": segment_mid, "Value": projected_availability},
            ])
        else:
            summary_rows.extend([
                {"Metric": "Need", "SegmentStart": seg_start, "SegmentEnd": seg_end, "X": segment_mid, "Value": need},
                {"Metric": "In Region", "SegmentStart": seg_start, "SegmentEnd": seg_end, "X": segment_mid, "Value": in_region},
                {"Metric": "Availability", "SegmentStart": seg_start, "SegmentEnd": seg_end, "X": segment_mid, "Value": availability},
            ])

    summary_df = pd.DataFrame(summary_rows)
    return board_df, summary_df, gridlines, tickvals, ticktext, x_end, selected_class



def render_planning_board(active_region: str, include_excluded: bool = False, section_title: str = "Planning Board"):
    st.subheader(section_title)
    req_all = filter_by_job_status(region_filter(requirement_summary_df(engine), active_region), include_excluded=include_excluded)
    if req_all.empty:
        st.info("No requirements yet." if not include_excluded else "No Bid / Awarded requirements yet.")
        return

    available_classes = req_all["class_name"].dropna().astype(str).unique().tolist()
    class_order = resource_classes_df["class_name"].dropna().astype(str).tolist()
    class_options = [c for c in class_order if c in available_classes]
    if not class_options:
        st.info("No resource classes available for this view.")
        return

    active_planning_class_key = f"planning_class_active_{active_region}"
    active_planning_start_key = "planning_start_active"
    active_planning_weeks_key = "planning_weeks_active"

    if include_excluded:
        selected_class = st.session_state.get(active_planning_class_key)
        if selected_class not in class_options:
            selected_class = class_options[0]

        board_start = st.session_state.get(active_planning_start_key, date.today())
        num_weeks = st.session_state.get(active_planning_weeks_key, 12)

        c1, c2, c3 = st.columns([1.6, 1, 1])
        c1.markdown(f"**Resource View:** {selected_class}")
        c2.markdown(f"**Board Start:** {pd.to_datetime(board_start).strftime('%m/%d/%Y')}")
        c3.markdown(f"**Weeks:** {num_weeks}")
    else:
        planning_key_suffix = "active"
        planning_class_key = active_planning_class_key
        if planning_class_key not in st.session_state or st.session_state[planning_class_key] not in class_options:
            st.session_state[planning_class_key] = class_options[0]

        c1, c2, c3 = st.columns([1.6, 1, 1])
        selected_class = c1.selectbox("Resource View", class_options, key=planning_class_key)
        board_start = c2.date_input("Board Start", value=date.today(), key=active_planning_start_key)
        num_weeks = c3.selectbox("Weeks", [1, 2, 4, 8, 10, 12, 16, 20, 24], index=5, key=active_planning_weeks_key)

    board_df, summary_df, gridlines, tickvals, ticktext, x_end, selected_class = build_planning_board_data(
        active_region=active_region,
        selected_class=selected_class,
        start_date=board_start,
        num_weeks=num_weeks,
        include_excluded=include_excluded,
    )

    if board_df.empty:
        st.info("No rows for that resource view.")
        return

    if include_excluded:
        st.caption("Bid / Awarded jobs shown below are excluded from allocation and need calculations in the main board.")

    summary_metrics = ["Need", "Unallocated Pool", "Availability"] if include_excluded else ["Need", "In Region", "Availability"]
    total_job_rows = len(board_df)
    total_rows = total_job_rows + 1 + len(summary_metrics)

    fig = go.Figure()
    x0 = pd.to_datetime(board_start).normalize()

    for ws in gridlines:
        fig.add_vline(x=ws, line_width=1, line_color="rgba(120,120,120,0.40)")
    for y in [i + 0.5 for i in range(total_rows + 1)]:
        width = 2 if abs(y - (len(summary_metrics) + 0.5)) < 1e-9 else 1
        color = "rgba(90,90,90,0.55)" if width == 2 else "rgba(160,160,160,0.28)"
        fig.add_hline(y=y, line_width=width, line_color=color)

    row_positions = []
    row_labels = []

    for i, (_, row) in enumerate(board_df.iterrows()):
        y = total_rows - i
        row_positions.append(y)
        row_labels.append(str(row["customer"]))

        fill = str(row.get("customer_color", "") or "") or customer_base_color(str(row["customer"]))

        start = pd.to_datetime(row["required_start"])
        finish = pd.to_datetime(row["required_end"]) + pd.Timedelta(days=1)

        fig.add_shape(
            type="rect",
            x0=start,
            x1=finish,
            y0=y - 0.34,
            y1=y + 0.34,
            line=dict(color=hex_to_rgba(fill, 0.70), width=1),
            fillcolor=hex_to_rgba(fill, 0.28),
            layer="below",
        )
        fig.add_annotation(
            x=start + (finish - start) / 2,
            y=y,
            text=str(row["label"]),
            showarrow=False,
            font=dict(size=11, color="black"),
            xanchor="center",
            yanchor="middle",
        )

    if include_excluded:
        summary_y = {"Need": 3, "Unallocated Pool": 2, "Availability": 1}
    else:
        summary_y = {"Need": 3, "In Region": 2, "Availability": 1}
    for metric, y in summary_y.items():
        row_positions.append(y)
        row_labels.append(metric)

    if not summary_df.empty:
        for _, rec in summary_df.iterrows():
            y = summary_y[rec["Metric"]]
            x = pd.to_datetime(rec["X"])
            val = float(rec["Value"])
            txt = format_compact_number(val)

            if rec["Metric"] == "Availability":
                color = availability_font_color(val)
                font = dict(size=14, color=color, family="Arial Black")
            elif rec["Metric"] in ["Need", "Shortfall"]:
                color = "#9b1c1c"
                font = dict(size=12, color=color)
            else:
                color = "#1b4f9b"
                font = dict(size=12, color=color)

            fig.add_annotation(
                x=x,
                y=y,
                text=txt,
                showarrow=False,
                font=font,
                xanchor="center",
                yanchor="middle",
            )

    fig.add_trace(go.Scatter(
        x=[x0, x_end],
        y=[0, total_rows + 1],
        mode="markers",
        marker_opacity=0,
        hoverinfo="skip",
        showlegend=False,
    ))

    fig.update_xaxes(
        tickmode="array",
        tickvals=tickvals,
        ticktext=ticktext,
        side="top",
        showgrid=False,
        range=[x0, x_end],
        tickfont=dict(size=10),
        tickangle=-30,
        fixedrange=True,
    )
    fig.update_yaxes(
        tickmode="array",
        tickvals=row_positions,
        ticktext=row_labels,
        range=[0.5, total_rows + 0.5],
        showgrid=False,
        zeroline=False,
        tickfont=dict(size=12),
        fixedrange=True,
    )
    fig.update_layout(
        height=max(520, 120 + total_rows * 50),
        margin=dict(l=30, r=20, t=30, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    st.plotly_chart(
        fig,
        width="stretch",
        config={
            "displayModeBar": True,
            "displaylogo": False,
            "modeBarButtons": [["toImage"]],
            "toImageButtonOptions": {
                "format": "png",
                "filename": "planning_board_pipeline" if include_excluded else "planning_board",
                "height": 1200,
                "width": 2200,
                "scale": 2,
            },
        },
    )

with st.sidebar:
    st.header("Workspace")

    region_options = ["Select a region...", "Global"] + regions_df["region_code"].tolist()

    ACTIVE_REGION = st.selectbox(
        "Active Region",
        region_options,
        format_func=lambda x: (
            "Select a region..." if x == "Select a region..."
            else "Global" if x == "Global"
            else region_format(x)
        ),
        key="active_region_selector",
    )

    if ACTIVE_REGION == "Select a region...":
        st.warning("Select an Active Region to continue.")
        st.stop()

    st.caption("Showing all regions" if ACTIVE_REGION == "Global" else region_format(ACTIVE_REGION))

    if st.button("Rebalance All Allocations", type="primary"):
        recalc_all_requirements(engine)
        st.success("Rebalanced")
        st.rerun()

if "last_active_region" not in st.session_state:
    st.session_state["last_active_region"] = ACTIVE_REGION
if st.session_state["last_active_region"] != ACTIVE_REGION:
    st.session_state["last_active_region"] = ACTIVE_REGION

tab_jobs, tab_job_requirements, tab_planning, tab_pools, tab_requirements, tab_allocations = st.tabs(
    ["Jobs", "Job Requirements", "Planning Board", "Pools", "Requirements", "Allocations"]
)

with tab_jobs:
    st.subheader("Create Job")

    region_list = regions_df["region_code"].tolist()
    default_region_index = region_default_index(region_list, ACTIVE_REGION)

    if "create_job_reset_counter" not in st.session_state:
        st.session_state["create_job_reset_counter"] = 0

    reset_counter = st.session_state["create_job_reset_counter"]
    region_key_suffix = f"{ACTIVE_REGION}_{reset_counter}"

    default_region_value = ACTIVE_REGION if ACTIVE_REGION != "Global" else region_list[default_region_index]

    c1, c2, c3 = st.columns(3)
    with c1:
        customer = st.text_input("Customer", key=f"create_job_customer_{region_key_suffix}")
        customer_color = st.color_picker("Job Color", value="#1f77b4", key=f"create_job_customer_color_{region_key_suffix}")
        region_code = st.selectbox(
            "Region",
            region_list,
            index=region_list.index(default_region_value) if default_region_value in region_list else default_region_index,
            format_func=region_format,
            disabled=region_disabled(ACTIVE_REGION),
            key=f"create_job_region_{region_key_suffix}",
        )
        location = st.text_input("Location", key=f"create_job_location_{region_key_suffix}")
    with c2:
        job_name = st.text_input("Job Name", key=f"create_job_name_{region_key_suffix}")
        job_start_date = st.date_input("Job Start Date", value=date.today(), key=f"create_job_start_date_{region_key_suffix}")
        st.caption(f"Selected: {job_start_date.strftime('%m/%d/%Y')}")
        job_duration_days = st.number_input("Job Duration (days)", min_value=1, value=7, step=1, key=f"create_job_duration_{region_key_suffix}")
    with c3:
        mob_days_before_job = st.number_input("Mobilization Days Before Job", min_value=0, value=3, step=1, key=f"create_job_mob_{region_key_suffix}")
        demob_days_after_job = st.number_input("Demobilization Days After Job", min_value=0, value=2, step=1, key=f"create_job_demob_{region_key_suffix}")
        status = st.selectbox("Status", ["Bid", "Awarded", "Planned", "Tentative", "Active", "Billing Pending", "Complete", "Cancelled"], key=f"create_job_status_{region_key_suffix}")
    notes = st.text_area("Notes", key=f"create_job_notes_{region_key_suffix}")

    dates_preview = calc_job_dates(
        job_start_date,
        int(job_duration_days),
        int(mob_days_before_job),
        int(demob_days_after_job),
    )
    st.info(
        f"Job End: {format_date_value(dates_preview['job_end_date'])}  |  "
        f"Mob Start: {format_date_value(dates_preview['mob_start_date'])}  |  "
        f"Demob End: {format_date_value(dates_preview['demob_end_date'])}"
    )

    if st.button("Create Job", key=f"create_job_submit_{region_key_suffix}"):
        if not job_name:
            st.error("Job Name is required.")
        else:
            create_job(engine, {
                "job_name": job_name,
                "region_code": ACTIVE_REGION if ACTIVE_REGION != "Global" else region_code,
                "customer": customer,
                "customer_color": customer_color,
                "location": location,
                "job_start_date": job_start_date,
                "job_duration_days": int(job_duration_days),
                "mob_days_before_job": int(mob_days_before_job),
                "demob_days_after_job": int(demob_days_after_job),
                "status": status,
                "notes": notes,
            })
            st.success("Created job.")
            st.session_state["create_job_reset_counter"] += 1
            st.rerun()

    jobs_df = filter_active_jobs_for_management(region_filter(get_jobs_df(engine), ACTIVE_REGION))
    render_jobs_manage_table(jobs_df, ACTIVE_REGION)




with tab_job_requirements:
    st.subheader("Job Requirements")
    jobs_df = region_filter(get_jobs_df(engine), ACTIVE_REGION)
    if jobs_df.empty:
        st.warning("Create a job first.")
    else:
        jobs_for_display = jobs_df.copy()
        jobs_for_display["customer"] = jobs_for_display["customer"].fillna("").replace("", "Unassigned")
        job_options = jobs_for_display.assign(
            display=jobs_for_display["customer"] + " | " + jobs_for_display["job_name"] + " | " + jobs_for_display["job_code"]
        ).sort_values(["customer", "job_name", "job_code"])

        job_req_select_key = f"job_req_selected_job_id_{ACTIVE_REGION}"
        job_option_ids = job_options["id"].tolist()
        if not job_option_ids:
            st.warning("Create a job first.")
            st.stop()
        if job_req_select_key not in st.session_state or st.session_state[job_req_select_key] not in job_option_ids:
            st.session_state[job_req_select_key] = int(job_option_ids[0])

        selected_job_id = st.selectbox(
            "Select Job",
            job_option_ids,
            format_func=lambda job_id: job_options.loc[job_options["id"] == job_id, "display"].iloc[0],
            key=job_req_select_key,
        )
        selected_job = job_options.loc[job_options["id"] == selected_job_id].iloc[0]

        st.markdown(
            f'''
            <div class="sticky-job-summary">
                <div class="title">Selected Job Summary</div>
                <div class="line"><strong>{selected_job['customer']}</strong> | <strong>{selected_job['job_name']}</strong> | <strong>{selected_job['job_code']}</strong> &nbsp;&nbsp;•&nbsp;&nbsp; Region: <strong>{region_format(str(selected_job['region_code']))}</strong></div>
                <div class="line">Mob Days Before: <strong>{int(selected_job['mob_days_before_job'])}</strong> &nbsp;&nbsp;|&nbsp;&nbsp; Demob Days After: <strong>{int(selected_job['demob_days_after_job'])}</strong> &nbsp;&nbsp;|&nbsp;&nbsp; Job Start: <strong>{format_date_value(selected_job['job_start_date'])}</strong> &nbsp;&nbsp;|&nbsp;&nbsp; Job End: <strong>{format_date_value(selected_job['job_end_date'])}</strong></div>
            </div>
            
            ''',
            unsafe_allow_html=True,
        )
        if str(selected_job.get("status", "")) in EXCLUDED_CALC_STATUSES:
            st.info("This Bid / Awarded job is excluded from needs and allocation calculations. It will appear only in the Bid / Awarded sections and pipeline planning board.")

        rc_df = resource_options_df(include_rental=False).copy()

        if "job_req_editor_reset_counter" not in st.session_state:
            st.session_state["job_req_editor_reset_counter"] = 0
        editor_key_suffix = f"{ACTIVE_REGION}_{int(selected_job['id'])}_{st.session_state['job_req_editor_reset_counter']}"

        editor_df = rc_df[["display"]].rename(columns={"display": "Class"})
        editor_df["Quantity"] = "0"
        editor_df["Days Before"] = "0"
        editor_df["Days After"] = "0"
        editor_df["Priority"] = "Normal"
        editor_df["Notes"] = ""

        st.markdown("##### Add Requirements for Selected Job")
        edited = st.data_editor(
            editor_df,
            num_rows="dynamic",
            hide_index=True,
            key=f"job_req_editor_{editor_key_suffix}",
            column_config={
                "Class": st.column_config.SelectboxColumn("Class", options=rc_df["display"].tolist(), required=True, width="medium"),
                "Quantity": st.column_config.TextColumn("Quantity", width="small"),
                "Days Before": st.column_config.TextColumn("Days Before", width="small"),
                "Days After": st.column_config.TextColumn("Days After", width="small"),
                "Priority": st.column_config.SelectboxColumn("Priority", options=["Low", "Normal", "High", "Critical"], required=True, width="small"),
                "Notes": st.column_config.TextColumn("Notes", width="large"),
            },
            width="stretch",
        )

        if st.button("Save All Requirements", key=f"job_req_submit_{editor_key_suffix}"):
            rows_saved = 0
            for _, r in edited.iterrows():
                try:
                    qty = float(str(r["Quantity"]).strip() or "0")
                except Exception:
                    qty = 0.0
                try:
                    days_before = int(float(str(r["Days Before"]).strip() or "0"))
                except Exception:
                    days_before = 0
                try:
                    days_after = int(float(str(r["Days After"]).strip() or "0"))
                except Exception:
                    days_after = 0
                if qty <= 0:
                    continue
                rc_match = rc_df.loc[rc_df["display"] == r["Class"]]
                if rc_match.empty:
                    continue
                create_requirement(engine, {
                    "job_id": int(selected_job["id"]),
                    "resource_class_id": int(rc_match.iloc[0]["id"]),
                    "quantity_required": qty,
                    "days_before_job_start": days_before,
                    "days_after_job_end": days_after,
                    "priority": r["Priority"],
                    "notes": str(r["Notes"]) if pd.notna(r["Notes"]) else "",
                })
                rows_saved += 1
            if rows_saved > 0:
                st.session_state["job_req_editor_reset_counter"] += 1
                st.success(f"Saved {rows_saved} requirement(s).")
                st.rerun()
            else:
                st.info("No rows with quantity greater than 0 were saved.")

        if "rental_req_editor_reset_counter" not in st.session_state:
            st.session_state["rental_req_editor_reset_counter"] = 0
        rental_key_suffix = f"{ACTIVE_REGION}_{int(selected_job['id'])}_{st.session_state['rental_req_editor_reset_counter']}"
        rental_editor_df = rc_df[["display"]].rename(columns={"display": "Class"})
        rental_editor_df["Quantity"] = "0"
        rental_editor_df["Days Before"] = "0"
        rental_editor_df["Days After"] = "0"
        rental_editor_df["Vendor"] = ""
        rental_editor_df["Notes"] = ""

        st.markdown("##### Add Rental Requirements for Selected Job")
        rental_edited = st.data_editor(
            rental_editor_df,
            num_rows="dynamic",
            hide_index=True,
            key=f"rental_req_editor_{rental_key_suffix}",
            column_config={
                "Class": st.column_config.SelectboxColumn("Class", options=rc_df["display"].tolist(), required=True, width="medium"),
                "Quantity": st.column_config.TextColumn("Quantity", width="small"),
                "Days Before": st.column_config.TextColumn("Days Before", width="small"),
                "Days After": st.column_config.TextColumn("Days After", width="small"),
                "Vendor": st.column_config.TextColumn("Vendor", width="medium"),
                "Notes": st.column_config.TextColumn("Notes", width="large"),
            },
            width="stretch",
        )

        if st.button("Save Rental Requirements", key=f"rental_req_submit_{rental_key_suffix}"):
            rows_saved = 0
            for _, r in rental_edited.iterrows():
                try:
                    qty = float(str(r["Quantity"]).strip() or "0")
                except Exception:
                    qty = 0.0
                try:
                    days_before = int(float(str(r["Days Before"]).strip() or "0"))
                except Exception:
                    days_before = 0
                try:
                    days_after = int(float(str(r["Days After"]).strip() or "0"))
                except Exception:
                    days_after = 0
                vendor = str(r["Vendor"]).strip()
                if qty <= 0 or not vendor:
                    continue
                rc_match = rc_df.loc[rc_df["display"] == r["Class"]]
                if rc_match.empty:
                    continue
                create_rental_requirement(engine, {
                    "job_id": int(selected_job["id"]),
                    "resource_class_id": int(rc_match.iloc[0]["id"]),
                    "quantity_required": qty,
                    "days_before_job_start": days_before,
                    "days_after_job_end": days_after,
                    "vendor_name": vendor,
                    "notes": str(r["Notes"]) if pd.notna(r["Notes"]) else "",
                })
                rows_saved += 1
            if rows_saved > 0:
                st.session_state["rental_req_editor_reset_counter"] += 1
                st.success(f"Saved {rows_saved} rental requirement(s).")
                st.rerun()
            else:
                st.info("No valid rental rows were saved.")

        st.subheader("Selected Job Requirement Summary")
        req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
        selected_job_reqs = req_summary.loc[req_summary["job_code"] == selected_job["job_code"]].copy() if not req_summary.empty else pd.DataFrame()
        if selected_job_reqs.empty:
            st.info("No owned requirements yet for this job.")
        else:
            display_req = selected_job_reqs[["class_name", "quantity_required", "required_start", "required_end"]].copy()
            display_req["quantity_required"] = display_req["quantity_required"].map(format_compact_number)
            display_req = format_dates_for_display(display_req)
            display_req.columns = ["Class Name", "Quantity Required", "Required Start", "Required End"]
            render_simple_html_table(display_req, qty_columns=["Quantity Required"])
            rental_manage = region_filter(get_rental_requirements_df(engine), ACTIVE_REGION)
            rental_manage = rental_manage[["job_id", "resource_class_id", "quantity_required", "vendor_name"]].copy() if not rental_manage.empty else pd.DataFrame(columns=["job_id", "resource_class_id", "quantity_required", "vendor_name"])
            if not rental_manage.empty:
                rental_manage = rental_manage.groupby(["job_id", "resource_class_id"], as_index=False).agg(
                    assigned_rental=("quantity_required", "sum"),
                    rental_vendor=("vendor_name", lambda s: ", ".join(sorted({str(v).strip() for v in s if str(v).strip()}))),
                )
            manual_manage = region_filter(get_manual_owned_allocations_df(engine), ACTIVE_REGION)
            manual_manage = manual_manage[["job_id", "resource_class_id", "quantity_assigned"]].copy() if not manual_manage.empty else pd.DataFrame(columns=["job_id", "resource_class_id", "quantity_assigned"])
            if not manual_manage.empty:
                manual_manage = manual_manage.groupby(["job_id", "resource_class_id"], as_index=False)["quantity_assigned"].sum().rename(columns={"quantity_assigned": "manual_assigned_ees"})
            manage_df = selected_job_reqs.copy()
            manage_df = manage_df.merge(rental_manage, on=["job_id", "resource_class_id"], how="left")
            manage_df = manage_df.merge(manual_manage, on=["job_id", "resource_class_id"], how="left")
            manage_df["assigned_ees"] = manage_df["manual_assigned_ees"].fillna(manage_df["quantity_assigned"]).fillna(0.0)
            manage_df["assigned_rental"] = manage_df["assigned_rental"].fillna(0.0)
            manage_df["rental_vendor"] = manage_df["rental_vendor"].fillna("")
            render_requirements_manage_table(manage_df, key_prefix="jobreq")

        st.markdown("##### Selected Job Rental Requirements")
        rental_summary = region_filter(get_rental_requirements_df(engine), ACTIVE_REGION)
        selected_job_rentals = rental_summary.loc[rental_summary["job_code"] == selected_job["job_code"]].copy() if not rental_summary.empty else pd.DataFrame()
        if selected_job_rentals.empty:
            st.info("No rental requirements yet for this job.")
        else:
            display_rent = selected_job_rentals[["class_name", "quantity_required", "vendor_name", "required_start", "required_end"]].copy()
            display_rent["quantity_required"] = display_rent["quantity_required"].map(format_compact_number)
            display_rent = format_dates_for_display(display_rent)
            display_rent.columns = ["Class Name", "Rental Qty", "Vendor", "Required Start", "Required End"]
            render_simple_html_table(display_rent, qty_columns=["Rental Qty"])
            for _, r in selected_job_rentals.iterrows():
                c1, c2, c3, c4, c5 = st.columns([1.4, 0.9, 1.2, 1.0, 0.8])
                c1.write(str(r["class_name"]))
                c2.write(format_compact_number(r["quantity_required"]))
                c3.write(str(r["vendor_name"]))
                c4.write(f"{format_date_value(r['required_start'])} - {format_date_value(r['required_end'])}")
                if c5.button("Delete", key=f"delete_rental_{int(r['id'])}"):
                    delete_rental_requirement(engine, int(r["id"]))
                    st.rerun()

with tab_requirements:
    st.subheader("Add Requirement")
    jobs_df = region_filter(get_jobs_df(engine), ACTIVE_REGION)
    if jobs_df.empty:
        st.warning("Create a job first.")
    else:
        job_options = jobs_df.assign(display=jobs_df["job_code"] + " | " + jobs_df["job_name"])
        rc_display = resource_options_df()

        if "create_req_reset_counter" not in st.session_state:
            st.session_state["create_req_reset_counter"] = 0
        req_reset_counter = st.session_state["create_req_reset_counter"]
        req_key_suffix = f"{ACTIVE_REGION}_{req_reset_counter}"

        c1, c2, c3 = st.columns(3)

        selected_job_display = c1.selectbox(
            "Job",
            job_options["display"].tolist(),
            key=f"create_req_job_{req_key_suffix}",
        )
        selected_job = job_options.loc[job_options["display"] == selected_job_display].iloc[0]

        selected_rc_display = c2.selectbox(
            "Resource Class",
            rc_display["display"].tolist(),
            key=f"create_req_rc_{req_key_suffix}",
        )
        selected_rc = rc_display.loc[rc_display["display"] == selected_rc_display].iloc[0]

        step = quantity_step(str(selected_rc["unit_type"]), str(selected_rc["category"]))
        fmt = quantity_format(str(selected_rc["unit_type"]), str(selected_rc["category"]))

        quantity_required = c3.number_input(
            f"Quantity Required ({selected_rc['unit_type']})",
            min_value=0.0,
            value=step,
            step=step,
            format=fmt,
            key=f"create_req_qty_{req_key_suffix}",
        )

        c4, c5, c6 = st.columns(3)
        days_before_job_start = c4.number_input(
            "Days Before Job Start",
            min_value=0,
            value=0,
            step=1,
            key=f"create_req_before_{req_key_suffix}",
        )
        days_after_job_end = c5.number_input(
            "Days After Job End",
            min_value=0,
            value=0,
            step=1,
            key=f"create_req_after_{req_key_suffix}",
        )
        priority = c6.selectbox(
            "Priority",
            ["Low", "Normal", "High", "Critical"],
            index=1,
            key=f"create_req_priority_{req_key_suffix}",
        )

        req_notes = st.text_area("Notes", key=f"create_req_notes_{req_key_suffix}")

        req_start = pd.to_datetime(selected_job["job_start_date"]).date() - pd.Timedelta(days=int(days_before_job_start))
        req_end = pd.to_datetime(selected_job["job_end_date"]).date() + pd.Timedelta(days=int(days_after_job_end))
        st.info(f"Requirement Window: {format_date_value(req_start)} to {format_date_value(req_end)}")

        if st.button("Add Requirement", key=f"create_req_submit_{req_key_suffix}"):
            create_requirement(
                engine,
                {
                    "job_id": int(selected_job["id"]),
                    "resource_class_id": int(selected_rc["id"]),
                    "quantity_required": float(quantity_required),
                    "days_before_job_start": int(days_before_job_start),
                    "days_after_job_end": int(days_after_job_end),
                    "priority": priority,
                    "notes": req_notes,
                },
            )
            st.success("Requirement added.")
            st.session_state["create_req_reset_counter"] += 1
            st.rerun()

    st.subheader("Requirement Summary")
    req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        display_req = format_dates_for_display(req_summary[["job_code","job_name","region_code","class_name","quantity_required","unit_type","required_start","required_end","quantity_assigned","quantity_shortfall","allocation_status"]])
        display_req["region_code"] = display_req["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_req, width="stretch")
        rental_manage = region_filter(get_rental_requirements_df(engine), ACTIVE_REGION)
        rental_manage = rental_manage[["job_id", "resource_class_id", "quantity_required", "vendor_name"]].copy() if not rental_manage.empty else pd.DataFrame(columns=["job_id", "resource_class_id", "quantity_required", "vendor_name"])
        if not rental_manage.empty:
            rental_manage = rental_manage.groupby(["job_id", "resource_class_id"], as_index=False).agg(
                assigned_rental=("quantity_required", "sum"),
                rental_vendor=("vendor_name", lambda s: ", ".join(sorted({str(v).strip() for v in s if str(v).strip()}))),
            )
            
        manual_manage = region_filter(get_manual_owned_allocations_df(engine), ACTIVE_REGION)
        manual_manage = manual_manage[["job_id", "resource_class_id", "quantity_assigned"]].copy() if not manual_manage.empty else pd.DataFrame(columns=["job_id", "resource_class_id", "quantity_assigned"])
        if not manual_manage.empty:
            manual_manage = manual_manage.groupby(["job_id", "resource_class_id"], as_index=False)["quantity_assigned"].sum().rename(columns={"quantity_assigned": "manual_assigned_ees"})
        manage_df = req_summary.copy()
        manage_df = manage_df.merge(rental_manage, on=["job_id", "resource_class_id"], how="left")
        manage_df = manage_df.merge(manual_manage, on=["job_id", "resource_class_id"], how="left")
        manage_df["assigned_ees"] = manage_df["manual_assigned_ees"].fillna(manage_df["quantity_assigned"]).fillna(0.0)
        manage_df["assigned_rental"] = manage_df["assigned_rental"].fillna(0.0)
        manage_df["rental_vendor"] = manage_df["rental_vendor"].fillna("")
        render_requirements_manage_table(manage_df)

with tab_planning:
    render_planning_board(ACTIVE_REGION, include_excluded=False, section_title="Planning Board")
    st.divider()
    render_pipeline_notice("Bid / Awarded Planning Board")
    render_planning_board(ACTIVE_REGION, include_excluded=True, section_title="Bid / Awarded Planning Board")



with tab_pools:
    st.subheader("Resource Pools")
    rc_display = resource_options_df()
    region_codes = regions_df["region_code"].tolist()
    default_region_index = region_default_index(region_codes, ACTIVE_REGION)

    if "create_pool_reset_counter" not in st.session_state:
        st.session_state["create_pool_reset_counter"] = 0
    pool_reset_counter = st.session_state["create_pool_reset_counter"]
    pool_key_suffix = f"{ACTIVE_REGION}_{pool_reset_counter}"

    c1, c2, c3 = st.columns(3)
    region_code = c1.selectbox(
        "Region",
        region_codes,
        index=default_region_index,
        format_func=region_format,
        disabled=region_disabled(ACTIVE_REGION),
        key=f"pool_region_{pool_key_suffix}",
    )
    selected_rc_display = c2.selectbox(
        "Resource Class",
        rc_display["display"].tolist(),
        key=f"pool_rc_{pool_key_suffix}",
    )
    selected_rc = rc_display.loc[rc_display["display"] == selected_rc_display].iloc[0]

    step = quantity_step(str(selected_rc["unit_type"]), str(selected_rc["category"]))
    fmt = quantity_format(str(selected_rc["unit_type"]), str(selected_rc["category"]))

    base_quantity = c3.number_input(
        f"Base Quantity ({selected_rc['unit_type']})",
        min_value=0.0,
        value=0.0,
        step=step,
        format=fmt,
        key=f"pool_base_quantity_{pool_key_suffix}",
    )

    notes = st.text_input("Notes", key=f"pool_notes_{pool_key_suffix}")

    if st.button("Save Pool Quantity", key=f"pool_submit_{pool_key_suffix}"):
        upsert_pool(
            engine,
            ACTIVE_REGION if ACTIVE_REGION != "Global" else region_code,
            int(selected_rc["id"]),
            float(base_quantity),
            notes,
        )
        st.success("Pool saved.")
        st.session_state["create_pool_reset_counter"] += 1
        st.rerun()

    st.subheader("Pool Adjustment Log")
    with st.form("pool_adjustment_form", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns(4)
        adj_region = c1.selectbox("Region", region_codes, index=default_region_index, format_func=region_format, disabled=region_disabled(ACTIVE_REGION), key=f"adj_region_{ACTIVE_REGION}")
        adj_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist(), key=f"adj_rc_{ACTIVE_REGION}")
        adj_rc = rc_display.loc[rc_display["display"] == adj_rc_display].iloc[0]
        step = quantity_step(str(adj_rc["unit_type"]), str(adj_rc["category"]))
        fmt = quantity_format(str(adj_rc["unit_type"]), str(adj_rc["category"]))
        qty_change = c3.number_input(f"Quantity Change ({adj_rc['unit_type']})", value=0.0, step=step, format=fmt)
        adjustment_date = c4.date_input("Adjustment Date", value=date.today())
        st.caption(f"Selected: {adjustment_date.strftime('%m/%d/%Y')}")
        c5, c6 = st.columns(2)
        reason = c5.selectbox("Reason", ["Purchase","Transfer In","Transfer Out","Retirement","Damage/Loss","Correction"])
        adj_notes = c6.text_input("Notes", key=f"adj_notes_{ACTIVE_REGION}")
        if st.form_submit_button("Add Adjustment"):
            add_pool_adjustment(engine, {
                "region_code": active_region_value(ACTIVE_REGION, adj_region),
                "resource_class_id": int(adj_rc["id"]),
                "quantity_change": float(qty_change),
                "adjustment_date": adjustment_date,
                "reason": reason,
                "notes": adj_notes,
            })
            st.success("Adjustment added.")
            st.rerun()

    as_of_date = st.date_input("Pool Snapshot As Of", value=date.today(), key="snapshot_date")
    snapshot = region_filter(pool_snapshot_df(engine, as_of_date=as_of_date), ACTIVE_REGION)
    if snapshot.empty:
        st.info("No pools set up yet.")
    else:
        display_snapshot = format_dates_for_display(snapshot.copy())
        display_snapshot["region_code"] = display_snapshot["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_snapshot, width="stretch")
        snapshot_display = format_dates_for_display(snapshot)
        csv_data = snapshot_display.to_csv(index=False).encode("utf-8")
        excel_data = export_excel({"Pool Snapshot": snapshot_display})
        c1, c2 = st.columns(2)
        c1.download_button("Download Pool Snapshot CSV", data=csv_data, file_name=f"pool_snapshot_{as_of_date}.csv", mime="text/csv")
        c2.download_button("Download Pool Snapshot Excel", data=excel_data, file_name=f"pool_snapshot_{as_of_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        render_pools_manage_table(snapshot, ACTIVE_REGION)

with tab_allocations:
    st.subheader("Allocations")
    req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
    ful = region_filter(get_fulfillment_df(engine), ACTIVE_REGION)
    active_req_summary = filter_by_job_status(req_summary, include_excluded=False)
    active_ful = filter_by_job_status(ful, include_excluded=False)
    if active_req_summary.empty:
        st.info("No active requirements yet.")
    else:
        display_req = format_dates_for_display(active_req_summary[["job_code","job_name","region_code","class_name","quantity_required","quantity_assigned","quantity_shortfall","allocation_status"]])
        display_req["region_code"] = display_req["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_req, width="stretch")
    st.subheader("Fulfillment Rows")
    if active_ful.empty:
        st.info("No active fulfillment rows yet.")
    else:
        display_ful = format_dates_for_display(active_ful[["job_code","job_name","region_code","class_name","fulfillment_type","source_name","specific_resource_name","quantity_assigned","required_start","required_end"]])
        display_ful["region_code"] = display_ful["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_ful, width="stretch")
    st.subheader("Allocation Debug")
    dbg = region_filter(allocation_debug_df(engine), ACTIVE_REGION)
    if dbg.empty:
        st.info("No allocation debug data yet.")
    else:
        display_dbg = format_dates_for_display(dbg.copy())
        display_dbg["region_code"] = display_dbg["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_dbg, width="stretch")

    st.divider()
    render_pipeline_notice("Bid / Awarded Jobs")
    pipeline_req_summary = filter_by_job_status(req_summary, include_excluded=True)
    pipeline_ful = filter_by_job_status(ful, include_excluded=True)
    if pipeline_req_summary.empty:
        st.info("No Bid / Awarded requirements yet.")
    else:
        st.markdown("##### Bid / Awarded Requirement Rows")
        display_pipeline_req = format_dates_for_display(pipeline_req_summary[["job_code","job_name","status","region_code","class_name","quantity_required","quantity_assigned","quantity_shortfall","allocation_status"]])
        display_pipeline_req["region_code"] = display_pipeline_req["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_pipeline_req, width="stretch")
    st.markdown("##### Bid / Awarded Fulfillment Rows")
    if pipeline_ful.empty:
        st.info("No fulfillment rows should appear here once recalculation is run, because Bid / Awarded jobs are excluded from allocation calculations.")
    else:
        display_pipeline_ful = format_dates_for_display(pipeline_ful[["job_code","job_name","status","region_code","class_name","fulfillment_type","source_name","specific_resource_name","quantity_assigned","required_start","required_end"]])
        display_pipeline_ful["region_code"] = display_pipeline_ful["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_pipeline_ful, width="stretch")


