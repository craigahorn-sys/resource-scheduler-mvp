
from __future__ import annotations
from datetime import date
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import colorsys
import streamlit as st

from services.db import execute, export_excel, get_engine, init_db, query_df
from services.models import calc_job_dates
from services.bidding import migrate_bidding
from services.revenue_scheduler import (
    migrate_revenue_columns, update_job_billing,
    get_line_items_df, save_line_items, delete_line_item,
    get_revenue_jobs_df, build_revenue_excel, build_ticket_excel,
)
from services.scheduler import (
    add_pool_adjustment, allocation_debug_df, create_job, create_requirement, delete_job,
    delete_pool, delete_pool_adjustment, delete_requirement, get_fulfillment_df,
    get_jobs_df, get_pools_df, pool_snapshot_df, recalc_all_requirements,
    requirement_summary_df, update_job, update_requirement, upsert_pool,
    create_rental_requirement, delete_rental_requirement, get_rental_requirements_df,
    delete_manual_owned_allocation, get_manual_owned_allocations_df,
    upsert_manual_owned_allocation_for_job_class, upsert_rental_requirement_for_job_class,
    add_requirement_version, migrate_effective_dates,
    migrate_snapshots, take_daily_snapshot, get_utilization_history,
    get_job_snapshot_history, get_actuals_requirements, get_actuals_billing,
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
migrate_revenue_columns(engine)
migrate_bidding(engine)
migrate_effective_dates(engine)
migrate_snapshots(engine)
take_daily_snapshot(engine)

try:
    query_df(engine, "SELECT customer_color FROM jobs LIMIT 1")
except Exception:
    try:
        execute(engine, "ALTER TABLE jobs ADD COLUMN customer_color TEXT")
    except Exception:
        pass

try:
    query_df(engine, "SELECT requirement_id FROM job_manual_owned_allocations LIMIT 1")
except Exception:
    try:
        execute(engine, "ALTER TABLE job_manual_owned_allocations ADD COLUMN requirement_id BIGINT")
    except Exception:
        pass

try:
    query_df(engine, "SELECT requirement_id FROM job_rental_requirements LIMIT 1")
except Exception:
    try:
        execute(engine, "ALTER TABLE job_rental_requirements ADD COLUMN requirement_id BIGINT REFERENCES job_requirements(id) ON DELETE CASCADE")
    except Exception:
        pass


if "create_job_start_date" not in st.session_state:
    st.session_state["create_job_start_date"] = date.today()

# 25 curated highlighter-style colors
JOB_COLOR_PALETTE = [
    "#EED202", "#FFFF00", "#BFFF00", "#00FF00", "#AAF0D1",
    "#17E9E9", "#008FFE", "#BF00FF", "#FF1DCE", "#FD5B78",
    "#DA1D81", "#FF003F", "#FFBF00", "#FF8214", "#808000",
    "#556B2F", "#967117", "#738678", "#888064", "#d2d2cd",
]

def color_swatch_picker(label: str, key: str, default: str = "#FF4D4D") -> str:
    """Renders a 5×5 grid of color swatches. Returns the selected hex color."""
    current = st.session_state.get(f"_swatch_{key}", None)
    if current not in JOB_COLOR_PALETTE:
        current = default if default in JOB_COLOR_PALETTE else JOB_COLOR_PALETTE[0]
        st.session_state[f"_swatch_{key}"] = current

    st.markdown(f"<div style='font-size:0.85rem;font-weight:600;margin-bottom:2px;'>{label}</div>", unsafe_allow_html=True)

    # Inject CSS once to style the swatch buttons
    st.markdown("""
    <style>
    button[data-swatch="true"] { padding: 0 !important; min-height: 0 !important; }
    </style>
    """, unsafe_allow_html=True)

    cols_per_row = 5
    rows = [JOB_COLOR_PALETTE[i:i+cols_per_row] for i in range(0, len(JOB_COLOR_PALETTE), cols_per_row)]
    for row_colors in rows:
        cols = st.columns(cols_per_row)
        for col, hex_color in zip(cols, row_colors):
            is_selected = (hex_color == current)
            outline = "3px solid #111" if is_selected else "2px solid #bbb"
            light_colors = {"#FFFFFF", "#FFF176", "#FFE033", "#BFFF00", "#C5E1A5", "#FFCC80", "#FFB300"}
            check = "✓" if is_selected else ""
            text_col = "#000" if hex_color in light_colors else "#fff"
            col.markdown(
                f"<div style='background:{hex_color};outline:{outline};border-radius:5px;"
                f"height:26px;display:flex;align-items:center;justify-content:center;"
                f"font-size:13px;font-weight:700;color:{text_col};margin:1px;cursor:pointer;'>{check}</div>",
                unsafe_allow_html=True,
            )
            if col.button("​", key=f"_swatchbtn_{key}_{hex_color}", help=hex_color, use_container_width=True):
                st.session_state[f"_swatch_{key}"] = hex_color
                current = hex_color
                st.rerun()

    st.markdown(
        f"<div style='display:flex;align-items:center;gap:8px;margin-top:4px;'>"
        f"<div style='width:18px;height:18px;background:{current};border:2px solid #999;border-radius:3px;flex-shrink:0;'></div>"
        f"<span style='font-size:0.78rem;color:#666;'>{current}</span></div>",
        unsafe_allow_html=True,
    )
    return current

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


def build_manual_manage_df(req_df: pd.DataFrame, manual_df: pd.DataFrame) -> pd.DataFrame:
    result = req_df[["id", "job_id", "resource_class_id"]].copy()
    result["manual_assigned_ees"] = pd.NA

    if manual_df.empty or "quantity_assigned" not in manual_df.columns:
        return result[["id", "manual_assigned_ees"]]

    if "requirement_id" in manual_df.columns:
        row_specific = manual_df.loc[manual_df["requirement_id"].notna(), ["requirement_id", "quantity_assigned"]].copy()
    else:
        row_specific = pd.DataFrame(columns=["requirement_id", "quantity_assigned"])

    if not row_specific.empty:
        row_specific["requirement_id"] = row_specific["requirement_id"].astype(int)
        row_specific = row_specific.groupby("requirement_id", as_index=False)["quantity_assigned"].sum()
        result = result.merge(row_specific, left_on="id", right_on="requirement_id", how="left")
        result["manual_assigned_ees"] = result["quantity_assigned"]
        result = result.drop(columns=["requirement_id", "quantity_assigned"])

    legacy_manual = manual_df.copy()
    if "requirement_id" in legacy_manual.columns:
        legacy_manual = legacy_manual.loc[legacy_manual["requirement_id"].isna()].copy()

    if not legacy_manual.empty:
        legacy_manual = legacy_manual.groupby(["job_id", "resource_class_id"], as_index=False)["quantity_assigned"].sum()
        if not legacy_manual.empty:
            result = result.merge(
                legacy_manual.rename(columns={"quantity_assigned": "legacy_manual_assigned_ees"}),
                on=["job_id", "resource_class_id"],
                how="left",
            )
            result["manual_assigned_ees"] = result["manual_assigned_ees"].fillna(result["legacy_manual_assigned_ees"])
            result = result.drop(columns=["legacy_manual_assigned_ees"])

    return result[["id", "manual_assigned_ees"]]

def build_rental_manage_df(rental_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate rental requirements per requirement_id for merging into manage tables.
    Returns a df with columns: requirement_id, job_id, resource_class_id, assigned_rental, rental_vendor.
    Falls back to job_id+resource_class_id for legacy NULL requirement_id records."""
    if rental_df.empty:
        return pd.DataFrame(columns=["requirement_id", "job_id", "resource_class_id", "assigned_rental", "rental_vendor"])

    has_req_id = "requirement_id" in rental_df.columns

    # Records with a requirement_id — group by it
    if has_req_id:
        linked = rental_df.loc[rental_df["requirement_id"].notna()].copy()
    else:
        linked = pd.DataFrame()

    if not linked.empty:
        linked = linked.groupby(["requirement_id", "job_id", "resource_class_id"], as_index=False).agg(
            assigned_rental=("quantity_required", "sum"),
            rental_vendor=("vendor_name", lambda s: ", ".join(sorted({str(v).strip() for v in s if str(v).strip()}))),
        )
    else:
        linked = pd.DataFrame(columns=["requirement_id", "job_id", "resource_class_id", "assigned_rental", "rental_vendor"])

    # Legacy NULL requirement_id records — group by job_id+resource_class_id
    if has_req_id:
        legacy = rental_df.loc[rental_df["requirement_id"].isna()].copy()
    else:
        legacy = rental_df.copy()

    if not legacy.empty:
        legacy = legacy.groupby(["job_id", "resource_class_id"], as_index=False).agg(
            assigned_rental=("quantity_required", "sum"),
            rental_vendor=("vendor_name", lambda s: ", ".join(sorted({str(v).strip() for v in s if str(v).strip()}))),
        )
        legacy["requirement_id"] = None
    else:
        legacy = pd.DataFrame(columns=["requirement_id", "job_id", "resource_class_id", "assigned_rental", "rental_vendor"])

    return pd.concat([linked, legacy], ignore_index=True)


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

def sort_requirements_like_board(df: pd.DataFrame, board_df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or board_df.empty or "job_code" not in df.columns:
        return df
    working = df.copy()
    board_order = {job_code: idx for idx, job_code in enumerate(board_df["job_code"].astype(str).tolist())}
    working["_board_order"] = working["job_code"].astype(str).map(board_order).fillna(10**6)
    sort_cols = ["_board_order"]
    ascending = [True]
    for col in ["required_start", "job_name", "job_code", "id"]:
        if col in working.columns:
            sort_cols.append(col)
            ascending.append(True)
    working = working.sort_values(sort_cols, ascending=ascending, kind="stable").drop(columns=["_board_order"])
    return working

def resource_options_df(include_rental: bool = False) -> pd.DataFrame:
    rc = resource_classes_df.copy()
    if not include_rental and "category" in rc.columns:
        rc = rc.loc[rc["category"].astype(str) != "Rental"].copy()
    rc["display"] = rc.apply(display_class_name, axis=1)
    return rc

@st.dialog("Edit Job & Requirement", width="large")
def _board_row_edit_dialog(req_row: pd.Series, job_row: pd.Series, active_region: str, key_prefix: str):
    """Combined job + requirement editor used on the planning board."""
    fill = str(req_row.get("customer_color", "") or "") or customer_base_color(str(req_row.get("customer", "Unassigned") or "Unassigned"))
    st.markdown(
        highlight_cell_html(
            f"{str(req_row.get('customer', '') or 'Unassigned')} | {req_row['job_name']} | {req_row['job_code']} — {req_row['class_name']}",
            fill, bold=True,
        ),
        unsafe_allow_html=True,
    )

    # ── Job section ──────────────────────────────────────────────────────────
    if True:
        region_codes = regions_df["region_code"].tolist()
        region_idx = region_codes.index(job_row["region_code"]) if job_row["region_code"] in region_codes else 0
        statuses = ["Bid", "Awarded", "Planned", "Tentative", "Active", "Billing Pending", "Complete", "Cancelled"]

        c1, c2 = st.columns(2)
        edit_customer = c1.text_input("Customer", value=str(job_row.get("customer", "") or ""))
        # Seed swatch from job's existing color on first render
        _color_key = f"job_{int(job_row['id'])}_color"
        _existing_color = str(job_row.get("customer_color", "") or "")
        if f"_swatch_{_color_key}" not in st.session_state:
            st.session_state[f"_swatch_{_color_key}"] = _existing_color if _existing_color in JOB_COLOR_PALETTE else JOB_COLOR_PALETTE[0]
        with c2:
            edit_customer_color = color_swatch_picker("Job Color", key=_color_key, default=_existing_color)

        c3, c4 = st.columns(2)
        edit_job_name = c3.text_input("Job Name", value=str(job_row["job_name"]))
        edit_region = c4.selectbox("Region", region_codes, index=region_idx, format_func=region_format, disabled=region_disabled(active_region))

        c5, c6 = st.columns(2)
        edit_location = c5.text_input("Location", value=str(job_row.get("location", "") or ""))
        status_index = statuses.index(job_row["status"]) if job_row["status"] in statuses else 0
        edit_status = c6.selectbox("Status", statuses, index=status_index)

        c7, c8, c9 = st.columns(3)
        edit_start = c7.date_input("Job Start Date", value=pd.to_datetime(job_row["job_start_date"]).date(), key=f"{key_prefix}_dlg_job_start")
        c7.caption(f"Selected: {edit_start.strftime('%m/%d/%Y')}")
        edit_duration = c8.number_input("Duration (days)", min_value=1, value=int(job_row["job_duration_days"]), step=1)
        edit_mob = c9.number_input("Mob Days Before", min_value=0, value=int(job_row["mob_days_before_job"]), step=1)

        c10, _ = st.columns(2)
        edit_demob = c10.number_input("Demob Days After", min_value=0, value=int(job_row["demob_days_after_job"]), step=1)

        preview = calc_job_dates(edit_start, int(edit_duration), int(edit_mob), int(edit_demob))
        st.info(
            f"Mob Start: {format_date_value(preview['mob_start_date'])}  |  "
            f"Job End: {format_date_value(preview['job_end_date'])}  |  "
            f"Demob End: {format_date_value(preview['demob_end_date'])}"
        )
        edit_job_notes = st.text_area("Job Notes", value=str(job_row.get("notes", "") or ""))

        st.divider()
        confirm_delete = st.checkbox("✔ Confirm delete job — this cannot be undone (also deletes all requirements)")
        col_save, col_delete = st.columns(2)
        if col_save.button("💾 Save Job", type="primary", use_container_width=True, key=f"{key_prefix}_dlg_save_job"):
            update_job(engine, int(job_row["id"]), {
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
                "notes": edit_job_notes,
            })
            st.rerun()
        if col_delete.button("🗑 Delete Job", type="secondary", use_container_width=True, disabled=not confirm_delete, key=f"{key_prefix}_dlg_delete_job"):
            delete_job(engine, int(job_row["id"]))
            st.rerun()
        if not confirm_delete:
            st.caption("Check the box above to enable job deletion.")

    # ── Requirement section ───────────────────────────────────────────────────
    st.divider()
    st.markdown("**📋 Requirement**")
    if True:
        rc_df = resource_options_df()
        rc_names = rc_df["class_name"].astype(str).tolist()
        legacy_class = str(req_row["class_name"])
        class_options = rc_names.copy()
        if legacy_class not in class_options:
            class_options = [legacy_class] + class_options
        current_idx = class_options.index(legacy_class)

        edit_class_name = st.selectbox("Resource Class", class_options, index=current_idx, key=f"{key_prefix}_dlg_class")
        edit_rc_match = rc_df.loc[rc_df["class_name"] == edit_class_name]
        edit_rc_id = int(edit_rc_match.iloc[0]["id"]) if not edit_rc_match.empty else int(req_row["resource_class_id"])
        edit_unit_type = str(edit_rc_match.iloc[0]["unit_type"]) if not edit_rc_match.empty else str(req_row.get("unit_type", "units"))
        edit_category = str(edit_rc_match.iloc[0]["category"]) if not edit_rc_match.empty else ""
        step = quantity_step(edit_unit_type, edit_category)
        fmt = quantity_format(edit_unit_type, edit_category)

        c1, c2, c3 = st.columns(3)
        edit_qty = c1.number_input("Quantity Required", min_value=0.0, value=float(req_row["quantity_required"]), step=step, format=fmt, key=f"{key_prefix}_dlg_qty")
        edit_assigned_ees = c2.number_input("Assigned EES", min_value=0.0, value=float(req_row.get("assigned_ees", 0.0)), step=step, format=fmt, key=f"{key_prefix}_dlg_ees")
        edit_assigned_rental = c3.number_input("Assigned Rental", min_value=0.0, value=float(req_row.get("assigned_rental", 0.0)), step=step, format=fmt, key=f"{key_prefix}_dlg_rental")

        c4, c5 = st.columns(2)
        edit_vendor = c4.text_input("Rental Vendor", value=str(req_row.get("rental_vendor", "") or ""), key=f"{key_prefix}_dlg_vendor")
        priorities = ["Low", "Normal", "High", "Critical"]
        p_index = priorities.index(req_row["priority"]) if req_row.get("priority") in priorities else 1
        edit_priority = c5.selectbox("Priority", priorities, index=p_index, key=f"{key_prefix}_dlg_priority")

        c6, c7 = st.columns(2)
        edit_before = c6.number_input("Days Before Job Start", min_value=0, value=int(req_row["days_before_job_start"]), step=1, key=f"{key_prefix}_dlg_before")
        edit_after = c7.number_input("Days After Job End", min_value=0, value=int(req_row["days_after_job_end"]), step=1, key=f"{key_prefix}_dlg_after")

        edit_req_notes = st.text_area("Requirement Notes", value=str(req_row.get("notes", "") or ""), key=f"{key_prefix}_dlg_req_notes")

        st.divider()
        col_save_req, col_del_req = st.columns(2)
        if col_save_req.button("💾 Save Requirement", type="primary", use_container_width=True, key=f"{key_prefix}_dlg_save_req"):
            capped_ees = min(float(edit_assigned_ees), float(edit_qty))
            update_requirement(engine, int(req_row["id"]), {
                "resource_class_id": edit_rc_id,
                "quantity_required": float(edit_qty),
                "days_before_job_start": int(edit_before),
                "days_after_job_end": int(edit_after),
                "priority": edit_priority,
                "notes": edit_req_notes,
            })
            upsert_manual_owned_allocation_for_job_class(
                engine, int(req_row["job_id"]), int(edit_rc_id), capped_ees,
                int(edit_before), int(edit_after), edit_req_notes, requirement_id=int(req_row["id"]),
            )
            upsert_rental_requirement_for_job_class(
                engine, int(req_row["job_id"]), int(edit_rc_id),
                float(edit_assigned_rental), int(edit_before), int(edit_after), edit_vendor, edit_req_notes,
                requirement_id=int(req_row["id"]),
            )
            st.rerun()
        if col_del_req.button("🗑 Delete Requirement", type="secondary", use_container_width=True, key=f"{key_prefix}_dlg_del_req"):
            delete_requirement(engine, int(req_row["id"]))
            st.rerun()


@st.dialog("Edit Job", width="large")
def _job_edit_dialog(row: pd.Series, active_region: str):
    region_codes = regions_df["region_code"].tolist()
    region_idx = region_codes.index(row["region_code"]) if row["region_code"] in region_codes else 0
    statuses = ["Bid", "Awarded", "Planned", "Tentative", "Active", "Billing Pending", "Complete", "Cancelled"]

    fill = str(row.get("customer_color", "") or "") or customer_base_color(str(row.get("customer", "Unassigned") or "Unassigned"))
    st.markdown(
        highlight_cell_html(
            f"{str(row.get('customer', '') or 'Unassigned')} | {row['job_name']} | {row['job_code']}",
            fill, bold=True,
        ),
        unsafe_allow_html=True,
    )
    st.divider()

    c1, c2 = st.columns(2)
    edit_customer = c1.text_input("Customer", value=str(row.get("customer", "") or ""))
    _color_key_jd = f"job_{int(row['id'])}_color"
    _existing_color_jd = str(row.get("customer_color", "") or "")
    if f"_swatch_{_color_key_jd}" not in st.session_state:
        st.session_state[f"_swatch_{_color_key_jd}"] = _existing_color_jd if _existing_color_jd in JOB_COLOR_PALETTE else JOB_COLOR_PALETTE[0]
    with c2:
        edit_customer_color = color_swatch_picker("Job Color", key=_color_key_jd, default=_existing_color_jd)

    c3, c4 = st.columns(2)
    edit_job_name = c3.text_input("Job Name", value=str(row["job_name"]))
    edit_region = c4.selectbox("Region", region_codes, index=region_idx, format_func=region_format, disabled=region_disabled(active_region))

    c5, c6 = st.columns(2)
    edit_location = c5.text_input("Location", value=str(row.get("location", "") or ""))
    status_index = statuses.index(row["status"]) if row["status"] in statuses else 0
    edit_status = c6.selectbox("Status", statuses, index=status_index)

    c7, c8, c9 = st.columns(3)
    edit_start = c7.date_input("Job Start Date", value=pd.to_datetime(row["job_start_date"]).date())
    c7.caption(f"Selected: {edit_start.strftime('%m/%d/%Y')}")
    edit_duration = c8.number_input("Job Duration (days)", min_value=1, value=int(row["job_duration_days"]), step=1)
    edit_mob = c9.number_input("Mob Days Before", min_value=0, value=int(row["mob_days_before_job"]), step=1)

    c10, c11 = st.columns(2)
    edit_demob = c10.number_input("Demob Days After", min_value=0, value=int(row["demob_days_after_job"]), step=1)

    preview = calc_job_dates(edit_start, int(edit_duration), int(edit_mob), int(edit_demob))
    st.info(
        f"Mob Start: {format_date_value(preview['mob_start_date'])}  |  "
        f"Job End: {format_date_value(preview['job_end_date'])}  |  "
        f"Demob End: {format_date_value(preview['demob_end_date'])}"
    )

    edit_notes = st.text_area("Notes", value=str(row.get("notes", "") or ""))

    st.divider()
    confirm_delete = st.checkbox("✔ Confirm delete — this cannot be undone")
    col_save, col_delete = st.columns(2)

    if col_save.button("💾 Save Changes", type="primary", use_container_width=True):
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

    if col_delete.button("🗑 Delete Job", type="secondary", use_container_width=True, disabled=not confirm_delete):
        delete_job(engine, int(row["id"]))
        st.rerun()

    if not confirm_delete:
        st.caption("Check the box above to enable deletion.")


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

    dialog_key = "jobs_table_open_job_id"

    for _, row in df.iterrows():
        cols = st.columns(widths)
        customer_text = str(row.get("customer", "") or "Unassigned")
        fill = str(row.get("customer_color", "") or "") or customer_base_color(customer_text)
        render_highlighted_column(cols[0], customer_text, fill, bold=True)
        render_highlighted_column(cols[1], str(row["job_name"]), fill)
        render_highlighted_column(cols[2], str(row["job_code"]), fill)
        render_highlighted_column(cols[3], format_date_value(row["mob_start_date"]), fill, center=True)
        render_highlighted_column(cols[4], format_date_value(row["job_start_date"]), fill, center=True)
        render_highlighted_column(cols[5], format_date_value(row["job_end_date"]), fill, center=True)
        render_highlighted_column(cols[6], format_date_value(row["demob_end_date"]), fill, center=True)

        if cols[7].button("Edit / Delete", key=f"open_job_dialog_{row['id']}", use_container_width=True):
            st.session_state[dialog_key] = (int(row["id"]), active_region)

    if dialog_key in st.session_state:
        open_id, open_region = st.session_state[dialog_key]
        del st.session_state[dialog_key]
        match = df.loc[df["id"] == open_id]
        if not match.empty:
            _job_edit_dialog(match.iloc[0], open_region)


def render_requirements_manage_table(df: pd.DataFrame, key_prefix: str = 'req', highlight_by_job: bool = False):
    st.markdown("##### Manage Requirements")
    if df.empty:
        st.info("No requirements yet.")
        return

    widths = [1.15, 1.35, 0.95, 1.2, 0.85, 0.9, 0.9, 1.0, 1.2, 0.8]
    headers = ["Customer", "Job Name", "Job Code", "Class", "Quantity", "Assigned EES", "Assigned Rental", "Status", "Notes", "Manage"]

    hdr = st.columns(widths)
    for c, h in zip(hdr, headers):
        c.markdown(f"**{h}**")

    rc_df = resource_options_df()
    rc_names = rc_df["class_name"].astype(str).tolist()
    df = sort_requirements_by_class_order(df)

    for _, row in df.iterrows():
        cols = st.columns(widths)
        customer_text = str(row.get("customer", "") or "Unassigned")
        job_name_text = str(row.get("job_name", "") or "")
        fill = str(row.get("customer_color", "") or "") or customer_base_color(customer_text)
        if highlight_by_job:
            render_highlighted_column(cols[0], customer_text, fill, bold=True)
            render_highlighted_column(cols[1], job_name_text, fill)
            render_highlighted_column(cols[2], str(row["job_code"]), fill)
            render_highlighted_column(cols[3], str(row["class_name"]), fill)
            render_highlighted_column(cols[4], format_compact_number(row["quantity_required"]), fill, center=True)
            render_highlighted_column(cols[5], format_compact_number(row.get("assigned_ees", 0)), fill, center=True)
            render_highlighted_column(cols[6], format_compact_number(row.get("assigned_rental", 0)), fill, center=True)
            render_highlighted_column(cols[7], str(row.get("allocation_status", "")), fill)
            render_highlighted_column(cols[8], str(row.get("notes", "") or ""), fill)
        else:
            cols[0].write(customer_text)
            cols[1].write(job_name_text)
            cols[2].write(str(row["job_code"]))
            cols[3].write(str(row["class_name"]))
            cols[4].write(format_compact_number(row["quantity_required"]))
            cols[5].write(format_compact_number(row.get("assigned_ees", 0)))
            cols[6].write(format_compact_number(row.get("assigned_rental", 0)))
            cols[7].write(str(row.get("allocation_status", "")))
            cols[8].write(str(row.get("notes", "") or ""))

        with cols[9].popover("Edit/Delete", use_container_width=True):
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
                capped_assigned_ees = min(float(edit_assigned_ees), float(edit_qty))
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
                    capped_assigned_ees,
                    int(edit_before),
                    int(edit_after),
                    edit_notes,
                    requirement_id=int(row["id"]),
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
                    requirement_id=int(row["id"]),
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


def highlight_cell_html(text, fill: str, bold: bool = False, center: bool = False) -> str:
    pill_bg = hex_to_rgba(fill, 0.18)
    pill_border = hex_to_rgba(fill, 0.55)
    weight = 700 if bold else 400
    align = "center" if center else "left"
    return (
        f"<div style='background:{pill_bg}; border-left:4px solid {fill}; border-radius:8px; "
        f"padding:6px 8px; font-weight:{weight}; text-align:{align}; min-height:38px; display:flex; align-items:center;'>"
        f"{text}</div>"
    )


def render_highlighted_column(col, text, fill: str, bold: bool = False, center: bool = False):
    col.markdown(highlight_cell_html(text, fill, bold=bold, center=center), unsafe_allow_html=True)


def class_order_map() -> dict[str, int]:
    return {name: idx for idx, name in enumerate(resource_options_df()["class_name"].astype(str).tolist())}


def sort_requirements_by_class_order(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "class_name" not in df.columns:
        return df
    working = df.copy()
    order_map = class_order_map()
    working["_class_order"] = working["class_name"].astype(str).map(order_map).fillna(10_000)
    sort_cols = ["_class_order"]
    ascending = [True]
    for col in ["required_start", "job_name", "job_code", "id"]:
        if col in working.columns:
            sort_cols.append(col)
            ascending.append(True)
    working = working.sort_values(sort_cols, ascending=ascending, kind="stable").drop(columns=["_class_order"])
    return working
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

    if not selected_class:
        selected_class = class_options[0]
    elif selected_class not in class_options and not include_excluded:
        # Only reset to default for the main board; Bid/Awarded board mirrors main board selection
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

        rental_qty = float(job_rent["quantity_required"].astype(float).sum()) if not job_rent.empty else 0.0
        total_required = float(job_req["quantity_required"].astype(float).sum()) if not job_req.empty else 0.0

        ees_qty = max(total_required - rental_qty, 0.0)
        if not job_req.empty:
            job_req_calc = job_req.copy()
            job_req_calc["assigned_rental"] = 0.0
            if not job_rent.empty:
                if "requirement_id" in job_rent.columns and job_rent["requirement_id"].notna().any():
                    rental_by_req = (
                        job_rent.loc[job_rent["requirement_id"].notna()]
                        .groupby("requirement_id", as_index=False)["quantity_required"]
                        .sum()
                        .rename(columns={"quantity_required": "assigned_rental"})
                    )
                    job_req_calc = job_req_calc.merge(rental_by_req, left_on="id", right_on="requirement_id", how="left", suffixes=("", "_rent"))
                    if "assigned_rental_rent" in job_req_calc.columns:
                        job_req_calc["assigned_rental"] = job_req_calc["assigned_rental_rent"].fillna(job_req_calc["assigned_rental"])
                        job_req_calc = job_req_calc.drop(columns=["assigned_rental_rent"])
                    if "requirement_id" in job_req_calc.columns:
                        job_req_calc = job_req_calc.drop(columns=["requirement_id"])
                    # Also handle any legacy NULL requirement_id rental records
                    rental_legacy = job_rent.loc[job_rent["requirement_id"].isna()]
                    if not rental_legacy.empty:
                        rental_by_bucket = (
                            rental_legacy.groupby(["job_id", "resource_class_id"], as_index=False)["quantity_required"]
                            .sum()
                            .rename(columns={"quantity_required": "assigned_rental_leg"})
                        )
                        job_req_calc = job_req_calc.merge(rental_by_bucket, on=["job_id", "resource_class_id"], how="left")
                        job_req_calc["assigned_rental"] = job_req_calc["assigned_rental"].fillna(job_req_calc["assigned_rental_leg"])
                        job_req_calc = job_req_calc.drop(columns=["assigned_rental_leg"])
                else:
                    rental_by_bucket = (
                        job_rent.groupby(["job_id", "resource_class_id"], as_index=False)["quantity_required"]
                        .sum()
                        .rename(columns={"quantity_required": "assigned_rental"})
                    )
                    job_req_calc = job_req_calc.merge(rental_by_bucket, on=["job_id", "resource_class_id"], how="left", suffixes=("", "_rent"))
                    if "assigned_rental_rent" in job_req_calc.columns:
                        job_req_calc["assigned_rental"] = job_req_calc["assigned_rental_rent"].fillna(job_req_calc["assigned_rental"])
                        job_req_calc = job_req_calc.drop(columns=["assigned_rental_rent"])
            if not job_manual.empty:
                if "requirement_id" in job_manual.columns:
                    manual_by_req = (
                        job_manual.groupby("requirement_id", as_index=False)["quantity_assigned"]
                        .sum()
                        .rename(columns={"quantity_assigned": "manual_assigned_ees"})
                    )
                    job_req_calc = job_req_calc.merge(manual_by_req, left_on="id", right_on="requirement_id", how="left")
                    if "requirement_id" in job_req_calc.columns:
                        job_req_calc = job_req_calc.drop(columns=["requirement_id"])
                else:
                    manual_by_bucket = (
                        job_manual.groupby(["job_id", "resource_class_id"], as_index=False)["quantity_assigned"]
                        .sum()
                        .rename(columns={"quantity_assigned": "manual_assigned_ees"})
                    )
                    job_req_calc = job_req_calc.merge(manual_by_bucket, on=["job_id", "resource_class_id"], how="left")

            if "manual_assigned_ees" not in job_req_calc.columns:
                job_req_calc["manual_assigned_ees"] = pd.NA
            elif "manual_assigned_ees_x" in job_req_calc.columns or "manual_assigned_ees_y" in job_req_calc.columns:
                left_col = pd.to_numeric(job_req_calc.get("manual_assigned_ees_x"), errors="coerce") if "manual_assigned_ees_x" in job_req_calc.columns else pd.Series(pd.NA, index=job_req_calc.index)
                right_col = pd.to_numeric(job_req_calc.get("manual_assigned_ees_y"), errors="coerce") if "manual_assigned_ees_y" in job_req_calc.columns else pd.Series(pd.NA, index=job_req_calc.index)
                job_req_calc["manual_assigned_ees"] = left_col.fillna(right_col)
                drop_cols = [c for c in ["manual_assigned_ees_x", "manual_assigned_ees_y"] if c in job_req_calc.columns]
                if drop_cols:
                    job_req_calc = job_req_calc.drop(columns=drop_cols)

            if "assigned_rental" not in job_req_calc.columns:
                job_req_calc["assigned_rental"] = 0.0
            job_req_calc["assigned_rental"] = pd.to_numeric(job_req_calc["assigned_rental"], errors="coerce").fillna(0.0).astype(float)
            default_ees = (job_req_calc["quantity_required"].astype(float) - job_req_calc["assigned_rental"]).clip(lower=0)
            job_req_calc["assigned_ees"] = pd.to_numeric(job_req_calc["manual_assigned_ees"], errors="coerce").fillna(default_ees)
            ees_qty = float(job_req_calc["assigned_ees"].astype(float).sum())

        vendors = ", ".join(sorted(set(v for v in job_rent.get("vendor_name", pd.Series(dtype=str)).fillna("").tolist() if str(v).strip())))
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
        # Use whatever the main board has selected; don't reset to class_options[0]
        # if the class simply has no Bid/Awarded rows — board data will be empty in that case
        if not selected_class:
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
        tickfont=dict(size=14),
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
            "staticPlot": True,
            "scrollZoom": False,
            "doubleClick": False,
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

    # ── Merged board manage table ─────────────────────────────────────────────
    st.markdown("##### Manage Jobs & Requirements Shown on Board")
    key_prefix = "boardreq_pipeline" if include_excluded else "boardreq_active"
    req_manage = filter_by_job_status(region_filter(requirement_summary_df(engine), active_region), include_excluded=include_excluded)
    req_manage = req_manage.loc[req_manage["class_name"] == selected_class].copy() if not req_manage.empty else pd.DataFrame()

    if req_manage.empty:
        st.info("No requirement rows shown on this board.")
    else:
        board_job_codes = board_df["job_code"].astype(str).tolist()
        req_manage = req_manage.loc[req_manage["job_code"].astype(str).isin(board_job_codes)].copy()

        rental_manage = filter_by_job_status(region_filter(get_rental_requirements_df(engine), active_region), include_excluded=include_excluded)
        rental_manage = rental_manage.loc[rental_manage["class_name"] == selected_class].copy() if not rental_manage.empty else pd.DataFrame()
        rental_manage = build_rental_manage_df(rental_manage)

        manual_manage = filter_by_job_status(region_filter(get_manual_owned_allocations_df(engine), active_region), include_excluded=include_excluded)
        manual_manage = manual_manage.loc[manual_manage["class_name"] == selected_class].copy() if not manual_manage.empty else pd.DataFrame()

        manage_df = req_manage.copy()
        # Merge rental by requirement_id first, fall back to job_id+resource_class_id for legacy NULLs
        rental_linked = rental_manage.loc[rental_manage["requirement_id"].notna()].copy() if not rental_manage.empty else pd.DataFrame()
        rental_legacy = rental_manage.loc[rental_manage["requirement_id"].isna()].copy() if not rental_manage.empty else pd.DataFrame()
        if not rental_linked.empty:
            rental_linked["requirement_id"] = rental_linked["requirement_id"].astype(int)
            manage_df = manage_df.merge(rental_linked[["requirement_id", "assigned_rental", "rental_vendor"]], left_on="id", right_on="requirement_id", how="left").drop(columns=["requirement_id"], errors="ignore")
        else:
            manage_df["assigned_rental"] = pd.NA
            manage_df["rental_vendor"] = pd.NA
        if not rental_legacy.empty:
            manage_df = manage_df.merge(rental_legacy[["job_id", "resource_class_id", "assigned_rental", "rental_vendor"]].rename(columns={"assigned_rental": "assigned_rental_leg", "rental_vendor": "rental_vendor_leg"}), on=["job_id", "resource_class_id"], how="left")
            manage_df["assigned_rental"] = manage_df["assigned_rental"].fillna(manage_df["assigned_rental_leg"])
            manage_df["rental_vendor"] = manage_df["rental_vendor"].fillna(manage_df["rental_vendor_leg"])
            manage_df = manage_df.drop(columns=["assigned_rental_leg", "rental_vendor_leg"], errors="ignore")
        manual_manage_df = build_manual_manage_df(manage_df, manual_manage)
        manage_df = manage_df.merge(manual_manage_df, on="id", how="left")

        # Resolve any suffixed columns from unexpected merges
        for col in ["quantity_required", "assigned_rental", "rental_vendor", "manual_assigned_ees"]:
            for suffix in ["_x", "_y"]:
                if f"{col}{suffix}" in manage_df.columns:
                    if col not in manage_df.columns:
                        manage_df[col] = manage_df[f"{col}{suffix}"]
                    manage_df = manage_df.drop(columns=[f"{col}{suffix}"])
        for col, default in [("assigned_rental", 0.0), ("rental_vendor", "")]:
            if col not in manage_df.columns:
                manage_df[col] = default
        if "manual_assigned_ees" not in manage_df.columns:
            manage_df["manual_assigned_ees"] = None
        if "quantity_required" not in manage_df.columns:
            manage_df["quantity_required"] = 0.0
        manage_df["quantity_required"] = manage_df["quantity_required"].astype(float)
        manage_df["assigned_rental"] = pd.to_numeric(manage_df["assigned_rental"], errors="coerce").fillna(0.0)
        manage_df["assigned_ees"] = pd.to_numeric(manage_df["manual_assigned_ees"], errors="coerce").fillna(
            (manage_df["quantity_required"] - manage_df["assigned_rental"]).clip(lower=0)
        )
        manage_df["rental_vendor"] = manage_df["rental_vendor"].fillna("")
        manage_df = sort_requirements_like_board(manage_df, board_df)

        # Fetch all jobs once for the dialog lookup
        all_jobs_lookup = region_filter(get_jobs_df(engine), active_region)

        widths = [1.15, 1.35, 0.95, 1.2, 0.85, 0.9, 0.9, 1.0, 1.2, 0.8]
        headers = ["Customer", "Job Name", "Job Code", "Class", "Quantity", "Assigned EES", "Assigned Rental", "Status", "Notes", "Manage"]
        hdr = st.columns(widths)
        for c, h in zip(hdr, headers):
            c.markdown(f"**{h}**")

        dialog_state_key = f"{key_prefix}_open_req_id"

        for _, row in manage_df.iterrows():
            cols = st.columns(widths)
            customer_text = str(row.get("customer", "") or "Unassigned")
            fill = str(row.get("customer_color", "") or "") or customer_base_color(customer_text)
            render_highlighted_column(cols[0], customer_text, fill, bold=True)
            render_highlighted_column(cols[1], str(row.get("job_name", "")), fill)
            render_highlighted_column(cols[2], str(row["job_code"]), fill)
            render_highlighted_column(cols[3], str(row["class_name"]), fill)
            render_highlighted_column(cols[4], format_compact_number(row["quantity_required"]), fill, center=True)
            render_highlighted_column(cols[5], format_compact_number(row.get("assigned_ees", 0)), fill, center=True)
            render_highlighted_column(cols[6], format_compact_number(row.get("assigned_rental", 0)), fill, center=True)
            render_highlighted_column(cols[7], str(row.get("allocation_status", "")), fill)
            render_highlighted_column(cols[8], str(row.get("notes", "") or ""), fill)

            if cols[9].button("Edit/Delete", key=f"{key_prefix}_open_{row['id']}", use_container_width=True):
                st.session_state[dialog_state_key] = int(row["id"])

        if dialog_state_key in st.session_state:
            open_req_id = st.session_state[dialog_state_key]
            del st.session_state[dialog_state_key]
            req_match = manage_df.loc[manage_df["id"] == open_req_id]
            if not req_match.empty:
                req_row = req_match.iloc[0]
                job_match = all_jobs_lookup.loc[all_jobs_lookup["id"] == req_row["job_id"]] if not all_jobs_lookup.empty else pd.DataFrame()
                if not job_match.empty:
                    _board_row_edit_dialog(req_row, job_match.iloc[0], active_region, key_prefix=f"{key_prefix}_{open_req_id}")

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

tab_jobs, tab_job_requirements, tab_planning, tab_pools, tab_requirements, tab_allocations, tab_revenue, tab_bidding, tab_history = st.tabs(
    ["Jobs", "Job Requirements", "Planning Board", "Pools", "Requirements", "Allocations", "Revenue", "Bidding", "📈 History"]
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
        _create_color_key = f"create_job_color_{region_key_suffix}"
        customer_color = color_swatch_picker("Job Color", key=_create_color_key, default=JOB_COLOR_PALETTE[0])
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
            st.session_state.pop("jobs_table_open_job_id", None)
            st.rerun()

    jobs_df = filter_active_jobs_for_management(region_filter(get_jobs_df(engine), ACTIVE_REGION))
    if not jobs_df.empty and "job_start_date" in jobs_df.columns:
        jobs_df = jobs_df.sort_values(["job_start_date", "mob_start_date", "job_name", "job_code"], ascending=[True, True, True, True]).reset_index(drop=True)
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

        selected_fill = str(selected_job.get("customer_color", "") or "") or customer_base_color(str(selected_job.get("customer", "Unassigned") or "Unassigned"))
        st.markdown(
            highlight_cell_html(
                f"Selected: {str(selected_job.get('customer', '') or 'Unassigned')} | {selected_job['job_name']} | {selected_job['job_code']}",
                selected_fill,
                bold=True,
            ),
            unsafe_allow_html=True,
        )

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
            selected_job_reqs = sort_requirements_by_class_order(selected_job_reqs)
            display_req = selected_job_reqs[["class_name", "quantity_required", "required_start", "required_end", "notes"]].copy()
            display_req["quantity_required"] = display_req["quantity_required"].map(format_compact_number)
            display_req["notes"] = display_req["notes"].fillna("")
            display_req = format_dates_for_display(display_req)
            display_req.columns = ["Class Name", "Quantity Required", "Required Start", "Required End", "Notes"]
            render_simple_html_table(display_req, qty_columns=["Quantity Required"])
            rental_manage = region_filter(get_rental_requirements_df(engine), ACTIVE_REGION)
            rental_manage = rental_manage.loc[rental_manage["job_code"] == selected_job["job_code"]].copy() if not rental_manage.empty else pd.DataFrame()
            rental_manage = build_rental_manage_df(rental_manage)
            manual_manage = region_filter(get_manual_owned_allocations_df(engine), ACTIVE_REGION)
            manual_manage = manual_manage[["job_id", "resource_class_id", "requirement_id", "quantity_assigned"]].copy() if not manual_manage.empty else pd.DataFrame(columns=["job_id", "resource_class_id", "requirement_id", "quantity_assigned"])
            manage_df = selected_job_reqs.copy()
            rental_linked = rental_manage.loc[rental_manage["requirement_id"].notna()].copy() if not rental_manage.empty else pd.DataFrame()
            rental_legacy = rental_manage.loc[rental_manage["requirement_id"].isna()].copy() if not rental_manage.empty else pd.DataFrame()
            if not rental_linked.empty:
                rental_linked["requirement_id"] = rental_linked["requirement_id"].astype(int)
                manage_df = manage_df.merge(rental_linked[["requirement_id", "assigned_rental", "rental_vendor"]], left_on="id", right_on="requirement_id", how="left").drop(columns=["requirement_id"], errors="ignore")
            else:
                manage_df["assigned_rental"] = pd.NA
                manage_df["rental_vendor"] = pd.NA
            if not rental_legacy.empty:
                manage_df = manage_df.merge(rental_legacy[["job_id", "resource_class_id", "assigned_rental", "rental_vendor"]].rename(columns={"assigned_rental": "assigned_rental_leg", "rental_vendor": "rental_vendor_leg"}), on=["job_id", "resource_class_id"], how="left")
                manage_df["assigned_rental"] = manage_df["assigned_rental"].fillna(manage_df["assigned_rental_leg"])
                manage_df["rental_vendor"] = manage_df["rental_vendor"].fillna(manage_df["rental_vendor_leg"])
                manage_df = manage_df.drop(columns=["assigned_rental_leg", "rental_vendor_leg"], errors="ignore")
            manage_df = manage_df.merge(build_manual_manage_df(manage_df, manual_manage), on="id", how="left")
            manage_df["assigned_rental"] = manage_df["assigned_rental"].fillna(0.0)
            manage_df["assigned_ees"] = manage_df["manual_assigned_ees"].fillna((manage_df["quantity_required"].astype(float) - manage_df["assigned_rental"].astype(float)).clip(lower=0))
            manage_df["rental_vendor"] = manage_df["rental_vendor"].fillna("")
            render_requirements_manage_table(manage_df, key_prefix="jobreq")

        st.markdown("##### Selected Job Rental Requirements")
        rental_summary = region_filter(get_rental_requirements_df(engine), ACTIVE_REGION)
        selected_job_rentals = rental_summary.loc[rental_summary["job_code"] == selected_job["job_code"]].copy() if not rental_summary.empty else pd.DataFrame()
        if selected_job_rentals.empty:
            st.info("No rental requirements yet for this job.")
        else:
            selected_job_rentals = sort_requirements_by_class_order(selected_job_rentals)
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
        req_summary = sort_requirements_by_class_order(req_summary)
        rental_manage = region_filter(get_rental_requirements_df(engine), ACTIVE_REGION)
        rental_manage = build_rental_manage_df(rental_manage)
        manual_manage = region_filter(get_manual_owned_allocations_df(engine), ACTIVE_REGION)
        manual_manage = manual_manage[["job_id", "resource_class_id", "requirement_id", "quantity_assigned"]].copy() if not manual_manage.empty else pd.DataFrame(columns=["job_id", "resource_class_id", "requirement_id", "quantity_assigned"])
        manage_df = req_summary.copy()
        rental_linked = rental_manage.loc[rental_manage["requirement_id"].notna()].copy() if not rental_manage.empty else pd.DataFrame()
        rental_legacy = rental_manage.loc[rental_manage["requirement_id"].isna()].copy() if not rental_manage.empty else pd.DataFrame()
        if not rental_linked.empty:
            rental_linked["requirement_id"] = rental_linked["requirement_id"].astype(int)
            manage_df = manage_df.merge(rental_linked[["requirement_id", "assigned_rental", "rental_vendor"]], left_on="id", right_on="requirement_id", how="left").drop(columns=["requirement_id"], errors="ignore")
        else:
            manage_df["assigned_rental"] = pd.NA
            manage_df["rental_vendor"] = pd.NA
        if not rental_legacy.empty:
            manage_df = manage_df.merge(rental_legacy[["job_id", "resource_class_id", "assigned_rental", "rental_vendor"]].rename(columns={"assigned_rental": "assigned_rental_leg", "rental_vendor": "rental_vendor_leg"}), on=["job_id", "resource_class_id"], how="left")
            manage_df["assigned_rental"] = manage_df["assigned_rental"].fillna(manage_df["assigned_rental_leg"])
            manage_df["rental_vendor"] = manage_df["rental_vendor"].fillna(manage_df["rental_vendor_leg"])
            manage_df = manage_df.drop(columns=["assigned_rental_leg", "rental_vendor_leg"], errors="ignore")
        manage_df = manage_df.merge(build_manual_manage_df(manage_df, manual_manage), on="id", how="left")
        manage_df["assigned_rental"] = pd.to_numeric(manage_df["assigned_rental"], errors="coerce").fillna(0.0)
        manage_df["assigned_ees"] = pd.to_numeric(manage_df["manual_assigned_ees"], errors="coerce").fillna((manage_df["quantity_required"].astype(float) - manage_df["assigned_rental"]).clip(lower=0))
        manage_df["rental_vendor"] = manage_df["rental_vendor"].fillna("")

        display_req = format_dates_for_display(manage_df[["job_code","job_name","region_code","class_name","quantity_required","unit_type","required_start","required_end","assigned_ees","assigned_rental","quantity_shortfall","allocation_status"]].copy())
        display_req = display_req.rename(columns={"assigned_ees": "assigned_ees", "assigned_rental": "rental"})
        display_req["region_code"] = display_req["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_req, width="stretch")

        manage_df = sort_requirements_by_class_order(manage_df)
        render_requirements_manage_table(manage_df)

with tab_planning:
    render_planning_board(ACTIVE_REGION, include_excluded=False, section_title="Planning Board")

    # ── Extra boards ──────────────────────────────────────────────────────────
    if "extra_board_classes" not in st.session_state:
        st.session_state["extra_board_classes"] = []

    extra_classes = st.session_state["extra_board_classes"]

    # Collect all available classes for the add selector
    _req_all_extra = filter_by_job_status(region_filter(requirement_summary_df(engine), ACTIVE_REGION), include_excluded=False)
    _class_order_extra = resource_classes_df["class_name"].dropna().astype(str).tolist()
    _available_extra = _req_all_extra["class_name"].dropna().astype(str).unique().tolist() if not _req_all_extra.empty else []
    _class_options_extra = [c for c in _class_order_extra if c in _available_extra]

    for idx, cls in enumerate(list(extra_classes)):
        st.divider()
        # Header row with class selector and remove button
        h1, h2, h3, h4 = st.columns([2, 1, 1, 0.4])
        sel_class = h1.selectbox(
            "Resource View",
            _class_options_extra,
            index=_class_options_extra.index(cls) if cls in _class_options_extra else 0,
            key=f"extra_board_class_{idx}",
            label_visibility="collapsed",
        )
        # Sync class selection back
        if sel_class != extra_classes[idx]:
            st.session_state["extra_board_classes"][idx] = sel_class
            st.rerun()

        board_start_extra = st.session_state.get("planning_start_active", date.today())
        num_weeks_extra = st.session_state.get("planning_weeks_active", 12)
        h2.markdown(f"**Board Start:** {pd.to_datetime(board_start_extra).strftime('%m/%d/%Y')}")
        h3.markdown(f"**Weeks:** {num_weeks_extra}")
        if h4.button("✕", key=f"remove_extra_board_{idx}", help="Remove this board"):
            st.session_state["extra_board_classes"].pop(idx)
            st.rerun()

        board_df_ex, summary_df_ex, gridlines_ex, tickvals_ex, ticktext_ex, x_end_ex, sel_class_ex = build_planning_board_data(
            active_region=ACTIVE_REGION,
            selected_class=sel_class,
            start_date=board_start_extra,
            num_weeks=num_weeks_extra,
            include_excluded=False,
        )

        if board_df_ex.empty:
            st.info(f"No requirements for {sel_class}.")
        else:
            st.subheader(sel_class)
            # Reuse the chart rendering logic inline
            summary_metrics_ex = ["Need", "In Region", "Availability"]
            total_job_rows_ex = len(board_df_ex)
            total_rows_ex = total_job_rows_ex + 1 + len(summary_metrics_ex)
            fig_ex = go.Figure()
            x0_ex = pd.to_datetime(board_start_extra).normalize()
            for ws in gridlines_ex:
                fig_ex.add_vline(x=ws, line_width=1, line_color="rgba(120,120,120,0.40)")
            for y in [i + 0.5 for i in range(total_rows_ex + 1)]:
                width = 2 if abs(y - (len(summary_metrics_ex) + 0.5)) < 1e-9 else 1
                color = "rgba(90,90,90,0.55)" if width == 2 else "rgba(160,160,160,0.28)"
                fig_ex.add_hline(y=y, line_width=width, line_color=color)
            row_positions_ex = []
            row_labels_ex = []
            for i, (_, row) in enumerate(board_df_ex.iterrows()):
                y = total_rows_ex - i
                row_positions_ex.append(y)
                row_labels_ex.append(str(row["customer"]))
                fill = str(row.get("customer_color", "") or "") or customer_base_color(str(row["customer"]))
                start = pd.to_datetime(row["required_start"])
                finish = pd.to_datetime(row["required_end"]) + pd.Timedelta(days=1)
                fig_ex.add_shape(type="rect", x0=start, x1=finish, y0=y-0.34, y1=y+0.34,
                    line=dict(color=hex_to_rgba(fill, 0.70), width=1), fillcolor=hex_to_rgba(fill, 0.28), layer="below")
                fig_ex.add_annotation(x=start+(finish-start)/2, y=y, text=str(row["label"]),
                    showarrow=False, font=dict(size=11, color="black"), xanchor="center", yanchor="middle")
            summary_y_ex = {"Need": 3, "In Region": 2, "Availability": 1}
            for metric, y in summary_y_ex.items():
                row_positions_ex.append(y)
                row_labels_ex.append(metric)
            if not summary_df_ex.empty:
                for _, rec in summary_df_ex.iterrows():
                    y = summary_y_ex.get(rec["Metric"])
                    if y is None:
                        continue
                    val = float(rec["Value"])
                    if rec["Metric"] == "Availability":
                        font = dict(size=14, color=availability_font_color(val), family="Arial Black")
                    elif rec["Metric"] == "Need":
                        font = dict(size=12, color="#9b1c1c")
                    else:
                        font = dict(size=12, color="#1b4f9b")
                    fig_ex.add_annotation(x=pd.to_datetime(rec["X"]), y=y, text=format_compact_number(val),
                        showarrow=False, font=font, xanchor="center", yanchor="middle")
            fig_ex.add_trace(go.Scatter(x=[x0_ex, x_end_ex], y=[0, total_rows_ex+1],
                mode="markers", marker_opacity=0, hoverinfo="skip", showlegend=False))
            fig_ex.update_xaxes(tickmode="array", tickvals=tickvals_ex, ticktext=ticktext_ex,
                side="top", showgrid=False, range=[x0_ex, x_end_ex], tickfont=dict(size=10), tickangle=-30, fixedrange=True)
            fig_ex.update_yaxes(tickmode="array", tickvals=row_positions_ex, ticktext=row_labels_ex,
                range=[0.5, total_rows_ex+0.5], showgrid=False, zeroline=False, tickfont=dict(size=14), fixedrange=True)
            fig_ex.update_layout(height=max(520, 120+total_rows_ex*50), margin=dict(l=30,r=20,t=30,b=20),
                plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig_ex, width="stretch", config={
                "staticPlot": True, "scrollZoom": False, "doubleClick": False,
                "displayModeBar": True, "displaylogo": False,
                "modeBarButtons": [["toImage"]],
                "toImageButtonOptions": {"format": "png", "filename": f"board_{sel_class_ex}", "height": 1200, "width": 2200, "scale": 2},
            })

            # Manage table
            key_prefix_ex = f"extra_board_{idx}"
            req_manage_ex = filter_by_job_status(region_filter(requirement_summary_df(engine), ACTIVE_REGION), include_excluded=False)
            req_manage_ex = req_manage_ex.loc[req_manage_ex["class_name"] == sel_class_ex].copy() if not req_manage_ex.empty else pd.DataFrame()
            if not req_manage_ex.empty:
                board_job_codes_ex = board_df_ex["job_code"].astype(str).tolist()
                req_manage_ex = req_manage_ex.loc[req_manage_ex["job_code"].astype(str).isin(board_job_codes_ex)].copy()
                rental_manage_ex = filter_by_job_status(region_filter(get_rental_requirements_df(engine), ACTIVE_REGION), include_excluded=False)
                rental_manage_ex = rental_manage_ex.loc[rental_manage_ex["class_name"] == sel_class_ex].copy() if not rental_manage_ex.empty else pd.DataFrame()
                rental_manage_ex = build_rental_manage_df(rental_manage_ex)
                manual_manage_ex = filter_by_job_status(region_filter(get_manual_owned_allocations_df(engine), ACTIVE_REGION), include_excluded=False)
                manual_manage_ex = manual_manage_ex.loc[manual_manage_ex["class_name"] == sel_class_ex].copy() if not manual_manage_ex.empty else pd.DataFrame()
                manage_df_ex = req_manage_ex.copy()
                rental_linked_ex = rental_manage_ex.loc[rental_manage_ex["requirement_id"].notna()].copy() if not rental_manage_ex.empty else pd.DataFrame()
                rental_legacy_ex = rental_manage_ex.loc[rental_manage_ex["requirement_id"].isna()].copy() if not rental_manage_ex.empty else pd.DataFrame()
                if not rental_linked_ex.empty:
                    rental_linked_ex["requirement_id"] = rental_linked_ex["requirement_id"].astype(int)
                    manage_df_ex = manage_df_ex.merge(rental_linked_ex[["requirement_id", "assigned_rental", "rental_vendor"]], left_on="id", right_on="requirement_id", how="left").drop(columns=["requirement_id"], errors="ignore")
                else:
                    manage_df_ex["assigned_rental"] = pd.NA
                    manage_df_ex["rental_vendor"] = pd.NA
                if not rental_legacy_ex.empty:
                    manage_df_ex = manage_df_ex.merge(rental_legacy_ex[["job_id", "resource_class_id", "assigned_rental", "rental_vendor"]].rename(columns={"assigned_rental": "assigned_rental_leg", "rental_vendor": "rental_vendor_leg"}), on=["job_id", "resource_class_id"], how="left")
                    manage_df_ex["assigned_rental"] = manage_df_ex["assigned_rental"].fillna(manage_df_ex["assigned_rental_leg"])
                    manage_df_ex["rental_vendor"] = manage_df_ex["rental_vendor"].fillna(manage_df_ex["rental_vendor_leg"])
                    manage_df_ex = manage_df_ex.drop(columns=["assigned_rental_leg", "rental_vendor_leg"], errors="ignore")
                manage_df_ex = manage_df_ex.merge(build_manual_manage_df(manage_df_ex, manual_manage_ex), on="id", how="left")
                for col, default in [("assigned_rental", 0.0), ("rental_vendor", "")]:
                    if col not in manage_df_ex.columns:
                        manage_df_ex[col] = default
                if "manual_assigned_ees" not in manage_df_ex.columns:
                    manage_df_ex["manual_assigned_ees"] = None
                if "quantity_required" not in manage_df_ex.columns:
                    manage_df_ex["quantity_required"] = 0.0
                manage_df_ex["quantity_required"] = manage_df_ex["quantity_required"].astype(float)
                manage_df_ex["assigned_rental"] = pd.to_numeric(manage_df_ex["assigned_rental"], errors="coerce").fillna(0.0)
                manage_df_ex["assigned_ees"] = pd.to_numeric(manage_df_ex["manual_assigned_ees"], errors="coerce").fillna(
                    (manage_df_ex["quantity_required"] - manage_df_ex["assigned_rental"]).clip(lower=0))
                manage_df_ex["rental_vendor"] = manage_df_ex["rental_vendor"].fillna("")
                manage_df_ex = sort_requirements_like_board(manage_df_ex, board_df_ex)
                all_jobs_lookup_ex = region_filter(get_jobs_df(engine), ACTIVE_REGION)
                st.markdown("##### Manage Jobs & Requirements Shown on Board")
                widths = [1.15, 1.35, 0.95, 1.2, 0.85, 0.9, 0.9, 1.0, 1.2, 0.8]
                headers = ["Customer", "Job Name", "Job Code", "Class", "Quantity", "Assigned EES", "Assigned Rental", "Status", "Notes", "Manage"]
                hdr = st.columns(widths)
                for c, h in zip(hdr, headers):
                    c.markdown(f"**{h}**")
                dialog_state_key_ex = f"{key_prefix_ex}_open_req_id"
                for _, row in manage_df_ex.iterrows():
                    cols = st.columns(widths)
                    customer_text = str(row.get("customer", "") or "Unassigned")
                    fill = str(row.get("customer_color", "") or "") or customer_base_color(customer_text)
                    render_highlighted_column(cols[0], customer_text, fill, bold=True)
                    render_highlighted_column(cols[1], str(row.get("job_name", "")), fill)
                    render_highlighted_column(cols[2], str(row["job_code"]), fill)
                    render_highlighted_column(cols[3], str(row["class_name"]), fill)
                    render_highlighted_column(cols[4], format_compact_number(row["quantity_required"]), fill, center=True)
                    render_highlighted_column(cols[5], format_compact_number(row.get("assigned_ees", 0)), fill, center=True)
                    render_highlighted_column(cols[6], format_compact_number(row.get("assigned_rental", 0)), fill, center=True)
                    render_highlighted_column(cols[7], str(row.get("allocation_status", "")), fill)
                    render_highlighted_column(cols[8], str(row.get("notes", "") or ""), fill)
                    if cols[9].button("Edit/Delete", key=f"{key_prefix_ex}_open_{row['id']}", use_container_width=True):
                        st.session_state[dialog_state_key_ex] = int(row["id"])
                if dialog_state_key_ex in st.session_state:
                    open_req_id_ex = st.session_state[dialog_state_key_ex]
                    del st.session_state[dialog_state_key_ex]
                    req_match_ex = manage_df_ex.loc[manage_df_ex["id"] == open_req_id_ex]
                    if not req_match_ex.empty:
                        req_row_ex = req_match_ex.iloc[0]
                        job_match_ex = all_jobs_lookup_ex.loc[all_jobs_lookup_ex["id"] == req_row_ex["job_id"]] if not all_jobs_lookup_ex.empty else pd.DataFrame()
                        if not job_match_ex.empty:
                            _board_row_edit_dialog(req_row_ex, job_match_ex.iloc[0], ACTIVE_REGION, key_prefix=f"{key_prefix_ex}_{open_req_id_ex}")

    # ── Add board button ──────────────────────────────────────────────────────
    st.divider()
    if _class_options_extra:
        add_col1, add_col2 = st.columns([2, 5])
        add_class = add_col1.selectbox(
            "Add board for resource class:",
            _class_options_extra,
            key="add_extra_board_class_select",
            label_visibility="collapsed",
        )
        if add_col2.button("＋ Add Planning Board", key="add_extra_board_btn"):
            st.session_state["extra_board_classes"].append(add_class)
            st.rerun()

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
        active_req_summary = sort_requirements_by_class_order(active_req_summary)
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
        pipeline_req_summary = sort_requirements_by_class_order(pipeline_req_summary)
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



# ═══════════════════════════════════════════════════════════════════════════════
# REVENUE TAB
# ═══════════════════════════════════════════════════════════════════════════════
with tab_revenue:
    st.subheader("Revenue Accrual")

    LOB_LABEL = "Rockies (WT-CO)"
    REVENUE_REGION = "RM"   # Rockies region code

    # ── Month / year selector ─────────────────────────────────────────────────
    today = date.today()
    rc1, rc2, rc3 = st.columns([1, 1, 4])
    rev_month = rc1.selectbox(
        "Month",
        list(range(1, 13)),
        index=today.month - 1,
        format_func=lambda m: date(2000, m, 1).strftime("%B"),
        key="rev_month",
    )
    rev_year = rc2.number_input(
        "Year", min_value=2020, max_value=2035,
        value=today.year, step=1, key="rev_year",
    )
    month_label = date(int(rev_year), int(rev_month), 1).strftime("%B %Y")

    st.divider()

    # ── Load jobs for this region / month ─────────────────────────────────────
    rev_jobs = get_revenue_jobs_df(engine, REVENUE_REGION, int(rev_month), int(rev_year))

    if rev_jobs.empty:
        st.info(f"No active Rockies jobs found for {month_label}. Create jobs in the Jobs tab with region **RM – Rockies**.")
    else:
        # ── Summary metrics bar ───────────────────────────────────────────────
        all_li = get_line_items_df(engine)
        job_ids = rev_jobs["id"].tolist()
        month_li = all_li[all_li["job_id"].isin(job_ids)] if not all_li.empty else pd.DataFrame()

        total_revenue = 0.0
        if not month_li.empty:
            for _, li in month_li.iterrows():
                lt = None
                if li.get("line_total") is not None and not pd.isna(li["line_total"]):
                    lt = float(li["line_total"])
                else:
                    qty = li.get("invoice_qty")
                    price = li.get("unit_price")
                    if qty is not None and price is not None and not pd.isna(qty) and not pd.isna(price):
                        lt = float(qty) * float(price)
                if lt:
                    total_revenue += lt

        m1, m2, m3 = st.columns(3)
        m1.metric("Active Jobs", len(rev_jobs))
        m2.metric("Total Revenue", f"${total_revenue:,.2f}")
        m3.metric("Customers", rev_jobs["customer"].nunique())

        st.divider()

        # ── Job selector ──────────────────────────────────────────────────────
        rev_jobs["_display"] = (
            rev_jobs["customer"].fillna("—") + "  |  " +
            rev_jobs["job_name"] + "  |  " + rev_jobs["job_code"]
        )
        rev_job_ids = rev_jobs["id"].tolist()

        rev_sel_key = f"rev_selected_job_id_{REVENUE_REGION}"
        if rev_sel_key not in st.session_state or st.session_state[rev_sel_key] not in rev_job_ids:
            st.session_state[rev_sel_key] = int(rev_job_ids[0])

        selected_rev_job_id = st.selectbox(
            "Select Job to Edit",
            rev_job_ids,
            format_func=lambda jid: rev_jobs.loc[rev_jobs["id"] == jid, "_display"].iloc[0],
            key=rev_sel_key,
        )
        sel_job = rev_jobs.loc[rev_jobs["id"] == selected_rev_job_id].iloc[0]
        sel_fill = str(sel_job.get("customer_color", "") or "") or customer_base_color(
            str(sel_job.get("customer", "Unassigned") or "Unassigned")
        )
        st.markdown(
            highlight_cell_html(
                f"{str(sel_job.get('customer','') or '—')}  |  "
                f"{sel_job['job_name']}  |  {sel_job['job_code']}",
                sel_fill, bold=True,
            ),
            unsafe_allow_html=True,
        )

        # ── Billing fields ────────────────────────────────────────────────────
        st.markdown("##### Billing Info")

        # Billing type — drives how line items are entered and how ticket is built
        BILLING_TYPE_LABELS = {
            "line_item": "Line Item (itemized setup, day rate, demob)",
            "day_rate":  "Day Rate (setup + single day rate + demob)",
            "per_bbl":   "Per BBL (fixed per-barrel price)",
        }
        existing_billing_type = str(sel_job.get("billing_type", "") or "line_item")
        if existing_billing_type not in BILLING_TYPE_LABELS:
            existing_billing_type = "line_item"
        b_billing_type = st.selectbox(
            "Billing Method",
            list(BILLING_TYPE_LABELS.keys()),
            format_func=lambda x: BILLING_TYPE_LABELS[x],
            index=list(BILLING_TYPE_LABELS.keys()).index(existing_billing_type),
            key=f"rev_billing_type_{selected_rev_job_id}",
        )

        # Row 1: Invoice / billing fields
        bf1, bf2, bf3, bf4 = st.columns([1.5, 1.5, 1.5, 0.8])
        # "Company Man" on revenue sheet = "Ordered By" on ticket — same field
        b_ordered_by     = bf1.text_input("Company Man / Ordered By",
                                          value=str(sel_job.get("ordered_by", "") or ""),
                                          key=f"rev_ob_{selected_rev_job_id}",
                                          help="Shown as 'Co. Man' on revenue report and 'Ordered By' on field ticket.")
        b_invoice_number = bf2.text_input("Invoice #",     value=str(sel_job.get("invoice_number",  "") or ""), key=f"rev_inv_{selected_rev_job_id}")
        b_so_ticket      = bf3.text_input("SO / Ticket #", value=str(sel_job.get("so_ticket_number","") or ""), key=f"rev_so_{selected_rev_job_id}")
        b_accrue_raw = sel_job.get("accrue")
        b_accrue = bf4.checkbox(
            "Accrue",
            value=bool(b_accrue_raw) if b_accrue_raw is not None else False,
            key=f"rev_accrue_{selected_rev_job_id}",
        )

        # Row 2: Field ticket fields
        tf1, tf2, tf3 = st.columns(3)
        b_ees_supervisor = tf1.text_input("EES Supervisor", value=str(sel_job.get("ees_supervisor", "") or ""), key=f"rev_sup_{selected_rev_job_id}")
        b_customer_po    = tf2.text_input("Customer PO",    value=str(sel_job.get("customer_po",    "") or ""), key=f"rev_po_{selected_rev_job_id}")
        b_county_state   = tf3.text_input("County & State", value=str(sel_job.get("county_state",   "") or ""), key=f"rev_cs_{selected_rev_job_id}")

        tf4, tf5, tf6 = st.columns(3)
        b_well_name      = tf4.text_input("Well Name",      value=str(sel_job.get("well_name",      "") or ""), key=f"rev_wn_{selected_rev_job_id}")
        b_well_number    = tf5.text_input("Well Number",    value=str(sel_job.get("well_number",    "") or ""), key=f"rev_wnr_{selected_rev_job_id}")
        b_department     = tf6.text_input("Department",     value=str(sel_job.get("department",     "") or ""), key=f"rev_dept_{selected_rev_job_id}")

        b_job_description = st.text_input(
            "Job Description (appears on ticket)",
            value=str(sel_job.get("job_description", "") or ""),
            key=f"rev_jd_{selected_rev_job_id}",
        )

        if st.button("💾 Save Billing Info", key=f"rev_save_billing_{selected_rev_job_id}"):
            update_job_billing(engine, int(selected_rev_job_id), {
                "ordered_by":       b_ordered_by,
                "invoice_number":   b_invoice_number,
                "so_ticket_number": b_so_ticket,
                "billing_type":     b_billing_type,
                "accrue":           bool(b_accrue),
                "ees_supervisor":   b_ees_supervisor,
                "customer_po":      b_customer_po,
                "county_state":     b_county_state,
                "well_name":        b_well_name,
                "well_number":      b_well_number,
                "department":       b_department,
                "job_description":  b_job_description,
            })
            st.success("Billing info saved.")
            st.rerun()

        st.divider()

        # ── Line item editor ──────────────────────────────────────────────────
        st.markdown("##### Line Items")

        # Guidance changes based on billing method
        if b_billing_type == "line_item":
            st.caption(
                "**Line Item billing:** Enter each setup, day rate, and demob charge as a separate row. "                "Setup dated day before job start, demob dated day after job end."
            )
        elif b_billing_type == "day_rate":
            st.caption(
                "**Day Rate billing:** Enter 3 rows — Setup (Ea, 1 unit), "                "Day Rate (Day, × number of days), Demob (Ea, 1 unit)."
            )
        elif b_billing_type == "per_bbl":
            st.caption(
                "**Per BBL billing:** Enter 1 row — description, BBL as UOM, "                "total BBLs as qty, per-BBL price as unit price."
            )

        existing_li = get_line_items_df(engine, job_id=int(selected_rev_job_id))

        UOM_OPTIONS = ["Day", "Ea", "BBL", "Gal", "MMBTU", "Ton", "Hour", "Month", "Ft", "Mile", ""]

        # Default rows depend on billing method
        if existing_li.empty:
            if b_billing_type == "day_rate":
                editor_base = pd.DataFrame({
                    "Description": ["Setup", "Day Rate", "Demob"],
                    "UOM":         ["Ea", "Day", "Ea"],
                    "Start":       [None, None, None],
                    "End":         [None, None, None],
                    "Invoice Qty": [1.0, None, 1.0],
                    "Unit Price":  [None, None, None],
                    "Line Total":  [None, None, None],
                    "Notes":       ["", "", ""],
                })
            elif b_billing_type == "per_bbl":
                editor_base = pd.DataFrame({
                    "Description": ["Water Transfer — Per BBL"],
                    "UOM":         ["BBL"],
                    "Start":       [None],
                    "End":         [None],
                    "Invoice Qty": [None],
                    "Unit Price":  [None],
                    "Line Total":  [None],
                    "Notes":       [""],
                })
            else:
                editor_base = pd.DataFrame({
                    "Description": [""] * 5,
                    "UOM":         ["Day"] * 5,
                    "Start":       [None] * 5,
                    "End":         [None] * 5,
                    "Invoice Qty": [None] * 5,
                    "Unit Price":  [None] * 5,
                    "Line Total":  [None] * 5,
                    "Notes":       [""] * 5,
                })
        else:
            editor_base = pd.DataFrame({
                "Description": existing_li["description"].fillna("").tolist(),
                "UOM":         existing_li["uom"].fillna("").tolist(),
                "Start":       pd.to_datetime(existing_li["start_date"], errors="coerce").dt.date.tolist(),
                "End":         pd.to_datetime(existing_li["end_date"],   errors="coerce").dt.date.tolist(),
                "Invoice Qty": existing_li["invoice_qty"].tolist(),
                "Unit Price":  existing_li["unit_price"].tolist(),
                "Line Total":  existing_li["line_total"].tolist(),
                "Notes":       existing_li["notes"].fillna("").tolist(),
            })

        li_reset_key = f"rev_li_reset_{selected_rev_job_id}"
        if li_reset_key not in st.session_state:
            st.session_state[li_reset_key] = 0
        li_editor_key = f"rev_li_editor_{selected_rev_job_id}_{st.session_state[li_reset_key]}"

        edited_li = st.data_editor(
            editor_base,
            num_rows="dynamic",
            hide_index=True,
            key=li_editor_key,
            column_config={
                "Description": st.column_config.TextColumn("Description", width="large"),
                "UOM":         st.column_config.SelectboxColumn("UOM", options=UOM_OPTIONS, width="small"),
                "Start":       st.column_config.DateColumn("Start", format="MM/DD/YYYY", width="medium"),
                "End":         st.column_config.DateColumn("End",   format="MM/DD/YYYY", width="medium"),
                "Invoice Qty": st.column_config.NumberColumn("Invoice Qty", format="%.2f", width="small"),
                "Unit Price":  st.column_config.NumberColumn("Unit Price ($)", format="%.4f", width="medium"),
                "Line Total":  st.column_config.NumberColumn("Line Total ($)", format="%.2f", width="medium"),
                "Notes":       st.column_config.TextColumn("Notes", width="medium"),
            },
            width="stretch",
        )

        st.caption(
            "💡 Leave **Line Total** blank to auto-calculate from Qty × Price on export. "
            "Enter it directly to override (e.g. fuel estimates)."
        )

        if st.button("💾 Save Line Items", type="primary", key=f"rev_save_li_{selected_rev_job_id}"):
            rows_to_save = []
            for _, r in edited_li.iterrows():
                desc = str(r.get("Description", "") or "").strip()
                if not desc:
                    continue
                rows_to_save.append({
                    "description": desc,
                    "uom":         str(r.get("UOM", "") or ""),
                    "start_date":  r.get("Start"),
                    "end_date":    r.get("End"),
                    "invoice_qty": r.get("Invoice Qty"),
                    "unit_price":  r.get("Unit Price"),
                    "line_total":  r.get("Line Total"),
                    "notes":       str(r.get("Notes", "") or ""),
                })
            save_line_items(engine, int(selected_rev_job_id), rows_to_save)
            st.session_state[li_reset_key] += 1
            st.success(f"Saved {len(rows_to_save)} line item(s).")
            st.rerun()

        # ── Line item preview for selected job ────────────────────────────────
        preview_li = get_line_items_df(engine, job_id=int(selected_rev_job_id))
        if not preview_li.empty:
            st.markdown("**Saved Line Items**")
            disp = preview_li[["line_number", "description", "uom",
                                "start_date", "end_date",
                                "invoice_qty", "unit_price", "line_total"]].copy()
            disp.columns = ["#", "Description", "UOM", "Start", "End",
                            "Qty", "Unit Price", "Line Total"]
            disp["Start"] = pd.to_datetime(disp["Start"], errors="coerce").dt.strftime("%m/%d/%Y").fillna("")
            disp["End"]   = pd.to_datetime(disp["End"],   errors="coerce").dt.strftime("%m/%d/%Y").fillna("")
            for col in ["Qty", "Unit Price", "Line Total"]:
                disp[col] = disp[col].apply(lambda v: f"${float(v):,.2f}" if pd.notna(v) and v != "" else "")
            st.dataframe(disp, hide_index=True, use_container_width=True)

        st.divider()

        # ── All jobs summary table ────────────────────────────────────────────
        st.markdown(f"##### All Rockies Jobs — {month_label}")
        all_li_for_month = get_line_items_df(engine)
        summary_rows = []
        for _, j in rev_jobs.iterrows():
            jli = all_li_for_month[all_li_for_month["job_id"] == j["id"]] if not all_li_for_month.empty else pd.DataFrame()
            jt = 0.0
            for _, li in jli.iterrows():
                lt = li.get("line_total")
                if lt is not None and not pd.isna(lt):
                    jt += float(lt)
                else:
                    qty = li.get("invoice_qty")
                    price = li.get("unit_price")
                    if qty is not None and price is not None and not pd.isna(qty) and not pd.isna(price):
                        jt += float(qty) * float(price)
            summary_rows.append({
                "Job Code":    j["job_code"],
                "Job Name":    j["job_name"],
                "Customer":    str(j.get("customer", "") or "—"),
                "Co. Man":     str(j.get("ordered_by",  "") or "—"),
                "Invoice #":   str(j.get("invoice_number", "") or "—"),
                "Day Rate":    f"${float(j['day_rate']):,.2f}" if j.get("day_rate") and not pd.isna(j["day_rate"]) else "—",
                "Job Total":   f"${jt:,.2f}",
                "Accrue":      "✓" if j.get("accrue") else "",
            })
        st.dataframe(pd.DataFrame(summary_rows), hide_index=True, use_container_width=True)

        st.divider()

        # ── Linked Bid ────────────────────────────────────────────────────────
        st.markdown("##### Linked Bid")
        linked_bid = query_df(engine,
            "SELECT id, bid_name, status, billing_type FROM bids WHERE job_id = :jid",
            {"jid": int(selected_rev_job_id)})
        if linked_bid.empty:
            st.caption("No bid linked to this job.")
            if st.button("➕ Create Bid for This Job",
                         key=f"rev_create_bid_{selected_rev_job_id}"):
                st.session_state["bb_active_bid_id"] = None
                st.session_state["bb_mode"] = "New Bid"
                st.info("Switch to the **Bidding → Bid Builder** tab.")
        else:
            bid_row = linked_bid.iloc[0]
            bc1, bc2 = st.columns([4, 1])
            BILLING_DISPLAY = {"line_item": "Line Item",
                               "day_rate": "Day Rate", "per_bbl": "Per BBL"}
            bc1.write(
                f"**{bid_row['bid_name']}** — "                f"{bid_row['status']} | "                f"{BILLING_DISPLAY.get(str(bid_row['billing_type']), bid_row['billing_type'])}"
            )
            if bc2.button("✏️ Edit Bid", key=f"rev_edit_bid_{selected_rev_job_id}"):
                bid_id = int(bid_row["id"])
                for k in ["bb_customer", "bb_rate_card", "bb_job_name",
                           "bb_status", "bb_billing_type", "bb_bid_days",
                           "bb_total_bbls", "bb_hrs_shift", "bb_labor_gen",
                           "bb_labor_lead", "bb_labor_sup", "bb_trucks"]:
                    st.session_state.pop(k, None)
                st.session_state["bb_active_bid_id"] = bid_id
                st.session_state["bb_last_loaded_bid_id"] = None
                st.session_state["bb_existing_bid"] = bid_id
                st.session_state["bb_mode"] = "Edit Existing"
                st.info("Switch to the **Bidding → Bid Builder** tab.")

        st.divider()

        # ── Field Ticket download ─────────────────────────────────────────────
        st.markdown("##### Field Ticket")
        ticket_li = get_line_items_df(engine, job_id=int(selected_rev_job_id))
        ticket_bytes = build_ticket_excel(sel_job.to_dict(), ticket_li)
        job_code_safe = str(sel_job.get("job_code", "job")).replace("/", "-")
        st.download_button(
            label="📋  Download Field Ticket",
            data=ticket_bytes,
            file_name=f"FieldTicket_{job_code_safe}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"rev_ticket_dl_{selected_rev_job_id}",
        )
        st.caption("Ticket pre-fills from saved billing info and line items above.")

        st.divider()

        # ── Excel export ──────────────────────────────────────────────────────
        st.markdown("##### Export Revenue Report")
        if st.button("📊 Generate Excel Report", key="rev_generate_excel"):
            all_li_export = get_line_items_df(engine)
            li_for_export = all_li_export[all_li_export["job_id"].isin(job_ids)] if not all_li_export.empty else pd.DataFrame()
            xlsx_bytes = build_revenue_excel(
                jobs_df=rev_jobs,
                line_items_df=li_for_export,
                lob_label=LOB_LABEL,
                month_label=month_label,
            )
            st.download_button(
                label=f"⬇️  Download {month_label} Revenue Report",
                data=xlsx_bytes,
                file_name=f"Revenue_{LOB_LABEL.replace(' ','_')}_{month_label.replace(' ','_')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="rev_download_xlsx",
            )


# ─────────────────────────────────────────────────────────────────────────────
# BIDDING TAB — paste this block into app.py after the Revenue tab
# Requires: from services.bidding import * at top of app.py
# ─────────────────────────────────────────────────────────────────────────────

def render_bidding_tab(engine):
    import pandas as pd
    import streamlit as st
    from services.bidding import (
        get_catalog, get_customers_with_rate_cards, get_rate_card,
        upsert_rate_card_row, add_customer_rate_card,
        get_bids, get_bid, get_bid_items, save_bid, save_bid_item,
        calc_bid, bid_to_job_line_items,
    )
    from services.revenue_scheduler import save_line_items

    BILLING_LABELS = {
        "line_item": "Line Item",
        "day_rate":  "Day Rate",
        "per_bbl":   "Per BBL",
    }
    STATUS_OPTIONS = ["Draft", "Sent", "Won", "Lost", "Expired"]
    MONEY = "${:,.2f}"

    sub_rate, sub_bids, sub_builder = st.tabs(
        ["📋 Rate Cards", "📁 Bids", "🔨 Bid Builder"]
    )

    # ═════════════════════════════════════════════════════════════════════════
    # RATE CARDS TAB
    # ═════════════════════════════════════════════════════════════════════════
    with sub_rate:
        st.subheader("Customer Rate Cards")
        st.caption("Blank = not applicable / not yet priced. Changes save immediately.")

        rc_customers = get_customers_with_rate_cards(engine)
        CREATE_OPT = "➕  Create new rate card…"

        # Single selectbox — blank on load, create option first, then existing customers
        options = [CREATE_OPT] + (rc_customers if rc_customers else [])
        selection = st.selectbox(
            "View / Edit Customer",
            options,
            index=None,
            placeholder="Select a customer…",
            key="rc_customer_select",
        )

        if selection is None:
            st.info("Select a customer above to view their rate card, or create a new one.")

        elif selection == CREATE_OPT:
            st.markdown("**New customer rate card**")
            nc1, nc2 = st.columns([3, 1])
            new_cust_name = nc1.text_input("Customer name", key="rc_new_customer",
                                            placeholder="e.g. Devon Energy")
            if nc2.button("Create", key="rc_create", use_container_width=True):
                if new_cust_name.strip():
                    add_customer_rate_card(engine, new_cust_name.strip())
                    st.success(
                        f"✅ Rate card created for **{new_cust_name.strip()}** — "                        f"seeded from Standard pricing.")
                    st.rerun()
                else:
                    st.warning("Enter a customer name first.")

        else:
            selected_cust = selection

            with st.expander(f"⚠️  Delete '{selected_cust}' rate card", expanded=False):
                st.warning(
                    f"This permanently deletes all rates for **{selected_cust}**. "                    f"Cannot be undone.")
                confirm_name = st.text_input(
                    "Type the customer name to confirm",
                    key=f"rc_delete_confirm_{selected_cust}",
                    placeholder=selected_cust,
                )
                if st.button("🗑️ Delete permanently", key=f"rc_delete_{selected_cust}",
                             type="primary"):
                    if confirm_name.strip() == selected_cust:
                        execute(engine,
                            "DELETE FROM customer_rate_cards WHERE customer_name = :cust",
                            {"cust": selected_cust})
                        st.success(f"Rate card for {selected_cust} deleted.")
                        st.rerun()
                    else:
                        st.error("Name doesn't match — deletion cancelled.")

            rc_df = get_rate_card(engine, selected_cust)

            for cat in rc_df["category"].unique().tolist():
                cat_df = rc_df[rc_df["category"] == cat].reset_index(drop=True)
                with st.expander(f"**{cat}**  ({len(cat_df)} items)", expanded=False):
                    h1, h2, h3, h4, h5 = st.columns([3, 1.2, 1.2, 1.2, 1.2])
                    h1.markdown("**Item**"); h2.markdown("**Setup**")
                    h3.markdown("**Day Rate**"); h4.markdown("**Demob**")
                    h5.markdown("**Save**")

                    for _, row in cat_df.iterrows():
                        c1, c2, c3, c4, c5 = st.columns([3, 1.2, 1.2, 1.2, 1.2])
                        ikey = f"rc_{selected_cust}_{row['item_id']}"
                        c1.write(row["name"])

                        # SQL NULLs come back as float NaN in pandas — use pd.isna()
                        _has_s = bool(row["has_setup"])
                        _has_d = bool(row["has_day_rate"])
                        _has_m = bool(row["has_demob"])
                        s_val = float(row["setup_rate"]) if not pd.isna(row["setup_rate"]) else None
                        d_val = float(row["day_rate"])   if not pd.isna(row["day_rate"])   else None
                        m_val = float(row["demob_rate"]) if not pd.isna(row["demob_rate"]) else None

                        s_inp = c2.number_input("", value=float(s_val) if s_val else 0.0,
                            min_value=0.0, step=0.01, format="%.4f",
                            key=f"{ikey}_s",
                            disabled=not _has_s,
                            label_visibility="collapsed")

                        d_inp = c3.number_input("", value=float(d_val) if d_val else 0.0,
                            min_value=0.0, step=0.01, format="%.4f",
                            key=f"{ikey}_d",
                            disabled=not _has_d,
                            label_visibility="collapsed")

                        m_inp = c4.number_input("", value=float(m_val) if m_val else 0.0,
                            min_value=0.0, step=0.01, format="%.4f",
                            key=f"{ikey}_m",
                            disabled=not _has_m,
                            label_visibility="collapsed")

                        if c5.button("💾", key=f"{ikey}_save"):
                            upsert_rate_card_row(
                                engine, selected_cust, int(row["item_id"]),
                                s_inp if _has_s else None,
                                d_inp if _has_d else None,
                                m_inp if _has_m else None,
                            )
                            st.success(f"Saved {row['name']}", icon="✓")
                            st.rerun()

    # ═════════════════════════════════════════════════════════════════════════
    # BIDS LIST TAB
    # ═════════════════════════════════════════════════════════════════════════
    with sub_bids:
        st.subheader("Bids")

        bids_df = get_bids(engine)

        # Status filter
        status_filter = st.multiselect(
            "Filter by status", STATUS_OPTIONS,
            default=["Draft", "Sent"],
            key="bids_status_filter",
        )
        if status_filter and not bids_df.empty:
            bids_df = bids_df[bids_df["status"].isin(status_filter)]

        if bids_df.empty:
            st.info("No bids yet — create one in the Bid Builder tab.")
        else:
            display_cols = ["id", "bid_name", "customer", "rate_card",
                            "billing_type", "status", "bid_days", "total_bbls", "job_code"]
            show_df = bids_df[[c for c in display_cols if c in bids_df.columns]].copy()
            BILLING_DISPLAY = {"line_item": "Line Item",
                               "day_rate": "Day Rate", "per_bbl": "Per BBL"}
            if "billing_type" in show_df.columns:
                show_df["billing_type"] = show_df["billing_type"].map(
                    lambda x: BILLING_DISPLAY.get(str(x), str(x)))
            show_df.columns = [c.replace("_", " ").title() for c in show_df.columns]
            st.dataframe(show_df, hide_index=True, use_container_width=True)

        # Quick status update
        if not bids_df.empty:
            st.divider()
            st.markdown("##### Update Bid Status")
            bc1, bc2, bc3 = st.columns(3)
            bid_id_options = bids_df["id"].tolist()
            sel_bid_id = bc1.selectbox(
                "Bid", bid_id_options,
                format_func=lambda x: f"#{x} — {bids_df.loc[bids_df['id']==x,'bid_name'].values[0]}",
                key="bids_status_sel",
            )
            new_status = bc2.selectbox("New Status", STATUS_OPTIONS,
                                       key="bids_new_status")
            if bc3.button("Update Status", key="bids_update_status"):
                from services.db import execute as db_execute
                db_execute(engine,
                    "UPDATE bids SET status=:s, updated_at=CURRENT_TIMESTAMP WHERE id=:id",
                    {"s": new_status, "id": sel_bid_id})
                st.success(f"Bid #{sel_bid_id} → {new_status}")
                st.rerun()

    # ═════════════════════════════════════════════════════════════════════════
    # BID BUILDER TAB
    # ═════════════════════════════════════════════════════════════════════════
    with sub_builder:
        st.subheader("Bid Builder")

        customers = get_customers_with_rate_cards(engine)
        all_bids  = get_bids(engine)

        # ── Select existing bid or create new ─────────────────────────────
        col_mode, col_sel = st.columns([1, 2])
        mode = col_mode.radio("", ["New Bid", "Edit Existing"],
                              key="bb_mode", horizontal=True)

        if mode == "Edit Existing" and not all_bids.empty:
            sel_bid_id = col_sel.selectbox(
                "Select bid",
                all_bids["id"].tolist(),
                format_func=lambda x: (
                    f"#{x} — "
                    f"{all_bids.loc[all_bids['id']==x,'bid_name'].values[0]} "
                    f"({all_bids.loc[all_bids['id']==x,'customer'].values[0]}) "
                    f"[{all_bids.loc[all_bids['id']==x,'status'].values[0]}]"
                ),
                key="bb_existing_bid",
            )
            existing = get_bid(engine, sel_bid_id)
            # When bid changes, write values directly into session state
            # so widgets pick them up immediately on next render
            last_loaded = st.session_state.get("bb_last_loaded_bid_id")
            if last_loaded != sel_bid_id and existing is not None:
                from services.bidding import get_customers_with_rate_cards as _get_rc
                _rc_names = _get_rc(engine)

                # Customer
                st.session_state["bb_customer"] = str(existing.get("customer") or "")

                # Rate card — use stored rate_card, fall back to matching customer name
                _stored_rc = str(existing.get("rate_card") or "")
                if not _stored_rc and st.session_state["bb_customer"] in _rc_names:
                    _stored_rc = st.session_state["bb_customer"]
                st.session_state["bb_rate_card"] = _stored_rc if _stored_rc in _rc_names else (_rc_names[0] if _rc_names else "Standard")

                # Status
                _status_opts = ["Draft", "Sent", "Won", "Lost", "Expired"]
                _status = str(existing.get("status") or "Draft")
                st.session_state["bb_status"] = _status if _status in _status_opts else "Draft"

                # Billing type
                _bt_opts = ["line_item", "day_rate", "per_bbl"]
                _bt = str(existing.get("billing_type") or "line_item")
                st.session_state["bb_billing_type"] = _bt if _bt in _bt_opts else "line_item"

                # Numeric fields
                st.session_state["bb_bid_days"]   = int(existing.get("bid_days") or 0)
                st.session_state["bb_total_bbls"]  = float(existing.get("total_bbls") or 0.0)
                st.session_state["bb_hrs_shift"]   = float(existing.get("hrs_per_shift") or 14.0)
                st.session_state["bb_labor_gen"]   = int(existing.get("labor_general") or 0)
                st.session_state["bb_labor_lead"]  = int(existing.get("labor_lead") or 0)
                st.session_state["bb_labor_sup"]   = int(existing.get("labor_supervisor") or 0)
                st.session_state["bb_trucks"]      = int(existing.get("trucks") or 0)

                # Job Name — find matching label
                from services.db import query_df as _qdf
                from sqlalchemy import text as _text
                _jobs = _qdf(engine, "SELECT id, job_code, job_name, customer FROM jobs ORDER BY job_code")
                _job_opts = ["(New — not linked to a job)"] + (
                    [f"{r['job_code']} — {r['job_name']} ({r['customer'] or ''})".strip(" ()")
                     for _, r in _jobs.iterrows()] if not _jobs.empty else []
                )
                _jid = existing.get("job_id")
                _job_label = "(New — not linked to a job)"
                if _jid is not None and not pd.isna(_jid) and not _jobs.empty:
                    _jm = _jobs[_jobs["id"] == int(_jid)]
                    if not _jm.empty:
                        r = _jm.iloc[0]
                        _lbl = f"{r['job_code']} — {r['job_name']} ({r['customer'] or ''})".strip(" ()")
                        if _lbl in _job_opts:
                            _job_label = _lbl
                st.session_state["bb_job_name"] = _job_label

                st.session_state["bb_last_loaded_bid_id"] = sel_bid_id
                st.rerun()
        else:
            sel_bid_id = None
            existing = None
            st.session_state.pop("bb_last_loaded_bid_id", None)

        st.divider()

        # ── Bid header form ───────────────────────────────────────────────
        st.markdown("##### Bid Details")

        # Customer (free text) + Rate Card (selectbox) — separate by design
        rc1, rc2 = st.columns(2)
        bid_customer = rc1.text_input(
            "Customer",
            value=str(existing["customer"] if existing is not None else ""),
            key="bb_customer",
            placeholder="Type the actual customer name…",
            help="Customer name used across Jobs, Revenue, and Tickets. "                 "Does not need to match a rate card.",
        )
        rc_names = get_customers_with_rate_cards(engine)
        existing_rc = (str(existing["rate_card"])
                       if existing is not None
                       and "rate_card" in existing.index
                       and existing["rate_card"] else "")
        if not existing_rc and bid_customer.strip() in rc_names:
            existing_rc = bid_customer.strip()
        rc_idx = rc_names.index(existing_rc) if existing_rc in rc_names else 0
        bid_rate_card = rc2.selectbox(
            "Rate Card",
            rc_names if rc_names else ["Standard"],
            index=rc_idx,
            key="bb_rate_card",
            help="Which pricing to apply. Use Standard for customers without a custom rate card.",
        )
        customer_has_rc = bid_customer.strip() in rc_names
        if bid_customer.strip() and not customer_has_rc:
            rc2.caption(f"ℹ️ No rate card for **{bid_customer.strip()}** — "                        f"using **{bid_rate_card}** pricing.")
        elif customer_has_rc and bid_rate_card != bid_customer.strip():
            rc2.caption(f"ℹ️ Using **{bid_rate_card}** pricing "                        f"(not {bid_customer.strip()}).")

        # Job Name (linked to jobs) + Status
        jn1, jn2 = st.columns([3, 1])
        all_jobs_df = query_df(engine,
            "SELECT id, job_code, job_name, customer FROM jobs ORDER BY job_code")
        job_name_options = ["(New — not linked to a job)"] + (
            [f"{r['job_code']} — {r['job_name']} ({r['customer'] or ''})".strip(" ()")
             for _, r in all_jobs_df.iterrows()]
            if not all_jobs_df.empty else []
        )
        existing_job_id = int(existing["job_id"]) if (
            existing is not None and "job_id" in existing.index
            and existing["job_id"] is not None and not pd.isna(existing["job_id"])
        ) else None
        job_sel_idx = 0
        if existing_job_id and not all_jobs_df.empty:
            job_match = all_jobs_df[all_jobs_df["id"] == existing_job_id]
            if not job_match.empty:
                r = job_match.iloc[0]
                lbl = f"{r['job_code']} — {r['job_name']} ({r['customer'] or ''})".strip(" ()")
                if lbl in job_name_options:
                    job_sel_idx = job_name_options.index(lbl)
        selected_job_label = jn1.selectbox(
            "Job Name", job_name_options, index=job_sel_idx, key="bb_job_name",
            help="Link to an existing job, or leave as New.",
        )
        linked_job_id = None
        if selected_job_label != "(New — not linked to a job)" and not all_jobs_df.empty:
            for _, r in all_jobs_df.iterrows():
                lbl = f"{r['job_code']} — {r['job_name']} ({r['customer'] or ''})".strip(" ()")
                if lbl == selected_job_label:
                    linked_job_id = int(r["id"])
                    break
        bid_status = jn2.selectbox(
            "Status", STATUS_OPTIONS,
            index=STATUS_OPTIONS.index(str(existing["status"]))
                  if existing is not None else 0,
            key="bb_status",
        )

        hf4, hf5, hf6 = st.columns(3)
        billing_type = hf4.selectbox(
            "Billing Type",
            list(BILLING_LABELS.keys()),
            format_func=lambda x: BILLING_LABELS[x],
            index=list(BILLING_LABELS.keys()).index(
                str(existing["billing_type"])
                if existing is not None else "line_item"
            ),
            key="bb_billing_type",
        )
        bid_days = hf5.number_input(
            "Bid Days", min_value=0, step=1,
            value=int(existing["bid_days"] or 0) if existing is not None else 0,
            key="bb_bid_days",
        )
        total_bbls = hf6.number_input(
            "Total BBLs (for Per BBL billing)", min_value=0.0, step=1000.0,
            value=float(existing["total_bbls"] or 0) if existing is not None else 0.0,
            key="bb_total_bbls",
        )

        # ── Crew / shift parameters ───────────────────────────────────────
        st.markdown("##### Crew & Shift Parameters")
        cf1, cf2, cf3, cf4, cf5 = st.columns(5)
        hrs_per_shift = cf1.number_input(
            "Hrs / Shift", min_value=1.0, max_value=24.0, step=0.5,
            value=float(existing["hrs_per_shift"] or 14) if existing is not None else 14.0,
            key="bb_hrs_shift",
        )
        labor_general = cf2.number_input(
            "General Labor", min_value=0, step=1,
            value=int(existing["labor_general"] or 0) if existing is not None else 0,
            key="bb_labor_gen",
        )
        labor_lead = cf3.number_input(
            "Lead Labor", min_value=0, step=1,
            value=int(existing["labor_lead"] or 0) if existing is not None else 0,
            key="bb_labor_lead",
        )
        labor_supervisor = cf4.number_input(
            "Supervisors", min_value=0, step=1,
            value=int(existing["labor_supervisor"] or 0) if existing is not None else 0,
            key="bb_labor_sup",
        )
        trucks = cf5.number_input(
            "Trucks", min_value=0, step=1,
            value=int(existing["trucks"] or 0) if existing is not None else 0,
            key="bb_trucks",
        )

        # ── Save bid header ───────────────────────────────────────────────
        if st.button("💾 Save Bid Header", key="bb_save_header"):
            if linked_job_id and not all_jobs_df.empty:
                derived_name = str(all_jobs_df[all_jobs_df["id"]==linked_job_id].iloc[0]["job_name"])
            else:
                derived_name = f"{bid_customer.strip()} Bid" if bid_customer.strip() else "New Bid"
            bid_data = {
                "id":               sel_bid_id,
                "bid_name":         derived_name,
                "customer":         bid_customer,
                "rate_card":        bid_rate_card,
                "region_code":      None,
                "billing_type":     billing_type,
                "status":           bid_status,
                "bid_days":         int(bid_days),
                "total_bbls":       float(total_bbls) if total_bbls else None,
                "hrs_per_shift":    float(hrs_per_shift),
                "labor_general":    int(labor_general),
                "labor_lead":       int(labor_lead),
                "labor_supervisor": int(labor_supervisor),
                "trucks":           int(trucks),
                "day_rate_override":None,
                "setup_override":    None,
                "demob_override":    None,
                "per_bbl_override":  None,
                "job_id":           linked_job_id,
                "notes":            None,
            }
            new_id = save_bid(engine, bid_data)
            st.success(f"Bid saved — ID #{new_id}")
            st.session_state["bb_active_bid_id"] = new_id
            st.session_state["bb_last_loaded_bid_id"] = new_id
            st.rerun()

        # ── Equipment quantities ──────────────────────────────────────────
        active_bid_id = st.session_state.get("bb_active_bid_id") or sel_bid_id
        if not active_bid_id:
            st.info("Save the bid header above to start entering equipment.")
            return

        st.divider()
        st.markdown("##### Equipment & Quantities")
        st.caption("Enter quantities for items used on this job. "
                   "Rates pull from the customer rate card — override here if needed for this bid only.")

        catalog   = get_catalog(engine)
        bid_items = get_bid_items(engine, active_bid_id)
        bid_obj   = get_bid(engine, active_bid_id)

        # Merge catalog with existing bid items
        item_qty_map = {}
        item_override_map = {}
        if not bid_items.empty:
            for _, bi in bid_items.iterrows():
                item_qty_map[int(bi["item_id"])] = float(bi["quantity"])
                item_override_map[int(bi["item_id"])] = {
                    "s": bi.get("setup_rate_override"),
                    "d": bi.get("day_rate_override"),
                    "m": bi.get("demob_rate_override"),
                }

        categories = catalog["category"].unique().tolist()
        updated_items = {}   # item_id -> {qty, overrides}

        for cat in categories:
            cat_items = catalog[catalog["category"] == cat].reset_index(drop=True)
            with st.expander(f"**{cat}**", expanded=(cat in ["Layflat", "Pump"])):
                # Header row
                h1, h2, h3, h4, h5, h6 = st.columns([3, 1.5, 1.5, 1.5, 1.5, 1])
                h1.markdown("**Item**")
                h2.markdown("**Qty**")
                h3.markdown("**Setup Override**")
                h4.markdown("**Day Rate Override**")
                h5.markdown("**Demob Override**")

                for _, item in cat_items.iterrows():
                    iid  = int(item["id"])
                    ikey = f"bb_item_{active_bid_id}_{iid}"
                    unit_label = "miles" if item["qty_source"] == "hose_ft" else item["unit"]
                    ovr  = item_override_map.get(iid, {})

                    r1, r2, r3, r4, r5, r6 = st.columns([3, 1.5, 1.5, 1.5, 1.5, 1])
                    r1.write(f"{item['name']}  *({unit_label})*")

                    qty = r2.number_input(
                        "", min_value=0.0, step=0.1 if item["qty_source"]=="hose_ft" else 1.0,
                        value=item_qty_map.get(iid, 0.0),
                        key=f"{ikey}_qty", label_visibility="collapsed",
                        format="%.2f" if item["qty_source"]=="hose_ft" else "%.0f",
                    )
                    s_ov = r3.number_input(
                        "", min_value=0.0, step=0.01,
                        value=float(ovr.get("s") or 0),
                        key=f"{ikey}_sov", label_visibility="collapsed",
                        disabled=not item["has_setup"], format="%.4f",
                    )
                    d_ov = r4.number_input(
                        "", min_value=0.0, step=0.01,
                        value=float(ovr.get("d") or 0),
                        key=f"{ikey}_dov", label_visibility="collapsed",
                        disabled=not item["has_day_rate"], format="%.4f",
                    )
                    m_ov = r5.number_input(
                        "", min_value=0.0, step=0.01,
                        value=float(ovr.get("m") or 0),
                        key=f"{ikey}_mov", label_visibility="collapsed",
                        disabled=not item["has_demob"], format="%.4f",
                    )
                    updated_items[iid] = {
                        "qty": qty,
                        "s_ov": s_ov if s_ov > 0 else None,
                        "d_ov": d_ov if d_ov > 0 else None,
                        "m_ov": m_ov if m_ov > 0 else None,
                    }

        if st.button("💾 Save Equipment", key="bb_save_equipment"):
            for iid, vals in updated_items.items():
                if vals["qty"] > 0 or iid in item_qty_map:
                    save_bid_item(engine, active_bid_id, iid, vals["qty"],
                                  vals["s_ov"], vals["d_ov"], vals["m_ov"])
            st.success("Equipment saved.")
            st.rerun()

        # ═════════════════════════════════════════════════════════════════
        # BID SUMMARY + LINE ITEMS TOGGLE
        # ═════════════════════════════════════════════════════════════════
        st.divider()
        st.markdown("##### Bid Summary")

        bid_items_calc = get_bid_items(engine, active_bid_id)
        if bid_items_calc.empty:
            st.info("No equipment entered yet.")
        else:
            # Build a working copy of bid_obj using current widget values
            # so Crew & Shift params are live without requiring a Save first
            import copy
            bid_obj_live = bid_obj.copy()
            bid_obj_live["hrs_per_shift"]    = float(st.session_state.get("bb_hrs_shift", bid_obj.get("hrs_per_shift") or 14))
            bid_obj_live["labor_general"]    = int(st.session_state.get("bb_labor_gen",  bid_obj.get("labor_general") or 0))
            bid_obj_live["labor_lead"]       = int(st.session_state.get("bb_labor_lead", bid_obj.get("labor_lead") or 0))
            bid_obj_live["labor_supervisor"] = int(st.session_state.get("bb_labor_sup",  bid_obj.get("labor_supervisor") or 0))
            bid_obj_live["trucks"]           = int(st.session_state.get("bb_trucks",     bid_obj.get("trucks") or 0))
            bid_obj_live["bid_days"]         = int(st.session_state.get("bb_bid_days",   bid_obj.get("bid_days") or 0))
            calc = calc_bid(bid_obj_live, bid_items_calc)

            # ── Price overrides — shown always, applied to all billing types ──
            st.markdown("**Price Overrides** — leave at 0 to use calculated value")
            ov1, ov2, ov3, ov4 = st.columns(4)

            ov_setup = ov1.number_input(
                "Setup Override ($)",
                min_value=0.0, step=25.0, format="%.2f",
                value=float(bid_obj.get("setup_override") or 0),
                key="bb_ov_setup",
                help=f"Calculated: {MONEY.format(calc['setup_total'])}",
            )
            ov_day = ov2.number_input(
                "Day Rate Override ($/day)",
                min_value=0.0, step=25.0, format="%.2f",
                value=float(bid_obj.get("day_rate_override") or 0),
                key="bb_ov_day",
                help=f"Calculated: {MONEY.format(calc['day_total'])}/day",
            )
            ov_demob = ov3.number_input(
                "Demob Override ($)",
                min_value=0.0, step=25.0, format="%.2f",
                value=float(bid_obj.get("demob_override") or 0),
                key="bb_ov_demob",
                help=f"Calculated: {MONEY.format(calc['demob_total'])}",
            )
            ov_per_bbl = ov4.number_input(
                "Per BBL Override ($/BBL)",
                min_value=0.0, step=0.001, format="%.5f",
                value=float(bid_obj.get("per_bbl_override") or 0),
                key="bb_ov_per_bbl",
                help="Calculated after other overrides are applied",
            )

            if st.button("💾 Save Price Overrides", key="bb_save_overrides"):
                execute(engine, """
                    UPDATE bids SET
                        setup_override   = :s,
                        day_rate_override= :d,
                        demob_override   = :m,
                        per_bbl_override = :p
                    WHERE id = :id
                """, {
                    "s": ov_setup   or None,
                    "d": ov_day     or None,
                    "m": ov_demob   or None,
                    "p": ov_per_bbl or None,
                    "id": active_bid_id,
                })
                st.success("Overrides saved.")
                st.rerun()

            # ── Effective values (override wins over calculated) ───────────
            eff_setup    = ov_setup   if ov_setup   > 0 else calc["setup_total"]
            eff_day_rate = ov_day     if ov_day     > 0 else calc["day_total"]
            eff_demob    = ov_demob   if ov_demob   > 0 else calc["demob_total"]
            eff_job_cost = eff_setup + eff_day_rate * calc["bid_days"] + eff_demob
            calc_per_bbl = eff_job_cost / calc["total_bbls"] if calc["total_bbls"] > 0 else None
            eff_per_bbl  = ov_per_bbl if ov_per_bbl > 0 else calc_per_bbl

            # ── Summary metrics ───────────────────────────────────────────
            st.markdown("**Bid Totals**")
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Setup",
                      MONEY.format(eff_setup),
                      delta=MONEY.format(eff_setup - calc["setup_total"]) if ov_setup > 0 else None)
            m2.metric("Day Rate/Day",
                      MONEY.format(eff_day_rate),
                      delta=MONEY.format(eff_day_rate - calc["day_total"]) if ov_day > 0 else None)
            m3.metric("Demob",
                      MONEY.format(eff_demob),
                      delta=MONEY.format(eff_demob - calc["demob_total"]) if ov_demob > 0 else None)
            m4.metric(f"Total ({calc['bid_days']}d)", MONEY.format(eff_job_cost))
            if eff_per_bbl:
                m5.metric("Cost / BBL",
                          f"${eff_per_bbl:.5f}",
                          delta=f"${(eff_per_bbl - calc_per_bbl):.5f}"
                          if ov_per_bbl > 0 and calc_per_bbl else None)

            # ── Line Item Toggle ──────────────────────────────────────────
            show_lines = st.toggle("🔍 Show Line Item Detail", key="bb_show_lines")
            if show_lines:
                def _lines_df(lines, label):
                    if not lines:
                        return
                    st.markdown(f"**{label}**")
                    rows = [{"Item": l["name"],
                             "Qty": f"{l['qty']:,.2f} {l['unit']}",
                             "Rate": f"${l['rate']:,.4f}",
                             "Total": MONEY.format(l["total"])}
                            for l in lines]
                    st.dataframe(pd.DataFrame(rows), hide_index=True,
                                 use_container_width=True)

                _lines_df(calc["setup_lines"],
                          f"Setup — Total: {MONEY.format(calc['setup_total'])}")
                _lines_df(calc["day_lines"],
                          f"Day Rate — {MONEY.format(calc['day_total'])}/day  "
                          f"× {calc['bid_days']} days = "
                          f"{MONEY.format(calc['day_total'] * calc['bid_days'])}")
                _lines_df(calc["demob_lines"],
                          f"Demob — Total: {MONEY.format(calc['demob_total'])}")

            # ── Convert to Job ────────────────────────────────────────────
            st.divider()
            st.markdown("##### Convert to Job")

            if str(bid_obj.get("status")) == "Won":
                wc1, wc2 = st.columns(2)
                convert_billing = wc1.selectbox(
                    "Billing type for ticket/revenue",
                    list(BILLING_LABELS.keys()),
                    format_func=lambda x: BILLING_LABELS[x],
                    index=list(BILLING_LABELS.keys()).index(billing_type),
                    key="bb_convert_billing",
                )
                existing_jobs = query_df(engine,
                    "SELECT id, job_code, job_name FROM jobs ORDER BY job_code")
                job_options = existing_jobs["id"].tolist() if not existing_jobs.empty else []

                if wc2.button("🔗 Push Line Items to Job", key="bb_push_lines"):
                    if not job_options:
                        st.warning("Create the job first in the Jobs tab.")
                    else:
                        target_job_id = st.session_state.get("bb_target_job_id")
                        if target_job_id:
                            job_lines = bid_to_job_line_items(
                                bid_obj, calc, convert_billing)
                            save_line_items(engine, target_job_id, job_lines)
                            from services.db import execute as db_execute
                            db_execute(engine,
                                "UPDATE bids SET job_id=:jid WHERE id=:bid_id",
                                {"jid": target_job_id,
                                 "bid_id": active_bid_id})
                            st.success(
                                f"✅ {len(job_lines)} line items pushed to job.")
                            st.rerun()

                target_job = st.selectbox(
                    "Target job", job_options,
                    format_func=lambda x: (
                        f"{existing_jobs.loc[existing_jobs['id']==x,'job_code'].values[0]} — "
                        f"{existing_jobs.loc[existing_jobs['id']==x,'job_name'].values[0]}"
                    ),
                    key="bb_target_job_id",
                ) if job_options else None
                st.session_state["bb_target_job_id"] = target_job

                st.caption(
                    "This will replace any existing line items on the job. "
                    "You can switch billing type here without changing the bid."
                )
            else:
                st.info("Set bid status to **Won** to convert to job line items.")


with tab_bidding:
    render_bidding_tab(engine)


# ─────────────────────────────────────────────────────────────────────────────
# UTILIZATION HISTORY TAB
# ─────────────────────────────────────────────────────────────────────────────
with tab_history:
    import datetime as _dt

    st.subheader("Utilization History")

    # Manual snapshot button
    snap_col, _ = st.columns([1, 3])
    if snap_col.button("📸 Capture Today's Snapshot", key="manual_snapshot"):
        if take_daily_snapshot(engine):
            st.success("Snapshot captured.")
        else:
            st.info("Today's snapshot already exists.")

    hist_sub1, hist_sub2, hist_sub3 = st.tabs([
        "📸 Schedule History", "✅ Actuals", "🔍 Requirements vs Billing"
    ])

    # ── Shared filters ────────────────────────────────────────────────────────
    regions_list = query_df(engine, "SELECT region_code FROM regions ORDER BY region_code")
    region_opts  = ["All"] + regions_list["region_code"].tolist() if not regions_list.empty else ["All"]

    # ═════════════════════════════════════════════════════════════════════════
    # TAB 1 — SCHEDULE HISTORY (snapshots)
    # ═════════════════════════════════════════════════════════════════════════
    with hist_sub1:
        st.caption(
            "Shows how the schedule looked on each day the app was open. "
            "Reflects the plan as it existed on that date, not adjusted actuals."
        )

        hist_df = get_utilization_history(engine)
        if hist_df.empty:
            st.info("No snapshot data yet — snapshots capture automatically each time the app loads.")
        else:
            hist_df["snapshot_date"] = pd.to_datetime(hist_df["snapshot_date"]).dt.date
            date_min = hist_df["snapshot_date"].min()
            date_max = hist_df["snapshot_date"].max()

            hf1, hf2, hf3, hf4 = st.columns(4)
            sel_region  = hf1.selectbox("Region", region_opts, key="sh_region")
            all_classes = ["All"] + sorted(hist_df["class_name"].unique().tolist())
            sel_class   = hf2.selectbox("Resource Class", all_classes, key="sh_class")
            date_from   = hf3.date_input("From", value=date_min, min_value=date_min, max_value=date_max, key="sh_from")
            date_to     = hf4.date_input("To",   value=date_max, min_value=date_min, max_value=date_max, key="sh_to")

            filt = hist_df[(hist_df["snapshot_date"] >= date_from) & (hist_df["snapshot_date"] <= date_to)].copy()
            if sel_region != "All": filt = filt[filt["region_code"] == sel_region]
            if sel_class  != "All": filt = filt[filt["class_name"]  == sel_class]

            if filt.empty:
                st.warning("No data for selected filters.")
            else:
                # Utilization % chart
                chart = filt.dropna(subset=["utilization_pct"]).copy()
                if not chart.empty:
                    color_col = "class_name" if sel_class == "All" else "region_code"
                    if sel_class == "All":
                        top = chart.groupby("class_name")["utilization_pct"].mean().nlargest(8).index
                        chart = chart[chart["class_name"].isin(top)]
                    st.markdown("**Utilization % Over Time**")
                    fig = px.line(chart, x="snapshot_date", y="utilization_pct",
                                  color=color_col, markers=True,
                                  labels={"snapshot_date":"Date","utilization_pct":"Utilization %"})
                    fig.update_layout(yaxis_ticksuffix="%", height=350,
                                      margin=dict(l=0,r=0,t=10,b=0))
                    st.plotly_chart(fig, use_container_width=True)

                # Pool vs Required
                st.markdown("**Pool Available vs Required**")
                agg = filt.groupby(["snapshot_date","region_code"]).agg(
                    pool_total=("pool_total","sum"),
                    total_required=("total_required","sum"),
                    allocated_ees=("allocated_ees","sum"),
                    allocated_rental=("allocated_rental","sum"),
                ).reset_index()
                fig2 = px.area(agg, x="snapshot_date",
                               y=["allocated_ees","allocated_rental","pool_total"],
                               color_discrete_map={"pool_total":"lightgray",
                                                   "allocated_ees":"steelblue",
                                                   "allocated_rental":"orange"},
                               labels={"snapshot_date":"Date","value":"Quantity",
                                       "variable":""},
                               height=320)
                fig2.update_layout(margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig2, use_container_width=True)

                # Raw table + download
                with st.expander("View raw data"):
                    st.dataframe(filt, hide_index=True, use_container_width=True)
                    st.download_button("⬇️ Download CSV",
                        filt.to_csv(index=False).encode(),
                        f"schedule_history_{date_from}_{date_to}.csv",
                        mime="text/csv", key="sh_download")

    # ═════════════════════════════════════════════════════════════════════════
    # TAB 2 — ACTUALS (current data, reflects any date adjustments)
    # ═════════════════════════════════════════════════════════════════════════
    with hist_sub2:
        st.caption(
            "Computed fresh from current job requirements and billing line items. "
            "Always reflects actual dates — if a job ran longer, this shows it."
        )

        af1, af2 = st.columns(2)
        sel_act_region = af1.selectbox("Region", region_opts, key="act_region")
        act_date = af2.date_input("View as of date", value=_dt.date.today(), key="act_date")

        region_filter = None if sel_act_region == "All" else sel_act_region

        req_df  = get_actuals_requirements(engine, region_filter)
        bill_df = get_actuals_billing(engine, region_filter)

        if not req_df.empty:
            req_df["required_start"] = pd.to_datetime(req_df["required_start"]).dt.date
            req_df["required_end"]   = pd.to_datetime(req_df["required_end"]).dt.date

        # Filter to active on act_date
        active_req = req_df[
            (req_df["required_start"] <= act_date) &
            (req_df["required_end"]   >= act_date)
        ].copy() if not req_df.empty else pd.DataFrame()

        active_bill = pd.DataFrame()
        if not bill_df.empty:
            bill_df["start_date"] = pd.to_datetime(bill_df["start_date"], errors="coerce").dt.date
            bill_df["end_date"]   = pd.to_datetime(bill_df["end_date"],   errors="coerce").dt.date
            # Items with dates that cover act_date, OR items with no dates (lump charges)
            mask_dated = (
                bill_df["start_date"].notna() &
                (bill_df["start_date"] <= act_date) &
                (bill_df["end_date"]   >= act_date)
            )
            mask_undated = bill_df["start_date"].isna()
            active_bill = bill_df[mask_dated | mask_undated].copy()

        st.markdown(f"**Active as of {act_date.strftime('%m/%d/%Y')}**")

        ac1, ac2, ac3 = st.columns(3)
        ac1.metric("Active Requirements", len(active_req) if not active_req.empty else 0)
        ac2.metric("Active Billing Lines", len(active_bill) if not active_bill.empty else 0)

        # Requirements breakdown
        if not active_req.empty:
            st.markdown("**Requirements (from job requirements)**")
            req_display = active_req[[
                "job_code","job_name","customer","class_name",
                "quantity_required","quantity_assigned_manual","quantity_assigned_rental",
                "required_start","required_end","allocation_status"
            ]].copy()
            req_display.columns = [
                "Job Code","Job Name","Customer","Resource",
                "Qty Required","EES Allocated","Rental Allocated",
                "Start","End","Status"
            ]
            st.dataframe(req_display, hide_index=True, use_container_width=True)

        # Billing breakdown
        if not active_bill.empty:
            st.markdown("**Billing Lines (from revenue line items)**")
            bill_display = active_bill[[
                "job_code","job_name","customer","description",
                "uom","invoice_qty","unit_price","line_total",
                "start_date","end_date"
            ]].copy()
            bill_display.columns = [
                "Job Code","Job Name","Customer","Description",
                "UOM","Qty","Unit Price","Line Total","Start","End"
            ]
            st.dataframe(bill_display, hide_index=True, use_container_width=True)

        if active_req.empty and active_bill.empty:
            st.info(f"No active requirements or billing lines on {act_date}.")

    # ═════════════════════════════════════════════════════════════════════════
    # TAB 3 — REQUIREMENTS vs BILLING (gap analysis)
    # ═════════════════════════════════════════════════════════════════════════
    with hist_sub3:
        st.caption(
            "Shows requirements and billing side by side per job. "
            "Gaps indicate items in requirements that never made it onto a ticket. "
            "Bid-linked jobs show automatic matching; manual jobs show side-by-side for eyeballing."
        )

        gf1, gf2 = st.columns(2)
        sel_gap_region = gf1.selectbox("Region", region_opts, key="gap_region")
        gap_region = None if sel_gap_region == "All" else sel_gap_region

        all_jobs = query_df(engine,
            "SELECT id, job_code, job_name, customer FROM jobs ORDER BY job_code")
        if not all_jobs.empty:
            job_opts = ["All Jobs"] + [
                f"{r['job_code']} — {r['job_name']}"
                for _, r in all_jobs.iterrows()
            ]
            sel_job_label = gf2.selectbox("Job", job_opts, key="gap_job")
            sel_job_id = None
            if sel_job_label != "All Jobs":
                sel_job_code = sel_job_label.split(" — ")[0]
                match = all_jobs[all_jobs["job_code"] == sel_job_code]
                if not match.empty:
                    sel_job_id = int(match.iloc[0]["id"])

        req_all  = get_actuals_requirements(engine, gap_region)
        bill_all = get_actuals_billing(engine, gap_region)

        if sel_job_id:
            if not req_all.empty:
                req_all = req_all[req_all["job_id"] == sel_job_id]
            if not bill_all.empty:
                bill_all = bill_all[bill_all["job_id"] == sel_job_id]

        # ── Requirements side ─────────────────────────────────────────────────
        col_req, col_bill = st.columns(2)

        with col_req:
            st.markdown("#### 📋 Requirements")
            if req_all.empty:
                st.info("No requirements.")
            else:
                for job_id, job_reqs in req_all.groupby("job_id"):
                    job_info = job_reqs.iloc[0]
                    with st.expander(
                        f"**{job_info['job_code']}** — {job_info['job_name']} "
                        f"({job_info.get('customer','') or '—'})",
                        expanded=(sel_job_id is not None)
                    ):
                        # Check if bid-linked
                        bid_check = query_df(engine,
                            "SELECT id FROM bids WHERE job_id=:jid",
                            {"jid": int(job_id)})
                        if not bid_check.empty:
                            st.caption("🔗 Bid-linked — automatic matching available")

                        for _, req in job_reqs.iterrows():
                            ees    = float(req.get("quantity_assigned_manual", 0))
                            rental = float(req.get("quantity_assigned_rental", 0))
                            total  = ees + rental
                            qty    = float(req["quantity_required"])
                            gap    = qty - total
                            color  = "🔴" if gap > 0 else "🟢"
                            st.write(
                                f"{color} **{req['class_name']}** — "
                                f"Required: {qty} {req['unit_type']} | "
                                f"EES: {ees} | Rental: {rental}"
                            )
                            if gap > 0:
                                st.caption(f"⚠️ Shortfall: {gap} {req['unit_type']}")

        with col_bill:
            st.markdown("#### 💰 Billing Lines")
            if bill_all.empty:
                st.info("No billing line items.")
            else:
                for job_id, job_bills in bill_all.groupby("job_id"):
                    job_info = job_bills.iloc[0]
                    is_bid_linked = not query_df(engine,
                        "SELECT id FROM bids WHERE job_id=:jid",
                        {"jid": int(job_id)}).empty

                    with st.expander(
                        f"**{job_info['job_code']}** — {job_info['job_name']} "
                        f"({job_info.get('customer','') or '—'})",
                        expanded=(sel_job_id is not None)
                    ):
                        if is_bid_linked:
                            st.caption("🔗 Bid-linked")

                        job_total = 0.0
                        for _, li in job_bills.iterrows():
                            lt = float(li["line_total"] or 0) if li["line_total"] is not None else 0.0
                            job_total += lt
                            date_str = ""
                            if li.get("start_date") and li.get("end_date"):
                                date_str = (f" | {li['start_date']} → {li['end_date']}")
                            elif li.get("start_date"):
                                date_str = f" | {li['start_date']}"
                            st.write(
                                f"**{li['description']}** — "
                                f"{li['invoice_qty']} {li['uom']} × "
                                f"${float(li['unit_price'] or 0):,.2f} = "
                                f"**${lt:,.2f}**{date_str}"
                            )
                        st.markdown(f"**Job Total: ${job_total:,.2f}**")

        # ── Gap summary table (for jobs with both req and billing) ────────────
        st.divider()
        st.markdown("#### Gap Summary — Items in Requirements Not Billed")
        st.caption("For bid-linked jobs this is automatic. For manual jobs, review the side-by-side above.")

        if not req_all.empty:
            # Jobs with requirements but zero billing
            billed_job_ids = set(bill_all["job_id"].tolist()) if not bill_all.empty else set()
            req_job_ids    = set(req_all["job_id"].tolist())
            unbilled_jobs  = req_job_ids - billed_job_ids

            if unbilled_jobs:
                st.warning(f"{len(unbilled_jobs)} job(s) have requirements but **no billing lines at all:**")
                for jid in unbilled_jobs:
                    job_row = all_jobs[all_jobs["id"] == jid]
                    if not job_row.empty:
                        r = job_row.iloc[0]
                        st.write(f"  • {r['job_code']} — {r['job_name']}")
            else:
                st.success("All jobs with requirements also have billing lines.")
