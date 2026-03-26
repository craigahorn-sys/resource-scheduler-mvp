
from __future__ import annotations
from datetime import date
import pandas as pd
import plotly.express as px
import streamlit as st

from services.db import export_excel, get_engine, init_db, query_df
from services.models import calc_job_dates
from services.scheduler import (
    add_pool_adjustment, allocation_debug_df, create_job, create_requirement, delete_job,
    delete_pool, delete_pool_adjustment, delete_requirement, get_fulfillment_df,
    get_jobs_df, get_pools_df, pool_snapshot_df, recalc_all_requirements,
    requirement_summary_df, update_job, update_requirement, upsert_pool,
)

st.set_page_config(page_title="Resource Scheduler V2", layout="wide")
st.title("Resource Scheduler V2")
st.caption("Priority-based scheduling with region-scoped workflow and pop-up row management.")

engine = get_engine()
init_db(engine)

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

def resource_options_df() -> pd.DataFrame:
    rc = resource_classes_df.copy()
    rc["display"] = rc["class_name"]
    return rc

def render_jobs_manage_table(df: pd.DataFrame, active_region: str):
    st.markdown("##### Manage Jobs")
    if df.empty:
        st.info("No jobs yet.")
        return

    show_region = active_region == "Global"
    widths = [1.0, 1.1, 1.2, 1.4, 1.0, 1.0, 1.0, 1.0, 0.8] if show_region else [1.1, 1.2, 1.4, 1.0, 1.0, 1.0, 1.0, 0.8]
    headers = ["Region", "Job Code", "Customer", "Job Name", "Mob Start", "Job Start", "Job End", "Demob End", "Manage"] if show_region else ["Job Code", "Customer", "Job Name", "Mob Start", "Job Start", "Job End", "Demob End", "Manage"]

    hdr = st.columns(widths)
    for c, h in zip(hdr, headers):
        c.markdown(f"**{h}**")

    region_codes = regions_df["region_code"].tolist()

    for _, row in df.iterrows():
        cols = st.columns(widths)
        col_idx = 0
        if show_region:
            cols[col_idx].write(region_format(str(row["region_code"])))
            col_idx += 1
        cols[col_idx].write(str(row["job_code"])); col_idx += 1
        cols[col_idx].write(str(row.get("customer", "") or "")); col_idx += 1
        cols[col_idx].write(str(row["job_name"])); col_idx += 1
        cols[col_idx].write(format_date_value(row["mob_start_date"])); col_idx += 1
        cols[col_idx].write(format_date_value(row["job_start_date"])); col_idx += 1
        cols[col_idx].write(format_date_value(row["job_end_date"])); col_idx += 1
        cols[col_idx].write(format_date_value(row["demob_end_date"])); col_idx += 1

        with cols[col_idx].popover("Edit/Delete", use_container_width=True):
            region_idx = region_codes.index(row["region_code"])
            edit_customer = st.text_input("Customer", value=str(row.get("customer", "") or ""), key=f"job_customer_{row['id']}")
            edit_region = st.selectbox("Region", region_codes, index=region_idx, format_func=region_format, disabled=region_disabled(active_region), key=f"job_region_{row['id']}")
            edit_job_name = st.text_input("Job Name", value=str(row["job_name"]), key=f"job_name_{row['id']}")
            edit_location = st.text_input("Location", value=str(row.get("location", "") or ""), key=f"job_loc_{row['id']}")
            edit_start = st.date_input("Job Start Date", value=pd.to_datetime(row["job_start_date"]).date(), key=f"job_start_{row['id']}")
            edit_duration = st.number_input("Job Duration (days)", min_value=1, value=int(row["job_duration_days"]), step=1, key=f"job_duration_{row['id']}")
            edit_mob = st.number_input("Mobilization Days Before Job", min_value=0, value=int(row["mob_days_before_job"]), step=1, key=f"job_mob_{row['id']}")
            edit_demob = st.number_input("Demobilization Days After Job", min_value=0, value=int(row["demob_days_after_job"]), step=1, key=f"job_demob_{row['id']}")
            statuses = ["Planned", "Tentative", "Active", "Billing Pending", "Complete", "Cancelled"]
            status_index = statuses.index(row["status"]) if row["status"] in statuses else 0
            edit_status = st.selectbox("Status", statuses, index=status_index, key=f"job_status_{row['id']}")
            edit_notes = st.text_area("Notes", value=str(row.get("notes", "") or ""), key=f"job_notes_{row['id']}")
            a, b = st.columns(2)
            if a.button("Save", key=f"save_job_{row['id']}"):
                update_job(engine, int(row["id"]), {
                    "job_name": edit_job_name,
                    "region_code": active_region_value(active_region, edit_region),
                    "customer": edit_customer,
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

def render_requirements_manage_table(df: pd.DataFrame):
    st.markdown("##### Manage Requirements")
    if df.empty:
        st.info("No requirements yet.")
        return
    hdr = st.columns([1.0, 1.3, 1.0, 0.8, 1.0, 0.9, 0.8])
    for c, h in zip(hdr, ["Job Code", "Class", "Region", "Qty", "Assigned", "Status", "Manage"]):
        c.markdown(f"**{h}**")
    rc_df = resource_options_df()
    for _, row in df.iterrows():
        cols = st.columns([1.0, 1.3, 1.0, 0.8, 1.0, 0.9, 0.8])
        cols[0].write(str(row["job_code"]))
        cols[1].write(str(row["class_name"]))
        cols[2].write(region_format(str(row["region_code"])))
        cols[3].write(str(row["quantity_required"]))
        cols[4].write(str(row["quantity_assigned"]))
        cols[5].write(str(row["allocation_status"]))
        rc_match = rc_df.loc[rc_df["class_name"] == row["class_name"]].iloc[0]
        with cols[6].popover("Edit/Delete", use_container_width=True):
            current_idx = rc_df["class_name"].tolist().index(row["class_name"])
            edit_rc_display = st.selectbox("Resource Class", rc_df["display"].tolist(), index=current_idx, key=f"req_class_{row['id']}")
            edit_rc = rc_df.loc[rc_df["display"] == edit_rc_display].iloc[0]
            step = quantity_step(str(edit_rc["unit_type"]), str(edit_rc["category"]))
            fmt = quantity_format(str(edit_rc["unit_type"]), str(edit_rc["category"]))
            edit_qty = st.number_input("Quantity Required", min_value=0.0, value=float(row["quantity_required"]), step=step, format=fmt, key=f"req_qty_{row['id']}")
            priorities = ["Low", "Normal", "High", "Critical"]
            p_index = priorities.index(row["priority"]) if row["priority"] in priorities else 1
            edit_priority = st.selectbox("Priority", priorities, index=p_index, key=f"req_priority_{row['id']}")
            edit_before = st.number_input("Days Before Job Start", min_value=0, value=int(row["days_before_job_start"]), step=1, key=f"req_before_{row['id']}")
            edit_after = st.number_input("Days After Job End", min_value=0, value=int(row["days_after_job_end"]), step=1, key=f"req_after_{row['id']}")
            edit_notes = st.text_area("Notes", value=str(row.get("notes", "") or ""), key=f"req_notes_{row['id']}")
            a, b = st.columns(2)
            if a.button("Save", key=f"save_req_{row['id']}"):
                update_requirement(engine, int(row["id"]), {
                    "resource_class_id": int(edit_rc["id"]),
                    "quantity_required": float(edit_qty),
                    "days_before_job_start": int(edit_before),
                    "days_after_job_end": int(edit_after),
                    "priority": edit_priority,
                    "notes": edit_notes,
                })
                st.rerun()
            if b.button("Delete", key=f"delete_req_{row['id']}"):
                delete_requirement(engine, int(row["id"]))
                st.rerun()

def render_pools_manage_table(df: pd.DataFrame, active_region: str):
    st.markdown("##### Manage Pools")
    if df.empty:
        st.info("No pool rows yet.")
        return
    hdr = st.columns([1.0, 1.3, 0.8, 1.0, 0.8])
    for c, h in zip(hdr, ["Region", "Class", "Pool", "Status", "Manage"]):
        c.markdown(f"**{h}**")
    region_codes = regions_df["region_code"].tolist()
    base_pool_df = get_pools_df(engine)
    rc_df = resource_options_df()
    for _, row in df.iterrows():
        cols = st.columns([1.0, 1.3, 0.8, 1.0, 0.8])
        cols[0].write(region_format(str(row["region_code"])))
        cols[1].write(str(row["class_name"]))
        cols[2].write(str(row["total_pool"]))
        cols[3].write(str(row["pool_status"]))
        rc_match = rc_df.loc[rc_df["class_name"] == row["class_name"]].iloc[0]
        step = quantity_step(str(rc_match["unit_type"]), str(rc_match["category"]))
        fmt = quantity_format(str(rc_match["unit_type"]), str(rc_match["category"]))
        with cols[4].popover("Edit/Delete", use_container_width=True):
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


def week_start_label(ts: pd.Timestamp) -> str:
    week_end = ts + pd.Timedelta(days=6)
    if ts.month == week_end.month:
        return f"{ts.strftime('%b')} {ts.day}-{week_end.day}"
    return f"{ts.strftime('%b')} {ts.day}-{week_end.strftime('%b')} {week_end.day}"


def build_planning_board_data(active_region: str, selected_class: str | None, start_date, num_weeks: int):
    req = region_filter(requirement_summary_df(engine), active_region)
    ful = region_filter(get_fulfillment_df(engine), active_region)
    snapshot = region_filter(pool_snapshot_df(engine, as_of_date=start_date), active_region)

    if req.empty:
        return pd.DataFrame(), pd.DataFrame(), [], ""

    class_options = req["class_name"].dropna().astype(str).unique().tolist()
    if not class_options:
        return pd.DataFrame(), pd.DataFrame(), [], ""
    if not selected_class or selected_class not in class_options:
        selected_class = class_options[0]

    req = req.loc[req["class_name"] == selected_class].copy()
    ful = ful.loc[ful["class_name"] == selected_class].copy() if not ful.empty else ful

    start_ts = pd.to_datetime(start_date).normalize()
    week_starts = [start_ts + pd.Timedelta(days=7 * i) for i in range(num_weeks)]
    week_ranges = [(ts, ts + pd.Timedelta(days=6)) for ts in week_starts]
    week_labels = [week_start_label(ts) for ts in week_starts]

    if req.empty:
        return pd.DataFrame(), pd.DataFrame(), week_labels, selected_class

    board_rows = []
    for _, row in req.sort_values(["job_name", "required_start", "job_code"]).iterrows():
        label = f"{row['job_code']} | {row['job_name']} | {row['quantity_required']} {row['unit_type']}"
        board_row = {
            "job_code": row["job_code"],
            "job_name": row["job_name"],
            "label": label,
            "required_start": row["required_start"],
            "required_end": row["required_end"],
            "quantity_required": float(row["quantity_required"]),
            "quantity_assigned": float(row["quantity_assigned"]),
            "quantity_shortfall": float(row["quantity_shortfall"]),
            "allocation_status": row["allocation_status"],
        }
        for week_label, (wk_start, wk_end) in zip(week_labels, week_ranges):
            overlaps = pd.to_datetime(row["required_start"]) <= wk_end and pd.to_datetime(row["required_end"]) >= wk_start
            board_row[week_label] = label if overlaps else ""
        board_rows.append(board_row)

    board_df = pd.DataFrame(board_rows)

    summary_rows = []
    for week_label, (wk_start, wk_end) in zip(week_labels, week_ranges):
        demand = 0.0
        fulfilled = 0.0
        shortfall = 0.0
        for _, row in req.iterrows():
            overlaps = pd.to_datetime(row["required_start"]) <= wk_end and pd.to_datetime(row["required_end"]) >= wk_start
            if overlaps:
                demand += float(row["quantity_required"])
                fulfilled += float(row["quantity_assigned"])
                shortfall += float(row["quantity_shortfall"])

        availability = 0.0
        if not snapshot.empty:
            snap_match = snapshot.loc[snapshot["class_name"] == selected_class]
            if not snap_match.empty:
                availability = float(snap_match["available_quantity"].iloc[0])

        summary_rows.append({"Metric": "Demand", "Week": week_label, "Value": demand})
        summary_rows.append({"Metric": "Fulfillment", "Week": week_label, "Value": fulfilled})
        summary_rows.append({"Metric": "Availability", "Week": week_label, "Value": availability})
        summary_rows.append({"Metric": "Shortfall", "Week": week_label, "Value": shortfall})

    summary_df = pd.DataFrame(summary_rows)
    summary_pivot = summary_df.pivot(index="Metric", columns="Week", values="Value").reset_index() if not summary_df.empty else pd.DataFrame()

    return board_df, summary_pivot, week_labels, selected_class


def render_planning_board(active_region: str):
    st.subheader("Planning Board")
    req_all = region_filter(requirement_summary_df(engine), active_region)
    if req_all.empty:
        st.info("No requirements yet.")
        return

    class_options = req_all["class_name"].dropna().astype(str).unique().tolist()
    class_options.sort()

    c1, c2, c3 = st.columns([1.5, 1, 1])
    selected_class = c1.selectbox("Resource View", class_options, key="planning_class")
    board_start = c2.date_input("Board Start", value=date.today(), key="planning_start")
    num_weeks = c3.selectbox("Weeks", [8, 10, 12, 16], index=2, key="planning_weeks")

    board_df, summary_df, week_labels, selected_class = build_planning_board_data(
        active_region=active_region,
        selected_class=selected_class,
        start_date=board_start,
        num_weeks=num_weeks,
    )

    if board_df.empty:
        st.info("No rows for that resource view.")
        return

    board_display = board_df[["label"] + week_labels].copy()
    board_display = board_display.rename(columns={"label": "Job / Demand"})
    st.dataframe(board_display, width="stretch", hide_index=True)

    st.markdown("##### Weekly Summary")
    if not summary_df.empty:
        summary_display = summary_df.copy()
        for col in summary_display.columns:
            if col != "Metric":
                summary_display[col] = summary_display[col].map(
                    lambda x: "" if pd.isna(x) else (f"{float(x):.3f}" if abs(float(x) - round(float(x))) > 1e-9 else f"{int(round(float(x)))}")
                )
        st.dataframe(summary_display, width="stretch", hide_index=True)

    st.markdown("##### Timeline")
    chart_rows = []
    for _, row in board_df.iterrows():
        chart_rows.append(
            {
                "Task": row["label"],
                "Start": pd.to_datetime(row["required_start"]),
                "Finish": pd.to_datetime(row["required_end"]),
                "Status": row["allocation_status"],
            }
        )
    chart_df = pd.DataFrame(chart_rows)
    if not chart_df.empty:
        fig = px.timeline(chart_df, x_start="Start", x_end="Finish", y="Task", color="Status")
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, width="stretch")

with st.sidebar:
    st.header("Workspace")
    region_options = ["Global"] + regions_df["region_code"].tolist()
    ACTIVE_REGION = st.selectbox("Active Region", region_options, format_func=lambda x: "Global" if x == "Global" else region_format(x))
    st.caption("Showing all regions" if ACTIVE_REGION == "Global" else region_format(ACTIVE_REGION))
    if st.button("Rebalance All Allocations", type="primary"):
        recalc_all_requirements(engine)
        st.success("Rebalanced")
        st.rerun()

tab_jobs, tab_requirements, tab_pools, tab_allocations, tab_planning, tab_calendar, tab_gantt = st.tabs(
    ["Jobs", "Requirements", "Pools", "Allocations", "Planning Board", "Calendar", "Gantt"]
)

with tab_jobs:
    st.subheader("Create Job")
    with st.form("create_job_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        region_list = regions_df["region_code"].tolist()
        default_region_index = region_default_index(region_list, ACTIVE_REGION)
        with c1:
            customer = st.text_input("Customer")
            region_code = st.selectbox("Region", region_list, index=default_region_index, format_func=region_format, disabled=region_disabled(ACTIVE_REGION))
            location = st.text_input("Location")
        with c2:
            job_name = st.text_input("Job Name")
            job_start_date = st.date_input("Job Start Date", key="create_job_start_date")
            st.caption(f"Selected: {job_start_date.strftime('%m/%d/%Y')}")
            job_duration_days = st.number_input("Job Duration (days)", min_value=1, value=7, step=1)
        with c3:
            mob_days_before_job = st.number_input("Mobilization Days Before Job", min_value=0, value=3, step=1)
            demob_days_after_job = st.number_input("Demobilization Days After Job", min_value=0, value=2, step=1)
            status = st.selectbox("Status", ["Planned", "Tentative", "Active", "Billing Pending", "Complete", "Cancelled"])
        notes = st.text_area("Notes")
        dates_preview = calc_job_dates(job_start_date, int(job_duration_days), int(mob_days_before_job), int(demob_days_after_job))
        st.info(f"Job End: {format_date_value(dates_preview['job_end_date'])}  |  Mob Start: {format_date_value(dates_preview['mob_start_date'])}  |  Demob End: {format_date_value(dates_preview['demob_end_date'])}")
        if st.form_submit_button("Create Job") and job_name:
            create_job(engine, {
                "job_name": job_name,
                "region_code": active_region_value(ACTIVE_REGION, region_code),
                "customer": customer,
                "location": location,
                "job_start_date": job_start_date,
                "job_duration_days": int(job_duration_days),
                "mob_days_before_job": int(mob_days_before_job),
                "demob_days_after_job": int(demob_days_after_job),
                "status": status,
                "notes": notes,
            })
            st.success("Created job.")
            st.rerun()

    jobs_df = filter_active_jobs_for_management(region_filter(get_jobs_df(engine), ACTIVE_REGION))
    render_jobs_manage_table(jobs_df, ACTIVE_REGION)

with tab_requirements:
    st.subheader("Add Requirement")
    jobs_df = region_filter(get_jobs_df(engine), ACTIVE_REGION)
    if jobs_df.empty:
        st.warning("Create a job first.")
    else:
        job_options = jobs_df.assign(display=jobs_df["job_code"] + " | " + jobs_df["job_name"])
        rc_display = resource_options_df()
        with st.form("create_requirement_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            selected_job_display = c1.selectbox("Job", job_options["display"].tolist())
            selected_job = job_options.loc[job_options["display"] == selected_job_display].iloc[0]
            selected_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist())
            selected_rc = rc_display.loc[rc_display["display"] == selected_rc_display].iloc[0]
            step = quantity_step(str(selected_rc["unit_type"]), str(selected_rc["category"]))
            fmt = quantity_format(str(selected_rc["unit_type"]), str(selected_rc["category"]))
            quantity_required = c3.number_input(f"Quantity Required ({selected_rc['unit_type']})", min_value=0.0, value=step, step=step, format=fmt)
            c4, c5, c6 = st.columns(3)
            days_before_job_start = c4.number_input("Days Before Job Start", min_value=0, value=0, step=1)
            days_after_job_end = c5.number_input("Days After Job End", min_value=0, value=0, step=1)
            priority = c6.selectbox("Priority", ["Low","Normal","High","Critical"], index=1)
            req_notes = st.text_area("Notes")
            req_start = pd.to_datetime(selected_job["job_start_date"]).date() - pd.Timedelta(days=int(days_before_job_start))
            req_end = pd.to_datetime(selected_job["job_end_date"]).date() + pd.Timedelta(days=int(days_after_job_end))
            st.info(f"Requirement Window: {format_date_value(req_start)} to {format_date_value(req_end)}")
            if st.form_submit_button("Add Requirement"):
                create_requirement(engine, {
                    "job_id": int(selected_job["id"]),
                    "resource_class_id": int(selected_rc["id"]),
                    "quantity_required": float(quantity_required),
                    "days_before_job_start": int(days_before_job_start),
                    "days_after_job_end": int(days_after_job_end),
                    "priority": priority,
                    "notes": req_notes,
                })
                st.success("Requirement added.")
                st.rerun()

    st.subheader("Requirement Summary")
    req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        display_req = format_dates_for_display(req_summary[["job_code","job_name","region_code","class_name","quantity_required","unit_type","required_start","required_end","quantity_assigned","quantity_shortfall","allocation_status"]])
        display_req["region_code"] = display_req["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_req, width="stretch")
        render_requirements_manage_table(req_summary)

with tab_pools:
    st.subheader("Resource Pools")
    rc_display = resource_options_df()
    region_codes = regions_df["region_code"].tolist()
    default_region_index = region_default_index(region_codes, ACTIVE_REGION)
    with st.form("upsert_pool_form"):
        c1, c2, c3 = st.columns(3)
        region_code = c1.selectbox("Region", region_codes, index=default_region_index, format_func=region_format, disabled=region_disabled(ACTIVE_REGION), key="pool_region")
        selected_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist(), key="pool_rc")
        selected_rc = rc_display.loc[rc_display["display"] == selected_rc_display].iloc[0]
        step = quantity_step(str(selected_rc["unit_type"]), str(selected_rc["category"]))
        fmt = quantity_format(str(selected_rc["unit_type"]), str(selected_rc["category"]))
        base_quantity = c3.number_input(f"Base Quantity ({selected_rc['unit_type']})", min_value=0.0, value=0.0, step=step, format=fmt, key="pool_base_quantity")
        notes = st.text_input("Notes", key="pool_notes")
        if st.form_submit_button("Save Pool Quantity"):
            upsert_pool(engine, active_region_value(ACTIVE_REGION, region_code), int(selected_rc["id"]), float(base_quantity), notes)
            st.success("Pool saved.")
            st.rerun()

    st.subheader("Pool Adjustment Log")
    with st.form("pool_adjustment_form", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns(4)
        adj_region = c1.selectbox("Region", region_codes, index=default_region_index, format_func=region_format, disabled=region_disabled(ACTIVE_REGION), key="adj_region")
        adj_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist(), key="adj_rc")
        adj_rc = rc_display.loc[rc_display["display"] == adj_rc_display].iloc[0]
        step = quantity_step(str(adj_rc["unit_type"]), str(adj_rc["category"]))
        fmt = quantity_format(str(adj_rc["unit_type"]), str(adj_rc["category"]))
        qty_change = c3.number_input(f"Quantity Change ({adj_rc['unit_type']})", value=0.0, step=step, format=fmt)
        adjustment_date = c4.date_input("Adjustment Date", value=date.today())
        st.caption(f"Selected: {adjustment_date.strftime('%m/%d/%Y')}")
        c5, c6 = st.columns(2)
        reason = c5.selectbox("Reason", ["Purchase","Transfer In","Transfer Out","Retirement","Damage/Loss","Correction"])
        adj_notes = c6.text_input("Notes", key="adj_notes")
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
    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        display_req = format_dates_for_display(req_summary[["job_code","job_name","region_code","class_name","quantity_required","quantity_assigned","quantity_shortfall","allocation_status"]])
        display_req["region_code"] = display_req["region_code"].map(lambda x: region_format(str(x)))
        st.dataframe(display_req, width="stretch")
    st.subheader("Fulfillment Rows")
    if ful.empty:
        st.info("No fulfillment rows yet.")
    else:
        display_ful = format_dates_for_display(ful[["job_code","job_name","region_code","class_name","fulfillment_type","source_name","specific_resource_name","quantity_assigned","required_start","required_end"]])
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


with tab_planning:
    render_planning_board(ACTIVE_REGION)

with tab_calendar:
    st.subheader("Calendar")
    mode = st.radio("View Mode", ["Demand","Fulfillment","Availability"], horizontal=True)
    as_of_date = st.date_input("Calendar As Of", value=date.today(), key="calendar_date")
    st.caption(f"Selected: {as_of_date.strftime('%m/%d/%Y')}")
    if mode == "Availability":
        snapshot = region_filter(pool_snapshot_df(engine, as_of_date=as_of_date), ACTIVE_REGION)
        if snapshot.empty:
            st.info("No pool data available.")
        else:
            display_snapshot = format_dates_for_display(snapshot.copy())
            display_snapshot["region_code"] = display_snapshot["region_code"].map(lambda x: region_format(str(x)))
            st.dataframe(display_snapshot[["region_code","class_name","unit_type","total_pool","committed_quantity","available_quantity","pool_status"]], width="stretch")
    elif mode == "Demand":
        req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
        if req_summary.empty:
            st.info("No requirement data.")
        else:
            req_summary = req_summary.copy()
            req_summary["start"] = pd.to_datetime(req_summary["required_start"])
            req_summary["finish"] = pd.to_datetime(req_summary["required_end"])
            fig = px.timeline(req_summary, x_start="start", x_end="finish", y="job_code", color="class_name", hover_data=["job_name","region_code","quantity_required","unit_type","allocation_status"])
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, width="stretch")
    else:
        ful = region_filter(get_fulfillment_df(engine), ACTIVE_REGION)
        if ful.empty:
            st.info("No fulfillment data.")
        else:
            ful = ful.copy()
            ful["start"] = pd.to_datetime(ful["required_start"])
            ful["finish"] = pd.to_datetime(ful["required_end"])
            fig = px.timeline(ful, x_start="start", x_end="finish", y="job_code", color="fulfillment_type", hover_data=["job_name","region_code","class_name","quantity_assigned","unit_type","source_name"])
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, width="stretch")

with tab_gantt:
    st.subheader("Job Gantt")
    req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        job_choices = req_summary[["job_code","job_name"]].drop_duplicates().sort_values(["job_code"])
        job_display = job_choices["job_code"] + " | " + job_choices["job_name"]
        selected = st.selectbox("Select Job", job_display.tolist())
        selected_code = selected.split(" | ")[0]
        job_df = req_summary.loc[req_summary["job_code"] == selected_code].copy()
        job_df["start"] = pd.to_datetime(job_df["required_start"])
        job_df["finish"] = pd.to_datetime(job_df["required_end"])
        job_df["task"] = job_df["class_name"] + " (" + job_df["quantity_required"].astype(str) + " " + job_df["unit_type"] + ")"
        fig = px.timeline(job_df, x_start="start", x_end="finish", y="task", color="allocation_status", hover_data=["quantity_required","quantity_assigned","quantity_shortfall","priority"])
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, width="stretch")
