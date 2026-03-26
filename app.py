from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

from services.db import export_excel, get_engine, init_db, query_df
from services.models import calc_job_dates
from services.scheduler import (
    add_pool_adjustment,
    allocation_debug_df,
    create_job,
    create_requirement,
    delete_job,
    delete_pool,
    delete_pool_adjustment,
    delete_requirement,
    get_fulfillment_df,
    get_jobs_df,
    get_pool_adjustments_df,
    get_pools_df,
    get_requirements_df,
    pool_snapshot_df,
    recalc_all_requirements,
    requirement_summary_df,
    update_job,
    update_requirement,
    upsert_pool,
)

st.set_page_config(page_title="Resource Scheduler V2", layout="wide")
st.title("Resource Scheduler V2")
st.caption("PostgreSQL-ready Streamlit MVP with duration-based scheduling and auto-allocation from internal pools.")

engine = get_engine()
init_db(engine)


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
    resource_classes = query_df(
        engine,
        "SELECT id, class_name, category, unit_type, planning_mode FROM resource_classes ORDER BY category, class_name",
    )
    jobs = get_jobs_df(engine)
    return regions, resource_classes, jobs


def region_filter(df: pd.DataFrame, active_region: str) -> pd.DataFrame:
    if df.empty or active_region == "Global" or "region_code" not in df.columns:
        return df
    return df.loc[df["region_code"] == active_region].copy()


regions_df, resource_classes_df, jobs_df = load_lookups()

with st.sidebar:
    st.header("Workspace")
    region_options = ["Global"] + regions_df["region_code"].tolist()
    ACTIVE_REGION = st.selectbox("Active Region", region_options)
    if ACTIVE_REGION == "Global":
        st.caption("Showing all regions")
    else:
        region_name = regions_df.loc[regions_df["region_code"] == ACTIVE_REGION, "region_name"].iloc[0]
        st.caption(f"{ACTIVE_REGION} - {region_name}")
    if st.button("Rebalance All Allocations", type="primary"):
        recalc_all_requirements(engine)
        st.success("Rebalanced")
        st.rerun()


tab_jobs, tab_requirements, tab_pools, tab_allocations, tab_calendar, tab_gantt = st.tabs(
    ["Jobs", "Requirements", "Pools", "Allocations", "Calendar", "Gantt"]
)

with tab_jobs:
    st.subheader("Create Job")
    with st.form("create_job_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        region_list = regions_df["region_code"].tolist()
        default_region_index = 0
        if ACTIVE_REGION != "Global" and ACTIVE_REGION in region_list:
            default_region_index = region_list.index(ACTIVE_REGION)
        with c1:
            job_name = st.text_input("Job Name")
            region_code = st.selectbox(
                "Region",
                region_list,
                index=default_region_index,
                format_func=lambda x: f"{x} - {regions_df.loc[regions_df.region_code.eq(x), 'region_name'].iloc[0]}",
            )
            customer = st.text_input("Customer")
        with c2:
            location = st.text_input("Location")
            job_start_date = st.date_input("Job Start Date", value=date.today())
            st.caption(f"Selected: {job_start_date.strftime('%m/%d/%Y')}")
            job_duration_days = st.number_input("Job Duration (days)", min_value=1, value=7)
        with c3:
            mob_days_before_job = st.number_input("Mobilization Days Before Job", min_value=0, value=3)
            demob_days_after_job = st.number_input("Demobilization Days After Job", min_value=0, value=2)
            status = st.selectbox("Status", ["Planned", "Tentative", "Active", "Complete", "Cancelled"])
        notes = st.text_area("Notes")
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
        submitted = st.form_submit_button("Create Job")
        if submitted and job_name:
            try:
                job_id = create_job(
                    engine,
                    {
                        "job_name": job_name,
                        "region_code": region_code,
                        "customer": customer,
                        "location": location,
                        "job_start_date": job_start_date,
                        "job_duration_days": int(job_duration_days),
                        "mob_days_before_job": int(mob_days_before_job),
                        "demob_days_after_job": int(demob_days_after_job),
                        "status": status,
                        "notes": notes,
                    },
                )
                st.success(f"Created job #{job_id}.")
                st.rerun()
            except Exception as e:
                st.error(f"Job creation failed: {e}")

    st.subheader("Edit / Delete Job")
    jobs_df = region_filter(get_jobs_df(engine), ACTIVE_REGION)
    if jobs_df.empty:
        st.info("No jobs yet.")
    else:
        job_options = jobs_df.assign(display=jobs_df["job_code"] + " | " + jobs_df["job_name"])
        selected_job_display = st.selectbox("Select Job", job_options["display"].tolist(), key="edit_job_select")
        selected_job = job_options.loc[job_options["display"] == selected_job_display].iloc[0]
        with st.form("edit_job_form"):
            c1, c2, c3 = st.columns(3)
            edit_region_codes = regions_df["region_code"].tolist()
            edit_region_index = edit_region_codes.index(selected_job["region_code"])
            with c1:
                edit_job_name = st.text_input("Job Name", value=str(selected_job["job_name"]))
                edit_region_code = st.selectbox(
                    "Region",
                    edit_region_codes,
                    index=edit_region_index,
                    format_func=lambda x: f"{x} - {regions_df.loc[regions_df.region_code.eq(x), 'region_name'].iloc[0]}",
                    key="edit_region_code",
                )
                edit_customer = st.text_input("Customer", value=str(selected_job.get("customer", "") or ""))
            with c2:
                edit_location = st.text_input("Location", value=str(selected_job.get("location", "") or ""))
                edit_start_date = st.date_input("Job Start Date", value=pd.to_datetime(selected_job["job_start_date"]).date(), key="edit_start")
                edit_duration = st.number_input("Job Duration (days)", min_value=1, value=int(selected_job["job_duration_days"]))
            with c3:
                edit_mob = st.number_input("Mobilization Days Before Job", min_value=0, value=int(selected_job["mob_days_before_job"]))
                edit_demob = st.number_input("Demobilization Days After Job", min_value=0, value=int(selected_job["demob_days_after_job"]))
                statuses = ["Planned", "Tentative", "Active", "Complete", "Cancelled"]
                status_index = statuses.index(selected_job["status"]) if selected_job["status"] in statuses else 0
                edit_status = st.selectbox("Status", statuses, index=status_index)
            edit_notes = st.text_area("Notes", value=str(selected_job.get("notes", "") or ""))
            s1, s2 = st.columns(2)
            save_clicked = s1.form_submit_button("Save Job Changes")
            delete_clicked = s2.form_submit_button("Delete Job")
            if save_clicked:
                try:
                    update_job(
                        engine,
                        int(selected_job["id"]),
                        {
                            "job_name": edit_job_name,
                            "region_code": edit_region_code,
                            "customer": edit_customer,
                            "location": edit_location,
                            "job_start_date": edit_start_date,
                            "job_duration_days": int(edit_duration),
                            "mob_days_before_job": int(edit_mob),
                            "demob_days_after_job": int(edit_demob),
                            "status": edit_status,
                            "notes": edit_notes,
                        },
                    )
                    st.success("Job updated.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Job update failed: {e}")
            if delete_clicked:
                try:
                    delete_job(engine, int(selected_job["id"]))
                    st.success("Job deleted.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Job delete failed: {e}")

    st.subheader("Jobs")
    jobs_df = region_filter(get_jobs_df(engine), ACTIVE_REGION)
    if jobs_df.empty:
        st.info("No jobs yet.")
    else:
        st.dataframe(
            format_dates_for_display(
                jobs_df[
                    [
                        "job_code",
                        "job_name",
                        "region_code",
                        "customer",
                        "location",
                        "job_start_date",
                        "job_end_date",
                        "mob_start_date",
                        "demob_end_date",
                        "status",
                    ]
                ]
            ),
            width="stretch",
        )

with tab_requirements:
    st.subheader("Add Requirement")
    jobs_df = region_filter(get_jobs_df(engine), ACTIVE_REGION)
    if jobs_df.empty:
        st.warning("Create a job first.")
    else:
        job_options = jobs_df.assign(display=jobs_df["job_code"] + " | " + jobs_df["job_name"])
        with st.form("create_requirement_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            selected_job_display = c1.selectbox("Job", job_options["display"].tolist())
            selected_job = job_options.loc[job_options["display"] == selected_job_display].iloc[0]

            rc_display = resource_classes_df.assign(
                display=resource_classes_df["category"] + " | " + resource_classes_df["class_name"]
            )
            selected_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist())
            selected_rc = rc_display.loc[rc_display["display"] == selected_rc_display].iloc[0]

            quantity_required = c3.number_input(
                f"Quantity Required ({selected_rc['unit_type']})",
                min_value=0.0,
                value=1.0,
                step=0.5,
            )

            c4, c5, c6 = st.columns(3)
            days_before_job_start = c4.number_input("Days Before Job Start", min_value=0, value=0)
            days_after_job_end = c5.number_input("Days After Job End", min_value=0, value=0)
            priority = c6.selectbox("Priority", ["Low", "Normal", "High", "Critical"], index=1)

            req_notes = st.text_area("Notes")

            req_start = pd.to_datetime(selected_job["job_start_date"]).date() - pd.Timedelta(days=int(days_before_job_start))
            req_end = pd.to_datetime(selected_job["job_end_date"]).date() + pd.Timedelta(days=int(days_after_job_end))
            st.info(f"Requirement Window: {format_date_value(req_start)} to {format_date_value(req_end)}")

            submitted = st.form_submit_button("Add Requirement")
            if submitted:
                try:
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
                    st.success("Requirement added and auto-allocation applied.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Requirement save failed: {e}")

    st.subheader("Edit / Delete Requirement")
    req_edit_df = region_filter(get_requirements_df(engine), ACTIVE_REGION)
    if req_edit_df.empty:
        st.info("No requirements yet.")
    else:
        req_options = req_edit_df.assign(display=req_edit_df["job_code"] + " | " + req_edit_df["class_name"] + " | ID " + req_edit_df["id"].astype(str))
        selected_req_display = st.selectbox("Select Requirement", req_options["display"].tolist(), key="edit_req_select")
        selected_req = req_options.loc[req_options["display"] == selected_req_display].iloc[0]
        rc_display = resource_classes_df.assign(display=resource_classes_df["category"] + " | " + resource_classes_df["class_name"])
        current_rc_display = rc_display.loc[rc_display["class_name"] == selected_req["class_name"], "display"].iloc[0]
        current_rc_index = rc_display["display"].tolist().index(current_rc_display)

        with st.form("edit_req_form"):
            c1, c2, c3 = st.columns(3)
            edit_rc_display = c1.selectbox("Resource Class", rc_display["display"].tolist(), index=current_rc_index)
            edit_rc = rc_display.loc[rc_display["display"] == edit_rc_display].iloc[0]
            edit_qty = c2.number_input("Quantity Required", min_value=0.0, value=float(selected_req["quantity_required"]), step=0.5)
            priorities = ["Low", "Normal", "High", "Critical"]
            p_index = priorities.index(selected_req["priority"]) if selected_req["priority"] in priorities else 1
            edit_priority = c3.selectbox("Priority", priorities, index=p_index)
            c4, c5 = st.columns(2)
            edit_before = c4.number_input("Days Before Job Start", min_value=0, value=int(selected_req["days_before_job_start"]))
            edit_after = c5.number_input("Days After Job End", min_value=0, value=int(selected_req["days_after_job_end"]))
            edit_notes = st.text_area("Notes", value=str(selected_req.get("notes", "") or ""))
            s1, s2 = st.columns(2)
            save_req = s1.form_submit_button("Save Requirement Changes")
            delete_req = s2.form_submit_button("Delete Requirement")
            if save_req:
                try:
                    update_requirement(
                        engine,
                        int(selected_req["id"]),
                        {
                            "resource_class_id": int(edit_rc["id"]),
                            "quantity_required": float(edit_qty),
                            "days_before_job_start": int(edit_before),
                            "days_after_job_end": int(edit_after),
                            "priority": edit_priority,
                            "notes": edit_notes,
                        },
                    )
                    st.success("Requirement updated.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Requirement update failed: {e}")
            if delete_req:
                try:
                    delete_requirement(engine, int(selected_req["id"]))
                    st.success("Requirement deleted.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Requirement delete failed: {e}")

    st.subheader("Requirement Summary")
    req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        st.dataframe(
            format_dates_for_display(
                req_summary[
                    [
                        "id",
                        "job_code",
                        "job_name",
                        "region_code",
                        "class_name",
                        "quantity_required",
                        "unit_type",
                        "required_start",
                        "required_end",
                        "quantity_assigned",
                        "quantity_shortfall",
                        "allocation_status",
                    ]
                ]
            ),
            width="stretch",
        )

with tab_pools:
    st.subheader("Resource Pools")
    rc_display = resource_classes_df.assign(
        display=resource_classes_df["category"] + " | " + resource_classes_df["class_name"]
    )

    with st.form("upsert_pool_form"):
        c1, c2, c3 = st.columns(3)
        region_codes = regions_df["region_code"].tolist()
        default_region_index = 0
        if ACTIVE_REGION != "Global" and ACTIVE_REGION in region_codes:
            default_region_index = region_codes.index(ACTIVE_REGION)
        region_code = c1.selectbox("Region", region_codes, index=default_region_index, key="pool_region")
        selected_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist(), key="pool_rc")
        selected_rc = rc_display.loc[rc_display["display"] == selected_rc_display].iloc[0]

        base_quantity = c3.number_input(
            f"Base Quantity ({selected_rc['unit_type']})",
            min_value=0.0,
            value=0.0,
            step=0.5,
            format="%.2f",
            key="pool_base_quantity",
        )

        notes = st.text_input("Notes", key="pool_notes")
        submitted = st.form_submit_button("Save Pool Quantity")

        if submitted:
            try:
                submitted_qty = float(base_quantity)
                upsert_pool(
                    engine,
                    region_code,
                    int(selected_rc["id"]),
                    submitted_qty,
                    notes,
                )
                st.success(
                    f"Pool saved: {region_code} / {selected_rc['class_name']} / {submitted_qty} {selected_rc['unit_type']}"
                )
                st.rerun()
            except Exception as e:
                st.error(f"Pool save failed: {e}")

    st.subheader("Edit / Delete Pool")
    pool_edit_df = region_filter(get_pools_df(engine), ACTIVE_REGION)
    if pool_edit_df.empty:
        st.info("No pool rows yet.")
    else:
        pool_options = pool_edit_df.assign(display=pool_edit_df["region_code"] + " | " + pool_edit_df["class_name"])
        selected_pool_display = st.selectbox("Select Pool", pool_options["display"].tolist(), key="edit_pool_select")
        selected_pool = pool_options.loc[pool_options["display"] == selected_pool_display].iloc[0]
        with st.form("edit_pool_form"):
            edit_pool_qty = st.number_input(
                f"Base Quantity ({selected_pool['unit_type']})",
                min_value=0.0,
                value=float(selected_pool["base_quantity"]),
                step=0.5,
                format="%.2f",
            )
            edit_pool_notes = st.text_input("Notes", value=str(selected_pool.get("notes", "") or ""))
            s1, s2 = st.columns(2)
            save_pool = s1.form_submit_button("Save Pool Changes")
            delete_pool_btn = s2.form_submit_button("Delete Pool")
            if save_pool:
                try:
                    upsert_pool(
                        engine,
                        str(selected_pool["region_code"]),
                        int(selected_pool["resource_class_id"]),
                        float(edit_pool_qty),
                        edit_pool_notes,
                    )
                    st.success("Pool updated.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Pool update failed: {e}")
            if delete_pool_btn:
                try:
                    delete_pool(engine, int(selected_pool["id"]))
                    st.success("Pool deleted.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Pool delete failed: {e}")

    st.subheader("Pool Adjustment Log")
    with st.form("pool_adjustment_form", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns(4)
        adj_region = c1.selectbox("Region", regions_df["region_code"].tolist(), index=default_region_index, key="adj_region")
        adj_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist(), key="adj_rc")
        adj_rc = rc_display.loc[rc_display["display"] == adj_rc_display].iloc[0]
        qty_change = c3.number_input(f"Quantity Change ({adj_rc['unit_type']})", value=0.0, step=0.5)
        adjustment_date = c4.date_input("Adjustment Date", value=date.today())
        st.caption(f"Selected: {adjustment_date.strftime('%m/%d/%Y')}")

        c5, c6 = st.columns(2)
        reason = c5.selectbox(
            "Reason",
            ["Purchase", "Transfer In", "Transfer Out", "Retirement", "Damage/Loss", "Correction"],
        )
        adj_notes = c6.text_input("Notes", key="adj_notes")
        submitted = st.form_submit_button("Add Adjustment")

        if submitted:
            try:
                add_pool_adjustment(
                    engine,
                    {
                        "region_code": adj_region,
                        "resource_class_id": int(adj_rc["id"]),
                        "quantity_change": float(qty_change),
                        "adjustment_date": adjustment_date,
                        "reason": reason,
                        "notes": adj_notes,
                    },
                )
                st.success("Adjustment added.")
                st.rerun()
            except Exception as e:
                st.error(f"Adjustment save failed: {e}")

    st.subheader("Delete Pool Adjustment")
    adj_df = region_filter(get_pool_adjustments_df(engine), ACTIVE_REGION)
    if adj_df.empty:
        st.info("No pool adjustments yet.")
    else:
        adj_options = adj_df.assign(display=adj_df["region_code"] + " | " + adj_df["class_name"] + " | ID " + adj_df["id"].astype(str))
        selected_adj_display = st.selectbox("Select Adjustment", adj_options["display"].tolist(), key="adj_select")
        selected_adj = adj_options.loc[adj_options["display"] == selected_adj_display].iloc[0]
        if st.button("Delete Selected Adjustment"):
            try:
                delete_pool_adjustment(engine, int(selected_adj["id"]))
                st.success("Adjustment deleted.")
                st.rerun()
            except Exception as e:
                st.error(f"Adjustment delete failed: {e}")

    as_of_date = st.date_input("Pool Snapshot As Of", value=date.today(), key="snapshot_date")
    snapshot = region_filter(pool_snapshot_df(engine, as_of_date=as_of_date), ACTIVE_REGION)
    if snapshot.empty:
        st.info("No pools set up yet.")
    else:
        st.dataframe(format_dates_for_display(snapshot), width="stretch")
        snapshot_display = format_dates_for_display(snapshot)
        csv_data = snapshot_display.to_csv(index=False).encode("utf-8")
        excel_data = export_excel({"Pool Snapshot": snapshot_display})
        c1, c2 = st.columns(2)
        c1.download_button(
            "Download Pool Snapshot CSV",
            data=csv_data,
            file_name=f"pool_snapshot_{as_of_date}.csv",
            mime="text/csv",
        )
        c2.download_button(
            "Download Pool Snapshot Excel",
            data=excel_data,
            file_name=f"pool_snapshot_{as_of_date}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

with tab_allocations:
    st.subheader("Allocations")
    req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
    ful = region_filter(get_fulfillment_df(engine), ACTIVE_REGION)

    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        st.dataframe(
            format_dates_for_display(
                req_summary[
                    [
                        "job_code",
                        "job_name",
                        "region_code",
                        "class_name",
                        "quantity_required",
                        "quantity_assigned",
                        "quantity_shortfall",
                        "allocation_status",
                    ]
                ]
            ),
            width="stretch",
        )

    st.subheader("Fulfillment Rows")
    if ful.empty:
        st.info("No fulfillment rows yet.")
    else:
        st.dataframe(
            format_dates_for_display(
                ful[
                    [
                        "job_code",
                        "job_name",
                        "region_code",
                        "class_name",
                        "fulfillment_type",
                        "source_name",
                        "specific_resource_name",
                        "quantity_assigned",
                        "required_start",
                        "required_end",
                    ]
                ]
            ),
            width="stretch",
        )

    st.subheader("Allocation Debug")
    dbg = region_filter(allocation_debug_df(engine), ACTIVE_REGION)
    if dbg.empty:
        st.info("No allocation debug data yet.")
    else:
        st.dataframe(format_dates_for_display(dbg), width="stretch")

with tab_calendar:
    st.subheader("Calendar")
    mode = st.radio("View Mode", ["Demand", "Fulfillment", "Availability"], horizontal=True)
    as_of_date = st.date_input("Calendar As Of", value=date.today(), key="calendar_date")
    st.caption(f"Selected: {as_of_date.strftime('%m/%d/%Y')}")

    if mode == "Availability":
        snapshot = region_filter(pool_snapshot_df(engine, as_of_date=as_of_date), ACTIVE_REGION)
        if snapshot.empty:
            st.info("No pool data available.")
        else:
            st.dataframe(
                format_dates_for_display(
                    snapshot[
                        [
                            "region_code",
                            "class_name",
                            "unit_type",
                            "total_pool",
                            "committed_quantity",
                            "available_quantity",
                            "pool_status",
                        ]
                    ]
                ),
                width="stretch",
            )

    elif mode == "Demand":
        req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
        if req_summary.empty:
            st.info("No requirement data.")
        else:
            req_summary = req_summary.copy()
            req_summary["start"] = pd.to_datetime(req_summary["required_start"])
            req_summary["finish"] = pd.to_datetime(req_summary["required_end"])
            fig = px.timeline(
                req_summary,
                x_start="start",
                x_end="finish",
                y="job_code",
                color="class_name",
                hover_data=["job_name", "region_code", "quantity_required", "unit_type", "allocation_status"],
            )
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
            fig = px.timeline(
                ful,
                x_start="start",
                x_end="finish",
                y="job_code",
                color="fulfillment_type",
                hover_data=["job_name", "region_code", "class_name", "quantity_assigned", "unit_type", "source_name"],
            )
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, width="stretch")

with tab_gantt:
    st.subheader("Job Gantt")
    req_summary = region_filter(requirement_summary_df(engine), ACTIVE_REGION)
    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        job_choices = req_summary[["job_code", "job_name"]].drop_duplicates().sort_values(["job_code"])
        job_display = job_choices["job_code"] + " | " + job_choices["job_name"]
        selected = st.selectbox("Select Job", job_display.tolist())
        selected_code = selected.split(" | ")[0]

        job_df = req_summary.loc[req_summary["job_code"] == selected_code].copy()
        job_df["start"] = pd.to_datetime(job_df["required_start"])
        job_df["finish"] = pd.to_datetime(job_df["required_end"])
        job_df["task"] = job_df["class_name"] + " (" + job_df["quantity_required"].astype(str) + " " + job_df["unit_type"] + ")"

        fig = px.timeline(
            job_df,
            x_start="start",
            x_end="finish",
            y="task",
            color="allocation_status",
            hover_data=["quantity_required", "quantity_assigned", "quantity_shortfall", "priority"],
        )
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, width="stretch")
