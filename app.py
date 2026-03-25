from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

from services.db import export_excel, get_engine, init_db, query_df
from services.models import calc_job_dates
from services.scheduler import (
    add_pool_adjustment,
    create_job,
    create_requirement,
    get_fulfillment_df,
    get_jobs_df,
    pool_snapshot_df,
    requirement_summary_df,
    upsert_pool,
)

st.set_page_config(page_title="Resource Scheduler V2", layout="wide")
st.title("Resource Scheduler V2")
st.caption("PostgreSQL-ready Streamlit MVP with duration-based scheduling and auto-allocation from internal pools.")

engine = get_engine()
init_db(engine)


def load_lookups():
    regions = query_df(engine, "SELECT region_code, region_name FROM regions WHERE active = 1 ORDER BY region_code")
    resource_classes = query_df(engine, "SELECT id, class_name, category, unit_type, planning_mode FROM resource_classes ORDER BY category, class_name")
    jobs = get_jobs_df(engine)
    return regions, resource_classes, jobs


regions_df, resource_classes_df, jobs_df = load_lookups()

tab_jobs, tab_requirements, tab_pools, tab_allocations, tab_calendar, tab_gantt = st.tabs([
    "Jobs", "Requirements", "Pools", "Allocations", "Calendar", "Gantt"
])

with tab_jobs:
    st.subheader("Create Job")
    with st.form("create_job_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            job_name = st.text_input("Job Name")
            region_code = st.selectbox("Region", regions_df["region_code"].tolist(), format_func=lambda x: f"{x} - {regions_df.loc[regions_df.region_code.eq(x), 'region_name'].iloc[0]}")
            customer = st.text_input("Customer")
        with c2:
            location = st.text_input("Location")
            job_start_date = st.date_input("Job Start Date", value=date.today())
            job_duration_days = st.number_input("Job Duration (days)", min_value=1, value=7)
        with c3:
            mob_days_before_job = st.number_input("Mobilization Days Before Job", min_value=0, value=3)
            demob_days_after_job = st.number_input("Demobilization Days After Job", min_value=0, value=2)
            status = st.selectbox("Status", ["Planned", "Tentative", "Active", "Complete", "Cancelled"])
        notes = st.text_area("Notes")
        dates_preview = calc_job_dates(job_start_date, int(job_duration_days), int(mob_days_before_job), int(demob_days_after_job))
        st.info(
            f"Job End: {dates_preview['job_end_date']}  |  Mob Start: {dates_preview['mob_start_date']}  |  Demob End: {dates_preview['demob_end_date']}"
        )
        submitted = st.form_submit_button("Create Job")
        if submitted and job_name:
            job_id = create_job(engine, {
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
            })
            st.success(f"Created job #{job_id}.")
            st.rerun()

    st.subheader("Jobs")
    jobs_df = get_jobs_df(engine)
    st.dataframe(jobs_df[["job_code", "job_name", "region_code", "customer", "location", "job_start_date", "job_end_date", "mob_start_date", "demob_end_date", "status"]], use_container_width=True)

with tab_requirements:
    st.subheader("Add Requirement")
    jobs_df = get_jobs_df(engine)
    if jobs_df.empty:
        st.warning("Create a job first.")
    else:
        job_options = jobs_df.assign(display=jobs_df["job_code"] + " | " + jobs_df["job_name"])
        with st.form("create_requirement_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            selected_job_display = c1.selectbox("Job", job_options["display"].tolist())
            selected_job = job_options.loc[job_options["display"] == selected_job_display].iloc[0]
            rc_display = resource_classes_df.assign(display=resource_classes_df["category"] + " | " + resource_classes_df["class_name"])
            selected_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist())
            selected_rc = rc_display.loc[rc_display["display"] == selected_rc_display].iloc[0]
            quantity_required = c3.number_input(f"Quantity Required ({selected_rc['unit_type']})", min_value=0.0, value=1.0, step=0.5)
            c4, c5, c6 = st.columns(3)
            days_before_job_start = c4.number_input("Days Before Job Start", min_value=0, value=0)
            days_after_job_end = c5.number_input("Days After Job End", min_value=0, value=0)
            priority = c6.selectbox("Priority", ["Low", "Normal", "High", "Critical"], index=1)
            req_notes = st.text_area("Notes")
            req_start = pd.to_datetime(selected_job["job_start_date"]).date() - pd.Timedelta(days=int(days_before_job_start))
            req_end = pd.to_datetime(selected_job["job_end_date"]).date() + pd.Timedelta(days=int(days_after_job_end))
            st.info(f"Requirement Window: {req_start.date() if hasattr(req_start,'date') else req_start} to {req_end.date() if hasattr(req_end,'date') else req_end}")
            submitted = st.form_submit_button("Add Requirement")
            if submitted:
                create_requirement(engine, {
                    "job_id": int(selected_job["id"]),
                    "resource_class_id": int(selected_rc["id"]),
                    "quantity_required": float(quantity_required),
                    "days_before_job_start": int(days_before_job_start),
                    "days_after_job_end": int(days_after_job_end),
                    "priority": priority,
                    "notes": req_notes,
                })
                st.success("Requirement added and auto-allocation applied.")
                st.rerun()

    st.subheader("Requirement Summary")
    req_summary = requirement_summary_df(engine)
    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        st.dataframe(req_summary[[
            "job_code", "job_name", "region_code", "class_name", "quantity_required", "unit_type",
            "required_start", "required_end", "quantity_assigned", "quantity_shortfall", "allocation_status"
        ]], use_container_width=True)

with tab_pools:
    st.subheader("Resource Pools")
    with st.form("upsert_pool_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        region_code = c1.selectbox("Region", regions_df["region_code"].tolist(), key="pool_region")
        rc_display = resource_classes_df.assign(display=resource_classes_df["category"] + " | " + resource_classes_df["class_name"])
        selected_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist(), key="pool_rc")
        selected_rc = rc_display.loc[rc_display["display"] == selected_rc_display].iloc[0]
        base_quantity = c3.number_input(f"Base Quantity ({selected_rc['unit_type']})", value=0.0, step=0.5)
        notes = st.text_input("Notes", key="pool_notes")
        submitted = st.form_submit_button("Save Pool Quantity")
        if submitted:
            upsert_pool(engine, region_code, int(selected_rc["id"]), float(base_quantity), notes)
            st.success("Pool saved.")
            st.rerun()

    st.subheader("Pool Adjustment Log")
    with st.form("pool_adjustment_form", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns(4)
        adj_region = c1.selectbox("Region", regions_df["region_code"].tolist(), key="adj_region")
        adj_rc_display = c2.selectbox("Resource Class", rc_display["display"].tolist(), key="adj_rc")
        adj_rc = rc_display.loc[rc_display["display"] == adj_rc_display].iloc[0]
        qty_change = c3.number_input(f"Quantity Change ({adj_rc['unit_type']})", value=0.0, step=0.5)
        adjustment_date = c4.date_input("Adjustment Date", value=date.today())
        c5, c6 = st.columns(2)
        reason = c5.selectbox("Reason", ["Purchase", "Transfer In", "Transfer Out", "Retirement", "Damage/Loss", "Correction"])
        adj_notes = c6.text_input("Notes", key="adj_notes")
        submitted = st.form_submit_button("Add Adjustment")
        if submitted:
            add_pool_adjustment(engine, {
                "region_code": adj_region,
                "resource_class_id": int(adj_rc["id"]),
                "quantity_change": float(qty_change),
                "adjustment_date": adjustment_date,
                "reason": reason,
                "notes": adj_notes,
            })
            st.success("Adjustment added.")
            st.rerun()

    as_of_date = st.date_input("Pool Snapshot As Of", value=date.today(), key="snapshot_date")
    snapshot = pool_snapshot_df(engine, as_of_date=as_of_date)
    if snapshot.empty:
        st.info("No pools set up yet.")
    else:
        st.dataframe(snapshot, use_container_width=True)
        csv_data = snapshot.to_csv(index=False).encode("utf-8")
        excel_data = export_excel({"Pool Snapshot": snapshot})
        c1, c2 = st.columns(2)
        c1.download_button("Download Pool Snapshot CSV", data=csv_data, file_name=f"pool_snapshot_{as_of_date}.csv", mime="text/csv")
        c2.download_button("Download Pool Snapshot Excel", data=excel_data, file_name=f"pool_snapshot_{as_of_date}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with tab_allocations:
    st.subheader("Allocations")
    req_summary = requirement_summary_df(engine)
    ful = get_fulfillment_df(engine)
    if req_summary.empty:
        st.info("No requirements yet.")
    else:
        st.dataframe(req_summary[[
            "job_code", "job_name", "region_code", "class_name", "quantity_required", "quantity_assigned", "quantity_shortfall", "allocation_status", "required_start", "required_end"
        ]], use_container_width=True)
        st.markdown("**Fulfillment Rows**")
        if ful.empty:
            st.caption("No fulfillment rows yet.")
        else:
            st.dataframe(ful[["job_code", "job_name", "region_code", "class_name", "fulfillment_type", "source_name", "specific_resource_name", "quantity_assigned", "required_start", "required_end"]], use_container_width=True)

with tab_calendar:
    st.subheader("Calendar")
    req_summary = requirement_summary_df(engine)
    if req_summary.empty:
        st.info("No requirements to display.")
    else:
        view_mode = st.radio("View", ["Demand", "Fulfillment", "Availability"], horizontal=True)
        if view_mode == "Demand":
            plot_df = req_summary.copy()
            plot_df["label"] = plot_df["job_code"] + " | " + plot_df["class_name"] + " | Req: " + plot_df["quantity_required"].astype(str)
            fig = px.timeline(plot_df, x_start="required_start", x_end="required_end", y="label", color="region_code", hover_data=["job_name", "unit_type", "allocation_status"])
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)
        elif view_mode == "Fulfillment":
            ful = get_fulfillment_df(engine)
            if ful.empty:
                st.info("No fulfillment rows to display.")
            else:
                ful["label"] = ful["job_code"] + " | " + ful["class_name"] + " | " + ful["fulfillment_type"]
                fig = px.timeline(ful, x_start="required_start", x_end="required_end", y="label", color="region_code", hover_data=["source_name", "quantity_assigned", "unit_type"])
                fig.update_yaxes(autorange="reversed")
                st.plotly_chart(fig, use_container_width=True)
        else:
            as_of_date = st.date_input("Availability As Of", value=date.today(), key="calendar_availability")
            snapshot = pool_snapshot_df(engine, as_of_date=as_of_date)
            st.dataframe(snapshot, use_container_width=True)

with tab_gantt:
    st.subheader("Job Gantt")
    jobs_df = get_jobs_df(engine)
    req_summary = requirement_summary_df(engine)
    if jobs_df.empty or req_summary.empty:
        st.info("Create jobs and requirements first.")
    else:
        selected_job_code = st.selectbox("Select Job", jobs_df["job_code"].tolist())
        gdf = req_summary[req_summary["job_code"] == selected_job_code].copy()
        if gdf.empty:
            st.info("No requirements for this job.")
        else:
            gdf["label"] = gdf["class_name"] + " | Req: " + gdf["quantity_required"].astype(str) + " | Assigned: " + gdf["quantity_assigned"].astype(str)
            fig = px.timeline(gdf, x_start="required_start", x_end="required_end", y="label", color="allocation_status", hover_data=["quantity_shortfall", "unit_type", "priority"])
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(fig, use_container_width=True)
