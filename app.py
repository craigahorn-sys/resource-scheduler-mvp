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


regions_df, resource_classes_df, jobs_df = load_lookups()


def week_start_label(ts: pd.Timestamp) -> str:
    week_end = ts + pd.Timedelta(days=6)
    if ts.month == week_end.month:
        return f"{ts.strftime('%b')} {ts.day}-{week_end.day}"
    return f"{ts.strftime('%b')} {ts.day}-{week_end.strftime('%b')} {week_end.day}"


def customer_base_color(customer: str) -> str:
    palette = [
        "#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e",
        "#17becf", "#e377c2", "#8c564b", "#bcbd22", "#7f7f7f",
    ]
    idx = abs(hash(customer)) % len(palette)
    return palette[idx]


def shade_hex(hex_color: str, factor: float) -> str:
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16) / 255.0
    g = int(hex_color[2:4], 16) / 255.0
    b = int(hex_color[4:6], 16) / 255.0
    h, l, s = colorsys.rgb_to_hls(r, g, b)
    l = max(0.28, min(0.78, l * factor))
    r2, g2, b2 = colorsys.hls_to_rgb(h, l, s)
    return f"#{int(r2*255):02x}{int(g2*255):02x}{int(b2*255):02x}"


def build_integrated_planner_data(selected_class: str | None, start_date, num_weeks: int):
    req = requirement_summary_df(engine)
    if req.empty:
        return pd.DataFrame(), pd.DataFrame(), [], [], ""

    class_options = req["class_name"].dropna().astype(str).unique().tolist()
    if not class_options:
        return pd.DataFrame(), pd.DataFrame(), [], [], ""
    if not selected_class or selected_class not in class_options:
        selected_class = class_options[0]

    req = req.loc[req["class_name"] == selected_class].copy()

    jobs_lookup = get_jobs_df(engine)
    if not jobs_lookup.empty and "customer" in jobs_lookup.columns:
        req = req.merge(jobs_lookup[["job_code", "customer"]].drop_duplicates(), on="job_code", how="left")
    else:
        req["customer"] = ""
    req["customer"] = req["customer"].fillna("").replace("", "Unassigned")

    start_ts = pd.to_datetime(start_date).normalize()
    week_starts = [start_ts + pd.Timedelta(days=7 * i) for i in range(num_weeks)]
    week_ranges = [(ts, ts + pd.Timedelta(days=6)) for ts in week_starts]
    week_labels = [week_start_label(ts) for ts in week_starts]

    rows = []
    customer_job_index = {}
    for _, row in req.sort_values(["customer", "required_start", "job_name", "job_code"]).iterrows():
        customer = str(row.get("customer", "") or "Unassigned")
        customer_job_index[customer] = customer_job_index.get(customer, -1) + 1
        rows.append(
            {
                "customer": customer,
                "job_code": row["job_code"],
                "job_name": row["job_name"],
                "label": f"{customer}, {row['job_name']}, {row['quantity_required']} {row['unit_type']}",
                "required_start": pd.to_datetime(row["required_start"]),
                "required_end": pd.to_datetime(row["required_end"]),
                "quantity_required": float(row["quantity_required"]),
                "quantity_assigned": float(row["quantity_assigned"]),
                "quantity_shortfall": float(row["quantity_shortfall"]),
                "allocation_status": row["allocation_status"],
                "unit_type": row["unit_type"],
                "customer_job_index": customer_job_index[customer],
            }
        )

    board_df = pd.DataFrame(rows)

    summary_rows = []
    for week_label, week_start, (wk_start, wk_end) in zip(week_labels, week_starts, week_ranges):
        demand = fulfilled = shortfall = 0.0
        for _, row in req.iterrows():
            overlaps = pd.to_datetime(row["required_start"]) <= wk_end and pd.to_datetime(row["required_end"]) >= wk_start
            if overlaps:
                demand += float(row["quantity_required"])
                fulfilled += float(row["quantity_assigned"])
                shortfall += float(row["quantity_shortfall"])

        snap = pool_snapshot_df(engine, as_of_date=wk_start.date())
        availability = 0.0
        if not snap.empty:
            snap_match = snap.loc[snap["class_name"] == selected_class]
            if not snap_match.empty:
                availability = float(snap_match["available_quantity"].iloc[0])

        summary_rows.extend([
            {"Metric": "Demand", "Week": week_label, "WeekStart": week_start, "Value": demand},
            {"Metric": "Fulfillment", "Week": week_label, "WeekStart": week_start, "Value": fulfilled},
            {"Metric": "Availability", "Week": week_label, "WeekStart": week_start, "Value": availability},
            {"Metric": "Shortfall", "Week": week_label, "WeekStart": week_start, "Value": shortfall},
        ])

    summary_df = pd.DataFrame(summary_rows)
    return board_df, summary_df, week_labels, week_starts, selected_class


def render_integrated_planning_board():
    st.subheader("Planning Board")
    req_all = requirement_summary_df(engine)
    if req_all.empty:
        st.info("No requirements yet.")
        return

    class_options = req_all["class_name"].dropna().astype(str).unique().tolist()
    class_options.sort()

    c1, c2, c3 = st.columns([1.6, 1, 1])
    selected_class = c1.selectbox("Resource View", class_options, key="planning_class")
    board_start = c2.date_input("Board Start", value=date.today(), key="planning_start")
    num_weeks = c3.selectbox("Weeks", [8, 10, 12, 16], index=2, key="planning_weeks")

    board_df, summary_df, week_labels, week_starts, selected_class = build_integrated_planner_data(
        selected_class=selected_class,
        start_date=board_start,
        num_weeks=num_weeks,
    )

    if board_df.empty:
        st.info("No rows for that resource view.")
        return

    summary_metrics = ["Demand", "Fulfillment", "Availability", "Shortfall"]
    total_job_rows = len(board_df)
    total_rows = total_job_rows + 1 + len(summary_metrics)

    fig = go.Figure()
    x0 = pd.to_datetime(week_starts[0])
    x_end = pd.to_datetime(week_starts[-1]) + pd.Timedelta(days=7)

    # Weekly vertical gridlines
    for ws in week_starts + [x_end]:
        fig.add_vline(x=ws, line_width=1, line_color="rgba(120,120,120,0.40)")

    # Horizontal row gridlines
    for y in [i + 0.5 for i in range(total_rows + 1)]:
        width = 2 if abs(y - (total_job_rows + 0.5)) < 1e-9 else 1
        color = "rgba(90,90,90,0.55)" if width == 2 else "rgba(160,160,160,0.28)"
        fig.add_hline(y=y, line_width=width, line_color=color)

    row_positions = []
    row_labels = []

    # Job bars
    for i, (_, row) in enumerate(board_df.iterrows()):
        y = total_job_rows - i
        row_positions.append(y)
        row_labels.append(str(row["customer"]))
        base = customer_base_color(str(row["customer"]))
        factor = 0.86 + 0.12 * (int(row["customer_job_index"]) % 4)
        fill = shade_hex(base, factor)

        start = pd.to_datetime(row["required_start"])
        finish = pd.to_datetime(row["required_end"]) + pd.Timedelta(days=1)

        fig.add_shape(
            type="rect",
            x0=start,
            x1=finish,
            y0=y - 0.34,
            y1=y + 0.34,
            line=dict(color=fill, width=1),
            fillcolor=fill,
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

    summary_y = {
        "Demand": 4,
        "Fulfillment": 3,
        "Availability": 2,
        "Shortfall": 1,
    }
    for metric, y in summary_y.items():
        row_positions.append(y)
        row_labels.append(metric)

    # Summary values inside same grid
    if not summary_df.empty:
        for _, rec in summary_df.iterrows():
            y = summary_y[rec["Metric"]]
            x = pd.to_datetime(rec["WeekStart"]) + pd.Timedelta(days=3.5)
            val = float(rec["Value"])
            txt = f"{val:.3f}" if abs(val - round(val)) > 1e-9 else f"{int(round(val))}"
            color = "#9b1c1c" if rec["Metric"] in ["Demand", "Shortfall"] else "#1b4f9b"
            fig.add_annotation(
                x=x,
                y=y,
                text=txt,
                showarrow=False,
                font=dict(size=12, color=color),
                xanchor="center",
                yanchor="middle",
            )

    # Invisible trace to force axes
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
        tickvals=week_starts,
        ticktext=week_labels,
        side="top",
        showgrid=False,
        range=[x0, x_end],
        tickfont=dict(size=11),
    )
    fig.update_yaxes(
        tickmode="array",
        tickvals=row_positions,
        ticktext=row_labels,
        range=[0.5, total_rows + 0.5],
        showgrid=False,
        zeroline=False,
        tickfont=dict(size=12),
    )
    fig.update_layout(
        height=max(520, 120 + total_rows * 50),
        margin=dict(l=30, r=20, t=30, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    st.plotly_chart(fig, width="stretch")


tab_jobs, tab_requirements, tab_pools, tab_allocations, tab_planning, tab_calendar, tab_gantt = st.tabs(
    ["Jobs", "Requirements", "Pools", "Allocations", "Planning Board", "Calendar", "Gantt"]
)

with tab_jobs:
    st.subheader("Create Job")
    with st.form("create_job_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            job_name = st.text_input("Job Name")
            region_code = st.selectbox(
                "Region",
                regions_df["region_code"].tolist(),
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

    st.subheader("Jobs")
    jobs_df = get_jobs_df(engine)
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
            use_container_width=True,
        )

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

    st.subheader("Requirement Summary")
    req_summary = requirement_summary_df(engine)
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
                        "unit_type",
                        "required_start",
                        "required_end",
                        "quantity_assigned",
                        "quantity_shortfall",
                        "allocation_status",
                    ]
                ]
            ),
            use_container_width=True,
        )

with tab_pools:
    st.subheader("Resource Pools")
    rc_display = resource_classes_df.assign(
        display=resource_classes_df["category"] + " | " + resource_classes_df["class_name"]
    )

    with st.form("upsert_pool_form"):
        c1, c2, c3 = st.columns(3)
        region_code = c1.selectbox(
            "Region",
            regions_df["region_code"].tolist(),
            key="pool_region",
        )
        selected_rc_display = c2.selectbox(
            "Resource Class",
            rc_display["display"].tolist(),
            key="pool_rc",
        )
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

                st.write("Submitted values:")
                st.json(
                    {
                        "region_code": region_code,
                        "resource_class_id": int(selected_rc["id"]),
                        "class_name": str(selected_rc["class_name"]),
                        "base_quantity": submitted_qty,
                        "unit_type": str(selected_rc["unit_type"]),
                        "notes": notes,
                    }
                )

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

                verify_df = query_df(
                    engine,
                    """
                    SELECT
                        rp.id,
                        rp.region_code,
                        rc.class_name,
                        rp.base_quantity,
                        rp.notes
                    FROM resource_pools rp
                    JOIN resource_classes rc
                      ON rp.resource_class_id = rc.id
                    WHERE rp.region_code = :region_code
                      AND rp.resource_class_id = :resource_class_id
                    """,
                    {
                        "region_code": region_code,
                        "resource_class_id": int(selected_rc["id"]),
                    },
                )

                st.write("Saved row check:")
                st.dataframe(verify_df, use_container_width=True)

            except Exception as e:
                st.error(f"Pool save failed: {e}")

    st.subheader("Pool Adjustment Log")
    with st.form("pool_adjustment_form", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns(4)
        adj_region = c1.selectbox("Region", regions_df["region_code"].tolist(), key="adj_region")
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

    as_of_date = st.date_input("Pool Snapshot As Of", value=date.today(), key="snapshot_date")
    st.caption(f"Selected: {as_of_date.strftime('%m/%d/%Y')}")
    snapshot = pool_snapshot_df(engine, as_of_date=as_of_date)
    if snapshot.empty:
        st.info("No pools set up yet.")
    else:
        st.dataframe(format_dates_for_display(snapshot), use_container_width=True)
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
    req_summary = requirement_summary_df(engine)
    ful = get_fulfillment_df(engine)

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
            use_container_width=True,
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
            use_container_width=True,
        )


with tab_planning:
    render_integrated_planning_board()

with tab_calendar:
    st.subheader("Calendar")
    mode = st.radio("View Mode", ["Demand", "Fulfillment", "Availability"], horizontal=True)
    as_of_date = st.date_input("Calendar As Of", value=date.today(), key="calendar_date")
    st.caption(f"Selected: {as_of_date.strftime('%m/%d/%Y')}")

    if mode == "Availability":
        snapshot = pool_snapshot_df(engine, as_of_date=as_of_date)
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
                use_container_width=True,
            )

    elif mode == "Demand":
        req_summary = requirement_summary_df(engine)
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
            st.plotly_chart(fig, use_container_width=True)

    else:
        ful = get_fulfillment_df(engine)
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
            st.plotly_chart(fig, use_container_width=True)

with tab_gantt:
    st.subheader("Job Gantt")
    req_summary = requirement_summary_df(engine)
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
        st.plotly_chart(fig, use_container_width=True)
