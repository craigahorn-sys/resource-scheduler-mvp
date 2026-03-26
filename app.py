
import streamlit as st
from services.db import get_engine, init_db, query_df
from services.scheduler import create_job, create_requirement, upsert_pool, recalc_all_requirements

engine = get_engine()
init_db(engine)

st.sidebar.header("Region")
region = st.sidebar.selectbox("Region", ["Global","RM","PM","ST"])

st.title("Scheduler")

st.header("Pools")
qty = st.number_input("Qty",0.0)
if st.button("Save Pool"):
    upsert_pool(engine,"RM",1,qty)

st.header("Jobs")
job = st.text_input("Job Name")
if st.button("Create Job"):
    create_job(engine,{
        "job_name":job,
        "region_code":"RM",
        "job_start_date":"2026-01-01",
        "job_duration_days":5,
        "mob_days_before_job":1,
        "demob_days_after_job":1,
        "status":"Planned"
    })

st.header("Requirements")
if st.button("Add Req"):
    create_requirement(engine,{
        "job_id":1,
        "resource_class_id":1,
        "quantity_required":5,
        "days_before_job_start":0,
        "days_after_job_end":0,
        "priority":"Normal"
    })

st.header("Data")
st.dataframe(query_df(engine,"SELECT * FROM requirement_fulfillment"))
