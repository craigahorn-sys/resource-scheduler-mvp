
import streamlit as st
from services.db import get_engine, init_db, query_df, execute
from services.scheduler import *

engine=get_engine()
init_db(engine)

regions=["Global","RM","PM","ST"]

with st.sidebar:
    region=st.selectbox("Region",regions)

def filt(df):
    if region=="Global": return df
    if "region_code" not in df.columns: return df
    return df[df.region_code==region]

st.title("Scheduler")

# Jobs
st.header("Jobs")
name=st.text_input("Job name")
if st.button("Create Job"):
    create_job(engine,name,"RM")
    st.rerun()

jobs=filt(query_df(engine,"SELECT * FROM jobs"))
st.dataframe(jobs)

if not jobs.empty:
    jid=st.selectbox("Delete Job",jobs.id)
    if st.button("Delete Job"):
        delete_job(engine,jid)
        st.rerun()

# Pools
st.header("Pools")
qty=st.number_input("Pool Qty",0.0)
if st.button("Save Pool"):
    upsert_pool(engine,"RM",1,qty)
    st.rerun()

# Requirements
st.header("Requirements")
if st.button("Add Requirement"):
    create_requirement(engine,1,1,5)
    st.rerun()

req=query_df(engine,"SELECT * FROM job_requirements")
st.dataframe(req)

if not req.empty:
    rid=st.selectbox("Delete Requirement",req.id)
    if st.button("Delete Requirement"):
        delete_requirement(engine,rid)
        st.rerun()

# Allocation
st.header("Allocation")
st.dataframe(query_df(engine,"SELECT * FROM requirement_fulfillment"))
