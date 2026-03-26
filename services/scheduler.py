
from .db import execute, query_df

def recalc_all_requirements(engine):
    execute(engine, "DELETE FROM requirement_fulfillment")
    req = query_df(engine, "SELECT * FROM job_requirements")
    pools = query_df(engine, "SELECT * FROM resource_pools")
    for _, r in req.iterrows():
        pool = pools[(pools.resource_class_id==r.resource_class_id)]
        if pool.empty:
            continue
        available = float(pool.iloc[0].base_quantity)
        assign = min(available, float(r.quantity_required))
        if assign>0:
            execute(engine,
                "INSERT INTO requirement_fulfillment(requirement_id, quantity_assigned) VALUES (:r,:q)",
                {"r": int(r.id), "q": assign}
            )

def create_job(engine,data):
    execute(engine,"INSERT INTO jobs(job_name,region_code,job_start_date,job_duration_days,mob_days_before_job,demob_days_after_job,status) VALUES (:job_name,:region_code,:job_start_date,:job_duration_days,:mob_days_before_job,:demob_days_after_job,:status)",data)

def create_requirement(engine,data):
    execute(engine,"INSERT INTO job_requirements(job_id,resource_class_id,quantity_required,days_before_job_start,days_after_job_end,priority) VALUES (:job_id,:resource_class_id,:quantity_required,:days_before_job_start,:days_after_job_end,:priority)",data)
    recalc_all_requirements(engine)

def upsert_pool(engine,region,rc,qty):
    execute(engine,"DELETE FROM resource_pools WHERE region_code=:r AND resource_class_id=:rc",{"r":region,"rc":rc})
    execute(engine,"INSERT INTO resource_pools(region_code,resource_class_id,base_quantity) VALUES (:r,:rc,:q)",{"r":region,"rc":rc,"q":qty})
    recalc_all_requirements(engine)
