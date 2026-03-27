from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from .db import execute, query_df
from .models import calc_job_dates

PRIORITY_RANK = {
    "Critical": 1,
    "High": 2,
    "Normal": 3,
    "Low": 4,
}

def _scalar(engine, sql, params=None):
    with engine.begin() as conn:
        return conn.execute(text(sql), params or {}).scalar()

def _scalar_conn(conn, sql, params=None):
    return conn.execute(text(sql), params or {}).scalar()

def _next_job_code(engine, region_code: str, year: int) -> str:
    prefix = f"{region_code}-{year}-"
    df = query_df(engine, "SELECT job_code FROM jobs WHERE job_code LIKE :pfx", {"pfx": f"{prefix}%"})
    nums = []
    for jc in df["job_code"].tolist() if not df.empty else []:
        try:
            nums.append(int(jc.split("-")[-1]))
        except Exception:
            pass
    nxt = max(nums, default=0) + 1
    return f"{region_code}-{year}-{nxt:03d}"

def create_job(engine, data: dict) -> int:
    dates = calc_job_dates(
        data["job_start_date"],
        data["job_duration_days"],
        data["mob_days_before_job"],
        data["demob_days_after_job"],
    )
    job_code = _next_job_code(engine, data["region_code"], pd.to_datetime(data["job_start_date"]).year)

    payload = {
        **data,
        "job_code": job_code,
        "job_start_date": dates["job_start_date"],
        "customer_color": data.get("customer_color", ""),
    }

    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                INSERT INTO jobs(
                    job_code, job_name, region_code, customer, customer_color, location,
                    job_start_date, job_duration_days, mob_days_before_job,
                    demob_days_after_job, status, notes
                )
                VALUES (
                    :job_code, :job_name, :region_code, :customer, :customer_color, :location,
                    :job_start_date, :job_duration_days, :mob_days_before_job,
                    :demob_days_after_job, :status, :notes
                )
                RETURNING id
                """
            ),
            payload,
        )
        return int(res.scalar_one())

def add_pool_adjustment(engine, data: dict):
    execute(
        engine,
        """
        INSERT INTO pool_adjustments(
            region_code, resource_class_id, quantity_change,
            adjustment_date, reason, notes
        )
        VALUES (
            :region_code, :resource_class_id, :quantity_change,
            :adjustment_date, :reason, :notes
        )
        """,
        data,
    )

def allocation_debug_df(engine):
    return query_df(engine, "SELECT * FROM job_requirements")
