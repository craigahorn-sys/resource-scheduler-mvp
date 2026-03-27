
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from .db import execute, query_df
from .models import calc_job_dates


PRIORITY_RANK = {"Critical": 1, "High": 2, "Normal": 3, "Low": 4}


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
                ) VALUES (
                    :job_code, :job_name, :region_code, :customer, :customer_color, :location,
                    :job_start_date, :job_duration_days, :mob_days_before_job,
                    :demob_days_after_job, :status, :notes
                ) RETURNING id
                """
            ),
            payload,
        )
        return int(res.scalar_one())


def update_job(engine, job_id: int, data: dict):
    dates = calc_job_dates(
        data["job_start_date"],
        data["job_duration_days"],
        data["mob_days_before_job"],
        data["demob_days_after_job"],
    )
    execute(
        engine,
        """
        UPDATE jobs
        SET job_name=:job_name,
            region_code=:region_code,
            customer=:customer,
            customer_color=:customer_color,
            location=:location,
            job_start_date=:job_start_date,
            job_duration_days=:job_duration_days,
            mob_days_before_job=:mob_days_before_job,
            demob_days_after_job=:demob_days_after_job,
            status=:status,
            notes=:notes
        WHERE id=:job_id
        """,
        {**data, "job_id": int(job_id), "job_start_date": dates["job_start_date"], "customer_color": data.get("customer_color", "")},
    )
    recalc_all_requirements(engine)


def delete_job(engine, job_id: int):
    execute(engine, "DELETE FROM jobs WHERE id=:job_id", {"job_id": int(job_id)})
    recalc_all_requirements(engine)


def get_jobs_df(engine):
    df = query_df(engine, "SELECT * FROM jobs ORDER BY id DESC")
    if df.empty:
        return df
    ends = df.apply(
        lambda r: calc_job_dates(
            r["job_start_date"],
            int(r["job_duration_days"]),
            int(r["mob_days_before_job"]),
            int(r["demob_days_after_job"]),
        ),
        axis=1,
    )
    df["job_end_date"] = [d["job_end_date"] for d in ends]
    df["mob_start_date"] = [d["mob_start_date"] for d in ends]
    df["demob_end_date"] = [d["demob_end_date"] for d in ends]
    return df


def _requirement_base_df(engine):
    df = query_df(
        engine,
        """
        SELECT
            jr.id,
            jr.job_id,
            jr.resource_class_id,
            jr.quantity_required,
            jr.days_before_job_start,
            jr.days_after_job_end,
            jr.priority,
            jr.notes,
            j.job_code,
            j.job_name,
            j.region_code,
            COALESCE(j.customer, '') AS customer,
            COALESCE(j.customer_color, '') AS customer_color,
            j.job_start_date,
            j.job_duration_days,
            j.mob_days_before_job,
            j.demob_days_after_job,
            rc.class_name,
            rc.category,
            rc.unit_type
        FROM job_requirements jr
        JOIN jobs j ON jr.job_id = j.id
        JOIN resource_classes rc ON jr.resource_class_id = rc.id
        ORDER BY jr.id
        """
    )
    if df.empty:
        return df
    req_start, req_end, ranks = [], [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(
            r["job_start_date"],
            int(r["job_duration_days"]),
            int(r["mob_days_before_job"]),
            int(r["demob_days_after_job"]),
        )
        req_start.append((pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))).date())
        req_end.append((pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))).date())
        ranks.append(PRIORITY_RANK.get(str(r["priority"]), 999))
    df["required_start"] = req_start
    df["required_end"] = req_end
    df["priority_rank"] = ranks
    return df


def _pool_total_as_of(engine, region_code: str, resource_class_id: int, as_of_date):
    base = _scalar(
        engine,
        "SELECT COALESCE(base_quantity,0) FROM resource_pools WHERE region_code=:r AND resource_class_id=:rc",
        {"r": region_code, "rc": resource_class_id},
    ) or 0.0
    adj = _scalar(
        engine,
        "SELECT COALESCE(SUM(quantity_change),0) FROM pool_adjustments WHERE region_code=:r AND resource_class_id=:rc AND adjustment_date<=:d",
        {"r": region_code, "rc": resource_class_id, "d": as_of_date},
    ) or 0.0
    return float(base) + float(adj)


def _pool_total_as_of_conn(conn, region_code: str, resource_class_id: int, as_of_date):
    base = _scalar_conn(
        conn,
        "SELECT COALESCE(base_quantity,0) FROM resource_pools WHERE region_code=:r AND resource_class_id=:rc",
        {"r": region_code, "rc": resource_class_id},
    ) or 0.0
    adj = _scalar_conn(
        conn,
        "SELECT COALESCE(SUM(quantity_change),0) FROM pool_adjustments WHERE region_code=:r AND resource_class_id=:rc AND adjustment_date<=:d",
        {"r": region_code, "rc": resource_class_id, "d": as_of_date},
    ) or 0.0
    return float(base) + float(adj)


def _windows_overlap(start_a, end_a, start_b, end_b) -> bool:
    return pd.to_datetime(start_a) <= pd.to_datetime(end_b) and pd.to_datetime(end_a) >= pd.to_datetime(start_b)


def recalc_all_requirements(engine):
    req = _requirement_base_df(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM requirement_fulfillment WHERE fulfillment_type='internal_pool'"))
        if req.empty:
            return

        req = req.sort_values(
            by=["region_code", "resource_class_id", "priority_rank", "required_start", "job_start_date", "id"],
            ascending=[True, True, True, True, True, True],
        ).reset_index(drop=True)

        for (region_code, resource_class_id), bucket_df in req.groupby(["region_code", "resource_class_id"], sort=False):
            bucket_df = bucket_df.reset_index(drop=True)

            global_start = pd.to_datetime(bucket_df["required_start"]).min().date()
            global_end = pd.to_datetime(bucket_df["required_end"]).max().date()
            all_days = pd.date_range(global_start, global_end, freq="D")

            availability_by_day = {
                d.date(): _pool_total_as_of_conn(conn, str(region_code), int(resource_class_id), d.date())
                for d in all_days
            }

            for _, row in bucket_df.iterrows():
                row_days = pd.date_range(row["required_start"], row["required_end"], freq="D")
                if len(row_days) == 0:
                    continue

                bottleneck = min(float(availability_by_day[d.date()]) for d in row_days)
                assign_qty = min(float(row["quantity_required"]), max(bottleneck, 0.0))

                if assign_qty > 0:
                    conn.execute(
                        text(
                            """
                            INSERT INTO requirement_fulfillment(
                                requirement_id, fulfillment_type, source_name, specific_resource_name, quantity_assigned, notes
                            ) VALUES (:rid, 'internal_pool', 'Internal Pool', NULL, :q, :notes)
                            """
                        ),
                        {
                            "rid": int(row["id"]),
                            "q": float(assign_qty),
                            "notes": (
                                "Auto-allocated by window bottleneck. "
                                f"Pool start={float(_pool_total_as_of_conn(conn, str(region_code), int(resource_class_id), row['required_start'])):.2f}, "
                                f"window bottleneck={float(bottleneck):.2f}"
                            ),
                        },
                    )
                    for d in row_days:
                        availability_by_day[d.date()] = float(availability_by_day[d.date()]) - float(assign_qty)


def upsert_pool(engine, region_code: str, resource_class_id: int, base_quantity: float, notes: str = ""):
    execute(
        engine,
        """
        INSERT INTO resource_pools(region_code, resource_class_id, base_quantity, notes)
        VALUES (:region_code,:resource_class_id,:base_quantity,:notes)
        ON CONFLICT (region_code, resource_class_id) DO UPDATE
        SET base_quantity=EXCLUDED.base_quantity, notes=EXCLUDED.notes
        """,
        {"region_code": str(region_code), "resource_class_id": int(resource_class_id), "base_quantity": float(base_quantity), "notes": str(notes) if notes is not None else ""},
    )
    recalc_all_requirements(engine)


def delete_pool(engine, pool_id: int):
    execute(engine, "DELETE FROM resource_pools WHERE id=:pool_id", {"pool_id": int(pool_id)})
    recalc_all_requirements(engine)


def add_pool_adjustment(engine, data: dict):
    execute(
        engine,
        """
        INSERT INTO pool_adjustments(region_code, resource_class_id, quantity_change, adjustment_date, reason, notes)
        VALUES (:region_code, :resource_class_id, :quantity_change, :adjustment_date, :reason, :notes)
        """,
        data,
    )
    recalc_all_requirements(engine)


def delete_pool_adjustment(engine, adjustment_id: int):
    execute(engine, "DELETE FROM pool_adjustments WHERE id=:id", {"id": int(adjustment_id)})
    recalc_all_requirements(engine)


def create_requirement(engine, data: dict):
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                INSERT INTO job_requirements(job_id, resource_class_id, quantity_required, days_before_job_start, days_after_job_end, priority, notes)
                VALUES (:job_id, :resource_class_id, :quantity_required, :days_before_job_start, :days_after_job_end, :priority, :notes)
                RETURNING id
                """
            ),
            data,
        )
        requirement_id = int(res.scalar_one())
    recalc_all_requirements(engine)
    return requirement_id


def update_requirement(engine, requirement_id: int, data: dict):
    execute(
        engine,
        """
        UPDATE job_requirements
        SET resource_class_id=:resource_class_id, quantity_required=:quantity_required,
            days_before_job_start=:days_before_job_start, days_after_job_end=:days_after_job_end,
            priority=:priority, notes=:notes
        WHERE id=:requirement_id
        """,
        {
            "requirement_id": int(requirement_id),
            "resource_class_id": int(data["resource_class_id"]),
            "quantity_required": float(data["quantity_required"]),
            "days_before_job_start": int(data["days_before_job_start"]),
            "days_after_job_end": int(data["days_after_job_end"]),
            "priority": data["priority"],
            "notes": data.get("notes", ""),
        },
    )
    recalc_all_requirements(engine)


def delete_requirement(engine, requirement_id: int):
    execute(engine, "DELETE FROM job_requirements WHERE id=:requirement_id", {"requirement_id": int(requirement_id)})
    recalc_all_requirements(engine)


def requirement_summary_df(engine):
    df = query_df(
        engine,
        """
        SELECT
            jr.id,
            jr.resource_class_id,
            j.job_code,
            j.job_name,
            j.region_code,
            COALESCE(j.customer, '') AS customer,
            COALESCE(j.customer_color, '') AS customer_color,
            rc.class_name,
            rc.unit_type,
            jr.quantity_required,
            jr.days_before_job_start,
            jr.days_after_job_end,
            jr.priority,
            jr.notes,
            j.job_start_date,
            j.job_duration_days,
            j.mob_days_before_job,
            j.demob_days_after_job,
            COALESCE(SUM(rf.quantity_assigned),0) AS quantity_assigned
        FROM job_requirements jr
        JOIN jobs j ON jr.job_id=j.id
        JOIN resource_classes rc ON jr.resource_class_id=rc.id
        LEFT JOIN requirement_fulfillment rf ON rf.requirement_id=jr.id
        GROUP BY
            jr.id, jr.resource_class_id, j.job_code, j.job_name, j.region_code, j.customer, j.customer_color,
            rc.class_name, rc.unit_type,
            jr.quantity_required, jr.days_before_job_start, jr.days_after_job_end, jr.priority, jr.notes,
            j.job_start_date, j.job_duration_days, j.mob_days_before_job, j.demob_days_after_job
        ORDER BY jr.id DESC
        """
    )
    if df.empty:
        return df
    starts, ends = [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(
            r["job_start_date"],
            int(r["job_duration_days"]),
            int(r["mob_days_before_job"]),
            int(r["demob_days_after_job"]),
        )
        starts.append((pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))).date())
        ends.append((pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))).date())
    df["required_start"] = starts
    df["required_end"] = ends
    df["quantity_assigned"] = df["quantity_assigned"].astype(float)
    df["quantity_shortfall"] = (df["quantity_required"].astype(float) - df["quantity_assigned"]).clip(lower=0)

    def status(row):
        req = float(row["quantity_required"])
        assigned = float(row["quantity_assigned"])
        shortfall = max(req - assigned, 0.0)
        if assigned <= 0:
            return "Unallocated"
        if shortfall <= 0:
            return "Fully Allocated"
        return "Partially Allocated with Shortfall"

    df["allocation_status"] = df.apply(status, axis=1)
    return df


def get_requirements_df(engine):
    return requirement_summary_df(engine)


def get_fulfillment_df(engine):
    df = query_df(
        engine,
        """
        SELECT
            rf.*,
            jr.days_before_job_start,
            jr.days_after_job_end,
            j.job_code,
            j.job_name,
            j.region_code,
            j.job_start_date,
            j.job_duration_days,
            j.mob_days_before_job,
            j.demob_days_after_job,
            rc.class_name,
            rc.unit_type
        FROM requirement_fulfillment rf
        JOIN job_requirements jr ON rf.requirement_id=jr.id
        JOIN jobs j ON jr.job_id=j.id
        JOIN resource_classes rc ON jr.resource_class_id=rc.id
        ORDER BY rf.id DESC
        """
    )
    if df.empty:
        return df
    starts, ends = [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(
            r["job_start_date"],
            int(r["job_duration_days"]),
            int(r["mob_days_before_job"]),
            int(r["demob_days_after_job"]),
        )
        starts.append((pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))).date())
        ends.append((pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))).date())
    df["required_start"] = starts
    df["required_end"] = ends
    return df


def get_pools_df(engine):
    return query_df(
        engine,
        """
        SELECT rp.id, rp.region_code, rp.resource_class_id, rc.class_name, rc.category, rc.unit_type, rp.base_quantity, rp.notes
        FROM resource_pools rp
        JOIN resource_classes rc ON rp.resource_class_id=rc.id
        ORDER BY rp.region_code, rp.id
        """
    )


def get_pool_adjustments_df(engine):
    return query_df(
        engine,
        """
        SELECT pa.id, pa.region_code, pa.resource_class_id, rc.class_name, rc.unit_type, pa.quantity_change, pa.adjustment_date, pa.reason, pa.notes, pa.created_at
        FROM pool_adjustments pa
        JOIN resource_classes rc ON pa.resource_class_id=rc.id
        ORDER BY pa.id DESC
        """
    )


def pool_snapshot_df(engine, as_of_date):
    df = query_df(
        engine,
        """
        SELECT
            rp.id,
            rp.region_code,
            rp.resource_class_id,
            rc.class_name,
            rc.category,
            rc.unit_type,
            rp.base_quantity,
            COALESCE(
                (
                    SELECT SUM(pa.quantity_change)
                    FROM pool_adjustments pa
                    WHERE pa.region_code=rp.region_code
                      AND pa.resource_class_id=rp.resource_class_id
                      AND pa.adjustment_date<=:d
                ),
                0
            ) AS adjustment_total
        FROM resource_pools rp
        JOIN resource_classes rc ON rp.resource_class_id=rc.id
        ORDER BY rp.region_code, rp.id
        """,
        {"d": as_of_date},
    )
    if df.empty:
        return df
    req = requirement_summary_df(engine)
    committed, pool_status = [], []
    for _, r in df.iterrows():
        q = 0.0
        related_shortfall = 0.0
        if not req.empty:
            mask = (
                (req["region_code"] == r["region_code"])
                & (req["resource_class_id"] == r["resource_class_id"])
                & (pd.to_datetime(req["required_start"]) <= pd.to_datetime(as_of_date))
                & (pd.to_datetime(req["required_end"]) >= pd.to_datetime(as_of_date))
            )
            q = float(req.loc[mask, "quantity_assigned"].sum())
            related_shortfall = float(req.loc[mask, "quantity_shortfall"].sum())
        committed.append(q)
        total_pool = float(r["base_quantity"]) + float(r["adjustment_total"])
        available = max(total_pool - q, 0.0)
        if related_shortfall > 0:
            pool_status.append("Fully Allocated with Shortfall")
        elif q > 0 and available <= 0:
            pool_status.append("Fully Allocated")
        elif q > 0 and available > 0:
            pool_status.append("Partially Committed")
        else:
            pool_status.append("Available")
    df["base_quantity"] = df["base_quantity"].astype(float)
    df["adjustment_total"] = df["adjustment_total"].astype(float)
    df["total_pool"] = df["base_quantity"] + df["adjustment_total"]
    df["committed_quantity"] = committed
    df["available_quantity"] = (df["total_pool"] - df["committed_quantity"]).clip(lower=0)
    df["pool_status"] = pool_status
    return df[
        [
            "id",
            "region_code",
            "resource_class_id",
            "class_name",
            "category",
            "unit_type",
            "base_quantity",
            "adjustment_total",
            "total_pool",
            "committed_quantity",
            "available_quantity",
            "pool_status",
        ]
    ]


def allocation_debug_df(engine):
    req = _requirement_base_df(engine)
    if req.empty:
        return req
    assigned_df = query_df(
        engine,
        """
        SELECT requirement_id, COALESCE(SUM(quantity_assigned),0) AS quantity_assigned
        FROM requirement_fulfillment
        WHERE fulfillment_type='internal_pool'
        GROUP BY requirement_id
        """
    )
    if assigned_df.empty:
        assigned_df = pd.DataFrame(columns=["requirement_id", "quantity_assigned"])
    req = req.merge(assigned_df, how="left", left_on="id", right_on="requirement_id")
    req["quantity_assigned"] = req["quantity_assigned"].fillna(0.0).astype(float)

    debug_rows = []
    sorted_req = req.sort_values(
        by=["region_code", "resource_class_id", "priority_rank", "required_start", "job_start_date", "id"],
        ascending=[True, True, True, True, True, True],
    ).reset_index(drop=True)

    for _, row in sorted_req.iterrows():
        day_range = pd.date_range(row["required_start"], row["required_end"], freq="D")
        if len(day_range) == 0:
            continue
        per_day_pool = [
            _pool_total_as_of(engine, str(row["region_code"]), int(row["resource_class_id"]), d.date())
            for d in day_range
        ]
        debug_rows.append(
            {
                "requirement_id": int(row["id"]),
                "job_code": row["job_code"],
                "job_name": row["job_name"],
                "region_code": row["region_code"],
                "class_name": row["class_name"],
                "priority": row["priority"],
                "priority_rank": int(row["priority_rank"]),
                "required_start": row["required_start"],
                "required_end": row["required_end"],
                "quantity_required": float(row["quantity_required"]),
                "quantity_assigned": float(row["quantity_assigned"]),
                "quantity_shortfall": max(float(row["quantity_required"]) - float(row["quantity_assigned"]), 0.0),
                "pool_total_min_across_window": float(min(per_day_pool)),
                "pool_total_max_across_window": float(max(per_day_pool)),
                "window_days": int(len(day_range)),
            }
        )
    return pd.DataFrame(debug_rows)
