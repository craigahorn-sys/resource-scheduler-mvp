from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

import pandas as pd
from sqlalchemy import text

from .db import execute, query_df
from .models import calc_job_dates

PRIORITY_ORDER = {"Critical": 0, "High": 1, "Normal": 2, "Low": 3}


def _scalar(engine, sql, params=None):
    with engine.begin() as conn:
        return conn.execute(text(sql), params or {}).scalar()


def _daterange(start_date, end_date):
    current = pd.to_datetime(start_date).date()
    finish = pd.to_datetime(end_date).date()
    while current <= finish:
        yield current
        current += timedelta(days=1)


def _priority_rank(priority: str) -> int:
    return PRIORITY_ORDER.get(str(priority or "Normal"), PRIORITY_ORDER["Normal"])


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
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                INSERT INTO jobs(
                    job_code, job_name, region_code, customer, location,
                    job_start_date, job_duration_days, mob_days_before_job,
                    demob_days_after_job, status, notes
                )
                VALUES (
                    :job_code, :job_name, :region_code, :customer, :location,
                    :job_start_date, :job_duration_days, :mob_days_before_job,
                    :demob_days_after_job, :status, :notes
                )
                RETURNING id
                """
            ),
            {**data, "job_code": job_code, "job_start_date": dates["job_start_date"]},
        )
        return int(res.scalar_one())


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


def _requirement_base_df(engine, region_code: str | None = None, resource_class_id: int | None = None):
    sql = """
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
            j.job_start_date,
            j.job_duration_days,
            j.mob_days_before_job,
            j.demob_days_after_job,
            j.created_at AS job_created_at,
            rc.class_name,
            rc.unit_type
        FROM job_requirements jr
        JOIN jobs j ON jr.job_id = j.id
        JOIN resource_classes rc ON jr.resource_class_id = rc.id
        WHERE 1=1
    """
    params: dict[str, object] = {}
    if region_code is not None:
        sql += " AND j.region_code = :region_code"
        params["region_code"] = region_code
    if resource_class_id is not None:
        sql += " AND jr.resource_class_id = :resource_class_id"
        params["resource_class_id"] = int(resource_class_id)
    sql += " ORDER BY jr.id"

    df = query_df(engine, sql, params)
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


def _capacity_by_day(engine, region_code: str, resource_class_id: int, start_date, end_date) -> dict:
    base = (
        _scalar(
            engine,
            """
            SELECT COALESCE(base_quantity, 0)
            FROM resource_pools
            WHERE region_code = :region_code AND resource_class_id = :resource_class_id
            """,
            {"region_code": region_code, "resource_class_id": int(resource_class_id)},
        )
        or 0
    )
    adjustments = query_df(
        engine,
        """
        SELECT adjustment_date, quantity_change
        FROM pool_adjustments
        WHERE region_code = :region_code
          AND resource_class_id = :resource_class_id
          AND adjustment_date <= :end_date
        ORDER BY adjustment_date
        """,
        {
            "region_code": region_code,
            "resource_class_id": int(resource_class_id),
            "end_date": end_date,
        },
    )

    change_map: dict = defaultdict(float)
    if not adjustments.empty:
        for _, row in adjustments.iterrows():
            change_map[pd.to_datetime(row["adjustment_date"]).date()] += float(row["quantity_change"])

    running = float(base)
    capacity: dict = {}
    for day in _daterange(start_date, end_date):
        running += float(change_map.get(day, 0.0))
        capacity[day] = running
    return capacity


def rebalance_region_class(engine, region_code: str, resource_class_id: int):
    """
    Priority-first allocation for a single (region, resource_class) combination.

    For reusable assets (pumps, operators, trucks, etc.) the pool capacity is a
    concurrent-use limit: 10 pumps means 10 can be out at the same time, not 10
    consumed per day.  The correct model is therefore:

      For each day D:
          available(D) = pool_capacity(D) - sum_of_already_allocated_requirements_active_on_D

    We process requirements in priority order (Critical first) and, within the
    same priority, by earliest required_start.  For each requirement we find the
    minimum per-day headroom across its entire window and assign that much.  We
    then record the assignment against every day in the window so subsequent
    requirements see reduced availability on those days.
    """
    req = _requirement_base_df(engine, region_code=region_code, resource_class_id=resource_class_id)
    if req.empty:
        return

    # FIX: use pandas .min()/.max() on the Series so NaT is handled gracefully
    start_series = pd.to_datetime(req["required_start"]).dropna()
    end_series = pd.to_datetime(req["required_end"]).dropna()
    if start_series.empty or end_series.empty:
        return
    global_start = start_series.min().date()
    global_end = end_series.max().date()

    capacity = _capacity_by_day(engine, region_code, resource_class_id, global_start, global_end)
    # remaining tracks how much of the pool is still unallocated on each day
    remaining: dict = {day: float(qty) for day, qty in capacity.items()}

    ordered = req.copy()
    ordered["priority_rank"] = ordered["priority"].apply(_priority_rank)
    ordered["required_start_sort"] = pd.to_datetime(ordered["required_start"])
    ordered = ordered.sort_values(
        by=["priority_rank", "required_start_sort", "job_created_at", "id"],
        ascending=[True, True, True, True],
    )

    allocations: dict[int, float] = {}
    for _, row in ordered.iterrows():
        days = list(_daterange(row["required_start"], row["required_end"]))
        if not days:
            allocations[int(row["id"])] = 0.0
            continue

        # The bottleneck is the day with the least remaining capacity in the window
        min_remaining = min(float(remaining.get(day, 0.0)) for day in days)
        alloc = max(min(float(row["quantity_required"]), min_remaining), 0.0)

        if alloc > 0:
            # Mark this quantity as occupied for every day of the window
            for day in days:
                remaining[day] = float(remaining.get(day, 0.0)) - alloc

        allocations[int(row["id"])] = float(alloc)

    req_ids = [int(v) for v in ordered["id"].tolist()]

    # FIX: perform the delete + all inserts inside a single transaction so a
    # mid-loop crash cannot leave requirements stranded with no fulfillment row.
    with engine.begin() as conn:
        if req_ids:
            placeholders = ", ".join([f":rid_{i}" for i in range(len(req_ids))])
            id_params = {f"rid_{i}": rid for i, rid in enumerate(req_ids)}
            conn.execute(
                text(
                    f"DELETE FROM requirement_fulfillment "
                    f"WHERE fulfillment_type = 'internal_pool' "
                    f"AND requirement_id IN ({placeholders})"
                ),
                id_params,
            )

        for rid, assigned in allocations.items():
            if assigned <= 0:
                continue
            conn.execute(
                text(
                    """
                    INSERT INTO requirement_fulfillment(
                        requirement_id, fulfillment_type, source_name,
                        specific_resource_name, quantity_assigned, notes
                    )
                    VALUES (
                        :requirement_id, 'internal_pool', 'Internal Pool',
                        NULL, :quantity_assigned, :notes
                    )
                    """
                ),
                {
                    "requirement_id": rid,
                    "quantity_assigned": assigned,
                    "notes": "Auto-allocated via priority-first rebalance",
                },
            )


def rebalance_all(engine):
    combos = query_df(
        engine,
        """
        SELECT DISTINCT j.region_code, jr.resource_class_id
        FROM job_requirements jr
        JOIN jobs j ON jr.job_id = j.id
        ORDER BY j.region_code, jr.resource_class_id
        """,
    )
    if combos.empty:
        return
    for _, row in combos.iterrows():
        rebalance_region_class(engine, str(row["region_code"]), int(row["resource_class_id"]))


def upsert_pool(engine, region_code: str, resource_class_id: int, base_quantity: float, notes: str = ""):
    execute(
        engine,
        """
        INSERT INTO resource_pools(region_code, resource_class_id, base_quantity, notes)
        VALUES (:region_code, :resource_class_id, :base_quantity, :notes)
        ON CONFLICT (region_code, resource_class_id) DO UPDATE
        SET base_quantity = EXCLUDED.base_quantity,
            notes = EXCLUDED.notes
        """,
        {
            "region_code": str(region_code),
            "resource_class_id": int(resource_class_id),
            "base_quantity": float(base_quantity),
            "notes": str(notes) if notes is not None else "",
        },
    )
    rebalance_region_class(engine, str(region_code), int(resource_class_id))


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
    rebalance_region_class(engine, str(data["region_code"]), int(data["resource_class_id"]))


def create_requirement(engine, data: dict):
    region_row = query_df(
        engine,
        "SELECT region_code FROM jobs WHERE id = :job_id",
        {"job_id": int(data["job_id"])}
    )
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                INSERT INTO job_requirements(
                    job_id, resource_class_id, quantity_required,
                    days_before_job_start, days_after_job_end, priority, notes
                )
                VALUES (
                    :job_id, :resource_class_id, :quantity_required,
                    :days_before_job_start, :days_after_job_end, :priority, :notes
                )
                RETURNING id
                """
            ),
            data,
        )
        requirement_id = int(res.scalar_one())

    if not region_row.empty:
        rebalance_region_class(engine, str(region_row.iloc[0]["region_code"]), int(data["resource_class_id"]))
    return requirement_id


def requirement_summary_df(engine):
    req = _requirement_base_df(engine)
    if req.empty:
        return req

    # Batch all fulfillment in one query instead of per-requirement lookups
    fulfillment = query_df(
        engine,
        """
        SELECT requirement_id, COALESCE(SUM(quantity_assigned), 0) AS quantity_assigned
        FROM requirement_fulfillment
        GROUP BY requirement_id
        """
    )
    if fulfillment.empty:
        req["quantity_assigned"] = 0.0
    else:
        req = req.merge(fulfillment, how="left", left_on="id", right_on="requirement_id")
        req["quantity_assigned"] = req["quantity_assigned"].fillna(0.0)

    # Batch capacity queries: one per (region, resource_class) group
    debug_rows = []
    for (region_code, resource_class_id), grp in req.groupby(["region_code", "resource_class_id"]):
        grp = grp.copy()
        start_series = pd.to_datetime(grp["required_start"]).dropna()
        end_series = pd.to_datetime(grp["required_end"]).dropna()
        if start_series.empty or end_series.empty:
            for _, row in grp.iterrows():
                debug_rows.append({"id": int(row["id"]), "debug_total_pool_window_min": 0.0})
            continue
        global_start = start_series.min().date()
        global_end = end_series.max().date()
        capacity = _capacity_by_day(engine, str(region_code), int(resource_class_id), global_start, global_end)
        for _, row in grp.iterrows():
            days = list(_daterange(row["required_start"], row["required_end"]))
            min_pool = min(float(capacity.get(day, 0.0)) for day in days) if days else 0.0
            debug_rows.append({
                "id": int(row["id"]),
                "debug_total_pool_window_min": float(min_pool),
            })

    debug_df = pd.DataFrame(debug_rows)
    req = req.merge(debug_df, how="left", on="id")
    req["quantity_required"] = req["quantity_required"].astype(float)
    req["quantity_assigned"] = req["quantity_assigned"].astype(float)
    req["quantity_shortfall"] = (req["quantity_required"] - req["quantity_assigned"]).clip(lower=0)

    def status(row):
        assigned = float(row["quantity_assigned"])
        shortfall = float(row["quantity_shortfall"])
        if assigned <= 0:
            return "Unallocated"
        if shortfall > 0:
            return "Partially Allocated with Shortfall"
        return "Fully Allocated"

    req["allocation_status"] = req.apply(status, axis=1)
    return req[[
        "id",
        "job_id",
        "resource_class_id",
        "job_code",
        "job_name",
        "region_code",
        "class_name",
        "unit_type",
        "priority",
        "quantity_required",
        "required_start",
        "required_end",
        "quantity_assigned",
        "quantity_shortfall",
        "allocation_status",
        "debug_total_pool_window_min",
    ]].sort_values(by=["required_start", "job_code", "class_name", "id"])


def get_fulfillment_df(engine):
    """
    Fetch fulfillment rows with requirement window dates.
    FIX: date arithmetic is computed in Python (via _requirement_base_df) rather
    than using PostgreSQL-only INTERVAL syntax, so this works on SQLite too.
    """
    rf = query_df(
        engine,
        """
        SELECT
            rf.id,
            rf.requirement_id,
            rf.fulfillment_type,
            rf.source_name,
            rf.specific_resource_name,
            rf.quantity_assigned,
            rf.notes,
            rf.created_at,
            jr.quantity_required,
            jr.priority,
            j.job_code,
            j.job_name,
            j.region_code,
            rc.class_name,
            rc.unit_type
        FROM requirement_fulfillment rf
        JOIN job_requirements jr ON rf.requirement_id = jr.id
        JOIN jobs j ON jr.job_id = j.id
        JOIN resource_classes rc ON jr.resource_class_id = rc.id
        ORDER BY rf.id DESC
        """,
    )
    if rf.empty:
        return rf

    # Compute window dates in Python using the same logic as _requirement_base_df
    base = _requirement_base_df(engine)
    if base.empty:
        rf["required_start"] = None
        rf["required_end"] = None
        return rf

    window = base[["id", "required_start", "required_end"]].rename(columns={"id": "requirement_id"})
    rf = rf.merge(window, on="requirement_id", how="left")
    return rf


def pool_snapshot_df(engine, as_of_date):
    pools = query_df(
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
            COALESCE((
                SELECT SUM(pa.quantity_change)
                FROM pool_adjustments pa
                WHERE pa.region_code = rp.region_code
                  AND pa.resource_class_id = rp.resource_class_id
                  AND pa.adjustment_date <= :as_of_date
            ), 0) AS adjustment_total
        FROM resource_pools rp
        JOIN resource_classes rc ON rp.resource_class_id = rc.id
        ORDER BY rp.region_code, rc.category, rc.class_name
        """,
        {"as_of_date": as_of_date},
    )
    if pools.empty:
        return pools

    req = requirement_summary_df(engine)
    rows = []
    as_of = pd.to_datetime(as_of_date).date()
    for _, pool in pools.iterrows():
        related = pd.DataFrame()
        if not req.empty:
            related = req.loc[
                (req["region_code"] == pool["region_code"])
                & (req["resource_class_id"] == pool["resource_class_id"])
                & (pd.to_datetime(req["required_start"]).dt.date <= as_of)
                & (pd.to_datetime(req["required_end"]).dt.date >= as_of)
            ].copy()

        total_pool = float(pool["base_quantity"] or 0) + float(pool["adjustment_total"] or 0)
        committed = float(related["quantity_assigned"].sum()) if not related.empty else 0.0
        shortfall = float(related["quantity_shortfall"].sum()) if not related.empty else 0.0
        available = max(total_pool - committed, 0.0)

        # FIX: distinguish "Partially Committed" (has active allocations but still
        # has headroom) from "Available" (nothing committed at all).
        if committed > 0 and shortfall > 0:
            pool_status = "Fully Allocated with Shortfall"
        elif committed > 0 and available <= 0:
            pool_status = "Fully Allocated"
        elif committed > 0:
            pool_status = "Partially Committed"
        else:
            pool_status = "Available"

        rows.append({
            "region_code": pool["region_code"],
            "resource_class_id": int(pool["resource_class_id"]),
            "class_name": pool["class_name"],
            "category": pool["category"],
            "unit_type": pool["unit_type"],
            "base_quantity": float(pool["base_quantity"] or 0),
            "adjustment_total": float(pool["adjustment_total"] or 0),
            "total_pool": total_pool,
            "committed_quantity": committed,
            "available_quantity": available,
            "shortfall_against_active_jobs": shortfall,
            "pool_status": pool_status,
        })

    return pd.DataFrame(rows)


def allocation_debug_df(engine):
    req = requirement_summary_df(engine)
    if req.empty:
        return req
    return req[[
        "job_code",
        "job_name",
        "region_code",
        "class_name",
        "priority",
        "required_start",
        "required_end",
        "quantity_required",
        "quantity_assigned",
        "quantity_shortfall",
        "debug_total_pool_window_min",
        "allocation_status",
    ]].sort_values(by=["region_code", "class_name", "priority", "required_start", "job_code"])
