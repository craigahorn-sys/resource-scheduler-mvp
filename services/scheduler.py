
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


def _scalar(engine, sql: str, params: dict | None = None):
    with engine.begin() as conn:
        return conn.execute(text(sql), params or {}).scalar()


def _table_columns(engine, table_name: str) -> set[str]:
    try:
        df = query_df(engine, f"PRAGMA table_info({table_name})")
        if not df.empty and "name" in df.columns:
            return set(df["name"].astype(str).tolist())
    except Exception:
        pass
    return set()


def _ensure_schema(engine):
    # Existing baseline schema is older than the current app expects.
    # Backfill missing columns/tables safely.
    try:
        execute(
            engine,
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_resource_pools_region_class "
            "ON resource_pools(region_code, resource_class_id)"
        )
    except Exception:
        pass

    jobs_cols = _table_columns(engine, "jobs")
    for col_def in [
        ("customer", "TEXT"),
        ("customer_color", "TEXT"),
        ("location", "TEXT"),
        ("notes", "TEXT"),
    ]:
        if col_def[0] not in jobs_cols:
            try:
                execute(engine, f"ALTER TABLE jobs ADD COLUMN {col_def[0]} {col_def[1]}")
            except Exception:
                pass

    pools_cols = _table_columns(engine, "resource_pools")
    if "notes" not in pools_cols:
        try:
            execute(engine, "ALTER TABLE resource_pools ADD COLUMN notes TEXT")
        except Exception:
            pass

    req_cols = _table_columns(engine, "job_requirements")
    if "notes" not in req_cols:
        try:
            execute(engine, "ALTER TABLE job_requirements ADD COLUMN notes TEXT")
        except Exception:
            pass

    fulfill_cols = _table_columns(engine, "requirement_fulfillment")
    if not fulfill_cols:
        try:
            execute(
                engine,
                '''
                CREATE TABLE IF NOT EXISTS requirement_fulfillment (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requirement_id INTEGER,
                    fulfillment_type TEXT,
                    source_name TEXT,
                    specific_resource_name TEXT,
                    quantity_assigned REAL,
                    notes TEXT
                )
                '''
            )
            fulfill_cols = _table_columns(engine, "requirement_fulfillment")
        except Exception:
            pass

    for name, typ in [
        ("fulfillment_type", "TEXT"),
        ("source_name", "TEXT"),
        ("specific_resource_name", "TEXT"),
        ("notes", "TEXT"),
    ]:
        if name not in fulfill_cols:
            try:
                execute(engine, f"ALTER TABLE requirement_fulfillment ADD COLUMN {name} {typ}")
            except Exception:
                pass

    # Older schema may not have pool_adjustments at all.
    try:
        execute(
            engine,
            '''
            CREATE TABLE IF NOT EXISTS pool_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region_code TEXT,
                resource_class_id INTEGER,
                quantity_change REAL,
                adjustment_date DATE,
                reason TEXT,
                notes TEXT
            )
            '''
        )
    except Exception:
        pass


def _next_job_code(engine, region_code: str, year: int) -> str:
    _ensure_schema(engine)
    prefix = f"{region_code}-{year}-"
    df = query_df(engine, "SELECT job_code FROM jobs WHERE job_code LIKE :pfx", {"pfx": f"{prefix}%"})
    nums = []
    if not df.empty:
        for jc in df["job_code"].fillna("").astype(str).tolist():
            try:
                nums.append(int(jc.split("-")[-1]))
            except Exception:
                pass
    nxt = max(nums, default=0) + 1
    return f"{region_code}-{year}-{nxt:03d}"


def create_job(engine, data: dict) -> int:
    _ensure_schema(engine)
    dates = calc_job_dates(
        data["job_start_date"],
        data["job_duration_days"],
        data["mob_days_before_job"],
        data["demob_days_after_job"],
    )
    job_code = _next_job_code(engine, str(data["region_code"]), pd.to_datetime(data["job_start_date"]).year)
    payload = {
        "job_code": job_code,
        "job_name": data["job_name"],
        "region_code": data["region_code"],
        "customer": data.get("customer", ""),
        "customer_color": data.get("customer_color", ""),
        "location": data.get("location", ""),
        "job_start_date": dates["job_start_date"],
        "job_duration_days": int(data["job_duration_days"]),
        "mob_days_before_job": int(data["mob_days_before_job"]),
        "demob_days_after_job": int(data["demob_days_after_job"]),
        "status": data.get("status", "Planned"),
        "notes": data.get("notes", ""),
    }
    with engine.begin() as conn:
        res = conn.execute(
            text(
                '''
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
                '''
            ),
            payload,
        )
        try:
            return int(res.lastrowid)
        except Exception:
            return int(_scalar(engine, "SELECT MAX(id) FROM jobs") or 0)


def update_job(engine, job_id: int, data: dict):
    _ensure_schema(engine)
    dates = calc_job_dates(
        data["job_start_date"],
        data["job_duration_days"],
        data["mob_days_before_job"],
        data["demob_days_after_job"],
    )
    execute(
        engine,
        '''
        UPDATE jobs
        SET job_name = :job_name,
            region_code = :region_code,
            customer = :customer,
            customer_color = :customer_color,
            location = :location,
            job_start_date = :job_start_date,
            job_duration_days = :job_duration_days,
            mob_days_before_job = :mob_days_before_job,
            demob_days_after_job = :demob_days_after_job,
            status = :status,
            notes = :notes
        WHERE id = :job_id
        ''',
        {
            "job_id": int(job_id),
            "job_name": data["job_name"],
            "region_code": data["region_code"],
            "customer": data.get("customer", ""),
            "customer_color": data.get("customer_color", ""),
            "location": data.get("location", ""),
            "job_start_date": dates["job_start_date"],
            "job_duration_days": int(data["job_duration_days"]),
            "mob_days_before_job": int(data["mob_days_before_job"]),
            "demob_days_after_job": int(data["demob_days_after_job"]),
            "status": data.get("status", "Planned"),
            "notes": data.get("notes", ""),
        },
    )
    recalc_all_requirements(engine)


def delete_job(engine, job_id: int):
    _ensure_schema(engine)
    execute(
        engine,
        "DELETE FROM requirement_fulfillment WHERE requirement_id IN (SELECT id FROM job_requirements WHERE job_id = :job_id)",
        {"job_id": int(job_id)},
    )
    execute(engine, "DELETE FROM job_requirements WHERE job_id = :job_id", {"job_id": int(job_id)})
    execute(engine, "DELETE FROM jobs WHERE id = :job_id", {"job_id": int(job_id)})
    recalc_all_requirements(engine)


def get_jobs_df(engine):
    _ensure_schema(engine)
    df = query_df(engine, "SELECT * FROM jobs ORDER BY id DESC")
    if df.empty:
        return df

    job_end_dates = []
    mob_start_dates = []
    demob_end_dates = []

    for _, r in df.iterrows():
        dates = calc_job_dates(
            r["job_start_date"],
            int(r["job_duration_days"]),
            int(r["mob_days_before_job"]),
            int(r["demob_days_after_job"]),
        )
        job_end_dates.append(pd.to_datetime(dates["job_end_date"]).date())
        mob_start_dates.append(pd.to_datetime(dates["mob_start_date"]).date())
        demob_end_dates.append(pd.to_datetime(dates["demob_end_date"]).date())

    df["job_end_date"] = job_end_dates
    df["mob_start_date"] = mob_start_dates
    df["demob_end_date"] = demob_end_dates
    return df


def get_pools_df(engine):
    _ensure_schema(engine)
    return query_df(
        engine,
        '''
        SELECT
            rp.id,
            rp.region_code,
            rp.resource_class_id,
            rc.class_name,
            rc.category,
            rc.unit_type,
            rp.base_quantity,
            COALESCE(rp.notes, '') AS notes
        FROM resource_pools rp
        JOIN resource_classes rc ON rc.id = rp.resource_class_id
        ORDER BY rp.region_code, rc.id, rp.id
        '''
    )


def upsert_pool(engine, region_code: str, resource_class_id: int, base_quantity: float, notes: str = ""):
    _ensure_schema(engine)
    existing = query_df(
        engine,
        '''
        SELECT id
        FROM resource_pools
        WHERE region_code = :region_code
          AND resource_class_id = :resource_class_id
        ''',
        {"region_code": region_code, "resource_class_id": int(resource_class_id)},
    )
    if existing.empty:
        execute(
            engine,
            '''
            INSERT INTO resource_pools(region_code, resource_class_id, base_quantity, notes)
            VALUES (:region_code, :resource_class_id, :base_quantity, :notes)
            ''',
            {
                "region_code": region_code,
                "resource_class_id": int(resource_class_id),
                "base_quantity": float(base_quantity),
                "notes": notes or "",
            },
        )
    else:
        execute(
            engine,
            '''
            UPDATE resource_pools
            SET base_quantity = :base_quantity,
                notes = :notes
            WHERE region_code = :region_code
              AND resource_class_id = :resource_class_id
            ''',
            {
                "region_code": region_code,
                "resource_class_id": int(resource_class_id),
                "base_quantity": float(base_quantity),
                "notes": notes or "",
            },
        )
    recalc_all_requirements(engine)


def delete_pool(engine, pool_id: int):
    _ensure_schema(engine)
    execute(engine, "DELETE FROM resource_pools WHERE id = :pool_id", {"pool_id": int(pool_id)})
    recalc_all_requirements(engine)


def add_pool_adjustment(engine, data: dict):
    _ensure_schema(engine)
    execute(
        engine,
        '''
        INSERT INTO pool_adjustments(
            region_code, resource_class_id, quantity_change,
            adjustment_date, reason, notes
        )
        VALUES (
            :region_code, :resource_class_id, :quantity_change,
            :adjustment_date, :reason, :notes
        )
        ''',
        {
            "region_code": data["region_code"],
            "resource_class_id": int(data["resource_class_id"]),
            "quantity_change": float(data["quantity_change"]),
            "adjustment_date": data["adjustment_date"],
            "reason": data.get("reason", ""),
            "notes": data.get("notes", ""),
        },
    )
    recalc_all_requirements(engine)


def delete_pool_adjustment(engine, adjustment_id: int):
    _ensure_schema(engine)
    execute(engine, "DELETE FROM pool_adjustments WHERE id = :id", {"id": int(adjustment_id)})
    recalc_all_requirements(engine)


def _requirement_base_df(engine) -> pd.DataFrame:
    _ensure_schema(engine)
    df = query_df(
        engine,
        '''
        SELECT
            jr.id,
            jr.job_id,
            jr.resource_class_id,
            jr.quantity_required,
            jr.days_before_job_start,
            jr.days_after_job_end,
            jr.priority,
            COALESCE(jr.notes, '') AS notes,
            j.job_code,
            j.job_name,
            j.region_code,
            COALESCE(j.customer, '') AS customer,
            COALESCE(j.customer_color, '') AS customer_color,
            COALESCE(j.location, '') AS location,
            j.job_start_date,
            j.job_duration_days,
            j.mob_days_before_job,
            j.demob_days_after_job,
            j.status,
            rc.class_name,
            rc.category,
            rc.unit_type,
            rc.planning_mode
        FROM job_requirements jr
        JOIN jobs j ON j.id = jr.job_id
        JOIN resource_classes rc ON rc.id = jr.resource_class_id
        ORDER BY jr.id DESC
        '''
    )
    if df.empty:
        return df

    required_start = []
    required_end = []
    priority_rank = []

    for _, r in df.iterrows():
        dates = calc_job_dates(
            r["job_start_date"],
            int(r["job_duration_days"]),
            int(r["mob_days_before_job"]),
            int(r["demob_days_after_job"]),
        )
        rs = pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))
        re = pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))
        required_start.append(rs.date())
        required_end.append(re.date())
        priority_rank.append(PRIORITY_RANK.get(str(r["priority"]), 999))

    df["required_start"] = required_start
    df["required_end"] = required_end
    df["priority_rank"] = priority_rank
    return df


def create_requirement(engine, data: dict):
    _ensure_schema(engine)
    with engine.begin() as conn:
        res = conn.execute(
            text(
                '''
                INSERT INTO job_requirements(
                    job_id, resource_class_id, quantity_required,
                    days_before_job_start, days_after_job_end, priority, notes
                )
                VALUES (
                    :job_id, :resource_class_id, :quantity_required,
                    :days_before_job_start, :days_after_job_end, :priority, :notes
                )
                '''
            ),
            {
                "job_id": int(data["job_id"]),
                "resource_class_id": int(data["resource_class_id"]),
                "quantity_required": float(data["quantity_required"]),
                "days_before_job_start": int(data["days_before_job_start"]),
                "days_after_job_end": int(data["days_after_job_end"]),
                "priority": data.get("priority", "Normal"),
                "notes": data.get("notes", ""),
            },
        )
        try:
            req_id = int(res.lastrowid)
        except Exception:
            req_id = int(_scalar(engine, "SELECT MAX(id) FROM job_requirements") or 0)
    recalc_all_requirements(engine)
    return req_id


def update_requirement(engine, requirement_id: int, data: dict):
    _ensure_schema(engine)
    execute(
        engine,
        '''
        UPDATE job_requirements
        SET resource_class_id = :resource_class_id,
            quantity_required = :quantity_required,
            days_before_job_start = :days_before_job_start,
            days_after_job_end = :days_after_job_end,
            priority = :priority,
            notes = :notes
        WHERE id = :requirement_id
        ''',
        {
            "requirement_id": int(requirement_id),
            "resource_class_id": int(data["resource_class_id"]),
            "quantity_required": float(data["quantity_required"]),
            "days_before_job_start": int(data["days_before_job_start"]),
            "days_after_job_end": int(data["days_after_job_end"]),
            "priority": data.get("priority", "Normal"),
            "notes": data.get("notes", ""),
        },
    )
    recalc_all_requirements(engine)


def delete_requirement(engine, requirement_id: int):
    _ensure_schema(engine)
    execute(engine, "DELETE FROM requirement_fulfillment WHERE requirement_id = :rid", {"rid": int(requirement_id)})
    execute(engine, "DELETE FROM job_requirements WHERE id = :rid", {"rid": int(requirement_id)})
    recalc_all_requirements(engine)


def _pool_total_as_of(engine, region_code: str, resource_class_id: int, as_of_date) -> float:
    _ensure_schema(engine)
    base = (
        _scalar(
            engine,
            '''
            SELECT COALESCE(SUM(base_quantity), 0)
            FROM resource_pools
            WHERE region_code = :region_code
              AND resource_class_id = :resource_class_id
            ''',
            {"region_code": region_code, "resource_class_id": int(resource_class_id)},
        )
        or 0.0
    )
    adj = (
        _scalar(
            engine,
            '''
            SELECT COALESCE(SUM(quantity_change), 0)
            FROM pool_adjustments
            WHERE region_code = :region_code
              AND resource_class_id = :resource_class_id
              AND adjustment_date <= :as_of_date
            ''',
            {
                "region_code": region_code,
                "resource_class_id": int(resource_class_id),
                "as_of_date": as_of_date,
            },
        )
        or 0.0
    )
    return float(base) + float(adj)


def _windows_overlap(start_a, end_a, start_b, end_b) -> bool:
    return pd.to_datetime(start_a) <= pd.to_datetime(end_b) and pd.to_datetime(end_a) >= pd.to_datetime(start_b)


def _date_range(start_date, end_date) -> list[pd.Timestamp]:
    start_ts = pd.to_datetime(start_date).normalize()
    end_ts = pd.to_datetime(end_date).normalize()
    if pd.isna(start_ts) or pd.isna(end_ts) or end_ts < start_ts:
        return []
    return list(pd.date_range(start_ts, end_ts, freq="D"))



def recalc_all_requirements(engine):
    _ensure_schema(engine)
    req = _requirement_base_df(engine)

    with engine.begin() as conn:
        conn.execute(
            text(
                '''
                DELETE FROM requirement_fulfillment
                WHERE COALESCE(fulfillment_type, 'internal_pool') = 'internal_pool'
                '''
            )
        )

        if req.empty:
            return

        req = req.sort_values(
            by=["priority_rank", "required_start", "required_end", "job_start_date", "job_code", "id"],
            ascending=[True, True, True, True, True, True],
        ).reset_index(drop=True)

        active_allocations: dict[tuple[str, int], list[dict]] = {}

        for _, row in req.iterrows():
            bucket = (str(row["region_code"]), int(row["resource_class_id"]))
            window_days = _date_range(row["required_start"], row["required_end"])
            if not window_days:
                continue

            day_availabilities: list[float] = []

            for day in window_days:
                day_pool_total = _pool_total_as_of(
                    engine,
                    bucket[0],
                    bucket[1],
                    day.date(),
                )
                day_committed = 0.0
                for prior in active_allocations.get(bucket, []):
                    if pd.to_datetime(prior["required_start"]) <= day and pd.to_datetime(prior["required_end"]) >= day:
                        day_committed += float(prior["quantity_assigned"])
                day_availabilities.append(max(float(day_pool_total) - float(day_committed), 0.0))

            bottleneck_available = min(day_availabilities) if day_availabilities else 0.0
            assign_qty = min(float(row["quantity_required"]), float(bottleneck_available))

            if assign_qty > 0:
                conn.execute(
                    text(
                        '''
                        INSERT INTO requirement_fulfillment(
                            requirement_id, fulfillment_type, source_name,
                            specific_resource_name, quantity_assigned, notes
                        )
                        VALUES (
                            :requirement_id, 'internal_pool', 'Internal Pool',
                            NULL, :quantity_assigned, :notes
                        )
                        '''
                    ),
                    {
                        "requirement_id": int(row["id"]),
                        "quantity_assigned": float(assign_qty),
                        "notes": (
                            f"Auto-allocation. Days={len(window_days)}; "
                            f"min_day_available={float(bottleneck_available):.3f}; "
                            f"required={float(row['quantity_required']):.3f}"
                        ),
                    },
                )

                active_allocations.setdefault(bucket, []).append(
                    {
                        "required_start": pd.to_datetime(row["required_start"]).normalize(),
                        "required_end": pd.to_datetime(row["required_end"]).normalize(),
                        "quantity_assigned": float(assign_qty),
                        "requirement_id": int(row["id"]),
                    }
                )


def requirement_summary_df(engine):
    _ensure_schema(engine)
    req = _requirement_base_df(engine)
    if req.empty:
        return req

    assigned = query_df(
        engine,
        '''
        SELECT
            requirement_id,
            COALESCE(SUM(quantity_assigned), 0) AS quantity_assigned
        FROM requirement_fulfillment
        GROUP BY requirement_id
        '''
    )
    if assigned.empty:
        assigned = pd.DataFrame(columns=["requirement_id", "quantity_assigned"])

    out = req.merge(assigned, how="left", left_on="id", right_on="requirement_id")
    out["quantity_assigned"] = out["quantity_assigned"].fillna(0.0).astype(float)
    out["quantity_required"] = out["quantity_required"].astype(float)
    out["quantity_shortfall"] = (out["quantity_required"] - out["quantity_assigned"]).clip(lower=0)

    def _status(r):
        if float(r["quantity_assigned"]) <= 0:
            return "Unallocated"
        if float(r["quantity_shortfall"]) <= 0:
            return "Fully Allocated"
        return "Partially Allocated with Shortfall"

    out["allocation_status"] = out.apply(_status, axis=1)
    return out


def get_fulfillment_df(engine):
    _ensure_schema(engine)
    rf = query_df(
        engine,
        '''
        SELECT
            rf.id,
            rf.requirement_id,
            COALESCE(rf.fulfillment_type, 'internal_pool') AS fulfillment_type,
            COALESCE(rf.source_name, 'Internal Pool') AS source_name,
            COALESCE(rf.specific_resource_name, '') AS specific_resource_name,
            rf.quantity_assigned,
            COALESCE(rf.notes, '') AS notes
        FROM requirement_fulfillment rf
        ORDER BY rf.id DESC
        '''
    )
    if rf.empty:
        return rf

    req = requirement_summary_df(engine)
    cols = [
        "id", "job_code", "job_name", "region_code", "class_name", "unit_type",
        "required_start", "required_end"
    ]
    merge_df = req[cols].rename(columns={"id": "requirement_id"})
    return rf.merge(merge_df, how="left", on="requirement_id")


def pool_snapshot_df(engine, as_of_date):
    _ensure_schema(engine)
    pools = get_pools_df(engine)
    if pools.empty:
        return pools

    req = requirement_summary_df(engine)
    rows = []

    for _, pool in pools.iterrows():
        total_pool = _pool_total_as_of(
            engine,
            str(pool["region_code"]),
            int(pool["resource_class_id"]),
            as_of_date,
        )

        committed = 0.0
        shortfall = 0.0
        if not req.empty:
            mask = (
                (req["region_code"] == pool["region_code"])
                & (req["resource_class_id"] == pool["resource_class_id"])
            )
            req_match = req.loc[mask].copy()
            if not req_match.empty:
                req_match["required_start"] = pd.to_datetime(req_match["required_start"])
                req_match["required_end"] = pd.to_datetime(req_match["required_end"])
                active_mask = (
                    req_match["required_start"] <= pd.to_datetime(as_of_date)
                ) & (
                    req_match["required_end"] >= pd.to_datetime(as_of_date)
                )
                active_req = req_match.loc[active_mask]
                committed = float(active_req["quantity_assigned"].sum())
                shortfall = float(active_req["quantity_shortfall"].sum())

        available = float(total_pool) - float(committed)

        if shortfall > 0:
            pool_status = "Fully Allocated with Shortfall"
        elif committed > 0 and available <= 0:
            pool_status = "Fully Allocated"
        elif committed > 0 and available > 0:
            pool_status = "Partially Committed"
        else:
            pool_status = "Available"

        rows.append(
            {
                "id": int(pool["id"]),
                "resource_class_id": int(pool["resource_class_id"]),
                "region_code": pool["region_code"],
                "class_name": pool["class_name"],
                "category": pool["category"],
                "unit_type": pool["unit_type"],
                "base_quantity": float(pool["base_quantity"]),
                "adjustment_total": float(total_pool) - float(pool["base_quantity"]),
                "total_pool": float(total_pool),
                "committed_quantity": float(committed),
                "available_quantity": float(available),
                "pool_status": pool_status,
            }
        )

    return pd.DataFrame(rows)



def allocation_debug_df(engine):
    _ensure_schema(engine)
    req = requirement_summary_df(engine)
    if req.empty:
        return req

    debug_rows = []
    sorted_req = req.sort_values(
        by=["region_code", "resource_class_id", "priority_rank", "required_start", "required_end", "job_code", "id"],
        ascending=[True, True, True, True, True, True, True],
    ).reset_index(drop=True)

    accepted: dict[tuple[str, int], list[dict]] = {}

    for _, row in sorted_req.iterrows():
        bucket = (str(row["region_code"]), int(row["resource_class_id"]))
        window_days = _date_range(row["required_start"], row["required_end"])

        min_day_available = 0.0
        if window_days:
            day_avail = []
            for day in window_days:
                day_pool_total = _pool_total_as_of(engine, bucket[0], bucket[1], day.date())
                day_committed = 0.0
                for prior in accepted.get(bucket, []):
                    if pd.to_datetime(prior["required_start"]) <= day and pd.to_datetime(prior["required_end"]) >= day:
                        day_committed += float(prior["quantity_assigned"])
                day_avail.append(max(float(day_pool_total) - float(day_committed), 0.0))
            min_day_available = min(day_avail) if day_avail else 0.0
        assigned = float(row["quantity_assigned"])
        if assigned > 0:
            accepted.setdefault(bucket, []).append(
                {
                    "required_start": pd.to_datetime(row["required_start"]).normalize(),
                    "required_end": pd.to_datetime(row["required_end"]).normalize(),
                    "quantity_assigned": assigned,
                }
            )

        debug_rows.append(
            {
                "requirement_id": int(row["id"]),
                "job_code": row["job_code"],
                "job_name": row["job_name"],
                "region_code": row["region_code"],
                "resource_class_id": int(row["resource_class_id"]),
                "class_name": row["class_name"],
                "priority": row["priority"],
                "required_start": row["required_start"],
                "required_end": row["required_end"],
                "quantity_required": float(row["quantity_required"]),
                "quantity_assigned": assigned,
                "quantity_shortfall": float(row["quantity_shortfall"]),
                "min_day_available": float(min_day_available),
                "days_in_window": len(window_days),
            }
        )

    return pd.DataFrame(debug_rows)
