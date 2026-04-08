from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from .db import execute, query_df
from .models import calc_job_dates


PRIORITY_RANK = {"Critical": 1, "High": 2, "Normal": 3, "Low": 4}
EXCLUDED_CALC_STATUSES = {"Bid", "Awarded"}


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
    dates = calc_job_dates(data["job_start_date"], data["job_duration_days"], data["mob_days_before_job"], data["demob_days_after_job"])
    job_code = _next_job_code(engine, data["region_code"], pd.to_datetime(data["job_start_date"]).year)
    with engine.begin() as conn:
        res = conn.execute(text("""
            INSERT INTO jobs(
                job_code, job_name, region_code, customer, customer_color, location,
                job_start_date, job_duration_days, mob_days_before_job,
                demob_days_after_job, status, notes
            ) VALUES (
                :job_code, :job_name, :region_code, :customer, :customer_color, :location,
                :job_start_date, :job_duration_days, :mob_days_before_job,
                :demob_days_after_job, :status, :notes
            ) RETURNING id
        """), {**data, "job_code": job_code, "job_start_date": dates["job_start_date"], "customer_color": data.get("customer_color", "")})
        return int(res.scalar_one())


def update_job(engine, job_id: int, data: dict):
    dates = calc_job_dates(data["job_start_date"], data["job_duration_days"], data["mob_days_before_job"], data["demob_days_after_job"])
    execute(engine, """
        UPDATE jobs
        SET job_name=:job_name, region_code=:region_code, customer=:customer, customer_color=:customer_color, location=:location,
            job_start_date=:job_start_date, job_duration_days=:job_duration_days,
            mob_days_before_job=:mob_days_before_job, demob_days_after_job=:demob_days_after_job,
            status=:status, notes=:notes
        WHERE id=:job_id
    """, {**data, "job_id": int(job_id), "job_start_date": dates["job_start_date"], "customer_color": data.get("customer_color", "")})
    recalc_all_requirements(engine)


def delete_job(engine, job_id: int):
    execute(engine, "DELETE FROM jobs WHERE id=:job_id", {"job_id": int(job_id)})
    recalc_all_requirements(engine)


def get_jobs_df(engine):
    df = query_df(engine, "SELECT * FROM jobs ORDER BY id DESC")
    if df.empty:
        return df
    ends = df.apply(lambda r: calc_job_dates(r["job_start_date"], int(r["job_duration_days"]), int(r["mob_days_before_job"]), int(r["demob_days_after_job"])), axis=1)
    df["job_end_date"] = [d["job_end_date"] for d in ends]
    df["mob_start_date"] = [d["mob_start_date"] for d in ends]
    df["demob_end_date"] = [d["demob_end_date"] for d in ends]
    return df


def _requirement_base_df(engine):
    df = query_df(engine, """
        SELECT
            jr.id, jr.job_id, jr.resource_class_id, jr.quantity_required, jr.days_before_job_start,
            jr.days_after_job_end, jr.priority, jr.notes,
            j.job_code, j.job_name, j.region_code, j.status, j.job_start_date, j.job_duration_days,
            j.mob_days_before_job, j.demob_days_after_job,
            rc.class_name, rc.category, rc.unit_type
        FROM job_requirements jr
        JOIN jobs j ON jr.job_id = j.id
        JOIN resource_classes rc ON jr.resource_class_id = rc.id
        ORDER BY jr.id
    """)
    if df.empty:
        return df
    req_start, req_end, ranks = [], [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(r["job_start_date"], int(r["job_duration_days"]), int(r["mob_days_before_job"]), int(r["demob_days_after_job"]))
        req_start.append((pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))).date())
        req_end.append((pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))).date())
        ranks.append(PRIORITY_RANK.get(str(r["priority"]), 999))
    df["required_start"] = req_start
    df["required_end"] = req_end
    df["priority_rank"] = ranks
    return df


def _pool_total_as_of(engine, region_code: str, resource_class_id: int, as_of_date):
    base = _scalar(engine, "SELECT COALESCE(base_quantity,0) FROM resource_pools WHERE region_code=:r AND resource_class_id=:rc", {"r": region_code, "rc": resource_class_id}) or 0.0
    adj = _scalar(engine, "SELECT COALESCE(SUM(quantity_change),0) FROM pool_adjustments WHERE region_code=:r AND resource_class_id=:rc AND adjustment_date<=:d", {"r": region_code, "rc": resource_class_id, "d": as_of_date}) or 0.0
    return float(base) + float(adj)


def _pool_total_as_of_conn(conn, region_code: str, resource_class_id: int, as_of_date):
    base = _scalar_conn(conn, "SELECT COALESCE(base_quantity,0) FROM resource_pools WHERE region_code=:r AND resource_class_id=:rc", {"r": region_code, "rc": resource_class_id}) or 0.0
    adj = _scalar_conn(conn, "SELECT COALESCE(SUM(quantity_change),0) FROM pool_adjustments WHERE region_code=:r AND resource_class_id=:rc AND adjustment_date<=:d", {"r": region_code, "rc": resource_class_id, "d": as_of_date}) or 0.0
    return float(base) + float(adj)


def _windows_overlap(start_a, end_a, start_b, end_b) -> bool:
    return pd.to_datetime(start_a) <= pd.to_datetime(end_b) and pd.to_datetime(end_a) >= pd.to_datetime(start_b)


def _migrate_null_requirement_ids(engine):
    """
    One-time migration that runs on every recalc: finds manual_owned_allocations
    records with requirement_id=NULL and links them to the correct job_requirement
    row by job_id + resource_class_id. Where exactly one match exists the id is
    written in-place. Where multiple requirements share the same job/class the
    quantity is split evenly across them. Duplicate NULL records for the same
    job/class are consolidated before matching.
    """
    null_records = query_df(engine, """
        SELECT id, job_id, resource_class_id, quantity_assigned,
               days_before_job_start, days_after_job_end, notes
        FROM job_manual_owned_allocations
        WHERE requirement_id IS NULL
    """)
    if null_records.empty:
        return

    reqs = query_df(engine, """
        SELECT id AS requirement_id, job_id, resource_class_id
        FROM job_requirements
    """)
    if reqs.empty:
        return

    # Consolidate multiple NULL records for the same job/class into one quantity
    consolidated = null_records.groupby(
        ["job_id", "resource_class_id"], as_index=False
    ).agg(
        quantity_assigned=("quantity_assigned", "sum"),
        days_before_job_start=("days_before_job_start", "first"),
        days_after_job_end=("days_after_job_end", "first"),
        notes=("notes", "first"),
        null_ids=("id", list),
    )

    with engine.begin() as conn:
        for _, row in consolidated.iterrows():
            matches = reqs[
                (reqs["job_id"] == row["job_id"]) &
                (reqs["resource_class_id"] == row["resource_class_id"])
            ]
            if matches.empty:
                continue

            # Delete all the NULL records for this job/class
            for null_id in row["null_ids"]:
                conn.execute(
                    text("DELETE FROM job_manual_owned_allocations WHERE id=:id"),
                    {"id": int(null_id)},
                )

            # Also delete any existing requirement_id-linked records for these
            # requirements so we don't create duplicates
            for req_id in matches["requirement_id"].tolist():
                conn.execute(
                    text("DELETE FROM job_manual_owned_allocations WHERE requirement_id=:rid"),
                    {"rid": int(req_id)},
                )

            # Distribute quantity evenly across matched requirements
            qty_each = float(row["quantity_assigned"]) / len(matches)
            for req_id in matches["requirement_id"].tolist():
                conn.execute(text("""
                    INSERT INTO job_manual_owned_allocations(
                        job_id, requirement_id, resource_class_id,
                        quantity_assigned, days_before_job_start, days_after_job_end, notes
                    ) VALUES (
                        :job_id, :requirement_id, :resource_class_id,
                        :quantity_assigned, :days_before_job_start, :days_after_job_end, :notes
                    )
                """), {
                    "job_id": int(row["job_id"]),
                    "requirement_id": int(req_id),
                    "resource_class_id": int(row["resource_class_id"]),
                    "quantity_assigned": qty_each,
                    "days_before_job_start": int(row["days_before_job_start"]),
                    "days_after_job_end": int(row["days_after_job_end"]),
                    "notes": str(row["notes"] or ""),
                })


def recalc_all_requirements(engine):
    _migrate_null_requirement_ids(engine)
    req = _requirement_base_df(engine)
    manual_df = _manual_owned_allocations_base_df(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM requirement_fulfillment WHERE fulfillment_type='internal_pool'"))
        if req.empty:
            return
        req = req.loc[~req["status"].astype(str).isin(EXCLUDED_CALC_STATUSES)].copy()
        manual_df = manual_df.loc[~manual_df["status"].astype(str).isin(EXCLUDED_CALC_STATUSES)].copy() if not manual_df.empty else manual_df
        if req.empty:
            return
        req = req.sort_values(by=["priority_rank", "required_start", "job_start_date", "id"], ascending=[True, True, True, True]).reset_index(drop=True)
        allocations_by_bucket = {}
        for _, row in req.iterrows():
            bucket = (str(row["region_code"]), int(row["resource_class_id"]))
            total_pool = _pool_total_as_of_conn(conn, str(row["region_code"]), int(row["resource_class_id"]), row["required_start"])
            prior_allocs = allocations_by_bucket.get(bucket, [])
            overlapping_committed = 0.0
            for alloc in prior_allocs:
                if _windows_overlap(row["required_start"], row["required_end"], alloc["required_start"], alloc["required_end"]):
                    overlapping_committed += float(alloc["quantity_assigned"])
            available = max(float(total_pool) - float(overlapping_committed), 0.0)

            manual_target = None
            if not manual_df.empty:
                row_specific_match = pd.DataFrame()
                if "requirement_id" in manual_df.columns:
                    row_specific_match = manual_df.loc[manual_df["requirement_id"] == row["id"]].copy()
                if not row_specific_match.empty:
                    manual_target = float(row_specific_match["quantity_assigned"].astype(float).sum())
                else:
                    manual_match = manual_df[
                        (manual_df["job_id"] == row["job_id"]) &
                        (manual_df["resource_class_id"] == row["resource_class_id"])
                    ].copy()
                    if "requirement_id" in manual_match.columns:
                        manual_match = manual_match.loc[manual_match["requirement_id"].isna()].copy()
                    if not manual_match.empty:
                        same_bucket_count = int(req.loc[(req["job_id"] == row["job_id"]) & (req["resource_class_id"] == row["resource_class_id"])].shape[0])
                        if same_bucket_count == 1:
                            overlap_match = manual_match[
                                (pd.to_datetime(manual_match["required_start"]) <= pd.to_datetime(row["required_end"])) &
                                (pd.to_datetime(manual_match["required_end"]) >= pd.to_datetime(row["required_start"]))
                            ]
                            if not overlap_match.empty:
                                manual_target = float(overlap_match["quantity_assigned"].astype(float).sum())

            target_qty = float(row["quantity_required"]) if manual_target is None else min(float(row["quantity_required"]), float(manual_target))
            assign_qty = min(target_qty, available)
            if assign_qty > 0:
                note_prefix = "Manual EES override" if manual_target is not None else "Auto-allocated by priority"
                conn.execute(text("""
                    INSERT INTO requirement_fulfillment(
                        requirement_id, fulfillment_type, source_name, specific_resource_name, quantity_assigned, notes
                    ) VALUES (:rid, 'internal_pool', 'Internal Pool', NULL, :q, :notes)
                """), {
                    "rid": int(row["id"]),
                    "q": float(assign_qty),
                    "notes": f"{note_prefix}. Pool={float(total_pool):.2f}, overlap_committed={float(overlapping_committed):.2f}, available={float(available):.2f}, target={float(target_qty):.2f}"
                })
                allocations_by_bucket.setdefault(bucket, []).append({
                    "requirement_id": int(row["id"]),
                    "required_start": row["required_start"],
                    "required_end": row["required_end"],
                    "quantity_assigned": float(assign_qty),
                })


def upsert_pool(engine, region_code: str, resource_class_id: int, base_quantity: float, notes: str = ""):
    execute(engine, """
        INSERT INTO resource_pools(region_code, resource_class_id, base_quantity, notes)
        VALUES (:region_code,:resource_class_id,:base_quantity,:notes)
        ON CONFLICT (region_code, resource_class_id) DO UPDATE
        SET base_quantity=EXCLUDED.base_quantity, notes=EXCLUDED.notes
    """, {"region_code": str(region_code), "resource_class_id": int(resource_class_id), "base_quantity": float(base_quantity), "notes": str(notes) if notes is not None else ""})
    recalc_all_requirements(engine)


def delete_pool(engine, pool_id: int):
    execute(engine, "DELETE FROM resource_pools WHERE id=:pool_id", {"pool_id": int(pool_id)})
    recalc_all_requirements(engine)


def add_pool_adjustment(engine, data: dict):
    execute(engine, """
        INSERT INTO pool_adjustments(region_code, resource_class_id, quantity_change, adjustment_date, reason, notes)
        VALUES (:region_code, :resource_class_id, :quantity_change, :adjustment_date, :reason, :notes)
    """, data)
    recalc_all_requirements(engine)


def delete_pool_adjustment(engine, adjustment_id: int):
    execute(engine, "DELETE FROM pool_adjustments WHERE id=:id", {"id": int(adjustment_id)})
    recalc_all_requirements(engine)


def create_requirement(engine, data: dict):
    with engine.begin() as conn:
        res = conn.execute(text("""
            INSERT INTO job_requirements(job_id, resource_class_id, quantity_required, days_before_job_start, days_after_job_end, priority, notes)
            VALUES (:job_id, :resource_class_id, :quantity_required, :days_before_job_start, :days_after_job_end, :priority, :notes)
            RETURNING id
        """), data)
        requirement_id = int(res.scalar_one())
    recalc_all_requirements(engine)
    return requirement_id


def update_requirement(engine, requirement_id: int, data: dict):
    execute(engine, """
        UPDATE job_requirements
        SET resource_class_id=:resource_class_id, quantity_required=:quantity_required,
            days_before_job_start=:days_before_job_start, days_after_job_end=:days_after_job_end,
            priority=:priority, notes=:notes
        WHERE id=:requirement_id
    """, {
        "requirement_id": int(requirement_id),
        "resource_class_id": int(data["resource_class_id"]),
        "quantity_required": float(data["quantity_required"]),
        "days_before_job_start": int(data["days_before_job_start"]),
        "days_after_job_end": int(data["days_after_job_end"]),
        "priority": data["priority"],
        "notes": data.get("notes", ""),
    })
    recalc_all_requirements(engine)


def delete_requirement(engine, requirement_id: int):
    execute(engine, "DELETE FROM job_requirements WHERE id=:requirement_id", {"requirement_id": int(requirement_id)})
    recalc_all_requirements(engine)


def requirement_summary_df(engine):
    df = query_df(engine, """
        SELECT
            jr.id, jr.job_id, jr.resource_class_id, j.job_code, j.job_name, j.region_code, j.status,
            COALESCE(j.customer, '') AS customer,
            COALESCE(j.customer_color, '') AS customer_color,
            rc.class_name, rc.unit_type,
            jr.quantity_required, jr.days_before_job_start, jr.days_after_job_end, jr.priority, jr.notes,
            j.job_start_date, j.job_duration_days, j.mob_days_before_job, j.demob_days_after_job,
            COALESCE(SUM(rf.quantity_assigned),0) AS quantity_assigned
        FROM job_requirements jr
        JOIN jobs j ON jr.job_id=j.id
        JOIN resource_classes rc ON jr.resource_class_id=rc.id
        LEFT JOIN requirement_fulfillment rf ON rf.requirement_id=jr.id
        GROUP BY jr.id, jr.job_id, jr.resource_class_id, j.job_code, j.job_name, j.region_code, j.status, j.customer, j.customer_color,
            rc.class_name, rc.unit_type,
            jr.quantity_required, jr.days_before_job_start, jr.days_after_job_end, jr.priority, jr.notes,
            j.job_start_date, j.job_duration_days, j.mob_days_before_job, j.demob_days_after_job
        ORDER BY jr.id DESC
    """)
    if df.empty:
        return df
    starts, ends = [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(r["job_start_date"], int(r["job_duration_days"]), int(r["mob_days_before_job"]), int(r["demob_days_after_job"]))
        starts.append((pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))).date())
        ends.append((pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))).date())
    df["required_start"] = starts
    df["required_end"] = ends
    df["quantity_assigned"] = df["quantity_assigned"].astype(float)
    df["quantity_shortfall"] = (df["quantity_required"].astype(float) - df["quantity_assigned"]).clip(lower=0)
    def status(row):
        req = float(row["quantity_required"]); assigned = float(row["quantity_assigned"]); shortfall = max(req-assigned,0.0)
        if assigned <= 0: return "Unallocated"
        if shortfall <= 0: return "Fully Allocated"
        return "Partially Allocated with Shortfall"
    df["allocation_status"] = df.apply(status, axis=1)
    return df


def get_requirements_df(engine):
    return requirement_summary_df(engine)


def get_fulfillment_df(engine):
    df = query_df(engine, """
        SELECT
            rf.*, jr.days_before_job_start, jr.days_after_job_end,
            j.job_code, j.job_name, j.region_code, j.status, j.job_start_date, j.job_duration_days, j.mob_days_before_job, j.demob_days_after_job,
            rc.class_name, rc.unit_type
        FROM requirement_fulfillment rf
        JOIN job_requirements jr ON rf.requirement_id=jr.id
        JOIN jobs j ON jr.job_id=j.id
        JOIN resource_classes rc ON jr.resource_class_id=rc.id
        ORDER BY rf.id DESC
    """)
    if df.empty:
        return df
    starts, ends = [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(r["job_start_date"], int(r["job_duration_days"]), int(r["mob_days_before_job"]), int(r["demob_days_after_job"]))
        starts.append((pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))).date())
        ends.append((pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))).date())
    df["required_start"] = starts
    df["required_end"] = ends
    return df


def get_pools_df(engine):
    return query_df(engine, """
        SELECT rp.id, rp.region_code, rp.resource_class_id, rc.class_name, rc.category, rc.unit_type, rp.base_quantity, rp.notes
        FROM resource_pools rp
        JOIN resource_classes rc ON rp.resource_class_id=rc.id
        ORDER BY rp.region_code, rp.id
    """)


def get_pool_adjustments_df(engine):
    return query_df(engine, """
        SELECT pa.id, pa.region_code, pa.resource_class_id, rc.class_name, rc.unit_type, pa.quantity_change, pa.adjustment_date, pa.reason, pa.notes, pa.created_at
        FROM pool_adjustments pa
        JOIN resource_classes rc ON pa.resource_class_id=rc.id
        ORDER BY pa.id DESC
    """)


def pool_snapshot_df(engine, as_of_date):
    df = query_df(engine, """
        SELECT
            rp.id, rp.region_code, rp.resource_class_id, rc.class_name, rc.category, rc.unit_type, rp.base_quantity,
            COALESCE((SELECT SUM(pa.quantity_change) FROM pool_adjustments pa WHERE pa.region_code=rp.region_code AND pa.resource_class_id=rp.resource_class_id AND pa.adjustment_date<=:d),0) AS adjustment_total
        FROM resource_pools rp
        JOIN resource_classes rc ON rp.resource_class_id=rc.id
        ORDER BY rp.region_code, rp.id
    """, {"d": as_of_date})
    if df.empty:
        return df
    req = requirement_summary_df(engine)
    if not req.empty and "status" in req.columns:
        req = req.loc[~req["status"].astype(str).isin(EXCLUDED_CALC_STATUSES)].copy()
    committed, pool_status = [], []
    for _, r in df.iterrows():
        q = 0.0; related_shortfall = 0.0
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
        available = total_pool - q
        if related_shortfall > 0: pool_status.append("Fully Allocated with Shortfall")
        elif q > 0 and available <= 0: pool_status.append("Fully Allocated")
        elif q > 0 and available > 0: pool_status.append("Partially Committed")
        else: pool_status.append("Available")
    df["base_quantity"] = df["base_quantity"].astype(float)
    df["adjustment_total"] = df["adjustment_total"].astype(float)
    df["total_pool"] = df["base_quantity"] + df["adjustment_total"]
    df["committed_quantity"] = committed
    df["available_quantity"] = df["total_pool"] - df["committed_quantity"]
    df["pool_status"] = pool_status
    return df[["id","region_code","resource_class_id","class_name","category","unit_type","base_quantity","adjustment_total","total_pool","committed_quantity","available_quantity","pool_status"]]


def allocation_debug_df(engine):
    req = _requirement_base_df(engine)
    if req.empty:
        return req
    req = req.loc[~req["status"].astype(str).isin(EXCLUDED_CALC_STATUSES)].copy()
    if req.empty:
        return req
    assigned_df = query_df(engine, """
        SELECT requirement_id, COALESCE(SUM(quantity_assigned),0) AS quantity_assigned
        FROM requirement_fulfillment
        WHERE fulfillment_type='internal_pool'
        GROUP BY requirement_id
    """)
    if assigned_df.empty:
        assigned_df = pd.DataFrame(columns=["requirement_id", "quantity_assigned"])
    req = req.merge(assigned_df, how="left", left_on="id", right_on="requirement_id")
    req["quantity_assigned"] = req["quantity_assigned"].fillna(0.0).astype(float)
    debug_rows = []
    sorted_req = req.sort_values(by=["region_code","resource_class_id","priority_rank","required_start","job_start_date","id"], ascending=[True,True,True,True,True,True]).reset_index(drop=True)
    for pos, (_, row) in enumerate(sorted_req.iterrows()):
        total_pool = _pool_total_as_of(engine, str(row["region_code"]), int(row["resource_class_id"]), row["required_start"])
        overlapping_assigned = 0.0
        prior_rows = sorted_req.iloc[:pos]
        same_bucket = prior_rows[(prior_rows["region_code"]==row["region_code"]) & (prior_rows["resource_class_id"]==row["resource_class_id"])]
        for _, other in same_bucket.iterrows():
            if _windows_overlap(row["required_start"], row["required_end"], other["required_start"], other["required_end"]):
                overlapping_assigned += float(other["quantity_assigned"])
        available_estimate = max(float(total_pool) - float(overlapping_assigned), 0.0)
        debug_rows.append({
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
            "pool_total_as_of_start": float(total_pool),
            "overlapping_prior_assigned": float(overlapping_assigned),
            "available_estimate": float(available_estimate),
        })
    return pd.DataFrame(debug_rows)


def _rental_requirements_base_df(engine):
    df = query_df(engine, """
        SELECT
            rr.id,
            rr.job_id,
            rr.requirement_id,
            rr.resource_class_id,
            rr.quantity_required,
            rr.days_before_job_start,
            rr.days_after_job_end,
            rr.vendor_name,
            rr.notes,
            j.job_code,
            j.job_name,
            j.region_code,
            j.status,
            COALESCE(j.customer, '') AS customer,
            COALESCE(j.customer_color, '') AS customer_color,
            j.job_start_date,
            j.job_duration_days,
            j.mob_days_before_job,
            j.demob_days_after_job,
            rc.class_name,
            rc.category,
            rc.unit_type
        FROM job_rental_requirements rr
        JOIN jobs j ON rr.job_id = j.id
        JOIN resource_classes rc ON rr.resource_class_id = rc.id
        ORDER BY rr.id
    """)
    if df.empty:
        return df
    req_start, req_end = [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(r["job_start_date"], int(r["job_duration_days"]), int(r["mob_days_before_job"]), int(r["demob_days_after_job"]))
        req_start.append((pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))).date())
        req_end.append((pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))).date())
    df["required_start"] = req_start
    df["required_end"] = req_end
    return df


def _manual_owned_allocations_base_df(engine):
    df = query_df(engine, """
        SELECT
            mo.id,
            mo.job_id,
            mo.requirement_id,
            mo.resource_class_id,
            mo.quantity_assigned,
            mo.days_before_job_start,
            mo.days_after_job_end,
            mo.notes,
            j.job_code,
            j.job_name,
            j.region_code,
            j.status,
            COALESCE(j.customer, '') AS customer,
            COALESCE(j.customer_color, '') AS customer_color,
            j.job_start_date,
            j.job_duration_days,
            j.mob_days_before_job,
            j.demob_days_after_job,
            rc.class_name,
            rc.category,
            rc.unit_type
        FROM job_manual_owned_allocations mo
        JOIN jobs j ON mo.job_id = j.id
        JOIN resource_classes rc ON mo.resource_class_id = rc.id
        ORDER BY mo.id
    """)
    if df.empty:
        return df
    req_start, req_end = [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(r["job_start_date"], int(r["job_duration_days"]), int(r["mob_days_before_job"]), int(r["demob_days_after_job"]))
        req_start.append((pd.to_datetime(dates["job_start_date"]) - pd.Timedelta(days=int(r["days_before_job_start"]))).date())
        req_end.append((pd.to_datetime(dates["job_end_date"]) + pd.Timedelta(days=int(r["days_after_job_end"]))).date())
    df["required_start"] = req_start
    df["required_end"] = req_end
    return df


def create_rental_requirement(engine, data: dict):
    with engine.begin() as conn:
        res = conn.execute(text("""
            INSERT INTO job_rental_requirements(
                job_id, resource_class_id, quantity_required, days_before_job_start, days_after_job_end, vendor_name, notes
            ) VALUES (
                :job_id, :resource_class_id, :quantity_required, :days_before_job_start, :days_after_job_end, :vendor_name, :notes
            ) RETURNING id
        """), data)
        rental_id = int(res.scalar_one())
    recalc_all_requirements(engine)
    return rental_id


def delete_rental_requirement(engine, rental_requirement_id: int):
    execute(engine, "DELETE FROM job_rental_requirements WHERE id=:id", {"id": int(rental_requirement_id)})
    recalc_all_requirements(engine)


def get_rental_requirements_df(engine):
    return _rental_requirements_base_df(engine)


def upsert_rental_requirement_for_job_class(engine, job_id: int, resource_class_id: int, quantity_required: float, days_before_job_start: int, days_after_job_end: int, vendor_name: str, notes: str = "", requirement_id: int | None = None):
    with engine.begin() as conn:
        if requirement_id is not None:
            # Delete only the rental record tied to this specific requirement
            conn.execute(text("DELETE FROM job_rental_requirements WHERE requirement_id=:requirement_id"), {"requirement_id": int(requirement_id)})
            # Also clean up any legacy NULL-requirement_id records for this job/class
            conn.execute(text("DELETE FROM job_rental_requirements WHERE job_id=:job_id AND resource_class_id=:resource_class_id AND requirement_id IS NULL"), {"job_id": int(job_id), "resource_class_id": int(resource_class_id)})
        else:
            conn.execute(text("DELETE FROM job_rental_requirements WHERE job_id=:job_id AND resource_class_id=:resource_class_id"), {"job_id": int(job_id), "resource_class_id": int(resource_class_id)})
        if float(quantity_required) > 0:
            conn.execute(text("""
                INSERT INTO job_rental_requirements(
                    job_id, requirement_id, resource_class_id, quantity_required, days_before_job_start, days_after_job_end, vendor_name, notes
                ) VALUES (
                    :job_id, :requirement_id, :resource_class_id, :quantity_required, :days_before_job_start, :days_after_job_end, :vendor_name, :notes
                )
            """), {
                "job_id": int(job_id),
                "requirement_id": int(requirement_id) if requirement_id is not None else None,
                "resource_class_id": int(resource_class_id),
                "quantity_required": float(quantity_required),
                "days_before_job_start": int(days_before_job_start),
                "days_after_job_end": int(days_after_job_end),
                "vendor_name": str(vendor_name or "").strip(),
                "notes": str(notes or ""),
            })
    recalc_all_requirements(engine)


def create_manual_owned_allocation(engine, data: dict):
    with engine.begin() as conn:
        res = conn.execute(text("""
            INSERT INTO job_manual_owned_allocations(
                job_id, resource_class_id, quantity_assigned, days_before_job_start, days_after_job_end, notes
            ) VALUES (
                :job_id, :resource_class_id, :quantity_assigned, :days_before_job_start, :days_after_job_end, :notes
            ) RETURNING id
        """), data)
        return int(res.scalar_one())


def delete_manual_owned_allocation(engine, manual_allocation_id: int):
    execute(engine, "DELETE FROM job_manual_owned_allocations WHERE id=:id", {"id": int(manual_allocation_id)})
    recalc_all_requirements(engine)


def upsert_manual_owned_allocation_for_job_class(engine, job_id: int, resource_class_id: int, quantity_assigned: float, days_before_job_start: int, days_after_job_end: int, notes: str = "", requirement_id: int | None = None):
    with engine.begin() as conn:
        if requirement_id is not None:
            # Delete the record tied to this specific requirement
            conn.execute(
                text("DELETE FROM job_manual_owned_allocations WHERE requirement_id=:requirement_id"),
                {"requirement_id": int(requirement_id)},
            )
            # Also clean up any legacy NULL-requirement_id records for the same job/class
            # (created before requirement_id column existed, or via old save paths)
            conn.execute(
                text("DELETE FROM job_manual_owned_allocations WHERE job_id=:job_id AND resource_class_id=:resource_class_id AND requirement_id IS NULL"),
                {"job_id": int(job_id), "resource_class_id": int(resource_class_id)},
            )
        else:
            conn.execute(
                text("DELETE FROM job_manual_owned_allocations WHERE job_id=:job_id AND resource_class_id=:resource_class_id AND requirement_id IS NULL"),
                {"job_id": int(job_id), "resource_class_id": int(resource_class_id)},
            )
        conn.execute(text("""
                INSERT INTO job_manual_owned_allocations(
                    job_id, requirement_id, resource_class_id, quantity_assigned, days_before_job_start, days_after_job_end, notes
                ) VALUES (
                    :job_id, :requirement_id, :resource_class_id, :quantity_assigned, :days_before_job_start, :days_after_job_end, :notes
                )
            """), {
                "job_id": int(job_id),
                "requirement_id": int(requirement_id) if requirement_id is not None else None,
                "resource_class_id": int(resource_class_id),
                "quantity_assigned": float(quantity_assigned),
                "days_before_job_start": int(days_before_job_start),
                "days_after_job_end": int(days_after_job_end),
                "notes": str(notes or ""),
            })
    recalc_all_requirements(engine)


def get_manual_owned_allocations_df(engine):
    return _manual_owned_allocations_base_df(engine)
