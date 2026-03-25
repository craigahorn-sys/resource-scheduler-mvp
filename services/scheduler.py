from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from .db import execute, query_df
from .models import calc_job_dates


def _scalar(engine, sql, params=None):
    with engine.begin() as conn:
        return conn.execute(text(sql), params or {}).scalar()


def _next_job_code(engine, region_code: str, year: int) -> str:
    prefix = f"{region_code}-{year}-"
    df = query_df(engine, "SELECT job_code FROM jobs WHERE job_code LIKE :pfx", {'pfx': f'{prefix}%'})
    nums = []
    for jc in df['job_code'].tolist() if not df.empty else []:
        try:
            nums.append(int(jc.split('-')[-1]))
        except Exception:
            pass
    nxt = max(nums, default=0) + 1
    return f"{region_code}-{year}-{nxt:03d}"


def create_job(engine, data: dict) -> int:
    dates = calc_job_dates(data['job_start_date'], data['job_duration_days'], data['mob_days_before_job'], data['demob_days_after_job'])
    job_code = _next_job_code(engine, data['region_code'], pd.to_datetime(data['job_start_date']).year)
    with engine.begin() as conn:
        res = conn.execute(text("""
            INSERT INTO jobs(job_code, job_name, region_code, customer, location, job_start_date, job_duration_days, mob_days_before_job, demob_days_after_job, status, notes)
            VALUES (:job_code, :job_name, :region_code, :customer, :location, :job_start_date, :job_duration_days, :mob_days_before_job, :demob_days_after_job, :status, :notes)
            RETURNING id
        """), {**data, 'job_code': job_code, 'job_start_date': dates['job_start_date']})
        return int(res.scalar_one())


def get_jobs_df(engine):
    df = query_df(engine, "SELECT * FROM jobs ORDER BY id DESC")
    if df.empty:
        return df
    ends = df.apply(lambda r: calc_job_dates(r['job_start_date'], int(r['job_duration_days']), int(r['mob_days_before_job']), int(r['demob_days_after_job'])), axis=1)
    df['job_end_date'] = [d['job_end_date'] for d in ends]
    df['mob_start_date'] = [d['mob_start_date'] for d in ends]
    df['demob_end_date'] = [d['demob_end_date'] for d in ends]
    return df


def upsert_pool(engine, region_code: str, resource_class_id: int, base_quantity: float, notes: str = ''):
    execute(engine, """
        INSERT INTO resource_pools(region_code, resource_class_id, base_quantity, notes)
        VALUES (:region_code, :resource_class_id, :base_quantity, :notes)
        ON CONFLICT (region_code, resource_class_id) DO UPDATE
        SET base_quantity = EXCLUDED.base_quantity,
            notes = EXCLUDED.notes
    """, {
        'region_code': region_code,
        'resource_class_id': resource_class_id,
        'base_quantity': base_quantity,
        'notes': notes,
    })


def add_pool_adjustment(engine, data: dict):
    execute(engine, """
        INSERT INTO pool_adjustments(region_code, resource_class_id, quantity_change, adjustment_date, reason, notes)
        VALUES (:region_code, :resource_class_id, :quantity_change, :adjustment_date, :reason, :notes)
    """, data)


def _requirement_window(engine, requirement_id: int):
    df = query_df(engine, """
        SELECT jr.id, jr.job_id, jr.resource_class_id, jr.quantity_required, jr.days_before_job_start, jr.days_after_job_end,
               j.region_code, j.job_start_date, j.job_duration_days, j.mob_days_before_job, j.demob_days_after_job
        FROM job_requirements jr
        JOIN jobs j ON jr.job_id = j.id
        WHERE jr.id = :id
    """, {'id': requirement_id})
    if df.empty:
        return None
    r = df.iloc[0]
    dates = calc_job_dates(r['job_start_date'], int(r['job_duration_days']), int(r['mob_days_before_job']), int(r['demob_days_after_job']))
    req_start = pd.to_datetime(dates['job_start_date']) - pd.Timedelta(days=int(r['days_before_job_start']))
    req_end = pd.to_datetime(dates['job_end_date']) + pd.Timedelta(days=int(r['days_after_job_end']))
    return {
        'region_code': r['region_code'],
        'resource_class_id': int(r['resource_class_id']),
        'required_start': req_start.date(),
        'required_end': req_end.date(),
        'quantity_required': float(r['quantity_required']),
    }


def _pool_total_as_of(engine, region_code: str, resource_class_id: int, as_of_date):
    base = _scalar(engine, """
        SELECT COALESCE(base_quantity, 0)
        FROM resource_pools
        WHERE region_code = :r AND resource_class_id = :rc
    """, {'r': region_code, 'rc': resource_class_id}) or 0.0
    adj = _scalar(engine, """
        SELECT COALESCE(SUM(quantity_change), 0)
        FROM pool_adjustments
        WHERE region_code = :r AND resource_class_id = :rc AND adjustment_date <= :d
    """, {'r': region_code, 'rc': resource_class_id, 'd': as_of_date}) or 0.0
    return float(base) + float(adj)


def _overlapping_internal_allocations(engine, region_code: str, resource_class_id: int, start_date, end_date, exclude_requirement_id: int | None = None):
    sql = """
        SELECT COALESCE(SUM(rf.quantity_assigned), 0)
        FROM requirement_fulfillment rf
        JOIN job_requirements jr ON rf.requirement_id = jr.id
        JOIN jobs j ON jr.job_id = j.id
        WHERE rf.fulfillment_type = 'internal_pool'
          AND j.region_code = :r
          AND jr.resource_class_id = :rc
          AND jr.id != COALESCE(:exclude_requirement_id, -1)
          AND (j.job_start_date - (jr.days_before_job_start * INTERVAL '1 day')) <= :end_date
          AND (j.job_start_date + ((j.job_duration_days - 1 + jr.days_after_job_end) * INTERVAL '1 day')) >= :start_date
    """
    val = _scalar(engine, sql, {
        'r': region_code,
        'rc': resource_class_id,
        'start_date': start_date,
        'end_date': end_date,
        'exclude_requirement_id': exclude_requirement_id,
    }) or 0.0
    return float(val)


def _recalc_requirement_allocation(engine, requirement_id: int):
    info = _requirement_window(engine, requirement_id)
    if not info:
        return
    total_pool = _pool_total_as_of(engine, info['region_code'], info['resource_class_id'], info['required_start'])
    overlap = _overlapping_internal_allocations(engine, info['region_code'], info['resource_class_id'], info['required_start'], info['required_end'], exclude_requirement_id=requirement_id)
    available = max(total_pool - overlap, 0.0)
    assign_qty = min(info['quantity_required'], available)
    existing = _scalar(engine, """
        SELECT id
        FROM requirement_fulfillment
        WHERE requirement_id = :rid AND fulfillment_type = 'internal_pool'
    """, {'rid': requirement_id})
    if existing and assign_qty > 0:
        execute(engine, """
            UPDATE requirement_fulfillment
            SET quantity_assigned = :q,
                source_name = 'Internal Pool',
                notes = 'Auto-allocated'
            WHERE id = :id
        """, {'q': assign_qty, 'id': existing})
    elif existing and assign_qty <= 0:
        execute(engine, "DELETE FROM requirement_fulfillment WHERE id = :id", {'id': existing})
    elif assign_qty > 0:
        execute(engine, """
            INSERT INTO requirement_fulfillment(requirement_id, fulfillment_type, source_name, specific_resource_name, quantity_assigned, notes)
            VALUES (:rid, 'internal_pool', 'Internal Pool', NULL, :q, 'Auto-allocated')
        """, {'rid': requirement_id, 'q': assign_qty})


def create_requirement(engine, data: dict):
    with engine.begin() as conn:
        res = conn.execute(text("""
            INSERT INTO job_requirements(job_id, resource_class_id, quantity_required, days_before_job_start, days_after_job_end, priority, notes)
            VALUES (:job_id, :resource_class_id, :quantity_required, :days_before_job_start, :days_after_job_end, :priority, :notes)
            RETURNING id
        """), data)
        requirement_id = int(res.scalar_one())
    _recalc_requirement_allocation(engine, requirement_id)
    return requirement_id


def requirement_summary_df(engine):
    df = query_df(engine, """
        SELECT jr.id, j.job_code, j.job_name, j.region_code, rc.class_name, rc.unit_type, jr.quantity_required,
               jr.days_before_job_start, jr.days_after_job_end, jr.priority,
               j.job_start_date, j.job_duration_days, j.mob_days_before_job, j.demob_days_after_job,
               COALESCE(SUM(rf.quantity_assigned), 0) AS quantity_assigned
        FROM job_requirements jr
        JOIN jobs j ON jr.job_id = j.id
        JOIN resource_classes rc ON jr.resource_class_id = rc.id
        LEFT JOIN requirement_fulfillment rf ON rf.requirement_id = jr.id
        GROUP BY jr.id, j.job_code, j.job_name, j.region_code, rc.class_name, rc.unit_type, jr.quantity_required,
                 jr.days_before_job_start, jr.days_after_job_end, jr.priority,
                 j.job_start_date, j.job_duration_days, j.mob_days_before_job, j.demob_days_after_job
        ORDER BY jr.id DESC
    """)
    if df.empty:
        return df
    starts, ends = [], []
    for _, r in df.iterrows():
        dates = calc_job_dates(r['job_start_date'], int(r['job_duration_days']), int(r['mob_days_before_job']), int(r['demob_days_after_job']))
        starts.append((pd.to_datetime(dates['job_start_date']) - pd.Timedelta(days=int(r['days_before_job_start']))).date())
        ends.append((pd.to_datetime(dates['job_end_date']) + pd.Timedelta(days=int(r['days_after_job_end']))).date())
    df['required_start'] = starts
    df['required_end'] = ends
    df['quantity_assigned'] = df['quantity_assigned'].astype(float)
    df['quantity_shortfall'] = (df['quantity_required'].astype(float) - df['quantity_assigned']).clip(lower=0)

    def status(row):
        req, assigned = float(row['quantity_required']), float(row['quantity_assigned'])
        if assigned <= 0:
            return 'Unallocated'
        if assigned >= req:
            return 'Fully Allocated'
        return 'Fully Allocated with Shortfall'

    df['allocation_status'] = df.apply(status, axis=1)
    return df


def get_fulfillment_df(engine):
    return query_df(engine, """
        SELECT rf.*, j.job_code, j.job_name, j.region_code, rc.class_name, rc.unit_type,
               (j.job_start_date - (jr.days_before_job_start * INTERVAL '1 day'))::date AS required_start,
               (j.job_start_date + ((j.job_duration_days - 1 + jr.days_after_job_end) * INTERVAL '1 day'))::date AS required_end
        FROM requirement_fulfillment rf
        JOIN job_requirements jr ON rf.requirement_id = jr.id
        JOIN jobs j ON jr.job_id = j.id
        JOIN resource_classes rc ON jr.resource_class_id = rc.id
        ORDER BY rf.id DESC
    """)


def pool_snapshot_df(engine, as_of_date):
    df = query_df(engine, """
        SELECT rp.id, rp.region_code, rc.class_name, rc.category, rc.unit_type, rp.base_quantity,
               COALESCE((
                   SELECT SUM(pa.quantity_change)
                   FROM pool_adjustments pa
                   WHERE pa.region_code = rp.region_code
                     AND pa.resource_class_id = rp.resource_class_id
                     AND pa.adjustment_date <= :d
               ), 0) AS adjustment_total
        FROM resource_pools rp
        JOIN resource_classes rc ON rp.resource_class_id = rc.id
        ORDER BY rp.region_code, rc.category, rc.class_name
    """, {'d': as_of_date})
    if df.empty:
        return df
    req = requirement_summary_df(engine)
    committed = []
    for _, r in df.iterrows():
        q = 0.0
        if not req.empty:
            mask = (
                (req['region_code'] == r['region_code'])
                & (req['class_name'] == r['class_name'])
                & (pd.to_datetime(req['required_start']) <= pd.to_datetime(as_of_date))
                & (pd.to_datetime(req['required_end']) >= pd.to_datetime(as_of_date))
            )
            q = float(req.loc[mask, 'quantity_assigned'].sum())
        committed.append(q)
    df['base_quantity'] = df['base_quantity'].astype(float)
    df['adjustment_total'] = df['adjustment_total'].astype(float)
    df['total_pool'] = df['base_quantity'] + df['adjustment_total']
    df['committed_quantity'] = committed
    df['available_quantity'] = (df['total_pool'] - df['committed_quantity']).clip(lower=0)
    return df[['region_code', 'class_name', 'category', 'unit_type', 'base_quantity', 'adjustment_total', 'total_pool', 'committed_quantity', 'available_quantity']]
