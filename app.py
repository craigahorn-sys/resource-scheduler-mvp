import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).with_name('resource_scheduler.db')

REGIONS = [
    ('RM', 'Rockies', 1),
    ('PM', 'Permian', 1),
    ('ST', 'South Texas', 1),
]

RESOURCE_CLASSES = [
    ('6" Layflat Hose', 'Hose', 'miles', 'quantity_only'),
    ('8" Layflat Hose', 'Hose', 'miles', 'quantity_only'),
    ('10" Layflat Hose', 'Hose', 'miles', 'quantity_only'),
    ('12" Layflat Hose', 'Hose', 'miles', 'quantity_only'),
    ('14" Layflat Hose', 'Hose', 'miles', 'quantity_only'),
    ('16" Layflat Hose', 'Hose', 'miles', 'quantity_only'),
    ('6" Clamps', 'Clamps', 'count', 'quantity_only'),
    ('8" Clamps', 'Clamps', 'count', 'quantity_only'),
    ('10" Clamps', 'Clamps', 'count', 'quantity_only'),
    ('12" Clamps', 'Clamps', 'count', 'quantity_only'),
    ('14" Clamps', 'Clamps', 'count', 'quantity_only'),
    ('16" Clamps', 'Clamps', 'count', 'quantity_only'),
    ('Low-Profile Road Crossings', 'Road Crossings', 'count', 'quantity_only'),
    ('12" Road Crossings', 'Road Crossings', 'count', 'quantity_only'),
    ('16" Road Crossings', 'Road Crossings', 'count', 'quantity_only'),
    ('Rental Hose Placeholder', 'Rental', 'miles', 'quantity_only'),
    ('Operators', 'Personnel', 'people', 'quantity_then_specific'),
    ('Trucks', 'Vehicles', 'units', 'quantity_then_specific'),
    ('4x3 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('4x4 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('6x3 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('Super 6x4 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('6x6 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('10x8 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('8x6 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('Super 8x6 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('12x8 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('12x10 Pumps', 'Pumps', 'units', 'quantity_then_specific'),
    ('6" Filter Pods', 'Filtration', 'units', 'quantity_then_specific'),
    ('10" Filter Pods', 'Filtration', 'units', 'quantity_then_specific'),
    ('12" Filter Pods', 'Filtration', 'units', 'quantity_then_specific'),
    ('375 Air Compressor', 'Air', 'units', 'quantity_then_specific'),
    ('750 Air Compressor', 'Air', 'units', 'quantity_then_specific'),
    ('900 Air Compressor', 'Air', 'units', 'quantity_then_specific'),
    ('1200 Air Compressor', 'Air', 'units', 'quantity_then_specific'),
    ('Doghouses', 'Support', 'units', 'quantity_then_specific'),
    ('Rental Pump Placeholder', 'Rental', 'units', 'quantity_then_specific'),
    ('Rental Truck Placeholder', 'Rental', 'units', 'quantity_then_specific'),
    ('Rental Filter Pod Placeholder', 'Rental', 'units', 'quantity_then_specific'),
]


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript(
        '''
        CREATE TABLE IF NOT EXISTS regions (
            region_code TEXT PRIMARY KEY,
            region_name TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS resource_classes (
            resource_class_id INTEGER PRIMARY KEY AUTOINCREMENT,
            class_name TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL,
            unit_type TEXT NOT NULL,
            planning_mode TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS jobs (
            job_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_code TEXT UNIQUE,
            job_name TEXT NOT NULL,
            region_code TEXT NOT NULL,
            customer TEXT,
            location TEXT,
            job_start TEXT NOT NULL,
            job_end TEXT NOT NULL,
            mobilization_start TEXT,
            demobilization_end TEXT,
            status TEXT NOT NULL,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(region_code) REFERENCES regions(region_code)
        );

        CREATE TABLE IF NOT EXISTS resource_pools (
            pool_id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_code TEXT NOT NULL,
            resource_class_id INTEGER NOT NULL,
            quantity_total REAL NOT NULL,
            notes TEXT,
            UNIQUE(region_code, resource_class_id),
            FOREIGN KEY(region_code) REFERENCES regions(region_code),
            FOREIGN KEY(resource_class_id) REFERENCES resource_classes(resource_class_id)
        );

        CREATE TABLE IF NOT EXISTS pool_adjustments (
            adjustment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            region_code TEXT NOT NULL,
            resource_class_id INTEGER NOT NULL,
            adjustment_date TEXT NOT NULL,
            quantity_change REAL NOT NULL,
            reason TEXT NOT NULL,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(region_code) REFERENCES regions(region_code),
            FOREIGN KEY(resource_class_id) REFERENCES resource_classes(resource_class_id)
        );

        CREATE TABLE IF NOT EXISTS job_requirements (
            requirement_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            resource_class_id INTEGER NOT NULL,
            quantity_required REAL NOT NULL,
            required_start TEXT NOT NULL,
            required_end TEXT NOT NULL,
            priority TEXT,
            notes TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(job_id),
            FOREIGN KEY(resource_class_id) REFERENCES resource_classes(resource_class_id)
        );

        CREATE TABLE IF NOT EXISTS fulfillment (
            fulfillment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            requirement_id INTEGER NOT NULL,
            fulfillment_type TEXT NOT NULL,
            source_region_code TEXT,
            specific_resource_name TEXT,
            source_name TEXT,
            quantity_assigned REAL NOT NULL,
            assigned_start TEXT NOT NULL,
            assigned_end TEXT NOT NULL,
            notes TEXT,
            FOREIGN KEY(requirement_id) REFERENCES job_requirements(requirement_id),
            FOREIGN KEY(source_region_code) REFERENCES regions(region_code)
        );
        '''
    )
    cur.executemany(
        'INSERT OR IGNORE INTO regions(region_code, region_name, active) VALUES (?, ?, ?)',
        REGIONS,
    )
    cur.executemany(
        'INSERT OR IGNORE INTO resource_classes(class_name, category, unit_type, planning_mode) VALUES (?, ?, ?, ?)',
        RESOURCE_CLASSES,
    )
    conn.commit()
    conn.close()


def run_query(query, params=None):
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params or [])
    conn.close()
    return df


def execute(query, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query, params or [])
    conn.commit()
    lastrowid = cur.lastrowid
    conn.close()
    return lastrowid


def executemany(query, values):
    conn = get_conn()
    cur = conn.cursor()
    cur.executemany(query, values)
    conn.commit()
    conn.close()


def generate_job_code(region_code: str, job_start: str) -> str:
    year = datetime.fromisoformat(job_start).year
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        '''SELECT COUNT(*) FROM jobs WHERE region_code = ? AND substr(job_start,1,4) = ?''',
        (region_code, str(year)),
    )
    count = cur.fetchone()[0] + 1
    conn.close()
    return f'{region_code}-{year}-{count:03d}'


def add_job(job_name, region_code, customer, location, job_start, job_end, mobilization_start, demobilization_end, status, notes):
    job_code = generate_job_code(region_code, job_start)
    execute(
        '''INSERT INTO jobs(job_code, job_name, region_code, customer, location, job_start, job_end,
           mobilization_start, demobilization_end, status, notes)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        [job_code, job_name, region_code, customer, location, job_start, job_end,
         mobilization_start, demobilization_end, status, notes],
    )
    return job_code


def add_or_update_pool(region_code, resource_class_id, quantity_total, notes):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        'SELECT pool_id, quantity_total FROM resource_pools WHERE region_code = ? AND resource_class_id = ?',
        (region_code, resource_class_id),
    )
    row = cur.fetchone()
    if row:
        cur.execute(
            'UPDATE resource_pools SET quantity_total = ?, notes = ? WHERE pool_id = ?',
            (quantity_total, notes, row['pool_id']),
        )
    else:
        cur.execute(
            'INSERT INTO resource_pools(region_code, resource_class_id, quantity_total, notes) VALUES (?, ?, ?, ?)',
            (region_code, resource_class_id, quantity_total, notes),
        )
    conn.commit()
    conn.close()


def apply_pool_adjustment(region_code, resource_class_id, adjustment_date, quantity_change, reason, notes):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        '''INSERT INTO pool_adjustments(region_code, resource_class_id, adjustment_date, quantity_change, reason, notes)
           VALUES (?, ?, ?, ?, ?, ?)''',
        (region_code, resource_class_id, adjustment_date, quantity_change, reason, notes),
    )
    cur.execute(
        'SELECT quantity_total FROM resource_pools WHERE region_code = ? AND resource_class_id = ?',
        (region_code, resource_class_id),
    )
    row = cur.fetchone()
    if row:
        new_total = float(row['quantity_total']) + float(quantity_change)
        cur.execute(
            'UPDATE resource_pools SET quantity_total = ? WHERE region_code = ? AND resource_class_id = ?',
            (new_total, region_code, resource_class_id),
        )
    else:
        cur.execute(
            'INSERT INTO resource_pools(region_code, resource_class_id, quantity_total, notes) VALUES (?, ?, ?, ?)',
            (region_code, resource_class_id, quantity_change, f'Created by adjustment: {reason}'),
        )
    conn.commit()
    conn.close()


def style_metric(title, value, help_text=''):
    st.metric(title, value, help=help_text)


def get_lookup_dict(df, key_col, value_col):
    return dict(zip(df[key_col], df[value_col]))


init_db()
st.set_page_config(page_title='Resource Scheduler MVP', layout='wide')
st.title('Resource Scheduler MVP')
st.caption('Jobs, requirements, fulfillment, resource pools, calendar, and Gantt views.')

regions_df = run_query('SELECT region_code, region_name FROM regions WHERE active = 1 ORDER BY region_code')
resource_classes_df = run_query('SELECT * FROM resource_classes ORDER BY category, class_name')
region_options = get_lookup_dict(regions_df, 'region_code', 'region_name')

with st.sidebar:
    st.header('Navigation')
    page = st.radio(
        'Go to',
        ['Dashboard', 'Jobs', 'Requirements', 'Fulfillment', 'Resource Pools', 'Calendar', 'Gantt'],
    )

if page == 'Dashboard':
    jobs_df = run_query('SELECT * FROM jobs ORDER BY job_start DESC')
    req_summary = run_query(
        '''
        SELECT COUNT(*) AS req_count, COALESCE(SUM(quantity_required), 0) AS qty_required
        FROM job_requirements
        '''
    )
    ful_summary = run_query(
        'SELECT COUNT(*) AS ful_count, COALESCE(SUM(quantity_assigned), 0) AS qty_assigned FROM fulfillment'
    )
    pools_summary = run_query(
        'SELECT COUNT(*) AS pool_rows, COALESCE(SUM(quantity_total), 0) AS qty_total FROM resource_pools'
    )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        style_metric('Jobs', int(len(jobs_df)))
    with c2:
        style_metric('Requirements', int(req_summary.iloc[0]['req_count']))
    with c3:
        style_metric('Fulfillment Qty', round(float(ful_summary.iloc[0]['qty_assigned'] or 0), 2))
    with c4:
        style_metric('Pool Qty Total', round(float(pools_summary.iloc[0]['qty_total'] or 0), 2))

    st.subheader('Upcoming jobs')
    if jobs_df.empty:
        st.info('No jobs yet.')
    else:
        show = jobs_df[['job_code', 'job_name', 'region_code', 'customer', 'location', 'job_start', 'job_end', 'status']].copy()
        show['region'] = show['region_code'].map(region_options)
        show = show[['job_code', 'job_name', 'region', 'customer', 'location', 'job_start', 'job_end', 'status']]
        st.dataframe(show, use_container_width=True)

elif page == 'Jobs':
    st.subheader('Create job')
    with st.form('create_job'):
        c1, c2 = st.columns(2)
        with c1:
            job_name = st.text_input('Job name')
            region_code = st.selectbox('Region', regions_df['region_code'].tolist(), format_func=lambda x: f'{x} - {region_options[x]}')
            customer = st.text_input('Customer')
            location = st.text_input('Location')
            status = st.selectbox('Status', ['Planned', 'Tentative', 'Active', 'Complete', 'Cancelled'])
        with c2:
            job_start = st.date_input('Job start', value=date.today())
            job_end = st.date_input('Job end', value=date.today() + timedelta(days=7))
            mobilization_start = st.date_input('Mobilization start', value=date.today() - timedelta(days=2))
            demobilization_end = st.date_input('Demobilization end', value=date.today() + timedelta(days=9))
            notes = st.text_area('Notes')
        submitted = st.form_submit_button('Create job')
        if submitted:
            if not job_name.strip():
                st.error('Job name is required.')
            else:
                code = add_job(
                    job_name.strip(), region_code, customer.strip(), location.strip(),
                    job_start.isoformat(), job_end.isoformat(), mobilization_start.isoformat(),
                    demobilization_end.isoformat(), status, notes.strip()
                )
                st.success(f'Created job {code}')
                st.rerun()

    st.subheader('Jobs')
    jobs_df = run_query('SELECT * FROM jobs ORDER BY job_start DESC, job_id DESC')
    if jobs_df.empty:
        st.info('No jobs yet.')
    else:
        jobs_df['region'] = jobs_df['region_code'].map(region_options)
        st.dataframe(jobs_df[['job_code', 'job_name', 'region', 'customer', 'location', 'job_start', 'job_end', 'mobilization_start', 'demobilization_end', 'status', 'notes']], use_container_width=True)

elif page == 'Requirements':
    jobs_df = run_query('SELECT job_id, job_code, job_name FROM jobs ORDER BY job_start DESC, job_code DESC')
    if jobs_df.empty:
        st.info('Create a job first.')
    else:
        job_lookup = {row['job_id']: f"{row['job_code']} | {row['job_name']}" for _, row in jobs_df.iterrows()}
        class_lookup = {row['resource_class_id']: f"{row['class_name']} ({row['unit_type']})" for _, row in resource_classes_df.iterrows()}
        st.subheader('Add requirement')
        with st.form('add_requirement'):
            c1, c2 = st.columns(2)
            with c1:
                job_id = st.selectbox('Job', list(job_lookup.keys()), format_func=lambda x: job_lookup[x])
                resource_class_id = st.selectbox('Resource class', list(class_lookup.keys()), format_func=lambda x: class_lookup[x])
                quantity_required = st.number_input('Quantity required', min_value=0.0, value=1.0, step=0.5)
                priority = st.selectbox('Priority', ['Low', 'Medium', 'High', 'Critical'])
            with c2:
                required_start = st.date_input('Required start', value=date.today())
                required_end = st.date_input('Required end', value=date.today() + timedelta(days=7))
                notes = st.text_area('Notes', key='req_notes')
            submitted = st.form_submit_button('Add requirement')
            if submitted:
                execute(
                    '''INSERT INTO job_requirements(job_id, resource_class_id, quantity_required, required_start, required_end, priority, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    [int(job_id), int(resource_class_id), float(quantity_required), required_start.isoformat(), required_end.isoformat(), priority, notes.strip()],
                )
                st.success('Requirement added.')
                st.rerun()

        st.subheader('Requirements')
        req_df = run_query(
            '''
            SELECT jr.requirement_id, j.job_code, j.job_name, j.region_code, rc.class_name, rc.unit_type,
                   jr.quantity_required, jr.required_start, jr.required_end, jr.priority, jr.notes,
                   COALESCE(SUM(f.quantity_assigned), 0) AS quantity_assigned,
                   jr.quantity_required - COALESCE(SUM(f.quantity_assigned), 0) AS shortfall
            FROM job_requirements jr
            JOIN jobs j ON j.job_id = jr.job_id
            JOIN resource_classes rc ON rc.resource_class_id = jr.resource_class_id
            LEFT JOIN fulfillment f ON f.requirement_id = jr.requirement_id
            GROUP BY jr.requirement_id
            ORDER BY jr.required_start DESC, jr.requirement_id DESC
            '''
        )
        if req_df.empty:
            st.info('No requirements yet.')
        else:
            req_df['region'] = req_df['region_code'].map(region_options)
            st.dataframe(req_df[['requirement_id', 'job_code', 'job_name', 'region', 'class_name', 'unit_type', 'quantity_required', 'quantity_assigned', 'shortfall', 'required_start', 'required_end', 'priority', 'notes']], use_container_width=True)

elif page == 'Fulfillment':
    req_df = run_query(
        '''
        SELECT jr.requirement_id, j.job_code, j.job_name, j.region_code, rc.class_name, rc.unit_type,
               jr.quantity_required, jr.required_start, jr.required_end
        FROM job_requirements jr
        JOIN jobs j ON j.job_id = jr.job_id
        JOIN resource_classes rc ON rc.resource_class_id = jr.resource_class_id
        ORDER BY jr.required_start DESC, jr.requirement_id DESC
        '''
    )
    if req_df.empty:
        st.info('Create requirements first.')
    else:
        req_lookup = {row['requirement_id']: f"REQ {row['requirement_id']} | {row['job_code']} | {row['class_name']} | Need {row['quantity_required']} {row['unit_type']}" for _, row in req_df.iterrows()}
        st.subheader('Add fulfillment')
        with st.form('add_fulfillment'):
            c1, c2 = st.columns(2)
            with c1:
                requirement_id = st.selectbox('Requirement', list(req_lookup.keys()), format_func=lambda x: req_lookup[x])
                fulfillment_type = st.selectbox('Fulfillment type', ['internal_pool', 'rental_generic', 'specific_asset', 'specific_person'])
                source_region_code = st.selectbox('Source region', [''] + regions_df['region_code'].tolist(), format_func=lambda x: 'None' if x == '' else f'{x} - {region_options[x]}')
                source_name = st.text_input('Source name', placeholder='Internal Inventory, Rental Pump Placeholder, etc.')
                specific_resource_name = st.text_input('Specific resource name', placeholder='Only if needed')
            with c2:
                quantity_assigned = st.number_input('Quantity assigned', min_value=0.0, value=1.0, step=0.5)
                assigned_start = st.date_input('Assigned start', value=date.today())
                assigned_end = st.date_input('Assigned end', value=date.today() + timedelta(days=7))
                notes = st.text_area('Notes', key='ful_notes')
            submitted = st.form_submit_button('Add fulfillment')
            if submitted:
                execute(
                    '''INSERT INTO fulfillment(requirement_id, fulfillment_type, source_region_code, specific_resource_name, source_name, quantity_assigned, assigned_start, assigned_end, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    [int(requirement_id), fulfillment_type, source_region_code or None, specific_resource_name.strip() or None,
                     source_name.strip() or None, float(quantity_assigned), assigned_start.isoformat(), assigned_end.isoformat(), notes.strip()],
                )
                st.success('Fulfillment added.')
                st.rerun()

        st.subheader('Fulfillment rows')
        ful_df = run_query(
            '''
            SELECT f.fulfillment_id, f.requirement_id, j.job_code, j.job_name, rc.class_name, rc.unit_type,
                   f.fulfillment_type, f.source_region_code, f.specific_resource_name, f.source_name,
                   f.quantity_assigned, f.assigned_start, f.assigned_end, f.notes
            FROM fulfillment f
            JOIN job_requirements jr ON jr.requirement_id = f.requirement_id
            JOIN jobs j ON j.job_id = jr.job_id
            JOIN resource_classes rc ON rc.resource_class_id = jr.resource_class_id
            ORDER BY f.assigned_start DESC, f.fulfillment_id DESC
            '''
        )
        if ful_df.empty:
            st.info('No fulfillment yet.')
        else:
            ful_df['source_region'] = ful_df['source_region_code'].map(region_options)
            st.dataframe(ful_df[['fulfillment_id', 'requirement_id', 'job_code', 'job_name', 'class_name', 'unit_type', 'fulfillment_type', 'source_region', 'specific_resource_name', 'source_name', 'quantity_assigned', 'assigned_start', 'assigned_end', 'notes']], use_container_width=True)

elif page == 'Resource Pools':
    st.subheader('Set current pool quantities')
    class_lookup = {row['resource_class_id']: f"{row['class_name']} ({row['unit_type']})" for _, row in resource_classes_df.iterrows()}
    with st.form('set_pool'):
        c1, c2 = st.columns(2)
        with c1:
            region_code = st.selectbox('Region', regions_df['region_code'].tolist(), format_func=lambda x: f'{x} - {region_options[x]}', key='pool_region')
            resource_class_id = st.selectbox('Resource class', list(class_lookup.keys()), format_func=lambda x: class_lookup[x], key='pool_class')
        with c2:
            quantity_total = st.number_input('Current pool quantity', value=0.0, step=0.5)
            notes = st.text_input('Notes')
        submitted = st.form_submit_button('Save pool quantity')
        if submitted:
            add_or_update_pool(region_code, int(resource_class_id), float(quantity_total), notes.strip())
            st.success('Pool saved.')
            st.rerun()

    st.subheader('Pool adjustments')
    with st.form('adjust_pool'):
        c1, c2 = st.columns(2)
        with c1:
            adj_region = st.selectbox('Region ', regions_df['region_code'].tolist(), format_func=lambda x: f'{x} - {region_options[x]}', key='adj_region')
            adj_class = st.selectbox('Resource class ', list(class_lookup.keys()), format_func=lambda x: class_lookup[x], key='adj_class')
            adjustment_date = st.date_input('Adjustment date', value=date.today())
        with c2:
            quantity_change = st.number_input('Quantity change (+/-)', value=0.0, step=0.5)
            reason = st.selectbox('Reason', ['purchase', 'transfer_in', 'transfer_out', 'retirement', 'correction', 'damaged', 'return_to_service'])
            adj_notes = st.text_input('Adjustment notes')
        submitted = st.form_submit_button('Apply adjustment')
        if submitted:
            apply_pool_adjustment(adj_region, int(adj_class), adjustment_date.isoformat(), float(quantity_change), reason, adj_notes.strip())
            st.success('Adjustment applied.')
            st.rerun()

    pool_df = run_query(
        '''
        SELECT rp.pool_id, rp.region_code, rc.class_name, rc.category, rc.unit_type, rc.planning_mode, rp.quantity_total, rp.notes
        FROM resource_pools rp
        JOIN resource_classes rc ON rc.resource_class_id = rp.resource_class_id
        ORDER BY rp.region_code, rc.category, rc.class_name
        '''
    )
    if not pool_df.empty:
        pool_df['region'] = pool_df['region_code'].map(region_options)
        st.subheader('Current pools')
        st.dataframe(pool_df[['pool_id', 'region', 'class_name', 'category', 'unit_type', 'planning_mode', 'quantity_total', 'notes']], use_container_width=True)

    adj_df = run_query(
        '''
        SELECT pa.adjustment_id, pa.region_code, rc.class_name, pa.adjustment_date, pa.quantity_change, pa.reason, pa.notes
        FROM pool_adjustments pa
        JOIN resource_classes rc ON rc.resource_class_id = pa.resource_class_id
        ORDER BY pa.adjustment_date DESC, pa.adjustment_id DESC
        '''
    )
    if not adj_df.empty:
        adj_df['region'] = adj_df['region_code'].map(region_options)
        st.subheader('Adjustment log')
        st.dataframe(adj_df[['adjustment_id', 'region', 'class_name', 'adjustment_date', 'quantity_change', 'reason', 'notes']], use_container_width=True)

elif page == 'Calendar':
    st.subheader('Calendar / timeline view')
    mode = st.selectbox('View mode', ['Demand', 'Fulfillment', 'Availability'])
    date_from = st.date_input('Window start', value=date.today() - timedelta(days=7))
    date_to = st.date_input('Window end', value=date.today() + timedelta(days=30))

    if mode == 'Demand':
        df = run_query(
            '''
            SELECT jr.requirement_id AS item_id, j.job_code, j.job_name, j.region_code, rc.class_name, rc.unit_type,
                   jr.quantity_required AS qty, jr.required_start AS start_date, jr.required_end AS end_date,
                   rc.class_name || ' | ' || j.job_code || ' | Need ' || jr.quantity_required || ' ' || rc.unit_type AS label
            FROM job_requirements jr
            JOIN jobs j ON j.job_id = jr.job_id
            JOIN resource_classes rc ON rc.resource_class_id = jr.resource_class_id
            WHERE jr.required_end >= ? AND jr.required_start <= ?
            ORDER BY jr.required_start
            ''',
            [date_from.isoformat(), date_to.isoformat()],
        )
        if df.empty:
            st.info('No demand in this date window.')
        else:
            fig = px.timeline(df, x_start='start_date', x_end='end_date', y='label', color='region_code', hover_data=['job_name', 'class_name', 'qty', 'unit_type'])
            fig.update_yaxes(autorange='reversed')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df[['job_code', 'job_name', 'region_code', 'class_name', 'qty', 'unit_type', 'start_date', 'end_date']], use_container_width=True)

    elif mode == 'Fulfillment':
        df = run_query(
            '''
            SELECT f.fulfillment_id AS item_id, j.job_code, j.job_name, COALESCE(f.source_region_code, j.region_code) AS region_code,
                   rc.class_name, rc.unit_type, f.quantity_assigned AS qty, f.assigned_start AS start_date,
                   f.assigned_end AS end_date,
                   COALESCE(f.specific_resource_name, f.source_name, f.fulfillment_type) || ' | ' || j.job_code || ' | ' || rc.class_name AS label,
                   f.fulfillment_type
            FROM fulfillment f
            JOIN job_requirements jr ON jr.requirement_id = f.requirement_id
            JOIN jobs j ON j.job_id = jr.job_id
            JOIN resource_classes rc ON rc.resource_class_id = jr.resource_class_id
            WHERE f.assigned_end >= ? AND f.assigned_start <= ?
            ORDER BY f.assigned_start
            ''',
            [date_from.isoformat(), date_to.isoformat()],
        )
        if df.empty:
            st.info('No fulfillment in this date window.')
        else:
            fig = px.timeline(df, x_start='start_date', x_end='end_date', y='label', color='fulfillment_type', hover_data=['job_name', 'class_name', 'qty', 'unit_type', 'region_code'])
            fig.update_yaxes(autorange='reversed')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df[['job_code', 'job_name', 'region_code', 'class_name', 'qty', 'unit_type', 'fulfillment_type', 'start_date', 'end_date']], use_container_width=True)

    else:
        pool_df = run_query(
            '''
            SELECT rp.region_code, rc.class_name, rc.unit_type, rp.quantity_total
            FROM resource_pools rp
            JOIN resource_classes rc ON rc.resource_class_id = rp.resource_class_id
            ORDER BY rp.region_code, rc.class_name
            '''
        )
        req_df = run_query(
            '''
            SELECT j.region_code, rc.class_name, rc.unit_type, SUM(jr.quantity_required) AS demand_qty
            FROM job_requirements jr
            JOIN jobs j ON j.job_id = jr.job_id
            JOIN resource_classes rc ON rc.resource_class_id = jr.resource_class_id
            WHERE jr.required_end >= ? AND jr.required_start <= ?
            GROUP BY j.region_code, rc.class_name, rc.unit_type
            ''',
            [date_from.isoformat(), date_to.isoformat()],
        )
        merged = pool_df.merge(req_df, on=['region_code', 'class_name', 'unit_type'], how='left').fillna({'demand_qty': 0})
        if merged.empty:
            st.info('No pool data yet.')
        else:
            merged['available_qty'] = merged['quantity_total'] - merged['demand_qty']
            merged['region'] = merged['region_code'].map(region_options)
            st.dataframe(merged[['region', 'class_name', 'unit_type', 'quantity_total', 'demand_qty', 'available_qty']], use_container_width=True)
            chart_df = merged.copy()
            chart_df['label'] = chart_df['region_code'] + ' | ' + chart_df['class_name']
            fig = px.bar(chart_df, x='label', y='available_qty', hover_data=['quantity_total', 'demand_qty', 'unit_type'])
            st.plotly_chart(fig, use_container_width=True)

elif page == 'Gantt':
    jobs_df = run_query('SELECT job_id, job_code, job_name FROM jobs ORDER BY job_start DESC, job_code DESC')
    if jobs_df.empty:
        st.info('Create a job first.')
    else:
        job_lookup = {row['job_id']: f"{row['job_code']} | {row['job_name']}" for _, row in jobs_df.iterrows()}
        selected_job = st.selectbox('Select job', list(job_lookup.keys()), format_func=lambda x: job_lookup[x])
        gantt_df = run_query(
            '''
            SELECT jr.requirement_id, rc.class_name, rc.unit_type, jr.quantity_required, jr.required_start, jr.required_end,
                   COALESCE(SUM(f.quantity_assigned), 0) AS quantity_assigned,
                   jr.quantity_required - COALESCE(SUM(f.quantity_assigned), 0) AS shortfall
            FROM job_requirements jr
            JOIN resource_classes rc ON rc.resource_class_id = jr.resource_class_id
            LEFT JOIN fulfillment f ON f.requirement_id = jr.requirement_id
            WHERE jr.job_id = ?
            GROUP BY jr.requirement_id
            ORDER BY jr.required_start, rc.class_name
            ''',
            [int(selected_job)],
        )
        if gantt_df.empty:
            st.info('No requirements for this job.')
        else:
            gantt_df['label'] = gantt_df['class_name'] + ' | Need ' + gantt_df['quantity_required'].astype(str) + ' ' + gantt_df['unit_type']
            fig = px.timeline(gantt_df, x_start='required_start', x_end='required_end', y='label', color='shortfall', hover_data=['quantity_required', 'quantity_assigned', 'unit_type'])
            fig.update_yaxes(autorange='reversed')
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(gantt_df[['requirement_id', 'class_name', 'unit_type', 'quantity_required', 'quantity_assigned', 'shortfall', 'required_start', 'required_end']], use_container_width=True)
