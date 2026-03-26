CREATE TABLE IF NOT EXISTS regions (
    region_code TEXT PRIMARY KEY,
    region_name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS resource_classes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    class_name TEXT UNIQUE,
    category TEXT,
    unit_type TEXT,
    planning_mode TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_code TEXT,
    job_name TEXT,
    region_code TEXT,
    job_start_date DATE,
    job_duration_days INTEGER,
    mob_days_before_job INTEGER,
    demob_days_after_job INTEGER,
    status TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS resource_pools (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_code TEXT,
    resource_class_id INTEGER,
    base_quantity REAL
);

CREATE TABLE IF NOT EXISTS job_requirements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    resource_class_id INTEGER,
    quantity_required REAL,
    days_before_job_start INTEGER,
    days_after_job_end INTEGER,
    priority TEXT
);

CREATE TABLE IF NOT EXISTS requirement_fulfillment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    requirement_id INTEGER,
    quantity_assigned REAL
);
