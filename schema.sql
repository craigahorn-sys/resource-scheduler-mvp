CREATE TABLE IF NOT EXISTS regions (
    region_code TEXT PRIMARY KEY,
    region_name TEXT,
    active BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS resource_classes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    class_name TEXT
);
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT,
    region_code TEXT
);
CREATE TABLE IF NOT EXISTS job_requirements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    resource_class_id INTEGER,
    quantity_required REAL
);
CREATE TABLE IF NOT EXISTS resource_pools (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_code TEXT,
    resource_class_id INTEGER,
    base_quantity REAL
);
CREATE TABLE IF NOT EXISTS requirement_fulfillment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    requirement_id INTEGER,
    quantity_assigned REAL
);
