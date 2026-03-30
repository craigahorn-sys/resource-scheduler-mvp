CREATE TABLE IF NOT EXISTS regions (
    region_code TEXT PRIMARY KEY,
    region_name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS resource_classes (
    id BIGSERIAL PRIMARY KEY,
    class_name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL,
    unit_type TEXT NOT NULL,
    planning_mode TEXT NOT NULL CHECK (planning_mode IN ('quantity_only', 'quantity_then_specific'))
);

CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    job_code TEXT NOT NULL UNIQUE,
    job_name TEXT NOT NULL,
    region_code TEXT NOT NULL REFERENCES regions(region_code),
    customer TEXT,
    customer_color TEXT,
    location TEXT,
    job_start_date DATE NOT NULL,
    job_duration_days INTEGER NOT NULL,
    mob_days_before_job INTEGER NOT NULL DEFAULT 0,
    demob_days_after_job INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'Planned',
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS resource_pools (
    id BIGSERIAL PRIMARY KEY,
    region_code TEXT NOT NULL REFERENCES regions(region_code),
    resource_class_id BIGINT NOT NULL REFERENCES resource_classes(id),
    base_quantity NUMERIC NOT NULL DEFAULT 0,
    notes TEXT,
    UNIQUE(region_code, resource_class_id)
);

CREATE TABLE IF NOT EXISTS pool_adjustments (
    id BIGSERIAL PRIMARY KEY,
    region_code TEXT NOT NULL REFERENCES regions(region_code),
    resource_class_id BIGINT NOT NULL REFERENCES resource_classes(id),
    quantity_change NUMERIC NOT NULL,
    adjustment_date DATE NOT NULL,
    reason TEXT NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_requirements (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    resource_class_id BIGINT NOT NULL REFERENCES resource_classes(id),
    quantity_required NUMERIC NOT NULL,
    days_before_job_start INTEGER NOT NULL DEFAULT 0,
    days_after_job_end INTEGER NOT NULL DEFAULT 0,
    priority TEXT DEFAULT 'Normal',
    notes TEXT
);

CREATE TABLE IF NOT EXISTS requirement_fulfillment (
    id BIGSERIAL PRIMARY KEY,
    requirement_id BIGINT NOT NULL REFERENCES job_requirements(id) ON DELETE CASCADE,
    fulfillment_type TEXT NOT NULL CHECK (
        fulfillment_type IN ('internal_pool', 'rental_generic', 'specific_asset', 'specific_person', 'manual_other')
    ),
    source_name TEXT,
    specific_resource_name TEXT,
    quantity_assigned NUMERIC NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_rental_requirements (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    resource_class_id BIGINT NOT NULL REFERENCES resource_classes(id),
    quantity_required NUMERIC NOT NULL,
    days_before_job_start INTEGER NOT NULL DEFAULT 0,
    days_after_job_end INTEGER NOT NULL DEFAULT 0,
    vendor_name TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS job_manual_owned_allocations (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    resource_class_id BIGINT NOT NULL REFERENCES resource_classes(id),
    quantity_assigned NUMERIC NOT NULL,
    days_before_job_start INTEGER NOT NULL DEFAULT 0,
    days_after_job_end INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);
