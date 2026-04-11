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
    -- Revenue / billing fields
    company_man TEXT,
    invoice_number TEXT,
    so_ticket_number TEXT,
    day_rate NUMERIC,
    accrue BOOLEAN NOT NULL DEFAULT FALSE,
    -- Field ticket fields
    ees_supervisor TEXT,
    customer_po TEXT,
    county_state TEXT,
    well_name TEXT,
    well_number TEXT,
    ordered_by TEXT,
    department TEXT,
    job_description TEXT,
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
    requirement_id BIGINT REFERENCES job_requirements(id) ON DELETE CASCADE,
    resource_class_id BIGINT NOT NULL REFERENCES resource_classes(id),
    quantity_assigned NUMERIC NOT NULL,
    days_before_job_start INTEGER NOT NULL DEFAULT 0,
    days_after_job_end INTEGER NOT NULL DEFAULT 0,
    notes TEXT
);

-- ─────────────────────────────────────────────────────────────────────────────
-- BIDDING MODULE TABLES
-- ─────────────────────────────────────────────────────────────────────────────

-- Master catalog of all billable line items
-- charge_types: S=Setup, D=Day Rate, M=Demob (comma-separated, e.g. 'S,D,M')
-- unit: 'ft', 'mile', 'unit', 'person', 'truck' — drives qty entry on bid form
-- qty_source: how qty is determined on bid
--   'hose_ft'      = entered in miles, converted to ft (×5280)
--   'equipment'    = direct count entered
--   'labor'        = qty from bid header (general/lead/supervisor count)
--   'trucks'       = qty from bid header truck count
--   'consumable'   = estimated daily qty entered separately
CREATE TABLE IF NOT EXISTS bid_catalog (
    id            BIGSERIAL PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    category      TEXT NOT NULL,  -- 'Layflat', 'Pipe', 'Pump', 'Filter Pod',
                                  -- 'Air Compressor', 'Generator', 'Location',
                                  -- 'Manifold', 'Road Crossing', 'Labor',
                                  -- 'Truck', 'Misc', 'Consumable'
    unit          TEXT NOT NULL,  -- 'ft', 'unit', 'person', 'truck'
    qty_source    TEXT NOT NULL DEFAULT 'equipment',
    has_setup     BOOLEAN NOT NULL DEFAULT FALSE,
    has_day_rate  BOOLEAN NOT NULL DEFAULT FALSE,
    has_demob     BOOLEAN NOT NULL DEFAULT FALSE,
    sort_order    INTEGER NOT NULL DEFAULT 0,
    active        BOOLEAN NOT NULL DEFAULT TRUE
);

-- Customer rate cards — one row per customer per catalog item
-- NULL rate = not applicable or not yet priced (not zero)
CREATE TABLE IF NOT EXISTS customer_rate_cards (
    id             BIGSERIAL PRIMARY KEY,
    customer_name  TEXT NOT NULL,
    item_id        BIGINT NOT NULL REFERENCES bid_catalog(id) ON DELETE CASCADE,
    setup_rate     NUMERIC,   -- NULL = N/A
    day_rate       NUMERIC,   -- NULL = N/A
    demob_rate     NUMERIC,   -- NULL = N/A
    notes          TEXT,
    updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_name, item_id)
);

-- Bid header
CREATE TABLE IF NOT EXISTS bids (
    id               BIGSERIAL PRIMARY KEY,
    bid_name         TEXT NOT NULL,
    customer         TEXT NOT NULL,
    region_code      TEXT REFERENCES regions(region_code),
    billing_type     TEXT NOT NULL DEFAULT 'line_item'
                     CHECK (billing_type IN ('line_item', 'day_rate', 'per_bbl')),
    status           TEXT NOT NULL DEFAULT 'Draft'
                     CHECK (status IN ('Draft', 'Sent', 'Won', 'Lost', 'Expired')),
    -- Job parameters
    bid_days         INTEGER,
    total_bbls       NUMERIC,
    hrs_per_shift    NUMERIC NOT NULL DEFAULT 14,
    -- Crew (used for labor calc)
    labor_general    INTEGER NOT NULL DEFAULT 0,
    labor_lead       INTEGER NOT NULL DEFAULT 0,
    labor_supervisor INTEGER NOT NULL DEFAULT 0,
    trucks           INTEGER NOT NULL DEFAULT 0,
    -- Day rate override (for billing_type='day_rate', manually adjusted)
    day_rate_override NUMERIC,
    -- Notes / linked job
    notes            TEXT,
    job_id           BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Equipment quantities on a bid + any per-item rate overrides
-- qty stored in natural units (miles for hose, count for equipment)
-- rates: NULL = use customer rate card; value = override for this bid only
CREATE TABLE IF NOT EXISTS bid_items (
    id              BIGSERIAL PRIMARY KEY,
    bid_id          BIGINT NOT NULL REFERENCES bids(id) ON DELETE CASCADE,
    item_id         BIGINT NOT NULL REFERENCES bid_catalog(id),
    quantity        NUMERIC NOT NULL DEFAULT 0,
    -- Rate overrides (NULL = pull from rate card)
    setup_rate_override   NUMERIC,
    day_rate_override     NUMERIC,
    demob_rate_override   NUMERIC,
    notes           TEXT,
    UNIQUE(bid_id, item_id)
);
