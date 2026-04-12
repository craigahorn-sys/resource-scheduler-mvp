"""
services/bidding.py — Bid catalog, rate cards, bid builder, bid→job workflow.
"""
from __future__ import annotations
import pandas as pd
from services.db import execute, query_df

# ─────────────────────────────────────────────────────────────────────────────
# CATALOG SEED DATA
# Columns: name, category, unit, qty_source, has_setup, has_day_rate, has_demob, sort_order
# ─────────────────────────────────────────────────────────────────────────────
BID_CATALOG = [
    # ── Layflat hose (entered in miles, calc in ft) ───────────────────────
    ('6" Layflat',            'Layflat',        'mile', 'hose_ft',   True,  True,  True,  10),
    ('8" Layflat',            'Layflat',        'mile', 'hose_ft',   True,  True,  True,  11),
    ('10" Layflat',           'Layflat',        'mile', 'hose_ft',   True,  True,  True,  12),
    ('12" Layflat',           'Layflat',        'mile', 'hose_ft',   True,  True,  True,  13),
    ('14" Layflat',           'Layflat',        'mile', 'hose_ft',   True,  True,  True,  14),
    ('16" Layflat',           'Layflat',        'mile', 'hose_ft',   True,  True,  True,  15),
    # ── Alum/Steel Pipe (per ft, same qty setup/day/demob) ────────────────
    ('Alum/Steel Pipe',       'Pipe',           'ft',   'equipment', True,  True,  True,  20),
    # ── Pumps (setup/day/demob each have own rate) ────────────────────────
    ('Pump 4x3',              'Pump',           'unit', 'equipment', True,  True,  True,  30),
    ('Pump 4x4',              'Pump',           'unit', 'equipment', True,  True,  True,  31),
    ('Pump 6x3',              'Pump',           'unit', 'equipment', True,  True,  True,  32),
    ('Pump Super 6x4',        'Pump',           'unit', 'equipment', True,  True,  True,  33),
    ('Pump 6x6',              'Pump',           'unit', 'equipment', True,  True,  True,  34),
    ('Pump 8x6',              'Pump',           'unit', 'equipment', True,  True,  True,  35),
    ('Pump Super 8x6',        'Pump',           'unit', 'equipment', True,  True,  True,  36),
    ('Pump 10x8',             'Pump',           'unit', 'equipment', True,  True,  True,  37),
    ('Pump 12x8',             'Pump',           'unit', 'equipment', True,  True,  True,  38),
    ('Pump 12x10',            'Pump',           'unit', 'equipment', True,  True,  True,  39),
    # ── Filter Pods (setup/day/demob) ─────────────────────────────────────
    ('Filter Pod 6" Dual',    'Filter Pod',     'unit', 'equipment', True,  True,  True,  40),
    ('Filter Pod 10" Dual',   'Filter Pod',     'unit', 'equipment', True,  True,  True,  41),
    ('Filter Pod 12" Dual',   'Filter Pod',     'unit', 'equipment', True,  True,  True,  42),
    # ── Air Compressors (setup/day/demob) ─────────────────────────────────
    ('Air Compressor 375 CFM',  'Air Compressor','unit','equipment', True,  True,  True,  50),
    ('Air Compressor 750 CFM',  'Air Compressor','unit','equipment', True,  True,  True,  51),
    ('Air Compressor 900 CFM',  'Air Compressor','unit','equipment', True,  True,  True,  52),
    ('Air Compressor 1200 CFM', 'Air Compressor','unit','equipment', True,  True,  True,  53),
    # ── Generators (setup/day/demob) ──────────────────────────────────────
    ('Generator 50-75 KW',    'Generator',      'unit', 'equipment', True,  True,  True,  60),
    ('Generator 100-130 KW',  'Generator',      'unit', 'equipment', True,  True,  True,  61),
    ('Generator 150-175 KW',  'Generator',      'unit', 'equipment', True,  True,  True,  62),
    # ── Flameless Rig Heater (setup/day/demob) ────────────────────────────
    ('Flameless Rig Heater',  'Misc',           'unit', 'equipment', True,  True,  True,  70),
    # ── Frac Location (setup/day/demob) ───────────────────────────────────
    ('Frac Location Setup',   'Location',       'unit', 'equipment', True,  False, False, 80),
    ('Frac Location Demob',   'Location',       'unit', 'equipment', False, False, True,  81),
    ('Frac Location Equipment','Location',      'unit', 'equipment', False, True,  False, 82),
    # ── Manifold (setup/day/demob) ────────────────────────────────────────
    ('Frac/Heater Manifold',  'Manifold',       'unit', 'equipment', True,  True,  True,  90),
    # ── Road / Ditch Crossings ────────────────────────────────────────────
    ('Road Crossing',         'Road Crossing',  'unit', 'equipment', True,  True,  True,  100),
    ('Ditch Crossing',        'Road Crossing',  'unit', 'equipment', False, True,  False, 101),
    # ── Meter Trailer ─────────────────────────────────────────────────────
    ('Meter Trailer',         'Misc',           'unit', 'equipment', True,  False, True,  110),
    # ── Day-rate-only misc ────────────────────────────────────────────────
    ('Skidsteer',             'Misc',           'unit', 'equipment', False, True,  False, 120),
    ('Gooseneck Trailer',     'Misc',           'unit', 'equipment', False, True,  False, 121),
    ('Flowmeter',             'Misc',           'unit', 'equipment', False, True,  False, 122),
    ('Doghouse',              'Misc',           'unit', 'equipment', False, True,  False, 123),
    ('Light Tower',           'Misc',           'unit', 'equipment', False, True,  False, 124),
    ('Pump Containment',      'Misc',           'unit', 'equipment', False, True,  False, 125),
    ('Hose 4" x 10ft',        'Misc',           'unit', 'equipment', False, True,  False, 126),
    ('Hose 4" x 20ft',        'Misc',           'unit', 'equipment', False, True,  False, 127),
    # ── Labor (rate per hour × hrs_per_shift from bid header) ─────────────
    ('Labor - General',       'Labor',          'person','labor',    False, True,  False, 130),
    ('Labor - Lead',          'Labor',          'person','labor',    False, True,  False, 131),
    ('Labor - Supervisor',    'Labor',          'person','labor',    False, True,  False, 132),
    # ── Trucks ────────────────────────────────────────────────────────────
    ('Truck',                 'Truck',          'truck', 'trucks',   False, True,  False, 140),
    # ── Consumables (estimated daily qty) ─────────────────────────────────
    ('Filter Socks',          'Consumable',     'unit', 'consumable',False, True,  False, 150),
    # ── Per diem / lodging ────────────────────────────────────────────────
    ('Per Diem',              'Labor',          'person','labor',    False, True,  False, 160),
    ('Lodging',               'Labor',          'person','labor',    False, True,  False, 161),
]

# ─────────────────────────────────────────────────────────────────────────────
# STANDARD RATE CARD (used as default / "Standard" customer)
# ─────────────────────────────────────────────────────────────────────────────
# Format: item_name -> (setup_rate, day_rate, demob_rate)  None = N/A
STANDARD_RATES = {
    '6" Layflat':             (0.25,   0.06,  0.20),
    '8" Layflat':             (0.25,   None,  0.20),
    '10" Layflat':            (0.30,   0.10,  0.25),
    '12" Layflat':            (0.30,   0.12,  0.25),
    '14" Layflat':            (0.40,   0.16,  0.35),
    '16" Layflat':            (0.45,   0.20,  0.40),
    'Alum/Steel Pipe':        (0.75,   0.15,  0.75),
    'Pump 4x3':               (500,    150,   300),
    'Pump 4x4':               (500,    125,   300),
    'Pump 6x3':               (500,    200,   300),
    'Pump Super 6x4':         (500,    300,   300),
    'Pump 6x6':               (500,    175,   300),
    'Pump 8x6':               (500,    300,   300),
    'Pump Super 8x6':         (500,    425,   300),
    'Pump 10x8':              (500,    300,   300),
    'Pump 12x8':              (500,    475,   300),
    'Pump 12x10':             (500,    650,   300),
    'Filter Pod 6" Dual':     (500,    125,   300),
    'Filter Pod 10" Dual':    (500,    400,   300),
    'Filter Pod 12" Dual':    (500,    400,   300),
    'Air Compressor 375 CFM': (None,   150,   None),
    'Air Compressor 750 CFM': (None,   275,   None),
    'Air Compressor 900 CFM': (None,   400,   None),
    'Air Compressor 1200 CFM':(None,   650,   None),
    'Generator 50-75 KW':     (500,    225,   300),
    'Generator 100-130 KW':   (500,    325,   300),
    'Generator 150-175 KW':   (500,    475,   300),
    'Flameless Rig Heater':   (None,   475,   None),
    'Frac Location Setup':    (3900,   None,  None),
    'Frac Location Demob':    (None,   None,  2700),
    'Frac Location Equipment':(None,   200,   None),
    'Frac/Heater Manifold':   (50,     35,    50),
    'Road Crossing':          (250,    40,    100),
    'Ditch Crossing':         (None,   75,    None),
    'Meter Trailer':          (None,   None,  None),
    'Skidsteer':              (None,   350,   None),
    'Gooseneck Trailer':      (None,   75,    None),
    'Flowmeter':              (None,   35,    None),
    'Doghouse':               (None,   75,    None),
    'Light Tower':            (None,   75,    None),
    'Pump Containment':       (None,   20,    None),
    'Hose 4" x 10ft':         (None,   5,     None),
    'Hose 4" x 20ft':         (None,   7,     None),
    'Labor - General':        (None,   47,    None),  # per hour
    'Labor - Lead':           (None,   57,    None),
    'Labor - Supervisor':     (None,   67,    None),
    'Truck':                  (None,   175,   None),
    'Filter Socks':           (None,   3.75,  None),
    'Per Diem':               (None,   60,    None),
    'Lodging':                (None,   60,    None),
}

# Chevron-specific rates (only overrides — rest falls back to standard)
CHEVRON_RATES = {
    '10" Layflat':            (0.25,  0.10,  0.25),
    '12" Layflat':            (0.25,  0.12,  0.25),
    '14" Layflat':            (0.35,  0.16,  0.35),
    '16" Layflat':            (0.40,  0.20,  0.40),
    'Alum/Steel Pipe':        (0.75,  0.15,  0.75),
    'Pump 4x3':               (500,   150,   300),
    'Pump 4x4':               (500,   125,   300),
    'Pump 6x3':               (500,   200,   300),
    'Pump Super 6x4':         (500,   300,   300),
    'Pump 6x6':               (500,   175,   300),
    'Pump 8x6':               (500,   300,   300),
    'Pump Super 8x6':         (500,   425,   300),
    'Pump 10x8':              (500,   300,   300),
    'Pump 12x8':              (500,   475,   300),
    'Pump 12x10':             (500,   650,   300),
    'Filter Pod 6" Dual':     (500,   125,   300),
    'Filter Pod 10" Dual':    (500,   400,   300),
    'Filter Pod 12" Dual':    (500,   400,   300),
    'Air Compressor 375 CFM': (None,  150,   None),
    'Air Compressor 750 CFM': (None,  275,   None),
    'Air Compressor 900 CFM': (None,  400,   None),
    'Air Compressor 1200 CFM':(None,  650,   None),
    'Generator 50-75 KW':     (500,   225,   300),
    'Generator 100-130 KW':   (500,   325,   300),
    'Generator 150-175 KW':   (500,   475,   300),
    'Frac Location Setup':    (3900,  None,  None),
    'Frac Location Demob':    (None,  None,  2700),
    'Frac Location Equipment':(None,  200,   None),
    'Frac/Heater Manifold':   (50,    35,    50),
    'Road Crossing':          (250,   40,    100),
    'Ditch Crossing':         (None,  75,    None),
    'Skidsteer':              (None,  350,   None),
    'Gooseneck Trailer':      (None,  75,    None),
    'Flowmeter':              (None,  35,    None),
    'Doghouse':               (None,  75,    None),
    'Light Tower':            (None,  75,    None),
    'Pump Containment':       (None,  20,    None),
    'Hose 4" x 10ft':         (None,  5,     None),
    'Hose 4" x 20ft':         (None,  7,     None),
    'Labor - General':        (None,  47,    None),
    'Labor - Lead':           (None,  57,    None),
    'Labor - Supervisor':     (None,  67,    None),
    'Truck':                  (None,  175,   None),
    'Filter Socks':           (None,  3.75,  None),
    'Per Diem':               (None,  60,    None),
    'Lodging':                (None,  60,    None),
}

OXY_RATES = {
    '10" Layflat':            (0.25,  0.10,  0.35),
    '12" Layflat':            (0.30,  0.12,  0.40),
    '14" Layflat':            (0.40,  0.16,  None),
    '16" Layflat':            (0.45,  0.24,  None),
    'Alum/Steel Pipe':        (0.75,  0.10,  0.75),
    'Pump 4x4':               (500,   125,   300),
    'Pump 6x6':               (500,   200,   300),
    'Pump 8x6':               (None,  None,  None),
    'Pump 10x8':              (500,   300,   300),
    'Pump 12x8':              (500,   450,   300),
    'Filter Pod 6" Dual':     (500,   125,   300),
    'Filter Pod 10" Dual':    (500,   400,   300),
    'Filter Pod 12" Dual':    (500,   400,   300),
    'Air Compressor 375 CFM': (None,  200,   None),
    'Air Compressor 750 CFM': (None,  400,   None),
    'Air Compressor 900 CFM': (None,  650,   None),
    'Air Compressor 1200 CFM':(None,  650,   None),
    'Frac/Heater Manifold':   (50,    30,    50),
    'Road Crossing':          (200,   60,    100),
    'Ditch Crossing':         (None,  75,    None),
    'Meter Trailer':          (200,   None,  200),
    'Skidsteer':              (None,  350,   None),
    'Gooseneck Trailer':      (None,  75,    None),
    'Hose 4" x 10ft':         (None,  5,     None),
    'Hose 4" x 20ft':         (None,  7,     None),
    'Light Tower':            (None,  65,    None),
    'Pump Containment':       (None,  20,    None),
    'Labor - General':        (None,  45,    None),
    'Labor - Lead':           (None,  55,    None),
    'Labor - Supervisor':     (None,  65,    None),
    'Truck':                  (None,  175,   None),
    'Filter Socks':           (None,  3.75,  None),
    'Per Diem':               (None,  60,    None),
    'Lodging':                (None,  60,    None),
}

CROWHEART_RATES = {
    '10" Layflat':            (0.40,  0.08,  0.45),
    '12" Layflat':            (0.45,  0.12,  0.50),
    '14" Layflat':            (0.50,  0.16,  None),
    '16" Layflat':            (0.55,  0.22,  None),
    'Pump 4x4':               (500,   150,   300),
    'Pump 8x6':               (500,   425,   300),
    'Pump 10x8':              (500,   375,   300),
    'Pump 12x8':              (500,   475,   300),
    'Pump 12x10':             (500,   675,   300),
    'Filter Pod 6" Dual':     (500,   125,   300),
    'Filter Pod 10" Dual':    (500,   400,   300),
    'Filter Pod 12" Dual':    (500,   400,   300),
    'Air Compressor 375 CFM': (None,  150,   None),
    'Air Compressor 750 CFM': (None,  275,   None),
    'Air Compressor 900 CFM': (None,  400,   None),
    'Air Compressor 1200 CFM':(None,  600,   None),
    'Generator 50-75 KW':     (500,   225,   300),
    'Generator 100-130 KW':   (500,   325,   300),
    'Generator 150-175 KW':   (500,   475,   300),
    'Frac Location Setup':    (3900,  None,  None),
    'Frac Location Demob':    (None,  None,  2700),
    'Frac Location Equipment':(None,  100,   None),
    'Road Crossing':          (250,   40,    100),
    'Labor - General':        (None,  49,    None),
    'Truck':                  (None,  200,   None),
    'Per Diem':               (None,  65,    None),
    'Lodging':                (None,  65,    None),
}

SEED_CUSTOMERS = {
    'Standard':  STANDARD_RATES,
    'Chevron':   CHEVRON_RATES,
    'OXY':       OXY_RATES,
    'Crowheart': CROWHEART_RATES,
}


# ─────────────────────────────────────────────────────────────────────────────
# MIGRATION
# ─────────────────────────────────────────────────────────────────────────────

def migrate_bidding(engine):
    """Create bidding tables and seed catalog + known rate cards."""
    from sqlalchemy import text

    # Create tables
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS bid_catalog (
            id           BIGSERIAL PRIMARY KEY,
            name         TEXT NOT NULL UNIQUE,
            category     TEXT NOT NULL,
            unit         TEXT NOT NULL,
            qty_source   TEXT NOT NULL DEFAULT 'equipment',
            has_setup    BOOLEAN NOT NULL DEFAULT FALSE,
            has_day_rate BOOLEAN NOT NULL DEFAULT FALSE,
            has_demob    BOOLEAN NOT NULL DEFAULT FALSE,
            sort_order   INTEGER NOT NULL DEFAULT 0,
            active       BOOLEAN NOT NULL DEFAULT TRUE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_rate_cards (
            id             BIGSERIAL PRIMARY KEY,
            customer_name  TEXT NOT NULL,
            item_id        BIGINT NOT NULL REFERENCES bid_catalog(id) ON DELETE CASCADE,
            setup_rate     NUMERIC,
            day_rate       NUMERIC,
            demob_rate     NUMERIC,
            notes          TEXT,
            updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(customer_name, item_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS bids (
            id                BIGSERIAL PRIMARY KEY,
            bid_name          TEXT NOT NULL,
            customer          TEXT NOT NULL,
            rate_card         TEXT,
            region_code       TEXT REFERENCES regions(region_code),
            billing_type      TEXT NOT NULL DEFAULT 'line_item'
                              CHECK (billing_type IN ('line_item','day_rate','per_bbl')),
            status            TEXT NOT NULL DEFAULT 'Draft'
                              CHECK (status IN ('Draft','Sent','Won','Lost','Expired')),
            bid_days          INTEGER,
            total_bbls        NUMERIC,
            hrs_per_shift     NUMERIC NOT NULL DEFAULT 14,
            labor_general     INTEGER NOT NULL DEFAULT 0,
            labor_lead        INTEGER NOT NULL DEFAULT 0,
            labor_supervisor  INTEGER NOT NULL DEFAULT 0,
            trucks            INTEGER NOT NULL DEFAULT 0,
            day_rate_override NUMERIC,
            notes             TEXT,
            job_id            BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
            created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS bid_items (
            id                    BIGSERIAL PRIMARY KEY,
            bid_id                BIGINT NOT NULL REFERENCES bids(id) ON DELETE CASCADE,
            item_id               BIGINT NOT NULL REFERENCES bid_catalog(id),
            quantity              NUMERIC NOT NULL DEFAULT 0,
            setup_rate_override   NUMERIC,
            day_rate_override     NUMERIC,
            demob_rate_override   NUMERIC,
            notes                 TEXT,
            UNIQUE(bid_id, item_id)
        )
        """,
    ]

    # Add rate_card column to existing bids tables (safe if already exists)
    with engine.begin() as conn:
        try:
            from sqlalchemy import text as _text
            conn.execute(_text(
                "ALTER TABLE bids ADD COLUMN IF NOT EXISTS rate_card TEXT"
            ))
        except Exception:
            pass

    for ddl in ddl_statements:
        with engine.begin() as conn:
            conn.execute(text(ddl))

    # Seed catalog items (upsert by name)
    with engine.begin() as conn:
        for (name, category, unit, qty_source,
             has_setup, has_day_rate, has_demob, sort_order) in BID_CATALOG:
            conn.execute(text("""
                INSERT INTO bid_catalog
                    (name, category, unit, qty_source,
                     has_setup, has_day_rate, has_demob, sort_order)
                VALUES
                    (:name, :category, :unit, :qty_source,
                     :has_setup, :has_day_rate, :has_demob, :sort_order)
                ON CONFLICT (name) DO UPDATE SET
                    category     = EXCLUDED.category,
                    unit         = EXCLUDED.unit,
                    qty_source   = EXCLUDED.qty_source,
                    has_setup    = EXCLUDED.has_setup,
                    has_day_rate = EXCLUDED.has_day_rate,
                    has_demob    = EXCLUDED.has_demob,
                    sort_order   = EXCLUDED.sort_order
            """), {
                "name": name, "category": category, "unit": unit,
                "qty_source": qty_source, "has_setup": has_setup,
                "has_day_rate": has_day_rate, "has_demob": has_demob,
                "sort_order": sort_order,
            })

    # Seed rate cards for known customers
    with engine.begin() as conn:
        # Fetch catalog id lookup
        rows = conn.execute(text("SELECT id, name FROM bid_catalog")).fetchall()
        item_ids = {row[1]: row[0] for row in rows}

        for customer, rates in SEED_CUSTOMERS.items():
            # Ensure every catalog item exists for this customer (NULL rates)
            for item_name, item_id in item_ids.items():
                s, d, m = rates.get(item_name, (None, None, None))
                conn.execute(text("""
                    INSERT INTO customer_rate_cards
                        (customer_name, item_id, setup_rate, day_rate, demob_rate)
                    VALUES (:cust, :item_id, :s, :d, :m)
                    ON CONFLICT (customer_name, item_id) DO UPDATE SET
                        setup_rate = EXCLUDED.setup_rate,
                        day_rate   = EXCLUDED.day_rate,
                        demob_rate = EXCLUDED.demob_rate,
                        updated_at = CURRENT_TIMESTAMP
                """), {"cust": customer, "item_id": item_id,
                       "s": s, "d": d, "m": m})


# ─────────────────────────────────────────────────────────────────────────────
# QUERIES
# ─────────────────────────────────────────────────────────────────────────────

def get_catalog(engine) -> pd.DataFrame:
    return query_df(engine, """
        SELECT id, name, category, unit, qty_source,
               has_setup, has_day_rate, has_demob, sort_order
        FROM bid_catalog WHERE active = TRUE ORDER BY sort_order, name
    """)


def get_customers_with_rate_cards(engine) -> list[str]:
    df = query_df(engine,
        "SELECT DISTINCT customer_name FROM customer_rate_cards ORDER BY customer_name")
    return df["customer_name"].tolist() if not df.empty else []


def get_rate_card(engine, customer: str) -> pd.DataFrame:
    """Returns full rate card for a customer joined with catalog."""
    return query_df(engine, """
        SELECT c.id AS item_id, c.name, c.category, c.unit, c.qty_source,
               c.has_setup, c.has_day_rate, c.has_demob, c.sort_order,
               r.setup_rate, r.day_rate, r.demob_rate, r.notes
        FROM bid_catalog c
        LEFT JOIN customer_rate_cards r
               ON r.item_id = c.id AND r.customer_name = :cust
        WHERE c.active = TRUE
        ORDER BY c.sort_order, c.name
    """, {"cust": customer})


def upsert_rate_card_row(engine, customer: str, item_id: int,
                         setup_rate, day_rate, demob_rate, notes=None):
    execute(engine, """
        INSERT INTO customer_rate_cards
            (customer_name, item_id, setup_rate, day_rate, demob_rate, notes)
        VALUES (:cust, :item_id, :s, :d, :m, :notes)
        ON CONFLICT (customer_name, item_id) DO UPDATE SET
            setup_rate = EXCLUDED.setup_rate,
            day_rate   = EXCLUDED.day_rate,
            demob_rate = EXCLUDED.demob_rate,
            notes      = EXCLUDED.notes,
            updated_at = CURRENT_TIMESTAMP
    """, {"cust": customer, "item_id": item_id,
          "s": setup_rate or None, "d": day_rate or None,
          "m": demob_rate or None, "notes": notes})


def add_customer_rate_card(engine, customer: str):
    """Create empty rate card rows for a new customer from catalog."""
    catalog = get_catalog(engine)
    with engine.begin() as conn:
        from sqlalchemy import text
        for _, row in catalog.iterrows():
            conn.execute(text("""
                INSERT INTO customer_rate_cards
                    (customer_name, item_id, setup_rate, day_rate, demob_rate)
                VALUES (:cust, :item_id, NULL, NULL, NULL)
                ON CONFLICT DO NOTHING
            """), {"cust": customer, "item_id": int(row["id"])})


def get_bids(engine, status=None) -> pd.DataFrame:
    where = "WHERE b.status = :status" if status else ""
    return query_df(engine, f"""
        SELECT b.*, j.job_code
        FROM bids b
        LEFT JOIN jobs j ON j.id = b.job_id
        {where}
        ORDER BY b.created_at DESC
    """, {"status": status} if status else {})


def get_bid(engine, bid_id: int) -> pd.Series:
    df = query_df(engine,
        "SELECT * FROM bids WHERE id = :bid_id", {"bid_id": bid_id})
    return df.iloc[0] if not df.empty else None


def get_bid_items(engine, bid_id: int) -> pd.DataFrame:
    """Bid items joined with catalog and effective rates (override > rate card)."""
    return query_df(engine, """
        SELECT
            bi.id, bi.bid_id, bi.item_id, bi.quantity, bi.notes,
            c.name, c.category, c.unit, c.qty_source,
            c.has_setup, c.has_day_rate, c.has_demob, c.sort_order,
            -- effective rates: override wins, else rate card
            COALESCE(bi.setup_rate_override,  rc.setup_rate)  AS setup_rate,
            COALESCE(bi.day_rate_override,    rc.day_rate)    AS day_rate,
            COALESCE(bi.demob_rate_override,  rc.demob_rate)  AS demob_rate,
            -- keep overrides visible separately for UI
            bi.setup_rate_override, bi.day_rate_override, bi.demob_rate_override
        FROM bid_items bi
        JOIN bid_catalog c ON c.id = bi.item_id
        JOIN bids b ON b.id = bi.bid_id
        LEFT JOIN customer_rate_cards rc
               ON rc.item_id = bi.item_id
              AND rc.customer_name = COALESCE(b.rate_card, b.customer)
        WHERE bi.bid_id = :bid_id
        ORDER BY c.sort_order, c.name
    """, {"bid_id": bid_id})


def save_bid(engine, data: dict) -> int:
    """Create or update a bid. Returns bid id."""
    from sqlalchemy import text
    if data.get("id"):
        execute(engine, """
            UPDATE bids SET
                bid_name=:bid_name, customer=:customer, rate_card=:rate_card,
                region_code=:region_code, billing_type=:billing_type,
                status=:status, bid_days=:bid_days, total_bbls=:total_bbls,
                hrs_per_shift=:hrs_per_shift,
                labor_general=:labor_general, labor_lead=:labor_lead,
                labor_supervisor=:labor_supervisor, trucks=:trucks,
                day_rate_override=:day_rate_override, notes=:notes,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=:id
        """, data)
        return int(data["id"])
    else:
        with engine.begin() as conn:
            row = conn.execute(text("""
                INSERT INTO bids
                    (bid_name, customer, rate_card, region_code, billing_type, status,
                     bid_days, total_bbls, hrs_per_shift,
                     labor_general, labor_lead, labor_supervisor, trucks,
                     day_rate_override, notes)
                VALUES
                    (:bid_name, :customer, :rate_card, :region_code, :billing_type, :status,
                     :bid_days, :total_bbls, :hrs_per_shift,
                     :labor_general, :labor_lead, :labor_supervisor, :trucks,
                     :day_rate_override, :notes)
                RETURNING id
            """), data).fetchone()
            return row[0]


def save_bid_item(engine, bid_id: int, item_id: int, quantity: float,
                  setup_override=None, day_override=None, demob_override=None,
                  notes=None):
    execute(engine, """
        INSERT INTO bid_items
            (bid_id, item_id, quantity,
             setup_rate_override, day_rate_override, demob_rate_override, notes)
        VALUES
            (:bid_id, :item_id, :qty, :s, :d, :m, :notes)
        ON CONFLICT (bid_id, item_id) DO UPDATE SET
            quantity            = EXCLUDED.quantity,
            setup_rate_override = EXCLUDED.setup_rate_override,
            day_rate_override   = EXCLUDED.day_rate_override,
            demob_rate_override = EXCLUDED.demob_rate_override,
            notes               = EXCLUDED.notes
    """, {"bid_id": bid_id, "item_id": item_id, "qty": quantity,
          "s": setup_override or None, "d": day_override or None,
          "m": demob_override or None, "notes": notes})


# ─────────────────────────────────────────────────────────────────────────────
# BID CALCULATIONS
# ─────────────────────────────────────────────────────────────────────────────

def calc_bid(bid: pd.Series, items: pd.DataFrame) -> dict:
    """
    Returns computed bid totals and line-item breakdown.
    Handles all qty_source types and billing rules.
    """
    hrs       = float(bid.get("hrs_per_shift") or 14)
    bid_days  = int(bid.get("bid_days") or 0)
    total_bbls = float(bid.get("total_bbls") or 0)
    n_general  = int(bid.get("labor_general") or 0)
    n_lead     = int(bid.get("labor_lead") or 0)
    n_super    = int(bid.get("labor_supervisor") or 0)
    n_trucks   = int(bid.get("trucks") or 0)

    setup_lines  = []   # {name, qty, unit, rate, total}
    day_lines    = []
    demob_lines  = []

    for _, row in items.iterrows():
        if row["quantity"] == 0:
            continue

        raw_qty    = float(row["quantity"])
        qty_source = row["qty_source"]
        name       = row["name"]
        unit       = row["unit"]

        # Resolve actual quantity for calc
        if qty_source == "hose_ft":
            qty_ft = raw_qty * 5280
        elif qty_source == "labor":
            # qty stored per labor type; actual count comes from bid header
            if "General" in name:
                qty_ft = n_general
            elif "Lead" in name:
                qty_ft = n_lead
            elif "Supervisor" in name:
                qty_ft = n_super
            elif "Per Diem" in name or "Lodging" in name:
                qty_ft = n_general + n_lead + n_super
            else:
                qty_ft = raw_qty
        elif qty_source == "trucks":
            qty_ft = n_trucks
        else:
            qty_ft = raw_qty

        def _line(rate, qty, note_unit):
            if rate is None or rate == 0:
                return None
            total = rate * qty
            display_qty = raw_qty if qty_source == "hose_ft" else qty
            display_unit = "miles" if qty_source == "hose_ft" else note_unit
            return {
                "name": name,
                "qty": display_qty,
                "unit": display_unit,
                "rate": rate,
                "calc_qty": qty,   # actual qty used in math
                "total": total,
            }

        s_rate = _to_f(row.get("setup_rate"))
        d_rate = _to_f(row.get("day_rate"))
        m_rate = _to_f(row.get("demob_rate"))

        if row["has_setup"] and s_rate:
            # Labor setup: rate is per-hour
            if qty_source == "labor" and "Labor" in row["category"]:
                total_rate = s_rate * hrs * qty_ft
                setup_lines.append({
                    "name": name, "qty": qty_ft, "unit": "person",
                    "rate": s_rate, "calc_qty": qty_ft,
                    "total": total_rate,
                })
            else:
                line = _line(s_rate, qty_ft, unit)
                if line:
                    setup_lines.append(line)

        if row["has_day_rate"] and d_rate:
            if qty_source == "labor" and "Labor" in row["category"]:
                # Labor day rate = qty × hrs × hourly_rate
                total_rate = d_rate * hrs * qty_ft
                day_lines.append({
                    "name": name, "qty": qty_ft, "unit": "person",
                    "rate": d_rate, "calc_qty": qty_ft * hrs,
                    "total": total_rate,
                })
            else:
                line = _line(d_rate, qty_ft, unit)
                if line:
                    day_lines.append(line)

        if row["has_demob"] and m_rate:
            line = _line(m_rate, qty_ft, unit)
            if line:
                demob_lines.append(line)

    setup_total  = sum(l["total"] for l in setup_lines)
    day_total    = sum(l["total"] for l in day_lines)
    demob_total  = sum(l["total"] for l in demob_lines)
    job_cost     = setup_total + (day_total * bid_days) + demob_total
    cost_per_bbl = (job_cost / total_bbls) if total_bbls > 0 else None

    return {
        "setup_total":  setup_total,
        "day_total":    day_total,
        "demob_total":  demob_total,
        "job_cost":     job_cost,
        "cost_per_bbl": cost_per_bbl,
        "bid_days":     bid_days,
        "total_bbls":   total_bbls,
        "setup_lines":  setup_lines,
        "day_lines":    day_lines,
        "demob_lines":  demob_lines,
    }


def _to_f(val):
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0 else None
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# BID → JOB CONVERSION
# ─────────────────────────────────────────────────────────────────────────────

def bid_to_job_line_items(bid: pd.Series, calc: dict,
                           billing_type: str) -> list[dict]:
    """
    Converts bid calc results into job_line_items rows.
    billing_type overrides bid.billing_type if caller wants to switch.
    Returns list of dicts ready for save_line_items().
    """
    btype = billing_type or bid.get("billing_type", "line_item")
    days  = int(bid.get("bid_days") or 0)

    lines = []

    if btype == "line_item":
        ln = 1
        # Setup lines — each item as its own line
        for item in calc["setup_lines"]:
            lines.append({
                "line_number": ln, "description": f"{item['name']} — Setup",
                "uom": item["unit"], "invoice_qty": item["calc_qty"],
                "unit_price": item["rate"], "line_total": item["total"],
                "start_date": None, "end_date": None, "notes": "Setup",
            })
            ln += 1
        # Day rate lines
        for item in calc["day_lines"]:
            lines.append({
                "line_number": ln, "description": item["name"],
                "uom": f"{item['unit']}/day",
                "invoice_qty": item["calc_qty"],
                "unit_price": item["rate"],
                "line_total": item["total"] * days,
                "start_date": None, "end_date": None, "notes": "Day Rate",
            })
            ln += 1
        # Demob lines
        for item in calc["demob_lines"]:
            lines.append({
                "line_number": ln, "description": f"{item['name']} — Demob",
                "uom": item["unit"], "invoice_qty": item["calc_qty"],
                "unit_price": item["rate"], "line_total": item["total"],
                "start_date": None, "end_date": None, "notes": "Demob",
            })
            ln += 1

    elif btype == "day_rate":
        dr = float(bid.get("day_rate_override") or calc["day_total"])
        lines = [
            {"line_number": 1, "description": "Setup",
             "uom": "Ea", "invoice_qty": 1,
             "unit_price": calc["setup_total"],
             "line_total": calc["setup_total"],
             "start_date": None, "end_date": None, "notes": "Setup"},
            {"line_number": 2, "description": "Day Rate",
             "uom": "Day", "invoice_qty": days,
             "unit_price": dr,
             "line_total": dr * days,
             "start_date": None, "end_date": None, "notes": "Day Rate"},
            {"line_number": 3, "description": "Demob",
             "uom": "Ea", "invoice_qty": 1,
             "unit_price": calc["demob_total"],
             "line_total": calc["demob_total"],
             "start_date": None, "end_date": None, "notes": "Demob"},
        ]

    elif btype == "per_bbl":
        total_bbls = float(bid.get("total_bbls") or 0)
        per_bbl    = calc.get("cost_per_bbl") or 0
        lines = [
            {"line_number": 1, "description": "Water Transfer — Per BBL",
             "uom": "BBL", "invoice_qty": total_bbls,
             "unit_price": per_bbl,
             "line_total": total_bbls * per_bbl,
             "start_date": None, "end_date": None, "notes": "Per BBL"},
        ]

    return lines
