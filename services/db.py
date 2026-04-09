from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.sql"

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
]

REGIONS = [
    ("RM", "Rockies", True),
    ("PM", "Permian", True),
    ("ST", "South Texas", True),
]


def get_engine():
    try:
        conn = st.connection("sql", type="sql")
        return conn.engine
    except Exception:
        url = os.getenv("DATABASE_URL")
        if not url:
            st.error("No database connection configured. Set DATABASE_URL or configure [connections.sql] in secrets.toml.")
            st.stop()
        return create_engine(url, future=True)


def _run_schema(engine):
    raw = SCHEMA_PATH.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in raw.split(";") if stmt.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def init_db(engine):
    _run_schema(engine)
    with engine.begin() as conn:
        for code, name, active in REGIONS:
            conn.execute(
                text(
                    """
                    INSERT INTO regions(region_code, region_name, active)
                    VALUES (:code, :name, :active)
                    ON CONFLICT (region_code) DO UPDATE
                    SET region_name = EXCLUDED.region_name,
                        active = EXCLUDED.active
                    """
                ),
                {"code": code, "name": name, "active": active},
            )

        for class_name, category, unit_type, planning_mode in RESOURCE_CLASSES:
            conn.execute(
                text(
                    """
                    INSERT INTO resource_classes(class_name, category, unit_type, planning_mode)
                    VALUES (:class_name, :category, :unit_type, :planning_mode)
                    ON CONFLICT (class_name) DO UPDATE
                    SET category = EXCLUDED.category,
                        unit_type = EXCLUDED.unit_type,
                        planning_mode = EXCLUDED.planning_mode
                    """
                ),
                {
                    "class_name": class_name,
                    "category": category,
                    "unit_type": unit_type,
                    "planning_mode": planning_mode,
                },
            )


def query_df(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def execute(engine, sql: str, params: dict | None = None):
    with engine.begin() as conn:
        return conn.execute(text(sql), params or {})


def export_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    return output.getvalue()
