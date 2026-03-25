from __future__ import annotations
import os
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'schema.sql')

RESOURCE_CLASSES = [
    ('6" Layflat Hose','Hose','miles','quantity_only'),('8" Layflat Hose','Hose','miles','quantity_only'),('10" Layflat Hose','Hose','miles','quantity_only'),('12" Layflat Hose','Hose','miles','quantity_only'),('14" Layflat Hose','Hose','miles','quantity_only'),('16" Layflat Hose','Hose','miles','quantity_only'),
    ('6" Clamps','Clamps','count','quantity_only'),('8" Clamps','Clamps','count','quantity_only'),('10" Clamps','Clamps','count','quantity_only'),('12" Clamps','Clamps','count','quantity_only'),('14" Clamps','Clamps','count','quantity_only'),('16" Clamps','Clamps','count','quantity_only'),
    ('Low-Profile Road Crossings','Road Crossings','count','quantity_only'),('12" Road Crossings','Road Crossings','count','quantity_only'),('16" Road Crossings','Road Crossings','count','quantity_only'),
    ('Rental Hose Placeholder','Rental','miles','quantity_only'),
    ('Operators','Personnel','people','quantity_then_specific'),('Trucks','Vehicles','units','quantity_then_specific'),('4x3 Pumps','Pumps','units','quantity_then_specific'),('4x4 Pumps','Pumps','units','quantity_then_specific'),('6x3 Pumps','Pumps','units','quantity_then_specific'),('Super 6x4 Pumps','Pumps','units','quantity_then_specific'),('6x6 Pumps','Pumps','units','quantity_then_specific'),('10x8 Pumps','Pumps','units','quantity_then_specific'),('8x6 Pumps','Pumps','units','quantity_then_specific'),('Super 8x6 Pumps','Pumps','units','quantity_then_specific'),('12x8 Pumps','Pumps','units','quantity_then_specific'),('12x10 Pumps','Pumps','units','quantity_then_specific'),
    ('6" Filter Pods','Filtration','units','quantity_then_specific'),('10" Filter Pods','Filtration','units','quantity_then_specific'),('12" Filter Pods','Filtration','units','quantity_then_specific'),
    ('375 Air Compressor','Air','units','quantity_then_specific'),('750 Air Compressor','Air','units','quantity_then_specific'),('900 Air Compressor','Air','units','quantity_then_specific'),('1200 Air Compressor','Air','units','quantity_then_specific'),('Doghouses','Support','units','quantity_then_specific'),('Rental Pump Placeholder','Rental','units','quantity_then_specific'),('Rental Truck Placeholder','Rental','units','quantity_then_specific'),('Rental Filter Pod Placeholder','Rental','units','quantity_then_specific')
]
REGIONS = [('RM','Rockies',True),('PM','Permian',True),('ST','South Texas',True)]


def get_engine():
    # Try Streamlit SQL connection first
    try:
        conn = st.connection('sql', type='sql')
        engine = conn.engine
        return engine
    except Exception:
        pass
    url = os.getenv('DATABASE_URL') or 'sqlite:///resource_scheduler.db'
    return create_engine(url, future=True)


def init_db(engine):
    with engine.begin() as conn:
        with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
            raw = f.read()
        for stmt in [s.strip() for s in raw.split(';') if s.strip()]:
            conn.execute(text(stmt))
        # seed regions
        for code, name, active in REGIONS:
            conn.execute(text("""
                INSERT INTO regions(region_code, region_name, active)
                SELECT :code, :name, :active
                WHERE NOT EXISTS (SELECT 1 FROM regions WHERE region_code=:code)
            """), {'code': code, 'name': name, 'active': active})
        for class_name, category, unit_type, planning_mode in RESOURCE_CLASSES:
            conn.execute(text("""
                INSERT INTO resource_classes(class_name, category, unit_type, planning_mode)
                SELECT :class_name, :category, :unit_type, :planning_mode
                WHERE NOT EXISTS (SELECT 1 FROM resource_classes WHERE class_name=:class_name)
            """), {'class_name': class_name, 'category': category, 'unit_type': unit_type, 'planning_mode': planning_mode})


def query_df(engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    return pd.read_sql(text(sql), engine, params=params or {})


def execute(engine, sql: str, params: dict | None = None):
    with engine.begin() as conn:
        return conn.execute(text(sql), params or {})


def export_excel(sheets: dict[str, pd.DataFrame]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name[:31], index=False)
    return output.getvalue()
