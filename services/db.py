
from sqlalchemy import create_engine, text
import pandas as pd

def get_engine():
    return create_engine("sqlite:///app.db", future=True)

def init_db(engine):
    with engine.begin() as conn:
        schema = open("schema.sql").read()
        for stmt in schema.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))

def query_df(engine, sql, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def execute(engine, sql, params=None):
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})
