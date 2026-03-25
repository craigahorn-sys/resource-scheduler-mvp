# Resource Scheduler V2

This version moves the scheduler toward a future-proof architecture:

- Streamlit UI
- PostgreSQL-ready via SQLAlchemy
- SQLite fallback for local testing
- Duration-based job dates
- Offset-based requirement dates
- Auto-allocation from internal pools
- CSV/Excel pool snapshot export

## Local run

```bash
py -m venv .venv
.venv\Scripts\activate
py -m pip install -r requirements.txt
py -m streamlit run app.py
```

By default the app uses a local SQLite file named `resource_scheduler_v2.db`.

## PostgreSQL / Streamlit Cloud

Set a database URL through environment variables or Streamlit secrets.

### `.streamlit/secrets.toml`

```toml
[connections.sql]
url = "postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME"
```

The app checks in this order:

1. `DATABASE_URL`
2. `SQLALCHEMY_DATABASE_URI`
3. `st.secrets["connections"]["sql"]["url"]`
4. `st.secrets["database_url"]`
5. local SQLite fallback

## Notes

- Auto-allocation currently fills from internal pool only.
- Requirement status values:
  - Unallocated
  - Partially Allocated
  - Fully Allocated
  - Fully Allocated with Shortfall
- Pool totals are calculated as:
  - base quantity in `resource_pools`
  - plus/minus rows from `pool_adjustments`

## Suggested next improvements

- Edit/delete actions
- As-of-date shortage forecasting over ranges
- Named assets / people assignment table
- Authentication and role controls
- API layer for a future React frontend
