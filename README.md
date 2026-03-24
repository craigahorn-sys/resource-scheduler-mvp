# Resource Scheduler MVP

A lightweight Streamlit app for:
- Region-based jobs with auto-generated job codes
- Resource classes
- Job requirements
- Fulfillment tracking
- Regional resource pools
- Pool adjustments
- Calendar views for demand, fulfillment, and availability
- Job-level Gantt view

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Notes
- Database is SQLite and is created automatically as `resource_scheduler.db`.
- Regions are preloaded: RM, PM, ST.
- Resource classes are preloaded from the planning structure discussed.
- IDs are internal and auto-generated.
