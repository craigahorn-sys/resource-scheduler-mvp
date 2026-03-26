
from datetime import timedelta
import pandas as pd

def calc_job_dates(start, dur, mob, demob):
    start = pd.to_datetime(start).date()
    end = start + timedelta(days=dur-1)
    return {
        "job_start_date": start,
        "job_end_date": end,
        "mob_start_date": start - timedelta(days=mob),
        "demob_end_date": end + timedelta(days=demob),
    }
