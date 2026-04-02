from __future__ import annotations

from datetime import timedelta
import pandas as pd


def calc_job_dates(job_start_date, job_duration_days: int, mob_days_before_job: int, demob_days_after_job: int):
    start = pd.to_datetime(job_start_date).date()
    job_end = start + timedelta(days=max(job_duration_days, 1) - 1)
    mob_start = start - timedelta(days=max(mob_days_before_job, 0))
    demob_end = job_end + timedelta(days=max(demob_days_after_job, 0))
    return {
        'job_start_date': start,
        'job_end_date': job_end,
        'mob_start_date': mob_start,
        'demob_end_date': demob_end,
    }
