"""
scheduler/model_monitor_job.py
Daily model-monitoring job for adaptive tuning.
"""

from __future__ import annotations

import datetime as dt

from apscheduler.schedulers.background import BackgroundScheduler
from monitoring.model_monitor import apply_adaptive_tuning


def run_model_monitor_job(window: int = 100) -> dict:
    result = apply_adaptive_tuning(window=window)
    print(
        "[ModelMonitor] sample=%s roi=%.2f%% win=%.2f%% clv=%s edge_base=%.4f kelly_mult=%.3f changed=%s"
        % (
            result.get("sample_size", 0),
            result.get("roi_pct", 0.0),
            result.get("win_rate_pct", 0.0),
            ("%.4f" % result["avg_clv_ratio"]) if result.get("avg_clv_ratio") is not None else "n/a",
            result.get("edge_threshold_base", 0.04),
            result.get("kelly_multiplier", 1.0),
            result.get("changed", False),
        )
    )
    return result


def start_model_monitor_scheduler(hour_utc: int = 4) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_model_monitor_job,
        trigger="cron",
        hour=int(hour_utc),
        minute=0,
        id="model_monitor_job",
        replace_existing=True,
        next_run_time=dt.datetime.utcnow() + dt.timedelta(minutes=1),
    )
    scheduler.start()
    print(f"[ModelMonitor] Scheduled daily run at {int(hour_utc):02d}:00 UTC")
    return scheduler
