"""Persist hourly forecast outputs to CSV for accuracy tracking.

See spec Section 9.5 Daily accuracy log.
"""
import csv
import datetime
import hashlib
import json
from pathlib import Path
from typing import Any

FORECAST_LOG = (
    Path(__file__).parent.parent / "output" / "forecasts" / "forecast_log.csv"
)

COLUMNS = [
    "run_ts",
    "target_date",
    "horizon",
    "point_est",
    "lower",
    "upper",
    "floor",
    "actual",
    "error",
    "params_hash",
]


def params_hash(params: dict[str, Any]) -> str:
    """SHA256 of serialized params for auditability."""
    serialized = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:12]


def log_forecast(
    run_ts: datetime.datetime,
    target_date: datetime.date,
    horizon: int,
    point_est: float,
    lower: float,
    upper: float,
    floor: int,
    params: dict[str, Any],
) -> None:
    """Append a single forecast row to the log CSV."""
    FORECAST_LOG.parent.mkdir(parents=True, exist_ok=True)

    file_exists = FORECAST_LOG.exists()
    with open(FORECAST_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "run_ts": str(run_ts),
            "target_date": str(target_date),
            "horizon": horizon,
            "point_est": round(point_est, 2),
            "lower": round(lower, 2),
            "upper": round(upper, 2),
            "floor": floor,
            "actual": "",   # filled retroactively
            "error": "",    # filled retroactively
            "params_hash": params_hash(params),
        })
