"""Main forecast runner — executed hourly by CI.

Usage:
    python -m src.run_forecast
"""
import datetime
from pathlib import Path

from src.config import PARAMS, is_peak_date
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_range,
    compute_seasonal_baseline,
)
from src.logger import log_forecast
from src.snowflake_runner import get_connection, run_sql_file

SQL_DIR = Path(__file__).parent.parent / "sql"

# IST is UTC+5:30
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def main() -> None:
    now = datetime.datetime.now(tz=IST)
    eval_date = now.date()
    run_hour = now.hour

    print(f"[run_forecast] eval_date={eval_date}  run_hour={run_hour}")

    conn = get_connection()
    try:
        df = run_sql_file(
            SQL_DIR / "base_signals.sql",
            params={
                "eval_date": str(eval_date),
                "backtest_mode": False,
                "run_hour": run_hour,
            },
            conn=conn,
        )
    finally:
        conn.close()

    if df.empty:
        print("[run_forecast] No rows from base_signals — nothing to forecast.")
        return

    print(f"[run_forecast] Processing {len(df)} rows")

    for _, row in df.iterrows():
        target_date = row["target_date"]
        horizon = int(row["horizon"])
        floor = int(row["floor"])

        n_buckets = len(PARAMS["bucket_boundaries"])
        bucket_opps = [
            int(row.get(f"bucket_{i}_opps", 0)) for i in range(n_buckets)
        ]
        bucket_convs = [
            float(row.get(f"bucket_{i}_conv", 0.0))
            for i in range(n_buckets)
        ]

        pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)
        seasonal = compute_seasonal_baseline(
            ten_week_avg=float(row["ten_week_avg"]),
            twelve_month_avg=float(row["twelve_month_avg"]),
            ten_week_weight=float(PARAMS["ten_week_weight"]),
        )
        peak = (
            is_peak_date(target_date)
            if isinstance(target_date, datetime.date)
            else False
        )
        peak_mult = float(row.get("peak_multiplier", 1.0))

        point_est = compute_point_estimate(
            floor=floor,
            pipeline_estimate=pipeline_est,
            seasonal_baseline=seasonal,
            horizon=horizon,
            pipeline_opp_count=sum(bucket_opps),
            is_peak=peak,
            peak_multiplier=peak_mult,
            params=PARAMS,
        )

        # Use stratified range percentiles (peak vs nonpeak)
        day_type = "peak" if peak else "nonpeak"
        p_lo_key = f"range_lower_pctl_T{horizon}_{day_type}"
        p_hi_key = f"range_upper_pctl_T{horizon}_{day_type}"
        # Fall back to horizon-level, then global
        if p_lo_key not in PARAMS:
            p_lo_key = f"range_lower_pctl_T{horizon}"
        if p_hi_key not in PARAMS:
            p_hi_key = f"range_upper_pctl_T{horizon}"
        p_lower = float(PARAMS.get(p_lo_key, PARAMS["range_lower_pctl"]))
        p_upper = float(PARAMS.get(p_hi_key, PARAMS["range_upper_pctl"]))
        lower, upper = compute_range(
            point_estimate=point_est,
            floor=floor,
            p_lower=p_lower,
            p_upper=p_upper,
        )

        log_forecast(
            run_ts=now,
            target_date=target_date,
            horizon=horizon,
            point_est=point_est,
            lower=lower,
            upper=upper,
            floor=floor,
            params=PARAMS,
        )

        print(
            f"  T+{horizon}  date={target_date}  "
            f"floor={floor}  point={point_est:.1f}  "
            f"range=[{lower:.1f}, {upper:.1f}]"
        )

    print(f"[run_forecast] Done — {len(df)} forecasts logged.")


if __name__ == "__main__":
    main()
