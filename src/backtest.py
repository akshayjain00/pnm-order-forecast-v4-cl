"""Retroactive simulation engine.

See spec: docs/superpowers/specs/2026-03-27-order-forecast-design.md (Section 6)
"""
import argparse
import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import PARAMS, is_peak_date
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_range,
    compute_seasonal_baseline,
)
from src.snowflake_runner import run_sql_file

SQL_DIR = Path(__file__).parent.parent / "sql"
OUTPUT_DIR = Path(__file__).parent.parent / "output" / "backtest_reports"


def run_forecast_for_date(
    eval_date: datetime.date,
    params: dict[str, Any],
    run_hour: int = 9,
    conn: Any = None,
) -> pd.DataFrame:
    """Run the full forecast for a single eval_date at a given hour.

    Returns 3 rows (one per horizon).
    """
    signals = run_sql_file(
        SQL_DIR / "base_signals.sql",
        params={
            "eval_date": str(eval_date),
            "backtest_mode": True,
            "run_hour": run_hour,
            "opp_volume_lower_pct": float(
                params.get("opp_volume_lower_pct", 0.90)
            ),
            "opp_volume_upper_pct": float(
                params.get("opp_volume_upper_pct", 1.20)
            ),
        },
        conn=conn,
    )

    results = []
    for _, row in signals.iterrows():
        horizon = int(row["horizon"])
        target_date = row["target_date"]
        if isinstance(target_date, str):
            target_date = datetime.date.fromisoformat(target_date)

        floor = int(row["floor_orders"])
        bucket_opps = [
            int(row["open_opps_b0"]),
            int(row["open_opps_b1"]),
            int(row["open_opps_b2"]),
            int(row["open_opps_b3"]),
        ]
        bucket_convs = [
            float(row["conv_rate_b0"]),
            float(row["conv_rate_b1"]),
            float(row["conv_rate_b2"]),
            float(row["conv_rate_b3"]),
        ]
        total_opps = int(row["total_open_opps"])

        ten_week = float(row["ten_week_avg"])
        twelve_month = float(row["twelve_month_avg"])
        seasonal = compute_seasonal_baseline(
            ten_week,
            twelve_month,
            float(params["ten_week_weight"]),
        )

        peak = (
            is_peak_date(target_date)
            if isinstance(target_date, datetime.date)
            else False
        )
        peak_mult = float(row["peak_multiplier"])

        pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)
        point_est = compute_point_estimate(
            floor=floor,
            pipeline_estimate=pipeline_est,
            seasonal_baseline=seasonal,
            horizon=horizon,
            pipeline_opp_count=total_opps,
            is_peak=peak,
            peak_multiplier=peak_mult,
            params=params,
        )

        # Compute range if percentiles are available for this horizon
        p_lower_key = f"range_lower_pctl_T{horizon}"
        p_upper_key = f"range_upper_pctl_T{horizon}"
        if p_lower_key in params and p_upper_key in params:
            lower, upper = compute_range(
                point_estimate=point_est,
                floor=floor,
                p_lower=float(params[p_lower_key]),
                p_upper=float(params[p_upper_key]),
            )
        else:
            # Use default percentiles from PARAMS
            lower, upper = compute_range(
                point_estimate=point_est,
                floor=floor,
                p_lower=float(params.get("range_lower_pctl", -0.10)),
                p_upper=float(params.get("range_upper_pctl", 0.10)),
            )

        results.append({
            "eval_date": eval_date,
            "target_date": target_date,
            "horizon": horizon,
            "floor": floor,
            "pipeline_opps": total_opps,
            "pipeline_est": round(pipeline_est, 2),
            "seasonal_base": round(seasonal, 2),
            "point_est": round(point_est, 2),
            "lower": round(lower, 2),
            "upper": round(upper, 2),
        })

    return pd.DataFrame(results)


def backtest(
    window: int = 7,
    params: dict[str, Any] | None = None,
    conn: Any = None,
) -> pd.DataFrame:
    """Run backtest for the last `window` days.

    Returns a DataFrame with columns matching spec Section 6 Output Schema.
    """
    if params is None:
        params = dict(PARAMS)

    today = datetime.date.today()
    eval_dates = [
        today - datetime.timedelta(days=i) for i in range(1, window + 1)
    ]

    # Collect all forecasts
    all_forecasts: list[pd.DataFrame] = []
    for ed in eval_dates:
        df = run_forecast_for_date(ed, params, conn=conn)
        all_forecasts.append(df)
    forecasts = pd.concat(all_forecasts, ignore_index=True)

    # Pull actuals
    min_target = forecasts["target_date"].min()
    max_target = forecasts["target_date"].max()
    actuals = run_sql_file(
        SQL_DIR / "backtest_actuals.sql",
        params={
            "start_date": str(min_target),
            "end_date": str(max_target),
        },
        conn=conn,
    )
    actuals_map = dict(
        zip(actuals["service_date"], actuals["actual_orders"], strict=True)
    )

    # Merge actuals and compute metrics
    forecasts["actual"] = forecasts["target_date"].map(actuals_map)
    forecasts = forecasts.dropna(subset=["actual"])
    forecasts["actual"] = forecasts["actual"].astype(int)
    forecasts["error"] = forecasts["actual"] - forecasts["point_est"]
    forecasts["abs_pct_error"] = (
        forecasts["error"].abs() / forecasts["actual"].clip(lower=1)
    )
    forecasts["in_range"] = (
        (forecasts["actual"] >= forecasts["lower"])
        & (forecasts["actual"] <= forecasts["upper"])
    )

    return forecasts


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-horizon metrics from backtest output.

    See spec Section 6 Metrics.
    """
    metrics = []
    for horizon in [0, 1, 2]:
        h = df[df["horizon"] == horizon]
        if h.empty:
            continue
        range_width = (h["upper"] - h["lower"]).mean()
        range_pct = (
            (h["upper"] - h["lower"]) / h["point_est"].clip(lower=1)
        ).mean()
        metrics.append({
            "horizon": horizon,
            "n": len(h),
            "mae": round(float(h["error"].abs().mean()), 2),
            "mape": round(float(h["abs_pct_error"].mean()) * 100, 2),
            "bias": round(float(h["error"].mean()), 2),
            "avg_range_width": round(float(range_width), 2),
            "range_width_pct": round(float(range_pct) * 100, 1),
            "coverage": round(float(h["in_range"].mean()) * 100, 1),
            "avg_floor_pct": round(
                float(
                    (h["floor"] / h["actual"].clip(lower=1)).mean()
                ) * 100,
                1,
            ),
        })
    return pd.DataFrame(metrics)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run backtest for PnM order forecast"
    )
    parser.add_argument(
        "--window", type=int, default=7,
        help="Number of days to backtest",
    )
    args = parser.parse_args()

    print(f"Running backtest for last {args.window} days...")
    results = backtest(window=args.window)
    metrics_df = compute_metrics(results)

    # Save outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(
        OUTPUT_DIR / f"backtest_{timestamp}.csv", index=False,
    )
    print("\n--- Per-Horizon Metrics ---")
    print(metrics_df.to_string(index=False))


if __name__ == "__main__":
    main()
