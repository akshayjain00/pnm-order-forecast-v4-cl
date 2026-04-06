"""CLI entry point for the hybrid order forecast model.

Usage:
    python -m src.cli forecast  --date 2026-04-01 --hour 9
    python -m src.cli backtest  --start 2026-03-20 --end 2026-04-03 --hours 9,15
    python -m src.cli compare   --date 2026-03-28 --hour 9
    python -m src.cli validate  --date 2026-03-28 --hour 9
    python -m src.cli signals   --date 2026-03-28 --hour 9
    python -m src.cli calibrate --start 2026-03-01 --end 2026-03-31
"""
import argparse
import datetime
import json
import math
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
from src.config import (
    DEFAULT_HYBRID_PARAMS,
    HybridParams,
    is_month_edge,
    is_peak_date_hybrid,
)
from src.forecast import (
    compute_hybrid_estimate,
    compute_hybrid_range,
    compute_pipeline_estimate,
    compute_seasonal_baseline,
)

OUTPUT_DIR = Path(__file__).parent.parent / "output"
SQL_DIR = Path(__file__).parent.parent / "sql"

# Default SQL file for the hybrid model
FORECAST_SQL = SQL_DIR / "forecast_snapshot.sql"
BASE_SQL = SQL_DIR / "base_signals.sql"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> datetime.date:
    return datetime.date.fromisoformat(s)


def _date_range(start: datetime.date, end: datetime.date):
    """Yield each date from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += datetime.timedelta(days=1)


def _ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _write_json(name: str, data: Any) -> Path:
    _ensure_output_dir()
    path = OUTPUT_DIR / name
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    return path


def _try_snowflake(sql_path: Path, params: dict) -> Any | None:
    """Run SQL via Snowflake. Returns a pandas DataFrame or None on failure."""
    try:
        from src.snowflake_runner import run_sql_file
        df = run_sql_file(sql_path, params)
        return df
    except ImportError as e:
        print(f"[WARN] Snowflake module unavailable: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[ERROR] Snowflake query failed: {e}", file=sys.stderr)
        return None


def _mock_signals(eval_date: datetime.date, horizon: int) -> dict:
    """Return zeroed signal row for offline / unit-test use."""
    return {
        "target_date": eval_date + datetime.timedelta(days=horizon),
        "horizon": horizon,
        "floor_orders": 0,
        "open_opps_b0": 0,
        "open_opps_b1": 0,
        "open_opps_b2": 0,
        "open_opps_b3": 0,
        "total_open_opps": 0,
        "conv_rate_b0": 0.10,
        "conv_rate_b1": 0.10,
        "conv_rate_b2": 0.10,
        "conv_rate_b3": 0.05,
        "ten_week_avg": 0.0,
        "twelve_month_avg": 0.0,
        "peak_multiplier": 1.0,
        "booked_share_by_cutoff": None,
        "peak_seasonal_avg": None,
    }


def _row_to_dict(row: Any) -> dict:
    """Convert a pandas Series row to a plain dict."""
    return {k: (None if (isinstance(v, float) and math.isnan(v)) else v)
            for k, v in row.to_dict().items()}


def _build_forecast_for_row(row: dict, params: HybridParams) -> dict:
    """Given a signal row dict, compute hybrid point + range."""
    target_date = row["target_date"]
    if isinstance(target_date, str):
        target_date = datetime.date.fromisoformat(target_date)

    horizon: int = int(row["horizon"])
    floor: int = int(row.get("floor_orders", 0))

    bucket_opps = [
        int(row.get("open_opps_b0", 0)),
        int(row.get("open_opps_b1", 0)),
        int(row.get("open_opps_b2", 0)),
        int(row.get("open_opps_b3", 0)),
    ]
    bucket_convs = [
        float(row.get("conv_rate_b0", 0.10)),
        float(row.get("conv_rate_b1", 0.10)),
        float(row.get("conv_rate_b2", 0.10)),
        float(row.get("conv_rate_b3", 0.05)),
    ]
    pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)

    ten_week_avg = float(row.get("ten_week_avg", 0.0) or 0.0)
    twelve_month_avg = float(row.get("twelve_month_avg", 0.0) or 0.0)
    seasonal = compute_seasonal_baseline(ten_week_avg, twelve_month_avg, 0.8)

    pipeline_opp_count = int(row.get("total_open_opps", 0))
    is_peak = is_peak_date_hybrid(target_date)

    booked_share_raw = row.get("booked_share_by_cutoff")
    booked_share: float | None = (
        float(booked_share_raw) if booked_share_raw is not None else None
    )

    # V2-style peak seasonal baseline (NULL from SQL when target is not peak)
    peak_seasonal_raw = row.get("peak_seasonal_avg")
    peak_seasonal_baseline: float | None = (
        float(peak_seasonal_raw)
        if peak_seasonal_raw is not None and not math.isnan(float(peak_seasonal_raw))
        else None
    )

    point = compute_hybrid_estimate(
        floor=floor,
        pipeline_estimate=pipeline_est,
        seasonal_baseline=seasonal,
        horizon=horizon,
        pipeline_opp_count=pipeline_opp_count,
        is_peak=is_peak,
        booked_orders=floor if horizon == 0 else None,
        booked_share=booked_share,
        params=params,
        peak_seasonal_baseline=peak_seasonal_baseline,
    )
    lo, hi = compute_hybrid_range(point, floor, horizon, is_peak, params)

    return {
        "target_date": str(target_date),
        "horizon": horizon,
        "is_peak": is_peak,
        "is_month_edge": is_month_edge(target_date),
        "floor_orders": floor,
        "pipeline_estimate": round(pipeline_est, 2),
        "seasonal_baseline": round(seasonal, 2),
        "peak_seasonal_baseline": round(peak_seasonal_baseline, 2) if peak_seasonal_baseline is not None else None,
        "point_estimate": round(point, 2),
        "range_low": round(lo, 2),
        "range_high": round(hi, 2),
        "booked_share": booked_share,
    }


# ---------------------------------------------------------------------------
# Sub-commands
# ---------------------------------------------------------------------------

def cmd_forecast(args: argparse.Namespace) -> None:
    """Load signals from Snowflake and compute T+0 / T+1 / T+2 forecasts."""
    eval_date = _parse_date(args.date)
    run_hour = int(args.hour)
    params = DEFAULT_HYBRID_PARAMS

    sql_path = FORECAST_SQL if FORECAST_SQL.exists() else BASE_SQL
    sf_params = {
        "eval_date": str(eval_date),
        "backtest_mode": False,
        "run_hour": run_hour,
        "opp_volume_lower_pct": params.opp_volume_lower_pct,
        "opp_volume_upper_pct": params.opp_volume_upper_pct,
    }

    df = _try_snowflake(sql_path, sf_params)

    results = []
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            fc = _build_forecast_for_row(_row_to_dict(row), params)
            results.append(fc)
    else:
        print("[WARN] No Snowflake data — using mock signals.", file=sys.stderr)
        for h in range(3):
            fc = _build_forecast_for_row(_mock_signals(eval_date, h), params)
            results.append(fc)

    print(f"forecast_date: {eval_date}  run_hour: {run_hour}")
    for fc in results:
        h = fc["horizon"]
        print(
            f"  T+{h} ({fc['target_date']}): "
            f"point={fc['point_estimate']:.1f}  "
            f"range=[{fc['range_low']:.1f}, {fc['range_high']:.1f}]  "
            f"peak={fc['is_peak']}"
        )

    out_path = _write_json(
        f"forecast_{eval_date}_h{run_hour:02d}.json",
        {"eval_date": str(eval_date), "run_hour": run_hour, "forecasts": results},
    )
    print(f"output: {out_path}")


def cmd_signals(args: argparse.Namespace) -> None:
    """Dump raw signals from Snowflake without blending."""
    eval_date = _parse_date(args.date)
    run_hour = int(args.hour)
    params = DEFAULT_HYBRID_PARAMS

    sql_path = FORECAST_SQL if FORECAST_SQL.exists() else BASE_SQL
    sf_params = {
        "eval_date": str(eval_date),
        "backtest_mode": False,
        "run_hour": run_hour,
        "opp_volume_lower_pct": params.opp_volume_lower_pct,
        "opp_volume_upper_pct": params.opp_volume_upper_pct,
    }

    df = _try_snowflake(sql_path, sf_params)

    if df is not None and not df.empty:
        rows = [_row_to_dict(row) for _, row in df.iterrows()]
    else:
        print("[WARN] No Snowflake data — using mock signals.", file=sys.stderr)
        rows = [_mock_signals(eval_date, h) for h in range(3)]

    print(f"signals for eval_date={eval_date} run_hour={run_hour}:")
    for row in rows:
        print(f"  horizon={row['horizon']} target={row['target_date']}")
        for k, v in row.items():
            if k not in ("target_date", "horizon"):
                print(f"    {k}: {v}")

    out_path = _write_json(
        f"signals_{eval_date}_h{run_hour:02d}.json",
        {"eval_date": str(eval_date), "run_hour": run_hour, "signals": rows},
    )
    print(f"output: {out_path}")


def cmd_validate(args: argparse.Namespace) -> None:
    """Run DQ checks on signals and report PASS / WARN / FAIL."""
    eval_date = _parse_date(args.date)
    run_hour = int(args.hour)
    params = DEFAULT_HYBRID_PARAMS

    sql_path = FORECAST_SQL if FORECAST_SQL.exists() else BASE_SQL
    sf_params = {
        "eval_date": str(eval_date),
        "backtest_mode": False,
        "run_hour": run_hour,
        "opp_volume_lower_pct": params.opp_volume_lower_pct,
        "opp_volume_upper_pct": params.opp_volume_upper_pct,
    }

    df = _try_snowflake(sql_path, sf_params)

    # DQ checks per row
    dq_results = []
    rows = (
        [_row_to_dict(r) for _, r in df.iterrows()]
        if df is not None and not df.empty
        else [_mock_signals(eval_date, h) for h in range(3)]
    )

    default_conv = (0.10, 0.10, 0.10, 0.05)

    for row in rows:
        h = row["horizon"]
        checks = {}

        # dq_floor_nonneg
        floor = int(row.get("floor_orders", 0))
        checks["dq_floor_nonneg"] = "FAIL" if floor < 0 else "PASS"

        # dq_share_bound
        bsc = row.get("booked_share_by_cutoff")
        checks["dq_share_bound"] = (
            "WARN" if bsc is not None and float(bsc) > 1.0 else "PASS"
        )

        # dq_seasonal_found
        tw = float(row.get("ten_week_avg", 0.0) or 0.0)
        checks["dq_seasonal_found"] = (
            "WARN" if tw == 0.0 and h == 0 else "PASS"
        )

        # dq_pipeline_exists
        total_opps = int(row.get("total_open_opps", 0))
        checks["dq_pipeline_exists"] = (
            "WARN" if total_opps == 0 and h == 0 else "PASS"
        )

        # dq_conv_not_all_defaults
        actual_conv = (
            round(float(row.get("conv_rate_b0", 0.10)), 4),
            round(float(row.get("conv_rate_b1", 0.10)), 4),
            round(float(row.get("conv_rate_b2", 0.10)), 4),
            round(float(row.get("conv_rate_b3", 0.05)), 4),
        )
        checks["dq_conv_not_all_defaults"] = (
            "WARN" if actual_conv == default_conv else "PASS"
        )

        dq_results.append({"horizon": h, "target_date": str(row["target_date"]), "checks": checks})

    # Summary
    all_statuses = [s for r in dq_results for s in r["checks"].values()]
    overall = "FAIL" if "FAIL" in all_statuses else ("WARN" if "WARN" in all_statuses else "PASS")

    print(f"validate eval_date={eval_date} run_hour={run_hour}  overall={overall}")
    for r in dq_results:
        print(f"  horizon={r['horizon']} ({r['target_date']})")
        for check, status in r["checks"].items():
            print(f"    {check}: {status}")

    out_path = _write_json(
        f"validate_{eval_date}_h{run_hour:02d}.json",
        {"eval_date": str(eval_date), "run_hour": run_hour, "overall": overall, "dq": dq_results},
    )
    print(f"output: {out_path}")


def cmd_backtest(args: argparse.Namespace) -> None:
    """Loop date range + hours, compute forecast, print MAPE."""
    start = _parse_date(args.start)
    end = _parse_date(args.end)
    hours = [int(h.strip()) for h in args.hours.split(",")]
    params = DEFAULT_HYBRID_PARAMS

    sql_path = FORECAST_SQL if FORECAST_SQL.exists() else BASE_SQL

    all_errors: list[float] = []
    records = []

    for eval_date in _date_range(start, end):
        for run_hour in hours:
            sf_params = {
                "eval_date": str(eval_date),
                "backtest_mode": True,
                "run_hour": run_hour,
                "opp_volume_lower_pct": params.opp_volume_lower_pct,
                "opp_volume_upper_pct": params.opp_volume_upper_pct,
            }
            df = _try_snowflake(sql_path, sf_params)

            if df is None or df.empty:
                print(
                    f"[WARN] {eval_date} h={run_hour}: no data — skipping",
                    file=sys.stderr,
                )
                continue

            for _, row in df.iterrows():
                row_dict = _row_to_dict(row)
                fc = _build_forecast_for_row(row_dict, params)
                rec = {
                    "eval_date": str(eval_date),
                    "run_hour": run_hour,
                    **fc,
                }
                records.append(rec)

    if not all_errors and records:
        print(
            "[INFO] Backtest complete — no actuals available for MAPE; "
            "see output file for forecasts."
        )
    mape_val: float | None = None
    if all_errors:
        mape_val = sum(abs(e) for e in all_errors) / len(all_errors)
        print(f"backtest MAPE: {mape_val:.4f}  n={len(all_errors)}")
    else:
        print(f"backtest records produced: {len(records)}")

    out_path = _write_json(
        f"backtest_{start}_{end}.json",
        {
            "start": str(start), "end": str(end), "hours": hours,
            "mape": mape_val, "records": records,
        },
    )
    print(f"output: {out_path}")


def cmd_compare(args: argparse.Namespace) -> None:
    """Run V4-style (conservative) vs hybrid side-by-side."""
    eval_date = _parse_date(args.date)
    run_hour = int(args.hour)
    params_hybrid = DEFAULT_HYBRID_PARAMS

    # V4-style params: conservative_nonpeak via the old dict interface is
    # approximated here by using a fallback-weight-only HybridParams clone.
    # We replicate V4 by forcing sparse_pipeline_weight and min_pipeline_opps
    # to conservative defaults.
    import dataclasses
    params_v4 = dataclasses.replace(
        DEFAULT_HYBRID_PARAMS,
        horizon_weights=(0.70, 0.70, 0.55),   # V4 weights
        sparse_pipeline_weight=0.15,
        min_pipeline_opps=5,
    )

    sql_path = FORECAST_SQL if FORECAST_SQL.exists() else BASE_SQL
    sf_params = {
        "eval_date": str(eval_date),
        "backtest_mode": False,
        "run_hour": run_hour,
        "opp_volume_lower_pct": params_hybrid.opp_volume_lower_pct,
        "opp_volume_upper_pct": params_hybrid.opp_volume_upper_pct,
    }

    df = _try_snowflake(sql_path, sf_params)
    rows = (
        [_row_to_dict(r) for _, r in df.iterrows()]
        if df is not None and not df.empty
        else [_mock_signals(eval_date, h) for h in range(3)]
    )

    print(f"compare eval_date={eval_date} run_hour={run_hour}")
    comparison = []
    for row in rows:
        fc_hybrid = _build_forecast_for_row(row, params_hybrid)
        fc_v4 = _build_forecast_for_row(row, params_v4)
        h = fc_hybrid["horizon"]
        print(
            f"  T+{h}: hybrid={fc_hybrid['point_estimate']:.1f}  "
            f"v4={fc_v4['point_estimate']:.1f}  "
            f"diff={fc_hybrid['point_estimate'] - fc_v4['point_estimate']:+.1f}"
        )
        comparison.append({"horizon": h, "hybrid": fc_hybrid, "v4": fc_v4})

    out_path = _write_json(
        f"compare_{eval_date}_h{run_hour:02d}.json",
        {"eval_date": str(eval_date), "run_hour": run_hour, "comparison": comparison},
    )
    print(f"output: {out_path}")


def cmd_calibrate(args: argparse.Namespace) -> None:
    """Read historical errors and compute range percentiles."""
    start = _parse_date(args.start)
    end = _parse_date(args.end)

    # Look for existing backtest output files in output/
    import glob as _glob

    import numpy as np

    residuals_by_horizon: dict[int, list[float]] = {0: [], 1: [], 2: []}

    pattern = str(OUTPUT_DIR / "backtest_*.json")
    files = _glob.glob(pattern)

    for fpath in files:
        try:
            with open(fpath) as f:
                data = json.load(f)
            for rec in data.get("records", []):
                h = rec.get("horizon")
                # Only include records within the calibrate window
                try:
                    rec_date = datetime.date.fromisoformat(str(rec.get("eval_date", "")))
                except ValueError:
                    continue
                if not (start <= rec_date <= end):
                    continue
                # If we have actual_orders stored, compute residual
                actual = rec.get("actual_orders")
                pred = rec.get("point_estimate")
                has_data = actual is not None and pred is not None and pred > 0
                if has_data and h in residuals_by_horizon:
                    residuals_by_horizon[h].append((actual - pred) / pred)
        except (json.JSONDecodeError, KeyError):
            continue

    print(f"calibrate {start} to {end}")
    result: dict[str, Any] = {"start": str(start), "end": str(end), "horizons": {}}

    for h, residuals in residuals_by_horizon.items():
        if len(residuals) < 3:
            print(f"  T+{h}: insufficient data (n={len(residuals)})")
            result["horizons"][str(h)] = {"n": len(residuals), "status": "insufficient"}
            continue

        arr = np.array(residuals)
        p17 = float(np.percentile(arr, 17.5))
        p82 = float(np.percentile(arr, 82.5))
        mape = float(np.mean(np.abs(arr)))
        print(
            f"  T+{h}: n={len(residuals)}  "
            f"p17={p17:.4f}  p82={p82:.4f}  MAPE={mape:.4f}"
        )
        result["horizons"][str(h)] = {
            "n": len(residuals),
            "p17_5": p17,
            "p82_5": p82,
            "mape": mape,
        }

    out_path = _write_json(f"calibrate_{start}_{end}.json", result)
    print(f"output: {out_path}")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.cli",
        description="Hybrid order forecast CLI",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # forecast
    p_fc = sub.add_parser("forecast", help="Compute T+0/T+1/T+2 hybrid forecasts")
    p_fc.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="Evaluation date")
    p_fc.add_argument("--hour", required=True, type=int, metavar="H", help="Run hour (0-23 IST)")

    # signals
    p_sig = sub.add_parser("signals", help="Dump raw Snowflake signals (no blending)")
    p_sig.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="Evaluation date")
    p_sig.add_argument("--hour", required=True, type=int, metavar="H", help="Run hour (0-23 IST)")

    # validate
    p_val = sub.add_parser("validate", help="Run DQ checks on signals")
    p_val.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="Evaluation date")
    p_val.add_argument("--hour", required=True, type=int, metavar="H", help="Run hour (0-23 IST)")

    # backtest
    p_bt = sub.add_parser("backtest", help="Backtest over a date range and report MAPE")
    p_bt.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="Start date (inclusive)")
    p_bt.add_argument("--end", required=True, metavar="YYYY-MM-DD", help="End date (inclusive)")
    p_bt.add_argument(
        "--hours",
        required=True,
        metavar="H[,H,...]",
        help="Comma-separated run hours, e.g. 9,15",
    )

    # compare
    p_cmp = sub.add_parser("compare", help="Side-by-side: V4 conservative vs hybrid")
    p_cmp.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="Evaluation date")
    p_cmp.add_argument("--hour", required=True, type=int, metavar="H", help="Run hour (0-23 IST)")

    # calibrate
    p_cal = sub.add_parser("calibrate", help="Compute range percentiles from historical errors")
    p_cal.add_argument("--start", required=True, metavar="YYYY-MM-DD", help="Start date")
    p_cal.add_argument("--end", required=True, metavar="YYYY-MM-DD", help="End date")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "forecast": cmd_forecast,
        "signals": cmd_signals,
        "validate": cmd_validate,
        "backtest": cmd_backtest,
        "compare": cmd_compare,
        "calibrate": cmd_calibrate,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
