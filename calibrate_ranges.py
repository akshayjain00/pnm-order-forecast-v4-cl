"""Calibrate stratified prediction ranges from backtest residuals.

Splits residuals by horizon x day_type (peak/nonpeak) and computes
percentiles targeting 65% coverage for each segment. Also reports
weekend vs weekday as a secondary analysis.

Usage:
    python calibrate_ranges.py
"""
import datetime

import numpy as np

from backtest_multihorizon import (
    ACTUALS_2025,
    ACTUALS_2026,
    CONV_2025,
    CONV_2026,
    FLOORS_2025,
    FLOORS_2026,
    SEASONAL_2025,
    SEASONAL_2026,
    V2_BUCKETS_2025,
    V2_BUCKETS_2026,
    forecast_one,
)
from src.config import is_peak_date
from src.forecast import calibrate_percentiles, compute_range

TARGET_COVERAGE = 0.65


def collect_residuals() -> list[dict]:
    """Run V2 model on all 62 days x 3 horizons, return residual rows."""
    rows: list[dict] = []

    datasets = [
        (ACTUALS_2025, FLOORS_2025, SEASONAL_2025,
         V2_BUCKETS_2025, CONV_2025, "2025"),
        (ACTUALS_2026, FLOORS_2026, SEASONAL_2026,
         V2_BUCKETS_2026, CONV_2026, "2026"),
    ]

    for actuals, floors, seasonal, buckets, conv_by_dow, label in datasets:
        for d in sorted(actuals.keys()):
            actual = actuals[d]
            tw, tm = seasonal[d]
            bkts = buckets[d]
            dt = datetime.date.fromisoformat(d)
            sf_dow = (dt.weekday() + 1) % 7
            conv = conv_by_dow.get(sf_dow, [0.20, 0.40, 0.40, 0.25])
            peak = is_peak_date(dt)
            is_weekend = dt.weekday() >= 5  # Sat=5, Sun=6

            for h in [0, 1, 2]:
                floor = floors[d].get(h, 0)
                est = forecast_one(d, h, floor, tw, tm, bkts, conv)
                rel_err = (actual - est) / max(est, 1.0)

                rows.append({
                    "date": d,
                    "period": label,
                    "horizon": h,
                    "actual": actual,
                    "estimate": est,
                    "floor": floor,
                    "rel_error": rel_err,
                    "is_peak": peak,
                    "is_weekend": is_weekend,
                    "day_type": "peak" if peak else "nonpeak",
                    "day_class": (
                        "weekend" if is_weekend else "weekday"
                    ),
                })

    return rows


def report_segment(
    label: str,
    rows: list[dict],
) -> tuple[float, float, float]:
    """Compute and print calibrated percentiles for a segment."""
    residuals = np.array([r["rel_error"] for r in rows])
    p_lower, p_upper = calibrate_percentiles(
        residuals, target_coverage=TARGET_COVERAGE,
    )

    # Verify coverage
    covered = 0
    for r in rows:
        lo, hi = compute_range(r["estimate"], r["floor"], p_lower, p_upper)
        if lo <= r["actual"] <= hi:
            covered += 1
    cov_pct = covered / len(rows) * 100

    print(f"  {label:30s}  n={len(rows):>3}  "
          f"P_lo={p_lower:>+.4f}  P_hi={p_upper:>+.4f}  "
          f"coverage={covered}/{len(rows)} ({cov_pct:.0f}%)")
    print(f"    residual stats: "
          f"mean={residuals.mean():>+.4f}  "
          f"std={residuals.std():.4f}  "
          f"min={residuals.min():>+.4f}  "
          f"max={residuals.max():>+.4f}")
    return p_lower, p_upper, cov_pct


def main() -> None:
    rows = collect_residuals()

    print("=" * 78)
    print("  STRATIFIED RANGE CALIBRATION")
    print(f"  Target coverage: {TARGET_COVERAGE:.0%}")
    print(f"  Total forecasts: {len(rows)}")
    print("=" * 78)

    # Primary: horizon x day_type (peak/nonpeak)
    print("\n--- PRIMARY: horizon x day_type ---\n")
    config_values: dict[str, float] = {}

    for h in [0, 1, 2]:
        for day_type in ["peak", "nonpeak"]:
            seg = [
                r for r in rows
                if r["horizon"] == h and r["day_type"] == day_type
            ]
            if not seg:
                print(f"  T+{h} {day_type:>8}: no data")
                continue
            p_lo, p_hi, _ = report_segment(
                f"T+{h} {day_type}", seg,
            )
            config_values[
                f"range_lower_pctl_T{h}_{day_type}"
            ] = round(p_lo, 4)
            config_values[
                f"range_upper_pctl_T{h}_{day_type}"
            ] = round(p_hi, 4)

    # Also report unstratified per-horizon for reference
    print("\n--- REFERENCE: horizon-only (unstratified) ---\n")
    for h in [0, 1, 2]:
        seg = [r for r in rows if r["horizon"] == h]
        report_segment(f"T+{h} all", seg)

    # Secondary: weekend vs weekday
    print("\n--- SECONDARY: horizon x weekday/weekend ---\n")
    for h in [0, 1, 2]:
        for day_class in ["weekday", "weekend"]:
            seg = [
                r for r in rows
                if r["horizon"] == h and r["day_class"] == day_class
            ]
            if not seg:
                print(f"  T+{h} {day_class:>8}: no data")
                continue
            report_segment(f"T+{h} {day_class}", seg)

    # Print recommended config
    print(f"\n{'=' * 78}")
    print("  RECOMMENDED CONFIG VALUES (for src/config.py PARAMS)")
    print(f"{'=' * 78}")
    for key, val in sorted(config_values.items()):
        print(f'    "{key}": {val},')

    print("\nDone.")


if __name__ == "__main__":
    main()
