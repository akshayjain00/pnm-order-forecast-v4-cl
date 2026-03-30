"""Sweep horizon_weight_T1 and T2 using multi-horizon backtest data.

T+0 was already optimized (hw=0.70). Now find best T+1 and T+2 weights
using realistic floor values from Snowflake.
"""
import datetime
import itertools
from typing import Any

import numpy as np
from backtest_multihorizon import (
    ACTUALS_2025,
    ACTUALS_2026,
    CONV_2025,
    CONV_2026,
    FLOORS_2025,
    FLOORS_2026,
    PEAK_MULT,
    SEASONAL_2025,
    SEASONAL_2026,
    V2_BUCKETS_2025,
    V2_BUCKETS_2026,
)
from src.config import PARAMS, is_peak_date
from src.forecast import (
    calibrate_percentiles,
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_seasonal_baseline,
)


def evaluate_horizon_config(
    hw_t0: float,
    hw_t1: float,
    hw_t2: float,
    tw: float,
    conservative: bool,
) -> dict[str, Any]:
    """Evaluate weights across all horizons on both periods."""
    params: dict[str, Any] = dict(PARAMS)
    params["horizon_weight_T0"] = hw_t0
    params["horizon_weight_T1"] = hw_t1
    params["horizon_weight_T2"] = hw_t2
    params["ten_week_weight"] = tw
    params["conservative_nonpeak"] = conservative

    results_by_h: dict[int, list[float]] = {0: [], 1: [], 2: []}
    residuals_by_h: dict[int, list[float]] = {0: [], 1: [], 2: []}

    for actuals, floors, seasonal, buckets, conv_map in [
        (ACTUALS_2025, FLOORS_2025, SEASONAL_2025,
         V2_BUCKETS_2025, CONV_2025),
        (ACTUALS_2026, FLOORS_2026, SEASONAL_2026,
         V2_BUCKETS_2026, CONV_2026),
    ]:
        for d in sorted(actuals.keys()):
            actual = actuals[d]
            tw_val, tm_val = seasonal[d]
            bkts = buckets[d]
            dt = datetime.date.fromisoformat(d)
            sf_dow = (dt.weekday() + 1) % 7
            conv = conv_map.get(sf_dow, [0.20, 0.40, 0.40, 0.25])

            pipeline_est = compute_pipeline_estimate(bkts, conv)
            seasonal_bl = compute_seasonal_baseline(
                tw_val, tm_val, tw,
            )
            peak = is_peak_date(dt)
            total_opps = sum(bkts)

            for h in [0, 1, 2]:
                floor = floors[d].get(h, 0)
                est = compute_point_estimate(
                    floor=floor,
                    pipeline_estimate=pipeline_est,
                    seasonal_baseline=seasonal_bl,
                    horizon=h,
                    pipeline_opp_count=total_opps,
                    is_peak=peak,
                    peak_multiplier=PEAK_MULT,
                    params=params,
                )
                ape = abs(est - actual) / max(actual, 1) * 100
                rel_err = (est - actual) / max(actual, 1)
                results_by_h[h].append(ape)
                residuals_by_h[h].append(rel_err)

    mape = {
        h: sum(apes) / len(apes)
        for h, apes in results_by_h.items()
    }
    overall = sum(
        sum(v) for v in results_by_h.values()
    ) / sum(len(v) for v in results_by_h.values())

    return {
        "mape_t0": mape[0],
        "mape_t1": mape[1],
        "mape_t2": mape[2],
        "mape_overall": overall,
        "residuals": residuals_by_h,
    }


def main() -> None:
    print("=" * 80)
    print("  HORIZON WEIGHT OPTIMIZATION (T+1 and T+2)")
    print("  Using 62 days x 3 horizons = 186 forecasts")
    print("=" * 80)

    # Current config baseline
    baseline = evaluate_horizon_config(
        hw_t0=0.70, hw_t1=0.65, hw_t2=0.45,
        tw=0.70, conservative=True,
    )
    print("\n  Current config baseline:")
    print(f"    T+0: {baseline['mape_t0']:.1f}%  "
          f"T+1: {baseline['mape_t1']:.1f}%  "
          f"T+2: {baseline['mape_t2']:.1f}%  "
          f"Overall: {baseline['mape_overall']:.1f}%")

    # Sweep T+1 and T+2 weights (T+0 fixed at 0.70)
    hw_t1_vals = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
    hw_t2_vals = [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55]
    tw_vals = [0.60, 0.65, 0.70, 0.75, 0.80]

    total = len(hw_t1_vals) * len(hw_t2_vals) * len(tw_vals)
    print(f"\n  Sweeping {total} combinations...")

    results: list[dict[str, Any]] = []
    for hw1, hw2, tww in itertools.product(
        hw_t1_vals, hw_t2_vals, tw_vals,
    ):
        r = evaluate_horizon_config(
            hw_t0=0.70, hw_t1=hw1, hw_t2=hw2,
            tw=tww, conservative=True,
        )
        results.append({
            "hw_t1": hw1, "hw_t2": hw2, "tw": tww,
            "mape_t0": r["mape_t0"],
            "mape_t1": r["mape_t1"],
            "mape_t2": r["mape_t2"],
            "overall": r["mape_overall"],
            "residuals": r["residuals"],
        })

    # Sort by overall MAPE
    results.sort(key=lambda x: x["overall"])

    print(f"\n{'=' * 80}")
    print("  TOP 15 CONFIGS (by overall MAPE across all horizons)")
    print(f"{'=' * 80}")
    print(
        f"  {'#':>3}  {'Overall':>8}  {'T+0':>6}  "
        f"{'T+1':>6}  {'T+2':>6}  "
        f"{'HW_T1':>6}  {'HW_T2':>6}  {'TW':>5}"
    )
    print("  " + "-" * 62)
    for i, r in enumerate(results[:15]):
        print(
            f"  {i+1:>3}  {r['overall']:>7.1f}%  "
            f"{r['mape_t0']:>5.1f}%  {r['mape_t1']:>5.1f}%  "
            f"{r['mape_t2']:>5.1f}%  "
            f"{r['hw_t1']:>6.2f}  {r['hw_t2']:>6.2f}  "
            f"{r['tw']:>5.2f}"
        )

    # Best by each horizon
    print("\n  BEST CONFIG PER HORIZON:")
    for h_label, sort_key in [
        ("T+1", "mape_t1"), ("T+2", "mape_t2"),
    ]:
        best_h = min(results, key=lambda x: x[sort_key])
        print(
            f"    Best {h_label}: {best_h[sort_key]:.1f}%  "
            f"(hw_t1={best_h['hw_t1']:.2f}  "
            f"hw_t2={best_h['hw_t2']:.2f}  "
            f"tw={best_h['tw']:.2f}  "
            f"overall={best_h['overall']:.1f}%)"
        )

    # Winner
    best = results[0]
    print(f"\n{'=' * 80}")
    print("  RECOMMENDED PARAMETERS")
    print(f"{'=' * 80}")
    print("  horizon_weight_T0: 0.70  (unchanged)")
    print(f"  horizon_weight_T1: {best['hw_t1']:.2f}  "
          f"(was 0.65)")
    print(f"  horizon_weight_T2: {best['hw_t2']:.2f}  "
          f"(was 0.45)")
    print(f"  ten_week_weight:   {best['tw']:.2f}  "
          f"(was 0.70)")

    improvement = baseline["mape_overall"] - best["overall"]
    print(f"\n  Overall MAPE: {baseline['mape_overall']:.1f}% → "
          f"{best['overall']:.1f}% ({improvement:+.1f}pp)")
    for h in [0, 1, 2]:
        old = baseline[f"mape_t{h}"]
        new = best[f"mape_t{h}"]
        delta = old - new
        print(f"  T+{h}: {old:.1f}% → {new:.1f}% ({delta:+.1f}pp)")

    # Re-calibrate ranges with the best config
    print(f"\n{'=' * 80}")
    print("  RE-CALIBRATED RANGE PERCENTILES (with best weights)")
    print(f"{'=' * 80}")
    for h in [0, 1, 2]:
        residuals = np.array(best["residuals"][h])
        p_lower, p_upper = calibrate_percentiles(
            residuals, target_coverage=0.65,
        )
        print(f"  T+{h}: P_lower={p_lower:+.4f}  P_upper={p_upper:+.4f}")


if __name__ == "__main__":
    main()
