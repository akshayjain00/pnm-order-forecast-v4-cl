"""Offline parameter sweep against 30-day backtest data.

Tests all four gap-closing strategies:
  A. Peak multiplier cap tuning (1.0 → 1.25)
  B. Narrower peak date definitions
  C. Horizon weight & ten_week_weight tuning
  D. Hybrid conservative-blend approach (V1 MIN on non-peak days)

Uses hardcoded Snowflake data from backtest_comparison.py — no live queries.
"""
import datetime
import itertools
import json
from pathlib import Path
from typing import Any

from backtest_comparison import (
    ACTUALS_RAW,
    BUCKET_CONV_BY_DOW,
    PEAK_MULT,
    SEASONAL_RAW,
    V1_PIPELINE_RAW,
    V1_SPECIAL,
    V2_SIGNALS_RAW,
    compute_v1_forecast,
)
from src.config import HOLIDAYS, is_peak_date_broad
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_seasonal_baseline,
)

# ── Peak definition strategies (B) ──────────────────────────────────


def peak_v2_original(d: datetime.date) -> bool:
    """Original V2: last 2 days + holidays ±1 + SPECIAL_DATES."""
    return is_peak_date_broad(d)


def peak_strict(d: datetime.date) -> bool:
    """Strict: last 2 days of month + holidays only (no adjacency)."""
    import calendar

    last_day = calendar.monthrange(d.year, d.month)[1]
    if d.day >= last_day - 1:
        return True
    return d in frozenset(HOLIDAYS)


def peak_month_end_only(d: datetime.date) -> bool:
    """Only last 2 days of month."""
    import calendar

    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day >= last_day - 1


def peak_v1_aligned(d: datetime.date) -> bool:
    """Match V1 special_dates exactly (for fair comparison)."""
    return d.isoformat() in V1_SPECIAL


def peak_narrow(d: datetime.date) -> bool:
    """Last 2 days + holidays ±1 only (no SPECIAL_DATES)."""
    import calendar

    last_day = calendar.monthrange(d.year, d.month)[1]
    if d.day >= last_day - 1:
        return True
    return any(abs((d - holiday).days) <= 1 for holiday in HOLIDAYS)


PEAK_STRATEGIES: dict[str, Any] = {
    "v2_original": peak_v2_original,
    "narrow": peak_narrow,
    "strict": peak_strict,
    "month_end_only": peak_month_end_only,
    "v1_aligned": peak_v1_aligned,
}

# ── Data loading ─────────────────────────────────────────────────────

actuals = {r["d"]: r["actual"] for r in ACTUALS_RAW}
seasonal = {r["d"]: r for r in SEASONAL_RAW}
v1_pipe = {r["d"]: r for r in V1_PIPELINE_RAW}
v2_sig = {r["d"]: r for r in V2_SIGNALS_RAW}
dates = sorted(actuals.keys())


def evaluate_config(
    ten_week_weight: float,
    horizon_weight_t0: float,
    peak_cap: float,
    peak_fn: Any,
    use_conservative_nonpeak: bool = False,
) -> dict[str, Any]:
    """Evaluate a parameter config against the 30-day backtest data.

    If use_conservative_nonpeak=True (strategy D), non-peak days use
    min(pipeline_total, seasonal) instead of weighted blend.
    """
    params: dict[str, Any] = {
        "ten_week_weight": ten_week_weight,
        "horizon_weight_T0": horizon_weight_t0,
        "horizon_weight_T1": 0.65,
        "horizon_weight_T2": 0.45,
        "min_pipeline_opps": 5,
        "peak_multiplier_cap": peak_cap,
    }

    total_ape = 0.0
    peak_apes: list[float] = []
    nonpeak_apes: list[float] = []
    n_peak = 0
    over_predictions = 0

    for d in dates:
        actual = actuals[d]
        s = seasonal[d]
        s2 = v2_sig[d]
        dt = datetime.date.fromisoformat(d)
        dow = dt.weekday()
        sf_dow = (dow + 1) % 7

        bucket_opps = [s2["b0"], s2["b1"], s2["b2"], s2["b3"]]
        bucket_convs = BUCKET_CONV_BY_DOW.get(
            sf_dow, [0.15, 0.45, 0.45, 0.20]
        )
        pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)
        seasonal_bl = compute_seasonal_baseline(
            s["tw"], s["tm"], ten_week_weight
        )

        is_peak = peak_fn(dt)
        if is_peak:
            n_peak += 1

        if use_conservative_nonpeak and not is_peak:
            # Strategy D: on non-peak days, use V1-style conservative
            # approach — min(pipeline_total, seasonal) as the estimate
            pipeline_total = max(float(s2["floor"]), pipeline_est)
            estimate = min(pipeline_total, seasonal_bl)
            estimate = max(estimate, float(s2["floor"]))
        else:
            estimate = compute_point_estimate(
                floor=s2["floor"],
                pipeline_estimate=pipeline_est,
                seasonal_baseline=seasonal_bl,
                horizon=0,
                pipeline_opp_count=s2["total"],
                is_peak=is_peak,
                peak_multiplier=PEAK_MULT,
                params=params,
            )

        ape = abs(estimate - actual) / max(actual, 1) * 100
        total_ape += ape

        if is_peak:
            peak_apes.append(ape)
        else:
            nonpeak_apes.append(ape)

        if estimate > actual:
            over_predictions += 1

    n = len(dates)
    return {
        "mape": total_ape / n,
        "peak_mape": (
            sum(peak_apes) / len(peak_apes) if peak_apes else 0
        ),
        "nonpeak_mape": (
            sum(nonpeak_apes) / len(nonpeak_apes) if nonpeak_apes else 0
        ),
        "n_peak": n_peak,
        "over_pct": over_predictions / n * 100,
    }


def main() -> None:
    n = len(dates)
    print("=" * 80)
    print("  OFFLINE PARAMETER SWEEP — 30-day Backtest")
    print(f"  Period: {dates[0]} → {dates[-1]} ({n} days)")
    print("=" * 80)

    # V1 baseline for reference
    v1_total_ape = 0.0
    for d in dates:
        actual = actuals[d]
        s = seasonal[d]
        p1 = v1_pipe[d]
        v1 = compute_v1_forecast(
            tw=s["tw"], tm=s["tm"],
            opps=p1["opps"], conv=p1["conv"],
            is_special=d in V1_SPECIAL,
        )
        v1_total_ape += abs(v1["midpoint"] - actual) / max(actual, 1) * 100
    v1_mape = v1_total_ape / n
    print(f"\n  V1 SQL Baseline MAPE: {v1_mape:.1f}%")
    print("-" * 80)

    # ── A+B+C: Grid sweep ────────────────────────────────────────────

    tw_weights = [0.5, 0.6, 0.7, 0.8]
    hw_t0_values = [0.70, 0.75, 0.80, 0.85, 0.90]
    peak_caps = [1.0, 1.05, 1.10, 1.15, 1.20, 1.25]
    peak_strats = list(PEAK_STRATEGIES.keys())
    conservative_modes = [False, True]

    # Total combos
    total = (
        len(tw_weights) * len(hw_t0_values)
        * len(peak_caps) * len(peak_strats)
        * len(conservative_modes)
    )
    print(f"\n  Sweeping {total} configurations...")

    results: list[dict[str, Any]] = []

    for tw, hw, pc, ps, cons in itertools.product(
        tw_weights, hw_t0_values, peak_caps, peak_strats, conservative_modes,
    ):
        r = evaluate_config(
            ten_week_weight=tw,
            horizon_weight_t0=hw,
            peak_cap=pc,
            peak_fn=PEAK_STRATEGIES[ps],
            use_conservative_nonpeak=cons,
        )
        results.append({
            "ten_week_weight": tw,
            "horizon_weight_T0": hw,
            "peak_cap": pc,
            "peak_strategy": ps,
            "conservative_nonpeak": cons,
            **r,
        })

    # Sort by MAPE
    results.sort(key=lambda x: x["mape"])

    # ── Results ──────────────────────────────────────────────────────

    print(f"\n{'=' * 80}")
    print("  TOP 15 CONFIGURATIONS (by overall MAPE)")
    print(f"{'=' * 80}")
    print(
        f"  {'#':>3}  {'MAPE':>6}  {'PkMAPE':>7}  {'NPMAPE':>7}  "
        f"{'TW':>4}  {'HW':>4}  {'Cap':>4}  {'Peak':>14}  "
        f"{'Cons':>4}  {'#Pk':>3}  {'Over%':>5}"
    )
    print("  " + "-" * 76)
    for i, r in enumerate(results[:15]):
        print(
            f"  {i+1:>3}  {r['mape']:>5.1f}%  "
            f"{r['peak_mape']:>6.1f}%  {r['nonpeak_mape']:>6.1f}%  "
            f"{r['ten_week_weight']:>4.2f}  "
            f"{r['horizon_weight_T0']:>4.2f}  "
            f"{r['peak_cap']:>4.2f}  "
            f"{r['peak_strategy']:>14}  "
            f"{'Y' if r['conservative_nonpeak'] else 'N':>4}  "
            f"{r['n_peak']:>3}  "
            f"{r['over_pct']:>4.0f}%"
        )

    # Best overall
    best = results[0]
    print(f"\n{'=' * 80}")
    print("  BEST CONFIGURATION")
    print(f"{'=' * 80}")
    print(f"  Overall MAPE:       {best['mape']:.1f}%  "
          f"(V1: {v1_mape:.1f}%)")
    print(f"  Peak MAPE:          {best['peak_mape']:.1f}%")
    print(f"  Non-Peak MAPE:      {best['nonpeak_mape']:.1f}%")
    print(f"  Over-prediction:    {best['over_pct']:.0f}%")
    print(f"  Days flagged peak:  {best['n_peak']}/{n}")
    print("\n  Parameters:")
    print(f"    ten_week_weight:      {best['ten_week_weight']:.2f}")
    print(f"    horizon_weight_T0:    {best['horizon_weight_T0']:.2f}")
    print(f"    peak_multiplier_cap:  {best['peak_cap']:.2f}")
    print(f"    peak_strategy:        {best['peak_strategy']}")
    print(f"    conservative_nonpeak: {best['conservative_nonpeak']}")

    # Best without conservative mode (pure V2 blend)
    best_pure = next(r for r in results if not r["conservative_nonpeak"])
    print("\n  BEST PURE V2 BLEND (no conservative mode):")
    print(f"    MAPE: {best_pure['mape']:.1f}%  "
          f"(peak: {best_pure['peak_mape']:.1f}%, "
          f"nonpeak: {best_pure['nonpeak_mape']:.1f}%)")
    print(f"    tw={best_pure['ten_week_weight']:.2f}  "
          f"hw={best_pure['horizon_weight_T0']:.2f}  "
          f"cap={best_pure['peak_cap']:.2f}  "
          f"peak={best_pure['peak_strategy']}")

    # Best with conservative mode
    best_cons = next(r for r in results if r["conservative_nonpeak"])
    print("\n  BEST HYBRID (conservative non-peak):")
    print(f"    MAPE: {best_cons['mape']:.1f}%  "
          f"(peak: {best_cons['peak_mape']:.1f}%, "
          f"nonpeak: {best_cons['nonpeak_mape']:.1f}%)")
    print(f"    tw={best_cons['ten_week_weight']:.2f}  "
          f"hw={best_cons['horizon_weight_T0']:.2f}  "
          f"cap={best_cons['peak_cap']:.2f}  "
          f"peak={best_cons['peak_strategy']}")

    # ── Improvement vs V1 ───────────────────────────────────────────
    delta = v1_mape - best["mape"]
    print(f"\n{'=' * 80}")
    if delta > 0:
        print(f"  V2 BEATS V1 by {delta:.1f}pp MAPE!")
    else:
        print(f"  V2 trails V1 by {-delta:.1f}pp MAPE")
    print(f"{'=' * 80}")

    # ── Peak strategy comparison ────────────────────────────────────
    print("\n  PEAK STRATEGY COMPARISON (best config per strategy):")
    print(
        f"  {'Strategy':<16}  {'MAPE':>6}  {'PkMAPE':>7}  "
        f"{'NPMAPE':>7}  {'#Peak':>5}"
    )
    print("  " + "-" * 50)
    for ps in peak_strats:
        ps_best = min(
            (r for r in results if r["peak_strategy"] == ps),
            key=lambda x: x["mape"],
        )
        print(
            f"  {ps:<16}  {ps_best['mape']:>5.1f}%  "
            f"{ps_best['peak_mape']:>6.1f}%  "
            f"{ps_best['nonpeak_mape']:>6.1f}%  "
            f"{ps_best['n_peak']:>5}"
        )

    # Save full results
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    out_path = output_dir / "param_sweep_results.json"
    with open(out_path, "w") as f:
        json.dump(results[:50], f, indent=2, default=str)
    print(f"\n  Top 50 configs saved to: {out_path}")


if __name__ == "__main__":
    main()
