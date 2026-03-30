"""Wider backtest: Mar 15 – Apr 15, 2025 (32 days).

Compares V1 SQL model vs V2 Optimized vs Actuals using historical
Snowflake data. This period includes Holi (Mar 14), month-end surge
(Mar 29-31), and the start of April — a strong stress test.
"""
import datetime
import json
from pathlib import Path
from typing import Any

from src.config import PARAMS, is_peak_date, is_peak_date_broad
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_seasonal_baseline,
)

# ── Raw data from Snowflake (32 days: Mar 15 – Apr 15, 2025) ────────

ACTUALS = {
    "2025-03-15": 1421, "2025-03-16": 1249, "2025-03-17": 690,
    "2025-03-18": 457, "2025-03-19": 639, "2025-03-20": 693,
    "2025-03-21": 884, "2025-03-22": 1338, "2025-03-23": 1440,
    "2025-03-24": 871, "2025-03-25": 675, "2025-03-26": 966,
    "2025-03-27": 1344, "2025-03-28": 1288, "2025-03-29": 2486,
    "2025-03-30": 3208, "2025-03-31": 2582, "2025-04-01": 1547,
    "2025-04-02": 1456, "2025-04-03": 1079, "2025-04-04": 1227,
    "2025-04-05": 2159, "2025-04-06": 1861, "2025-04-07": 1041,
    "2025-04-08": 665, "2025-04-09": 751, "2025-04-10": 1127,
    "2025-04-11": 887, "2025-04-12": 1782, "2025-04-13": 1599,
    "2025-04-14": 1155, "2025-04-15": 835,
}

# V1-style pipeline: total opps + overall conversion rate
V1_PIPELINE = {
    "2025-03-15": (5667, 0.2507), "2025-03-16": (5038, 0.2477),
    "2025-03-17": (2807, 0.2458), "2025-03-18": (2324, 0.1966),
    "2025-03-19": (2647, 0.2414), "2025-03-20": (3291, 0.2106),
    "2025-03-21": (3371, 0.2622), "2025-03-22": (5365, 0.2494),
    "2025-03-23": (5500, 0.2618), "2025-03-24": (3317, 0.2626),
    "2025-03-25": (3092, 0.2183), "2025-03-26": (3629, 0.2662),
    "2025-03-27": (4703, 0.2858), "2025-03-28": (5236, 0.2460),
    "2025-03-29": (9028, 0.2754), "2025-03-30": (12100, 0.2650),
    "2025-03-31": (10661, 0.2422), "2025-04-01": (6174, 0.2506),
    "2025-04-02": (5166, 0.2818), "2025-04-03": (4371, 0.2469),
    "2025-04-04": (4841, 0.2535), "2025-04-05": (7668, 0.2816),
    "2025-04-06": (6957, 0.2675), "2025-04-07": (4232, 0.2460),
    "2025-04-08": (3082, 0.2158), "2025-04-09": (3587, 0.2094),
    "2025-04-10": (5116, 0.2203), "2025-04-11": (3907, 0.2270),
    "2025-04-12": (6537, 0.2726), "2025-04-13": (6169, 0.2592),
    "2025-04-14": (4648, 0.2485), "2025-04-15": (4042, 0.2066),
}

# V2 pipeline: bucket-level opps (b0=0-1d, b1=2-3d, b2=4-7d, b3=8+d)
V2_BUCKETS: dict[str, list[int]] = {
    "2025-03-15": [1699, 624, 807, 2537],
    "2025-03-16": [2148, 542, 638, 1710],
    "2025-03-17": [1310, 279, 272, 946],
    "2025-03-18": [1201, 204, 158, 761],
    "2025-03-19": [1186, 317, 230, 914],
    "2025-03-20": [1318, 325, 296, 1352],
    "2025-03-21": [1261, 345, 429, 1336],
    "2025-03-22": [1742, 576, 759, 2288],
    "2025-03-23": [2192, 570, 670, 2068],
    "2025-03-24": [1405, 367, 371, 1174],
    "2025-03-25": [1332, 284, 286, 1190],
    "2025-03-26": [1470, 422, 380, 1357],
    "2025-03-27": [1727, 534, 513, 1929],
    "2025-03-28": [1769, 540, 626, 2301],
    "2025-03-29": [2597, 911, 1323, 4197],
    "2025-03-30": [3693, 1302, 1723, 5382],
    "2025-03-31": [3537, 1273, 1274, 4577],
    "2025-04-01": [2167, 731, 612, 2664],
    "2025-04-02": [1879, 707, 544, 2036],
    "2025-04-03": [1770, 535, 431, 1635],
    "2025-04-04": [1837, 536, 559, 1909],
    "2025-04-05": [2580, 873, 1037, 3178],
    "2025-04-06": [2828, 865, 827, 2437],
    "2025-04-07": [1838, 477, 406, 1511],
    "2025-04-08": [1507, 295, 234, 1046],
    "2025-04-09": [1707, 370, 297, 1213],
    "2025-04-10": [2012, 569, 569, 1966],
    "2025-04-11": [1641, 435, 394, 1437],
    "2025-04-12": [2249, 716, 936, 2636],
    "2025-04-13": [2553, 743, 750, 2123],
    "2025-04-14": [2076, 501, 471, 1600],
    "2025-04-15": [1653, 398, 359, 1632],
}

# Seasonal signals
SEASONAL = {
    "2025-03-15": (1555.9, 909.3), "2025-03-16": (1346.0, 761.1),
    "2025-03-17": (823.6, 662.5), "2025-03-18": (578.3, 624.6),
    "2025-03-19": (739.4, 616.8), "2025-03-20": (621.9, 656.7),
    "2025-03-21": (760.3, 667.5), "2025-03-22": (1578.4, 686.3),
    "2025-03-23": (1361.3, 681.7), "2025-03-24": (845.4, 708.7),
    "2025-03-25": (586.4, 810.9), "2025-03-26": (761.8, 852.1),
    "2025-03-27": (658.9, 975.7), "2025-03-28": (807.6, 1086.4),
    "2025-03-29": (1627.4, 958.6), "2025-03-30": (1424.5, 0.0),
    "2025-03-31": (889.4, 0.0), "2025-04-01": (594.1, 1673.3),
    "2025-04-02": (792.5, 1253.3), "2025-04-03": (753.0, 967.3),
    "2025-04-04": (893.4, 907.8), "2025-04-05": (1774.7, 937.0),
    "2025-04-06": (1467.4, 846.3), "2025-04-07": (912.8, 863.2),
    "2025-04-08": (710.9, 880.1), "2025-04-09": (895.5, 843.9),
    "2025-04-10": (834.3, 786.8), "2025-04-11": (959.3, 737.1),
    "2025-04-12": (1852.8, 701.7), "2025-04-13": (1554.3, 717.8),
    "2025-04-14": (948.0, 821.6), "2025-04-15": (719.9, 972.2),
}

# Bucket conversion rates by DOW (from 8-week lookback before Mar 15)
# Snowflake DAYOFWEEK: 0=Sun, 1=Mon, ..., 6=Sat
BUCKET_CONV_BY_DOW: dict[int, list[float]] = {
    0: [0.2280, 0.3833, 0.3684, 0.2698],  # Sunday
    1: [0.2260, 0.4662, 0.4516, 0.2847],  # Monday
    2: [0.2067, 0.4078, 0.4186, 0.2399],  # Tuesday
    3: [0.2139, 0.4133, 0.4098, 0.2612],  # Wednesday
    4: [0.2126, 0.4067, 0.4011, 0.2529],  # Thursday
    5: [0.2179, 0.3941, 0.3799, 0.2573],  # Friday
    6: [0.2265, 0.3929, 0.3934, 0.2745],  # Saturday
}

# V1 special dates for 2025 (Holi period, month-end, weekends)
# Approximate based on V1 SQL pattern
V1_SPECIAL_2025 = {
    "2025-03-15", "2025-03-16", "2025-03-20", "2025-03-21",
    "2025-03-22", "2025-03-27", "2025-03-28", "2025-03-29",
    "2025-03-30", "2025-03-31",
    "2025-04-01", "2025-04-02", "2025-04-03", "2025-04-04",
    "2025-04-05", "2025-04-06", "2025-04-12", "2025-04-13",
    "2025-04-14",
}

# Peak multiplier from 12-month history as of Mar 2025
PEAK_MULT_2025 = 1.73  # similar to 2026 value


def compute_v1(
    tw: float, tm: float, opps: int, conv: float, is_special: bool,
) -> dict[str, float]:
    """V1 model: floor=MIN, ceil=MAX, midpoint=avg, special=1.25x."""
    seasonal = min(tw, tm)
    pipeline = conv * opps
    floor = min(seasonal, pipeline)
    ceil = max(seasonal, pipeline)
    midpoint = (floor + ceil) / 2.0
    if is_special:
        floor *= 1.25
        ceil *= 1.25
        midpoint = (floor + ceil) / 2.0
    return {
        "floor": floor, "ceil": ceil, "midpoint": midpoint,
    }


def main() -> None:
    dates = sorted(ACTUALS.keys())
    n = len(dates)

    print("=" * 100)
    print("  WIDER BACKTEST: V1 SQL vs V2 Optimized vs Actuals")
    print(f"  Period: {dates[0]} → {dates[-1]} ({n} days)")
    print(
        "  Includes: Holi (Mar 14), month-end surge "
        "(Mar 29-31), early April"
    )
    print("=" * 100)

    rows: list[dict[str, Any]] = []

    for d in dates:
        actual = ACTUALS[d]
        tw, tm = SEASONAL[d]
        opps, conv = V1_PIPELINE[d]
        buckets = V2_BUCKETS[d]
        dt = datetime.date.fromisoformat(d)
        dow = dt.weekday()  # Python: 0=Mon..6=Sun
        sf_dow = (dow + 1) % 7  # Snowflake: 0=Sun..6=Sat

        # V1 forecast
        v1 = compute_v1(tw, tm, opps, conv, d in V1_SPECIAL_2025)

        # V2 forecast (optimized config)
        bucket_convs = BUCKET_CONV_BY_DOW.get(
            sf_dow, [0.20, 0.40, 0.40, 0.25],
        )
        pipeline_est = compute_pipeline_estimate(buckets, bucket_convs)
        seasonal_bl = compute_seasonal_baseline(
            tw, tm, float(PARAMS["ten_week_weight"]),
        )
        peak = is_peak_date(dt)
        total_opps = sum(buckets)

        v2_point = compute_point_estimate(
            floor=actual,  # using actual as floor (end-of-day backtest)
            pipeline_estimate=pipeline_est,
            seasonal_baseline=seasonal_bl,
            horizon=0,
            pipeline_opp_count=total_opps,
            is_peak=peak,
            peak_multiplier=PEAK_MULT_2025,
            params=PARAMS,
        )

        # V2 without conservative mode (pure blend) for comparison
        pure_params = dict(PARAMS)
        pure_params["conservative_nonpeak"] = False
        v2_pure = compute_point_estimate(
            floor=actual,
            pipeline_estimate=pipeline_est,
            seasonal_baseline=seasonal_bl,
            horizon=0,
            pipeline_opp_count=total_opps,
            is_peak=peak,
            peak_multiplier=PEAK_MULT_2025,
            params=pure_params,
        )

        # Errors
        v1_err = abs(v1["midpoint"] - actual) / max(actual, 1) * 100
        v2_err = abs(v2_point - actual) / max(actual, 1) * 100
        v2p_err = abs(v2_pure - actual) / max(actual, 1) * 100
        v1_in_range = v1["floor"] <= actual <= v1["ceil"]

        rows.append({
            "date": d, "dow": dt.strftime("%a"),
            "actual": actual,
            "v1_mid": v1["midpoint"], "v1_mape": v1_err,
            "v1_floor": v1["floor"], "v1_ceil": v1["ceil"],
            "v1_in_range": v1_in_range,
            "v2_opt": v2_point, "v2_mape": v2_err,
            "v2_pure": v2_pure, "v2p_mape": v2p_err,
            "is_peak": peak,
            "is_peak_broad": is_peak_date_broad(dt),
            "is_v1_special": d in V1_SPECIAL_2025,
        })

    # ── Table ────────────────────────────────────────────────────────
    hdr = (
        f"\n{'Date':<12} {'Day':<4} {'Actual':>7} │ "
        f"{'V1 Mid':>7} {'V1 Err':>7} │ "
        f"{'V2 Opt':>7} {'V2 Err':>7} │ "
        f"{'V2Pure':>7} {'PrErr':>7} │ "
        f"{'Best':>6} {'Pk':>3}"
    )
    print(hdr)
    print("─" * 95)

    for r in rows:
        v1e = r["v1_mape"]
        v2e = r["v2_mape"]
        v2pe = r["v2p_mape"]
        best_err = min(v1e, v2e, v2pe)
        if best_err == v2e:
            best = "V2opt"
        elif best_err == v2pe:
            best = "V2pur"
        else:
            best = "V1"
        pk = "P" if r["is_peak"] else ""
        print(
            f"{r['date']:<12} {r['dow']:<4} {r['actual']:>7} │ "
            f"{r['v1_mid']:>7.0f} {v1e:>6.1f}% │ "
            f"{r['v2_opt']:>7.0f} {v2e:>6.1f}% │ "
            f"{r['v2_pure']:>7.0f} {v2pe:>6.1f}% │ "
            f"{best:>6} {pk:>3}"
        )

    # ── Summary ──────────────────────────────────────────────────────
    v1_mape = sum(r["v1_mape"] for r in rows) / n
    v2_mape = sum(r["v2_mape"] for r in rows) / n
    v2p_mape = sum(r["v2p_mape"] for r in rows) / n
    v1_wins = sum(
        1 for r in rows
        if r["v1_mape"] <= r["v2_mape"]
        and r["v1_mape"] <= r["v2p_mape"]
    )
    v2_wins = sum(
        1 for r in rows
        if r["v2_mape"] <= r["v1_mape"]
        and r["v2_mape"] <= r["v2p_mape"]
    )
    v2p_wins = sum(
        1 for r in rows
        if r["v2p_mape"] <= r["v1_mape"]
        and r["v2p_mape"] <= r["v2_mape"]
    )
    v1_range = sum(1 for r in rows if r["v1_in_range"])

    print(f"\n{'=' * 72}")
    print("  SUMMARY STATISTICS")
    print(f"{'=' * 72}")
    print(
        f"  {'Metric':<30} {'V1 SQL':>10} "
        f"{'V2 Optim':>10} {'V2 Pure':>10}"
    )
    sep = "─" * 64
    print(f"  {sep}")
    print(
        f"  {'MAPE':<30} {v1_mape:>9.1f}% "
        f"{v2_mape:>9.1f}% {v2p_mape:>9.1f}%"
    )
    print(
        f"  {'Days Won':<30} {v1_wins:>10} "
        f"{v2_wins:>10} {v2p_wins:>10}"
    )
    print(f"  {'V1 Range Coverage':<30} "
          f"{v1_range}/{n} ({v1_range/n*100:.0f}%)")

    # Peak vs non-peak
    peak_rows = [r for r in rows if r["is_peak"]]
    np_rows = [r for r in rows if not r["is_peak"]]

    if peak_rows:
        pk_v1 = sum(r["v1_mape"] for r in peak_rows) / len(peak_rows)
        pk_v2 = sum(r["v2_mape"] for r in peak_rows) / len(peak_rows)
        print(f"\n  Peak Days ({len(peak_rows)} days, narrow def):")
        print(f"    V1 MAPE:   {pk_v1:>6.1f}%")
        print(f"    V2 MAPE:   {pk_v2:>6.1f}%")

    if np_rows:
        np_v1 = sum(r["v1_mape"] for r in np_rows) / len(np_rows)
        np_v2 = sum(r["v2_mape"] for r in np_rows) / len(np_rows)
        print(f"  Non-Peak Days ({len(np_rows)} days):")
        print(f"    V1 MAPE:   {np_v1:>6.1f}%")
        print(f"    V2 MAPE:   {np_v2:>6.1f}%")

    # Month-end surge analysis (Mar 29-31)
    surge = [r for r in rows if r["date"] in {
        "2025-03-29", "2025-03-30", "2025-03-31",
    }]
    if surge:
        print("\n  Month-End Surge (Mar 29-31):")
        for r in surge:
            print(
                f"    {r['date']}: actual={r['actual']}  "
                f"V1={r['v1_mid']:.0f} ({r['v1_mape']:.1f}%)  "
                f"V2={r['v2_opt']:.0f} ({r['v2_mape']:.1f}%)"
            )

    # Bias
    v1_over = sum(1 for r in rows if r["v1_mid"] > r["actual"])
    v2_over = sum(1 for r in rows if r["v2_opt"] > r["actual"])
    print("\n  Over-prediction rate:")
    print(f"    V1: {v1_over}/{n} ({v1_over/n*100:.0f}%)")
    print(f"    V2: {v2_over}/{n} ({v2_over/n*100:.0f}%)")

    # Verdict
    delta = v1_mape - v2_mape
    print(f"\n{'=' * 72}")
    if delta > 0:
        print(f"  V2 Optimized BEATS V1 by {delta:.1f}pp MAPE!")
    else:
        print(f"  V2 Optimized trails V1 by {-delta:.1f}pp MAPE")
    print(f"{'=' * 72}")

    # Save
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    out_path = output_dir / "backtest_wide_2025.json"
    with open(out_path, "w") as f:
        json.dump(rows, f, indent=2, default=str)
    print(f"\n  Raw data saved to: {out_path}")


if __name__ == "__main__":
    main()
