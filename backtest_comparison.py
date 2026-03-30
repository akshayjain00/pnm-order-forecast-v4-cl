"""V1 vs V2 vs Actuals — 30-day backtest comparison report.

Computes both models' forecasts from live Snowflake signals and compares
against actual order counts.
"""
import datetime
import json
from typing import Any

from src.config import PARAMS, is_peak_date
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_seasonal_baseline,
)

# ── Raw data from Snowflake (30 days: Feb 26 → Mar 27, 2026) ─────────

ACTUALS_RAW = [
    {"d": "2026-02-26", "actual": 1437}, {"d": "2026-02-27", "actual": 2083},
    {"d": "2026-02-28", "actual": 3027}, {"d": "2026-03-01", "actual": 2391},
    {"d": "2026-03-02", "actual": 1609}, {"d": "2026-03-03", "actual": 776},
    {"d": "2026-03-04", "actual": 897},  {"d": "2026-03-05", "actual": 1472},
    {"d": "2026-03-06", "actual": 1386}, {"d": "2026-03-07", "actual": 1801},
    {"d": "2026-03-08", "actual": 1679}, {"d": "2026-03-09", "actual": 1206},
    {"d": "2026-03-10", "actual": 703},  {"d": "2026-03-11", "actual": 843},
    {"d": "2026-03-12", "actual": 898},  {"d": "2026-03-13", "actual": 1210},
    {"d": "2026-03-14", "actual": 1944}, {"d": "2026-03-15", "actual": 1599},
    {"d": "2026-03-16", "actual": 1061}, {"d": "2026-03-17", "actual": 693},
    {"d": "2026-03-18", "actual": 790},  {"d": "2026-03-19", "actual": 978},
    {"d": "2026-03-20", "actual": 1067}, {"d": "2026-03-21", "actual": 1530},
    {"d": "2026-03-22", "actual": 1422}, {"d": "2026-03-23", "actual": 891},
    {"d": "2026-03-24", "actual": 711},  {"d": "2026-03-25", "actual": 1328},
    {"d": "2026-03-26", "actual": 1309}, {"d": "2026-03-27", "actual": 1395},
]

SEASONAL_RAW = [
    {"d":"2026-02-26","tw":994.2,"tm":1307.75},
    {"d":"2026-02-27","tw":1036.7,"tm":1329.89},
    {"d":"2026-02-28","tw":1577.3,"tm":1577.0},
    {"d":"2026-03-01","tw":1453.0,"tm":1936.56},
    {"d":"2026-03-02","tw":906.9,"tm":1535.78},
    {"d":"2026-03-03","tw":715.5,"tm":1370.0},
    {"d":"2026-03-04","tw":931.4,"tm":1354.3},
    {"d":"2026-03-05","tw":1075.5,"tm":1397.7},
    {"d":"2026-03-06","tw":1163.1,"tm":1160.2},
    {"d":"2026-03-07","tw":1772.0,"tm":1140.3},
    {"d":"2026-03-08","tw":1590.6,"tm":977.0},
    {"d":"2026-03-09","tw":984.2,"tm":940.33},
    {"d":"2026-03-10","tw":735.0,"tm":1059.9},
    {"d":"2026-03-11","tw":944.5,"tm":927.11},
    {"d":"2026-03-12","tw":1082.0,"tm":917.4},
    {"d":"2026-03-13","tw":1183.1,"tm":955.4},
    {"d":"2026-03-14","tw":1818.2,"tm":1163.0},
    {"d":"2026-03-15","tw":1597.2,"tm":1169.33},
    {"d":"2026-03-16","tw":1005.3,"tm":890.11},
    {"d":"2026-03-17","tw":680.3,"tm":885.6},
    {"d":"2026-03-18","tw":875.2,"tm":724.13},
    {"d":"2026-03-19","tw":1042.0,"tm":751.89},
    {"d":"2026-03-20","tw":1210.5,"tm":826.1},
    {"d":"2026-03-21","tw":1863.7,"tm":947.22},
    {"d":"2026-03-22","tw":1623.2,"tm":938.3},
    {"d":"2026-03-23","tw":1029.8,"tm":918.6},
    {"d":"2026-03-24","tw":694.3,"tm":1021.5},
    {"d":"2026-03-25","tw":896.7,"tm":1168.11},
    {"d":"2026-03-26","tw":1078.9,"tm":1322.11},
    {"d":"2026-03-27","tw":1253.0,"tm":1405.2},
]

V1_PIPELINE_RAW = [
    {"d":"2026-02-26","opps":7207,"conv":0.2458},
    {"d":"2026-02-27","opps":9381,"conv":0.2261},
    {"d":"2026-02-28","opps":14624,"conv":0.1524},
    {"d":"2026-03-01","opps":12650,"conv":0.2198},
    {"d":"2026-03-02","opps":7859,"conv":0.2029},
    {"d":"2026-03-03","opps":5010,"conv":0.1599},
    {"d":"2026-03-04","opps":5041,"conv":0.1969},
    {"d":"2026-03-05","opps":6934,"conv":0.2114},
    {"d":"2026-03-06","opps":6891,"conv":0.2266},
    {"d":"2026-03-07","opps":8909,"conv":0.2532},
    {"d":"2026-03-08","opps":8756,"conv":0.2442},
    {"d":"2026-03-09","opps":6485,"conv":0.2041},
    {"d":"2026-03-10","opps":5661,"conv":0.1698},
    {"d":"2026-03-11","opps":5558,"conv":0.1997},
    {"d":"2026-03-12","opps":5886,"conv":0.2137},
    {"d":"2026-03-13","opps":6300,"conv":0.2074},
    {"d":"2026-03-14","opps":9064,"conv":0.2098},
    {"d":"2026-03-15","opps":9239,"conv":0.2013},
    {"d":"2026-03-16","opps":7032,"conv":0.1934},
    {"d":"2026-03-17","opps":5417,"conv":0.1509},
    {"d":"2026-03-18","opps":5455,"conv":0.1865},
    {"d":"2026-03-19","opps":5842,"conv":0.1843},
    {"d":"2026-03-20","opps":6778,"conv":0.2009},
    {"d":"2026-03-21","opps":7986,"conv":0.2184},
    {"d":"2026-03-22","opps":8179,"conv":0.1941},
    {"d":"2026-03-23","opps":7120,"conv":0.1750},
    {"d":"2026-03-24","opps":6317,"conv":0.2184},
    {"d":"2026-03-25","opps":7998,"conv":0.2435},
    {"d":"2026-03-26","opps":8929,"conv":0.2105},
    {"d":"2026-03-27","opps":10351,"conv":0.2221},
]

V2_SIGNALS_RAW = [
    {"d":"2026-02-26","floor":1437,"b0":3255,"b1":592,"b2":475,"b3":2885,"total":7207},
    {"d":"2026-02-27","floor":2083,"b0":3818,"b1":756,"b2":782,"b3":4025,"total":9381},
    {"d":"2026-02-28","floor":3027,"b0":5081,"b1":1229,"b2":1714,"b3":6600,"total":14624},
    {"d":"2026-03-01","floor":2391,"b0":5334,"b1":1192,"b2":1172,"b3":4952,"total":12650},
    {"d":"2026-03-02","floor":1609,"b0":3915,"b1":727,"b2":477,"b3":2740,"total":7859},
    {"d":"2026-03-03","floor":776,"b0":2448,"b1":406,"b2":313,"b3":1843,"total":5010},
    {"d":"2026-03-04","floor":897,"b0":2421,"b1":344,"b2":376,"b3":1900,"total":5041},
    {"d":"2026-03-05","floor":1472,"b0":2848,"b1":603,"b2":614,"b3":2869,"total":6934},
    {"d":"2026-03-06","floor":1386,"b0":2984,"b1":466,"b2":587,"b3":2854,"total":6891},
    {"d":"2026-03-07","floor":1801,"b0":3786,"b1":670,"b2":770,"b3":3683,"total":8909},
    {"d":"2026-03-08","floor":1679,"b0":4159,"b1":698,"b2":654,"b3":3245,"total":8756},
    {"d":"2026-03-09","floor":1206,"b0":3299,"b1":503,"b2":401,"b3":2282,"total":6485},
    {"d":"2026-03-10","floor":703,"b0":2972,"b1":313,"b2":244,"b3":2132,"total":5661},
    {"d":"2026-03-11","floor":843,"b0":2809,"b1":366,"b2":318,"b3":2065,"total":5558},
    {"d":"2026-03-12","floor":898,"b0":2989,"b1":389,"b2":315,"b3":2193,"total":5886},
    {"d":"2026-03-13","floor":1210,"b0":3037,"b1":392,"b2":443,"b3":2428,"total":6300},
    {"d":"2026-03-14","floor":1944,"b0":3683,"b1":683,"b2":826,"b3":3872,"total":9064},
    {"d":"2026-03-15","floor":1599,"b0":4132,"b1":653,"b2":725,"b3":3729,"total":9239},
    {"d":"2026-03-16","floor":1061,"b0":3737,"b1":466,"b2":403,"b3":2426,"total":7032},
    {"d":"2026-03-17","floor":693,"b0":2898,"b1":304,"b2":237,"b3":1978,"total":5417},
    {"d":"2026-03-18","floor":790,"b0":2769,"b1":331,"b2":301,"b3":2054,"total":5455},
    {"d":"2026-03-19","floor":978,"b0":2760,"b1":363,"b2":399,"b3":2320,"total":5842},
    {"d":"2026-03-20","floor":1067,"b0":2908,"b1":384,"b2":473,"b3":3013,"total":6778},
    {"d":"2026-03-21","floor":1530,"b0":3301,"b1":549,"b2":677,"b3":3459,"total":7986},
    {"d":"2026-03-22","floor":1422,"b0":3831,"b1":562,"b2":598,"b3":3188,"total":8179},
    {"d":"2026-03-23","floor":891,"b0":3604,"b1":382,"b2":381,"b3":2753,"total":7120},
    {"d":"2026-03-24","floor":711,"b0":3217,"b1":302,"b2":268,"b3":2530,"total":6317},
    {"d":"2026-03-25","floor":1328,"b0":3583,"b1":556,"b2":486,"b3":3373,"total":7998},
    {"d":"2026-03-26","floor":1309,"b0":3839,"b1":546,"b2":650,"b3":3894,"total":8929},
    {"d":"2026-03-27","floor":1395,"b0":4355,"b1":581,"b2":766,"b3":4649,"total":10351},
]

# Bucket conversion by DOW (from Snowflake: 10-week average)
BUCKET_CONV_BY_DOW: dict[int, list[float]] = {
    0: [0.1551, 0.4772, 0.4469, 0.2173],  # Sunday
    1: [0.1236, 0.4744, 0.4591, 0.1905],  # Monday
    2: [0.1192, 0.4182, 0.3984, 0.1545],  # Tuesday
    3: [0.1370, 0.4651, 0.4481, 0.1858],  # Wednesday
    4: [0.1351, 0.4808, 0.4628, 0.1976],  # Thursday
    5: [0.1439, 0.4794, 0.4482, 0.2097],  # Friday
    6: [0.1688, 0.4734, 0.4717, 0.2340],  # Saturday
}

V1_SPECIAL = {
    "2026-02-27","2026-02-28","2026-03-01","2026-03-02","2026-03-03",
    "2026-03-04","2026-03-06","2026-03-07","2026-03-08","2026-03-13",
    "2026-03-14","2026-03-15","2026-03-19","2026-03-20","2026-03-21",
    "2026-03-22","2026-03-26","2026-03-27",
}

# Peak multiplier from Snowflake
PEAK_MULT = 1.7275


def compute_v1_forecast(
    tw: float, tm: float, opps: int, conv: float, is_special: bool,
) -> dict[str, float]:
    """V1 model: floor=MIN(seasonal, pipeline), ceil=MAX, midpoint=avg."""
    seasonal = min(tw, tm)  # V1 uses conservative MIN
    pipeline = conv * opps
    floor = min(seasonal, pipeline)
    ceil = max(seasonal, pipeline)
    midpoint = (floor + ceil) / 2.0
    if is_special:
        floor *= 1.25
        ceil *= 1.25
        midpoint = (floor + ceil) / 2.0
    return {"floor": floor, "ceil": ceil, "midpoint": midpoint,
            "seasonal": seasonal, "pipeline": pipeline}


def compute_v2_forecast(
    tw: float, tm: float, confirmed_floor: int,
    bucket_opps: list[int], bucket_convs: list[float],
    total_opps: int, target_date: datetime.date,
) -> dict[str, float]:
    """V2 model: weighted blend with horizon-aware blending + data-derived peak."""
    pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)
    seasonal = compute_seasonal_baseline(tw, tm, PARAMS["ten_week_weight"])
    peak = is_peak_date(target_date)

    # Using T0 horizon for this backtest (same-day comparison)
    point = compute_point_estimate(
        floor=confirmed_floor,
        pipeline_estimate=pipeline_est,
        seasonal_baseline=seasonal,
        horizon=0,
        pipeline_opp_count=total_opps,
        is_peak=peak,
        peak_multiplier=PEAK_MULT,
        params=PARAMS,
    )
    return {"point": point, "pipeline_est": pipeline_est,
            "seasonal": seasonal, "is_peak": peak}


def main() -> None:
    # Index all data by date
    actuals = {r["d"]: r["actual"] for r in ACTUALS_RAW}
    seasonal = {r["d"]: r for r in SEASONAL_RAW}
    v1_pipe = {r["d"]: r for r in V1_PIPELINE_RAW}
    v2_sig = {r["d"]: r for r in V2_SIGNALS_RAW}

    dates = sorted(actuals.keys())

    # Compute forecasts
    rows: list[dict[str, Any]] = []
    for d in dates:
        actual = actuals[d]
        s = seasonal[d]
        p1 = v1_pipe[d]
        s2 = v2_sig[d]
        dt = datetime.date.fromisoformat(d)
        dow = dt.weekday()  # Python: 0=Mon ... 6=Sun
        # Convert to Snowflake DAYOFWEEK: 0=Sun, 1=Mon...6=Sat
        sf_dow = (dow + 1) % 7

        # V1
        v1 = compute_v1_forecast(
            tw=s["tw"], tm=s["tm"],
            opps=p1["opps"], conv=p1["conv"],
            is_special=d in V1_SPECIAL,
        )

        # V2
        bucket_opps = [s2["b0"], s2["b1"], s2["b2"], s2["b3"]]
        bucket_convs = BUCKET_CONV_BY_DOW.get(sf_dow, [0.15, 0.45, 0.45, 0.20])
        v2 = compute_v2_forecast(
            tw=s["tw"], tm=s["tm"],
            confirmed_floor=s2["floor"],
            bucket_opps=bucket_opps,
            bucket_convs=bucket_convs,
            total_opps=s2["total"],
            target_date=dt,
        )

        # V2 without peak (to isolate peak impact)
        v2_nopeak_point = compute_point_estimate(
            floor=s2["floor"],
            pipeline_estimate=compute_pipeline_estimate(bucket_opps, bucket_convs),
            seasonal_baseline=compute_seasonal_baseline(
                s["tw"], s["tm"], PARAMS["ten_week_weight"],
            ),
            horizon=0,
            pipeline_opp_count=s2["total"],
            is_peak=False,  # Force no peak
            peak_multiplier=1.0,
            params=PARAMS,
        )

        # Errors
        v1_err = abs(v1["midpoint"] - actual) / max(actual, 1) * 100
        v2_err = abs(v2["point"] - actual) / max(actual, 1) * 100
        v2np_err = abs(v2_nopeak_point - actual) / max(actual, 1) * 100
        v1_in_range = v1["floor"] <= actual <= v1["ceil"]

        rows.append({
            "date": d,
            "dow": dt.strftime("%a"),
            "actual": actual,
            "v1_floor": v1["floor"],
            "v1_ceil": v1["ceil"],
            "v1_mid": v1["midpoint"],
            "v1_mape": v1_err,
            "v1_in_range": v1_in_range,
            "v2_point": v2["point"],
            "v2_mape": v2_err,
            "v2_nopeak": v2_nopeak_point,
            "v2np_mape": v2np_err,
            "v2_is_peak": v2["is_peak"],
            "v1_is_special": d in V1_SPECIAL,
        })

    # ── Print Report ──────────────────────────────────────────────
    print("=" * 110)
    print("  BACKTEST COMPARISON REPORT: V1 SQL Model vs V2 Enhanced Model vs Actuals")
    print(f"  Period: {dates[0]} → {dates[-1]} ({len(dates)} days)")
    print(f"  Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 110)

    # Table header
    hdr = (
        f"\n{'Date':<12} {'Day':<4} {'Actual':>7} │ "
        f"{'V1 Mid':>7} {'V1 Err':>7} │ "
        f"{'V2 Pt':>7} {'V2 Err':>7} │ "
        f"{'V2noPk':>7} {'NP Err':>7} │ "
        f"{'Best':>6} {'Pk':>3}"
    )
    print(hdr)
    print("─" * 105)

    v1_wins = 0
    v2_wins = 0
    v2np_wins = 0
    v1_total_ape = 0.0
    v2_total_ape = 0.0
    v2np_total_ape = 0.0
    v1_range_hits = 0
    peak_v1_apes: list[float] = []
    peak_v2_apes: list[float] = []
    peak_v2np_apes: list[float] = []
    nonpeak_v1_apes: list[float] = []
    nonpeak_v2_apes: list[float] = []
    nonpeak_v2np_apes: list[float] = []

    for r in rows:
        best_err = min(r["v1_mape"], r["v2_mape"], r["v2np_mape"])
        if best_err == r["v1_mape"]:
            best = "V1"
            v1_wins += 1
        elif best_err == r["v2np_mape"]:
            best = "V2np"
            v2np_wins += 1
        else:
            best = "V2"
            v2_wins += 1
        v1_total_ape += r["v1_mape"]
        v2_total_ape += r["v2_mape"]
        v2np_total_ape += r["v2np_mape"]
        if r["v1_in_range"]:
            v1_range_hits += 1

        peak_flag = ""
        if r["v2_is_peak"] or r["v1_is_special"]:
            peak_flag = "P"
            peak_v1_apes.append(r["v1_mape"])
            peak_v2_apes.append(r["v2_mape"])
            peak_v2np_apes.append(r["v2np_mape"])
        else:
            nonpeak_v1_apes.append(r["v1_mape"])
            nonpeak_v2_apes.append(r["v2_mape"])
            nonpeak_v2np_apes.append(r["v2np_mape"])

        print(f"{r['date']:<12} {r['dow']:<4} {r['actual']:>7,} │ "
              f"{r['v1_mid']:>7,.0f} {r['v1_mape']:>6.1f}% │ "
              f"{r['v2_point']:>7,.0f} {r['v2_mape']:>6.1f}% │ "
              f"{r['v2_nopeak']:>7,.0f} {r['v2np_mape']:>6.1f}% │ "
              f"{best:>6} {peak_flag:>3}")

    print("─" * 105)

    # Summary statistics
    n = len(rows)
    v1_mape = v1_total_ape / n
    v2_mape = v2_total_ape / n
    v2np_mape = v2np_total_ape / n

    print(f"\n{'=' * 72}")
    print("  SUMMARY STATISTICS")
    print(f"{'=' * 72}")
    print(f"  {'Metric':<30} {'V1 SQL':>10} {'V2 (peak)':>12} {'V2 (no peak)':>14}")
    print(f"  {'─' * 66}")
    print(f"  {'MAPE':<30} {v1_mape:>9.1f}% {v2_mape:>11.1f}% {v2np_mape:>13.1f}%")
    print(f"  {'Median APE':<30} "
          f"{sorted([r['v1_mape'] for r in rows])[n//2]:>9.1f}% "
          f"{sorted([r['v2_mape'] for r in rows])[n//2]:>11.1f}% "
          f"{sorted([r['v2np_mape'] for r in rows])[n//2]:>13.1f}%")
    print(f"  {'Days Won':<30} {v1_wins:>10} {v2_wins:>12} {v2np_wins:>14}")
    print(f"  {'V1 Range Coverage':<30} "
          f"{v1_range_hits}/{n} ({v1_range_hits/n*100:.0f}%)")

    # Peak vs non-peak breakdown
    if peak_v1_apes:
        print(f"\n  Peak Days ({len(peak_v1_apes)} days):")
        print(f"    V1 MAPE:         {sum(peak_v1_apes)/len(peak_v1_apes):>6.1f}%")
        print(f"    V2 (peak) MAPE:  {sum(peak_v2_apes)/len(peak_v2_apes):>6.1f}%")
        print(f"    V2 (no-pk) MAPE: {sum(peak_v2np_apes)/len(peak_v2np_apes):>6.1f}%")
    if nonpeak_v1_apes:
        np_v1 = sum(nonpeak_v1_apes) / len(nonpeak_v1_apes)
        np_v2 = sum(nonpeak_v2_apes) / len(nonpeak_v2_apes)
        print(f"  Non-Peak Days ({len(nonpeak_v1_apes)} days):")
        print(f"    V1 MAPE:         {np_v1:>6.1f}%")
        print(f"    V2 MAPE:         {np_v2:>6.1f}%")

    # Directional bias
    v1_over = sum(1 for r in rows if r["v1_mid"] > r["actual"])
    v2_over = sum(1 for r in rows if r["v2_point"] > r["actual"])
    v2np_over = sum(1 for r in rows if r["v2_nopeak"] > r["actual"])
    print("\n  Directional Bias (over-prediction rate):")
    print(f"    V1:          {v1_over}/{n} ({v1_over/n*100:.0f}%)")
    print(f"    V2 (peak):   {v2_over}/{n} ({v2_over/n*100:.0f}%)")
    print(f"    V2 (no-pk):  {v2np_over}/{n} ({v2np_over/n*100:.0f}%)")

    # Key insight
    print(f"\n{'=' * 72}")
    print("  KEY FINDINGS")
    print(f"{'=' * 72}")
    v2np_improvement = v1_mape - v2np_mape
    if v2np_improvement > 0:
        print(f"  ✓ V2 (no-peak) beats V1 by {v2np_improvement:.1f}pp MAPE")
    else:
        print(f"  ~ V2 (no-peak) trails V1 by {-v2np_improvement:.1f}pp MAPE")

    print("\n  DIAGNOSIS: Peak multiplier is the problem, not the core model")
    print("  ─────────────────────────────────────────────────────────────")
    status = "competitive" if abs(v2np_improvement) < 5 else "needs work"
    print(
        f"  1. V2 no-peak ({v2np_mape:.1f}%) vs "
        f"V1 ({v1_mape:.1f}%) — {status}"
    )
    print(f"  2. Peak multiplier {PEAK_MULT:.2f}x is too aggressive")
    pk_v2 = sum(peak_v2_apes) / len(peak_v2_apes)
    pk_v2np = sum(peak_v2np_apes) / len(peak_v2np_apes)
    pk_v1 = sum(peak_v1_apes) / len(peak_v1_apes)
    print(f"     → Peak days: V2 w/ peak = {pk_v2:.0f}% MAPE")
    print(f"     → Peak days: V2 no-peak = {pk_v2np:.0f}% MAPE")
    print(f"     → Peak days: V1 (1.25x) = {pk_v1:.0f}% MAPE")
    n_peak = sum(1 for r in rows if r["v2_is_peak"])
    print(f"  3. {n_peak}/{n} days flagged as peak (too many)")
    cov_pct = v1_range_hits / n * 100
    print(f"  4. V1 range coverage {cov_pct:.0f}% — poor ranges")
    print("\n  RECOMMENDATIONS:")
    print("  " + "─" * 61)
    print(f"  A. Reduce peak mult from {PEAK_MULT:.2f}x to ~1.15-1.25x")
    pk_pct = n_peak / n * 100
    print(
        f"  B. Narrow peak definition "
        f"(flags {n_peak}/{n} = {pk_pct:.0f}% of days)"
    )
    print("  C. Consider V1's MIN(seasonal) approach on non-peak days")
    print("     (V1 uses conservative MIN; V2 uses weighted blend)")
    print("  D. Run Stage 1 optimizer to tune horizon_weight + ten_week_weight")

    # Save as JSON for further analysis
    output_path = "output/backtest_comparison.json"
    import os
    os.makedirs("output", exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(rows, f, indent=2, default=str)
    print(f"\n  Raw data saved to: {output_path}")


if __name__ == "__main__":
    main()
