"""Calibrate prediction interval percentiles from backtest residuals.

Reads output/backtest_multihorizon.json (must contain rel_error, horizon,
is_peak fields) and computes stratified P17.5 / P82.5 for 65% coverage.

Usage:
    python -m src.calibrate_ranges                    # uses default backtest file
    python -m src.calibrate_ranges --files output/backtest_multihorizon.json output/backtest_wide_2025.json
    python -m src.calibrate_ranges --apply            # also patches HybridParams defaults in config.py

See spec: docs/HYBRID_ARCHITECTURE.md Section 3.5 (calibration)
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import numpy as np

OUTPUT_DIR = Path(__file__).parent.parent / "output"
CONFIG_PATH = Path(__file__).parent / "config.py"

# 65% symmetric coverage → (1-0.65)/2 = 17.5th, (1+0.65)/2 = 82.5th
TARGET_COVERAGE = 0.65
LOWER_PCTL = (1.0 - TARGET_COVERAGE) / 2.0 * 100   # 17.5
UPPER_PCTL = (1.0 + TARGET_COVERAGE) / 2.0 * 100   # 82.5

HORIZONS = [0, 1, 2]
SUFFIXES = ["nonpeak", "peak"]


def load_residuals(paths: list[Path]) -> list[dict[str, Any]]:
    """Load and merge records from one or more backtest JSON files."""
    records: list[dict[str, Any]] = []
    for p in paths:
        with p.open() as fh:
            data = json.load(fh)
        records.extend(data)
    return records


def compute_stratified_percentiles(
    records: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    """Compute P17.5/P82.5 per (horizon, peak/nonpeak).

    Returns dict keyed by e.g. "T0_nonpeak" with keys "lower" and "upper".

    Notes:
    - Uses rel_error = (actual - estimate) / max(estimate, 1)
    - Records missing rel_error are skipped
    - For peak strata with < 5 samples, falls back to all-horizon peak pool
      and warns — small samples give unreliable percentiles
    """
    # Bucket residuals: buckets[horizon][is_peak] = [rel_error, ...]
    buckets: dict[int, dict[bool, list[float]]] = {
        h: {True: [], False: []} for h in HORIZONS
    }

    skipped = 0
    for rec in records:
        h = rec.get("horizon")
        err = rec.get("rel_error")
        is_peak = bool(rec.get("is_peak", False))
        if h not in HORIZONS or err is None:
            skipped += 1
            continue
        buckets[h][is_peak].append(float(err))

    if skipped:
        print(f"  [calibrate] Skipped {skipped} records missing horizon or rel_error")

    results: dict[str, dict[str, float]] = {}
    for h in HORIZONS:
        for is_peak in [False, True]:
            suffix = "peak" if is_peak else "nonpeak"
            key = f"T{h}_{suffix}"
            errs = buckets[h][is_peak]

            if len(errs) < 5:
                # Warn and fall back to all-horizon pool for this peak type
                all_errs = []
                for hh in HORIZONS:
                    all_errs.extend(buckets[hh][is_peak])
                print(
                    f"  [calibrate] WARNING {key}: only {len(errs)} samples — "
                    f"falling back to all-horizon {suffix} pool ({len(all_errs)} samples)"
                )
                errs = all_errs if all_errs else [0.0]

            arr = np.array(errs, dtype=float)
            results[key] = {
                "lower": float(np.percentile(arr, LOWER_PCTL)),
                "upper": float(np.percentile(arr, UPPER_PCTL)),
                "n": len(errs),
                "median": float(np.median(arr)),
            }

    return results


def print_report(results: dict[str, dict[str, float]]) -> None:
    """Print a human-readable calibration report."""
    print()
    print("=" * 65)
    print(f"  Calibration report  (target coverage = {TARGET_COVERAGE:.0%})")
    print(f"  Percentile bounds: P{LOWER_PCTL:.1f} (lower)  P{UPPER_PCTL:.1f} (upper)")
    print("=" * 65)
    print(f"  {'Stratum':<20} {'n':>4}  {'lower':>8}  {'upper':>8}  {'median':>8}")
    print("  " + "-" * 55)
    for key in sorted(results.keys()):
        r = results[key]
        print(
            f"  {key:<20} {r['n']:>4}  "
            f"{r['lower']:>8.4f}  {r['upper']:>8.4f}  {r['median']:>8.4f}"
        )
    print()
    print("  HybridParams updates to apply:")
    print("  " + "-" * 55)
    for h in HORIZONS:
        for suffix in SUFFIXES:
            key = f"T{h}_{suffix}"
            r = results[key]
            lower_field = f"range_lower_pctl_T{h}_{suffix}"
            upper_field = f"range_upper_pctl_T{h}_{suffix}"
            print(f"    {lower_field} = {r['lower']:.4f}")
            print(f"    {upper_field} = {r['upper']:.4f}")
    print()


def apply_to_config(results: dict[str, dict[str, float]]) -> None:
    """Patch the HybridParams defaults in src/config.py in-place.

    Replaces the float literal for each range_*_pctl_T{h}_{suffix} field.
    Creates a backup at src/config.py.bak before modifying.
    """
    import shutil

    backup = CONFIG_PATH.with_suffix(".py.bak")
    shutil.copy2(CONFIG_PATH, backup)
    print(f"  [calibrate] Backed up config.py → {backup.name}")

    text = CONFIG_PATH.read_text()

    for h in HORIZONS:
        for suffix in SUFFIXES:
            key = f"T{h}_{suffix}"
            r = results[key]
            for bound, val in [("lower", r["lower"]), ("upper", r["upper"])]:
                field = f"range_{bound}_pctl_T{h}_{suffix}"
                # Match e.g.   range_lower_pctl_T0_peak: float = -0.0123
                pattern = rf"({re.escape(field)}: float = )[-\d.]+"
                replacement = rf"\g<1>{val:.4f}"
                new_text, count = re.subn(pattern, replacement, text)
                if count == 1:
                    text = new_text
                    print(f"  [calibrate]   {field} → {val:.4f}")
                elif count == 0:
                    print(f"  [calibrate] WARNING: field {field} not found in config.py")
                else:
                    print(
                        f"  [calibrate] WARNING: {count} matches for {field} — skipping"
                    )

    CONFIG_PATH.write_text(text)
    print(f"  [calibrate] config.py updated.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate hybrid range percentiles")
    parser.add_argument(
        "--files",
        nargs="+",
        type=Path,
        default=[OUTPUT_DIR / "backtest_multihorizon.json"],
        help="Backtest JSON files to load (default: output/backtest_multihorizon.json)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Patch HybridParams defaults in src/config.py with calibrated values",
    )
    args = parser.parse_args()

    files = [Path(f) for f in args.files]
    missing = [f for f in files if not f.exists()]
    if missing:
        raise FileNotFoundError(f"Backtest files not found: {missing}")

    print(f"Loading residuals from {len(files)} file(s):")
    for f in files:
        print(f"  {f}")

    records = load_residuals(files)
    print(f"  Total records: {len(records)}")

    results = compute_stratified_percentiles(records)
    print_report(results)

    if args.apply:
        apply_to_config(results)
    else:
        print("  (pass --apply to patch src/config.py with these values)")

    # Write calibration output for reference
    out_path = OUTPUT_DIR / "calibration_results.json"
    with out_path.open("w") as fh:
        json.dump(
            {k: {kk: round(vv, 6) for kk, vv in v.items()} for k, v in results.items()},
            fh,
            indent=2,
        )
    print(f"  Calibration results written to {out_path}")


if __name__ == "__main__":
    main()
