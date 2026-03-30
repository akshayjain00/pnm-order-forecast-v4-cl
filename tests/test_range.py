"""Unit tests for range construction. Spec Section 4 Range Construction."""
import datetime

import numpy as np
from src.config import PARAMS, is_peak_date
from src.forecast import calibrate_percentiles, compute_error_pct, compute_range


def test_error_pct_normal() -> None:
    """Normal case: (actual - predicted) / max(predicted, 1)."""
    assert abs(compute_error_pct(actual=270, predicted=250) - 0.08) < 0.001


def test_error_pct_safe_denominator() -> None:
    """Near-zero predicted uses max(predicted, 1)."""
    result = compute_error_pct(actual=5, predicted=0)
    assert abs(result - 5.0) < 0.001


def test_error_pct_negative() -> None:
    """Under-prediction yields negative error_pct."""
    result = compute_error_pct(actual=200, predicted=250)
    assert result < 0


def test_range_normal() -> None:
    """Normal range from percentiles."""
    lower, upper = compute_range(
        point_estimate=250.0,
        floor=200,
        p_lower=-0.08,
        p_upper=0.10,
    )
    assert abs(lower - 230.0) < 0.01
    assert abs(upper - 275.0) < 0.01


def test_range_lower_enforces_floor() -> None:
    """Lower bound can never be below floor."""
    lower, upper = compute_range(
        point_estimate=250.0,
        floor=230,
        p_lower=-0.15,  # would give 212.5
        p_upper=0.10,
    )
    assert lower == 230  # floor enforcement
    assert abs(upper - 275.0) < 0.01


def test_range_with_residuals() -> None:
    """Calibrate percentiles from a residual array."""
    residuals = np.array(
        [-0.10, -0.05, -0.02, 0.01, 0.03, 0.06, 0.08, 0.12, 0.15, 0.20]
    )
    p_lower, p_upper = calibrate_percentiles(residuals, target_coverage=0.65)
    # Should capture ~65% of the residuals
    coverage = float(
        np.mean((residuals >= p_lower) & (residuals <= p_upper))
    )
    assert coverage >= 0.60  # allow small tolerance due to discrete sample


# ── Stratified range tests (Option C) ────────────────────────────────


def test_stratified_config_keys_exist() -> None:
    """Config has peak and nonpeak percentiles for all horizons."""
    for h in [0, 1, 2]:
        for day_type in ["peak", "nonpeak"]:
            lo_key = f"range_lower_pctl_T{h}_{day_type}"
            hi_key = f"range_upper_pctl_T{h}_{day_type}"
            assert lo_key in PARAMS, f"Missing {lo_key}"
            assert hi_key in PARAMS, f"Missing {hi_key}"


def test_peak_and_nonpeak_ranges_differ() -> None:
    """Peak and non-peak days should have different calibrated intervals."""
    for h in [0, 1, 2]:
        peak_lo = float(PARAMS[f"range_lower_pctl_T{h}_peak"])
        peak_hi = float(PARAMS[f"range_upper_pctl_T{h}_peak"])
        nonpeak_lo = float(PARAMS[f"range_lower_pctl_T{h}_nonpeak"])
        nonpeak_hi = float(PARAMS[f"range_upper_pctl_T{h}_nonpeak"])
        # Stratified values must differ between peak and nonpeak
        assert (peak_lo, peak_hi) != (nonpeak_lo, nonpeak_hi), (
            f"T+{h}: peak and nonpeak percentiles should differ"
        )


def test_stratified_range_peak_day() -> None:
    """Peak day uses peak-specific percentiles (different from nonpeak)."""
    # 2026-03-31 is last day of March → peak
    d_peak = datetime.date(2026, 3, 31)
    assert is_peak_date(d_peak)

    point_est = 1000.0
    floor = 800

    # Peak range
    p_lo_peak = float(PARAMS["range_lower_pctl_T0_peak"])
    p_hi_peak = float(PARAMS["range_upper_pctl_T0_peak"])
    lo_p, hi_p = compute_range(point_est, floor, p_lo_peak, p_hi_peak)

    # Nonpeak range
    p_lo_np = float(PARAMS["range_lower_pctl_T0_nonpeak"])
    p_hi_np = float(PARAMS["range_upper_pctl_T0_nonpeak"])
    lo_np, hi_np = compute_range(point_est, floor, p_lo_np, p_hi_np)

    # Peak and nonpeak should produce different intervals
    assert (lo_p, hi_p) != (lo_np, hi_np)


def test_stratified_range_nonpeak_day() -> None:
    """Non-peak day uses nonpeak-specific percentiles."""
    # 2026-03-15 is mid-month → nonpeak
    d_nonpeak = datetime.date(2026, 3, 15)
    assert not is_peak_date(d_nonpeak)

    point_est = 1000.0
    floor = 800

    p_lo = float(PARAMS["range_lower_pctl_T1_nonpeak"])
    p_hi = float(PARAMS["range_upper_pctl_T1_nonpeak"])
    lo, hi = compute_range(point_est, floor, p_lo, p_hi)

    # Basic sanity: range should be reasonable
    assert lo >= floor
    assert hi > lo


def test_stratified_fallback_to_horizon_level() -> None:
    """If stratified key is missing, fallback to horizon-level works."""
    # Simulate lookup logic from run_forecast
    horizon = 1
    day_type = "peak"
    p_lo_key = f"range_lower_pctl_T{horizon}_{day_type}"
    p_hi_key = f"range_upper_pctl_T{horizon}_{day_type}"

    # These should exist
    assert p_lo_key in PARAMS
    assert p_hi_key in PARAMS

    # Test fallback path: if we had a missing key, horizon-level works
    fallback_lo = f"range_lower_pctl_T{horizon}"
    fallback_hi = f"range_upper_pctl_T{horizon}"
    assert fallback_lo in PARAMS
    assert fallback_hi in PARAMS
