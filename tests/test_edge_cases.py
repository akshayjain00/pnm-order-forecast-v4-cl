"""Regression tests for known edge cases. Spec Section 9.2."""
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_range,
)

DEFAULT_PARAMS = {
    "horizon_weight_T0": 0.85,
    "horizon_weight_T1": 0.65,
    "horizon_weight_T2": 0.45,
    "min_pipeline_opps": 5,
    "peak_multiplier_cap": 1.25,
}


def test_no_opps_for_target_date() -> None:
    """Zero pipeline opps -> fallback to seasonal."""
    result = compute_point_estimate(
        floor=0, pipeline_estimate=0.0, seasonal_baseline=250.0,
        horizon=0, pipeline_opp_count=0, is_peak=False,
        peak_multiplier=1.0, params=DEFAULT_PARAMS,
    )
    # hw fallback = 0.15, blend = 0.15*0 + 0.85*250 = 212.5
    assert abs(result - 212.5) < 0.01


def test_all_opps_already_converted() -> None:
    """All opps converted -> open_opps=0, pipeline_est=0, floor is high."""
    result = compute_point_estimate(
        floor=280, pipeline_estimate=0.0, seasonal_baseline=260.0,
        horizon=0, pipeline_opp_count=50, is_peak=False,
        peak_multiplier=1.0, params=DEFAULT_PARAMS,
    )
    # pipeline_total = 280, blend = 0.85*280 + 0.15*260 = 277.0
    # max(277.0, 280) = 280.0 (floor wins)
    assert abs(result - 280.0) < 0.01


def test_weekend_low_volume() -> None:
    """Model handles low seasonal without breaking."""
    result = compute_point_estimate(
        floor=30, pipeline_estimate=15.0, seasonal_baseline=80.0,
        horizon=2, pipeline_opp_count=20, is_peak=False,
        peak_multiplier=1.0, params=DEFAULT_PARAMS,
    )
    assert result > 0
    assert result >= 30  # floor


def test_month_end_peak() -> None:
    """Peak multiplier applied correctly on month-end."""
    result = compute_point_estimate(
        floor=200, pipeline_estimate=50.0, seasonal_baseline=250.0,
        horizon=0, pipeline_opp_count=100, is_peak=True,
        peak_multiplier=1.25, params=DEFAULT_PARAMS,
    )
    assert result > 250  # multiplier must have effect


def test_range_never_below_floor() -> None:
    """Even with large negative error percentile, lower >= floor."""
    lower, _upper = compute_range(
        point_estimate=100.0, floor=95,
        p_lower=-0.20, p_upper=0.10,
    )
    assert lower >= 95


def test_pipeline_estimate_empty_buckets() -> None:
    """All buckets empty -> pipeline estimate is 0."""
    result = compute_pipeline_estimate(
        [0, 0, 0, 0], [0.70, 0.45, 0.30, 0.15],
    )
    assert result == 0.0


def test_pipeline_estimate_single_bucket() -> None:
    """Only one bucket has opps."""
    result = compute_pipeline_estimate(
        [50, 0, 0, 0], [0.70, 0.45, 0.30, 0.15],
    )
    assert abs(result - 35.0) < 0.01
