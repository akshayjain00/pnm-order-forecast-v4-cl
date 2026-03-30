"""Tests for config module."""
import datetime

from src.config import (
    HOLIDAYS,
    PARAM_RANGES,
    PARAMS,
    SPECIAL_DATES,
    is_peak_date,
    is_peak_date_broad,
)


def test_all_tunable_params_have_ranges() -> None:
    """Every tunable param must have a corresponding range."""
    tunable = [
        "ten_week_weight",
        "horizon_weight_T0",
        "horizon_weight_T1",
        "horizon_weight_T2",
        "opp_volume_lower_pct",
        "opp_volume_upper_pct",
        "optimization_lambda",
        "min_pipeline_opps",
    ]
    for p in tunable:
        assert p in PARAM_RANGES, f"Missing range: {p}"
        lo, hi = PARAM_RANGES[p]
        assert lo < hi, f"Invalid range for {p}: [{lo}, {hi}]"
        val = PARAMS[p]
        assert (
            lo <= val <= hi
        ), f"Default {p}={val} outside [{lo}, {hi}]"


def test_fixed_params_exist() -> None:
    """Fixed structural params must be present."""
    fixed = [
        "bucket_boundaries",
        "conversion_lookback_weeks",
        "recency_decay_fn",
        "run_cadence_hours",
        "backtest_hour_step",
    ]
    for p in fixed:
        assert p in PARAMS, f"Missing fixed param: {p}"


def test_conservative_nonpeak_param() -> None:
    """Conservative non-peak mode must be enabled by default."""
    assert PARAMS["conservative_nonpeak"] is True


def test_bucket_boundaries_sorted() -> None:
    assert PARAMS["bucket_boundaries"] == sorted(
        PARAMS["bucket_boundaries"]
    )


def test_peak_date_month_end() -> None:
    """Last 2 days of month are always peak (narrow definition)."""
    assert is_peak_date(datetime.date(2026, 3, 31)) is True
    assert is_peak_date(datetime.date(2026, 3, 30)) is True
    # Mid-month dates are NOT peak in narrow definition
    assert is_peak_date(datetime.date(2026, 3, 15)) is False


def test_peak_date_broad_holiday() -> None:
    """Broad peak definition still flags holidays."""
    if HOLIDAYS:
        assert is_peak_date_broad(HOLIDAYS[0]) is True


def test_peak_date_broad_adjacency() -> None:
    """Broad definition: ±1 day holiday adjacency."""
    # Republic Day is Jan 26 → Jan 25 and Jan 27 should be peak
    assert is_peak_date_broad(datetime.date(2026, 1, 25)) is True
    assert is_peak_date_broad(datetime.date(2026, 1, 27)) is True


def test_peak_date_normal() -> None:
    """A mid-month weekday is not peak in narrow definition."""
    assert is_peak_date(datetime.date(2026, 3, 18)) is False


def test_special_dates_not_empty() -> None:
    """SPECIAL_DATES from V1 SQL must be populated."""
    assert len(SPECIAL_DATES) > 50


def test_narrow_peak_is_subset_of_broad() -> None:
    """Narrow peak dates are always a subset of broad."""
    test_dates = [
        datetime.date(2026, m, d)
        for m in range(1, 8)
        for d in range(1, 29)
    ]
    for d in test_dates:
        if is_peak_date(d):
            assert is_peak_date_broad(d), (
                f"{d} is narrow-peak but not broad-peak"
            )
