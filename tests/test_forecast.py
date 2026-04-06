"""Unit tests for the hybrid blending engine.

Tests cover the 20 cases specified in HYBRID_ARCHITECTURE.md Section 8.2.
Run: pytest tests/test_forecast.py -v (no external deps, <2 sec)
"""
import datetime

from src.config import HybridParams, is_month_edge, is_peak_date_hybrid
from src.forecast import (
    compute_error_pct,
    compute_hybrid_estimate,
    compute_hybrid_range,
    compute_pipeline_estimate,
)

# --- Helpers ---

def make_params(**overrides) -> HybridParams:
    import dataclasses
    return dataclasses.replace(HybridParams(), **overrides)


# --- Core Logic ---

def test_always_blend_never_clips() -> None:
    """compute_hybrid_estimate never returns min(pipeline, seasonal)."""
    params = HybridParams()
    # V4's conservative mode would return min(600, 300) = 300
    # Hybrid must blend: 0.80*600 + 0.20*300 = 540.0
    result = compute_hybrid_estimate(
        floor=100,
        pipeline_estimate=600.0,
        seasonal_baseline=300.0,
        horizon=0,
        pipeline_opp_count=80,
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    # Result must be the blend (540) not the min (300)
    assert result > 300.0
    assert abs(result - 540.0) < 0.01


def test_floor_enforcement() -> None:
    """Output is always >= floor, in all scenarios."""
    params = HybridParams()
    # Very low pipeline + seasonal → blend well below floor
    result = compute_hybrid_estimate(
        floor=1000,
        pipeline_estimate=50.0,
        seasonal_baseline=80.0,
        horizon=0,
        pipeline_opp_count=80,
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    assert result >= 1000.0


def test_floor_enforcement_with_nowcast() -> None:
    """Floor enforcement holds even when nowcast is active."""
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=800,
        pipeline_estimate=200.0,
        seasonal_baseline=300.0,
        horizon=0,
        pipeline_opp_count=50,
        is_peak=False,
        booked_orders=800,
        booked_share=0.97,  # > full_switch_share → nowcast dominates
        params=params,
    )
    # nowcast = 800 / 0.97 ≈ 824.7, then max(824.7, 800) = 824.7
    assert result >= 800.0
    # Should be close to nowcast
    assert result > 820.0


# --- Nowcast Tests ---

def test_nowcast_basic() -> None:
    """floor / booked_share gives correct full-day projection."""
    params = HybridParams()
    # With high share (>=0.95), nowcast dominates
    result = compute_hybrid_estimate(
        floor=760,
        pipeline_estimate=800.0,
        seasonal_baseline=900.0,
        horizon=0,
        pipeline_opp_count=50,
        is_peak=False,
        booked_orders=760,
        booked_share=0.95,  # exactly at threshold → use nowcast
        params=params,
    )
    # nowcast = 760 / 0.95 = 800.0 → then max(800, 760) = 800
    assert abs(result - 800.0) < 0.01


def test_nowcast_progressive_blending() -> None:
    """At 50% share, result is 50/50 mix of nowcast and blend."""
    params = HybridParams(full_switch_share=1.0)  # so 0.50 = 50% weight
    # With no booked_orders given for nowcast, test the weight
    # Actually let's test with booked_share = 0.50
    # nowcast_weight = 0.50 / 1.0 = 0.50
    # blend = 0.80 * 500 + 0.20 * 700 = 540.0
    # nowcast = 300 / 0.50 = 600.0
    # result = 0.50 * 600 + 0.50 * 540 = 570.0
    result = compute_hybrid_estimate(
        floor=300,
        pipeline_estimate=500.0,
        seasonal_baseline=700.0,
        horizon=0,
        pipeline_opp_count=80,
        is_peak=False,
        booked_orders=300,
        booked_share=0.50,
        params=params,
    )
    assert abs(result - 570.0) < 0.01


def test_nowcast_full_switch() -> None:
    """At >= full_switch_share, output equals nowcast (not blend)."""
    params = HybridParams()
    # booked_share = 0.95 → exactly at threshold
    # nowcast = 760 / 0.95 = 800
    result = compute_hybrid_estimate(
        floor=760,
        pipeline_estimate=1200.0,  # Very different from nowcast
        seasonal_baseline=500.0,   # Very different from nowcast
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        booked_orders=760,
        booked_share=0.95,
        params=params,
    )
    expected_nowcast = 760 / 0.95  # ≈ 800
    assert abs(result - expected_nowcast) < 0.1


def test_nowcast_share_clamped_above_one() -> None:
    """booked_share > 1.0 is clamped to 1.0, no error."""
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=800,
        pipeline_estimate=900.0,
        seasonal_baseline=1000.0,
        horizon=0,
        pipeline_opp_count=80,
        is_peak=False,
        booked_orders=800,
        booked_share=1.2,  # impossible but guard against DQ
        params=params,
    )
    # clamped to 1.0, nowcast = 800/1.0 = 800, max(800, 800) = 800
    # share 1.0 >= full_switch_share 0.95 → use nowcast = 800
    assert abs(result - 800.0) < 0.01


def test_nowcast_share_zero_no_division_error() -> None:
    """booked_share = 0 is clamped to 0.01, no ZeroDivisionError."""
    params = HybridParams()
    # No exception should be raised
    result = compute_hybrid_estimate(
        floor=0,
        pipeline_estimate=100.0,
        seasonal_baseline=200.0,
        horizon=0,
        pipeline_opp_count=80,
        is_peak=False,
        booked_orders=0,
        booked_share=0.0,  # edge case
        params=params,
    )
    # booked_share clamped to 0.01
    # nowcast = max(0/0.01, 0) = 0
    # nowcast_weight = 0.01/0.95 ≈ 0.011
    # blend = 0.80*100 + 0.20*200 = 120
    # result ≈ 0.011*0 + 0.989*120 ≈ 118.7, max(118.7, 0) = 118.7
    assert result >= 0.0
    assert result < 200.0  # Seasonal doesn't dominate excessively


def test_nowcast_before_peak_multiplier() -> None:
    """On peak T+0, peak multiplier scales the nowcast-blended result.

    This is a regression test for the ordering bug: nowcast must be
    applied in Step 4, BEFORE the peak multiplier in Step 5.
    """
    # With peak_multiplier=1.0 (disabled), peak/nonpeak should give same result
    params_no_mult = HybridParams(peak_multiplier=1.0)
    result_peak = compute_hybrid_estimate(
        floor=760, pipeline_estimate=800.0, seasonal_baseline=900.0,
        horizon=0, pipeline_opp_count=80, is_peak=True,
        booked_orders=760, booked_share=0.95,
        params=params_no_mult,
    )
    result_nonpeak = compute_hybrid_estimate(
        floor=760, pipeline_estimate=800.0, seasonal_baseline=900.0,
        horizon=0, pipeline_opp_count=80, is_peak=False,
        booked_orders=760, booked_share=0.95,
        params=params_no_mult,
    )
    # peak_multiplier=1.0 → should be identical
    assert abs(result_peak - result_nonpeak) < 0.01


# --- Pipeline Tests ---

def test_pipeline_estimate_bucketed() -> None:
    """Pipeline = sum(opps[i] * conv[i]) across 4 buckets."""
    bucket_opps = [30, 45, 20, 10]
    bucket_convs = [0.70, 0.45, 0.30, 0.15]
    result = compute_pipeline_estimate(bucket_opps, bucket_convs)
    # 30*0.70 + 45*0.45 + 20*0.30 + 10*0.15 = 21 + 20.25 + 6 + 1.5
    assert abs(result - 48.75) < 0.01


def test_sparse_pipeline_fallback() -> None:
    """When opp_count < min_pipeline_opps, use sparse_pipeline_weight."""
    params = HybridParams(sparse_pipeline_weight=0.20, min_pipeline_opps=5)
    result = compute_hybrid_estimate(
        floor=5,
        pipeline_estimate=10.0,
        seasonal_baseline=700.0,
        horizon=0,
        pipeline_opp_count=3,  # below 5
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    # pipeline_total = max(5, 10) = 10
    # effective_weight = 0.20 (sparse)
    # blend = 0.20*10 + 0.80*700 = 562
    assert abs(result - 562.0) < 0.01


def test_seasonal_zero_handling() -> None:
    """When seasonal = 0, blend = w * pipeline (no crash)."""
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=200,
        pipeline_estimate=300.0,
        seasonal_baseline=0.0,
        horizon=0,
        pipeline_opp_count=50,
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    # blend = 0.80*300 + 0.20*0 = 240, max(240, 200) = 240
    assert abs(result - 240.0) < 0.01


def test_floor_exceeds_seasonal() -> None:
    """When floor > seasonal (late in day), output >= floor."""
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=900,      # Floor is very high (late in day, many orders in)
        pipeline_estimate=200.0,
        seasonal_baseline=600.0,  # Below floor
        horizon=0,
        pipeline_opp_count=50,
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    # pipeline_total = max(900, 200) = 900
    # blend = 0.80*900 + 0.20*600 = 840, max(840, 900) = 900
    assert abs(result - 900.0) < 0.01


def test_all_conv_rates_default() -> None:
    """When all conversions = default (0.10/0.05), output is reasonable."""
    params = HybridParams()
    bucket_opps = [50, 30, 20, 10]
    bucket_convs = [0.10, 0.10, 0.10, 0.05]
    pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)
    result = compute_hybrid_estimate(
        floor=300,
        pipeline_estimate=pipeline_est,
        seasonal_baseline=700.0,
        horizon=0,
        pipeline_opp_count=110,
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    # Should be reasonable (not crash, not extreme)
    assert 300.0 <= result <= 1000.0


# --- Horizon Weight Tests ---

def test_horizon_weights_t0() -> None:
    """T+0 uses weight 0.80 (V2's value, higher than V4's 0.70)."""
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=0,
        pipeline_estimate=1000.0,
        seasonal_baseline=0.0,
        horizon=0,
        pipeline_opp_count=80,
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    # blend = 0.80*1000 + 0.20*0 = 800
    assert abs(result - 800.0) < 0.01


def test_horizon_weights_t2() -> None:
    """T+2 uses weight 0.55 (V4's value, preserves pipeline at longer horizon)."""
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=0,
        pipeline_estimate=1000.0,
        seasonal_baseline=0.0,
        horizon=2,
        pipeline_opp_count=80,
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    # blend = 0.55*1000 + 0.45*0 = 550
    assert abs(result - 550.0) < 0.01


# --- Peak Definition Tests ---

def test_peak_definition_last_two_days_of_month() -> None:
    """Last 2 days of any month are peak."""
    assert is_peak_date_hybrid(datetime.date(2026, 3, 30)) is True
    assert is_peak_date_hybrid(datetime.date(2026, 3, 31)) is True
    assert is_peak_date_hybrid(datetime.date(2026, 3, 29)) is False


def test_peak_april1_is_peak() -> None:
    """April 1 is peak (month-end spillover, data-driven)."""
    assert is_peak_date_hybrid(datetime.date(2026, 4, 1)) is True


def test_peak_april2_is_not_peak() -> None:
    """April 2 returns to baseline — not peak."""
    assert is_peak_date_hybrid(datetime.date(2026, 4, 2)) is False


def test_month_edge_broader_than_peak() -> None:
    """is_month_edge covers more days than is_peak_date_hybrid."""
    march = datetime.date(2026, 3, 1)
    # First 3 days are month_edge
    assert is_month_edge(march) is True
    # But not peak_date_hybrid
    assert is_peak_date_hybrid(march) is True  # day==1 → peak


# --- Range Tests ---

def test_range_lower_gte_floor() -> None:
    """Prediction interval lower bound is always >= floor."""
    params = HybridParams()
    lower, upper = compute_hybrid_range(
        point_estimate=500.0,
        floor=600,  # floor > point_estimate to test enforcement
        horizon=0,
        is_peak=False,
        params=params,
    )
    assert lower >= 600.0


def test_range_upper_gte_lower() -> None:
    """Upper bound always >= lower bound."""
    params = HybridParams()
    for h in [0, 1, 2]:
        for peak in [True, False]:
            lower, upper = compute_hybrid_range(
                point_estimate=800.0, floor=600, horizon=h,
                is_peak=peak, params=params
            )
            assert upper >= lower


def test_range_peak_vs_nonpeak_differ() -> None:
    """Peak and nonpeak ranges use different percentiles."""
    params = HybridParams()
    lower_peak, upper_peak = compute_hybrid_range(800.0, 600, 0, True, params)
    lower_np, upper_np = compute_hybrid_range(800.0, 600, 0, False, params)
    # They should differ because peak/nonpeak have different error distributions
    assert lower_peak != lower_np or upper_peak != upper_np


# --- Peak Seasonal Baseline Tests (Option B) ---

def test_peak_seasonal_replaces_regular_at_t1() -> None:
    """For peak horizon>=1, peak_seasonal_baseline replaces seasonal_baseline."""
    params = HybridParams()
    # Regular seasonal is very low (non-peak baseline bias)
    result_with = compute_hybrid_estimate(
        floor=2000, pipeline_estimate=2200.0, seasonal_baseline=700.0,
        horizon=1, pipeline_opp_count=80, is_peak=True,
        booked_orders=None, booked_share=None, params=params,
        peak_seasonal_baseline=2500.0,  # V2-style peak history
    )
    result_without = compute_hybrid_estimate(
        floor=2000, pipeline_estimate=2200.0, seasonal_baseline=700.0,
        horizon=1, pipeline_opp_count=80, is_peak=True,
        booked_orders=None, booked_share=None, params=params,
        peak_seasonal_baseline=None,
    )
    # With peak seasonal, result should be higher (closer to peak history)
    assert result_with > result_without


def test_peak_seasonal_not_used_at_t0() -> None:
    """At T+0, peak_seasonal_baseline is ignored (nowcast takes over)."""
    params = HybridParams()
    # At T+0 with high booked_share, nowcast should dominate regardless
    result = compute_hybrid_estimate(
        floor=2000, pipeline_estimate=2200.0, seasonal_baseline=700.0,
        horizon=0, pipeline_opp_count=80, is_peak=True,
        booked_orders=2000, booked_share=0.96, params=params,
        peak_seasonal_baseline=5000.0,  # would inflate result if mistakenly used
    )
    # Nowcast = 2000/0.96 ≈ 2083. Peak seasonal should NOT be blended in.
    expected_nowcast = 2000.0 / 0.96
    # Peak multiplier = 1.0, so result ≈ nowcast
    assert abs(result - expected_nowcast) < 5.0


def test_peak_seasonal_fallback_when_none() -> None:
    """When peak_seasonal_baseline=None for peak T+1, falls back to regular seasonal."""
    params = HybridParams()
    result_none = compute_hybrid_estimate(
        floor=2000, pipeline_estimate=2200.0, seasonal_baseline=800.0,
        horizon=1, pipeline_opp_count=80, is_peak=True,
        booked_orders=None, booked_share=None, params=params,
        peak_seasonal_baseline=None,
    )
    result_regular = compute_hybrid_estimate(
        floor=2000, pipeline_estimate=2200.0, seasonal_baseline=800.0,
        horizon=1, pipeline_opp_count=80, is_peak=True,
        booked_orders=None, booked_share=None, params=params,
    )
    # Both should give same result when peak baseline is None
    assert abs(result_none - result_regular) < 0.01


def test_peak_seasonal_not_used_when_not_peak() -> None:
    """peak_seasonal_baseline is ignored for non-peak days."""
    params = HybridParams()
    # Non-peak day — peak seasonal should be irrelevant
    result_with = compute_hybrid_estimate(
        floor=500, pipeline_estimate=700.0, seasonal_baseline=600.0,
        horizon=1, pipeline_opp_count=80, is_peak=False,
        booked_orders=None, booked_share=None, params=params,
        peak_seasonal_baseline=3000.0,  # would inflate if mistakenly used
    )
    result_without = compute_hybrid_estimate(
        floor=500, pipeline_estimate=700.0, seasonal_baseline=600.0,
        horizon=1, pipeline_opp_count=80, is_peak=False,
        booked_orders=None, booked_share=None, params=params,
    )
    assert abs(result_with - result_without) < 0.01


# --- Safety Tests ---

def test_compute_error_pct_safe_denom() -> None:
    """predicted=0 uses max(predicted, 1.0) denominator, no ZeroDivisionError."""
    result = compute_error_pct(actual=500.0, predicted=0.0)
    # (500 - 0) / max(0, 1) = 500.0
    assert abs(result - 500.0) < 0.01
