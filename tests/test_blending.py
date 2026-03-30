"""Unit tests for blending logic. Spec Section 4."""
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_seasonal_baseline,
)


def test_pipeline_estimate_basic() -> None:
    """Pipeline estimate = sum of (bucket_opps * bucket_conv_rate)."""
    bucket_opps = [30, 45, 20, 10]
    bucket_convs = [0.70, 0.45, 0.30, 0.15]
    result = compute_pipeline_estimate(bucket_opps, bucket_convs)
    # 30*0.70 + 45*0.45 + 20*0.30 + 10*0.15 = 21 + 20.25 + 6 + 1.5
    assert abs(result - 48.75) < 0.01


def test_point_estimate_basic_t0() -> None:
    """Basic blending at horizon 0 without peak or fallback."""
    result = compute_point_estimate(
        floor=200,
        pipeline_estimate=50.0,
        seasonal_baseline=270.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        peak_multiplier=1.0,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # pipeline_total = max(200, 50) = 200, blend = 0.85*200 + 0.15*270 = 210.5
    assert abs(result - 210.5) < 0.01


def test_floor_enforcement() -> None:
    """Estimate can never be below floor."""
    result = compute_point_estimate(
        floor=200,
        pipeline_estimate=10.0,
        seasonal_baseline=150.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        peak_multiplier=1.0,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # pipeline_total = max(200, 10) = 200, blend = 0.85*200 + 0.15*150 = 192.5
    # Floor enforcement: max(192.5, 200) = 200
    assert abs(result - 200.0) < 0.01


def test_floor_enforcement_below() -> None:
    """When seasonal is very low, floor dominates."""
    result = compute_point_estimate(
        floor=200,
        pipeline_estimate=0.0,
        seasonal_baseline=100.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        peak_multiplier=1.0,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # blend = 0.85*200 + 0.15*100 = 185.0, max(185, 200) = 200
    assert abs(result - 200.0) < 0.01


def test_empty_pipeline_fallback() -> None:
    """When pipeline is sparse, weight shifts to seasonal."""
    result = compute_point_estimate(
        floor=5,
        pipeline_estimate=0.0,
        seasonal_baseline=250.0,
        horizon=0,
        pipeline_opp_count=2,  # below min_pipeline_opps=5
        is_peak=False,
        peak_multiplier=1.0,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # Fallback hw = 0.15, blend = 0.15*5 + 0.85*250 = 213.25
    assert abs(result - 213.25) < 0.01


def test_seasonal_baseline_blend() -> None:
    """Seasonal = w * ten_week + (1-w) * twelve_month."""
    result = compute_seasonal_baseline(
        ten_week_avg=260.0,
        twelve_month_avg=240.0,
        ten_week_weight=0.6,
    )
    assert abs(result - 252.0) < 0.01


def test_seasonal_baseline_extreme_weight() -> None:
    """Weight=0.8 (near-max) should heavily favor ten_week."""
    result = compute_seasonal_baseline(
        ten_week_avg=300.0,
        twelve_month_avg=200.0,
        ten_week_weight=0.8,
    )
    assert abs(result - 280.0) < 0.01


def test_peak_multiplier() -> None:
    """Peak multiplier applied after blend, floor still enforced."""
    result = compute_point_estimate(
        floor=200,
        pipeline_estimate=50.0,
        seasonal_baseline=270.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=True,
        peak_multiplier=1.3,
        params={
            "horizon_weight_T0": 0.85,
            "min_pipeline_opps": 5,
            "peak_multiplier_cap": 1.3,
        },
    )
    # pipeline_total = max(200, 50) = 200, blend = 0.85*200 + 0.15*270 = 210.5
    # peak: 210.5 * 1.3 = 273.65
    assert abs(result - 273.65) < 0.01


def test_conservative_nonpeak_uses_min() -> None:
    """Conservative mode uses min(pipeline_total, seasonal) on non-peak."""
    result = compute_point_estimate(
        floor=100,
        pipeline_estimate=500.0,
        seasonal_baseline=300.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        peak_multiplier=1.0,
        params={
            "horizon_weight_T0": 0.85,
            "min_pipeline_opps": 5,
            "conservative_nonpeak": True,
        },
    )
    # pipeline_total = max(100, 500) = 500
    # conservative non-peak: min(500, 300) = 300
    assert abs(result - 300.0) < 0.01


def test_conservative_peak_still_blends() -> None:
    """Conservative mode still uses weighted blend on peak days."""
    result = compute_point_estimate(
        floor=100,
        pipeline_estimate=500.0,
        seasonal_baseline=300.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=True,
        peak_multiplier=1.2,
        params={
            "horizon_weight_T0": 0.85,
            "min_pipeline_opps": 5,
            "conservative_nonpeak": True,
            "peak_multiplier_cap": 1.2,
        },
    )
    # Even with conservative mode, peak days use standard blend:
    # pipeline_total = max(100, 500) = 500
    # blend = 0.85*500 + 0.15*300 = 470
    # peak: 470 * 1.2 = 564.0
    assert abs(result - 564.0) < 0.01


def test_conservative_floor_enforcement() -> None:
    """Conservative mode still enforces floor."""
    result = compute_point_estimate(
        floor=400,
        pipeline_estimate=100.0,
        seasonal_baseline=200.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        peak_multiplier=1.0,
        params={
            "horizon_weight_T0": 0.85,
            "min_pipeline_opps": 5,
            "conservative_nonpeak": True,
        },
    )
    # pipeline_total = max(400, 100) = 400
    # conservative: min(400, 200) = 200
    # floor enforcement: max(200, 400) = 400
    assert abs(result - 400.0) < 0.01
