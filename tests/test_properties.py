"""Property-based tests for the hybrid blending engine.

Uses hypothesis to verify mathematical invariants hold across all valid inputs.
Run: pytest tests/test_properties.py -v (no external deps, ~5 sec)
"""
from hypothesis import assume, given, settings
from hypothesis import strategies as st
from src.config import HybridParams
from src.forecast import compute_hybrid_estimate, compute_hybrid_range

# Strategy for valid forecast inputs
valid_floor = st.integers(min_value=0, max_value=10000)
_floats = dict(allow_nan=False, allow_infinity=False)
valid_pipeline = st.floats(min_value=0.0, max_value=50000.0, **_floats)
valid_seasonal = st.floats(min_value=0.0, max_value=50000.0, **_floats)
valid_horizon = st.integers(min_value=0, max_value=2)
valid_opp_count = st.integers(min_value=0, max_value=10000)
valid_booked_share = st.floats(min_value=0.0, max_value=2.0, **_floats)  # includes edge cases


@given(
    floor=valid_floor,
    pipeline=valid_pipeline,
    seasonal=valid_seasonal,
    horizon=valid_horizon,
    opp_count=valid_opp_count,
)
@settings(max_examples=200)
def test_floor_is_lower_bound(
    floor: int,
    pipeline: float,
    seasonal: float,
    horizon: int,
    opp_count: int,
) -> None:
    """∀ valid inputs: forecast >= floor."""
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=floor,
        pipeline_estimate=pipeline,
        seasonal_baseline=seasonal,
        horizon=horizon,
        pipeline_opp_count=opp_count,
        is_peak=False,
        booked_orders=None,
        booked_share=None,
        params=params,
    )
    assert result >= float(floor)


@given(
    floor=valid_floor,
    booked_share=st.floats(min_value=0.01, max_value=1.0, allow_nan=False, allow_infinity=False),
)
@settings(max_examples=200)
def test_nowcast_monotonic_in_floor(floor: int, booked_share: float) -> None:
    """floor↑ ⟹ nowcast = floor/share↑ (for fixed booked_share)."""
    params = HybridParams()
    floor2 = floor + 100
    r1 = compute_hybrid_estimate(
        floor=floor, pipeline_estimate=0.0, seasonal_baseline=0.0,
        horizon=0, pipeline_opp_count=0, is_peak=False,
        booked_orders=floor, booked_share=booked_share, params=params,
    )
    r2 = compute_hybrid_estimate(
        floor=floor2, pipeline_estimate=0.0, seasonal_baseline=0.0,
        horizon=0, pipeline_opp_count=0, is_peak=False,
        booked_orders=floor2, booked_share=booked_share, params=params,
    )
    assert r2 >= r1


@given(
    pipeline=valid_pipeline,
    seasonal=valid_seasonal,
    horizon=valid_horizon,
    opp_count=valid_opp_count,
)
@settings(max_examples=200)
def test_pipeline_estimate_non_negative(
    pipeline: float,
    seasonal: float,
    horizon: int,
    opp_count: int,
) -> None:
    """∀ opps>=0, convs>=0: pipeline_estimate >= 0."""
    # This tests compute_hybrid_estimate with non-negative inputs
    assume(pipeline >= 0 and seasonal >= 0)
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=0, pipeline_estimate=pipeline, seasonal_baseline=seasonal,
        horizon=horizon, pipeline_opp_count=opp_count, is_peak=False,
        booked_orders=None, booked_share=None, params=params,
    )
    assert result >= 0.0


@given(
    floor=valid_floor,
    pipeline=valid_pipeline,
    seasonal=valid_seasonal,
    horizon=valid_horizon,
    opp_count=valid_opp_count,
)
@settings(max_examples=200)
def test_forecast_finite(
    floor: int,
    pipeline: float,
    seasonal: float,
    horizon: int,
    opp_count: int,
) -> None:
    """∀ valid inputs: result is finite (no inf/nan)."""
    import math
    params = HybridParams()
    result = compute_hybrid_estimate(
        floor=floor, pipeline_estimate=pipeline, seasonal_baseline=seasonal,
        horizon=horizon, pipeline_opp_count=opp_count, is_peak=False,
        booked_orders=None, booked_share=None, params=params,
    )
    assert math.isfinite(result)


@given(
    point_estimate=st.floats(min_value=1.0, max_value=10000.0, **_floats),
    floor=valid_floor,
    horizon=valid_horizon,
    is_peak=st.booleans(),
)
@settings(max_examples=200)
def test_range_upper_gte_lower(
    point_estimate: float,
    floor: int,
    horizon: int,
    is_peak: bool,
) -> None:
    """∀ inputs: upper >= lower."""
    params = HybridParams()
    lower, upper = compute_hybrid_range(point_estimate, floor, horizon, is_peak, params)
    assert upper >= lower


@given(
    floor=valid_floor,
    point_estimate=st.floats(min_value=1.0, max_value=10000.0, **_floats),
    horizon=valid_horizon,
    is_peak=st.booleans(),
)
@settings(max_examples=200)
def test_range_lower_gte_floor(
    floor: int,
    point_estimate: float,
    horizon: int,
    is_peak: bool,
) -> None:
    """∀ inputs: range lower >= floor."""
    params = HybridParams()
    lower, _ = compute_hybrid_range(point_estimate, floor, horizon, is_peak, params)
    assert lower >= float(floor)
