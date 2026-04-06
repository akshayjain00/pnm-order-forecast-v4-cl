"""Shared test fixtures for forecast tests."""
import pandas as pd
import pytest
from src.config import DEFAULT_HYBRID_PARAMS, HybridParams


@pytest.fixture
def sample_signals() -> pd.DataFrame:
    """Typical signal output from SQL for 3 horizons."""
    return pd.DataFrame([
        {
            "target_date": "2026-03-27",
            "horizon": 0,
            "floor_orders": 200,
            "open_opps_b0": 30,
            "open_opps_b1": 45,
            "open_opps_b2": 20,
            "open_opps_b3": 10,
            "conv_rate_b0": 0.70,
            "conv_rate_b1": 0.45,
            "conv_rate_b2": 0.30,
            "conv_rate_b3": 0.15,
            "ten_week_avg": 260.0,
            "twelve_month_avg": 240.0,
            "is_peak": False,
            "peak_multiplier": 1.0,
            "total_open_opps": 105,
        },
        {
            "target_date": "2026-03-28",
            "horizon": 1,
            "floor_orders": 120,
            "open_opps_b0": 0,
            "open_opps_b1": 35,
            "open_opps_b2": 40,
            "open_opps_b3": 25,
            "conv_rate_b0": 0.70,
            "conv_rate_b1": 0.45,
            "conv_rate_b2": 0.30,
            "conv_rate_b3": 0.15,
            "ten_week_avg": 255.0,
            "twelve_month_avg": 235.0,
            "is_peak": False,
            "peak_multiplier": 1.0,
            "total_open_opps": 100,
        },
        {
            "target_date": "2026-03-29",
            "horizon": 2,
            "floor_orders": 55,
            "open_opps_b0": 0,
            "open_opps_b1": 0,
            "open_opps_b2": 30,
            "open_opps_b3": 35,
            "conv_rate_b0": 0.70,
            "conv_rate_b1": 0.45,
            "conv_rate_b2": 0.30,
            "conv_rate_b3": 0.15,
            "ten_week_avg": 250.0,
            "twelve_month_avg": 230.0,
            "is_peak": False,
            "peak_multiplier": 1.0,
            "total_open_opps": 65,
        },
    ])


@pytest.fixture
def hybrid_params() -> HybridParams:
    """Default hybrid parameters for testing."""
    return DEFAULT_HYBRID_PARAMS


@pytest.fixture
def normal_day_signals() -> dict:
    """Typical non-peak Wednesday signals: floor=500, seasonal=700, pipeline=650."""
    return {
        "floor": 500,
        "pipeline_estimate": 650.0,
        "seasonal_baseline": 700.0,
        "horizon": 0,
        "pipeline_opp_count": 80,
        "is_peak": False,
        "booked_orders": 500,
        "booked_share": 0.72,
    }


@pytest.fixture
def peak_day_signals() -> dict:
    """Month-end Saturday: floor=2500, seasonal=2800, pipeline=2600, booked_share=0.87."""
    return {
        "floor": 2500,
        "pipeline_estimate": 2600.0,
        "seasonal_baseline": 2800.0,
        "horizon": 0,
        "pipeline_opp_count": 300,
        "is_peak": True,
        "booked_orders": 2500,
        "booked_share": 0.87,
    }


@pytest.fixture
def early_morning_signals() -> dict:
    """6 AM run: floor=100, booked_share=0.15 — tests low-share nowcast behavior."""
    return {
        "floor": 100,
        "pipeline_estimate": 600.0,
        "seasonal_baseline": 700.0,
        "horizon": 0,
        "pipeline_opp_count": 80,
        "is_peak": False,
        "booked_orders": 100,
        "booked_share": 0.15,
    }


@pytest.fixture
def sparse_pipeline_signals() -> dict:
    """Only 3 opps (below min_pipeline_opps=5) — tests sparse fallback."""
    return {
        "floor": 200,
        "pipeline_estimate": 2.1,
        "seasonal_baseline": 700.0,
        "horizon": 0,
        "pipeline_opp_count": 3,
        "is_peak": False,
        "booked_orders": 200,
        "booked_share": 0.28,
    }


@pytest.fixture
def all_defaults_signals() -> dict:
    """All conv rates at 0.10/0.05 defaults — tests volume matching failure."""
    bucket_opps = [50, 30, 20, 10]
    bucket_convs = [0.10, 0.10, 0.10, 0.05]
    pipeline_est = sum(o * c for o, c in zip(bucket_opps, bucket_convs))
    return {
        "floor": 300,
        "pipeline_estimate": pipeline_est,
        "seasonal_baseline": 700.0,
        "horizon": 0,
        "pipeline_opp_count": 110,
        "is_peak": False,
        "booked_orders": 300,
        "booked_share": 0.43,
    }
