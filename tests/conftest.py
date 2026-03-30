"""Shared test fixtures for forecast tests."""
import pandas as pd
import pytest


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
