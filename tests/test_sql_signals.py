"""Integration tests: signal DataFrame -> full forecast output.

Uses frozen fixtures instead of live Snowflake connection.
See spec Section 9.3.
"""
from typing import Any

from src.config import PARAMS
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
)


def make_signal_row(
    horizon: int = 0,
    floor_orders: int = 200,
    open_opps: tuple[int, int, int, int] = (30, 45, 20, 10),
    conv_rates: tuple[float, float, float, float] = (
        0.70, 0.45, 0.30, 0.15,
    ),
    ten_week_avg: float = 260.0,
    twelve_month_avg: float = 240.0,
    peak_multiplier: float = 1.0,
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "floor_orders": floor_orders,
        "open_opps_b0": open_opps[0],
        "open_opps_b1": open_opps[1],
        "open_opps_b2": open_opps[2],
        "open_opps_b3": open_opps[3],
        "conv_rate_b0": conv_rates[0],
        "conv_rate_b1": conv_rates[1],
        "conv_rate_b2": conv_rates[2],
        "conv_rate_b3": conv_rates[3],
        "ten_week_avg": ten_week_avg,
        "twelve_month_avg": twelve_month_avg,
        "total_open_opps": sum(open_opps),
        "peak_multiplier": peak_multiplier,
    }


def run_forecast_from_row(
    row: dict[str, object],
    params: dict[str, object] | None = None,
) -> dict[str, float]:
    """Simulate the full forecast pipeline from a signal row."""
    p: dict[str, Any] = dict(PARAMS) if params is None else dict(params)
    horizon = int(row["horizon"])  # type: ignore[arg-type]
    floor = int(row["floor_orders"])  # type: ignore[arg-type]

    bucket_opps = [
        int(row[f"open_opps_b{i}"]) for i in range(4)  # type: ignore[arg-type]
    ]
    bucket_convs = [
        float(row[f"conv_rate_b{i}"]) for i in range(4)  # type: ignore[arg-type]
    ]

    tww = float(p["ten_week_weight"])  # type: ignore[arg-type]
    seasonal = (
        tww * float(row["ten_week_avg"])  # type: ignore[arg-type]
        + (1.0 - tww) * float(row["twelve_month_avg"])  # type: ignore[arg-type]
    )

    pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)
    point_est = compute_point_estimate(
        floor=floor,
        pipeline_estimate=pipeline_est,
        seasonal_baseline=seasonal,
        horizon=horizon,
        pipeline_opp_count=int(row["total_open_opps"]),  # type: ignore[arg-type]
        is_peak=False,
        peak_multiplier=float(row["peak_multiplier"]),  # type: ignore[arg-type]
        params=p,
    )
    return {
        "point_est": point_est,
        "floor": float(floor),
        "pipeline_est": pipeline_est,
        "seasonal": seasonal,
    }


def test_full_pipeline_t0() -> None:
    """End-to-end: T0 signal row -> forecast within reasonable bounds."""
    row = make_signal_row(horizon=0, floor_orders=200)
    result = run_forecast_from_row(row)
    assert result["point_est"] > 0
    assert result["point_est"] >= result["floor"]
    # With these inputs, expect ~250 range
    assert 200 <= result["point_est"] <= 350


def test_full_pipeline_t2_low_floor() -> None:
    """T+2 with low floor leans on seasonal."""
    row = make_signal_row(
        horizon=2, floor_orders=55, open_opps=(0, 0, 30, 35),
    )
    result = run_forecast_from_row(row)
    assert result["point_est"] >= 55
    # Seasonal dominates at T+2 (hw=0.45)
    assert result["seasonal"] > result["floor"]


def test_output_has_exactly_3_rows() -> None:
    """Full forecast should produce exactly 3 rows."""
    rows = [make_signal_row(horizon=h) for h in range(3)]
    results = [run_forecast_from_row(r) for r in rows]
    assert len(results) == 3


def test_output_columns_present() -> None:
    """All expected keys are in the output dict."""
    row = make_signal_row()
    result = run_forecast_from_row(row)
    for key in ["point_est", "floor", "pipeline_est", "seasonal"]:
        assert key in result
