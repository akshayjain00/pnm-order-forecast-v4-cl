"""Unit tests for backtest engine (no Snowflake dependency)."""
import pandas as pd
from src.backtest import compute_metrics


def test_compute_metrics_basic() -> None:
    """Metrics computed correctly from a small backtest DataFrame."""
    df = pd.DataFrame([
        {
            "horizon": 0, "actual": 250, "point_est": 240,
            "error": 10, "abs_pct_error": 0.04, "floor": 200,
            "lower": 220, "upper": 260, "in_range": True,
        },
        {
            "horizon": 0, "actual": 260, "point_est": 270,
            "error": -10, "abs_pct_error": 0.038, "floor": 210,
            "lower": 250, "upper": 290, "in_range": True,
        },
        {
            "horizon": 1, "actual": 280, "point_est": 250,
            "error": 30, "abs_pct_error": 0.107, "floor": 120,
            "lower": 230, "upper": 270, "in_range": False,
        },
    ])
    metrics = compute_metrics(df)
    assert len(metrics) == 2  # horizons 0 and 1
    h0 = metrics[metrics["horizon"] == 0].iloc[0]
    assert h0["mae"] == 10.0
    assert h0["coverage"] == 100.0  # both in range
    h1 = metrics[metrics["horizon"] == 1].iloc[0]
    assert h1["coverage"] == 0.0  # out of range


def test_compute_metrics_empty() -> None:
    """Empty horizon returns empty metrics."""
    df = pd.DataFrame(
        columns=[
            "horizon", "actual", "point_est", "error",
            "abs_pct_error", "floor", "lower", "upper", "in_range",
        ]
    )
    metrics = compute_metrics(df)
    assert len(metrics) == 0
