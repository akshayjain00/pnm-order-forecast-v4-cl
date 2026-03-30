"""Unit tests for forecast logger."""
import datetime
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.logger import log_forecast, params_hash


def test_params_hash_deterministic() -> None:
    """Same params produce same hash."""
    p = {"a": 1, "b": 2}
    assert params_hash(p) == params_hash(p)


def test_params_hash_different() -> None:
    """Different params produce different hashes."""
    assert params_hash({"a": 1}) != params_hash({"a": 2})


def test_log_forecast_creates_file() -> None:
    """Log creates CSV with correct headers."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir) / "forecast_log.csv"
        with patch("src.logger.FORECAST_LOG", log_path):
            log_forecast(
                run_ts=datetime.datetime(2026, 3, 27, 9, 0, 0),
                target_date=datetime.date(2026, 3, 27),
                horizon=0,
                point_est=250.0,
                lower=230.0,
                upper=270.0,
                floor=200,
                params={"a": 1},
            )
        assert log_path.exists()
        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 2  # header + 1 row
        assert "run_ts" in lines[0]
