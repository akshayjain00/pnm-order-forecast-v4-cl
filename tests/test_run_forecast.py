"""Tests for the hourly forecast runner logging contract."""
import json

import pytest

from src import run_forecast


class _DummyConnection:
    def close(self) -> None:
        return None


def test_main_logs_structured_failure_and_reraises(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Runner failures should emit structured JSON before propagating."""
    monkeypatch.setattr(run_forecast, "get_connection", lambda: _DummyConnection())

    def _boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("query failed")

    monkeypatch.setattr(run_forecast, "run_sql_file", _boom)

    with pytest.raises(RuntimeError, match="query failed"):
        run_forecast.main()

    lines = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.strip()
    ]
    assert lines
    assert lines[-1]["event"] == "pnm_forecast_job"
    assert lines[-1]["status"] == "failure"
    assert lines[-1]["error_type"] == "RuntimeError"
    assert lines[-1]["error_message"] == "query failed"
