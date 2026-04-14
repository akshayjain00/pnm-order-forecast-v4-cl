"""Unit tests for Snowflake runner (mocked connection)."""
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.snowflake_runner import run_sql_file


def test_run_sql_file_returns_dataframe() -> None:
    """Verify SQL file is read and executed via connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.description = [("col1",), ("col2",)]
    mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]

    with tempfile.NamedTemporaryFile(
        suffix=".sql", mode="w", delete=False,
    ) as f:
        f.write("SELECT 1 AS col1, 'a' AS col2")
        f.flush()
        result = run_sql_file(f.name, conn=mock_conn)

    assert len(result) == 2
    assert list(result.columns) == ["col1", "col2"]


def test_run_sql_file_logs_structured_failure(capsys: pytest.CaptureFixture[str]) -> None:
    """Execution failures should emit a structured JSON log before re-raising."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = RuntimeError("bad sql")

    with tempfile.NamedTemporaryFile(
        suffix=".sql", mode="w", delete=False,
    ) as f:
        f.write("SELECT bad")
        f.flush()
        with pytest.raises(RuntimeError, match="bad sql"):
            run_sql_file(f.name, conn=mock_conn)

    lines = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.strip()
    ]
    assert lines
    assert lines[-1]["event"] == "snowflake_query"
    assert lines[-1]["status"] == "failure"
    assert lines[-1]["error_type"] == "RuntimeError"
    assert lines[-1]["error_message"] == "bad sql"


def test_sql_files_use_python_connector_pyformat_placeholders() -> None:
    """The Python-executed SQL must not use unsupported :name placeholders."""
    repo_root = Path(__file__).resolve().parents[1]
    for relative_path in ("sql/base_signals.sql", "sql/forecast_snapshot.sql"):
        sql_text = (repo_root / relative_path).read_text()
        assert "%(eval_date)s" in sql_text
        assert "%(run_hour)s" in sql_text
        assert "%(backtest_mode)s" in sql_text
        assert ":eval_date" not in sql_text
        assert ":run_hour" not in sql_text
        assert ":backtest_mode" not in sql_text
        assert "INTERVAL ':run_hour hours'" not in sql_text
