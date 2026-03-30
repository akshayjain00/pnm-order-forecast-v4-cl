"""Unit tests for Snowflake runner (mocked connection)."""
import tempfile
from unittest.mock import MagicMock

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
