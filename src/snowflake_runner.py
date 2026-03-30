"""Thin wrapper to execute SQL against Snowflake and return DataFrames.

Expects environment variables:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""
import os
from pathlib import Path
from typing import Any

import pandas as pd
import snowflake.connector


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection from environment variables."""
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "prod_curated"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "pnm_application"),
    )


def run_sql_file(
    sql_path: str | Path,
    params: dict[str, Any] | None = None,
    conn: snowflake.connector.SnowflakeConnection | None = None,
) -> pd.DataFrame:
    """Execute a SQL file and return results as a DataFrame.

    Args:
        sql_path: Path to the .sql file.
        params: Named parameters to bind.
        conn: Optional existing connection. If None, creates one.
    """
    sql_text = Path(sql_path).read_text()
    close_conn = conn is None
    if conn is None:
        conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql_text, params or {})
        columns = (
            [desc[0].lower() for desc in cur.description]
            if cur.description
            else []
        )
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        if close_conn:
            conn.close()
