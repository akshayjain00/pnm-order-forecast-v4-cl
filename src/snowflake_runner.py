"""Thin wrapper to execute SQL against Snowflake and return DataFrames.

Supports two auth modes (selected automatically by env vars):
  1. Key-pair: SNOWFLAKE_PRIVATE_KEY + SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
  2. Password: SNOWFLAKE_PASSWORD (fallback)

Common env vars:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
    SNOWFLAKE_ROLE (optional)
"""
import os
from pathlib import Path
from typing import Any

import pandas as pd
import snowflake.connector

from src.runtime_logging import emit_runtime_log


def _load_private_key() -> bytes:
    """Load and decrypt the RSA private key from env var."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    key_pem = os.environ["SNOWFLAKE_PRIVATE_KEY"]
    passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    p_key = serialization.load_pem_private_key(
        key_pem.encode(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection from environment variables."""
    connect_args: dict[str, Any] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.environ.get("SNOWFLAKE_DATABASE", "prod_curated"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "pnm_application"),
    }

    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        connect_args["role"] = role

    if os.environ.get("SNOWFLAKE_PRIVATE_KEY"):
        connect_args["private_key"] = _load_private_key()
    else:
        connect_args["password"] = os.environ["SNOWFLAKE_PASSWORD"]

    return snowflake.connector.connect(**connect_args)


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
    resolved_sql_path = Path(sql_path)
    sql_text = resolved_sql_path.read_text()
    close_conn = conn is None
    stage = "connect" if conn is None else "execute"
    try:
        if conn is None:
            emit_runtime_log(
                event="snowflake_query",
                stage="connect",
                status="start",
                sql_path=str(resolved_sql_path),
            )
            conn = get_connection()
            emit_runtime_log(
                event="snowflake_query",
                stage="connect",
                status="success",
                sql_path=str(resolved_sql_path),
            )
        cur = conn.cursor()
        stage = "execute"
        emit_runtime_log(
            event="snowflake_query",
            stage="execute",
            status="start",
            sql_path=str(resolved_sql_path),
            param_keys=sorted((params or {}).keys()),
        )
        cur.execute(sql_text, params or {})
        columns = (
            [desc[0].lower() for desc in cur.description]
            if cur.description
            else []
        )
        rows = cur.fetchall()
        emit_runtime_log(
            event="snowflake_query",
            stage="execute",
            status="success",
            sql_path=str(resolved_sql_path),
            row_count=len(rows),
            param_keys=sorted((params or {}).keys()),
        )
        return pd.DataFrame(rows, columns=columns)
    except Exception as exc:
        emit_runtime_log(
            event="snowflake_query",
            stage=stage,
            status="failure",
            sql_path=str(resolved_sql_path),
            param_keys=sorted((params or {}).keys()),
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        raise
    finally:
        if close_conn:
            conn.close()
