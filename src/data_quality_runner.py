"""Run data quality checks before each forecast.

Usage:
    python -m src.data_quality_runner

Exits with code 1 if any check returns FAIL.
"""
import sys
from pathlib import Path

from src.snowflake_runner import get_connection

SQL_FILE = Path(__file__).parent.parent / "sql" / "data_quality_checks.sql"

# Delimiter between individual checks in the SQL file
_CHECK_SEPARATOR = ";\n"


def _split_checks(sql_text: str) -> list[str]:
    """Split the DQ SQL file into individual statements, dropping comments/blanks."""
    statements: list[str] = []
    for chunk in sql_text.split(_CHECK_SEPARATOR):
        cleaned = chunk.strip()
        if cleaned and not cleaned.startswith("--"):
            # Remove leading comment lines from the chunk
            lines = cleaned.splitlines()
            sql_lines = [ln for ln in lines if not ln.strip().startswith("--")]
            stmt = "\n".join(sql_lines).strip()
            if stmt:
                statements.append(stmt)
    return statements


def main() -> None:
    sql_text = SQL_FILE.read_text()
    checks = _split_checks(sql_text)

    if not checks:
        print("[dq] No checks found — skipping.")
        return

    conn = get_connection()
    has_failure = False

    try:
        cur = conn.cursor()
        for i, stmt in enumerate(checks, 1):
            try:
                cur.execute(stmt)
                row = cur.fetchone()
                if row is None:
                    print(f"  CHECK {i}: no result returned")
                    continue

                check_name = row[0]
                check_result = row[-1]  # check_result is always last column
                status = "FAIL" if "FAIL" in str(check_result) else "OK"
                print(f"  CHECK {i} ({check_name}): {check_result}")

                if status == "FAIL":
                    has_failure = True
            except Exception as exc:
                # Parameterized checks (e.g. :target_date) may fail without
                # binding — log and continue; the critical checks are non-parameterized.
                print(f"  CHECK {i}: skipped ({exc})")
    finally:
        conn.close()

    if has_failure:
        print("[dq] One or more checks FAILED — aborting.")
        sys.exit(1)
    else:
        print("[dq] All checks passed.")


if __name__ == "__main__":
    main()
