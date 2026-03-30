# Plan Review: 2026-03-27-order-forecast-plan.md

**Verdict: ISSUES FOUND**

---

## What Was Done Well

- TDD discipline is strong: Tasks 2-5 all write tests before implementation
- Math in test assertions is correct and matches spec Section 4 formulas
- Dependency order is clean: no forward references between tasks
- All files from the spec Section 7 file layout are created
- SQL includes all spec-required filters (cancelled status, nano exclusion, backtest as-of, censoring guard)
- Config covers all tunable and fixed params from spec Section 5

---

## Critical Issues (Must Fix)

### C1: Holiday conflict in test vs config
`test_peak_date_normal` (line 185 of plan) asserts `is_peak_date(2026-03-10) is False`, but `config.py` (line 212) includes March 10 as Maha Shivaratri. The plan notes this conflict (line 283) but the test code itself is NOT corrected -- the note says to use March 15 instead but the test still uses March 10. An implementer following the steps literally will get a failing test.

**Fix:** Change the test assertion to use `datetime.date(2026, 3, 15)` in the actual test code block.

### C2: Seasonal baseline computation missing from SQL final output
The SQL `base_signals.sql` outputs `ten_week_avg` and `twelve_month_avg` separately, and Python blends them. But the SQL output is missing the `is_peak` column that the `conftest.py` fixture (line 345) and `backtest.py` both expect. The SQL relies on Python's `is_peak_date()` for this, which is correct, but the integration tests in `test_sql_signals.py` hardcode `is_peak=False` rather than calling `is_peak_date()`. This is a minor inconsistency but the bigger issue is:

### C3: Backtest engine does NOT compute or apply ranges
`backtest.py` computes `point_est` but never calls `compute_range()`. The backtest output schema in the spec (Section 6) requires `lower`, `upper`, and `in_range` columns. The `compute_metrics()` function has no range coverage or range width metrics, which are required by the spec:
- `Avg range width`
- `Range width %`
- `Coverage` (% of actuals within [lower, upper])

This means Stage 2 calibration will work (it uses residuals), but the backtest output and metrics are incomplete.

**Fix:** After computing `point_est` in `run_forecast_for_date`, also compute error percentiles from historical data (or use defaults) and call `compute_range()`. Add `lower`, `upper`, `in_range`, and `effective_hw` to the output. Add coverage, range width, and range width % to `compute_metrics()`.

### C4: `compute_seasonal_baseline` is inline, not a testable function
The seasonal baseline formula `tww * ten_week + (1 - tww) * twelve_month` appears in both `backtest.py` (line 1441) and `test_sql_signals.py` (line 1935), duplicated as inline code. There is no unit test for this formula in isolation. The spec defines it as a distinct signal (Section 3, Signal 3). It should be a function in `forecast.py` with its own test.

---

## Important Issues (Should Fix)

### I1: Peak date exclusion in `ten_week` CTE is approximate
The SQL (line 1146-1148) uses `DAY(hist.order_date) < DAYOFMONTH(LAST_DAY(hist.order_date)) - 1` which correctly excludes last 2 days of month, but does NOT exclude holidays from the seasonal baseline. The spec says "Excludes peak dates" which includes holidays. The plan's comment at line 1195 says "holiday check done in Python" but the seasonal average is computed entirely in SQL -- there is no Python-side holiday exclusion for the seasonal baseline.

**Fix:** Either pass the holiday list as a SQL parameter/temp table and filter in the CTE, or accept this as a known limitation and document it.

### I2: Optimizer only grid-searches 4 of 8 tunable params
`stage1_optimize` (line 1626) only searches `ten_week_weight` and the three `horizon_weight_T*` params. The spec Section 6 lists `opp_volume_lower_pct`, `opp_volume_upper_pct`, `optimization_lambda`, and `min_pipeline_opps` as tunable, but these are never optimized. With only 4 params at 3 steps, the grid is 81 combinations, which is manageable. Adding even 2 more params would make it 729 -- still feasible.

**Fix:** Either include all tunable params (with coarser grid) or document which are deferred and why.

### I3: No test for `snowflake_runner.py`
The Snowflake runner module has no unit test. Even without a live connection, a test could mock the connector and verify `run_sql_file` reads the file, passes params, and returns a DataFrame with lowercased column names.

### I4: No test for `logger.py`
The logger module has no unit test. A simple test could call `log_forecast()` with a temp path and verify the CSV output has correct headers and values.

### I5: No test for `backtest.py` or `optimize.py`
Tasks 9 and 10 skip tests entirely (no TDD step). This breaks the TDD discipline established in Tasks 2-5. At minimum, unit tests should cover:
- `run_forecast_for_date` with mocked SQL runner
- `compute_metrics` with a known DataFrame
- `generate_grid` producing expected combinations
- `stage2_calibrate` logic

### I6: `twelve_month` CTE peak exclusion uses hardcoded `dom < 29`
Line 1182: `AND hist.dom < 29` is an overly conservative approximation. For months with 31 days, days 29 and 30 are wrongly excluded. For February (28 days), days 27-28 should be excluded but 27 is not. The spec says "last 2 calendar days of each month" which varies by month.

**Fix:** Use `DAY(hist.order_date) < DAYOFMONTH(LAST_DAY(hist.order_date)) - 1` consistently (same as the `ten_week` CTE).

---

## Suggestions (Nice to Have)

### S1: `conftest.py` fixture unused
The `sample_signals` fixture in `conftest.py` is never referenced by any test. It should either be used in integration tests or removed.

### S2: Missing `effective_hw` in backtest output
The spec Section 6 Output Schema requires `effective_hw` for debugging. The backtest engine does not capture or output this value.

### S3: Consider adding `--dry-run` to backtest/optimize
For initial development, a dry-run mode using fixture data (no Snowflake) would speed up iteration.

### S4: `opp_volume_lower_pct` and `opp_volume_upper_pct` are hardcoded in SQL
Lines 1098-1103 use literal `0.90` and `1.20` instead of parameterized `:opp_volume_lower_pct` and `:opp_volume_upper_pct`. This means optimizing these params in Python has no effect on the SQL query.

---

## Summary Checklist

| Criterion | Status |
|-----------|--------|
| Spec coverage | Partial -- range metrics missing from backtest, holiday exclusion incomplete |
| Task completeness | Good -- exact file paths, code, commands present |
| TDD discipline | Broken for Tasks 9-11 (no tests for backtest, optimizer, logger) |
| Dependency order | Clean -- no forward references |
| Code correctness | Test assertions match spec formulas; holiday test has known bug |
| Missing pieces | Range in backtest, seasonal baseline function, holiday exclusion in SQL |
| Verification checks | 7 of 10 spec Section 9.7 items covered; 3 deferred to manual Snowflake |
