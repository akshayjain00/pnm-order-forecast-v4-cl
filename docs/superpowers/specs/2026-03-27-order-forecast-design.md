# PnM Intracity Order Forecast — Design Spec

**Date:** 2026-03-27
**Scope:** Intracity shifting orders only (`shifting_type = 'intra_city'`, excluding Nano packages)
**Status:** Approved for implementation

---

## 1. Problem Statement

### Current State
The existing SQL-based forecast produces a 3-day service-date demand forecast by computing two independent signals — a seasonal baseline and a pipeline estimate — then outputting `lower = min(A, B)` and `upper = max(A, B)` with a hardcoded 25% peak uplift.

### Current Gaps
- **Range too wide:** 100%+ range for today, 36% for tomorrow, 23% for day-after.
- **Range inverts:** Should narrow as the date approaches (more information available), but does the opposite.
- **Root cause:** The range measures *disagreement between two heuristics*, not forecast uncertainty. When live pipeline diverges from historical average (most likely for today), the range explodes.

### Operational Context
- **Consumers:** Truck/crew capacity planning + sales/support team staffing.
- **Grain:** date × shifting_type (intracity only: ~200-300 orders/day).
- **Cadence:** Hourly run, 3-day horizon (T, T+1, T+2).
- **Usage:** Ops team uses the midpoint of (lower, upper) as the planning number, so range width directly degrades planning quality.

---

## 2. Solution Architecture

### Three-Layer Design

```
┌─────────────────────────────────────────────────────────┐
│  Layer 1: FIXED ENVELOPE (Approach A — SQL-only)        │
│  - Weighted point estimate (pipeline↔seasonal blend)    │
│  - Horizon-aware weights                                │
│  - Range from historical error percentiles              │
├─────────────────────────────────────────────────────────┤
│  Layer 2: FUNNEL FORECAST (Approach B — SQL + Python)   │
│  - Confirmed-orders floor                               │
│  - Bucket-level conversion rates                        │
│  - Variance-derived confidence intervals                │
├─────────────────────────────────────────────────────────┤
│  Layer 3: BACKTESTING HARNESS                           │
│  - Retroactive simulation for last 5-7 days             │
│  - MAE / MAPE / range coverage metrics                  │
│  - Weight & logic optimization loop                     │
└─────────────────────────────────────────────────────────┘
```

### Data Flow

```
Morning run (T = today)
  │
  ├─► For each target_date in [T, T+1, T+2]:
  │     (shifting_type = intra_city only)
  │       │
  │       ├─ SQL: Query confirmed orders (already booked) ──► floor
  │       ├─ SQL: Query open pipeline by days-before-service bucket
  │       ├─ SQL: Query trailing 8-week bucket conversion rates
  │       ├─ SQL: Query seasonal baseline (10-week + 12-month)
  │       ├─ SQL: Query peak date multiplier from historical data
  │       │
  │       ├─ Python: Apply blending weights → point estimate
  │       ├─ Python: Compute variance-based confidence interval
  │       │
  │       └─► Output: (date, point_est, lower, upper, floor)
  │
  ├─► Backtester: re-run for T-7..T-1, compare to actuals
  │
  └─► Log: persist forecast output for accuracy tracking
```

### Core Design Principle
The confirmed-orders floor anchors the forecast. As the service date approaches, more orders are confirmed, so the floor rises and the uncertain portion (pipeline × conversion) shrinks — range naturally narrows toward the date.

---

## 3. Signals

### Definitions

| Term | Definition |
|------|-----------|
| **Confirmed order** | Any order (all statuses) joined to a shifting_requirement for the target date. No status filter applied — opportunity status is null for ~90% of rows historically, and order status filtering adds noise. |
| **Opportunity** | Any opportunity joined to a shifting_requirement for the target date. No status filter applied — status fill rate is too low (~90% null) for reliable filtering. All opportunities are treated equally. Conversion rate = total orders / total opportunities per shifting date. |
| **Peak date** | A date in the static holiday calendar (derived from known Indian holidays and month-end patterns), OR any date within ±1 day of a named holiday (adjacency window). This captures the PnM-specific behavior where moving peaks occur the day before or after major holidays like Diwali, Holi, etc. |

### Signal 1: Confirmed Orders Floor

All orders already booked for the target date as of the current run hour. No status filter — all order statuses are included to avoid noise from unreliable status data.

```sql
-- Confirmed orders for target_date (all statuses)
SELECT COUNT(DISTINCT o.id) AS floor_orders
FROM orders o
JOIN shifting_requirements sr ON o.sr_id = sr.id
WHERE sr.shifting_ts::date = :target_date
  AND sr.shifting_type = 'intra_city'
  AND o.crn ILIKE 'PNM%'
  AND sr.package_name NOT ILIKE '%nano%'
  -- Backtest mode only:
  -- AND o.created_at < :eval_date::date + INTERVAL ':run_hour hours'
```

Behavioral expectations:
- **T0 (today):** Floor captures ~70-80% of final actuals.
- **T+1 (tomorrow):** Floor captures ~40-60%.
- **T+2 (day-after):** Floor captures ~20-30%.

### Signal 2: Pipeline Conversion Estimate

All opportunities for the target shifting date, bucketed by creation-time relative to service date. No status filter applied — opportunity status is null for ~90% of rows, so all opportunities are treated equally. Conversion = total orders / total opportunities per shifting date.

| Bucket | Days Before Service | Behavior |
|--------|-------------------|----------|
| B0 | 0-1 | Late/urgent moves — high conversion |
| B1 | 2-3 | Standard lead time — moderate conversion |
| B2 | 4-7 | Planned moves — lower conversion, more shopping |
| B3 | 8+ | Early planners — lowest conversion |

**Note on bucketing:** Majority of leads are created 1 day before the move. Bucket distribution may be partially shaped by PnM app behavior (lead assignment logic). Bucketing is kept as a starting structure but may be simplified to a single flat conversion rate if backtesting shows no benefit from bucketing. Normalization across buckets may be needed if B0 dominates.

For each bucket, compute a trailing 8-week conversion rate from historical service dates that have already passed, matching on same weekday group and similar opportunity volume within configurable bounds.

```
pipeline_estimate = Σ (all_opps_in_bucket_k × bucket_k_conversion_rate)
```

**Conversion rate computation:**
- Conversion = total orders / total opportunities per shifting date (no status filter on either side).
- Match on same weekday.
- Filter to days with similar opportunity volume (configurable bounds, default 90%-120%).
- Recency-weighted: `weight = 1 / (days_gap + 1)`.
- Fallback to 14-day average if insufficient similar days found.
- **No censoring buffer needed:** Since conversion is measured by shifting_ts (service date), past service dates have fully settled conversions — you don't book a move after the move date has passed.

### Signal 3: Seasonal Baseline

Blended from two historical windows:

```
seasonal_baseline = w_ten × ten_week_weekday_avg + (1 - w_ten) × twelve_month_date_avg
```

- **ten_week_weekday_avg:** Average orders on the same weekday over last 10 weeks. Excludes peak dates.
- **twelve_month_date_avg:** Average orders on the same date-of-month over last 12 months. Excludes peak dates.

**Known limitation — growth bias:** The 12-month average does not adjust for business growth (~3-5% MoM). This makes it systematically conservative. Mitigation: the optimizer can push `ten_week_weight` toward its upper bound (0.8) to favor the more recent 10-week signal. A growth-adjusted 12-month baseline is deferred to Phase C.

### Peak Date Handling

Replace hardcoded 25% uplift with data-driven multiplier:

```
peak_multiplier = AVG(orders on historical peak dates) / AVG(orders on normal dates)
```

Computed from trailing 12 months. Peak dates are defined by a **static holiday calendar** with ±1 day adjacency window. The calendar is seeded from the existing special_dates list in the current SQL and mapped to named Indian holidays. This captures the PnM-specific behavior where demand peaks 1 day before or after major holidays (e.g., people move before Diwali, after Holi).

**Holiday calendar structure:**

```python
HOLIDAY_CALENDAR = {
    # Named holidays with ±1 day adjacency automatically applied
    datetime.date(2026, 1, 26): "Republic Day",
    datetime.date(2026, 3, 14): "Holi",
    datetime.date(2026, 3, 30): "Holi (regional)",
    datetime.date(2026, 4, 2): "Ram Navami",
    datetime.date(2026, 4, 14): "Ambedkar Jayanti",
    datetime.date(2026, 5, 1): "May Day",
    datetime.date(2026, 8, 15): "Independence Day",
    datetime.date(2026, 10, 2): "Gandhi Jayanti",
    datetime.date(2026, 10, 20): "Dussehra",
    datetime.date(2026, 11, 9): "Diwali",
    datetime.date(2026, 12, 25): "Christmas",
    # ... extend annually
}

# Month-end dates (last 2 days) are also peak — derived programmatically
# Adjacency: holiday ± 1 day is also peak
```

Peak dates are **excluded** from the seasonal baseline computation (Signal 3) to avoid double-counting. The multiplier is applied only once, after blending.

**Additional special dates** from the existing V1 SQL are preserved as a supplementary static list for dates that don't map to named holidays but are known peak dates (e.g., specific weekends around month-end).

### Empty Pipeline Fallback

When pipeline opportunity count for a target date falls below a minimum threshold (`min_pipeline_opps`, default 5), the horizon weight shifts toward the seasonal baseline:

```python
if pipeline_opp_count < PARAMS["min_pipeline_opps"]:
    effective_horizon_weight = 0.15  # heavily favor seasonal
else:
    effective_horizon_weight = PARAMS[f"horizon_weight_T{horizon}"]
```

This prevents over-anchoring on a small floor when pipeline data is anomalously sparse (e.g., system outage, new service area).

---

## 4. Blending Logic

### Point Estimate

```python
# Pipeline-based estimate
pipeline_total = floor + pipeline_estimate

# Determine effective horizon weight (fallback if pipeline is sparse)
if pipeline_opp_count < PARAMS["min_pipeline_opps"]:
    effective_hw = 0.15
else:
    effective_hw = PARAMS[f"horizon_weight_T{horizon}"]

# Blend with seasonal baseline using horizon-dependent weight
final_estimate = effective_hw * pipeline_total + (1 - effective_hw) * seasonal_baseline

# Apply peak multiplier if applicable (seasonal baseline excludes peak dates to avoid double-count)
if is_peak_date:
    final_estimate *= peak_multiplier

# Enforce floor: estimate can never be below confirmed orders
final_estimate = max(final_estimate, floor)
```

### Range Construction

Replace `min/max` heuristic with empirical error percentiles. Residuals are **pooled across all weekday groups within the same horizon** to ensure adequate sample size (~56 data points from 8 weeks × 7 days, rather than ~8 from a single weekday group).

```python
# From backtested residuals (trailing 8 weeks, ALL weekdays, per horizon)
# Safety floor on denominator to prevent division-by-zero on anomalous days
error_pct = (actual - predicted) / max(predicted, 1)

lower = final_estimate * (1 + P_lower(error_pct))  # e.g., P15
upper = final_estimate * (1 + P_upper(error_pct))  # e.g., P85

# Enforce: lower bound can never be below floor
lower = max(lower, floor)
```

This produces a ~70% prediction interval. The percentile bounds are calibrated in Stage 2 of optimization.

---

## 5. Optimizable Parameters

All tunable knobs live in a single config:

```python
PARAMS = {
    # Seasonal baseline blend
    "ten_week_weight":            0.6,    # initial; range [0.3, 0.8]

    # Horizon trust in pipeline vs seasonal
    "horizon_weight_T0":          0.85,   # initial; range [0.7, 0.9]
    "horizon_weight_T1":          0.65,   # initial; range [0.5, 0.7]
    "horizon_weight_T2":          0.45,   # initial; range [0.3, 0.5]

    # Similar-opp-volume filter for conversion matching
    "opp_volume_lower_pct":       0.90,   # range [0.80, 0.95]
    "opp_volume_upper_pct":       1.20,   # range [1.10, 1.30]

    # Conversion lookback
    "conversion_lookback_weeks":  8,      # fixed for now

    # Recency decay
    "recency_decay_fn":           "1/(days_gap+1)",  # fixed for now

    # Range percentiles (calibrated in Stage 2, not grid-searched)
    "range_lower_pctl":           0.15,   # initial; calibrated to coverage target
    "range_upper_pctl":           0.85,   # initial; calibrated to coverage target

    # Pipeline buckets (days before service)
    "bucket_boundaries":          [0, 2, 4, 8],  # fixed for now

    # Empty pipeline fallback threshold
    "min_pipeline_opps":          5,              # below this, shift to seasonal

    # Optimization trade-off (range-width penalty)
    "optimization_lambda":        0.3,            # range [0.1, 0.5]

    # Run cadence (hours)
    "run_cadence_hours":          1,              # hourly runs

    # Backtest as-of granularity
    "backtest_hour_step":         1,              # step size in hours for backtest replay
}
```

**Fixed vs. Tunable:**
- **Fixed (structural):** bucket boundaries, recency decay function, conversion lookback weeks, run cadence, backtest hour step.
- **Tunable (Stage 1 grid search):** ten_week_weight, horizon_weight_T0/T1/T2, opp_volume_lower_pct, opp_volume_upper_pct, optimization_lambda, min_pipeline_opps.
- **Calibrated (Stage 2):** range_lower_pctl, range_upper_pctl — derived from residuals, not grid-searched.

---

## 6. Backtesting Harness

### Simulation

For each evaluation hour `H` in the backtest window:

1. Freeze "as-of" to hour H of evaluation date D.
2. Run full forecast logic for D, D+1, D+2 using only data available before hour H of D.
3. Pull actuals from orders table for those 3 dates (all orders, no status filter, no time filter — full final count).
4. Record per-row metrics.

**As-of filtering required:** Historical CTEs (seasonal, conversion rates) are naturally deterministic — they look backward from the target date using past service dates. However, **pipeline and confirmed-orders queries must filter by creation timestamp** to avoid data leakage:

```sql
-- All pipeline/floor queries in backtest mode must include:
AND opportunities.created_at < :eval_date::date + INTERVAL ':run_hour hours'
AND orders.created_at < :eval_date::date + INTERVAL ':run_hour hours'
```

**No censoring buffer needed:** Conversion rates use shifting_ts (service date) as the grain. Past service dates have fully settled conversions — moves are not booked after the service date has passed. Historical service dates where `shifting_ts::date < eval_date` are safe to use for conversion computation.

The `:run_hour` parameter represents the hour of the backtest replay (e.g., 9 for 9 AM IST, 15 for 3 PM IST). With hourly runs, the backtest can simulate any hour of the day. In production mode, the created_at filter is unnecessary since the query runs in real-time.

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| eval_date | date | The simulated "today" |
| target_date | date | The date being forecast |
| horizon | int | 0, 1, or 2 (days from eval_date) |
| actual | int | Actual orders from orders table (final, non-cancelled) |
| point_est | float | Model's point estimate |
| lower | float | Lower bound |
| upper | float | Upper bound |
| floor | int | Confirmed orders at eval time |
| pipeline_opps | int | Open opportunities at eval time |
| pipeline_est | float | Pipeline conversion estimate |
| seasonal_base | float | Seasonal baseline value |
| effective_hw | float | Horizon weight used (for debugging) |
| error | float | actual - point_est |
| abs_pct_error | float | |error| / actual |
| in_range | bool | lower ≤ actual ≤ upper |

### Metrics (per horizon)

| Metric | Formula | Target |
|--------|---------|--------|
| MAE | mean(\|actual - predicted\|) | Minimize |
| MAPE | mean(\|actual - predicted\| / actual) | < 10% for T0, < 15% for T1, < 20% for T2 |
| Avg range width | mean(upper - lower) | Minimize (subject to coverage) |
| Range width % | mean((upper - lower) / point_est) | < 20% for T0, < 30% for T1, < 40% for T2 |
| Coverage | % of actuals within [lower, upper] | ≥ 65% |
| Bias | mean(actual - predicted) | ≈ 0 (no systematic over/under) |
| Floor accuracy | mean(floor / actual) | Tracks how much is "locked in" per horizon |

### Optimization (Two-Stage)

Optimization is split into two stages to avoid circular dependency between point-estimate weights and range percentiles:

**Stage 1 — Optimize point-estimate weights (minimize MAE):**

```python
# Grid search over: ten_week_weight, horizon_weight_T0/T1/T2, opp_volume bounds
# Objective: minimize MAE across eval window
# No range computation in this stage

best_weights = argmin(MAE(weights))  # over 5-7 day eval window
```

**Stage 2 — Calibrate range percentiles (achieve target coverage):**

```python
# With weights fixed from Stage 1, compute residuals across eval window
# Pool residuals across all weekdays within each horizon for adequate sample size
# Find percentile bounds that achieve target coverage

residuals_by_horizon = {h: [] for h in [0, 1, 2]}
for eval_date in eval_window:
    for horizon in [0, 1, 2]:
        residuals_by_horizon[horizon].append(
            (actual - predicted) / max(predicted, 1)
        )

# For each horizon, find symmetric or asymmetric percentiles achieving 65% coverage
for horizon in [0, 1, 2]:
    r = sorted(residuals_by_horizon[horizon])
    range_lower_pctl[horizon] = percentile(r, target=0.175)  # 17.5th pctl
    range_upper_pctl[horizon] = percentile(r, target=0.825)  # 82.5th pctl
    # Verify: coverage of this interval ≈ 65%
```

**Validation:**

```python
# Process:
# 1. Stage 1 + Stage 2 on 5-7 day window
# 2. Validate on 30-day lookback (no re-optimization)
# 3. If 30-day MAPE degrades > 5pp vs 7-day, flag for review
# 4. If optimal params hit boundary of allowed ranges, flag for review
# 5. Log all param choices and scores for auditability
```

**Lambda for range-width penalty:** `optimization_lambda` (default 0.3, range [0.1, 0.5]) controls the trade-off between point accuracy and range tightness in any combined-objective variant. Listed in PARAMS.

---

## 7. Implementation Structure

### File Layout

```
order_forecast/
├── sql/
│   ├── base_signals.sql          # All SQL CTEs: floor, pipeline buckets,
│   │                             # conversion rates, seasonal baselines,
│   │                             # peak multipliers
│   ├── backtest_actuals.sql      # Pull actuals for evaluation window
│   └── data_quality_checks.sql   # Pre-flight assertions (see Section 9)
│
├── src/
│   ├── config.py                 # All PARAMS in one place
│   ├── forecast.py               # Core: blend signals → point estimate + range
│   ├── backtest.py               # Run retroactive simulations
│   ├── optimize.py               # Grid search over params
│   └── logger.py                 # Persist daily forecast outputs
│
├── tests/
│   ├── test_blending.py          # Unit tests for blending logic
│   ├── test_range.py             # Unit tests for range construction
│   ├── test_edge_cases.py        # Edge case regression tests
│   └── test_sql_signals.py       # Integration tests: SQL → known output
│
├── output/
│   ├── forecasts/                # Daily forecast CSVs (append-only log)
│   └── backtest_reports/         # Backtest result CSVs
│
├── docs/superpowers/specs/
│   └── 2026-03-27-order-forecast-design.md  # This document
│
├── requirements.txt              # Python dependencies
├── pyproject.toml                # Linting/typing config (ruff, mypy)
├── PnM order forecasting SQL explanation.md     # existing reference
├── PnM order forecasting SQL.md                 # existing reference
└── pnm_application_source_docs.yml              # existing reference
```

### SQL ↔ Python Boundary

- **SQL (Snowflake):** Heavy lifting — scans `orders`, `opportunities`, `shifting_requirements`. Outputs aggregated signals as a small result set (~6 rows: 3 horizons × 2 signal columns each).
- **Python:** Lightweight — reads aggregated signals, applies blending/weights, computes range, runs optimization. No large data transfers.

### Execution

```bash
# Hourly forecast (production) — run via cron every hour
python src/forecast.py --date today --hour $(date +%H)

# Backtest last 7 days (simulates each hour)
python src/backtest.py --window 7

# Optimize parameters
python src/optimize.py --eval-window 7 --validate-window 30

# Run all verification checks
python -m pytest tests/ -v
python -m ruff check src/
python -m mypy src/
```

---

## 8. Implementation Roadmap

### Phase A: SQL Quick Fix (Target: Week 1)

Modify existing SQL to:
1. Replace `min/max` envelope with weighted point estimate.
2. Use fixed horizon weights (0.85 / 0.65 / 0.45).
3. Construct range from trailing 8-week error percentiles instead of signal disagreement.
4. Use all orders (no status filter) for floor query — status data is unreliable.
5. Ship immediately — ops gets a better number while Phase B is built.

**Shadow mode:** Run Phase A alongside the existing model for 2-3 days. Log both outputs. Compare before cutting over.

### Phase B: Full Funnel Forecast (Target: Weeks 2-3)

1. Build `sql/base_signals.sql` with pipeline bucketing, confirmed floor, and conversion rates.
2. Build `sql/data_quality_checks.sql` pre-flight assertions.
3. Build `src/config.py`, `src/forecast.py`, `src/logger.py`.
4. Build `src/backtest.py` and `src/optimize.py`.
5. Build `tests/` — full unit + integration test suite.
6. Run optimization against 5-7 day window, validate on 30 days.
7. Shadow-run Phase B alongside Phase A for 3-5 days.
8. Replace Phase A output with Phase B output after evaluation gate passes.

### Evaluation Gate

Before replacing Phase A with Phase B:
- Phase B must show MAPE improvement on 5-7 day window.
- Phase B range width must be ≤ Phase A range width at same coverage level.
- 30-day validation must not degrade > 5 percentage points vs 7-day.
- All verification checks (Section 9) must pass.

### Rollback Plan

If Phase B degrades after deployment:
1. Revert to Phase A SQL (kept as `sql/phase_a_fallback.sql`).
2. Phase A remains runnable with no Python dependency.
3. Decision to rollback triggered by: 3 consecutive days where Phase B MAPE exceeds Phase A MAPE by > 5pp, or any anomaly alert (Section 9) fires.

---

## 9. Verification & Quality Strategy

This section defines all verification mechanisms, organized by when they run.

### 9.1 Pre-Flight: Data Quality Assertions (SQL)

Run before every forecast. If any assertion fails, the forecast is flagged (not silently wrong).

```sql
-- data_quality_checks.sql

-- CHECK 1: Orders table has data for recent dates
-- Fail if zero orders in last 3 days (suggests data pipeline outage)
SELECT CASE WHEN COUNT(*) = 0 THEN 'FAIL: No orders in last 3 days'
       ELSE 'PASS' END AS check_orders_recency
FROM orders o
JOIN shifting_requirements sr ON o.sr_id = sr.id
WHERE sr.shifting_ts::date >= CURRENT_DATE - 3
  AND sr.shifting_type = 'intra_city'
  AND o.crn ILIKE 'PNM%'
  AND sr.package_name NOT ILIKE '%nano%';

-- CHECK 2: Opportunities table has data for target dates
-- Fail if zero opps for any of the 3 forecast dates
SELECT :target_date AS target_date,
       CASE WHEN COUNT(*) = 0 THEN 'FAIL: No opportunities for target date'
       ELSE 'PASS' END AS check_opps_exist
FROM opportunities opp
JOIN shifting_requirements sr ON opp.sr_id = sr.id
WHERE sr.shifting_ts::date = :target_date
  AND sr.shifting_type = 'intra_city';

-- CHECK 3: No date outliers in shifting_ts
-- Fail if any target shifting_ts is > 1 year from today
SELECT CASE WHEN COUNT(*) > 0 THEN 'WARN: shifting_ts outliers detected'
       ELSE 'PASS' END AS check_date_outliers
FROM shifting_requirements
WHERE shifting_ts::date > CURRENT_DATE + 365
  AND shifting_type = 'intra_city';

-- CHECK 4: Conversion rate sanity
-- Warn if trailing 14-day conversion > 1.0 or < 0.01 (suggests join/filter bug)
SELECT CASE WHEN conv_rate > 1.0 OR conv_rate < 0.01
            THEN 'WARN: Conversion rate out of expected range: ' || conv_rate::VARCHAR
       ELSE 'PASS' END AS check_conversion_sanity
FROM (
    SELECT COUNT(DISTINCT o.id)::FLOAT / NULLIF(COUNT(DISTINCT opp.id), 0) AS conv_rate
    FROM opportunities opp
    JOIN shifting_requirements sr ON opp.sr_id = sr.id
    LEFT JOIN orders o ON o.sr_id = sr.id AND o.crn ILIKE 'PNM%'
    WHERE sr.shifting_ts::date BETWEEN CURRENT_DATE - 16 AND CURRENT_DATE - 1
      AND sr.shifting_type = 'intra_city'
      AND sr.package_name NOT ILIKE '%nano%'
);
```

**Failure behavior:**
- `FAIL` checks → forecast is NOT produced; alert is sent; previous day's forecast is carried forward with a staleness warning.
- `WARN` checks → forecast is produced but flagged in output for human review.

### 9.2 Unit Tests (Python)

```
tests/
├── test_blending.py       # Blending logic correctness
├── test_range.py          # Range construction correctness
├── test_edge_cases.py     # Regression tests for known edge cases
└── test_sql_signals.py    # Integration: SQL → known output
```

**test_blending.py — core cases:**

| Test | Input | Expected |
|------|-------|----------|
| Basic blend T0 | floor=200, pipeline_est=50, seasonal=270, hw=0.85 | 0.85×250 + 0.15×270 = 253 |
| Floor enforcement | floor=200, pipeline_est=10, seasonal=150, hw=0.85 | max(0.85×210 + 0.15×150, 200) = 201.0 |
| Empty pipeline fallback | floor=5, pipeline_est=0, opp_count=2, seasonal=250 | 0.15×5 + 0.85×250 = 213.25 |
| Peak multiplier | base_est=250, peak_mult=1.3 | 325, then floor-enforced |
| Pipeline double-count guard | floor includes order X, open_opps excludes opp for X | No double-count |

**test_range.py — core cases:**

| Test | Input | Expected |
|------|-------|----------|
| Normal range | est=250, P15=-0.08, P85=+0.10 | lower=230, upper=275 |
| Lower bound floor enforcement | est=250, P15=-0.15, floor=230 | lower=max(212.5, 230)=230 |
| Near-zero denominator safety | predicted=0, actual=5 | error_pct uses max(predicted,1)=1 |
| Adequate sample size | residuals pooled across 7 weekdays × 8 weeks | n ≥ 50 |

**test_edge_cases.py — regression suite:**

| Test | Scenario | Expected behavior |
|------|----------|-------------------|
| No opps for target date | pipeline_opp_count=0 | Falls back to seasonal (hw=0.15) |
| All opps already converted | open_opps=0, floor=high | point_est ≈ floor (pipeline_est=0) |
| Weekend with low volume | Saturday target, seasonal=80 | Model still produces valid output |
| Month-end peak | target=March 31 | Peak multiplier applied, seasonal excludes peaks |
| Backtest for yesterday | eval_date=T-1 | created_at filter excludes today's data |

### 9.3 Integration Tests

End-to-end test with a **frozen test dataset** (a known week of historical data):

1. Load test fixtures (pre-computed SQL signal outputs for a known eval_date).
2. Run `forecast.py` with test fixtures.
3. Assert output matches expected values within tolerance (±1 order for point_est, ±2 for bounds).
4. Assert all output columns are present and correctly typed.
5. Assert output row count = exactly 3 (one per horizon).

### 9.4 Shadow Mode (Phase Transitions)

Before any phase cutover:

1. Run **both** old and new models for 3-5 days in parallel.
2. Log both outputs side-by-side in `output/shadow_comparison/`.
3. Compare: MAPE, range width, coverage, bias — per horizon.
4. Cutover only if new model is equal or better on all metrics.
5. Keep old model's SQL as `sql/phase_a_fallback.sql` for rollback.

### 9.5 Production Monitoring & Alerting

**Anomaly alerts** (checked after each hourly forecast run):

| Alert | Condition | Severity |
|-------|-----------|----------|
| Output sanity | point_est < 50 OR point_est > 600 (for intracity) | CRITICAL — likely bug |
| Range explosion | (upper - lower) / point_est > 0.5 | WARNING — range too wide |
| Floor > estimate | floor > point_est (should never happen after floor enforcement) | CRITICAL — logic bug |
| Stale data | Pre-flight data quality check failed | CRITICAL — carry forward + alert |
| Drift detection | Rolling 7-day MAPE > 20% for any horizon | WARNING — model may need re-optimization |

**Anomaly bounds** (50, 600) are derived from: historical intracity daily orders range ~150-400, with 2× safety margin on both sides. These should be recalibrated quarterly as the business scales.

**Daily accuracy log:**

Every production run appends to `output/forecasts/forecast_log.csv`:

| Column | Description |
|--------|-------------|
| run_ts | Timestamp the forecast was generated (hourly granularity) |
| target_date | Service date being forecast |
| horizon | 0, 1, or 2 |
| point_est | Predicted value |
| lower | Lower bound |
| upper | Upper bound |
| floor | Confirmed orders at run time |
| actual | Filled in retroactively once target_date passes |
| error | Filled in retroactively |
| params_hash | SHA of the PARAMS config used |

This enables long-term accuracy tracking and drift detection without relying on retroactive backtesting alone.

### 9.6 Linting & Static Analysis

Enforced via `pyproject.toml`:

```toml
[tool.ruff]
target-version = "py311"
select = ["E", "F", "W", "I", "N", "UP", "B", "A", "SIM"]

[tool.mypy]
python_version = "3.11"
strict = true
warn_return_any = true
warn_unused_configs = true
```

**SQL linting:** All SQL files formatted with consistent style (uppercase keywords, lowercase identifiers, 4-space indentation). Manual review checklist:
- All JOINs are explicit (no implicit cross joins).
- All `shifting_type` references are table-qualified (ambiguous column exists on both `opportunities` and `shifting_requirements`).
- All backtest-mode filters are present and commented (hourly as-of filter).
- No status filters on orders or opportunities (all statuses included).
- No `SELECT *` — all columns are explicit.

### 9.7 Verification Checklist (Pre-Deploy Gate)

Before any deployment (Phase A or Phase B), ALL of the following must pass:

- [ ] `python -m pytest tests/ -v` — all green
- [ ] `python -m ruff check src/` — no errors
- [ ] `python -m mypy src/` — no errors
- [ ] `sql/data_quality_checks.sql` — all PASS on production data
- [ ] Backtest 7-day window — MAPE within target thresholds
- [ ] Backtest 30-day validation — no degradation > 5pp
- [ ] Shadow comparison (if phase transition) — new ≥ old on all metrics
- [ ] Anomaly bounds verified against recent order volume
- [ ] Output schema matches spec (all columns present, correct types)
- [ ] Forecast log CSV is writeable and has correct headers

---

## 10. Decision Log

| # | Decision | Options Considered | Choice | Rationale |
|---|----------|--------------------|--------|-----------|
| D1 | Scope: intracity only | (a) Both intercity + intracity, (b) Intracity only, (c) Separate models | **(b) Intracity only** | Intercity volumes (~50-70/day) too low for reliable bucketed conversion; avoids adding shrinkage complexity. Intercity addressed separately later. |
| D2 | Range method | (a) min/max of two signals (current), (b) Fixed ± percentage, (c) Empirical error percentiles | **(c) Empirical percentiles** | (a) measures heuristic disagreement not uncertainty; (b) doesn't adapt to actual accuracy; (c) calibrates to real historical error distribution. |
| D3 | Prediction interval width | (a) 50% (P25-P75), (b) 70% (P15-P85), (c) 90% (P5-P95) | **(b) 70%** | Ops needs actionable bounds. 90% is too wide for planning; 50% too narrow (misses too often). 70% is standard for operational planning. |
| D4 | Optimization window | (a) 30-day, (b) 14-day, (c) 5-7 day with 30-day validation | **(c) 5-7 day + validation** | Recent days reflect current business regime (marketing mix, city footprint changes fast). 30-day validation prevents overfitting to a transient week. |
| D5 | SQL ↔ Python boundary | (a) Pure SQL, (b) SQL signals + Python blending, (c) Full Python with raw data pull | **(b) SQL + Python** | Heavy scanning stays in Snowflake (efficient). Python handles lightweight blending/optimization (flexible, testable). No large data transfers. |
| D6 | Bucket boundaries | (a) [0,1,3,7], (b) [0,2,4,8], (c) Optimize via grid search | **(b) [0,2,4,8]** | Aligned with observed PnM booking lead times. Fixed for simplicity; optimize in future if backtesting reveals sensitivity. |
| D7 | Peak date definition | (a) Hardcoded list (current), (b) Data-derived (outlier detection), (c) Deterministic calendar rule | **(c) Calendar rule** | (a) requires manual updates; (b) creates circularity with seasonal baseline; (c) is computable without order data, breaking the circularity. |
| D8 | Growth adjustment for 12-month signal | (a) Multiply by trailing growth rate, (b) Weighted 12-month average, (c) Let optimizer compensate via ten_week_weight | **(c) Optimizer compensates** | Simpler. The optimizer has range [0.3, 0.8] for ten_week_weight — enough room to nearly zero out the 12-month signal if it hurts. Growth adjustment deferred to Phase C. |
| D9 | Percentile sample pooling | (a) Same weekday group only (~8 points), (b) All weekdays within horizon (~56 points), (c) All weekdays all horizons (~168 points) | **(b) Per-horizon, all weekdays** | (a) too few data points for stable percentiles; (c) mixes horizons with different error profiles; (b) balances sample size with horizon specificity. |
| D10 | Order status filter for floor | (a) All orders, (b) Exclude cancelled, (c) Only completed + in-progress | **(a) All orders** | Status data is unreliable. Cancellation rates are relatively stable and the optimizer can absorb any bias. Filtering on noisy status data adds more error than it removes. |
| D11 | Conversion censoring buffer | (a) No buffer, (b) 1 day, (c) 2 days | **(a) No buffer** | Conversion is measured by shifting_ts (service date). Past service dates have settled — you don't book a move after the date has passed. Censoring buffer was designed for scenarios that don't apply to this business. |
| D12 | Opportunity status filter | (a) No filter (all opps), (b) status NOT IN (3,4), (c) status != 4 | **(a) No filter** | Status is null for ~90% of rows. Historical fill rate is even worse. Using all opps and all orders for conversion (total orders / total opps) is more robust than filtering on unreliable status. |
| D16 | Run cadence | (a) Once-daily morning, (b) Hourly, (c) Every 4 hours | **(b) Hourly** | Hourly runs give ops fresher numbers, especially for T0 where the floor changes rapidly as orders are booked through the day. Floor anchoring + hourly updates = progressively tighter range. |
| D17 | Holiday calendar approach | (a) Rule-based (month-end + holidays), (b) Static calendar from V1 SQL, (c) Static + adjacency window | **(c) Static + adjacency** | PnM demand peaks 1 day before/after holidays, not just on them. Static calendar avoids data circularity. V1 special_dates list provides the base; adjacency captures the shoulder effect. |
| D13 | Optimization approach | (a) Joint optimization of weights + percentiles, (b) Two-stage: weights first, then percentiles | **(b) Two-stage** | (a) creates circular dependency (percentiles depend on residuals which depend on weights). (b) cleanly separates concerns. |
| D14 | Verification strategy | (a) Manual backtest review only, (b) Automated tests + shadow mode + monitoring | **(b) Full automated verification** | Manual review doesn't scale and misses regressions. Automated tests catch logic bugs early. Shadow mode prevents bad deployments. Monitoring catches drift. |
| D15 | Error percentile formula denominator | (a) / predicted, (b) / actual, (c) / max(predicted, 1) | **(c) Safe denominator** | (a) blows up near zero predictions; (b) not available at prediction time for production use; (c) prevents division-by-zero while being equivalent for normal volumes. |

---

## 11. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| 5-7 day window overfits to transient pattern | Params work for this week but not next | 30-day validation gate; re-optimize weekly; alert if params hit boundary |
| Pipeline bucketing has sparse B3 data | Conversion rates noisy for early-booked opps | 14-day fallback conversion (carried from current SQL) |
| Peak dates list incomplete | Under-forecast on unlisted peaks | Deterministic calendar rule (month-end + holidays); review quarterly |
| Confirmed orders floor is low for T+2 | Floor provides little anchoring for day-after | Expected; horizon weight shifts toward seasonal for T+2 |
| Cancelled orders inflate floor | Slight over-forecast | Cancellation rates are stable (~10-12%); optimizer absorbs this bias. No status filter avoids noise from unreliable status data. |
| Pipeline double-counting with floor | Over-forecast | Conversion rate (orders/opps) is computed at the shifting_date grain; already-converted opps contribute to both numerator and denominator proportionally. |
| Hourly runs increase Snowflake compute | Higher cost | Monitor query cost; consider caching signals that don't change intra-day (seasonal baseline, conversion rates). Only floor and pipeline need hourly refresh. |
| 12-month signal growth bias | Systematic under-forecast during growth periods | Optimizer can push ten_week_weight to 0.8; growth adjustment deferred to Phase C |
| Data pipeline outage | Stale data → wrong forecast | Pre-flight checks detect; carry-forward with staleness warning |
| Model drift after business changes | Accuracy degrades silently | Rolling 7-day MAPE monitoring; alert at > 20% threshold |
| Phase B worse than Phase A | Ops gets worse numbers | Shadow mode + evaluation gate before cutover; Phase A kept as rollback |

---

## 12. Future Enhancements (Out of Scope)

- **Intercity model:** Separate model with shrinkage for low-volume segments.
- **Growth-adjusted 12-month baseline:** Multiply historical values by trailing MoM growth rate.
- **City-level forecasting:** Break intracity into per-city forecasts (requires geo_region_id segmentation).
- **ML model (Phase C):** LightGBM quantile regression with engineered features, once backtesting harness is mature.
- **Cancellation rate modeling:** Apply historical cancellation rate to floor for a "net floor" once status data quality improves.
- **Opportunity status enrichment:** Once status fill rate improves beyond 50%, revisit using status filters for pipeline segmentation.
