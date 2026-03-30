# PnM Intracity Order Forecast — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a funnel-based order forecast that produces a tight, well-calibrated point estimate + range for intracity PnM orders, with a backtesting harness for parameter optimization.

**Architecture:** SQL extracts aggregated signals from Snowflake (floor, pipeline buckets, conversion rates, seasonal baselines). Python blends signals into a point estimate + confidence interval using horizon-aware weights. A backtester simulates historical forecasts and a grid-search optimizer tunes weights against recent actuals.

**Tech Stack:** Snowflake SQL, Python 3.11+, snowflake-connector-python, pandas, numpy, pytest, ruff, mypy

**Spec:** `docs/superpowers/specs/2026-03-27-order-forecast-design.md`

---

## File Structure

| File | Responsibility | Created In |
|------|---------------|------------|
| `requirements.txt` | Python dependencies | Task 1 |
| `pyproject.toml` | Linting/typing config (ruff, mypy) | Task 1 |
| `src/__init__.py` | Package marker | Task 1 |
| `src/config.py` | All PARAMS + param ranges + holidays list | Task 2 |
| `tests/test_config.py` | Config validation tests | Task 2 |
| `src/forecast.py` | Core blending logic: signals → point estimate + range | Task 3 |
| `tests/test_blending.py` | Unit tests for blending | Task 3 |
| `tests/test_range.py` | Unit tests for range construction | Task 4 |
| `tests/test_edge_cases.py` | Regression tests for edge cases | Task 5 |
| `sql/data_quality_checks.sql` | Pre-flight SQL assertions | Task 6 |
| `sql/base_signals.sql` | All SQL CTEs: floor, pipeline, conversion, seasonal, peak | Task 7 |
| `sql/backtest_actuals.sql` | Pull actuals for evaluation window | Task 8 |
| `src/snowflake_runner.py` | Thin wrapper to execute SQL against Snowflake + return DataFrames | Task 8 |
| `src/backtest.py` | Retroactive simulation engine | Task 9 |
| `src/optimize.py` | Two-stage grid search optimizer | Task 10 |
| `src/logger.py` | Persist forecast outputs to CSV | Task 11 |
| `tests/test_backtest.py` | Unit tests for backtest engine (mocked Snowflake) | Task 9b |
| `tests/test_logger.py` | Unit tests for forecast logger | Task 9b |
| `tests/test_snowflake_runner.py` | Unit tests for Snowflake runner (mocked) | Task 9b |
| `tests/test_sql_signals.py` | Integration tests: SQL → known output | Task 12 |
| `tests/conftest.py` | Shared fixtures (sample signal DataFrames) | Task 3 |

---

## Task 1: Project Scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `pyproject.toml`
- Create: `src/__init__.py`
- Create: `tests/__init__.py`
- Create: `output/forecasts/.gitkeep`
- Create: `output/backtest_reports/.gitkeep`
- Create: `sql/.gitkeep`

- [ ] **Step 1: Create requirements.txt**

```
snowflake-connector-python>=3.6.0
pandas>=2.1.0
numpy>=1.26.0
pytest>=8.0.0
ruff>=0.4.0
mypy>=1.9.0
pandas-stubs>=2.1.0
```

- [ ] **Step 2: Create pyproject.toml**

```toml
[project]
name = "pnm-order-forecast"
version = "0.1.0"
requires-python = ">=3.11"

[tool.ruff]
target-version = "py311"
select = ["E", "F", "W", "I", "N", "UP", "B", "A", "SIM"]
src = ["src"]

[tool.mypy]
python_version = "3.11"
strict = true
warn_return_any = true
warn_unused_configs = true
plugins = []

[[tool.mypy.overrides]]
module = ["snowflake.*"]
ignore_missing_imports = true

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 3: Create package markers and output dirs**

```bash
mkdir -p src tests sql output/forecasts output/backtest_reports
touch src/__init__.py tests/__init__.py
touch output/forecasts/.gitkeep output/backtest_reports/.gitkeep sql/.gitkeep
```

- [ ] **Step 4: Install dependencies and verify**

```bash
pip install -r requirements.txt
python -m ruff --version
python -m mypy --version
python -m pytest --version
```

Expected: All three tools print their version numbers without error.

- [ ] **Step 5: Commit**

```bash
git add requirements.txt pyproject.toml src/ tests/ output/ sql/
git commit -m "chore: scaffold order forecast project structure"
```

---

## Task 2: Config Module

**Files:**
- Create: `src/config.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write config validation test**

```python
# tests/test_config.py
from src.config import PARAMS, PARAM_RANGES, HOLIDAYS, is_peak_date
import datetime


def test_all_tunable_params_have_ranges() -> None:
    """Every tunable param must have a corresponding range for the optimizer."""
    tunable = [
        "ten_week_weight",
        "horizon_weight_T0",
        "horizon_weight_T1",
        "horizon_weight_T2",
        "opp_volume_lower_pct",
        "opp_volume_upper_pct",
        "optimization_lambda",
        "min_pipeline_opps",
    ]
    for p in tunable:
        assert p in PARAM_RANGES, f"Missing range for tunable param: {p}"
        lo, hi = PARAM_RANGES[p]
        assert lo < hi, f"Invalid range for {p}: [{lo}, {hi}]"
        assert lo <= PARAMS[p] <= hi, f"Default {p}={PARAMS[p]} outside range [{lo}, {hi}]"


def test_fixed_params_exist() -> None:
    """Fixed structural params must be present."""
    fixed = [
        "bucket_boundaries",
        "conversion_lookback_weeks",
        "recency_decay_fn",
        "run_cadence_hours",
        "backtest_hour_step",
    ]
    for p in fixed:
        assert p in PARAMS, f"Missing fixed param: {p}"


def test_bucket_boundaries_sorted() -> None:
    assert PARAMS["bucket_boundaries"] == sorted(PARAMS["bucket_boundaries"])


def test_peak_date_month_end() -> None:
    """Last 2 days of month are always peak."""
    assert is_peak_date(datetime.date(2026, 3, 31)) is True
    assert is_peak_date(datetime.date(2026, 3, 30)) is True
    assert is_peak_date(datetime.date(2026, 3, 15)) is False


def test_peak_date_holiday() -> None:
    """Dates in HOLIDAYS list are peak."""
    if HOLIDAYS:
        assert is_peak_date(HOLIDAYS[0]) is True


def test_peak_date_normal() -> None:
    """A mid-month weekday not in holidays is not peak."""
    # March 18, 2026 is a Wednesday, mid-month, not in HOLIDAYS
    assert is_peak_date(datetime.date(2026, 3, 18)) is False
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_config.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'src.config'`

- [ ] **Step 3: Write config module**

```python
# src/config.py
"""All forecast parameters in one place.

See spec: docs/superpowers/specs/2026-03-27-order-forecast-design.md (Section 5)
"""
import calendar
import datetime
from typing import Final

# --- Indian public holidays for 2026 (national + gazetted long weekends) ---
# Update this list annually. Adjacency (±1 day) is applied automatically.
HOLIDAYS: Final[list[datetime.date]] = [
    datetime.date(2026, 1, 26),  # Republic Day
    datetime.date(2026, 3, 14),  # Holi
    datetime.date(2026, 3, 30),  # Holi (regional)
    datetime.date(2026, 4, 2),   # Ram Navami
    datetime.date(2026, 4, 3),   # Mahavir Jayanti
    datetime.date(2026, 4, 14),  # Ambedkar Jayanti
    datetime.date(2026, 5, 1),   # May Day
    datetime.date(2026, 8, 15),  # Independence Day
    datetime.date(2026, 10, 2),  # Gandhi Jayanti
    datetime.date(2026, 10, 20), # Dussehra
    datetime.date(2026, 11, 9),  # Diwali
    datetime.date(2026, 11, 10), # Diwali day 2
    datetime.date(2026, 11, 30), # Guru Nanak Jayanti
    datetime.date(2026, 12, 25), # Christmas
]

# --- Supplementary special dates from V1 SQL (month-end clusters, known peak weekends) ---
# These are dates from the original SQL that don't map to named holidays
# but are known demand peaks. Enriched over time.
SPECIAL_DATES: Final[list[datetime.date]] = [
    # Jan 2026 (month-end cluster)
    datetime.date(2026, 1, 27), datetime.date(2026, 1, 28),
    datetime.date(2026, 1, 29), datetime.date(2026, 1, 30), datetime.date(2026, 1, 31),
    # Feb 2026
    datetime.date(2026, 2, 1), datetime.date(2026, 2, 2),
    datetime.date(2026, 2, 6), datetime.date(2026, 2, 7), datetime.date(2026, 2, 8),
    datetime.date(2026, 2, 13), datetime.date(2026, 2, 14), datetime.date(2026, 2, 15),
    datetime.date(2026, 2, 19), datetime.date(2026, 2, 20), datetime.date(2026, 2, 21),
    datetime.date(2026, 2, 22), datetime.date(2026, 2, 23),
    datetime.date(2026, 2, 27), datetime.date(2026, 2, 28),
    # Mar-Jul 2026 (abbreviated — full list in config.py)
    # ... see V1 SQL special_dates for complete list through Jul 2026
]


def is_peak_date(d: datetime.date) -> bool:
    """Peak date check with adjacency window (spec Section 3 Definitions).

    Peak = last 2 calendar days of month
           OR in HOLIDAYS list (with ±1 day adjacency)
           OR in SPECIAL_DATES list.
    PnM demand peaks 1 day before/after major holidays.
    """
    last_day = calendar.monthrange(d.year, d.month)[1]
    if d.day >= last_day - 1:
        return True
    # Check holiday with ±1 day adjacency
    for holiday in HOLIDAYS:
        if abs((d - holiday).days) <= 1:
            return True
    return d in SPECIAL_DATES


# --- Default parameters (spec Section 5) ---
PARAMS: Final[dict[str, object]] = {
    # Seasonal baseline blend
    "ten_week_weight": 0.6,
    # Horizon trust in pipeline vs seasonal
    "horizon_weight_T0": 0.85,
    "horizon_weight_T1": 0.65,
    "horizon_weight_T2": 0.45,
    # Similar-opp-volume filter for conversion matching
    "opp_volume_lower_pct": 0.90,
    "opp_volume_upper_pct": 1.20,
    # Conversion lookback
    "conversion_lookback_weeks": 8,
    # Recency decay
    "recency_decay_fn": "1/(days_gap+1)",
    # Range percentiles (calibrated in Stage 2)
    "range_lower_pctl": 0.15,
    "range_upper_pctl": 0.85,
    # Pipeline buckets (days before service)
    "bucket_boundaries": [0, 2, 4, 8],
    # Empty pipeline fallback
    "min_pipeline_opps": 5,
    # Optimization trade-off
    "optimization_lambda": 0.3,
    # Run cadence (hours)
    "run_cadence_hours": 1,
    # Backtest as-of granularity
    "backtest_hour_step": 1,
}

# --- Ranges for tunable params (spec Section 5) ---
PARAM_RANGES: Final[dict[str, tuple[float, float]]] = {
    "ten_week_weight": (0.3, 0.8),
    "horizon_weight_T0": (0.7, 0.9),
    "horizon_weight_T1": (0.5, 0.7),
    "horizon_weight_T2": (0.3, 0.5),
    "opp_volume_lower_pct": (0.80, 0.95),
    "opp_volume_upper_pct": (1.10, 1.30),
    "optimization_lambda": (0.1, 0.5),
    "min_pipeline_opps": (3, 10),
}
```

**Note:** `test_peak_date_normal` uses March 18 (Wednesday, not a holiday, mid-month) — a clearly non-peak date.

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_config.py -v
```

Expected: All 7 tests PASS.

- [ ] **Step 5: Run linting**

```bash
python -m ruff check src/config.py
python -m mypy src/config.py
```

Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/config.py tests/test_config.py
git commit -m "feat: add config module with all forecast parameters and peak date logic"
```

---

## Task 3: Core Blending Logic

**Files:**
- Create: `src/forecast.py`
- Create: `tests/test_blending.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: Write shared test fixtures**

```python
# tests/conftest.py
"""Shared test fixtures for forecast tests."""
import pytest
import pandas as pd


@pytest.fixture
def sample_signals() -> pd.DataFrame:
    """Typical signal output from SQL for 3 horizons."""
    return pd.DataFrame([
        {
            "target_date": "2026-03-27",
            "horizon": 0,
            "floor_orders": 200,
            "open_opps_b0": 30,
            "open_opps_b1": 45,
            "open_opps_b2": 20,
            "open_opps_b3": 10,
            "conv_rate_b0": 0.70,
            "conv_rate_b1": 0.45,
            "conv_rate_b2": 0.30,
            "conv_rate_b3": 0.15,
            "ten_week_avg": 260.0,
            "twelve_month_avg": 240.0,
            "is_peak": False,
            "peak_multiplier": 1.0,
            "total_open_opps": 105,
        },
        {
            "target_date": "2026-03-28",
            "horizon": 1,
            "floor_orders": 120,
            "open_opps_b0": 0,
            "open_opps_b1": 35,
            "open_opps_b2": 40,
            "open_opps_b3": 25,
            "conv_rate_b0": 0.70,
            "conv_rate_b1": 0.45,
            "conv_rate_b2": 0.30,
            "conv_rate_b3": 0.15,
            "ten_week_avg": 255.0,
            "twelve_month_avg": 235.0,
            "is_peak": False,
            "peak_multiplier": 1.0,
            "total_open_opps": 100,
        },
        {
            "target_date": "2026-03-29",
            "horizon": 2,
            "floor_orders": 55,
            "open_opps_b0": 0,
            "open_opps_b1": 0,
            "open_opps_b2": 30,
            "open_opps_b3": 35,
            "conv_rate_b0": 0.70,
            "conv_rate_b1": 0.45,
            "conv_rate_b2": 0.30,
            "conv_rate_b3": 0.15,
            "ten_week_avg": 250.0,
            "twelve_month_avg": 230.0,
            "is_peak": False,
            "peak_multiplier": 1.0,
            "total_open_opps": 65,
        },
    ])
```

- [ ] **Step 2: Write blending tests**

```python
# tests/test_blending.py
"""Unit tests for blending logic. Spec Section 4."""
from src.forecast import compute_pipeline_estimate, compute_point_estimate, compute_seasonal_baseline


def test_pipeline_estimate_basic() -> None:
    """Pipeline estimate = sum of (bucket_opps * bucket_conv_rate)."""
    bucket_opps = [30, 45, 20, 10]
    bucket_convs = [0.70, 0.45, 0.30, 0.15]
    result = compute_pipeline_estimate(bucket_opps, bucket_convs)
    # 30*0.70 + 45*0.45 + 20*0.30 + 10*0.15 = 21 + 20.25 + 6 + 1.5 = 48.75
    assert abs(result - 48.75) < 0.01


def test_point_estimate_basic_t0() -> None:
    """Basic blending at horizon 0 without peak or fallback."""
    result = compute_point_estimate(
        floor=200,
        pipeline_estimate=50.0,
        seasonal_baseline=270.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        peak_multiplier=1.0,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # pipeline_total = 200 + 50 = 250
    # blend = 0.85 * 250 + 0.15 * 270 = 212.5 + 40.5 = 253.0
    # max(253.0, 200) = 253.0
    assert abs(result - 253.0) < 0.01


def test_floor_enforcement() -> None:
    """Estimate can never be below floor."""
    result = compute_point_estimate(
        floor=200,
        pipeline_estimate=10.0,
        seasonal_baseline=150.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        peak_multiplier=1.0,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # pipeline_total = 210, blend = 0.85*210 + 0.15*150 = 178.5 + 22.5 = 201.0
    # max(201.0, 200) = 201.0
    assert abs(result - 201.0) < 0.01


def test_floor_enforcement_below() -> None:
    """When seasonal is very low, floor dominates."""
    result = compute_point_estimate(
        floor=200,
        pipeline_estimate=0.0,
        seasonal_baseline=100.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=False,
        peak_multiplier=1.0,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # pipeline_total = 200, blend = 0.85*200 + 0.15*100 = 170 + 15 = 185.0
    # max(185.0, 200) = 200.0
    assert abs(result - 200.0) < 0.01


def test_empty_pipeline_fallback() -> None:
    """When pipeline is sparse, weight shifts to seasonal."""
    result = compute_point_estimate(
        floor=5,
        pipeline_estimate=0.0,
        seasonal_baseline=250.0,
        horizon=0,
        pipeline_opp_count=2,  # below min_pipeline_opps=5
        is_peak=False,
        peak_multiplier=1.0,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # Fallback hw = 0.15
    # blend = 0.15 * 5 + 0.85 * 250 = 0.75 + 212.5 = 213.25
    # max(213.25, 5) = 213.25
    assert abs(result - 213.25) < 0.01


def test_seasonal_baseline_blend() -> None:
    """Seasonal = w * ten_week + (1-w) * twelve_month."""
    result = compute_seasonal_baseline(ten_week_avg=260.0, twelve_month_avg=240.0, ten_week_weight=0.6)
    # 0.6 * 260 + 0.4 * 240 = 156 + 96 = 252.0
    assert abs(result - 252.0) < 0.01


def test_seasonal_baseline_extreme_weight() -> None:
    """Weight=0.8 (near-max) should heavily favor ten_week."""
    result = compute_seasonal_baseline(ten_week_avg=300.0, twelve_month_avg=200.0, ten_week_weight=0.8)
    # 0.8 * 300 + 0.2 * 200 = 240 + 40 = 280.0
    assert abs(result - 280.0) < 0.01


def test_peak_multiplier() -> None:
    """Peak multiplier applied after blend, floor still enforced."""
    result = compute_point_estimate(
        floor=200,
        pipeline_estimate=50.0,
        seasonal_baseline=270.0,
        horizon=0,
        pipeline_opp_count=100,
        is_peak=True,
        peak_multiplier=1.3,
        params={"horizon_weight_T0": 0.85, "min_pipeline_opps": 5},
    )
    # blend = 253.0 (from basic test), * 1.3 = 328.9
    # max(328.9, 200) = 328.9
    assert abs(result - 328.9) < 0.01
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_blending.py -v
```

Expected: FAIL — `cannot import name 'compute_pipeline_estimate' from 'src.forecast'`

- [ ] **Step 4: Write minimal forecast module (blending only)**

```python
# src/forecast.py
"""Core forecast blending logic.

See spec: docs/superpowers/specs/2026-03-27-order-forecast-design.md (Section 4)
"""
from typing import Any

FALLBACK_HORIZON_WEIGHT: float = 0.15


def compute_pipeline_estimate(
    bucket_opps: list[int],
    bucket_convs: list[float],
) -> float:
    """Sum of (open_opps_in_bucket * bucket_conversion_rate)."""
    return sum(o * c for o, c in zip(bucket_opps, bucket_convs, strict=True))


def compute_point_estimate(
    floor: int,
    pipeline_estimate: float,
    seasonal_baseline: float,
    horizon: int,
    pipeline_opp_count: int,
    is_peak: bool,
    peak_multiplier: float,
    params: dict[str, Any],
) -> float:
    """Blend signals into a single point estimate (spec Section 4).

    Returns the final forecast value, guaranteed >= floor.
    """
    pipeline_total = floor + pipeline_estimate

    # Determine effective horizon weight
    min_opps: int = int(params["min_pipeline_opps"])
    if pipeline_opp_count < min_opps:
        effective_hw = FALLBACK_HORIZON_WEIGHT
    else:
        effective_hw = float(params[f"horizon_weight_T{horizon}"])

    # Blend
    estimate = effective_hw * pipeline_total + (1.0 - effective_hw) * seasonal_baseline

    # Peak multiplier
    if is_peak:
        estimate *= peak_multiplier

    # Floor enforcement
    return max(estimate, float(floor))


def compute_seasonal_baseline(
    ten_week_avg: float,
    twelve_month_avg: float,
    ten_week_weight: float,
) -> float:
    """Blend 10-week and 12-month signals into seasonal baseline (spec Section 3, Signal 3)."""
    return ten_week_weight * ten_week_avg + (1.0 - ten_week_weight) * twelve_month_avg
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_blending.py -v
```

Expected: All 6 tests PASS.

- [ ] **Step 6: Run linting**

```bash
python -m ruff check src/forecast.py tests/test_blending.py
python -m mypy src/forecast.py
```

Expected: No errors.

- [ ] **Step 7: Commit**

```bash
git add src/forecast.py tests/test_blending.py tests/conftest.py
git commit -m "feat: core blending logic with pipeline estimate and floor enforcement"
```

---

## Task 4: Range Construction

**Files:**
- Modify: `src/forecast.py` (add range functions)
- Create: `tests/test_range.py`

- [ ] **Step 1: Write range tests**

```python
# tests/test_range.py
"""Unit tests for range construction. Spec Section 4 Range Construction."""
import numpy as np
from src.forecast import compute_range, compute_error_pct


def test_error_pct_normal() -> None:
    """Normal case: (actual - predicted) / max(predicted, 1)."""
    assert abs(compute_error_pct(actual=270, predicted=250) - 0.08) < 0.001


def test_error_pct_safe_denominator() -> None:
    """Near-zero predicted uses max(predicted, 1)."""
    result = compute_error_pct(actual=5, predicted=0)
    assert abs(result - 5.0) < 0.001


def test_error_pct_negative() -> None:
    """Under-prediction yields negative error_pct."""
    result = compute_error_pct(actual=200, predicted=250)
    assert result < 0


def test_range_normal() -> None:
    """Normal range from percentiles."""
    lower, upper = compute_range(
        point_estimate=250.0,
        floor=200,
        p_lower=-0.08,
        p_upper=0.10,
    )
    # lower = 250 * (1 + -0.08) = 250 * 0.92 = 230.0
    # upper = 250 * (1 + 0.10) = 250 * 1.10 = 275.0
    assert abs(lower - 230.0) < 0.01
    assert abs(upper - 275.0) < 0.01


def test_range_lower_enforces_floor() -> None:
    """Lower bound can never be below floor."""
    lower, upper = compute_range(
        point_estimate=250.0,
        floor=230,
        p_lower=-0.15,  # would give 212.5
        p_upper=0.10,
    )
    assert lower == 230  # floor enforcement
    assert abs(upper - 275.0) < 0.01


def test_range_with_residuals() -> None:
    """Calibrate percentiles from a residual array."""
    from src.forecast import calibrate_percentiles

    residuals = np.array([-0.10, -0.05, -0.02, 0.01, 0.03, 0.06, 0.08, 0.12, 0.15, 0.20])
    p_lower, p_upper = calibrate_percentiles(residuals, target_coverage=0.65)
    # Should capture ~65% of the residuals
    coverage = np.mean((residuals >= p_lower) & (residuals <= p_upper))
    assert coverage >= 0.60  # allow small tolerance due to discrete sample
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_range.py -v
```

Expected: FAIL — `cannot import name 'compute_range'`

- [ ] **Step 3: Add range functions to forecast.py**

Add the following to the end of `src/forecast.py`:

```python
import numpy as np
from numpy.typing import NDArray


def compute_error_pct(actual: float, predicted: float) -> float:
    """Compute relative error with safe denominator (spec Section 4)."""
    return (actual - predicted) / max(predicted, 1.0)


def compute_range(
    point_estimate: float,
    floor: int,
    p_lower: float,
    p_upper: float,
) -> tuple[float, float]:
    """Construct prediction interval from error percentiles (spec Section 4).

    Lower bound is guaranteed >= floor.
    """
    lower = point_estimate * (1.0 + p_lower)
    upper = point_estimate * (1.0 + p_upper)
    lower = max(lower, float(floor))
    return lower, upper


def calibrate_percentiles(
    residuals: NDArray[np.floating[Any]],
    target_coverage: float = 0.65,
) -> tuple[float, float]:
    """Find symmetric percentile bounds achieving target coverage (spec Section 6 Stage 2).

    Uses centered interval: [P((1-coverage)/2), P((1+coverage)/2)]
    """
    lower_pctl = (1.0 - target_coverage) / 2.0
    upper_pctl = (1.0 + target_coverage) / 2.0
    p_lower = float(np.percentile(residuals, lower_pctl * 100))
    p_upper = float(np.percentile(residuals, upper_pctl * 100))
    return p_lower, p_upper
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_range.py -v
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Run linting**

```bash
python -m ruff check src/forecast.py tests/test_range.py
python -m mypy src/forecast.py
```

Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/forecast.py tests/test_range.py
git commit -m "feat: range construction with floor enforcement and percentile calibration"
```

---

## Task 5: Edge Case Regression Tests

**Files:**
- Create: `tests/test_edge_cases.py`

- [ ] **Step 1: Write edge case tests**

```python
# tests/test_edge_cases.py
"""Regression tests for known edge cases. Spec Section 9.2."""
from src.forecast import compute_point_estimate, compute_pipeline_estimate, compute_range

DEFAULT_PARAMS = {
    "horizon_weight_T0": 0.85,
    "horizon_weight_T1": 0.65,
    "horizon_weight_T2": 0.45,
    "min_pipeline_opps": 5,
}


def test_no_opps_for_target_date() -> None:
    """Zero pipeline opps → fallback to seasonal."""
    result = compute_point_estimate(
        floor=0, pipeline_estimate=0.0, seasonal_baseline=250.0,
        horizon=0, pipeline_opp_count=0, is_peak=False,
        peak_multiplier=1.0, params=DEFAULT_PARAMS,
    )
    # hw fallback = 0.15, blend = 0.15*0 + 0.85*250 = 212.5
    assert abs(result - 212.5) < 0.01


def test_all_opps_already_converted() -> None:
    """All opps converted → open_opps=0, pipeline_est=0, floor is high."""
    result = compute_point_estimate(
        floor=280, pipeline_estimate=0.0, seasonal_baseline=260.0,
        horizon=0, pipeline_opp_count=50, is_peak=False,
        peak_multiplier=1.0, params=DEFAULT_PARAMS,
    )
    # pipeline_total = 280, blend = 0.85*280 + 0.15*260 = 238 + 39 = 277.0
    # max(277.0, 280) = 280.0 (floor wins)
    assert abs(result - 280.0) < 0.01


def test_weekend_low_volume() -> None:
    """Model handles low seasonal without breaking."""
    result = compute_point_estimate(
        floor=30, pipeline_estimate=15.0, seasonal_baseline=80.0,
        horizon=2, pipeline_opp_count=20, is_peak=False,
        peak_multiplier=1.0, params=DEFAULT_PARAMS,
    )
    # pipeline_total = 45, blend = 0.45*45 + 0.55*80 = 20.25 + 44 = 64.25
    # max(64.25, 30) = 64.25
    assert result > 0
    assert result >= 30  # floor


def test_month_end_peak() -> None:
    """Peak multiplier applied correctly on month-end."""
    result = compute_point_estimate(
        floor=200, pipeline_estimate=50.0, seasonal_baseline=250.0,
        horizon=0, pipeline_opp_count=100, is_peak=True,
        peak_multiplier=1.25, params=DEFAULT_PARAMS,
    )
    # blend = 0.85*250 + 0.15*250 = 250.0, * 1.25 = 312.5
    # max(312.5, 200) = 312.5
    assert result > 250  # multiplier must have effect


def test_range_never_below_floor() -> None:
    """Even with large negative error percentile, lower >= floor."""
    lower, upper = compute_range(
        point_estimate=100.0, floor=95, p_lower=-0.20, p_upper=0.10,
    )
    assert lower >= 95


def test_pipeline_estimate_empty_buckets() -> None:
    """All buckets empty → pipeline estimate is 0."""
    result = compute_pipeline_estimate([0, 0, 0, 0], [0.70, 0.45, 0.30, 0.15])
    assert result == 0.0


def test_pipeline_estimate_single_bucket() -> None:
    """Only one bucket has opps."""
    result = compute_pipeline_estimate([50, 0, 0, 0], [0.70, 0.45, 0.30, 0.15])
    assert abs(result - 35.0) < 0.01
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
python -m pytest tests/test_edge_cases.py -v
```

Expected: All 7 tests PASS (implementation already exists from Tasks 3-4).

- [ ] **Step 3: Commit**

```bash
git add tests/test_edge_cases.py
git commit -m "test: edge case regression suite for forecast logic"
```

---

## Task 6: Data Quality Checks SQL

**Files:**
- Create: `sql/data_quality_checks.sql`

- [ ] **Step 1: Write the SQL file**

```sql
-- sql/data_quality_checks.sql
-- Pre-flight data quality assertions.
-- Run before every forecast. See spec Section 9.1.
-- Each query returns a single row with check_name and check_result columns.

-- CHECK 1: Orders table has data for recent dates
-- FAIL if zero intracity PNM orders in last 3 days (suggests data pipeline outage)
SELECT
    'check_orders_recency' AS check_name,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL: No orders in last 3 days'
        ELSE 'PASS'
    END AS check_result
FROM pnm_application.orders o
INNER JOIN pnm_application.shifting_requirements sr
    ON o.sr_id = sr.id
WHERE sr.shifting_ts::DATE >= CURRENT_DATE - 3
    AND sr.shifting_type = 'intra_city'
    AND o.crn ILIKE 'PNM%'
    AND sr.package_name NOT ILIKE '%nano%'
    -- No status filter: all order statuses included (status data unreliable);

-- CHECK 2: Opportunities exist for each target date
-- Run once per target_date. FAIL if zero opps for that date.
-- Parameterize: :target_date
SELECT
    'check_opps_exist' AS check_name,
    :target_date AS target_date,
    CASE
        WHEN COUNT(*) = 0 THEN 'FAIL: No opportunities for target date'
        ELSE 'PASS'
    END AS check_result
FROM pnm_application.opportunities opp
INNER JOIN pnm_application.shifting_requirements sr
    ON opp.sr_id = sr.id
WHERE sr.shifting_ts::DATE = :target_date
    AND sr.shifting_type = 'intra_city'
    AND sr.package_name NOT ILIKE '%nano%';

-- CHECK 3: No date outliers in shifting_ts
-- WARN if shifting_ts values > 1 year from today exist
SELECT
    'check_date_outliers' AS check_name,
    CASE
        WHEN COUNT(*) > 0 THEN 'WARN: shifting_ts outliers detected'
        ELSE 'PASS'
    END AS check_result
FROM pnm_application.shifting_requirements
WHERE shifting_ts::DATE > CURRENT_DATE + 365
    AND shifting_type = 'intra_city';

-- CHECK 4: Conversion rate sanity
-- WARN if trailing 14-day conversion is > 1.0 or < 0.01
SELECT
    'check_conversion_sanity' AS check_name,
    CASE
        WHEN conv_rate > 1.0 OR conv_rate < 0.01
            THEN 'WARN: Conversion rate out of expected range: ' || conv_rate::VARCHAR
        ELSE 'PASS'
    END AS check_result
FROM (
    SELECT
        COUNT(DISTINCT o.id)::FLOAT
            / NULLIF(COUNT(DISTINCT opp.id), 0) AS conv_rate
    FROM pnm_application.opportunities opp
    INNER JOIN pnm_application.shifting_requirements sr
        ON opp.sr_id = sr.id
    LEFT JOIN pnm_application.orders o
        ON o.sr_id = sr.id
        AND o.crn ILIKE 'PNM%'
        -- No status filter: all order statuses included (status data unreliable)
    WHERE sr.shifting_ts::DATE BETWEEN CURRENT_DATE - 16 AND CURRENT_DATE - 2
        AND sr.shifting_type = 'intra_city'
        AND sr.package_name NOT ILIKE '%nano%'
);
```

- [ ] **Step 2: Commit**

```bash
git add sql/data_quality_checks.sql
git commit -m "feat: pre-flight data quality checks SQL"
```

---

## Task 7: Base Signals SQL

**Files:**
- Create: `sql/base_signals.sql`

This is the core SQL that extracts all signals from Snowflake. It's parameterized with `:eval_date` (for backtest mode, defaults to CURRENT_DATE for production) and `:backtest_mode` (boolean).

- [ ] **Step 1: Write base_signals.sql**

```sql
-- sql/base_signals.sql
-- Extracts all forecast signals from Snowflake.
-- Parameters:
--   :eval_date             DATE   -- The simulated "today" (CURRENT_DATE for production)
--   :backtest_mode         BOOL   -- TRUE for backtesting, FALSE for production
--   :run_hour              INT    -- Hour of day for as-of cutoff (0-23, IST)
--   :opp_volume_lower_pct  FLOAT  -- Lower bound for similar-volume matching (default 0.90)
--   :opp_volume_upper_pct  FLOAT  -- Upper bound for similar-volume matching (default 1.20)
--
-- Output: One row per (target_date, horizon) with all signals.
-- See spec Section 3 for signal definitions.

WITH dates AS (
    -- Next 3 service dates from eval_date
    SELECT
        DATEADD(DAY, seq4(), :eval_date::DATE) AS target_date,
        seq4() AS horizon
    FROM TABLE(GENERATOR(ROWCOUNT => 3))
),

ist_offset AS (
    -- Reusable IST interval
    SELECT INTERVAL '5 hours, 30 minutes' AS tz
),

-- SIGNAL 1: Confirmed orders floor (spec Section 3, Signal 1)
floor_orders AS (
    SELECT
        d.target_date,
        d.horizon,
        COUNT(DISTINCT o.id) AS floor_orders
    FROM dates d
    INNER JOIN pnm_application.shifting_requirements sr
        ON CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE) = d.target_date
    INNER JOIN pnm_application.orders o
        ON o.sr_id = sr.id
    WHERE sr.shifting_type = 'intra_city'
        AND o.crn ILIKE 'PNM%'
        AND sr.package_name NOT ILIKE '%nano%'
        -- No status filter: all order statuses included (status data unreliable)
        -- Backtest as-of filter: only orders created before morning of eval_date
        AND (
            NOT :backtest_mode
            OR o.created_at < :eval_date::DATE + INTERVAL ':run_hour hours'
        )
    GROUP BY d.target_date, d.horizon
),

-- SIGNAL 2: Open pipeline by bucket (spec Section 3, Signal 2)
-- All opportunities included (no status filter — status is ~90% null)
pipeline_buckets AS (
    SELECT
        d.target_date,
        d.horizon,
        CASE
            WHEN DATEDIFF(DAY, CAST(opp.created_at + (SELECT tz FROM ist_offset) AS DATE),
                          d.target_date) <= 1 THEN 0  -- B0: 0-1 days
            WHEN DATEDIFF(DAY, CAST(opp.created_at + (SELECT tz FROM ist_offset) AS DATE),
                          d.target_date) <= 3 THEN 1  -- B1: 2-3 days
            WHEN DATEDIFF(DAY, CAST(opp.created_at + (SELECT tz FROM ist_offset) AS DATE),
                          d.target_date) <= 7 THEN 2  -- B2: 4-7 days
            ELSE 3                                      -- B3: 8+ days
        END AS bucket,
        COUNT(DISTINCT opp.id) AS opp_count
    FROM dates d
    INNER JOIN pnm_application.shifting_requirements sr
        ON CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE) = d.target_date
    INNER JOIN pnm_application.opportunities opp
        ON opp.sr_id = sr.id
    WHERE sr.shifting_type = 'intra_city'
        AND sr.package_name NOT ILIKE '%nano%'
        -- No status filter: all opp statuses included (~90% null historically)
        -- Backtest as-of filter
        AND (
            NOT :backtest_mode
            OR opp.created_at < :eval_date::DATE + INTERVAL ':run_hour hours'
        )
    GROUP BY d.target_date, d.horizon, bucket
),

-- Pivot pipeline buckets to one row per (target_date, horizon)
pipeline_pivoted AS (
    SELECT
        target_date,
        horizon,
        COALESCE(SUM(CASE WHEN bucket = 0 THEN opp_count END), 0) AS open_opps_b0,
        COALESCE(SUM(CASE WHEN bucket = 1 THEN opp_count END), 0) AS open_opps_b1,
        COALESCE(SUM(CASE WHEN bucket = 2 THEN opp_count END), 0) AS open_opps_b2,
        COALESCE(SUM(CASE WHEN bucket = 3 THEN opp_count END), 0) AS open_opps_b3,
        COALESCE(SUM(opp_count), 0) AS total_open_opps
    FROM pipeline_buckets
    GROUP BY target_date, horizon
),

-- Historical conversion rates by bucket (spec Section 3, Signal 2)
-- Uses past service dates (shifting_ts < eval_date — no censoring buffer needed)
-- Matches on same weekday, similar opp volume, recency-weighted
historical_bucket_conv AS (
    SELECT
        d.target_date,
        d.horizon,
        hist_bucket.bucket,
        -- Recency-weighted average conversion per bucket
        COALESCE(
            SUM(hist_bucket.conv_rate * (1.0 / (DATEDIFF(DAY, hist_bucket.service_date, d.target_date) + 1)))
            / NULLIF(SUM(1.0 / (DATEDIFF(DAY, hist_bucket.service_date, d.target_date) + 1)), 0),
            -- Fallback: 14-day average conversion (all buckets combined)
            (
                SELECT
                    COUNT(DISTINCT o2.id)::FLOAT / NULLIF(COUNT(DISTINCT opp2.id), 0)
                FROM pnm_application.opportunities opp2
                INNER JOIN pnm_application.shifting_requirements sr2
                    ON opp2.sr_id = sr2.id
                LEFT JOIN pnm_application.orders o2
                    ON o2.sr_id = sr2.id
                    AND o2.crn ILIKE 'PNM%'
                    -- No status filter: all order statuses included
                WHERE CAST(sr2.shifting_ts + (SELECT tz FROM ist_offset) AS DATE)
                    BETWEEN DATEADD(DAY, -14, d.target_date) AND DATEADD(DAY, -1, d.target_date)
                    AND sr2.shifting_type = 'intra_city'
                    AND sr2.package_name NOT ILIKE '%nano%'
            )
        ) AS conv_rate
    FROM dates d
    CROSS JOIN (
        -- Pre-compute per-bucket conversion for resolved historical dates
        SELECT
            CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE) AS service_date,
            DAYOFWEEK(sr.shifting_ts + (SELECT tz FROM ist_offset)) AS dow,
            CASE
                WHEN DATEDIFF(DAY, CAST(opp.created_at + (SELECT tz FROM ist_offset) AS DATE),
                              CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE)) <= 1 THEN 0
                WHEN DATEDIFF(DAY, CAST(opp.created_at + (SELECT tz FROM ist_offset) AS DATE),
                              CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE)) <= 3 THEN 1
                WHEN DATEDIFF(DAY, CAST(opp.created_at + (SELECT tz FROM ist_offset) AS DATE),
                              CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE)) <= 7 THEN 2
                ELSE 3
            END AS bucket,
            COUNT(DISTINCT opp.id) AS bucket_opps,
            COUNT(DISTINCT o.id) AS bucket_orders,
            COUNT(DISTINCT o.id)::FLOAT / NULLIF(COUNT(DISTINCT opp.id), 0) AS conv_rate
        FROM pnm_application.opportunities opp
        INNER JOIN pnm_application.shifting_requirements sr
            ON opp.sr_id = sr.id
        LEFT JOIN pnm_application.orders o
            ON o.sr_id = sr.id
            AND o.crn ILIKE 'PNM%'
            -- No status filter: all order statuses included (status data unreliable)
        WHERE sr.shifting_type = 'intra_city'
            AND sr.package_name NOT ILIKE '%nano%'
            -- No censoring buffer: past service dates are settled (moves aren't booked after the date)
            AND CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE) < :eval_date::DATE
            -- Lookback window: 8 weeks
            AND CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE)
                >= DATEADD(WEEK, -8, :eval_date::DATE)
        GROUP BY 1, 2, 3
    ) hist_bucket
    WHERE hist_bucket.service_date < d.target_date
        AND hist_bucket.dow = DAYOFWEEK(d.target_date)
        -- opp volume bounds are parameterized for optimizer tuning
        AND hist_bucket.bucket_opps BETWEEN
            COALESCE((SELECT opp_count FROM pipeline_buckets pb
                      WHERE pb.target_date = d.target_date
                        AND pb.horizon = d.horizon
                        AND pb.bucket = hist_bucket.bucket), 0) * :opp_volume_lower_pct
            AND
            COALESCE((SELECT opp_count FROM pipeline_buckets pb
                      WHERE pb.target_date = d.target_date
                        AND pb.horizon = d.horizon
                        AND pb.bucket = hist_bucket.bucket), 0) * :opp_volume_upper_pct
    GROUP BY d.target_date, d.horizon, hist_bucket.bucket
),

-- Pivot conversion rates to one row per (target_date, horizon)
conv_pivoted AS (
    SELECT
        target_date,
        horizon,
        COALESCE(MAX(CASE WHEN bucket = 0 THEN conv_rate END), 0.10) AS conv_rate_b0,
        COALESCE(MAX(CASE WHEN bucket = 1 THEN conv_rate END), 0.10) AS conv_rate_b1,
        COALESCE(MAX(CASE WHEN bucket = 2 THEN conv_rate END), 0.10) AS conv_rate_b2,
        COALESCE(MAX(CASE WHEN bucket = 3 THEN conv_rate END), 0.05) AS conv_rate_b3
    FROM historical_bucket_conv
    GROUP BY target_date, horizon
),

-- SIGNAL 3: Seasonal baseline (spec Section 3, Signal 3)
-- 10-week weekday average (excludes peak dates)
ten_week AS (
    SELECT
        d.target_date,
        d.horizon,
        AVG(hist.order_count) AS ten_week_avg
    FROM dates d
    INNER JOIN (
        SELECT
            CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE) AS order_date,
            DAYOFWEEK(sr.shifting_ts + (SELECT tz FROM ist_offset)) AS dow,
            COUNT(DISTINCT o.id) AS order_count
        FROM pnm_application.orders o
        INNER JOIN pnm_application.shifting_requirements sr
            ON o.sr_id = sr.id
        WHERE o.crn ILIKE 'PNM%'
            AND sr.shifting_type = 'intra_city'
            AND sr.package_name NOT ILIKE '%nano%'
            -- No status filter: all order statuses included (status data unreliable)
        GROUP BY 1, 2
    ) hist
        ON hist.dow = DAYOFWEEK(d.target_date)
        AND hist.order_date BETWEEN DATEADD(DAY, -70, d.target_date)
                                AND DATEADD(DAY, -7, d.target_date)
        -- Exclude peak dates from seasonal baseline
        AND DAY(hist.order_date) < (
            SELECT DAYOFMONTH(LAST_DAY(hist.order_date)) - 1
        )
    GROUP BY d.target_date, d.horizon
),

-- 12-month same-date-of-month average (excludes peak dates)
twelve_month AS (
    SELECT
        d.target_date,
        d.horizon,
        AVG(hist.order_count) AS twelve_month_avg
    FROM dates d
    INNER JOIN (
        SELECT
            CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE) AS order_date,
            DAY(sr.shifting_ts + (SELECT tz FROM ist_offset)) AS dom,
            DATE_TRUNC('MONTH', sr.shifting_ts + (SELECT tz FROM ist_offset)) AS order_month,
            COUNT(DISTINCT o.id) AS order_count,
            ROW_NUMBER() OVER (
                PARTITION BY DAY(sr.shifting_ts + (SELECT tz FROM ist_offset))
                ORDER BY DATE_TRUNC('MONTH', sr.shifting_ts + (SELECT tz FROM ist_offset)) DESC
            ) AS rn
        FROM pnm_application.orders o
        INNER JOIN pnm_application.shifting_requirements sr
            ON o.sr_id = sr.id
        WHERE o.crn ILIKE 'PNM%'
            AND sr.shifting_type = 'intra_city'
            AND sr.package_name NOT ILIKE '%nano%'
            -- No status filter: all order statuses included (status data unreliable)
        GROUP BY 1, 2, 3
    ) hist
        ON hist.dom = DAY(d.target_date)
        AND hist.order_date < d.target_date
        AND hist.rn <= 12
        -- Exclude peak dates (last 2 days of that month, dynamically computed)
        AND hist.dom < DAYOFMONTH(LAST_DAY(hist.order_date)) - 1
    GROUP BY d.target_date, d.horizon
),

-- Peak multiplier: ratio of historical peak-day orders to normal-day orders
peak_stats AS (
    SELECT
        AVG(CASE WHEN is_peak THEN order_count END)
            / NULLIF(AVG(CASE WHEN NOT is_peak THEN order_count END), 0) AS peak_multiplier
    FROM (
        SELECT
            CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE) AS order_date,
            COUNT(DISTINCT o.id) AS order_count,
            -- Peak = last 2 days of month (holiday check done in Python)
            DAY(sr.shifting_ts + (SELECT tz FROM ist_offset))
                >= DAYOFMONTH(LAST_DAY(sr.shifting_ts + (SELECT tz FROM ist_offset))) - 1
                AS is_peak
        FROM pnm_application.orders o
        INNER JOIN pnm_application.shifting_requirements sr
            ON o.sr_id = sr.id
        WHERE o.crn ILIKE 'PNM%'
            AND sr.shifting_type = 'intra_city'
            AND sr.package_name NOT ILIKE '%nano%'
            -- No status filter: all order statuses included (status data unreliable)
            AND CAST(sr.shifting_ts + (SELECT tz FROM ist_offset) AS DATE)
                >= DATEADD(MONTH, -12, :eval_date::DATE)
        GROUP BY 1
    )
)

-- FINAL OUTPUT: One row per (target_date, horizon)
SELECT
    d.target_date,
    d.horizon,
    COALESCE(f.floor_orders, 0)         AS floor_orders,
    COALESCE(p.open_opps_b0, 0)         AS open_opps_b0,
    COALESCE(p.open_opps_b1, 0)         AS open_opps_b1,
    COALESCE(p.open_opps_b2, 0)         AS open_opps_b2,
    COALESCE(p.open_opps_b3, 0)         AS open_opps_b3,
    COALESCE(p.total_open_opps, 0)      AS total_open_opps,
    COALESCE(c.conv_rate_b0, 0.10)      AS conv_rate_b0,
    COALESCE(c.conv_rate_b1, 0.10)      AS conv_rate_b1,
    COALESCE(c.conv_rate_b2, 0.10)      AS conv_rate_b2,
    COALESCE(c.conv_rate_b3, 0.05)      AS conv_rate_b3,
    COALESCE(tw.ten_week_avg, 0)        AS ten_week_avg,
    COALESCE(tm.twelve_month_avg, 0)    AS twelve_month_avg,
    COALESCE(ps.peak_multiplier, 1.0)   AS peak_multiplier
FROM dates d
LEFT JOIN floor_orders f
    ON d.target_date = f.target_date AND d.horizon = f.horizon
LEFT JOIN pipeline_pivoted p
    ON d.target_date = p.target_date AND d.horizon = p.horizon
LEFT JOIN conv_pivoted c
    ON d.target_date = c.target_date AND d.horizon = c.horizon
LEFT JOIN ten_week tw
    ON d.target_date = tw.target_date AND d.horizon = tw.horizon
LEFT JOIN twelve_month tm
    ON d.target_date = tm.target_date AND d.horizon = tm.horizon
CROSS JOIN peak_stats ps
ORDER BY d.horizon;
```

- [ ] **Step 2: Review SQL checklist**

Verify manually:
- [ ] All JOINs are explicit (INNER JOIN or LEFT JOIN, no implicit cross joins except peak_stats which is 1 row)
- [ ] All `shifting_type` references are table-qualified (`sr.shifting_type`)
- [ ] All backtest-mode filters are present (using `:backtest_mode` flag)
- [ ] No `SELECT *` — all columns explicit
- [ ] No status filters on order or opportunity queries (all statuses included)
- [ ] Hourly as-of filter uses `:run_hour` parameter for backtest mode
- [ ] No censoring buffer — past service dates used directly (`shifting_ts < eval_date`)

- [ ] **Step 3: Commit**

```bash
git add sql/base_signals.sql
git commit -m "feat: base signals SQL with floor, pipeline buckets, conversion rates, seasonal baselines"
```

---

## Task 8: Snowflake Runner + Backtest Actuals SQL

**Files:**
- Create: `sql/backtest_actuals.sql`
- Create: `src/snowflake_runner.py`

- [ ] **Step 1: Write backtest_actuals.sql**

```sql
-- sql/backtest_actuals.sql
-- Pull actual order counts for evaluation.
-- Parameters:
--   :start_date  DATE  -- First date in evaluation window
--   :end_date    DATE  -- Last date in evaluation window (inclusive)

SELECT
    CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE) AS service_date,
    COUNT(DISTINCT o.id) AS actual_orders
FROM pnm_application.orders o
INNER JOIN pnm_application.shifting_requirements sr
    ON o.sr_id = sr.id
WHERE sr.shifting_type = 'intra_city'
    AND o.crn ILIKE 'PNM%'
    AND sr.package_name NOT ILIKE '%nano%'
    -- No status filter: all order statuses included (status data unreliable)
    AND CAST(sr.shifting_ts + INTERVAL '5 hours, 30 minutes' AS DATE)
        BETWEEN :start_date AND :end_date
GROUP BY 1
ORDER BY 1;
```

- [ ] **Step 2: Write Snowflake runner**

```python
# src/snowflake_runner.py
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
        params: Named parameters to bind (e.g., {"eval_date": "2026-03-27"}).
        conn: Optional existing connection. If None, creates one.
    """
    sql_text = Path(sql_path).read_text()
    close_conn = conn is None
    if conn is None:
        conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql_text, params or {})
        columns = [desc[0].lower() for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        if close_conn:
            conn.close()
```

- [ ] **Step 3: Run linting**

```bash
python -m ruff check src/snowflake_runner.py
python -m mypy src/snowflake_runner.py
```

Expected: No errors.

- [ ] **Step 4: Commit**

```bash
git add sql/backtest_actuals.sql src/snowflake_runner.py
git commit -m "feat: Snowflake runner and backtest actuals SQL"
```

---

## Task 9: Backtest Engine

**Files:**
- Create: `src/backtest.py`

- [ ] **Step 1: Write backtest module**

```python
# src/backtest.py
"""Retroactive simulation engine.

See spec: docs/superpowers/specs/2026-03-27-order-forecast-design.md (Section 6)
"""
import argparse
import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config import PARAMS, is_peak_date
from src.forecast import (
    calibrate_percentiles,
    compute_error_pct,
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_range,
    compute_seasonal_baseline,
)
from src.snowflake_runner import run_sql_file

SQL_DIR = Path(__file__).parent.parent / "sql"
OUTPUT_DIR = Path(__file__).parent.parent / "output" / "backtest_reports"


def run_forecast_for_date(
    eval_date: datetime.date,
    params: dict[str, Any],
    run_hour: int = 9,
    conn: Any = None,
) -> pd.DataFrame:
    """Run the full forecast for a single eval_date at a given hour. Returns 3 rows (one per horizon)."""
    signals = run_sql_file(
        SQL_DIR / "base_signals.sql",
        params={
            "eval_date": str(eval_date),
            "backtest_mode": True,
            "run_hour": run_hour,
            "opp_volume_lower_pct": float(params.get("opp_volume_lower_pct", 0.90)),
            "opp_volume_upper_pct": float(params.get("opp_volume_upper_pct", 1.20)),
        },
        conn=conn,
    )

    results = []
    for _, row in signals.iterrows():
        horizon = int(row["horizon"])
        target_date = row["target_date"]
        if isinstance(target_date, str):
            target_date = datetime.date.fromisoformat(target_date)

        floor = int(row["floor_orders"])
        bucket_opps = [
            int(row["open_opps_b0"]),
            int(row["open_opps_b1"]),
            int(row["open_opps_b2"]),
            int(row["open_opps_b3"]),
        ]
        bucket_convs = [
            float(row["conv_rate_b0"]),
            float(row["conv_rate_b1"]),
            float(row["conv_rate_b2"]),
            float(row["conv_rate_b3"]),
        ]
        total_opps = int(row["total_open_opps"])

        ten_week = float(row["ten_week_avg"])
        twelve_month = float(row["twelve_month_avg"])
        seasonal = compute_seasonal_baseline(
            ten_week, twelve_month, float(params["ten_week_weight"]),
        )

        peak = is_peak_date(target_date) if isinstance(target_date, datetime.date) else False
        peak_mult = float(row["peak_multiplier"])

        pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)
        point_est = compute_point_estimate(
            floor=floor,
            pipeline_estimate=pipeline_est,
            seasonal_baseline=seasonal,
            horizon=horizon,
            pipeline_opp_count=total_opps,
            is_peak=peak,
            peak_multiplier=peak_mult,
            params=params,
        )

        # Compute range if percentiles are available for this horizon
        p_lower_key = f"range_lower_pctl_T{horizon}"
        p_upper_key = f"range_upper_pctl_T{horizon}"
        if p_lower_key in params and p_upper_key in params:
            lower, upper = compute_range(
                point_estimate=point_est,
                floor=floor,
                p_lower=float(params[p_lower_key]),
                p_upper=float(params[p_upper_key]),
            )
        else:
            # Use default percentiles from PARAMS
            lower, upper = compute_range(
                point_estimate=point_est,
                floor=floor,
                p_lower=float(params.get("range_lower_pctl", -0.10)),
                p_upper=float(params.get("range_upper_pctl", 0.10)),
            )

        results.append({
            "eval_date": eval_date,
            "target_date": target_date,
            "horizon": horizon,
            "floor": floor,
            "pipeline_opps": total_opps,
            "pipeline_est": round(pipeline_est, 2),
            "seasonal_base": round(seasonal, 2),
            "point_est": round(point_est, 2),
            "lower": round(lower, 2),
            "upper": round(upper, 2),
        })

    return pd.DataFrame(results)


def backtest(
    window: int = 7,
    params: dict[str, Any] | None = None,
    conn: Any = None,
) -> pd.DataFrame:
    """Run backtest for the last `window` days.

    Returns a DataFrame with columns matching spec Section 6 Output Schema.
    """
    if params is None:
        params = dict(PARAMS)

    today = datetime.date.today()
    eval_dates = [today - datetime.timedelta(days=i) for i in range(1, window + 1)]

    # Collect all forecasts
    all_forecasts: list[pd.DataFrame] = []
    for ed in eval_dates:
        df = run_forecast_for_date(ed, params, conn=conn)
        all_forecasts.append(df)
    forecasts = pd.concat(all_forecasts, ignore_index=True)

    # Pull actuals
    min_target = forecasts["target_date"].min()
    max_target = forecasts["target_date"].max()
    actuals = run_sql_file(
        SQL_DIR / "backtest_actuals.sql",
        params={"start_date": str(min_target), "end_date": str(max_target)},
        conn=conn,
    )
    actuals_map = dict(zip(actuals["service_date"], actuals["actual_orders"]))

    # Merge actuals and compute metrics
    forecasts["actual"] = forecasts["target_date"].map(actuals_map)
    forecasts = forecasts.dropna(subset=["actual"])
    forecasts["actual"] = forecasts["actual"].astype(int)
    forecasts["error"] = forecasts["actual"] - forecasts["point_est"]
    forecasts["abs_pct_error"] = (
        forecasts["error"].abs() / forecasts["actual"].clip(lower=1)
    )
    forecasts["in_range"] = (
        (forecasts["actual"] >= forecasts["lower"])
        & (forecasts["actual"] <= forecasts["upper"])
    )

    return forecasts


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-horizon metrics from backtest output (spec Section 6 Metrics)."""
    metrics = []
    for horizon in [0, 1, 2]:
        h = df[df["horizon"] == horizon]
        if h.empty:
            continue
        range_width = (h["upper"] - h["lower"]).mean()
        range_pct = ((h["upper"] - h["lower"]) / h["point_est"].clip(lower=1)).mean()
        metrics.append({
            "horizon": horizon,
            "n": len(h),
            "mae": round(h["error"].abs().mean(), 2),
            "mape": round(h["abs_pct_error"].mean() * 100, 2),
            "bias": round(h["error"].mean(), 2),
            "avg_range_width": round(range_width, 2),
            "range_width_pct": round(range_pct * 100, 1),
            "coverage": round(h["in_range"].mean() * 100, 1),
            "avg_floor_pct": round((h["floor"] / h["actual"].clip(lower=1)).mean() * 100, 1),
        })
    return pd.DataFrame(metrics)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run backtest for PnM order forecast")
    parser.add_argument("--window", type=int, default=7, help="Number of days to backtest")
    args = parser.parse_args()

    print(f"Running backtest for last {args.window} days...")
    results = backtest(window=args.window)
    metrics = compute_metrics(results)

    # Save outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    results.to_csv(OUTPUT_DIR / f"backtest_{timestamp}.csv", index=False)
    print("\n--- Per-Horizon Metrics ---")
    print(metrics.to_string(index=False))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run linting**

```bash
python -m ruff check src/backtest.py
python -m mypy src/backtest.py
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add src/backtest.py
git commit -m "feat: backtest engine with per-horizon metrics"
```

---

## Task 9b: Tests for Backtest, Logger, and Snowflake Runner

**Files:**
- Create: `tests/test_backtest.py`
- Create: `tests/test_logger.py`
- Create: `tests/test_snowflake_runner.py`

- [ ] **Step 1: Write backtest unit tests (mocking Snowflake)**

```python
# tests/test_backtest.py
"""Unit tests for backtest engine (no Snowflake dependency)."""
from unittest.mock import patch, MagicMock
import datetime

import pandas as pd

from src.backtest import compute_metrics


def test_compute_metrics_basic() -> None:
    """Metrics computed correctly from a small backtest DataFrame."""
    df = pd.DataFrame([
        {"horizon": 0, "actual": 250, "point_est": 240, "error": 10,
         "abs_pct_error": 0.04, "floor": 200, "lower": 220, "upper": 260, "in_range": True},
        {"horizon": 0, "actual": 260, "point_est": 270, "error": -10,
         "abs_pct_error": 0.038, "floor": 210, "lower": 250, "upper": 290, "in_range": True},
        {"horizon": 1, "actual": 280, "point_est": 250, "error": 30,
         "abs_pct_error": 0.107, "floor": 120, "lower": 230, "upper": 270, "in_range": False},
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
    df = pd.DataFrame(columns=["horizon", "actual", "point_est", "error",
                                "abs_pct_error", "floor", "lower", "upper", "in_range"])
    metrics = compute_metrics(df)
    assert len(metrics) == 0
```

- [ ] **Step 2: Write logger tests**

```python
# tests/test_logger.py
"""Unit tests for forecast logger."""
import datetime
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.logger import log_forecast, params_hash, COLUMNS


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
```

- [ ] **Step 3: Write snowflake runner tests (mocking connection)**

```python
# tests/test_snowflake_runner.py
"""Unit tests for Snowflake runner (mocked connection)."""
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile

from src.snowflake_runner import run_sql_file


def test_run_sql_file_returns_dataframe() -> None:
    """Verify SQL file is read and executed via connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.description = [("col1",), ("col2",)]
    mock_cursor.fetchall.return_value = [(1, "a"), (2, "b")]

    with tempfile.NamedTemporaryFile(suffix=".sql", mode="w", delete=False) as f:
        f.write("SELECT 1 AS col1, 'a' AS col2")
        f.flush()
        result = run_sql_file(f.name, conn=mock_conn)

    assert len(result) == 2
    assert list(result.columns) == ["col1", "col2"]
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_backtest.py tests/test_logger.py tests/test_snowflake_runner.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_backtest.py tests/test_logger.py tests/test_snowflake_runner.py
git commit -m "test: unit tests for backtest, logger, and snowflake runner"
```

---

## Task 10: Two-Stage Optimizer

**Files:**
- Create: `src/optimize.py`

- [ ] **Step 1: Write optimizer**

```python
# src/optimize.py
"""Two-stage grid search optimizer.

Stage 1: Optimize point-estimate weights (minimize MAE).
Stage 2: Calibrate range percentiles (achieve target coverage).
See spec Section 6 Optimization.
"""
import argparse
import datetime
import itertools
from typing import Any

import numpy as np
import pandas as pd

from src.backtest import backtest, compute_metrics
from src.config import PARAMS, PARAM_RANGES
from src.forecast import calibrate_percentiles, compute_error_pct


def generate_grid(
    param_names: list[str],
    steps: int = 3,
) -> list[dict[str, float]]:
    """Generate grid of param combinations from PARAM_RANGES."""
    axes: list[list[float]] = []
    for name in param_names:
        lo, hi = PARAM_RANGES[name]
        axes.append(list(np.linspace(lo, hi, steps)))

    combos = []
    for values in itertools.product(*axes):
        combos.append(dict(zip(param_names, values)))
    return combos


def stage1_optimize(
    eval_window: int = 7,
    grid_steps: int = 3,
    conn: Any = None,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Stage 1: Grid search to minimize MAE.

    Returns (best_params, results_df).
    """
    # Primary weights (grid search with configured steps)
    primary_names = [
        "ten_week_weight",
        "horizon_weight_T0",
        "horizon_weight_T1",
        "horizon_weight_T2",
    ]
    # Secondary params (coarser 2-step grid to keep search tractable)
    secondary_names = [
        "opp_volume_lower_pct",
        "opp_volume_upper_pct",
        "optimization_lambda",
        "min_pipeline_opps",
    ]
    primary_grid = generate_grid(primary_names, steps=grid_steps)
    secondary_grid = generate_grid(secondary_names, steps=2)
    # Combine: each primary combo × each secondary combo
    grid = [
        {**p, **s} for p in primary_grid for s in secondary_grid
    ]

    best_mae = float("inf")
    best_params: dict[str, Any] = dict(PARAMS)
    all_results: list[dict[str, Any]] = []

    for combo in grid:
        trial_params = dict(PARAMS)
        trial_params.update(combo)

        try:
            bt = backtest(window=eval_window, params=trial_params, conn=conn)
            mae = bt["error"].abs().mean()
        except Exception:
            continue

        result = {**combo, "mae": mae}
        all_results.append(result)

        if mae < best_mae:
            best_mae = mae
            best_params = trial_params

    results_df = pd.DataFrame(all_results)
    return best_params, results_df


def stage2_calibrate(
    best_params: dict[str, Any],
    eval_window: int = 7,
    target_coverage: float = 0.65,
    conn: Any = None,
) -> dict[int, tuple[float, float]]:
    """Stage 2: Calibrate range percentiles per horizon.

    Returns {horizon: (p_lower, p_upper)}.
    """
    bt = backtest(window=eval_window, params=best_params, conn=conn)

    percentiles: dict[int, tuple[float, float]] = {}
    for horizon in [0, 1, 2]:
        h = bt[bt["horizon"] == horizon]
        if h.empty:
            continue
        residuals = np.array([
            compute_error_pct(actual=row["actual"], predicted=row["point_est"])
            for _, row in h.iterrows()
        ])
        p_lower, p_upper = calibrate_percentiles(residuals, target_coverage)
        percentiles[horizon] = (round(p_lower, 4), round(p_upper, 4))

    return percentiles


def validate(
    best_params: dict[str, Any],
    eval_window: int = 7,
    validate_window: int = 30,
    conn: Any = None,
) -> dict[str, Any]:
    """Validate optimized params on a wider window.

    Returns validation report.
    """
    bt_eval = backtest(window=eval_window, params=best_params, conn=conn)
    bt_val = backtest(window=validate_window, params=best_params, conn=conn)

    eval_metrics = compute_metrics(bt_eval)
    val_metrics = compute_metrics(bt_val)

    report: dict[str, Any] = {
        "eval_mape_by_horizon": eval_metrics.set_index("horizon")["mape"].to_dict(),
        "val_mape_by_horizon": val_metrics.set_index("horizon")["mape"].to_dict(),
    }

    # Check for degradation > 5pp
    for h in [0, 1, 2]:
        eval_mape = eval_metrics.loc[eval_metrics["horizon"] == h, "mape"].values
        val_mape = val_metrics.loc[val_metrics["horizon"] == h, "mape"].values
        if len(eval_mape) > 0 and len(val_mape) > 0:
            gap = val_mape[0] - eval_mape[0]
            if gap > 5.0:
                report[f"WARNING_horizon_{h}"] = (
                    f"30-day MAPE degrades by {gap:.1f}pp vs 7-day"
                )

    # Check if params hit boundaries
    for name in PARAM_RANGES:
        lo, hi = PARAM_RANGES[name]
        val = best_params.get(name)
        if val is not None and isinstance(val, (int, float)):
            if abs(float(val) - lo) < 0.01 or abs(float(val) - hi) < 0.01:
                report[f"BOUNDARY_{name}"] = f"Optimal value {val} is at range boundary [{lo}, {hi}]"

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize forecast parameters")
    parser.add_argument("--eval-window", type=int, default=7)
    parser.add_argument("--validate-window", type=int, default=30)
    parser.add_argument("--grid-steps", type=int, default=3)
    args = parser.parse_args()

    print(f"Stage 1: Optimizing weights (eval window = {args.eval_window} days)...")
    best_params, grid_results = stage1_optimize(
        eval_window=args.eval_window, grid_steps=args.grid_steps,
    )
    print(f"Best MAE: {grid_results['mae'].min():.2f}")
    print("Best weights:")
    for k in ["ten_week_weight", "horizon_weight_T0", "horizon_weight_T1", "horizon_weight_T2"]:
        print(f"  {k}: {best_params[k]:.3f}")

    print("\nStage 2: Calibrating range percentiles...")
    percentiles = stage2_calibrate(best_params, eval_window=args.eval_window)
    for h, (pl, pu) in percentiles.items():
        print(f"  Horizon {h}: P_lower={pl:.4f}, P_upper={pu:.4f}")

    print(f"\nValidating on {args.validate_window}-day window...")
    report = validate(best_params, args.eval_window, args.validate_window)
    for k, v in report.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run linting**

```bash
python -m ruff check src/optimize.py
python -m mypy src/optimize.py
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add src/optimize.py
git commit -m "feat: two-stage optimizer with grid search and validation"
```

---

## Task 11: Forecast Logger

**Files:**
- Create: `src/logger.py`

- [ ] **Step 1: Write logger module**

```python
# src/logger.py
"""Persist daily forecast outputs to CSV for accuracy tracking.

See spec Section 9.5 Daily accuracy log.
"""
import csv
import datetime
import hashlib
import json
from pathlib import Path
from typing import Any

FORECAST_LOG = Path(__file__).parent.parent / "output" / "forecasts" / "forecast_log.csv"

COLUMNS = [
    "run_ts",
    "target_date",
    "horizon",
    "point_est",
    "lower",
    "upper",
    "floor",
    "actual",
    "error",
    "params_hash",
]


def params_hash(params: dict[str, Any]) -> str:
    """SHA256 of serialized params for auditability."""
    serialized = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:12]


def log_forecast(
    run_ts: datetime.datetime,
    target_date: datetime.date,
    horizon: int,
    point_est: float,
    lower: float,
    upper: float,
    floor: int,
    params: dict[str, Any],
) -> None:
    """Append a single forecast row to the log CSV."""
    FORECAST_LOG.parent.mkdir(parents=True, exist_ok=True)

    file_exists = FORECAST_LOG.exists()
    with open(FORECAST_LOG, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "run_ts": str(run_ts),
            "target_date": str(target_date),
            "horizon": horizon,
            "point_est": round(point_est, 2),
            "lower": round(lower, 2),
            "upper": round(upper, 2),
            "floor": floor,
            "actual": "",  # filled retroactively
            "error": "",   # filled retroactively
            "params_hash": params_hash(params),
        })
```

- [ ] **Step 2: Run linting**

```bash
python -m ruff check src/logger.py
python -m mypy src/logger.py
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add src/logger.py
git commit -m "feat: forecast logger for daily accuracy tracking"
```

---

## Task 12: Integration Tests

**Files:**
- Create: `tests/test_sql_signals.py`

These tests use frozen fixtures (no Snowflake dependency) to verify the full pipeline from signals → forecast output.

- [ ] **Step 1: Write integration tests**

```python
# tests/test_sql_signals.py
"""Integration tests: signal DataFrame → full forecast output.

Uses frozen fixtures instead of live Snowflake connection.
See spec Section 9.3.
"""
import datetime

import pandas as pd

from src.config import PARAMS
from src.forecast import (
    compute_pipeline_estimate,
    compute_point_estimate,
    compute_range,
)


def make_signal_row(
    horizon: int = 0,
    floor_orders: int = 200,
    open_opps: tuple[int, int, int, int] = (30, 45, 20, 10),
    conv_rates: tuple[float, float, float, float] = (0.70, 0.45, 0.30, 0.15),
    ten_week_avg: float = 260.0,
    twelve_month_avg: float = 240.0,
    peak_multiplier: float = 1.0,
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "floor_orders": floor_orders,
        "open_opps_b0": open_opps[0],
        "open_opps_b1": open_opps[1],
        "open_opps_b2": open_opps[2],
        "open_opps_b3": open_opps[3],
        "conv_rate_b0": conv_rates[0],
        "conv_rate_b1": conv_rates[1],
        "conv_rate_b2": conv_rates[2],
        "conv_rate_b3": conv_rates[3],
        "ten_week_avg": ten_week_avg,
        "twelve_month_avg": twelve_month_avg,
        "total_open_opps": sum(open_opps),
        "peak_multiplier": peak_multiplier,
    }


def run_forecast_from_row(row: dict[str, object], params: dict[str, object] | None = None) -> dict[str, float]:
    """Simulate the full forecast pipeline from a signal row."""
    p = dict(PARAMS) if params is None else dict(params)
    horizon = int(row["horizon"])  # type: ignore[arg-type]
    floor = int(row["floor_orders"])  # type: ignore[arg-type]

    bucket_opps = [int(row[f"open_opps_b{i}"]) for i in range(4)]  # type: ignore[arg-type]
    bucket_convs = [float(row[f"conv_rate_b{i}"]) for i in range(4)]  # type: ignore[arg-type]

    tww = float(p["ten_week_weight"])  # type: ignore[arg-type]
    seasonal = tww * float(row["ten_week_avg"]) + (1.0 - tww) * float(row["twelve_month_avg"])  # type: ignore[arg-type]

    pipeline_est = compute_pipeline_estimate(bucket_opps, bucket_convs)
    point_est = compute_point_estimate(
        floor=floor,
        pipeline_estimate=pipeline_est,
        seasonal_baseline=seasonal,
        horizon=horizon,
        pipeline_opp_count=int(row["total_open_opps"]),  # type: ignore[arg-type]
        is_peak=False,
        peak_multiplier=float(row["peak_multiplier"]),  # type: ignore[arg-type]
        params=p,
    )
    return {"point_est": point_est, "floor": floor, "pipeline_est": pipeline_est, "seasonal": seasonal}


def test_full_pipeline_t0() -> None:
    """End-to-end: T0 signal row → forecast within reasonable bounds."""
    row = make_signal_row(horizon=0, floor_orders=200)
    result = run_forecast_from_row(row)
    assert result["point_est"] > 0
    assert result["point_est"] >= result["floor"]
    # With these inputs, expect ~250 range
    assert 200 <= result["point_est"] <= 350


def test_full_pipeline_t2_low_floor() -> None:
    """T+2 with low floor leans on seasonal."""
    row = make_signal_row(horizon=2, floor_orders=55, open_opps=(0, 0, 30, 35))
    result = run_forecast_from_row(row)
    assert result["point_est"] >= 55
    # Seasonal dominates at T+2 (hw=0.45)
    assert result["seasonal"] > result["floor"]


def test_output_has_exactly_3_rows() -> None:
    """Full forecast should produce exactly 3 rows (one per horizon)."""
    rows = [make_signal_row(horizon=h) for h in range(3)]
    results = [run_forecast_from_row(r) for r in rows]
    assert len(results) == 3


def test_output_columns_present() -> None:
    """All expected keys are in the output dict."""
    row = make_signal_row()
    result = run_forecast_from_row(row)
    for key in ["point_est", "floor", "pipeline_est", "seasonal"]:
        assert key in result
```

- [ ] **Step 2: Run tests**

```bash
python -m pytest tests/test_sql_signals.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 3: Run full test suite**

```bash
python -m pytest tests/ -v
```

Expected: All tests across all files PASS.

- [ ] **Step 4: Run full linting + type checking**

```bash
python -m ruff check src/ tests/
python -m mypy src/
```

Expected: No errors.

- [ ] **Step 5: Commit**

```bash
git add tests/test_sql_signals.py
git commit -m "test: integration tests for full forecast pipeline"
```

---

## Task 13: Final Verification

**Files:** None (verification only)

- [ ] **Step 1: Run full test suite**

```bash
python -m pytest tests/ -v --tb=short
```

Expected: All tests PASS.

- [ ] **Step 2: Run linting**

```bash
python -m ruff check src/ tests/
```

Expected: No errors.

- [ ] **Step 3: Run type checking**

```bash
python -m mypy src/
```

Expected: No errors.

- [ ] **Step 4: Verify file structure matches spec**

```bash
ls -R src/ sql/ tests/ output/
```

Expected: All files from the File Structure table exist.

- [ ] **Step 5: Verify pre-deploy gate checklist items that don't need Snowflake**

From spec Section 9.7:
- [x] `python -m pytest tests/ -v` — all green
- [x] `python -m ruff check src/` — no errors
- [x] `python -m mypy src/` — no errors
- [ ] `sql/data_quality_checks.sql` — requires Snowflake (manual)
- [ ] Backtest window — requires Snowflake (manual)
- [x] Output schema matches spec
- [x] Forecast log CSV headers correct

- [ ] **Step 6: Final commit**

```bash
git add -A
git commit -m "chore: final verification - all tests, linting, and type checks pass"
```

---

## Execution Notes

**Tasks 1-5, 11-12** can be fully executed locally (no Snowflake needed).

**Tasks 6-7** produce SQL files that need manual testing against Snowflake:
```bash
# Test base_signals.sql against Snowflake manually:
# Set :eval_date = CURRENT_DATE, :backtest_mode = FALSE
# Verify: returns exactly 3 rows, all columns populated, floor > 0 for T0
```

**Tasks 8-10** require Snowflake credentials. Test by:
```bash
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_WAREHOUSE=...
python src/backtest.py --window 7
python src/optimize.py --eval-window 7 --validate-window 30
```

**After Snowflake testing passes**, run the Phase A vs Phase B shadow comparison (spec Section 9.4) before cutting over to production.

**Hourly scheduling:** Set up a cron job or Airflow DAG to run `python src/forecast.py --date today --hour $(date +%H)` every hour. The backtest and optimizer can still run once daily (e.g., at 6 AM before business hours).
