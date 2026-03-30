# PnM Intracity Order Forecast — Model Upgrade Proposal

**Prepared:** 2026-03-27
**Audience:** Operations, Sales, Product, Engineering
**Status:** Approved for implementation — pending assumption validation (see Section 5)

---

## Executive Summary

Our current order forecast for Packers & Movers intracity moves produces a once-daily 3-day demand range. The planning team uses the midpoint of this range to size truck/crew capacity and staffing. The model works — but has a structural flaw: its uncertainty range is **widest on the day we need it most (today)** and narrows as we look further out. This is backwards. The upgrade fixes this, moves to hourly runs for fresher numbers, and makes the model self-correcting through automated backtesting.

---

## 1. Current Model Overview

### What it does

Every morning, the model looks at the next 3 shifting dates and asks two questions:

1. **What does history say?** — Average orders for the same weekday over the last 10 weeks, and for the same date-of-month over the last 12 months.
2. **What does the live sales pipeline say?** — Count open opportunities for each date, and estimate how many will convert based on similar past days.

It then outputs a `floor` (lower) and `ceil` (upper) forecast range. If the date is a known peak day (e.g., month-end), it multiplies both by 1.25.

### How the range is constructed

```
floor = MIN(historical baseline, pipeline estimate)
ceil  = MAX(historical baseline, pipeline estimate)
```

The range is simply the gap between two independent signals. When they agree, the range is tight. When they disagree, it explodes.

### The core problem

The range measures **heuristic disagreement, not forecast uncertainty**. This causes it to behave backwards:

| Horizon | Why signals diverge | Resulting range |
|---------|-------------------|----------------|
| **Today (T0)** | Pipeline is fully populated; history is a stale average → large divergence | **~100%+ width** |
| **Tomorrow (T+1)** | Pipeline partially formed; gap is smaller | ~36% width |
| **Day-after (T+2)** | Pipeline sparse; both signals more similar | ~23% width |

In reality, we know the most about today (orders are already booked) — so the range *should* be narrowest for T0, not widest. The ops team compensates by using the midpoint, effectively ignoring the range entirely. This means the model is providing less signal than it appears to.

### Other known gaps

- **Peak dates are hardcoded:** The list of "special dates" is manually maintained and will expire in July 2026.
- **No accuracy tracking:** There is no record of how well the forecast has historically performed.
- **Single signal for pipeline:** All open opportunities are treated identically regardless of how far in advance they were created.

---

## 2. Version 2 — What Changes and Why

V2 is a three-layer upgrade. Each layer builds on the previous and can be delivered independently.

---

### Layer 1 — Fixed Envelope (SQL-only, no new infrastructure)

**What changes:** Replace the `min/max` heuristic range with a range derived from actual historical forecast errors.

**How it works:**
- Run the model retroactively against the last 7 days.
- Compute the percentage error on each day: `(actual − predicted) / predicted`.
- Pool these errors across all days and derive the 15th and 85th percentiles.
- Use those percentiles to construct a range around tomorrow's forecast: `lower = forecast × (1 + P15)`, `upper = forecast × (1 + P85)`.

**Why this is better:** The range now reflects how uncertain the model actually is, not how much its two signals disagree. It will be narrower when the model is historically accurate, and wider when it is not — which is the correct behavior.

**What also changes:** Run cadence moves from once-daily (morning) to **hourly**, so ops gets progressively fresher numbers throughout the day.

**What stays the same:** The underlying SQL structure and Snowflake connection are unchanged.

---

### Layer 2 — Funnel Forecast (SQL + Python)

**What changes:** Introduce a three-signal architecture that treats each horizon differently.

#### Signal 1: Confirmed Orders Floor

All orders already booked for the target date (regardless of status) are used as a **hard floor** — the forecast can never go below what is already in the system. No status filter is applied because order status data has unreliable fill rates historically.

| Horizon | Expected floor as % of final actuals |
|---------|--------------------------------------|
| Today (T0) | ~70–80% |
| Tomorrow (T+1) | ~40–60% |
| Day-after (T+2) | ~20–30% |

As the service date approaches, more orders are confirmed → floor rises → uncertain portion shrinks → range narrows naturally. This is the structural fix to the inversion problem.

#### Signal 2: Bucketed Pipeline Conversion

Rather than treating all opportunities the same, V2 groups them by how far in advance they were created relative to the moving date. All opportunities are included regardless of status (~90% of status values are null, making filtering counterproductive). Conversion = total orders / total opportunities per shifting date.

| Bucket | Lead time | Behavior |
|--------|-----------|---------|
| B0 | 0–1 day before | Late/urgent — high conversion rate |
| B1 | 2–3 days before | Standard — moderate conversion |
| B2 | 4–7 days before | Planned — lower conversion |
| B3 | 8+ days before | Early planners — lowest conversion |

Each bucket carries its own historically-derived conversion rate. The pipeline estimate is the sum of `(opps in bucket × bucket conversion rate)` across all four buckets. Note: majority of leads are created 1 day before the move, so B0 dominates. Bucketing may be simplified to a flat rate if backtesting shows no benefit.

#### Signal 3: Seasonal Baseline

A blend of two historical averages:
- **10-week weekday average:** Same weekday, last 10 weeks (short-term seasonality)
- **12-month date-of-month average:** Same calendar date, last 12 months (long-term calendar behavior)

Peak dates are **excluded** from this baseline calculation so they don't inflate "normal" expectations.

#### Blending — Horizon-Aware Weights

The model trusts pipeline data more when forecasting today (information is richest) and trusts the seasonal baseline more when forecasting two days out (pipeline is thinner):

| Horizon | Pipeline weight | Seasonal weight |
|---------|----------------|----------------|
| T0 (today) | 85% | 15% |
| T+1 (tomorrow) | 65% | 35% |
| T+2 (day-after) | 45% | 55% |

These weights are optimizable — the backtesting harness (Layer 3) finds the best values automatically.

#### Peak Date Handling

Replace the hardcoded special-dates list with a **static holiday calendar** seeded from the existing V1 SQL dates, mapped to named Indian holidays (Diwali, Holi, Republic Day, etc.) with a **±1 day adjacency window**. This captures the PnM-specific behavior where moving demand peaks the day before or after major holidays. Month-end dates (last 2 days) are also included programmatically. The uplift multiplier is data-derived from historical peak-vs-normal ratios, not a fixed 25%.

---

### Layer 3 — Backtesting Harness

**What changes:** Introduce an automated accuracy loop that runs every morning alongside the forecast.

**How it works:**
1. Re-simulate yesterday's forecasts at each hour using only data that was available at that hour (as-of filtering prevents hindsight).
2. Compare to actual orders.
3. Recompute weights and error percentiles on a rolling 7-day window.
4. Log all forecast outputs, actuals, and parameters for audit.

**Why this matters:**
- The model becomes self-correcting. If conversion rates shift (e.g., new competition, seasonal change in behavior), the model adapts within 7 days without manual intervention.
- Accuracy metrics (MAE, MAPE, range coverage %) become visible to the team for the first time.
- Parameter changes leave an audit trail.

**Target accuracy thresholds:**

| Horizon | MAPE target | Range width target | Coverage target |
|---------|------------|-------------------|----------------|
| T0 | < 10% | < 20% of point estimate | ≥ 65% |
| T+1 | < 15% | < 30% of point estimate | ≥ 65% |
| T+2 | < 20% | < 40% of point estimate | ≥ 65% |

---

## 3. Strategic Rationale

| Current behavior | V2 behavior | Business impact |
|-----------------|-------------|----------------|
| Range widest today (~100%) | Range narrowest today (~15–20%) | Ops can trust the number for same-day capacity decisions |
| Range measures signal disagreement | Range measures actual forecast error | Meaningful confidence intervals for planning |
| Hardcoded peak dates (expires Jul '26) | Static holiday calendar with ±1 day adjacency + month-end | No expiry; captures PnM-specific demand shoulder around holidays |
| All pipeline opps treated equally | Bucketed by lead time with separate conversion rates | More accurate pipeline estimate; less dilution from early-stage opps |
| Status filters on orders/opps (unreliable) | No status filters — all records included | Avoids noise from ~90% null status data |
| Once-daily morning run | Hourly runs | Fresher numbers; floor tightens throughout the day as orders land |
| No accuracy visibility | Hourly accuracy log with MAE/MAPE/coverage | Team can see if the forecast is degrading |
| Weights never change | Auto-optimized on rolling 7-day backtest | Model adapts to business changes without manual tuning |
| Single SQL query | SQL + Python blending layer | Enables richer logic without rewriting data infrastructure |

---

## 4. Delivery Roadmap

| Phase | Deliverable | Description |
|-------|------------|-------------|
| **A (Quick fix)** | Layer 1 only | Fix range inversion using error percentiles. Pure SQL. Days, not weeks. |
| **B (Full model)** | Layers 1 + 2 | Funnel forecast with confirmed-orders floor, bucketed pipeline, seasonal blend. SQL + Python. |
| **C (Self-optimizing)** | Layer 3 added | Backtesting harness, rolling weight optimization, accuracy dashboard. Runs alongside B. |

The plan is to ship A, evaluate B's accuracy for 5–7 days to tune weights and logic, then use C's optimization loop to lock in the best configuration. Once B+C is stable, it becomes the production model.

---

## 5. Assumptions for Validation

The following assumptions are embedded in the design. The team should validate these before or during implementation to avoid surprises.

### Data Assumptions

| # | Assumption | Why it matters | How to validate |
|---|-----------|---------------|----------------|
| D1 | All order statuses are safe to include in the floor (no status filter) | If a large fraction of orders are in a "test" or "duplicate" status, floor will be over-counted | Run `SELECT status, COUNT(*) FROM pnm_application.orders GROUP BY 1` and review the distribution |
| D2 | All opportunities are safe to include (no status filter; ~90% null) | If status fill rate improves and non-viable opps become identifiable, flat conversion rate may be too diluted | Monitor status fill rate quarterly; revisit filtering once > 50% fill rate |
| D3 | `created_at` on orders and opportunities is in UTC and reliably populated | The backtest as-of filter depends on this timestamp; gaps = data leakage | Check for NULLs and compare to IST business hours |
| D4 | Conversion rates are stable enough across weekday groups | The model pools weekdays for conversion matching; if Monday and Friday behave very differently, per-weekday rates would be better | Plot historical conversion by weekday from `daily_opps` / `daily_orders` data |
| D5 | Past service dates have fully settled conversions (no censoring buffer needed) | If orders are ever backdated to past shifting dates (e.g., manual entry), conversion rates for recent dates could be understated | Check `MAX(created_at - shifting_ts)` for orders created after the move date |

### Business Assumptions

| # | Assumption | Why it matters | How to validate |
|---|-----------|---------------|----------------|
| B1 | Ops team uses the **midpoint** of (lower, upper) as the planning number | The model is designed for this; if they use upper as a safety buffer, behavior changes | Confirm directly with ops team |
| B2 | Hourly runs provide meaningful improvement over daily | If most orders are placed in a narrow window (e.g., 10 AM–2 PM), many hourly runs will show no change; compute cost increases 24× | Check order creation time distribution — if > 80% of orders are in a 4-hour window, consider 4-hourly instead |
| B3 | Intracity is the right scope — intercity moves don't need separate tracking right now | Model is built for `shifting_type = 'intra_city'` only; intercity is excluded | Confirm scope with product/ops |
| B4 | ~200–300 orders/day is the expected volume range | Percentile-based ranges need sufficient sample size; very low-volume days may behave differently | Pull last 90 days of daily order counts for intracity |

### Model Assumptions

| # | Assumption | Why it matters | How to validate |
|---|-----------|---------------|----------------|
| M1 | 10 weeks of history is enough for the seasonal baseline | If seasonality shifts faster (e.g., new city launch, major pricing change), 10 weeks is too long | Review MBR data for structural breaks in demand |
| M2 | The 4-bucket structure (0-1, 2-3, 4-7, 8+ days) aligns with actual lead-time behavior | If most opps are created 1–2 days before, B0+B1 dominate and B2/B3 have no signal | Plot opp creation lead-time distribution from historical data |
| M3 | Peak multiplier derived from last 12 months is representative | If this year's peak demand is structurally higher (new city, better brand), a 12-month average understates uplift | Compare recent peak actuals to historical peak averages |

---

## 6. What We Are Not Changing

To keep scope focused:

- **Intercity moves:** Out of scope. Model covers intracity only.
- **Nano packages:** Excluded from all signals and forecasts (as today).
- **Forecast horizon:** Still 3 days (T, T+1, T+2). Extending to 7 days is noted for Phase C.
- **Underlying data infrastructure:** No new tables or pipelines required. All queries run on existing `prod_curated.pnm_application` schema.

---

## 7. Pre-Deploy Gate

Before going live, the following must pass:

- [ ] All unit tests pass (`pytest tests/`)
- [ ] SQL data quality pre-flight checks pass (4 checks: orders recency, opps exist, date outliers, conversion sanity)
- [ ] Backtest MAE on most recent 7 days meets horizon targets
- [ ] Range coverage ≥ 65% on most recent 7 days
- [ ] No parameter hit boundary of allowed optimization range
- [ ] Assumptions D1, D2, B1, B2 confirmed (high-confidence, low-effort checks)
- [ ] Shadow mode run for 3+ days with output reviewed by ops team

---

*For detailed technical architecture, see `docs/superpowers/specs/2026-03-27-order-forecast-design.md`.*
*For implementation task breakdown, see `docs/superpowers/plans/2026-03-27-order-forecast-plan.md`.*
