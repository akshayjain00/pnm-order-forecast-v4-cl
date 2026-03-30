# Model Upgrade Proposal: PnM Order Forecast Enhanced V2

**Date:** 2026-03-27  
**Prepared for:** Wider PnM, Ops, Sales, and Data stakeholders  
**Prepared by:** Lead Data Science Product framing based on the current SQL logic, approved V2 design, implementation plan, and review notes

---

## Executive Summary

The current PnM forecast is a practical first-generation heuristic, but its output range is becoming difficult to use operationally. It combines historical patterns with current opportunity volume, then uses the disagreement between those two signals as the forecast band. That means the range often gets wider exactly when we should feel more confident, especially for same-day planning.

Enhanced V2 keeps the parts that are working, but changes the core logic in three important ways:

- It starts with **confirmed orders already booked** for each shifting date.
- It forecasts the **remaining upside from the open pipeline** using better-defined conversion logic.
- It builds the forecast range from **historical model error**, not from disagreement between two heuristics.

The result should be a forecast that is easier for operations to trust: more stable, more interpretable, and better aligned with how order certainty actually improves as the service date gets closer.

---

## Current Model Overview

### What the current model does today

The current SQL model produces a **3-day forecast** for packers and movers service dates. For each target date, it asks two questions:

- What do similar historical dates usually deliver?
- Given the opportunities currently sitting on that date, how many orders should convert?

It then outputs a lower and upper bound for each shifting date.

### How the current model works in plain language

1. **Forecast horizon**
- It forecasts demand for `today`, `tomorrow`, and `day after tomorrow`.

2. **Historical seasonal baseline**
- It looks at the same weekday over the last 10 weeks.
- It also looks at the same date-of-month over the last 12 months.
- It takes the more conservative of those two historical views as the baseline.

3. **Pipeline-based estimate**
- It counts opportunities currently mapped to each service date.
- It estimates how many of those opportunities should convert based on historical conversion from similar weekdays and similar opportunity-volume days.
- More recent historical dates are weighted more heavily.

4. **Forecast band construction**
- It compares the historical baseline and the pipeline estimate.
- The lower of the two becomes the floor.
- The higher of the two becomes the ceiling.

5. **Peak date uplift**
- If the date is on a manually maintained peak-date list, both bounds are uplifted by 25%.

### What is good about the current model

- It combines both **history** and **live pipeline**, so it is not purely backward-looking.
- It is simple enough to run in SQL and easy to operationalize.
- It gives a range rather than a single hard number, which is directionally useful for planning.

### Where the current model falls short

- The range is often **too wide to be actionable**.
- The range can get **wider for same-day forecasts**, even though certainty should improve as orders get locked in.
- The band reflects **disagreement between two heuristics**, not true forecast uncertainty.
- Peak handling is based on a **hardcoded list**, which is brittle and requires manual upkeep.
- The current approach is not explicitly anchored on **confirmed booked orders**, which is the most trustworthy same-day signal.

---

## Why We Are Upgrading

The wider team is using the midpoint of the forecast range as the planning number for truck, crew, and support staffing. That makes two things especially important:

- The point estimate must be credible.
- The range must represent real uncertainty, not model confusion.

Under the current setup, the range expands when the pipeline signal and history disagree. In practice, that punishes the exact cases where current demand is shifting away from historical norms. Instead of helping the team decide, the forecast becomes harder to trust.

Enhanced V2 is designed to fix that behavior while still staying operationally simple.

---

## Version 2 Enhancements

### Summary of what changes

- Narrow scope to **intracity** orders only for the initial rollout.
- Introduce a **confirmed-orders floor** for each target date.
- Replace one pooled pipeline estimate with **bucketed pipeline conversion** by days-before-service.
- Replace the conservative seasonal minimum with a **weighted seasonal blend**.
- Replace the hardcoded peak list with a **deterministic peak-date rule plus data-driven multiplier**.
- Replace the current min/max band with a **historical-error-based confidence interval**.
- Add a **backtesting and optimization loop** so weights and ranges can be tuned with evidence.
- Add **data quality checks, shadow mode, monitoring, and rollback paths** before full cutover.

### Major changes, rationale, and expected value

| Major change | What changes in V2 | Logical reasoning | Expected business / technical value |
|---|---|---|---|
| **1. Confirmed-orders floor** | The forecast starts with already booked non-cancelled orders for each shifting date. | Orders already booked are the most reliable demand signal. As the service date gets closer, more of final demand is known rather than guessed. | Improves trust, prevents under-forecasting below already committed volume, and naturally tightens the forecast as the date approaches. |
| **2. Funnel-based pipeline estimate** | Open opportunities are split into buckets based on how many days remain before service, and each bucket gets its own conversion rate. | A lead created 0-1 days before service behaves very differently from one created 8+ days before service. Treating them the same hides useful signal. | Makes the pipeline estimate more behaviorally accurate and more interpretable for business users. |
| **3. Open-opportunity cleanup** | Only opportunities that are still open are counted; converted and dead/closed opportunities are excluded. | Closed leads will not convert, and converted leads should already be reflected in confirmed orders. Counting them distorts conversion math. | Reduces double counting and improves forecast quality by using a cleaner demand denominator. |
| **4. Resolved-date censoring** | Historical conversion rates are calculated only on service dates old enough for conversion to have settled. | Very recent service dates still have in-flight conversions, which can bias conversion rates downward. | Produces more stable and less biased conversion estimates. |
| **5. Weighted seasonal baseline** | Instead of taking the lower of the 10-week and 12-month signals, V2 blends them using tunable weights. | The current conservative minimum throws away information. A weighted blend is a better way to combine short-term recency and longer-term calendar effects. | Improves point estimate quality while keeping the model transparent and tunable. |
| **6. Horizon-aware blending** | The model uses different weights for T0, T+1, and T+2 when combining pipeline and seasonal signals. | Same-day pipeline and floor should matter more than day-after pipeline. The value of each signal changes by horizon. | Produces a forecast that matches how information quality changes over time. |
| **7. Empty-pipeline fallback** | If the pipeline is unusually sparse, the model leans more heavily on seasonality. | Very small pipeline counts can be noise, outages, or edge-case data rather than true low demand. | Makes the system more robust to sparse data and operational anomalies. |
| **8. Peak-date redesign** | Hardcoded peak dates are replaced with deterministic rules: month-end peaks and holiday-driven peaks, then uplift is learned from historical peak-vs-normal behavior. | Manual lists are fragile. Peak logic should be repeatable and not depend on ad hoc updates. | Reduces maintenance overhead and improves consistency across future periods. |
| **9. Error-based range construction** | The lower and upper bounds are calculated from historical forecast errors for each horizon. | Forecast ranges should reflect how wrong the model has historically been, not how much two internal heuristics disagree. | Produces tighter, more meaningful ranges and improves planning usability. |
| **10. Backtesting and optimization** | The model is replayed on recent historical mornings to measure MAPE, range width, bias, and coverage before rollout. | Forecast quality should be demonstrated, not assumed. Tuning without backtesting is guesswork. | Creates a measurable path to better accuracy and gives stakeholders confidence before cutover. |
| **11. Verification, shadow mode, and rollback** | V2 is introduced in stages, compared side by side with the current model, and rolled back if it underperforms. | Forecast models affect daily operations. Even a good design needs controlled rollout and safety checks. | Lowers deployment risk and builds trust with Ops and business users. |

---

## Strategic Rationale

### Why this design is a better fit for the business

- **It aligns with operational reality.**
  As the service date gets closer, more of the final order count is already known. V2 is explicitly built around that idea.

- **It turns the range into a planning tool.**
  The current band is partly a disagreement indicator. V2 redefines the band as an uncertainty indicator based on actual model performance.

- **It improves transparency without overcomplicating the system.**
  SQL still does the heavy data work. Python handles lightweight blending, backtesting, and optimization. That keeps the system explainable and scalable.

- **It creates a disciplined path from heuristic to evidence-backed forecasting.**
  Instead of replacing the model with a black-box ML solution, V2 upgrades the heuristic in a way the team can inspect, validate, and tune.

### Expected business value

- Better staffing and truck/crew planning for intracity moves.
- More reliable same-day and next-day planning numbers.
- Less operational noise from very wide or unstable ranges.
- Faster diagnosis when forecast quality degrades because metrics and monitoring are built in.
- A stronger foundation for future upgrades such as city-level forecasts or ML-based quantile models.

---

## Assumptions for Validation

The following assumptions should be explicitly validated with the wider team before implementation or final sign-off.

### Business assumptions

- The first production scope should be **intracity only**, because intercity volume is materially lower and likely needs separate treatment.
- The main consumer still wants **one planning number plus a range**, and the point estimate should be the primary planning input.
- A roughly **70% prediction interval** is the right balance between usefulness and safety for operations.
- The business is comfortable treating **last two days of the month** and approved holiday periods as peak drivers.

### Data and logic assumptions

- `orders.status != 'cancelled'` is the right business definition for a confirmed order floor.
- `opportunities.status NOT IN (3, 4)` correctly captures live opportunities and excludes both dead and already converted leads.
- `shifting_requirements.shifting_ts` is the right canonical service date for both opportunities and orders.
- A **2-day settling buffer** is enough to treat historical service dates as resolved for conversion-rate learning.
- Bucketing by **0-1, 2-3, 4-7, and 8+ days before service** matches real booking behavior well enough to start.
- The **10-week** and **12-month** historical windows are still representative despite business growth and mix shifts.

### Operational and implementation assumptions

- The morning forecast run should use a **9 AM IST cutoff** in backtesting and production comparisons.
- The team can support a lightweight **SQL + Python** architecture rather than keeping everything in SQL.
- Forecast outputs can be logged daily and joined to actuals later for monitoring and re-optimization.
- The team is willing to run **shadow mode** for several days before cutover.

### Constraints and dependencies

- Accurate status definitions across `orders`, `opportunities`, and `shifting_requirements` are critical. Any status misuse will distort the floor or pipeline.
- Peak-date logic depends on a reliable and maintained **holiday calendar**.
- Conversion logic depends on enough historical intracity volume to estimate bucket-level conversion rates robustly.
- Pre-flight data quality checks must be wired in so outages or missing data do not silently produce bad forecasts.
- Snowflake access and lightweight Python execution need to be available in the production workflow.

---

## Key Questions for Stakeholder Validation

- Do we agree that the current model’s biggest problem is **range quality**, not just point-estimate quality?
- Do we agree that **confirmed booked orders** should anchor the forecast for the target date?
- Do we agree with the initial **intracity-only** scope for V2?
- Do we agree with the proposed definition of **open opportunity** and **confirmed order**?
- Do we want the forecast range to target roughly **65%-70% coverage**, or should it be tighter or more conservative?
- Do we agree that **month-end plus holiday logic** is the right first version of peak handling?
- Are there any operational events, campaigns, or market changes that make the last 5-7 days unrepresentative for optimization?

---

## Recommended Rollout Approach

### Phase A: Quick improvement in SQL

- Replace the current min/max envelope with a weighted point estimate.
- Use fixed horizon-based weights.
- Build the range from historical error percentiles.
- Add the order-status cleanup needed for a more trustworthy base signal.

**Why this matters:** It gives Ops a better forecast quickly, without waiting for the full V2 build.

### Phase B: Full Enhanced V2

- Add confirmed-order floor.
- Add bucketed pipeline conversion logic.
- Add optimized blending and calibrated intervals.
- Add logging, backtesting, validation, and monitoring.

**Why this matters:** It delivers the full product vision while keeping a safer bridge from current state to future state.

### Cutover safeguards

- Run old and new models in parallel before switching.
- Compare accuracy, range width, coverage, and bias.
- Only cut over if V2 is better or equal on the agreed metrics.
- Keep a rollback path available if performance degrades post-launch.

---

## Success Criteria

The V2 upgrade should be considered successful if it achieves most of the following:

- Better MAPE than the current model, especially for T0 and T+1.
- Meaningfully tighter ranges at comparable or better coverage.
- Forecast ranges that narrow as the service date approaches.
- No forecast output below already confirmed booked volume.
- Stable daily operations during shadow mode and post-launch.

---

## Bottom Line

The current model is a sensible first heuristic, but it uses a forecast band that does not represent real uncertainty well enough for operational planning. Enhanced V2 improves the model in the places that matter most: it anchors on confirmed orders, models the remaining pipeline more realistically, and turns the forecast range into an evidence-based planning tool.

This is not a jump to a black-box model. It is a disciplined upgrade to a more trustworthy heuristic system, with backtesting, monitoring, and rollback built in. If the team agrees with the assumptions called out above, V2 is a strong next step to improve planning confidence before moving to more advanced forecasting approaches later.






changes basis these changes need to incorporated as update and refinement in existing spec, design and implemetantion plan
  <inputs>
  1. Opportunity stages | `0=open`, `1=prospect`, `2=quoted`, `3=closed`, `4=converted`
  do hold information however currently status values is null for 90% of the rows and a better approach might be to consider all oportunities and treat then equally because earlier we had even poorer
  status fill rate for historical data and look at total orders and totla opportunities for conversion percentage sense
  2. the forecasting engine needs to run hourly instead of daily morning 9 am
  3. model needs to account for historical holidays and current year holidays when using historical data and live data trends
  4. we should store the following dates as holiday calendar you can map holidys and create a static calendar further enriched if needed and incorporate to further refine forecast as generally peaks happen 1 day before or after holidays like Diwali, Holi etc for packer and mover business
 special_dates AS ( SELECT column1::DATE AS dt FROM VALUES ('2026-01-27'),('2026-01-28'),('2026-01-29'),('2026-01-30'),('2026-01-31'), ('2026-02-01'),('2026-02-02'),('2026-02-06'),('2026-02-07'),('2026-02-08'), ('2026-02-13'),('2026-02-14'),('2026-02-15'),('2026-02-19'),('2026-02-20'), ('2026-02-21'),('2026-02-22'),('2026-02-23'),('2026-02-27'),('2026-02-28'), ('2026-03-01'),('2026-03-02'),('2026-03-03'),('2026-03-04'),('2026-03-06'), ('2026-03-07'),('2026-03-08'),('2026-03-13'),('2026-03-14'),('2026-03-15'), ('2026-03-19'),('2026-03-20'),('2026-03-21'),('2026-03-22'),('2026-03-26'), ('2026-03-27'),('2026-03-28'),('2026-03-29'),('2026-03-30'),('2026-03-31'), ('2026-04-01'),('2026-04-02'),('2026-04-03'),('2026-04-04'),('2026-04-05'), ('2026-04-10'),('2026-04-11'),('2026-04-12'),('2026-04-14'),('2026-04-15'), ('2026-04-17'),('2026-04-18'),('2026-04-19'),('2026-04-24'),('2026-04-25'), ('2026-04-26'),('2026-04-29'),('2026-04-30'),('2026-05-01'),('2026-05-02'), ('2026-05-03'),('2026-05-08'),('2026-05-09'),('2026-05-10'),('2026-05-15'), ('2026-05-16'),('2026-05-17'),('2026-05-22'),('2026-05-23'),('2026-05-24'), ('2026-05-29'),('2026-05-30'),('2026-05-31'),('2026-06-01'),('2026-06-02'), ('2026-06-05'),('2026-06-06'),('2026-06-07'),('2026-06-12'),('2026-06-13'), ('2026-06-14'),('2026-06-19'),('2026-06-20'),('2026-06-21'),('2026-06-26'), ('2026-06-27'),('2026-06-28'),('2026-06-29'),('2026-06-30'),('2026-07-01'), ('2026-07-02'),('2026-07-03'),('2026-07-04'),('2026-07-05'),('2026-07-10'), ('2026-07-11'),('2026-07-12'),('2026-07-16'),('2026-07-17'),('2026-07-18'), ('2026-07-19'),('2026-07-24'),('2026-07-25'),('2026-07-26'),('2026-07-29'), ('2026-07-30'),('2026-07-31') )
  the model needs to run hourly so i want you to evlauate whether starts with **confirmed orders already booked** for each shifting date or no consideration to confirmed orders already booked which is a better approach
`orders.status != 'cancelled'` is the not right business definition for a confirmed order floor. need to consider all order status.
Bucketing by **0-1, 2-3, 4-7, and 8+ days before service** matches real booking behavior well enough to start. Majority of the leads are created one day before, Might need normaisation or standardisation. Can also be skipped consdeting this bucketing is partially manipulated and behavior is shaped by pnm app 
 Resolved-date censoring** | Historical conversion rates are calculated only on service dates old enough for conversion to have settled. | Very recent service dates still have in-flight conversions, which can bias conversion rates downward. Since we are doing this at shited_ts and not created thus Very recent service dates still have in-flight conversions doesnot hold tru according to my understanding
  </inputs>


