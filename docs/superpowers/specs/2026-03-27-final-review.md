# Final Spec Review: 2026-03-27-order-forecast-design.md

**Reviewer:** Claude Opus 4.6 (Senior Code Reviewer)
**Date:** 2026-03-27
**Verdict:** APPROVED with 1 minor fix applied (see below)

---

## Issue Found and Fixed

**[Important] Section cross-references pointed to wrong section.**
Lines 393, 477, and 484 referenced "Section 11" for verification/quality checks. The actual Verification & Quality Strategy is Section 9. Section 11 is Risks & Mitigations. All three references have been corrected to "Section 9."

---

## Review Summary

### Internal Consistency: PASS
- Definitions (Section 3) align with SQL filters and backtest logic throughout.
- `status NOT IN (3,4)` for open opportunities is consistent between Section 3 definition and Section 6 backtest SQL.
- `status != 'cancelled'` for confirmed orders is consistent across Section 3, Signal 1 SQL, and Decision D10.
- Peak date deterministic definition (Section 3) matches exclusion from seasonal baseline (Section 3 Signal 3) and Decision D7.
- Censoring buffer of 2 days is consistent across Section 3 definition, Section 5 PARAMS, Section 6 backtest SQL, and Decision D11.
- Floor enforcement appears correctly in both point estimate (Section 4) and range lower bound (Section 4).
- Percentile pooling description (Section 4) matches Decision D9 rationale.
- Error percentile safe denominator `max(predicted, 1)` consistent in Section 4 formula and Decision D15.
- `ten_week_weight` range [0.3, 0.8] consistent in Section 5 PARAMS and Decision D8.
- File layout (Section 7) matches execution commands and test structure (Section 9).
- Shadow mode described in Section 8 matches Section 9.4.
- Rollback plan (Section 8) references anomaly alerts correctly (after fix).

### Logic Gaps / Edge Cases: PASS (none blocking)
- Empty pipeline fallback (Section 3) correctly shifts weight to seasonal when `pipeline_opp_count < 5`.
- The spec correctly notes the 12-month growth bias limitation and provides mitigation (optimizer range + Phase C deferral).
- Double-counting guard between floor and pipeline is addressed: open opps exclude status 4 (converted), so confirmed orders in floor should not also appear as open pipeline.

### Statistical Correctness: PASS
- Two-stage optimization avoids the circular dependency between weights and percentiles.
- Percentile pooling across all weekdays per horizon gives ~56 data points (8 weeks x 7 days) -- adequate for P15/P85 estimation.
- Safe denominator `max(predicted, 1)` prevents division-by-zero without materially distorting error percentages at normal volumes (150-400 orders/day).
- Coverage target of 65% with P17.5/P82.5 is internally consistent.

### Verification Section (Section 9): PASS -- comprehensive
- Pre-flight SQL checks would catch: data pipeline outages, missing opportunities, date outliers, conversion rate anomalies.
- Unit tests cover: basic blending, floor enforcement, empty pipeline fallback, peak multiplier, double-count guard, range construction, near-zero denominator, sample size adequacy.
- Integration tests use frozen fixtures -- deterministic and reproducible.
- Shadow mode prevents blind deployments.
- Production monitoring covers: output sanity bounds, range explosion, floor>estimate logic bug, stale data, drift detection.
- The daily forecast log with retroactive actuals enables long-term accuracy tracking.
- Pre-deploy gate checklist is thorough (10 items).

### Decision Log (Section 10): PASS -- 15 well-documented entries
- All major design choices have options considered and rationale.
- No undocumented decisions identified. The decisions cover: scope, range method, interval width, optimization window, SQL/Python boundary, bucket boundaries, peak dates, growth adjustment, pooling strategy, order status filter, censoring buffer, opportunity definition, optimization approach, verification strategy, and denominator safety.

### Suggestions (non-blocking)
- Consider adding a Decision D16 for the choice of 65% coverage target (vs 70% or 80%) if challenged by stakeholders.
- The anomaly bounds (50, 600) could benefit from being parameterized in config.py rather than hardcoded in monitoring logic, for easier quarterly recalibration.
