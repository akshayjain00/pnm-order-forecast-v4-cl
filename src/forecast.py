"""Core forecast blending logic.

See spec: docs/superpowers/specs/2026-03-27-order-forecast-design.md (Section 4)
"""
from __future__ import annotations

from typing import Any

import numpy as np
from numpy.typing import NDArray

from src.config import HybridParams

FALLBACK_HORIZON_WEIGHT: float = 0.15


def compute_pipeline_estimate(
    bucket_opps: list[int],
    bucket_convs: list[float],
) -> float:
    """Sum of (open_opps_in_bucket * bucket_conversion_rate)."""
    return sum(
        o * c for o, c in zip(bucket_opps, bucket_convs)
    )


def compute_seasonal_baseline(
    ten_week_avg: float,
    twelve_month_avg: float,
    ten_week_weight: float,
) -> float:
    """Blend 10-week and 12-month signals into seasonal baseline.

    See spec Section 3, Signal 3.
    """
    return (
        ten_week_weight * ten_week_avg
        + (1.0 - ten_week_weight) * twelve_month_avg
    )


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

    On non-peak days with conservative mode enabled, uses
    min(pipeline_total, seasonal) to clip over-predictions (inspired
    by V1's approach).  On peak days, uses the standard weighted blend.

    Returns the final forecast value, guaranteed >= floor.
    """
    # Pipeline estimate already includes expected conversions from all opps,
    # including those that became confirmed orders (floor). Use max to avoid
    # double counting — the floor is a realized lower bound, not additive.
    pipeline_total = max(float(floor), pipeline_estimate)

    # Determine effective horizon weight
    min_opps: int = int(params["min_pipeline_opps"])
    if pipeline_opp_count < min_opps:
        effective_hw = FALLBACK_HORIZON_WEIGHT
    else:
        effective_hw = float(params[f"horizon_weight_T{horizon}"])

    conservative = bool(params.get("conservative_nonpeak", False))

    if conservative and not is_peak:
        # Conservative mode: use the lower of pipeline vs seasonal
        # to clip over-predictions on normal days.
        estimate = min(pipeline_total, seasonal_baseline)
    else:
        # Standard weighted blend
        estimate = (
            effective_hw * pipeline_total
            + (1.0 - effective_hw) * seasonal_baseline
        )

    # Peak multiplier
    if is_peak:
        capped = min(
            peak_multiplier,
            float(params.get("peak_multiplier_cap", 1.0)),
        )
        estimate *= capped

    # Floor enforcement
    return max(estimate, float(floor))


def compute_error_pct(actual: float, predicted: float) -> float:
    """Compute relative error with safe denominator (spec Section 4)."""
    return (actual - predicted) / max(predicted, 1.0)


def compute_range(
    point_estimate: float,
    floor: int,
    p_lower: float,
    p_upper: float,
) -> tuple[float, float]:
    """Construct prediction interval from error percentiles.

    Lower bound is guaranteed >= floor. See spec Section 4.
    """
    lower = point_estimate * (1.0 + p_lower)
    upper = point_estimate * (1.0 + p_upper)
    lower = max(lower, float(floor))
    return lower, upper


def calibrate_percentiles(
    residuals: NDArray[np.floating[Any]],
    target_coverage: float = 0.65,
) -> tuple[float, float]:
    """Find symmetric percentile bounds achieving target coverage.

    See spec Section 6 Stage 2.
    Uses centered interval: [P((1-coverage)/2), P((1+coverage)/2)]
    """
    lower_pctl = (1.0 - target_coverage) / 2.0
    upper_pctl = (1.0 + target_coverage) / 2.0
    p_lower = float(np.percentile(residuals, lower_pctl * 100))
    p_upper = float(np.percentile(residuals, upper_pctl * 100))
    return p_lower, p_upper


def compute_hybrid_estimate(
    floor: int,
    pipeline_estimate: float,
    seasonal_baseline: float,
    horizon: int,
    pipeline_opp_count: int,
    is_peak: bool,
    booked_orders: int | None,
    booked_share: float | None,
    params: HybridParams,
    peak_seasonal_baseline: float | None = None,
) -> float:
    """Hybrid blending engine: V2 brain + V4 body.

    Key differences from V4:
    - NEVER uses min(pipeline, seasonal) — always blends
    - Adds nowcast for T+0: floor / booked_share
    - Uses V2's higher T+0 weight (0.80 vs V4's 0.70)

    Key differences from V2:
    - Uses bucketed pipeline (4 buckets) instead of single conversion
    - Uses V4's T+2 weight (0.55 vs V2's 0.20) — pipeline is informative
    - Nowcast applied BEFORE peak multiplier (bug fix from architecture review)

    peak_seasonal_baseline:
        When provided and is_peak=True and horizon >= 1, this replaces the
        regular seasonal_baseline for blending.  The regular seasonal baseline
        excludes peak-day history (by design, to avoid contaminating normal days),
        so for peak target dates at T+1/T+2 it is structurally biased low.
        peak_seasonal_baseline is computed from last-12-months of same-DOW
        month-boundary days (V2-style k-nearest matching).

    Returns the final forecast, guaranteed >= floor.
    """
    # Step 1: Pipeline floor enforcement (avoids double-counting floor orders)
    pipeline_total = max(float(floor), pipeline_estimate)

    # Step 2: Determine effective weight
    if pipeline_opp_count < params.min_pipeline_opps:
        effective_weight = params.sparse_pipeline_weight
    else:
        effective_weight = params.horizon_weights[horizon]

    # Step 3: Select seasonal signal
    # For peak days at T+1/T+2, prefer the peak-specific baseline (V2 insight).
    # Falls back to regular seasonal if peak baseline unavailable.
    if is_peak and horizon >= 1 and peak_seasonal_baseline is not None:
        effective_seasonal = peak_seasonal_baseline
    else:
        effective_seasonal = seasonal_baseline

    # Step 4: ALWAYS blend — never clip to single signal (key V2 insight)
    blend = (
        effective_weight * pipeline_total
        + (1.0 - effective_weight) * effective_seasonal
    )

    # Step 5: Nowcast for T+0 (from V2) — BEFORE peak adjustment
    # At 9 AM, booked_share ≈ 0.70-0.80. nowcast = floor/share projects
    # the partial-day floor to a full-day estimate.
    if horizon == 0 and booked_orders is not None and booked_share is not None:
        # Clamp to (0.01, 1.0] to guard against DQ issues (share=0 → division error)
        clamped_share = max(min(booked_share, 1.0), 0.01)
        nowcast = max(booked_orders / clamped_share, float(booked_orders))

        if clamped_share >= params.full_switch_share:
            # High confidence in floor: use nowcast directly
            blend = nowcast
        else:
            # Progressive blending: lerp toward nowcast as orders arrive
            nowcast_weight = clamped_share / params.full_switch_share
            blend = nowcast_weight * nowcast + (1.0 - nowcast_weight) * blend

    # Step 6: Peak multiplier — AFTER nowcast so multiplier scales the
    # nowcast-blended result consistently (critical ordering from arch review)
    if is_peak:
        blend *= min(params.peak_multiplier, params.peak_multiplier_cap)

    # Step 7: Floor enforcement
    return max(blend, float(floor))


def compute_hybrid_range(
    point_estimate: float,
    floor: int,
    horizon: int,
    is_peak: bool,
    params: HybridParams,
) -> tuple[float, float]:
    """Compute stratified prediction interval for hybrid model.

    Uses separate peak/non-peak percentiles per horizon (from V4, recalibrated).
    Lower bound is guaranteed >= floor.
    """
    suffix = "peak" if is_peak else "nonpeak"
    p_lower = getattr(params, f"range_lower_pctl_T{horizon}_{suffix}")
    p_upper = getattr(params, f"range_upper_pctl_T{horizon}_{suffix}")
    lower = max(point_estimate * (1.0 + p_lower), float(floor))
    upper = max(point_estimate * (1.0 + p_upper), lower)
    return lower, upper
