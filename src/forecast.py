"""Core forecast blending logic.

See spec: docs/superpowers/specs/2026-03-27-order-forecast-design.md (Section 4)
"""
from typing import Any

import numpy as np
from numpy.typing import NDArray

FALLBACK_HORIZON_WEIGHT: float = 0.15


def compute_pipeline_estimate(
    bucket_opps: list[int],
    bucket_convs: list[float],
) -> float:
    """Sum of (open_opps_in_bucket * bucket_conversion_rate)."""
    return sum(
        o * c for o, c in zip(bucket_opps, bucket_convs, strict=True)
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
