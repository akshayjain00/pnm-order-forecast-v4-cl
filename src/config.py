"""All forecast parameters in one place.

See spec: docs/superpowers/specs/2026-03-27-order-forecast-design.md (Section 5)
"""
import calendar
import dataclasses
import datetime
from typing import Final

# --- Indian public holidays for 2026 (national + gazetted long weekends) ---
# Update this list annually. Adjacency (±1 day) is applied automatically.
HOLIDAYS: Final[list[datetime.date]] = [
    datetime.date(2026, 1, 26),   # Republic Day
    datetime.date(2026, 3, 14),   # Holi
    datetime.date(2026, 3, 30),   # Holi (regional)
    datetime.date(2026, 4, 2),    # Ram Navami
    datetime.date(2026, 4, 3),    # Mahavir Jayanti
    datetime.date(2026, 4, 14),   # Ambedkar Jayanti
    datetime.date(2026, 5, 1),    # May Day
    datetime.date(2026, 8, 15),   # Independence Day
    datetime.date(2026, 10, 2),   # Gandhi Jayanti
    datetime.date(2026, 10, 20),  # Dussehra
    datetime.date(2026, 11, 9),   # Diwali
    datetime.date(2026, 11, 10),  # Diwali day 2
    datetime.date(2026, 11, 30),  # Guru Nanak Jayanti
    datetime.date(2026, 12, 25),  # Christmas
]

# --- Supplementary special dates from V1 SQL ---
SPECIAL_DATES: Final[list[datetime.date]] = [
    # Jan 2026
    datetime.date(2026, 1, 27), datetime.date(2026, 1, 28),
    datetime.date(2026, 1, 29), datetime.date(2026, 1, 30), datetime.date(2026, 1, 31),
    # Feb 2026
    datetime.date(2026, 2, 1), datetime.date(2026, 2, 2),
    datetime.date(2026, 2, 6), datetime.date(2026, 2, 7), datetime.date(2026, 2, 8),
    datetime.date(2026, 2, 13), datetime.date(2026, 2, 14), datetime.date(2026, 2, 15),
    datetime.date(2026, 2, 19), datetime.date(2026, 2, 20), datetime.date(2026, 2, 21),
    datetime.date(2026, 2, 22), datetime.date(2026, 2, 23),
    datetime.date(2026, 2, 27), datetime.date(2026, 2, 28),
    # Mar 2026
    datetime.date(2026, 3, 1), datetime.date(2026, 3, 2), datetime.date(2026, 3, 3),
    datetime.date(2026, 3, 4), datetime.date(2026, 3, 6), datetime.date(2026, 3, 7),
    datetime.date(2026, 3, 8), datetime.date(2026, 3, 13), datetime.date(2026, 3, 14),
    datetime.date(2026, 3, 15), datetime.date(2026, 3, 19), datetime.date(2026, 3, 20),
    datetime.date(2026, 3, 21), datetime.date(2026, 3, 22), datetime.date(2026, 3, 26),
    datetime.date(2026, 3, 27), datetime.date(2026, 3, 28), datetime.date(2026, 3, 29),
    datetime.date(2026, 3, 30), datetime.date(2026, 3, 31),
    # Apr 2026
    datetime.date(2026, 4, 1), datetime.date(2026, 4, 2), datetime.date(2026, 4, 3),
    datetime.date(2026, 4, 4), datetime.date(2026, 4, 5), datetime.date(2026, 4, 10),
    datetime.date(2026, 4, 11), datetime.date(2026, 4, 12), datetime.date(2026, 4, 14),
    datetime.date(2026, 4, 15), datetime.date(2026, 4, 17), datetime.date(2026, 4, 18),
    datetime.date(2026, 4, 19), datetime.date(2026, 4, 24), datetime.date(2026, 4, 25),
    datetime.date(2026, 4, 26), datetime.date(2026, 4, 29), datetime.date(2026, 4, 30),
    # May 2026
    datetime.date(2026, 5, 1), datetime.date(2026, 5, 2), datetime.date(2026, 5, 3),
    datetime.date(2026, 5, 8), datetime.date(2026, 5, 9), datetime.date(2026, 5, 10),
    datetime.date(2026, 5, 15), datetime.date(2026, 5, 16), datetime.date(2026, 5, 17),
    datetime.date(2026, 5, 22), datetime.date(2026, 5, 23), datetime.date(2026, 5, 24),
    datetime.date(2026, 5, 29), datetime.date(2026, 5, 30), datetime.date(2026, 5, 31),
    # Jun 2026
    datetime.date(2026, 6, 1), datetime.date(2026, 6, 2), datetime.date(2026, 6, 5),
    datetime.date(2026, 6, 6), datetime.date(2026, 6, 7), datetime.date(2026, 6, 12),
    datetime.date(2026, 6, 13), datetime.date(2026, 6, 14), datetime.date(2026, 6, 19),
    datetime.date(2026, 6, 20), datetime.date(2026, 6, 21), datetime.date(2026, 6, 26),
    datetime.date(2026, 6, 27), datetime.date(2026, 6, 28), datetime.date(2026, 6, 29),
    datetime.date(2026, 6, 30),
    # Jul 2026
    datetime.date(2026, 7, 1), datetime.date(2026, 7, 2), datetime.date(2026, 7, 3),
    datetime.date(2026, 7, 4), datetime.date(2026, 7, 5), datetime.date(2026, 7, 10),
    datetime.date(2026, 7, 11), datetime.date(2026, 7, 12), datetime.date(2026, 7, 16),
    datetime.date(2026, 7, 17), datetime.date(2026, 7, 18), datetime.date(2026, 7, 19),
    datetime.date(2026, 7, 24), datetime.date(2026, 7, 25), datetime.date(2026, 7, 26),
    datetime.date(2026, 7, 29), datetime.date(2026, 7, 30), datetime.date(2026, 7, 31),
]

# Convert SPECIAL_DATES to a set for O(1) lookup
_SPECIAL_DATES_SET: Final[frozenset[datetime.date]] = frozenset(SPECIAL_DATES)


def is_peak_date(d: datetime.date) -> bool:
    """Narrow peak date check (optimized — see optimize_offline.py).

    Peak = last 2 calendar days of month ONLY.
    The original V2 also flagged holidays ±1 and all SPECIAL_DATES, but
    the backtest sweep showed that "month_end_only" with conservative
    non-peak mode gives 3.8% MAPE vs 18.0% for V1. The broad definition
    flagged 60% of days as peak, diluting the signal.
    """
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day >= last_day - 1


def is_peak_date_broad(d: datetime.date) -> bool:
    """Original broad peak definition (kept for A/B testing).

    Peak = last 2 calendar days of month
           OR in HOLIDAYS list (with ±1 day adjacency)
           OR in SPECIAL_DATES list.
    """
    last_day = calendar.monthrange(d.year, d.month)[1]
    if d.day >= last_day - 1:
        return True
    for holiday in HOLIDAYS:
        if abs((d - holiday).days) <= 1:
            return True
    return d in _SPECIAL_DATES_SET


# --- Default parameters (spec Section 5) ---
PARAMS: Final[dict[str, object]] = {
    # Seasonal baseline blend (tuned: 0.6 → 0.7 → 0.8 by sweep)
    "ten_week_weight": 0.8,
    # Horizon trust in pipeline vs seasonal (all tuned by sweep)
    "horizon_weight_T0": 0.70,
    "horizon_weight_T1": 0.70,
    "horizon_weight_T2": 0.55,
    # Similar-opp-volume filter for conversion matching
    "opp_volume_lower_pct": 0.90,
    "opp_volume_upper_pct": 1.20,
    # Conversion lookback
    "conversion_lookback_weeks": 8,
    # Recency decay
    "recency_decay_fn": "1/(days_gap+1)",
    # Range percentiles (Stage 2 calibrated, optimized weights)
    # Per-horizon asymmetric intervals targeting 65% coverage
    "range_lower_pctl_T0": -0.1318,
    "range_upper_pctl_T0": 0.0932,
    "range_lower_pctl_T1": -0.2657,
    "range_upper_pctl_T1": 0.0932,
    "range_lower_pctl_T2": -0.2657,
    "range_upper_pctl_T2": 0.0932,
    # Stratified range percentiles by day type (peak vs nonpeak)
    # Peak days (last 2 of month) have wider error distributions
    "range_lower_pctl_T0_peak": 0.0012,
    "range_upper_pctl_T0_peak": 0.1133,
    "range_lower_pctl_T1_peak": 0.0012,
    "range_upper_pctl_T1_peak": 0.1624,
    "range_lower_pctl_T2_peak": 0.1539,
    "range_upper_pctl_T2_peak": 0.2976,
    # Non-peak days — calibrated from 58 non-peak residuals
    "range_lower_pctl_T0_nonpeak": -0.0899,
    "range_upper_pctl_T0_nonpeak": 0.1514,
    "range_lower_pctl_T1_nonpeak": -0.0899,
    "range_upper_pctl_T1_nonpeak": 0.3665,
    "range_lower_pctl_T2_nonpeak": -0.0899,
    "range_upper_pctl_T2_nonpeak": 0.3665,
    # Fallback (used when horizon-specific not available)
    "range_lower_pctl": -0.1318,
    "range_upper_pctl": 0.0932,
    # Pipeline buckets (days before service)
    "bucket_boundaries": [0, 2, 4, 8],
    # Empty pipeline fallback
    "min_pipeline_opps": 5,
    # Peak multiplier cap (1.0 = disabled; optimizer can tune upward)
    "peak_multiplier_cap": 1.0,
    # Conservative non-peak mode (strategy D from sweep)
    "conservative_nonpeak": True,
    # Optimization trade-off
    "optimization_lambda": 0.3,
    # Run cadence (hours)
    "run_cadence_hours": 1,
    # Backtest as-of granularity
    "backtest_hour_step": 1,
}

# --- Ranges for tunable params (spec Section 5) ---
PARAM_RANGES: Final[dict[str, tuple[float, float]]] = {
    "ten_week_weight": (0.3, 0.9),
    "horizon_weight_T0": (0.7, 0.9),
    "horizon_weight_T1": (0.4, 0.8),
    "horizon_weight_T2": (0.3, 0.6),
    "opp_volume_lower_pct": (0.80, 0.95),
    "opp_volume_upper_pct": (1.10, 1.30),
    "optimization_lambda": (0.1, 0.5),
    "min_pipeline_opps": (3, 10),
    "peak_multiplier_cap": (1.0, 1.3),
}

# --- Hybrid peak definition: "month_boundary" ---
# Data-driven from Snowflake backtest: April 1 shows 1.9-2.2x normal volume
# (month-end spillover). April 2-3 returns to baseline.
# Peak = last 2 days of current month + first 1 day of next month.


def is_peak_date_hybrid(d: datetime.date) -> bool:
    """Hybrid peak date: last 2 days of month OR first day of next month.

    More accurate than V4's 'last 2 days only' because the month-end
    surge spills into the first day of the following month (verified
    from March/April 2025 and 2026 actuals).
    """
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day >= last_day - 1 or d.day == 1


def is_month_edge(d: datetime.date) -> bool:
    """V2-style broader month-edge flag for seasonal matching.

    Used to find historically similar days — finds more matches than
    the narrow peak definition. Edge = day <= 3 OR days_to_end <= 2.
    """
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day <= 3 or d.day >= last_day - 2


def holiday_phase(d: datetime.date) -> str:
    """Return 'holiday', 'pre_holiday', 'post_holiday', or 'normal'.

    Used as a matching feature for tiered seasonal matching.
    """
    for holiday in HOLIDAYS:
        delta = (d - holiday).days
        if delta == 0:
            return "holiday"
        if delta == -1:
            return "pre_holiday"
        if delta == 1:
            return "post_holiday"
    return "normal"


@dataclasses.dataclass
class HybridParams:
    """All tunable parameters for the hybrid blending engine.

    Designed for mypy strict-mode: every parameter has an explicit type.
    Default values reflect the final architecture decisions from
    docs/HYBRID_ARCHITECTURE.md Section 3.4.
    """

    # Horizon weights: (T+0, T+1, T+2)
    # T+0=0.80 from V2 (higher pipeline trust when nowcast is active)
    # T+2=0.55 from V4 (bucketed pipeline is informative at 2-day horizon)
    horizon_weights: tuple[float, float, float] = (0.80, 0.70, 0.55)

    # Nowcast parameters (T+0 only, from V2)
    full_switch_share: float = 0.95   # At this share, use nowcast exclusively
    sparse_pipeline_weight: float = 0.20  # Fallback weight when opps < min

    # Pipeline matching
    min_pipeline_opps: int = 5
    opp_volume_lower_pct: float = 0.80   # Wider tolerance than V4's 0.90
    opp_volume_upper_pct: float = 1.20

    # History lookback (V2's 180 days for deeper conversion match pool)
    history_lookback_days: int = 180

    # Peak settings
    peak_multiplier: float = 1.0       # Currently disabled (both models agree)
    peak_multiplier_cap: float = 1.0   # Hard cap to prevent runaway scaling

    # Seasonal settings (V2's approach)
    seasonal_top_k: int = 10           # Top-k matches for median
    seasonal_match_method: str = "median"  # "median" (V2) vs "mean" (V4)

    # Range percentiles — calibrated from backtest residuals (P17.5 / P82.5 for 65% coverage)
    # Non-peak: calibrated from 58 samples per horizon (reliable).
    # Peak: calibrated from 12 pooled samples (all horizons); negative upper
    #   means the model over-predicts peak days on average (see calibration report).
    #   Re-calibrate after collecting T+2-specific peak backtest data.
    range_lower_pctl_T0_peak: float = -0.2517
    range_upper_pctl_T0_peak: float = -0.0380
    range_lower_pctl_T1_peak: float = -0.2517
    range_upper_pctl_T1_peak: float = -0.0380
    range_lower_pctl_T2_peak: float = -0.2517
    range_upper_pctl_T2_peak: float = -0.0380
    range_lower_pctl_T0_nonpeak: float = -0.1306
    range_upper_pctl_T0_nonpeak: float = 0.1142
    range_lower_pctl_T1_nonpeak: float = -0.2685
    range_upper_pctl_T1_nonpeak: float = 0.1142
    range_lower_pctl_T2_nonpeak: float = -0.2685
    range_upper_pctl_T2_nonpeak: float = 0.1142


# Default hybrid parameters (use for production + CLI)
DEFAULT_HYBRID_PARAMS: HybridParams = HybridParams()
