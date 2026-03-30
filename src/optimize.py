"""Two-stage grid search optimizer.

Stage 1: Optimize point-estimate weights (minimize MAE).
Stage 2: Calibrate range percentiles (achieve target coverage).
See spec Section 6 Optimization.
"""
import argparse
import itertools
from typing import Any

import numpy as np
import pandas as pd

from src.backtest import backtest, compute_metrics
from src.config import PARAM_RANGES, PARAMS
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
        combos.append(dict(zip(param_names, values, strict=True)))
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
    # Secondary params (coarser 2-step grid)
    secondary_names = [
        "opp_volume_lower_pct",
        "opp_volume_upper_pct",
        "optimization_lambda",
        "min_pipeline_opps",
    ]
    primary_grid = generate_grid(primary_names, steps=grid_steps)
    secondary_grid = generate_grid(secondary_names, steps=2)
    # Combine: each primary combo x each secondary combo
    grid = [{**p, **s} for p in primary_grid for s in secondary_grid]

    best_mae = float("inf")
    best_params: dict[str, Any] = dict(PARAMS)
    all_results: list[dict[str, Any]] = []

    for combo in grid:
        trial_params = dict(PARAMS)
        trial_params.update(combo)

        try:
            bt = backtest(
                window=eval_window, params=trial_params, conn=conn,
            )
            mae = float(bt["error"].abs().mean())
        except Exception:
            continue

        result: dict[str, Any] = {**combo, "mae": mae}
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
    bt = backtest(
        window=eval_window, params=best_params, conn=conn,
    )

    percentiles: dict[int, tuple[float, float]] = {}
    for horizon in [0, 1, 2]:
        h = bt[bt["horizon"] == horizon]
        if h.empty:
            continue
        residuals = np.array([
            compute_error_pct(
                actual=float(row["actual"]),
                predicted=float(row["point_est"]),
            )
            for _, row in h.iterrows()
        ])
        p_lower, p_upper = calibrate_percentiles(
            residuals, target_coverage,
        )
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
    bt_eval = backtest(
        window=eval_window, params=best_params, conn=conn,
    )
    bt_val = backtest(
        window=validate_window, params=best_params, conn=conn,
    )

    eval_metrics = compute_metrics(bt_eval)
    val_metrics = compute_metrics(bt_val)

    report: dict[str, Any] = {
        "eval_mape_by_horizon": (
            eval_metrics.set_index("horizon")["mape"].to_dict()
        ),
        "val_mape_by_horizon": (
            val_metrics.set_index("horizon")["mape"].to_dict()
        ),
    }

    # Check for degradation > 5pp
    for h in [0, 1, 2]:
        eval_mape = eval_metrics.loc[
            eval_metrics["horizon"] == h, "mape"
        ].values
        val_mape = val_metrics.loc[
            val_metrics["horizon"] == h, "mape"
        ].values
        if len(eval_mape) > 0 and len(val_mape) > 0:
            gap = float(val_mape[0]) - float(eval_mape[0])
            if gap > 5.0:
                report[f"WARNING_horizon_{h}"] = (
                    f"30-day MAPE degrades by {gap:.1f}pp vs 7-day"
                )

    # Check if params hit boundaries
    for name in PARAM_RANGES:
        lo, hi = PARAM_RANGES[name]
        val = best_params.get(name)
        if (
            val is not None
            and isinstance(val, (int, float))
            and (abs(float(val) - lo) < 0.01 or abs(float(val) - hi) < 0.01)
        ):
                report[f"BOUNDARY_{name}"] = (
                    f"Optimal value {val} is at range boundary "
                    f"[{lo}, {hi}]"
                )

    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Optimize forecast parameters"
    )
    parser.add_argument("--eval-window", type=int, default=7)
    parser.add_argument("--validate-window", type=int, default=30)
    parser.add_argument("--grid-steps", type=int, default=3)
    args = parser.parse_args()

    print(
        f"Stage 1: Optimizing weights "
        f"(eval window = {args.eval_window} days)..."
    )
    best_params, grid_results = stage1_optimize(
        eval_window=args.eval_window, grid_steps=args.grid_steps,
    )
    print(f"Best MAE: {grid_results['mae'].min():.2f}")
    print("Best weights:")
    for k in [
        "ten_week_weight",
        "horizon_weight_T0",
        "horizon_weight_T1",
        "horizon_weight_T2",
    ]:
        print(f"  {k}: {best_params[k]:.3f}")

    print("\nStage 2: Calibrating range percentiles...")
    percentiles = stage2_calibrate(
        best_params, eval_window=args.eval_window,
    )
    for h, (pl, pu) in percentiles.items():
        print(f"  Horizon {h}: P_lower={pl:.4f}, P_upper={pu:.4f}")

    print(f"\nValidating on {args.validate_window}-day window...")
    report = validate(
        best_params, args.eval_window, args.validate_window,
    )
    for k, v in report.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
