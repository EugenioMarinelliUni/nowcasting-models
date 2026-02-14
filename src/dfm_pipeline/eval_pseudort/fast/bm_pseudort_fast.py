from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_bm_ml.types import BMDfmResult, BMParams, EMStepCache
from dfm_pipeline.dfm_dyn.state_space_new import kalman_filter, kalman_smoother
from dfm_pipeline.eval_pseudort.bm_pseudort import (
    PseudoRTEvalConfig,
    _apply_delay_mask,
    _apply_quarterly_release_mask,
    _horizon_target_date,
    _month_of_quarter,
    compute_scores,
)


FitCallable = Callable[
    [np.ndarray, np.ndarray, object, BMParams | None, EMStepCache | None],
    BMDfmResult,
]


@dataclass
class FastPseudoRTOptions:
    warm_start: bool = False
    n_jobs: int = 1
    blas_threads: int = 1


def _maybe_scale_observations_for_smoother(Y_stack: np.ndarray, res: BMDfmResult) -> np.ndarray:
    scaler = getattr(res, "scaler", None)
    if scaler is None:
        return Y_stack
    return scaler.transform(Y_stack)


def run_pseudo_rt_eval_fast(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    fit_fn: FitCallable,
    model_config,
    eval_cfg: PseudoRTEvalConfig,
    warm_start: bool = False,
    n_jobs: int = 1,
    blas_threads: int = 1,
) -> tuple[pd.DataFrame, dict]:
    # Parse eval window
    eval_start = pd.Timestamp(eval_cfg.eval_start)
    eval_end = pd.Timestamp(eval_cfg.eval_end)

    idx = X_full.index
    eval_months = idx[(idx >= eval_start) & (idx <= eval_end)]
    if eval_months.empty:
        raise ValueError("No eval months in requested window after alignment with panel index.")

    delay_map = None
    if eval_cfg.delay_style == "json_map":
        if not eval_cfg.delay_json:
            raise ValueError("delay_style='json_map' requires delay_json.")
        delay_map = json.loads(Path(eval_cfg.delay_json).read_text(encoding="utf-8"))

    out_rows: list[dict] = []

    init_params = None
    em_cache = None

    for t in eval_months:
        X_work = X_full.loc[:t].copy()
        y_work = y_full.loc[:t].copy()

        X_work = _apply_delay_mask(X_work, t, eval_cfg.delay_style, delay_map)
        y_work = _apply_quarterly_release_mask(y_work, t, eval_cfg.gdp_rel)

        res = fit_fn(
            X_work.to_numpy(dtype=float),
            y_work.to_numpy(dtype=float),
            model_config,
            init_params if warm_start else None,
            em_cache if warm_start else None,
        )

        if warm_start:
            init_params = res.params
            em_cache = res.em_cache

        Y_stack = np.column_stack([X_full.to_numpy(dtype=float), y_full.to_numpy(dtype=float)])
        Y_stack_scaled = _maybe_scale_observations_for_smoother(Y_stack, res)

        ss = res.state_space
        kf = kalman_filter(Y=Y_stack_scaled, T=ss.T, Z=ss.C, R=ss.R, Q=ss.Q, a0=ss.a0, P0=ss.P0)
        ks = kalman_smoother(kf)

        a_smooth = ks["a_smooth"]
        C = ss.C

        t_moq = _month_of_quarter(pd.Timestamp(t))

        scaler = getattr(res, "scaler", None)
        mu_y = float(getattr(scaler, "mu", np.array([0.0]))[-1]) if scaler is not None else 0.0
        sd_y = float(getattr(scaler, "sd", np.array([1.0]))[-1]) if scaler is not None else 1.0

        for h in eval_cfg.horizons:
            target_date = _horizon_target_date(pd.Timestamp(t), h)
            if target_date not in idx:
                pred_scaled = float("nan")
                actual_raw = float("nan")
            else:
                target_idx = idx.get_loc(target_date)
                Cq = C[-1, :]
                pred_scaled = float(Cq @ a_smooth[target_idx, :])
                actual_raw = float(y_full.loc[target_date])

            pred_raw = float(pred_scaled * sd_y + mu_y) if np.isfinite(pred_scaled) else float("nan")
            actual_scaled = (
                float((actual_raw - mu_y) / sd_y) if np.isfinite(actual_raw) and sd_y != 0 else float("nan")
            )

            out_rows.append(
                {
                    "eval_date": pd.Timestamp(t),
                    "moq": int(t_moq),
                    "horizon": h,
                    "target_date": pd.Timestamp(target_date),
                    "pred_scaled": pred_scaled,
                    "actual_scaled": actual_scaled,
                    "pred_raw": pred_raw,
                    "actual_raw": actual_raw,
                    "pred": pred_raw,
                    "actual": actual_raw,
                    "scaling_mode": str(getattr(model_config, "scaling_mode", "")),
                }
            )

    pred_df = pd.DataFrame(out_rows).sort_values(["eval_date", "horizon", "moq"]).reset_index(drop=True)
    scores = compute_scores(pred_df)

    return pred_df, scores