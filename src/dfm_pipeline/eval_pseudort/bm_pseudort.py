from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from dfm_pipeline.dfm_bm_ml.fit import fit_bm_dfm
from dfm_pipeline.dfm_bm_ml.scaling import TargetOutputScaler
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import (
    EvalConfig,
    compute_scores,
    run_pseudo_rt_eval_fast,
)
from dfm_pipeline.eval_pseudort.vintages import VintageProvider


@dataclass
class PseudoRTEvalConfig:
    """Compatibility configuration for the canonical leakage-safe evaluator."""

    eval_start: str
    eval_end: str
    delay_style: str = "none"
    delay_json: str | None = None
    gdp_rel: int = 0
    horizons: tuple[str, ...] = ("bac", "now", "for")
    no_qe_leak: bool = False
    prediction_interval_levels: tuple[float, ...] = (0.68, 0.90, 0.95)
    require_convergence: bool = False
    on_nonconvergence: str = "raise"
    apply_masks_to_vintage_provider: bool = False
    vintage_as_of_rule: str = "month_start"

    def to_fast_config(self) -> EvalConfig:
        return EvalConfig(
            eval_start=self.eval_start,
            eval_end=self.eval_end,
            delay_style=self.delay_style,
            delay_json=self.delay_json,
            gdp_rel=int(self.gdp_rel),
            horizons=tuple(self.horizons),
            no_qe_leak=bool(self.no_qe_leak),
            prediction_interval_levels=tuple(self.prediction_interval_levels),
            require_convergence=bool(self.require_convergence),
            on_nonconvergence=str(self.on_nonconvergence),
            apply_masks_to_vintage_provider=bool(self.apply_masks_to_vintage_provider),
            vintage_as_of_rule=str(self.vintage_as_of_rule),
        )


def run_pseudo_rt_eval(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    model_config,
    eval_cfg: PseudoRTEvalConfig,
    outdir: str | Path | None = None,
    warm_start: bool = False,
    blas_threads: int | None = None,
    *,
    vintage_provider: Optional[VintageProvider] = None,
    target_output_scaler: Optional[TargetOutputScaler] = None,
) -> tuple[pd.DataFrame, dict]:
    """Run the non-fast public API through the same leakage-safe core.

    The former implementation rebuilt a smoother on the complete panel after
    every vintage and therefore used future observations.  Delegating to the
    canonical evaluator ensures that all smoothing and forecasting are based
    only on the vintage information set.
    """
    pred_df, scores = run_pseudo_rt_eval_fast(
        X_full=X_full,
        y_full=y_full,
        fit_fn=fit_bm_dfm,
        model_config=model_config,
        eval_cfg=eval_cfg.to_fast_config(),
        warm_start=bool(warm_start),
        fixed_params=False,
        blas_threads=blas_threads,
        vintage_provider=vintage_provider,
        target_output_scaler=target_output_scaler,
    )

    if outdir is not None:
        out_path = Path(outdir)
        out_path.mkdir(parents=True, exist_ok=True)
        pred_df.to_csv(out_path / "predictions.csv", index=False)
        with (out_path / "scores.json").open("w", encoding="utf-8") as handle:
            json.dump(scores, handle, indent=2)
    return pred_df, scores


__all__ = ["PseudoRTEvalConfig", "compute_scores", "run_pseudo_rt_eval"]
