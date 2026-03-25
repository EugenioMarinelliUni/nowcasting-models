from __future__ import annotations

import argparse
import json
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.fast import fit_bm_dfm_fast_numba as fit_bm_dfm_fast
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import EvalConfig as PseudoRTEvalConfig, run_pseudo_rt_eval_fast
from dfm_pipeline.utils.threadpool import limit_blas_threads


def _filter_kwargs_for_dataclass(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    if not is_dataclass(cls):
        return kwargs
    valid = {f.name for f in fields(cls)}
    return {k: v for k, v in kwargs.items() if k in valid}


def _load_panel(panel_csv: str, date_col: str) -> pd.DataFrame:
    X = pd.read_csv(panel_csv)
    if date_col not in X.columns:
        raise ValueError(f"date_col='{date_col}' not found in panel CSV columns.")
    X[date_col] = pd.to_datetime(X[date_col])
    X = X.set_index(date_col).sort_index()
    return X


def _load_target(target_csv: str, date_col: str, target_col: str) -> pd.Series:
    y = pd.read_csv(target_csv)
    if date_col not in y.columns:
        raise ValueError(f"date_col='{date_col}' not found in target CSV columns.")
    if target_col not in y.columns:
        raise ValueError(f"target_col='{target_col}' not found in target CSV columns.")
    y[date_col] = pd.to_datetime(y[date_col])
    y = y.set_index(date_col).sort_index()
    return y[target_col]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pseudo-real-time evaluation of the BM-DFM (fast numba).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--panel-csv", required=True)
    parser.add_argument("--target-csv", required=True)
    parser.add_argument("--date-col", default="sasdate")
    parser.add_argument("--target-col", required=True)
    parser.add_argument("--outdir", required=True)

    parser.add_argument("--r", type=int, required=True)
    parser.add_argument("--p", type=int, required=True)

    parser.add_argument("--mm-style", choices=["toolbox", "scaled"], default="toolbox")
    parser.add_argument("--pca-fill", default="mean", choices=["mean", "ffill"])

    parser.add_argument("--max-iter", type=int, default=200)
    parser.add_argument("--tol", type=float, default=1e-6)

    parser.add_argument("--rho-idio-init", default=0.10, type=float)
    parser.add_argument("--min-var", default=1e-8, type=float)
    parser.add_argument("--jitter", default=1e-12, type=float)

    parser.add_argument("--monthly-meas-var-floor", default=1e-4, type=float)
    parser.add_argument("--quarterly-meas-var-floor", default=1e-4, type=float)

    parser.add_argument(
        "--scaling-mode",
        default="external_frozen",
        choices=["external_frozen", "internal_per_run", "toolbox_vintage"],
    )

    parser.add_argument("--idio-ar1", dest="idio_ar1", action="store_true", default=True)
    parser.add_argument("--no-idio-ar1", dest="idio_ar1", action="store_false")

    parser.add_argument("--enforce-quarterly-loading-constraint", action="store_true", default=True)
    parser.add_argument(
        "--no-enforce-quarterly-loading-constraint",
        dest="enforce_quarterly_loading_constraint",
        action="store_false",
    )

    parser.add_argument("--fix-quarterly-R", action="store_true", default=True)
    parser.add_argument("--no-fix-quarterly-R", dest="fix_quarterly_R", action="store_false")

    parser.add_argument("--force-var-stability", dest="force_var_stability", action="store_true", default=True)
    parser.add_argument("--no-force-var-stability", dest="force_var_stability", action="store_false")
    parser.add_argument("--var-stability-shrink", default=0.98, type=float)

    parser.add_argument("--eval-start", required=True)
    parser.add_argument("--eval-end", required=True)

    parser.add_argument("--delay-style", choices=["none", "trailing_nan", "json_map"], default="none")
    parser.add_argument("--delay-json", default=None)
    parser.add_argument("--gdp-rel", type=int, default=0)

    parser.add_argument("--warm-start", action="store_true")
    parser.add_argument("--blas-threads", type=int, default=1)

    args = parser.parse_args()

    if int(args.blas_threads) > 0:
        limit_blas_threads(int(args.blas_threads))

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    X_full = _load_panel(args.panel_csv, args.date_col)
    y_full = _load_target(args.target_csv, args.date_col, args.target_col)

    common_idx = X_full.index.intersection(y_full.index)
    if common_idx.empty:
        raise ValueError("Panel and target have empty intersection after alignment on dates.")
    X_full = X_full.loc[common_idx]
    y_full = y_full.loc[common_idx]

    X_full = X_full.apply(pd.to_numeric, errors="coerce")
    y_full = pd.to_numeric(y_full, errors="coerce")

    model_kwargs: Dict[str, Any] = dict(
        r_by_block=(int(args.r),),
        p=int(args.p),
        blocks=None,
        n_quarterly=1,
        mm_weight_style=str(args.mm_style),
        pca_fill=str(args.pca_fill),
        max_iter=int(args.max_iter),
        tol=float(args.tol),
        rho_idio_init=float(args.rho_idio_init),
        min_var=float(args.min_var),
        jitter=float(args.jitter),
        monthly_meas_var_floor=float(args.monthly_meas_var_floor),
        quarterly_meas_var_floor=float(args.quarterly_meas_var_floor),
        scaling_mode=str(args.scaling_mode),
        idio_ar1=bool(args.idio_ar1),
        enforce_quarterly_loading_constraint=bool(args.enforce_quarterly_loading_constraint),
        fix_quarterly_R=bool(args.fix_quarterly_R),
        force_var_stability=bool(args.force_var_stability),
        var_stability_shrink=float(args.var_stability_shrink),
    )
    model_kwargs = _filter_kwargs_for_dataclass(BMDfmConfig, model_kwargs)
    model_config = BMDfmConfig(**model_kwargs)

    eval_kwargs: Dict[str, Any] = dict(
        eval_start=str(args.eval_start),
        eval_end=str(args.eval_end),
        delay_style=str(args.delay_style),
        delay_json=args.delay_json,
        gdp_rel=int(args.gdp_rel),
        horizons=("now",),
    )
    eval_kwargs = _filter_kwargs_for_dataclass(PseudoRTEvalConfig, eval_kwargs)
    eval_cfg = PseudoRTEvalConfig(**eval_kwargs)

    pred_df, scores = run_pseudo_rt_eval_fast(
        X_full=X_full,
        y_full=y_full,
        fit_fn=fit_bm_dfm_fast,
        model_config=model_config,
        eval_cfg=eval_cfg,
        warm_start=bool(args.warm_start),
        blas_threads=int(args.blas_threads),
    )

    pred_path = outdir / "predictions.csv"
    pred_df.to_csv(pred_path, index=False)

    scores_path = outdir / "scores.json"
    with open(scores_path, "w", encoding="utf-8") as f:
        json.dump(scores, f, indent=2)

    cfg_dump = {
        "panel_csv": args.panel_csv,
        "target_csv": args.target_csv,
        "date_col": args.date_col,
        "target_col": args.target_col,
        "model_config": model_kwargs,
        "eval_config": eval_kwargs,
        "warm_start": bool(args.warm_start),
        "blas_threads": int(args.blas_threads),
    }
    with open(outdir / "run_config.json", "w", encoding="utf-8") as f:
        json.dump(cfg_dump, f, indent=2)

    print(str(pred_path))
    print(str(scores_path))


if __name__ == "__main__":
    main()
