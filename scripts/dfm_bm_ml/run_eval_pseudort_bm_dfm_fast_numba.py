from __future__ import annotations

import argparse
import json
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.fast import fit_bm_dfm_fast_numba as fit_bm_dfm_fast
from dfm_pipeline.eval_pseudort.bm_pseudort import PseudoRTEvalConfig
from dfm_pipeline.eval_pseudort.fast import run_pseudo_rt_eval_fast


def _filter_kwargs_for_dataclass(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Return only kwargs that are valid fields for the given dataclass `cls`.

    This makes the script robust if the dataclass differs slightly across versions.
    """
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
        description="Pseudo-real-time evaluation of the BM-DFM (fast implementation).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Inputs / outputs
    parser.add_argument("--panel-csv", required=True, help="Path to monthly panel CSV (X).")
    parser.add_argument("--target-csv", required=True, help="Path to target CSV (y on monthly index).")
    parser.add_argument("--date-col", default="sasdate", help="Name of date column in both CSV files.")
    parser.add_argument("--target-col", required=True, help="Name of target column in target CSV.")
    parser.add_argument("--outdir", required=True, help="Output directory for predictions and scores.")

    # Model params
    parser.add_argument("--r", type=int, required=True, help="Number of factors (single block => r_by_block=(r,)).")
    parser.add_argument("--p", type=int, required=True, help="Factor VAR lag order.")
    parser.add_argument("--mm-style", choices=["toolbox", "scaled"], default="toolbox", help="Mariano-Murasawa style.")
    parser.add_argument("--max-iter", type=int, default=200, help="Maximum EM iterations per refit.")
    parser.add_argument("--tol", type=float, default=1e-6, help="Convergence tolerance on dLL.")

    parser.add_argument(
        "--enforce-quarterly-loading-constraint",
        dest="enforce_q_loading_constraint",
        action="store_true",
        help="Enforce toolbox-style quarterly loading constraints.",
    )
    parser.add_argument(
        "--no-enforce-quarterly-loading-constraint",
        dest="enforce_q_loading_constraint",
        action="store_false",
        help="Disable quarterly loading constraints.",
    )
    parser.set_defaults(enforce_q_loading_constraint=False)

    parser.add_argument(
        "--fix-quarterly-R",
        dest="fix_quarterly_R",
        action="store_true",
        help="Fix quarterly measurement variance to floor (toolbox-like).",
    )
    parser.add_argument(
        "--no-fix-quarterly-R",
        dest="fix_quarterly_R",
        action="store_false",
        help="Do not fix quarterly measurement variance (estimate it).",
    )
    parser.set_defaults(fix_quarterly_R=False)

    std_group = parser.add_mutually_exclusive_group()
    std_group.add_argument(
        "--standardize",
        dest="standardize",
        action="store_true",
        help="Standardize Y inside the fitter (use if inputs are not z-scored).",
    )
    std_group.add_argument(
        "--no-standardize",
        dest="standardize",
        action="store_false",
        help="Disable internal standardization (use if inputs are already z-scored).",
    )
    parser.set_defaults(standardize=False)

    # Evaluation window
    parser.add_argument("--eval-start", required=True, help="Evaluation start date (YYYY-MM-DD).")
    parser.add_argument("--eval-end", required=True, help="Evaluation end date (YYYY-MM-DD).")

    # Ragged edge / delays
    parser.add_argument(
        "--delay-style",
        choices=["none", "trailing_nan", "json_map"],
        default="none",
        help="Ragged-edge simulation style.",
    )
    parser.add_argument("--delay-json", default=None, help="JSON file for delays (required if delay-style=json_map).")
    parser.add_argument("--gdp-rel", type=int, default=0, help="Release lag in months for quarterly target masking.")

    # NEW: Speed toggles (Point A)
    parser.add_argument(
        "--warm-start",
        action="store_true",
        help="Warm-start EM across evaluation months (sequential; big speed-up).",
    )
    parser.add_argument(
        "--n-jobs",
        type=int,
        default=1,
        help="Parallelize across evaluation months when >1 (not compatible with --warm-start).",
    )
    parser.add_argument(
        "--blas-threads",
        type=int,
        default=1,
        help="BLAS threads per job when using --n-jobs > 1 (avoid oversubscription).",
    )

    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # -------------------------
    # Load data
    # -------------------------
    X_full = _load_panel(args.panel_csv, args.date_col)
    y_full = _load_target(args.target_csv, args.date_col, args.target_col)

    # Align on common index
    common_idx = X_full.index.intersection(y_full.index)
    if common_idx.empty:
        raise ValueError("Panel and target have empty intersection after alignment on dates.")
    X_full = X_full.loc[common_idx]
    y_full = y_full.loc[common_idx]

    # Ensure numeric dtypes (avoid object columns)
    X_full = X_full.apply(pd.to_numeric, errors="coerce")
    y_full = pd.to_numeric(y_full, errors="coerce")

    # -------------------------
    # Build model config (robustly)
    # -------------------------
    model_kwargs = dict(
        r_by_block=(int(args.r),),
        p=int(args.p),
        mm_weight_style=str(args.mm_style),
        max_iter=int(args.max_iter),
        tol=float(args.tol),
        enforce_quarterly_loading_constraint=bool(args.enforce_q_loading_constraint),
        fix_quarterly_R=bool(args.fix_quarterly_R),
        standardize=bool(args.standardize),
        n_quarterly=1,
    )
    model_kwargs = _filter_kwargs_for_dataclass(BMDfmConfig, model_kwargs)
    model_config = BMDfmConfig(**model_kwargs)

    # -------------------------
    # Build pseudo-RT eval config (robustly)
    # -------------------------
    eval_kwargs: Dict[str, Any] = dict(
        eval_start=str(args.eval_start),
        eval_end=str(args.eval_end),
        delay_style=str(args.delay_style),
        delay_json=args.delay_json,
        gdp_rel=int(args.gdp_rel),
    )
    # Provide default horizons if supported by dataclass
    # (matches typical bac/now/for outputs)
    eval_kwargs.setdefault("horizons", ("bac", "now", "for"))
    eval_kwargs = _filter_kwargs_for_dataclass(PseudoRTEvalConfig, eval_kwargs)
    eval_cfg = PseudoRTEvalConfig(**eval_kwargs)

    # -------------------------
    # Run evaluation
    # -------------------------
    pred_df, scores = run_pseudo_rt_eval_fast(
        X_full=X_full,
        y_full=y_full,
        fit_fn=fit_bm_dfm_fast,
        model_config=model_config,
        eval_cfg=eval_cfg,
        warm_start=bool(args.warm_start),
        n_jobs=int(args.n_jobs),
        blas_threads=int(args.blas_threads),
    )

    # -------------------------
    # Save outputs
    # -------------------------
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
        "outdir": str(outdir),
        "r": args.r,
        "p": args.p,
        "mm_style": args.mm_style,
        "max_iter": args.max_iter,
        "tol": args.tol,
        "standardize": args.standardize,
        "enforce_quarterly_loading_constraint": args.enforce_q_loading_constraint,
        "fix_quarterly_R": args.fix_quarterly_R,
        "eval_start": args.eval_start,
        "eval_end": args.eval_end,
        "delay_style": args.delay_style,
        "delay_json": args.delay_json,
        "gdp_rel": args.gdp_rel,
        "warm_start": args.warm_start,
        "n_jobs": args.n_jobs,
        "blas_threads": args.blas_threads,
    }
    with open(outdir / "run_config.json", "w", encoding="utf-8") as f:
        json.dump(cfg_dump, f, indent=2)

    print(f"[OK] Saved predictions to: {pred_path}")
    print(f"[OK] Saved scores to:       {scores_path}")
    print(f"[OK] Saved run config to:   {outdir / 'run_config.json'}")


if __name__ == "__main__":
    main()
