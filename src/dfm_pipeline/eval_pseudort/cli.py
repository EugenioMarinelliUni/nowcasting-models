from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Callable

import pandas as pd

from dfm_pipeline.dfm_bm_ml.scaling import TargetOutputScaler
from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.eval_pseudort.fast.bm_pseudort_fast import EvalConfig, run_pseudo_rt_eval_fast
from dfm_pipeline.eval_pseudort.vintages import LongFormatVintageProvider
from dfm_pipeline.utils.threadpool import limit_blas_threads


def _csv_tuple(value: str, cast):
    return tuple(cast(x.strip()) for x in value.split(",") if x.strip())


def _load_panel(path: str, date_col: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if date_col not in df.columns:
        raise ValueError(f"date column {date_col!r} is missing from panel CSV.")
    df[date_col] = pd.to_datetime(df[date_col])
    return df.set_index(date_col).sort_index().apply(pd.to_numeric, errors="coerce")


def _load_target(path: str, date_col: str, target_col: str) -> pd.Series:
    df = pd.read_csv(path)
    if date_col not in df.columns or target_col not in df.columns:
        raise ValueError("Target CSV does not contain the requested date/target columns.")
    df[date_col] = pd.to_datetime(df[date_col])
    return pd.to_numeric(df.set_index(date_col).sort_index()[target_col], errors="coerce")


def build_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--panel-csv", required=True)
    p.add_argument("--target-csv", required=True)
    p.add_argument("--date-col", default="sasdate")
    p.add_argument("--target-col", required=True)
    p.add_argument("--outdir", required=True)

    factor_group = p.add_mutually_exclusive_group(required=True)
    factor_group.add_argument("--r", type=int, help="Single factor-block dimension (backward compatible)")
    factor_group.add_argument(
        "--r-by-block",
        help="Comma-separated factor dimensions, e.g. 1,1,2",
    )
    p.add_argument(
        "--blocks-json",
        default=None,
        help="Optional JSON list assigning each monthly predictor to a zero-based factor block",
    )
    p.add_argument("--p", type=int, required=True)
    p.add_argument("--mm-style", choices=["toolbox", "scaled"], default="toolbox")
    p.add_argument("--pca-fill", choices=["mean", "ffill"], default="mean")
    p.add_argument(
        "--init-missing-method",
        choices=["legacy_mean", "legacy_ffill", "toolbox_spline", "linear_interp"],
        default="toolbox_spline",
    )
    p.add_argument("--rho-idio-init", type=float, default=0.10)
    p.add_argument("--max-iter", type=int, default=200)
    p.add_argument("--tol", type=float, default=1e-6)
    p.add_argument("--scaling-mode", choices=["external_frozen", "internal_per_run", "toolbox_vintage"], default="external_frozen")
    p.add_argument("--monthly-meas-var-floor", type=float, default=1e-4)
    p.add_argument("--quarterly-meas-var-floor", type=float, default=1e-4)
    p.add_argument("--min-var", type=float, default=1e-8)
    p.add_argument("--jitter", type=float, default=1e-12)

    p.add_argument("--idio-ar1", dest="idio_ar1", action="store_true", default=True)
    p.add_argument("--no-idio-ar1", dest="idio_ar1", action="store_false")
    p.add_argument("--force-var-stability", dest="force_var_stability", action="store_true", default=True)
    p.add_argument("--no-force-var-stability", dest="force_var_stability", action="store_false")
    p.add_argument("--var-stability-shrink", type=float, default=0.98)
    p.add_argument("--no-quarterly-loading-constraint", dest="quarterly_constraint", action="store_false", default=True)
    p.add_argument("--estimate-quarterly-R", dest="fix_quarterly_R", action="store_false", default=True)

    p.add_argument(
        "--P0-mode",
        choices=["diffuse", "steady_state", "steady_state_factor_diffuse_idio", "estimated"],
        default="steady_state",
    )
    p.add_argument("--update-initial-state-each-iter", action="store_true", default=False)
    p.add_argument(
        "--estimation-mode",
        choices=["projected_gem", "unrestricted_em", "exact_constrained_ml"],
        default="projected_gem",
    )
    p.add_argument("--no-gem-likelihood-guard", dest="gem_guard", action="store_false", default=True)
    p.add_argument("--gem-ll-tolerance", type=float, default=1e-7)
    p.add_argument("--gem-min-step", type=float, default=1e-3)
    p.add_argument("--gem-max-backtracks", type=int, default=12)
    p.add_argument("--identification-mode", choices=["none", "sign_anchor"], default="none")
    p.add_argument("--identification-anchor-indices", default=None, help="Comma-separated monthly row indices, one per factor")

    p.add_argument("--eval-start", required=True)
    p.add_argument("--eval-end", required=True)
    p.add_argument("--horizons", default="now", help="Comma-separated bac,now,for")
    p.add_argument("--prediction-interval-levels", default="0.68,0.90,0.95")
    p.add_argument("--delay-style", choices=["none", "trailing_nan", "json_map"], default="none")
    p.add_argument("--delay-json", default=None)
    p.add_argument("--gdp-rel", type=int, default=0)
    p.add_argument("--no-qe-leak", action="store_true")
    p.add_argument("--require-convergence", action="store_true")
    p.add_argument("--on-nonconvergence", choices=["raise", "skip", "keep"], default="raise")

    p.add_argument("--warm-start", action="store_true")
    p.add_argument("--fixed-params", action="store_true")
    p.add_argument("--train-end", default=None)
    p.add_argument("--train-max-iter", type=int, default=None)
    p.add_argument("--blas-threads", type=int, default=1)

    p.add_argument("--target-scale-json", default=None, help="JSON with frozen target mean/std for reporting original GDP units")
    p.add_argument("--vintage-long-csv", default=None, help="Optional long-format historical-vintage table")
    p.add_argument("--vintage-target-series", default=None)
    p.add_argument("--vintage-series-col", default="series")
    p.add_argument("--vintage-reference-col", default="reference_date")
    p.add_argument("--vintage-date-col", default="vintage_date")
    p.add_argument("--vintage-value-col", default="value")
    p.add_argument("--apply-release-masks-to-vintages", action="store_true")
    p.add_argument("--vintage-as-of-rule", choices=["month_start", "month_end"], default="month_start")
    return p


def main_with_fit(fit_fn: Callable, *, description: str) -> None:
    args = build_parser(description).parse_args()
    if args.blas_threads > 0:
        limit_blas_threads(args.blas_threads)

    X_full = _load_panel(args.panel_csv, args.date_col)
    y_full = _load_target(args.target_csv, args.date_col, args.target_col)
    common = X_full.index.intersection(y_full.index)
    if common.empty:
        raise ValueError("Panel and target have an empty date intersection.")
    X_full, y_full = X_full.loc[common], y_full.loc[common]

    anchors = None if args.identification_anchor_indices is None else _csv_tuple(args.identification_anchor_indices, int)
    r_by_block = (int(args.r),) if args.r is not None else _csv_tuple(args.r_by_block, int)
    if not r_by_block:
        raise ValueError("At least one factor-block dimension is required.")
    blocks = None
    if args.blocks_json:
        blocks = json.loads(Path(args.blocks_json).read_text(encoding="utf-8"))
        if not isinstance(blocks, list):
            raise ValueError("--blocks-json must contain a JSON list of zero-based block indices.")
        blocks = [int(x) for x in blocks]
        if len(blocks) != X_full.shape[1]:
            raise ValueError("--blocks-json length must equal the number of monthly predictors.")

    model_config = BMDfmConfig(
        r_by_block=r_by_block,
        p=int(args.p),
        blocks=blocks,
        pca_fill=args.pca_fill,
        init_missing_method=args.init_missing_method,
        rho_idio_init=float(args.rho_idio_init),
        mm_weight_style=args.mm_style,
        max_iter=int(args.max_iter),
        tol=float(args.tol),
        scaling_mode=args.scaling_mode,
        monthly_meas_var_floor=float(args.monthly_meas_var_floor),
        quarterly_meas_var_floor=float(args.quarterly_meas_var_floor),
        min_var=float(args.min_var),
        jitter=float(args.jitter),
        idio_ar1=bool(args.idio_ar1),
        force_var_stability=bool(args.force_var_stability),
        var_stability_shrink=float(args.var_stability_shrink),
        enforce_quarterly_loading_constraint=bool(args.quarterly_constraint),
        fix_quarterly_R=bool(args.fix_quarterly_R),
        P0_mode=args.P0_mode,
        update_initial_state_each_iter=bool(args.update_initial_state_each_iter),
        estimation_mode=args.estimation_mode,
        gem_likelihood_guard=bool(args.gem_guard),
        gem_ll_tolerance=float(args.gem_ll_tolerance),
        gem_min_step=float(args.gem_min_step),
        gem_max_backtracks=int(args.gem_max_backtracks),
        identification_mode=args.identification_mode,
        identification_anchor_indices=anchors,
    )
    model_config.validate()

    eval_config = EvalConfig(
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        delay_style=args.delay_style,
        delay_json=args.delay_json,
        gdp_rel=int(args.gdp_rel),
        horizons=_csv_tuple(args.horizons, str),
        no_qe_leak=bool(args.no_qe_leak),
        prediction_interval_levels=_csv_tuple(args.prediction_interval_levels, float),
        require_convergence=bool(args.require_convergence),
        on_nonconvergence=args.on_nonconvergence,
        apply_masks_to_vintage_provider=bool(args.apply_release_masks_to_vintages),
        vintage_as_of_rule=str(args.vintage_as_of_rule),
    )

    target_scaler = TargetOutputScaler.from_json(args.target_scale_json) if args.target_scale_json else None
    vintage_provider = None
    if args.vintage_long_csv:
        if not args.vintage_target_series:
            raise ValueError("--vintage-target-series is required with --vintage-long-csv.")
        vintage_provider = LongFormatVintageProvider.from_csv(
            args.vintage_long_csv,
            target_series=args.vintage_target_series,
            predictor_order=list(X_full.columns),
            series_col=args.vintage_series_col,
            reference_col=args.vintage_reference_col,
            vintage_col=args.vintage_date_col,
            value_col=args.vintage_value_col,
        )

    pred, scores = run_pseudo_rt_eval_fast(
        X_full=X_full,
        y_full=y_full,
        fit_fn=fit_fn,
        model_config=model_config,
        eval_cfg=eval_config,
        warm_start=bool(args.warm_start),
        fixed_params=bool(args.fixed_params),
        train_end=args.train_end,
        train_max_iter=args.train_max_iter,
        blas_threads=int(args.blas_threads),
        vintage_provider=vintage_provider,
        target_output_scaler=target_scaler,
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    pred.to_csv(outdir / "predictions.csv", index=False)
    (outdir / "scores.json").write_text(json.dumps(scores, indent=2), encoding="utf-8")
    run_config = {"arguments": vars(args), "resolved_model_config": model_config.__dict__, "resolved_eval_config": eval_config.__dict__}
    (outdir / "run_config.json").write_text(json.dumps(run_config, indent=2, default=str), encoding="utf-8")
    print(str(outdir / "predictions.csv"))
    print(str(outdir / "scores.json"))


__all__ = ["build_parser", "main_with_fit"]
