from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from midas_pipeline import MIDASConfig, MIDASPseudoRTConfig, run_midas_pseudort
from rt_benchmarks.config_io import load_delay_map, select_predictors
from rt_benchmarks.data_io import load_panel_csv, load_target_csv
from rt_benchmarks.outputs import write_predictions, write_run_config, write_scores
from rt_benchmarks.sample_builder import SampleBuilderConfig
from rt_benchmarks.vintage import VintageConfig


def _parse_predictors_arg(value: str | None) -> list[str] | None:
    if value is None:
        return None
    items = [x.strip() for x in value.split(",")]
    items = [x for x in items if x]
    return items or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run MIDAS pseudo-real-time nowcast benchmark."
    )

    parser.add_argument("--x-path", required=True)
    parser.add_argument("--y-path", required=True)
    parser.add_argument("--outdir", default="outputs/midas/basic_nowcast")

    parser.add_argument("--eval-start", required=True)
    parser.add_argument("--eval-end", required=True)

    parser.add_argument("--predictors", default=None)
    parser.add_argument("--predictors-path", default=None)
    parser.add_argument("--predictors-col", default=None)
    parser.add_argument("--n-predictors", type=int, default=10)

    parser.add_argument("--n-monthly-lags", type=int, default=6)
    parser.add_argument("--n-y-lags", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=36)

    parser.add_argument(
        "--weight-scheme",
        choices=["beta", "exp_almon", "equal", "unrestricted"],
        default="beta",
    )
    parser.add_argument(
        "--combination",
        choices=["mean", "median", "trimmed_mean", "inverse_rmse", "top_k"],
        default="mean",
    )
    parser.add_argument("--trimmed-alpha", type=float, default=0.10)
    parser.add_argument("--top-k", type=int, default=None)

    parser.add_argument("--max-iter", type=int, default=200)
    parser.add_argument("--tol", type=float, default=1e-6)
    parser.add_argument("--n-starts", type=int, default=4)
    parser.add_argument("--parameter-bound", type=float, default=3.0)
    parser.add_argument("--fallback-weight-scheme", default="equal")
    parser.add_argument("--ridge-alpha", type=float, default=0.0)

    parser.add_argument("--validation-tail-rows", type=int, default=0)
    parser.add_argument(
        "--moq-specific",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--warm-start",
        action=argparse.BooleanOptionalAction,
        default=True,
    )

    parser.add_argument("--delay-style", default="none")
    parser.add_argument("--delay-map-path", default=None)
    parser.add_argument("--gdp-rel", type=int, default=0)
    parser.add_argument(
        "--no-qe-leak",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show tqdm progress bar over pseudo-real-time vintages.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    X = load_panel_csv(args.x_path)
    y = load_target_csv(args.y_path)

    predictors = select_predictors(
        X_columns=list(X.columns),
        predictors=_parse_predictors_arg(args.predictors),
        predictors_path=args.predictors_path,
        predictors_col=args.predictors_col,
        n_predictors=args.n_predictors,
    )

    delay_map = load_delay_map(args.delay_map_path)

    vintage_cfg = VintageConfig(
        delay_style=args.delay_style,
        delay_map=delay_map,
        gdp_rel=args.gdp_rel,
        no_qe_leak=bool(args.no_qe_leak),
    )

    sample_cfg = SampleBuilderConfig(
        vintage_cfg=vintage_cfg,
        min_train_rows=args.min_train_rows,
        include_row_metadata=True,
    )

    model_cfg = MIDASConfig(
        predictors=predictors,
        n_monthly_lags=args.n_monthly_lags,
        n_y_lags=args.n_y_lags,
        min_train_rows=args.min_train_rows,
        weight_scheme=args.weight_scheme,
        combination=args.combination,
        trimmed_alpha=args.trimmed_alpha,
        top_k=args.top_k,
        max_iter=args.max_iter,
        tol=args.tol,
        n_starts=args.n_starts,
        parameter_bound=args.parameter_bound,
        fallback_weight_scheme=args.fallback_weight_scheme,
        ridge_alpha=args.ridge_alpha,
        validation_tail_rows=args.validation_tail_rows,
        moq_specific=bool(args.moq_specific),
        warm_start=bool(args.warm_start),
    )

    model_cfg.validate()

    eval_cfg = MIDASPseudoRTConfig(
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        horizons=("now",),
        drop_invalid_rows=True,
        show_progress=bool(args.progress),
    )

    pred_df, scores, diagnostics_df, lag_weights_df, predictor_forecasts_df = run_midas_pseudort(
        X_full=X,
        y_full=y,
        eval_cfg=eval_cfg,
        sample_cfg=sample_cfg,
        model_cfg=model_cfg,
        return_details=True,
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    pred_path = write_predictions(pred_df, outdir)
    scores_path = write_scores(scores, outdir)
    run_cfg_path = write_run_config(
        {
            "x_path": args.x_path,
            "y_path": args.y_path,
            "outdir": str(outdir),
            "eval_start": args.eval_start,
            "eval_end": args.eval_end,
            "horizons": ["now"],
            "predictors": predictors,
            "predictors_path": args.predictors_path,
            "predictors_col": args.predictors_col,
            "n_predictors": args.n_predictors,
            "n_monthly_lags": args.n_monthly_lags,
            "n_y_lags": args.n_y_lags,
            "min_train_rows": args.min_train_rows,
            "weight_scheme": args.weight_scheme,
            "combination": args.combination,
            "trimmed_alpha": args.trimmed_alpha,
            "top_k": args.top_k,
            "max_iter": args.max_iter,
            "tol": args.tol,
            "n_starts": args.n_starts,
            "parameter_bound": args.parameter_bound,
            "fallback_weight_scheme": args.fallback_weight_scheme,
            "ridge_alpha": args.ridge_alpha,
            "validation_tail_rows": args.validation_tail_rows,
            "moq_specific": bool(args.moq_specific),
            "warm_start": bool(args.warm_start),
            "delay_style": args.delay_style,
            "delay_map_path": args.delay_map_path,
            "gdp_rel": args.gdp_rel,
            "no_qe_leak": bool(args.no_qe_leak),
        },
        outdir,
    )

    diagnostics_path = outdir / "diagnostics.csv"
    lag_weights_path = outdir / "lag_weights.csv"
    predictor_forecasts_path = outdir / "predictor_forecasts.csv"

    diagnostics_df.to_csv(diagnostics_path, index=False)
    lag_weights_df.to_csv(lag_weights_path, index=False)
    predictor_forecasts_df.to_csv(predictor_forecasts_path, index=False)

    print("MIDAS pseudo-RT run completed")
    print("X shape:", X.shape)
    print("y length:", len(y))
    print("Predictors used:", predictors)
    print("Weight scheme:", args.weight_scheme)
    print("Combination:", args.combination)
    print("Validation tail rows:", args.validation_tail_rows)
    print("MOQ specific:", bool(args.moq_specific))
    print("Warm start:", bool(args.warm_start))
    print("Prediction rows:", len(pred_df))
    print("Scores:", scores)
    print("Saved predictions to:", pred_path)
    print("Saved scores to:", scores_path)
    print("Saved run config to:", run_cfg_path)
    print("Saved diagnostics to:", diagnostics_path)
    print("Saved lag weights to:", lag_weights_path)
    print("Saved predictor forecasts to:", predictor_forecasts_path)


if __name__ == "__main__":
    main()