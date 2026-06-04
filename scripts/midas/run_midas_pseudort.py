from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from midas_pipeline import (
    MIDASConfig,
    MIDASPseudoRTConfig,
    run_midas_pseudort,
    summarize_midas_by_month_of_quarter,
    summarize_midas_by_subperiod,
    summarize_midas_by_target_quarter,
)
from rt_benchmarks.config_io import load_delay_map, select_predictors
from rt_benchmarks.data_io import load_panel_csv, load_target_csv
from rt_benchmarks.outputs import write_predictions, write_run_config, write_scores
from rt_benchmarks.sample_builder import SampleBuilderConfig
from rt_benchmarks.vintage import VintageConfig


@dataclass(frozen=True)
class TargetScaler:
    mean: float
    std: float

    def __post_init__(self) -> None:
        if not np.isfinite(self.mean):
            raise ValueError("target scaler mean must be finite")
        if not np.isfinite(self.std) or self.std <= 0:
            raise ValueError("target scaler std must be positive and finite")

    def inverse(self, z: float) -> float:
        if not np.isfinite(z):
            return float("nan")
        return float(z) * self.std + self.mean


def _parse_predictors_arg(value: str | None) -> list[str] | None:
    if value is None:
        return None
    items = [x.strip() for x in value.split(",")]
    items = [x for x in items if x]
    return items or None


def _load_target_scaler(path: str | Path | None, mean: float | None, std: float | None) -> TargetScaler | None:
    if path is None and mean is None and std is None:
        return None

    if path is not None:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)

        mean_value = None
        for key in ("mean", "mu", "target_mean", "y_mean"):
            if key in obj:
                mean_value = float(obj[key])
                break

        std_value = None
        for key in ("std", "sigma", "target_std", "y_std"):
            if key in obj:
                std_value = float(obj[key])
                break

        if mean_value is None or std_value is None:
            raise KeyError(
                "target scaler JSON must contain one accepted mean key and one accepted std key"
            )
        return TargetScaler(mean=mean_value, std=std_value)

    if mean is None or std is None:
        raise ValueError("Provide both --target-mean and --target-std, or provide --target-scaler-path")

    return TargetScaler(mean=float(mean), std=float(std))


def _default_subperiods(eval_start: str, eval_end: str) -> dict[str, tuple[str, str]]:
    start = str(Path("dummy"))  # placeholder only to avoid accidental date mutation
    del start
    return {
        "full_eval": (eval_start, eval_end),
        "pre_covid_2018_2019": ("2018-01-01", "2019-12-01"),
        "covid_2020_2021": ("2020-01-01", "2021-12-01"),
        "post_covid_2022_2025q1": ("2022-01-01", "2025-03-01"),
        "non_covid_2018_2025q1_pre_plus_post": ("2018-01-01", "2025-03-01"),
    }


def _write_extra_summaries(pred_df, outdir: Path, eval_start: str, eval_end: str, *, raw_available: bool) -> dict[str, str]:
    paths: dict[str, str] = {}

    summary_by_moq = summarize_midas_by_month_of_quarter(pred_df)
    p = outdir / "summary_by_moq.csv"
    summary_by_moq.to_csv(p, index=False)
    paths["summary_by_moq"] = str(p)

    summary_by_target = summarize_midas_by_target_quarter(pred_df)
    p = outdir / "summary_by_target_quarter.csv"
    summary_by_target.to_csv(p, index=False)
    paths["summary_by_target_quarter"] = str(p)

    periods = _default_subperiods(eval_start, eval_end)
    summary_by_subperiod = summarize_midas_by_subperiod(pred_df, periods)
    # Remove rows with no observations to keep short validation windows readable.
    if "n" in summary_by_subperiod.columns:
        summary_by_subperiod = summary_by_subperiod[summary_by_subperiod["n"] > 0].reset_index(drop=True)
    p = outdir / "summary_by_subperiod.csv"
    summary_by_subperiod.to_csv(p, index=False)
    paths["summary_by_subperiod"] = str(p)

    if raw_available:
        raw_moq = summarize_midas_by_month_of_quarter(
            pred_df,
            pred_col="pred_raw",
            actual_col="actual_raw",
        )
        p = outdir / "summary_by_moq_raw.csv"
        raw_moq.to_csv(p, index=False)
        paths["summary_by_moq_raw"] = str(p)

        raw_target = summarize_midas_by_target_quarter(
            pred_df,
            pred_col="pred_raw",
            actual_col="actual_raw",
        )
        p = outdir / "summary_by_target_quarter_raw.csv"
        raw_target.to_csv(p, index=False)
        paths["summary_by_target_quarter_raw"] = str(p)

        raw_subperiod = summarize_midas_by_subperiod(
            pred_df,
            periods,
            pred_col="pred_raw",
            actual_col="actual_raw",
        )
        if "n" in raw_subperiod.columns:
            raw_subperiod = raw_subperiod[raw_subperiod["n"] > 0].reset_index(drop=True)
        p = outdir / "summary_by_subperiod_raw.csv"
        raw_subperiod.to_csv(p, index=False)
        paths["summary_by_subperiod_raw"] = str(p)

    return paths


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
        "--n-jobs",
        type=int,
        default=1,
        help="Parallel jobs over predictor-specific MIDAS fits inside each vintage. Use -1 for all cores.",
    )

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
    parser.add_argument(
        "--accept-nonconverged",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use the best finite nonlinear optimizer result even if scipy reports non-convergence. Use --no-accept-nonconverged to fall back instead.",
    )

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

    parser.add_argument("--y-raw-path", default=None)
    parser.add_argument("--target-scaler-path", default=None)
    parser.add_argument("--target-mean", type=float, default=None)
    parser.add_argument("--target-std", type=float, default=None)
    parser.add_argument(
        "--write-diagnostics",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write diagnostics, lag weights, predictor forecasts, and summary CSVs.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    X = load_panel_csv(args.x_path)
    y = load_target_csv(args.y_path)

    y_raw = load_target_csv(args.y_raw_path) if args.y_raw_path else None
    scaler = _load_target_scaler(
        path=args.target_scaler_path,
        mean=args.target_mean,
        std=args.target_std,
    )
    pred_to_raw_fn = (lambda z, _target_date: scaler.inverse(z)) if scaler is not None else None

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
        n_jobs=args.n_jobs,
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
        accept_nonconverged=bool(args.accept_nonconverged),
    )

    model_cfg.validate()

    eval_cfg = MIDASPseudoRTConfig(
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        horizons=("now",),
        drop_invalid_rows=True,
        show_progress=bool(args.progress),
    )

    result = run_midas_pseudort(
        X_full=X,
        y_full=y,
        eval_cfg=eval_cfg,
        sample_cfg=sample_cfg,
        model_cfg=model_cfg,
        y_raw_full=y_raw,
        pred_to_raw_fn=pred_to_raw_fn,
        return_details=bool(args.write_diagnostics),
    )

    if args.write_diagnostics:
        pred_df, scores, diagnostics_df, lag_weights_df, predictor_forecasts_df = result
    else:
        pred_df, scores = result
        diagnostics_df = lag_weights_df = predictor_forecasts_df = None

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    pred_path = write_predictions(pred_df, outdir)
    scores_path = write_scores(scores, outdir)
    run_cfg_path = write_run_config(
        {
            "x_path": args.x_path,
            "y_path": args.y_path,
            "y_raw_path": args.y_raw_path,
            "target_scaler_path": args.target_scaler_path,
            "target_mean": args.target_mean,
            "target_std": args.target_std,
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
            "n_jobs": args.n_jobs,
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
            "accept_nonconverged": bool(args.accept_nonconverged),
            "delay_style": args.delay_style,
            "delay_map_path": args.delay_map_path,
            "gdp_rel": args.gdp_rel,
            "no_qe_leak": bool(args.no_qe_leak),
            "write_diagnostics": bool(args.write_diagnostics),
        },
        outdir,
    )

    diagnostics_path = outdir / "diagnostics.csv"
    lag_weights_path = outdir / "lag_weights.csv"
    predictor_forecasts_path = outdir / "predictor_forecasts.csv"

    summary_paths: dict[str, str] = {}
    if args.write_diagnostics:
        diagnostics_df.to_csv(diagnostics_path, index=False)
        lag_weights_df.to_csv(lag_weights_path, index=False)
        predictor_forecasts_df.to_csv(predictor_forecasts_path, index=False)
        summary_paths = _write_extra_summaries(
            pred_df,
            outdir=outdir,
            eval_start=args.eval_start,
            eval_end=args.eval_end,
            raw_available=(y_raw is not None and pred_to_raw_fn is not None),
        )

    print("MIDAS pseudo-RT run completed")
    print("X shape:", X.shape)
    print("y length:", len(y))
    print("Predictors used:", predictors)
    print("Weight scheme:", args.weight_scheme)
    print("Combination:", args.combination)
    print("Validation tail rows:", args.validation_tail_rows)
    print("MOQ specific:", bool(args.moq_specific))
    print("Warm start:", bool(args.warm_start))
    print("n_jobs:", args.n_jobs)
    print("Accept nonconverged:", bool(args.accept_nonconverged))
    print("Prediction rows:", len(pred_df))
    print("Scores:", scores)
    print("Saved predictions to:", pred_path)
    print("Saved scores to:", scores_path)
    print("Saved run config to:", run_cfg_path)

    if args.write_diagnostics:
        print("Saved diagnostics to:", diagnostics_path)
        print("Saved lag weights to:", lag_weights_path)
        print("Saved predictor forecasts to:", predictor_forecasts_path)
        for label, path in summary_paths.items():
            print(f"Saved {label} to:", path)


if __name__ == "__main__":
    main()
