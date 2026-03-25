from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd

from qrf_pipeline import QRFConfig
from qrf_pipeline.pseudort import QRFPseudoRTConfig, run_qrf_pseudort
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


def _parse_max_features(value: str) -> str | float:
    try:
        return float(value)
    except ValueError:
        return value


def _select_predictors(
    X: pd.DataFrame,
    predictors: list[str] | None,
    n_predictors: int,
) -> list[str]:
    if predictors is not None:
        missing = [c for c in predictors if c not in X.columns]
        if missing:
            raise ValueError(f"Requested predictors not found in X: {missing}")
        return predictors

    if n_predictors <= 0:
        raise ValueError("--n-predictors must be >= 1 when --predictors is not provided")

    return list(X.columns[:n_predictors])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run initial point-forecast QRF pseudo-real-time nowcast benchmark."
    )

    parser.add_argument(
        "--x-path",
        required=True,
        help="Path to monthly predictor CSV",
    )
    parser.add_argument(
        "--y-path",
        required=True,
        help="Path to target CSV on the model scale used for fitting",
    )
    parser.add_argument(
        "--outdir",
        default="outputs/qrf/basic_nowcast",
        help="Output directory",
    )

    parser.add_argument(
        "--eval-start",
        required=True,
        help="Evaluation start month, e.g. 2016-01-01",
    )
    parser.add_argument(
        "--eval-end",
        required=True,
        help="Evaluation end month, e.g. 2017-12-01",
    )

    parser.add_argument(
        "--predictors",
        default=None,
        help="Comma-separated predictor list. If omitted, the first n predictors are used.",
    )
    parser.add_argument(
        "--n-predictors",
        type=int,
        default=10,
        help="Number of predictors to use if --predictors is omitted",
    )

    parser.add_argument("--n-lags", type=int, default=3)
    parser.add_argument("--n-y-lags", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=36)

    parser.add_argument("--n-estimators", type=int, default=500)
    parser.add_argument("--min-samples-leaf", type=int, default=10)
    parser.add_argument("--max-features", default="sqrt")
    parser.add_argument("--random-state", type=int, default=0)

    parser.add_argument("--delay-style", default="none")
    parser.add_argument("--gdp-rel", type=int, default=0)
    parser.add_argument(
        "--no-qe-leak",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Apply quarter-end leakage guard (default: enabled)",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    X = load_panel_csv(args.x_path)
    y = load_target_csv(args.y_path)

    predictors_arg = _parse_predictors_arg(args.predictors)
    predictors = _select_predictors(
        X=X,
        predictors=predictors_arg,
        n_predictors=args.n_predictors,
    )

    vintage_cfg = VintageConfig(
        delay_style=args.delay_style,
        delay_map=None,
        gdp_rel=args.gdp_rel,
        no_qe_leak=bool(args.no_qe_leak),
    )

    sample_cfg = SampleBuilderConfig(
        vintage_cfg=vintage_cfg,
        min_train_rows=args.min_train_rows,
        include_row_metadata=True,
    )

    model_cfg = QRFConfig(
        predictors=predictors,
        n_lags=args.n_lags,
        n_y_lags=args.n_y_lags,
        min_train_rows=args.min_train_rows,
        n_estimators=args.n_estimators,
        min_samples_leaf=args.min_samples_leaf,
        max_features=_parse_max_features(args.max_features),
        random_state=args.random_state,
    )

    eval_cfg = QRFPseudoRTConfig(
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        horizons=("now",),
        drop_invalid_rows=True,
    )

    pred_df, scores = run_qrf_pseudort(
        X_full=X,
        y_full=y,
        eval_cfg=eval_cfg,
        sample_cfg=sample_cfg,
        model_cfg=model_cfg,
    )

    outdir = Path(args.outdir)

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
            "n_lags": args.n_lags,
            "n_y_lags": args.n_y_lags,
            "min_train_rows": args.min_train_rows,
            "n_estimators": args.n_estimators,
            "min_samples_leaf": args.min_samples_leaf,
            "max_features": _parse_max_features(args.max_features),
            "random_state": args.random_state,
            "delay_style": args.delay_style,
            "gdp_rel": args.gdp_rel,
            "no_qe_leak": bool(args.no_qe_leak),
        },
        outdir,
    )

    print("QRF pseudo-RT run completed")
    print("X shape:", X.shape)
    print("y length:", len(y))
    print("Predictors used:", predictors)
    print("Prediction rows:", len(pred_df))
    print("Scores:", scores)
    print("Saved predictions to:", pred_path)
    print("Saved scores to:", scores_path)
    print("Saved run config to:", run_cfg_path)


if __name__ == "__main__":
    main()