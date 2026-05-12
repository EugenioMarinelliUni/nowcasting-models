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
        description="Run initial MIDAS pseudo-real-time nowcast benchmark."
    )

    parser.add_argument("--x-path", required=True, help="Path to monthly predictor CSV")
    parser.add_argument("--y-path", required=True, help="Path to target CSV on the model scale used for fitting")
    parser.add_argument("--outdir", default="outputs/midas/basic_nowcast", help="Output directory")

    parser.add_argument("--eval-start", required=True, help="Evaluation start month, e.g. 2016-01-01")
    parser.add_argument("--eval-end", required=True, help="Evaluation end month, e.g. 2017-12-01")

    parser.add_argument(
        "--predictors",
        default=None,
        help="Comma-separated predictor list. Overrides --predictors-path when provided.",
    )
    parser.add_argument(
        "--predictors-path",
        default=None,
        help="Path to predictor list file (.json/.txt/.csv). Used if --predictors is omitted.",
    )
    parser.add_argument(
        "--predictors-col",
        default=None,
        help="Column name to use when --predictors-path points to a CSV file.",
    )
    parser.add_argument(
        "--n-predictors",
        type=int,
        default=10,
        help="Number of predictors to keep. If a predictor file/list is provided, keeps the first n entries.",
    )

    parser.add_argument("--n-monthly-lags", type=int, default=12)
    parser.add_argument("--n-y-lags", type=int, default=2)
    parser.add_argument("--min-train-rows", type=int, default=30)

    parser.add_argument("--delay-style", default="none")
    parser.add_argument("--delay-map-path", default=None, help="Optional JSON delay-map path")
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
    )

    eval_cfg = MIDASPseudoRTConfig(
        eval_start=args.eval_start,
        eval_end=args.eval_end,
        horizons=("now",),
        drop_invalid_rows=True,
    )

    pred_df, scores = run_midas_pseudort(
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
            "predictors_path": args.predictors_path,
            "predictors_col": args.predictors_col,
            "n_predictors": args.n_predictors,
            "n_monthly_lags": args.n_monthly_lags,
            "n_y_lags": args.n_y_lags,
            "min_train_rows": args.min_train_rows,
            "delay_style": args.delay_style,
            "delay_map_path": args.delay_map_path,
            "gdp_rel": args.gdp_rel,
            "no_qe_leak": bool(args.no_qe_leak),
        },
        outdir,
    )

    print("MIDAS pseudo-RT run completed")
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