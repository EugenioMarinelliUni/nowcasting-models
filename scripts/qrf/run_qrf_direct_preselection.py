from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qrf_pipeline.preselection_direct import (
    DirectPreselectionConfig,
    run_direct_hac_preselection,
    write_direct_preselection_outputs,
)
from rt_benchmarks.config_io import load_predictor_list
from rt_benchmarks.data_io import load_panel_csv, load_target_csv


def _month_start(value: str):
    import pandas as pd
    return pd.Timestamp(value).to_period("M").to_timestamp(how="start")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Direct-horizon, lag-aware HAC t-stat QRF predictor preselection."
    )
    p.add_argument("--x-path", required=True)
    p.add_argument("--y-path", required=True)
    p.add_argument("--train-start", required=True)
    p.add_argument("--train-end", required=True)
    p.add_argument("--outdir", required=True)

    p.add_argument("--predictors-path", default=None)
    p.add_argument("--predictors-col", default=None)

    p.add_argument("--top-k", type=int, default=20)
    p.add_argument("--n-lags", type=int, default=3)
    p.add_argument("--n-y-lags", type=int, default=2)
    p.add_argument("--hac-lags", type=int, default=4)
    p.add_argument("--min-obs", type=int, default=36)
    p.add_argument("--include-ar-y", action=argparse.BooleanOptionalAction, default=True)
    return p


def main() -> None:
    args = build_parser().parse_args()

    X = load_panel_csv(args.x_path)
    y = load_target_csv(args.y_path)

    train_start = _month_start(args.train_start)
    train_end = _month_start(args.train_end)

    X = X.loc[(X.index >= train_start) & (X.index <= train_end)]
    y = y.loc[(y.index >= train_start) & (y.index <= train_end)]

    predictors = None
    if args.predictors_path:
        predictors = load_predictor_list(args.predictors_path, column_name=args.predictors_col)

    cfg = DirectPreselectionConfig(
        n_lags=args.n_lags,
        n_y_lags=args.n_y_lags,
        top_k=args.top_k,
        include_ar_y=bool(args.include_ar_y),
        hac_lags=args.hac_lags,
        min_obs=args.min_obs,
    )

    rank_df, selected = run_direct_hac_preselection(
        X=X,
        y=y,
        predictors=predictors,
        cfg=cfg,
    )

    rank_path, selected_path = write_direct_preselection_outputs(
        rank_df=rank_df,
        selected=selected,
        outdir=args.outdir,
    )

    config_path = Path(args.outdir) / "preselection_config.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "x_path": args.x_path,
                "y_path": args.y_path,
                "train_start": args.train_start,
                "train_end": args.train_end,
                "top_k": args.top_k,
                "n_lags": args.n_lags,
                "n_y_lags": args.n_y_lags,
                "hac_lags": args.hac_lags,
                "min_obs": args.min_obs,
                "include_ar_y": bool(args.include_ar_y),
                "selected": selected,
            },
            f,
            indent=2,
        )

    print("Direct HAC preselection completed")
    print("n selected:", len(selected))
    print("selected:", selected)
    print("Saved rank to:", rank_path)
    print("Saved selected predictors to:", selected_path)
    print("Saved config to:", config_path)


if __name__ == "__main__":
    main()
