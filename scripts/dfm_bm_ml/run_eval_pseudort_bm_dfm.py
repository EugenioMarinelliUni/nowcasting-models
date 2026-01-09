from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm

from dfm_pipeline.dfm_bm_ml import BMDfmConfig, fit_bm_dfm
from dfm_pipeline.eval_pseudort.bm_pseudort import (
    PseudoRTEvalConfig,
    run_pseudo_rt_eval,
)


def _read_panel(path: Path, date_col: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if date_col not in df.columns:
        raise ValueError(f"Panel missing {date_col!r}. Columns: {list(df.columns)[:10]} ...")
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).set_index(date_col)
    return df


def _read_target(path: Path, date_col: str, target_col: str) -> pd.Series:
    df = pd.read_csv(path)
    if date_col not in df.columns:
        raise ValueError(f"Target missing {date_col!r}. Columns: {list(df.columns)[:10]} ...")
    if target_col not in df.columns:
        raise ValueError(f"Target missing {target_col!r}. Columns: {list(df.columns)[:10]} ...")
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).set_index(date_col)
    return df[target_col].astype(float)


def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--panel-csv", required=True, type=str)
    ap.add_argument("--target-csv", required=True, type=str)
    ap.add_argument("--date-col", default="sasdate", type=str)
    ap.add_argument("--target-col", required=True, type=str)

    ap.add_argument("--outdir", required=True, type=str)

    # model hyperparameters
    ap.add_argument("--r", required=True, type=int)
    ap.add_argument("--p", required=True, type=int)
    ap.add_argument("--mm-style", default="toolbox", choices=["toolbox", "scaled"])
    ap.add_argument("--max-iter", default=200, type=int)
    ap.add_argument("--tol", default=1e-6, type=float)

    ap.add_argument("--no-standardize", dest="standardize", action="store_false", default=True)
    ap.add_argument("--standardize", dest="standardize", action="store_true")

    ap.add_argument("--no-fix-quarterly-R", dest="fix_quarterly_R", action="store_false", default=True)
    ap.add_argument("--fix-quarterly-R", dest="fix_quarterly_R", action="store_true")

    ap.add_argument("--no-enforce-quarterly-loading-constraint",
                    dest="enforce_quarterly_loading_constraint", action="store_false", default=True)
    ap.add_argument("--enforce-quarterly-loading-constraint",
                    dest="enforce_quarterly_loading_constraint", action="store_true")

    # evaluation window
    ap.add_argument("--eval-start", required=True, type=str)  # YYYY-MM-DD
    ap.add_argument("--eval-end", required=True, type=str)    # YYYY-MM-DD

    # pseudo real-time masking (optional)
    ap.add_argument("--delay-style", default="none", choices=["none", "trailing_nan", "json_map"])
    ap.add_argument("--delay-json", default=None, type=str)
    ap.add_argument("--gdp-rel", default=0, type=int)

    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    X = _read_panel(Path(args.panel_csv), date_col=args.date_col)
    y = _read_target(Path(args.target_csv), date_col=args.date_col, target_col=args.target_col)

    # align target to panel monthly grid
    y = y.reindex(X.index)

    model_cfg = BMDfmConfig(
        r_by_block=(int(args.r),),
        p=int(args.p),
        blocks=None,
        mm_weight_style=str(args.mm_style),
        enforce_quarterly_loading_constraint=bool(args.enforce_quarterly_loading_constraint),
        fix_quarterly_R=bool(args.fix_quarterly_R),
        max_iter=int(args.max_iter),
        tol=float(args.tol),
        standardize=bool(args.standardize),
    )

    eval_cfg = PseudoRTEvalConfig(
        eval_start=str(args.eval_start),
        eval_end=str(args.eval_end),
        delay_style=str(args.delay_style),
        delay_json=args.delay_json,
        gdp_rel=int(args.gdp_rel),
        horizons=("bac", "now", "for"),
    )

    # progress bar over evaluation months
    # (run_pseudo_rt_eval already loops; wrap at a higher level by monkeypatching tqdm if needed)
    pred_df, scores = run_pseudo_rt_eval(
        X_full=X,
        y_full=y,
        fit_fn=fit_bm_dfm,
        model_config=model_cfg,
        eval_cfg=eval_cfg,
    )

    pred_path = outdir / "pseudort_predictions.csv"
    scores_path = outdir / "pseudort_scores.json"

    pred_df.to_csv(pred_path, index=False)
    scores_path.write_text(json.dumps(scores, indent=2, default=str), encoding="utf-8")

    print(str(pred_path))
    print(str(scores_path))


if __name__ == "__main__":
    main()
