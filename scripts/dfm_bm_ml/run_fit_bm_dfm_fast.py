from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.fast import fit_bm_dfm_fast


def _read_panel_csv(path: Path, date_col: str = "sasdate") -> pd.DataFrame:
    df = pd.read_csv(path)
    if date_col not in df.columns:
        raise ValueError(f"Panel CSV missing date column {date_col!r}. Columns: {list(df.columns)[:10]} ...")
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).set_index(date_col)
    return df


def _read_target_csv(path: Path, target_col: str, date_col: str = "sasdate") -> pd.Series:
    df = pd.read_csv(path)
    if date_col not in df.columns:
        raise ValueError(f"Target CSV missing date column {date_col!r}. Columns: {list(df.columns)[:10]} ...")
    if target_col not in df.columns:
        raise ValueError(f"Target CSV missing target column {target_col!r}. Columns: {list(df.columns)[:10]} ...")
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col).set_index(date_col)
    return df[target_col].astype(float)


def _align_to_panel_index(
    X: pd.DataFrame,
    y: pd.Series,
) -> tuple[np.ndarray, np.ndarray, list[str], pd.DatetimeIndex]:
    idx = X.index
    y_aligned = y.reindex(idx)
    return X.to_numpy(dtype=float), y_aligned.to_numpy(dtype=float), list(X.columns), idx


def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--panel-csv", required=True, type=str)
    ap.add_argument("--target-csv", required=True, type=str)
    ap.add_argument("--target-col", required=True, type=str)
    ap.add_argument("--outdir", required=True, type=str)

    ap.add_argument("--date-col", default="sasdate", type=str)

    ap.add_argument("--r", required=True, type=int)
    ap.add_argument("--p", required=True, type=int)

    ap.add_argument("--mm-style", default="toolbox", choices=["toolbox", "scaled"])
    ap.add_argument("--max-iter", default=50, type=int)
    ap.add_argument("--tol", default=1e-6, type=float)

    ap.add_argument("--enforce-quarterly-loading-constraint", action="store_true", default=True)
    ap.add_argument(
        "--no-enforce-quarterly-loading-constraint",
        dest="enforce_quarterly_loading_constraint",
        action="store_false",
    )

    ap.add_argument("--fix-quarterly-R", action="store_true", default=True)
    ap.add_argument("--no-fix-quarterly-R", dest="fix_quarterly_R", action="store_false")

    # ------------------------------------------------------------------
    # Standardization toggles:
    # Default is now NO standardization (standardize=False).
    # Use --standardize to enable it.
    # ------------------------------------------------------------------
    std_group = ap.add_mutually_exclusive_group()
    std_group.add_argument(
        "--standardize",
        dest="standardize",
        action="store_true",
        help="Standardize Y inside fit_bm_dfm_fast.",
    )
    std_group.add_argument(
        "--no-standardize",
        dest="standardize",
        action="store_false",
        help="Disable internal standardization (use when inputs are already z-scored).",
    )
    ap.set_defaults(standardize=False)

    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    X_df = _read_panel_csv(Path(args.panel_csv), date_col=args.date_col)
    y_s = _read_target_csv(Path(args.target_csv), target_col=args.target_col, date_col=args.date_col)

    X, y, x_cols, idx = _align_to_panel_index(X_df, y_s)

    cfg = BMDfmConfig(
        r_by_block=(int(args.r),),
        p=int(args.p),
        blocks=None,
        mm_weight_style=args.mm_style,
        enforce_quarterly_loading_constraint=bool(args.enforce_quarterly_loading_constraint),
        fix_quarterly_R=bool(args.fix_quarterly_R),
        max_iter=int(args.max_iter),
        tol=float(args.tol),
        standardize=bool(args.standardize),
    )

    res = fit_bm_dfm_fast(Y_monthly=X, y_quarterly=y, config=cfg)

    npz_path = outdir / "bm_dfm_fit_fast.npz"
    json_path = outdir / "bm_dfm_fit_fast.json"

    np.savez_compressed(
        npz_path,
        T=res.T,
        Q=res.Q,
        C=res.C,
        R=res.R,
        a0=res.a0,
        P0=res.P0,
        a_smooth=res.a_smooth,
        P_smooth=res.P_smooth,
        P_lag_smooth=res.P_lag_smooth,
        f_t_idx=res.f_t_idx,
        f_stack_idx=res.f_stack_idx,
        date_index=idx.astype("datetime64[ns]").values,
        x_columns=np.array(x_cols, dtype=object),
    )

    meta = {
        "panel_csv": args.panel_csv,
        "target_csv": args.target_csv,
        "date_col": args.date_col,
        "target_col": args.target_col,
        "x_columns": x_cols,
        "n_obs": int(X.shape[0]),
        "n_monthly": int(X.shape[1]),
        "config": {
            "r_by_block": [int(args.r)],
            "p": int(args.p),
            "mm_weight_style": args.mm_style,
            "enforce_quarterly_loading_constraint": bool(args.enforce_quarterly_loading_constraint),
            "fix_quarterly_R": bool(args.fix_quarterly_R),
            "max_iter": int(args.max_iter),
            "tol": float(args.tol),
            "standardize": bool(args.standardize),
        },
        "loglik_trace": [float(v) for v in res.loglik_trace],
    }
    json_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(str(npz_path))
    print(str(json_path))


if __name__ == "__main__":
    main()
