from __future__ import annotations

import argparse
import json
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, Tuple, List

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig
from dfm_pipeline.dfm_bm_ml.fast import fit_bm_dfm_fast_numba


def _filter_kwargs_for_dataclass(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    if not is_dataclass(cls):
        return kwargs
    valid = {f.name for f in fields(cls)}
    return {k: v for k, v in kwargs.items() if k in valid}


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
) -> Tuple[np.ndarray, np.ndarray, List[str], pd.DatetimeIndex]:
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
    ap.add_argument("--pca-fill", default="mean", choices=["mean", "ffill"])

    ap.add_argument("--max-iter", default=200, type=int)
    ap.add_argument("--tol", default=1e-6, type=float)

    ap.add_argument("--rho-idio-init", default=0.10, type=float)
    ap.add_argument("--min-var", default=1e-8, type=float)
    ap.add_argument("--jitter", default=1e-12, type=float)

    ap.add_argument("--monthly-meas-var-floor", default=1e-4, type=float)
    ap.add_argument("--quarterly-meas-var-floor", default=1e-4, type=float)

    ap.add_argument("--scaling-mode", default="external_frozen", choices=["external_frozen", "internal_per_run"])

    ap.add_argument("--idio-ar1", dest="idio_ar1", action="store_true", default=True)
    ap.add_argument("--no-idio-ar1", dest="idio_ar1", action="store_false")

    ap.add_argument("--enforce-quarterly-loading-constraint", action="store_true", default=True)
    ap.add_argument(
        "--no-enforce-quarterly-loading-constraint",
        dest="enforce_quarterly_loading_constraint",
        action="store_false",
    )

    ap.add_argument("--fix-quarterly-R", action="store_true", default=True)
    ap.add_argument("--no-fix-quarterly-R", dest="fix_quarterly_R", action="store_false")

    ap.add_argument("--force-var-stability", dest="force_var_stability", action="store_true", default=True)
    ap.add_argument("--no-force-var-stability", dest="force_var_stability", action="store_false")
    ap.add_argument("--var-stability-shrink", default=0.98, type=float)

    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    X_df = _read_panel_csv(Path(args.panel_csv), date_col=args.date_col)
    y_s = _read_target_csv(Path(args.target_csv), target_col=args.target_col, date_col=args.date_col)

    X, y, x_cols, idx = _align_to_panel_index(X_df, y_s)

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
    cfg = BMDfmConfig(**model_kwargs)

    res = fit_bm_dfm_fast_numba(Y_monthly=X, y_quarterly=y, config=cfg)

    npz_path = outdir / "bm_dfm_fit_fast_numba.npz"
    json_path = outdir / "bm_dfm_fit_fast_numba.json"

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
        "config": model_kwargs,
        "loglik_trace": [float(v) for v in res.loglik_trace],
    }
    json_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(str(npz_path))
    print(str(json_path))


if __name__ == "__main__":
    main()
