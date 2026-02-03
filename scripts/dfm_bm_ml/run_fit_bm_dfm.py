from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from src.dfm_pipeline.dfm_bm_ml.fit import fit_bm_dfm
from src.dfm_pipeline.dfm_bm_ml.spec import BMDfmConfig


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--monthly_csv", type=str, required=True)
    p.add_argument("--quarterly_csv", type=str, required=True)
    p.add_argument("--out_npz", type=str, required=True)

    p.add_argument("--r_by_block", type=int, nargs="+", required=True)
    p.add_argument("--p", type=int, required=True)

    p.add_argument("--standardize", action="store_true", default=False)
    p.add_argument("--no-standardize", dest="standardize", action="store_false")

    p.add_argument("--pca_fill", type=str, default="mean", choices=["mean", "ffill"])

    p.add_argument("--rho_idio_init", type=float, default=0.10)
    p.add_argument("--max_iter", type=int, default=200)
    p.add_argument("--tol", type=float, default=1e-6)

    p.add_argument("--force_var_stability", action="store_true", default=True)
    p.add_argument("--no-force_var_stability", dest="force_var_stability", action="store_false")
    p.add_argument("--var_stability_shrink", type=float, default=0.98)

    p.add_argument("--monthly_meas_var_floor", type=float, default=1e-4)
    p.add_argument("--quarterly_meas_var_floor", type=float, default=1e-4)

    return p.parse_args()


def main() -> None:
    args = _parse_args()

    X = pd.read_csv(args.monthly_csv, index_col=0, parse_dates=True)
    yq = pd.read_csv(args.quarterly_csv, index_col=0, parse_dates=True).iloc[:, 0]

    Y_monthly = X.to_numpy(dtype=float)
    y_quarterly = yq.reindex(X.index).to_numpy(dtype=float)

    scaling_mode = "internal_per_run" if bool(args.standardize) else "external_frozen"

    config = BMDfmConfig(
        r_by_block=tuple(int(x) for x in args.r_by_block),
        p=int(args.p),
        idio_ar1=True,
        rho_idio_init=float(args.rho_idio_init),
        n_quarterly=1,
        mm_weight_style="toolbox",
        quarterly_meas_var_floor=float(args.quarterly_meas_var_floor),
        monthly_meas_var_floor=float(args.monthly_meas_var_floor),
        enforce_quarterly_loading_constraint=True,
        fix_quarterly_R=True,
        max_iter=int(args.max_iter),
        tol=float(args.tol),
        pca_fill=str(args.pca_fill),
        scaling_mode=scaling_mode,
        force_var_stability=bool(args.force_var_stability),
        var_stability_shrink=float(args.var_stability_shrink),
    )

    res = fit_bm_dfm(Y_monthly=Y_monthly, y_quarterly=y_quarterly, config=config)

    out_path = Path(args.out_npz)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    np.savez_compressed(
        out_path,
        loglik=np.array(res.loglik_trace, dtype=float),
        a_smooth=res.a_smooth,
        P_smooth=res.P_smooth,
        P_lag_smooth=res.P_lag_smooth,
        T=res.T,
        Q=res.Q,
        C=res.C,
        R=res.R,
        a0=res.a0,
        P0=res.P0,
        scaler_mu=res.scaler.mu,
        scaler_sd=res.scaler.sd,
        scaler_mode=np.array([res.scaler.mode], dtype=object),
        f_t_idx=res.f_t_idx,
        f_stack_idx=res.f_stack_idx,
    )


if __name__ == "__main__":
    main()
