#!/usr/bin/env python
import argparse
from dataclasses import dataclass
from pathlib import Path
import concurrent.futures as cf

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_dyn.em_dfm_mf_gdp_numba import (
    em_dfm_mf_gdp_full,
    build_state_matrices_from_params,
    kalman_filter_only,
    mariano_murasawa_from_monthly,
)


def parse_int_list(s: str):
    return [int(x) for x in s.split(",") if x.strip()]


@dataclass
class CVResult:
    r: int
    p: int
    rmse_val: float


def _fit_one_config(job) -> CVResult:
    (
        r,
        p,
        panel_values,
        y_values,
        train_mask,
        val_mask,
        is_qe,
        max_iter,
        tol,
        n,
    ) = job

    try:
        # 1) Train EM on training window
        X_train = panel_values[train_mask]
        y_train = y_values[train_mask]

        params_train = em_dfm_mf_gdp_full(
            X=X_train,
            y_q=y_train,
            r=r,
            p=p,
            max_iter=max_iter,
            tol=tol,
            verbose=False,
            use_tqdm=False,  # no tqdm in workers
        )

        # 2) Build state matrices
        T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0 = build_state_matrices_from_params(
            params_train,
            n=n,
            r=r,
            p=p,
        )

        # 3) Build Y and hide quarterly info after train_end
        X_all = panel_values
        y_all = y_values
        Y = np.column_stack([X_all, y_all])

        Y[~train_mask, -1] = np.nan

        # 4) One-sided Kalman filter over full sample
        alpha_filt, _ = kalman_filter_only(
            Y=Y,
            T=T_mat,
            Q=Q_mat,
            C=C_mat,
            R_meas=R_meas,
            a0=a0,
            P0=P0,
        )

        # 5) Monthly GDP + MM quarterly approximation
        y_m_hat_all = alpha_filt[:, idx_y0]
        y_q_hat_all = mariano_murasawa_from_monthly(y_m_hat_all)

        # 6) Validation RMSE on val window, quarter-ends only
        val_eval_mask = (
            val_mask
            & is_qe
            & ~np.isnan(y_all)
            & ~np.isnan(y_q_hat_all)
        )

        if not np.any(val_eval_mask):
            rmse_val = np.nan
        else:
            errors_val = y_q_hat_all[val_eval_mask] - y_all[val_eval_mask]
            rmse_val = float(np.sqrt(np.mean(errors_val**2)))

    except Exception:
        # if EM/Kalman explodes for this (r,p), mark it as invalid
        rmse_val = np.nan

    return CVResult(r=r, p=p, rmse_val=rmse_val)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Fast train/validation/test for mixed-frequency DFM (Mariano–Murasawa): "
            "grid over (r, p), select on validation RMSE, then test OOS. "
            "Uses numba Kalman and parallel CV."
        )
    )
    parser.add_argument(
        "--panel-csv",
        required=True,
        type=str,
        help="Path to standardized monthly panel CSV.",
    )
    parser.add_argument(
        "--target-csv",
        required=True,
        type=str,
        help=(
            "Path to mixed-frequency target CSV "
            "(quarterly GDP on monthly index, NaN except quarter-end months)."
        ),
    )
    parser.add_argument(
        "--r-grid",
        type=str,
        default="1,2,3",
        help="Comma-separated list of r values, e.g. '1,2,3'.",
    )
    parser.add_argument(
        "--p-grid",
        type=str,
        default="0,1,2",
        help="Comma-separated list of p values, e.g. '0,1,2'.",
    )
    parser.add_argument(
        "--train-end",
        type=str,
        default="2005-12-01",
        help="Last date (inclusive) of training window.",
    )
    parser.add_argument(
        "--val-end",
        type=str,
        default="2015-12-01",
        help="Last date (inclusive) of validation window.",
    )
    parser.add_argument(
        "--max-iter",
        type=int,
        default=100,
        help="Max EM iterations for training (default: 100).",
    )
    parser.add_argument(
        "--tol",
        type=float,
        default=1e-4,
        help="EM convergence tolerance on relative loglik (default: 1e-4).",
    )
    parser.add_argument(
        "--label",
        type=str,
        required=True,
        help="Label used in output filenames.",
    )

    args = parser.parse_args()

    panel_path = Path(args.panel_csv)
    target_path = Path(args.target_csv)
    label = args.label

    r_grid = parse_int_list(args.r_grid)
    p_grid = parse_int_list(args.p_grid)

    train_end = pd.to_datetime(args.train_end)
    val_end = pd.to_datetime(args.val_end)

    # 1) Load data
    panel = pd.read_csv(panel_path, index_col=0, parse_dates=True)
    y_q = pd.read_csv(target_path, index_col=0, parse_dates=True).iloc[:, 0]

    # align
    panel, y_q = panel.align(y_q, join="inner", axis=0)

    # explicitly cast to DatetimeIndex so .month is well-typed
    dates = pd.DatetimeIndex(panel.index)
    T_total, n = panel.shape

    print(f"Loaded panel: {panel_path} -> T={T_total}, n={n}")
    print(f"Loaded target: {target_path}")
    print(f"Date range: {dates[0]} .. {dates[-1]}")
    print(f"Train end: {train_end}, Val end: {val_end}")

    train_mask = np.asarray(dates <= train_end, dtype=bool)
    val_mask = np.asarray((dates > train_end) & (dates <= val_end), dtype=bool)
    test_mask = np.asarray(dates > val_end, dtype=bool)

    quarter_end_months = {3, 6, 9, 12}
    is_qe = np.asarray(dates.month.isin(quarter_end_months), dtype=bool)

    panel_values = panel.to_numpy(dtype=float)
    y_values = y_q.to_numpy(dtype=float)

    results_dir = Path("results") / "dfm_mf_mm"
    results_dir.mkdir(parents=True, exist_ok=True)

    # 2) Build jobs for parallel CV
    jobs = []
    for r in r_grid:
        for p in p_grid:
            jobs.append(
                (
                    r,
                    p,
                    panel_values,
                    y_values,
                    train_mask,
                    val_mask,
                    is_qe,
                    args.max_iter,
                    args.tol,
                    n,
                )
            )

    # 3) Parallel CV
    cv_rows = []
    print("\n=== Grid search over (r, p) ===")
    with cf.ProcessPoolExecutor() as ex:
        for res in ex.map(_fit_one_config, jobs):
            print(f"(r={res.r}, p={res.p}) -> Validation RMSE: {res.rmse_val}")
            cv_rows.append(
                {"r": res.r, "p": res.p, "rmse_val": res.rmse_val}
            )

    cv_df = pd.DataFrame(cv_rows)
    cv_path = results_dir / f"mf_dfm_cv_summary_{label}.csv"
    cv_df.to_csv(cv_path, index=False)
    print(f"\nSaved CV summary to: {cv_path}")

    # 4) Select best (r,p)
    cv_df_clean = cv_df.dropna(subset=["rmse_val"])
    if cv_df_clean.empty:
        print("No valid RMSE values; cannot select best hyperparameters.")
        return

    best_row = cv_df_clean.loc[cv_df_clean["rmse_val"].idxmin()]
    best_r = int(best_row["r"])
    best_p = int(best_row["p"])
    best_rmse = float(best_row["rmse_val"])
    print(
        f"\nBest (r, p) on validation: r={best_r}, p={best_p}, "
        f"RMSE_val={best_rmse:.4f}"
    )

    # 5) Final training on Train+Validation
    print("\n=== Final training on Train+Validation, testing on > val_end ===")

    tv_mask = np.asarray(dates <= val_end, dtype=bool)
    X_tv = panel_values[tv_mask]
    y_tv = y_values[tv_mask]

    params_final = em_dfm_mf_gdp_full(
        X=X_tv,
        y_q=y_tv,
        r=best_r,
        p=best_p,
        max_iter=args.max_iter,
        tol=args.tol,
        verbose=False,
        use_tqdm=True,  # single bar for final fit
    )

    T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0 = build_state_matrices_from_params(
        params_final,
        n=n,
        r=best_r,
        p=best_p,
    )

    X_all = panel_values
    y_all = y_values
    Y = np.column_stack([X_all, y_all])

    Y[~tv_mask, -1] = np.nan

    alpha_filt, _ = kalman_filter_only(
        Y=Y,
        T=T_mat,
        Q=Q_mat,
        C=C_mat,
        R_meas=R_meas,
        a0=a0,
        P0=P0,
    )

    y_m_hat_all = alpha_filt[:, idx_y0]
    y_q_hat_all = mariano_murasawa_from_monthly(y_m_hat_all)

    test_eval_mask = (
        test_mask
        & is_qe
        & ~np.isnan(y_all)
        & ~np.isnan(y_q_hat_all)
    )

    if not np.any(test_eval_mask):
        print("No test points; cannot compute test RMSE.")
        rmse_test = np.nan
    else:
        errors_test = y_q_hat_all[test_eval_mask] - y_all[test_eval_mask]
        rmse_test = float(np.sqrt(np.mean(errors_test**2)))
        print("Test RMSE (> val_end, standardized): {:.4f}".format(rmse_test))

    # 6) Save full OOS nowcast series for best model
    out_df = pd.DataFrame(
        {
            "y_q": y_all,
            "y_q_hat": y_q_hat_all,
            "error": y_q_hat_all - y_all,
            "y_m_hat": y_m_hat_all,
        },
        index=dates,
    )
    out_path = results_dir / f"mf_dfm_oos_{label}_r{best_r}_p{best_p}.csv"
    out_df.to_csv(out_path)
    print(f"Saved OOS nowcast series for best model to: {out_path}")


if __name__ == "__main__":
    main()
