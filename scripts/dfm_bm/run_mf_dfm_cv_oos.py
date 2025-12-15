#!/usr/bin/env python
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_dyn.em_dfm_mf_gdp import (
    em_dfm_mf_gdp_full,
    build_state_matrices_from_params,
    kalman_filter_only,
    mariano_murasawa_from_monthly,
)


def parse_int_list(s: str):
    return [int(x) for x in s.split(",") if x.strip()]


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Train/validation/test for mixed-frequency DFM (Mariano–Murasawa): "
            "grid over (r, p), select on validation RMSE, then test OOS."
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
        default="2",
        help="Comma-separated list of r values, e.g. '1,2,3'.",
    )
    parser.add_argument(
        "--p-grid",
        type=str,
        default="1",
        help="Comma-separated list of p values, e.g. '0,1,2'.",
    )
    parser.add_argument(
        "--train-end",
        type=str,
        default="1999-12-01",
        help="Last date (inclusive) of training window, e.g. '1999-12-01'.",
    )
    parser.add_argument(
        "--val-end",
        type=str,
        default="2009-12-01",
        help="Last date (inclusive) of validation window, e.g. '2009-12-01'.",
    )
    parser.add_argument(
        "--max-iter",
        type=int,
        default=30,
        help="Max EM iterations for training.",
    )
    parser.add_argument(
        "--tol",
        type=float,
        default=1e-4,
        help="EM convergence tolerance.",
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

    # ensure same index
    panel, y_q = panel.align(y_q, join="inner", axis=0)
    dates = pd.DatetimeIndex(panel.index)  # ensure DatetimeIndex
    T_total, n = panel.shape

    print(f"Loaded panel: {panel_path} -> T={T_total}, n={n}")
    print(f"Loaded target: {target_path}")
    print(f"Date range: {dates[0]} .. {dates[-1]}")
    print(f"Train end: {train_end}, Val end: {val_end}")

    train_mask = dates <= train_end
    val_mask = (dates > train_end) & (dates <= val_end)
    test_mask = dates > val_end

    # quarter-end months on your MS index (Mar/Jun/Sep/Dec)
    quarter_end_months = {3, 6, 9, 12}
    is_qe = dates.month.isin(quarter_end_months)

    results_dir = Path("results") / "dfm_mf_mm"
    results_dir.mkdir(parents=True, exist_ok=True)

    cv_rows = []

    # 2) Grid over (r, p)
    for r in r_grid:
        for p in p_grid:
            print(f"\n=== (r={r}, p={p}) ===")

            # 2a) Train on 1990–1999
            X_train = panel.values[train_mask]
            y_train = y_q.values[train_mask]

            print("  Training EM on training window...")
            params_train = em_dfm_mf_gdp_full(
                X=X_train,
                y_q=y_train,
                r=r,
                p=p,
                max_iter=args.max_iter,
                tol=args.tol,
                verbose=False,
                use_tqdm=True,
            )

            # 2b) Build state matrices from trained params
            T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0 = build_state_matrices_from_params(
                params_train,
                n=n,
                r=r,
                p=p,
            )

            # 2c) Build observation array Y for full sample
            X_all = panel.values
            y_all = y_q.values
            Y = np.column_stack([X_all, y_all])

            # No future quarterly information after training end for OOS nowcasts
            Y[~train_mask, -1] = np.nan

            # 2d) One-sided Kalman filter over full sample
            print("  Running one-sided Kalman filter (frozen params)...")
            alpha_filt, _ = kalman_filter_only(
                Y,
                T_mat,
                Q_mat,
                C_mat,
                R_meas,
                a0,
                P0,
            )

            # 2e) Extract monthly GDP and MM quarterly approximation
            y_m_hat_all = alpha_filt[:, idx_y0]
            y_q_hat_all = mariano_murasawa_from_monthly(y_m_hat_all)

            # 2f) Validation RMSE on 2000–2009 (quarter-end months)
            val_eval_mask = (
                val_mask
                & is_qe
                & ~np.isnan(y_q.values)
                & ~np.isnan(y_q_hat_all)
            )

            if not np.any(val_eval_mask):
                rmse_val = np.nan
                print("  [WARN] No validation points for this (r,p).")
            else:
                errors_val = y_q_hat_all[val_eval_mask] - y_q.values[val_eval_mask]
                rmse_val = np.sqrt(np.mean(errors_val**2))
                print(f"  Validation RMSE (2000–2009, standardized): {rmse_val:.4f}")

            cv_rows.append(
                {
                    "r": r,
                    "p": p,
                    "rmse_val": rmse_val,
                }
            )

    cv_df = pd.DataFrame(cv_rows)
    cv_path = results_dir / f"mf_dfm_cv_summary_{label}.csv"
    cv_df.to_csv(cv_path, index=False)
    print(f"\nSaved CV summary to: {cv_path}")

    # 3) Pick best (r, p) by validation RMSE
    cv_df_clean = cv_df.dropna(subset=["rmse_val"])
    if cv_df_clean.empty:
        print("No valid RMSE values; cannot select best hyperparameters.")
        return

    best_row = cv_df_clean.loc[cv_df_clean["rmse_val"].idxmin()]
    best_r = int(best_row["r"])
    best_p = int(best_row["p"])
    best_rmse = float(best_row["rmse_val"])
    print(f"\nBest (r, p) on validation: r={best_r}, p={best_p}, RMSE_val={best_rmse:.4f}")

    # 4) Final training on Train+Validation, test on 2010–end
    print("\n=== Final training on Train+Validation, testing on 2010–end ===")

    tv_mask = dates <= val_end
    X_tv = panel.values[tv_mask]
    y_tv = y_q.values[tv_mask]

    params_final = em_dfm_mf_gdp_full(
        X=X_tv,
        y_q=y_tv,
        r=best_r,
        p=best_p,
        max_iter=args.max_iter,
        tol=args.tol,
        verbose=False,
        use_tqdm=True,
    )

    T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0 = build_state_matrices_from_params(
        params_final,
        n=n,
        r=best_r,
        p=best_p,
    )

    X_all = panel.values
    y_all = y_q.values
    Y = np.column_stack([X_all, y_all])

    # No future quarterly information after val_end for true OOS test
    Y[~tv_mask, -1] = np.nan

    alpha_filt, _ = kalman_filter_only(
        Y,
        T_mat,
        Q_mat,
        C_mat,
        R_meas,
        a0,
        P0,
    )

    y_m_hat_all = alpha_filt[:, idx_y0]
    y_q_hat_all = mariano_murasawa_from_monthly(y_m_hat_all)

    test_eval_mask = (
        test_mask
        & is_qe
        & ~np.isnan(y_q.values)
        & ~np.isnan(y_q_hat_all)
    )

    if not np.any(test_eval_mask):
        print("No test points; cannot compute test RMSE.")
        rmse_test = np.nan
    else:
        errors_test = y_q_hat_all[test_eval_mask] - y_q.values[test_eval_mask]
        rmse_test = np.sqrt(np.mean(errors_test**2))
        print(f"Test RMSE (2010–end, standardized): {rmse_test:.4f}")

    # 5) Save full OOS nowcast series for best model
    out_df = pd.DataFrame(
        {
            "y_q": y_q.values,
            "y_q_hat": y_q_hat_all,
            "error": y_q_hat_all - y_q.values,
            "y_m_hat": y_m_hat_all,
        },
        index=dates,
    )
    out_path = results_dir / f"mf_dfm_oos_{label}_r{best_r}_p{best_p}.csv"
    out_df.to_csv(out_path)
    print(f"Saved OOS nowcast series for best model to: {out_path}")


if __name__ == "__main__":
    main()
