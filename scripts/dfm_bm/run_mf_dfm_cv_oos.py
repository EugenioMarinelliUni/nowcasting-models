# scripts/dfm_bm/run_mf_dfm_cv_oos.py
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


def compute_window_metrics_mm(
    dates: pd.DatetimeIndex,
    y_hat: np.ndarray,
    y_obs: np.ndarray,
    window_mask: np.ndarray,
):
    """
    Compute within-quarter horizon RMSEs (m1/m2/m3) and pooled MSE/RMSE over all horizons.

    Assumes Mariano–Murasawa stamping for y_obs:
      - y_obs is observed (finite) only at Mar/Jun/Sep/Dec.

    To avoid cross-window mixing at boundaries, we define the evaluation set by quarter-end months j such that:
      - window_mask[j] True
      - y_obs[j] finite
      - month(j) in {3,6,9,12}

    For each quarter-end j, we evaluate errors at:
      - m1: j-2 (Jan/Apr/Jul/Oct) if within window
      - m2: j-1 (Feb/May/Aug/Nov) if within window
      - m3: j   (Mar/Jun/Sep/Dec) if within window
    """
    dates = pd.DatetimeIndex(dates)
    window_mask = np.asarray(window_mask, dtype=bool)

    qend_mask = (
        window_mask
        & np.isfinite(y_obs)
        & np.isin(dates.month, [3, 6, 9, 12])
    )
    qend_idx = np.where(qend_mask)[0]

    e1, e2, e3, epool = [], [], [], []

    for j in qend_idx:
        yq = float(y_obs[j])

        t1 = j - 2
        if t1 >= 0 and window_mask[t1] and np.isfinite(y_hat[t1]):
            err = float(y_hat[t1] - yq)
            e1.append(err)
            epool.append(err)

        t2 = j - 1
        if t2 >= 0 and window_mask[t2] and np.isfinite(y_hat[t2]):
            err = float(y_hat[t2] - yq)
            e2.append(err)
            epool.append(err)

        t3 = j
        if window_mask[t3] and np.isfinite(y_hat[t3]):
            err = float(y_hat[t3] - yq)
            e3.append(err)
            epool.append(err)

    def _rmse(vals):
        if len(vals) == 0:
            return np.nan
        v = np.asarray(vals, dtype=float)
        return float(np.sqrt(np.mean(v * v)))

    def _mse(vals):
        if len(vals) == 0:
            return np.nan
        v = np.asarray(vals, dtype=float)
        return float(np.mean(v * v))

    rmse_m1 = _rmse(e1)
    rmse_m2 = _rmse(e2)
    rmse_m3 = _rmse(e3)
    pooled_mse = _mse(epool)
    pooled_rmse = float(np.sqrt(pooled_mse)) if np.isfinite(pooled_mse) else np.nan

    return {
        "rmse_m1": rmse_m1,
        "rmse_m2": rmse_m2,
        "rmse_m3": rmse_m3,
        "pooled_mse": pooled_mse,
        "pooled_rmse": pooled_rmse,
        "n_m1": len(e1),
        "n_m2": len(e2),
        "n_m3": len(e3),
        "n_pool": len(epool),
    }


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Train/validation/test for mixed-frequency DFM (Mariano–Murasawa): "
            "grid over (r, p), select on validation RMSE, then test OOS. "
            "Also reports within-quarter RMSE_m1/m2/m3 and pooled MSE over all horizons."
        )
    )
    parser.add_argument("--panel-csv", required=True, type=str)
    parser.add_argument("--target-csv", required=True, type=str)

    parser.add_argument("--r-grid", type=str, default="1,2,3")
    parser.add_argument("--p-grid", type=str, default="1,2")

    parser.add_argument("--train-end", type=str, default="1999-12-01")
    parser.add_argument("--val-end", type=str, default="2009-12-01")

    parser.add_argument("--max-iter", type=int, default=30)
    parser.add_argument("--tol", type=float, default=1e-4)

    parser.add_argument("--label", type=str, required=True)

    parser.add_argument(
        "--results-dir",
        type=str,
        default=str(Path("results") / "dfm_mf_mm"),
        help="Output directory for CSV artifacts (CV summary, best-model OOS series).",
    )

    parser.add_argument(
        "--ragged-mode",
        type=str,
        default="none",
        choices=["none", "mask"],
        help="If 'mask', apply a (T,n) availability mask CSV to the panel (sets unavailable entries to NaN).",
    )
    parser.add_argument(
        "--mask-csv",
        type=str,
        default=None,
        help="Availability mask CSV with the same index/columns as the panel. Used only if --ragged-mode mask.",
    )

    parser.add_argument(
        "--sigma-x-meas2",
        type=float,
        default=1e-6,
        help="Measurement-noise variance for monthly indicators in the MF-DFM measurement equation (>0).",
    )

    args = parser.parse_args()

    panel_path = Path(args.panel_csv)
    target_path = Path(args.target_csv)
    label = args.label

    r_grid = parse_int_list(args.r_grid)
    p_grid = parse_int_list(args.p_grid)

    train_end = pd.to_datetime(args.train_end)
    val_end = pd.to_datetime(args.val_end)

    panel = pd.read_csv(panel_path, index_col=0, parse_dates=True)
    y_q = pd.read_csv(target_path, index_col=0, parse_dates=True).iloc[:, 0]

    if args.ragged_mode == "mask":
        if args.mask_csv is None:
            raise ValueError("--ragged-mode mask requires --mask-csv")
        mask_df = pd.read_csv(Path(args.mask_csv), index_col=0, parse_dates=True)
        mask_df = mask_df.reindex(columns=panel.columns)
        panel, mask_df = panel.align(mask_df, join="inner", axis=0)
        mask_bool = mask_df.astype(float).to_numpy() != 0.0
        panel_values0 = panel.to_numpy(dtype=float)
        panel_values0[~mask_bool] = np.nan
        panel = pd.DataFrame(panel_values0, index=panel.index, columns=panel.columns)

    panel, y_q = panel.align(y_q, join="inner", axis=0)

    dates = pd.DatetimeIndex(panel.index)
    T_total, n = panel.shape

    print(f"Loaded panel: {panel_path} -> T={T_total}, n={n}")
    print(f"Loaded target: {target_path}")
    print(f"Date range: {dates[0]} .. {dates[-1]}")
    print(f"Train end: {train_end}, Val end: {val_end}")

    # FIX: robust boolean masks, no .to_numpy() on already-numpy objects
    train_mask = np.asarray(dates <= train_end, dtype=bool)
    val_mask = np.asarray((dates > train_end) & (dates <= val_end), dtype=bool)
    test_mask = np.asarray(dates > val_end, dtype=bool)

    panel_values = panel.to_numpy(dtype=float)
    y_values = y_q.to_numpy(dtype=float)

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    qend_all = np.isfinite(y_values) & np.isin(dates.month, [3, 6, 9, 12])
    print(f"Quarter-end observed y points: val={int(np.sum(qend_all & val_mask))}, test={int(np.sum(qend_all & test_mask))}")

    cv_rows = []

    for r in r_grid:
        for p in p_grid:
            print(f"\n=== (r={r}, p={p}) ===")

            X_train = panel_values[train_mask]
            y_train = y_values[train_mask]

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
                sigma_x_meas2=float(args.sigma_x_meas2),
            )

            T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0 = build_state_matrices_from_params(
                params_train,
                n=n,
                r=r,
                p=p,
            )

            # One-sided wrt y_q: hide y_q after train_end for validation scoring pass
            Y = np.column_stack([panel_values, y_values])
            Y[~train_mask, -1] = np.nan

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

            y_m_hat_all = alpha_filt[:, idx_y0]
            y_q_hat_all = mariano_murasawa_from_monthly(y_m_hat_all)

            m_val = compute_window_metrics_mm(
                dates=dates,
                y_hat=y_q_hat_all,
                y_obs=y_values,
                window_mask=val_mask,
            )

            print(
                "  Validation metrics: "
                f"RMSE_m1={m_val['rmse_m1']}, RMSE_m2={m_val['rmse_m2']}, RMSE_m3={m_val['rmse_m3']}, "
                f"Pooled_MSE={m_val['pooled_mse']}"
            )

            cv_rows.append(
                {
                    "r": r,
                    "p": p,
                    "rmse_val_m1": m_val["rmse_m1"],
                    "rmse_val_m2": m_val["rmse_m2"],
                    "rmse_val_m3": m_val["rmse_m3"],
                    "pooled_mse_val": m_val["pooled_mse"],
                    "pooled_rmse_val": m_val["pooled_rmse"],
                    "n_val_m1": m_val["n_m1"],
                    "n_val_m2": m_val["n_m2"],
                    "n_val_m3": m_val["n_m3"],
                    "n_val_pool": m_val["n_pool"],
                    # selection metric kept backward-compatible
                    "rmse_val": m_val["rmse_m3"],
                }
            )

    cv_df = pd.DataFrame(cv_rows)
    cv_path = results_dir / f"mf_dfm_cv_summary_{label}.csv"
    cv_df.to_csv(cv_path, index=False)
    print(f"\nSaved CV summary to: {cv_path}")

    cv_df_clean = cv_df.dropna(subset=["rmse_val"])
    if cv_df_clean.empty:
        print("No valid RMSE values; cannot select best hyperparameters.")
        return

    best_row = cv_df_clean.loc[cv_df_clean["rmse_val"].idxmin()]
    best_r = int(best_row["r"])
    best_p = int(best_row["p"])
    print(
        f"\nBest (r, p) on validation (m3): r={best_r}, p={best_p}, "
        f"RMSE_val_m3={float(best_row['rmse_val_m3']):.6g}, "
        f"Pooled_MSE_val={float(best_row['pooled_mse_val']):.6g}"
    )

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
        use_tqdm=True,
        sigma_x_meas2=float(args.sigma_x_meas2),
    )

    T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0 = build_state_matrices_from_params(
        params_final,
        n=n,
        r=best_r,
        p=best_p,
    )

    # One-sided wrt y_q after val_end for test evaluation
    Y = np.column_stack([panel_values, y_values])
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

    m_test = compute_window_metrics_mm(
        dates=dates,
        y_hat=y_q_hat_all,
        y_obs=y_values,
        window_mask=test_mask,
    )

    print(
        "Test metrics (> val_end): "
        f"RMSE_m1={m_test['rmse_m1']:.6g}, RMSE_m2={m_test['rmse_m2']:.6g}, RMSE_m3={m_test['rmse_m3']:.6g}, "
        f"Pooled_MSE={m_test['pooled_mse']:.6g}"
    )

    out_df = pd.DataFrame(
        {
            "y_q": y_values,                  # observed at Mar/Jun/Sep/Dec only
            "y_q_hat": y_q_hat_all,           # nowcast each month
            "error": y_q_hat_all - y_values,  # meaningful only where y_q is observed
            "y_m_hat": y_m_hat_all,           # latent monthly GDP component
        },
        index=dates,
    )
    out_path = results_dir / f"mf_dfm_oos_{label}_r{best_r}_p{best_p}.csv"
    out_df.to_csv(out_path)
    print(f"Saved OOS nowcast series for best model to: {out_path}")


if __name__ == "__main__":
    main()
