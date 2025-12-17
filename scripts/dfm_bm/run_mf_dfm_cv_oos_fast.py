# scripts/dfm_bm/run_mf_dfm_cv_oos_fast.py
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


def _is_qend_mm(month: int) -> bool:
    # Mariano–Murasawa convention: quarter-end stamps in Mar/Jun/Sep/Dec
    return month in (3, 6, 9, 12)


def _month_in_quarter_mm(month: int) -> int:
    # 1: Jan/Apr/Jul/Oct, 2: Feb/May/Aug/Nov, 3: Mar/Jun/Sep/Dec
    if month in (1, 4, 7, 10):
        return 1
    if month in (2, 5, 8, 11):
        return 2
    if month in (3, 6, 9, 12):
        return 3
    raise ValueError(f"Invalid month: {month}")


@dataclass
class WindowMetrics:
    rmse_m1: float
    rmse_m2: float
    rmse_m3: float
    pooled_mse: float
    pooled_rmse: float
    n_m1: int
    n_m2: int
    n_m3: int
    n_pool: int


def compute_within_quarter_errors_mm(
    dates: pd.DatetimeIndex,
    y_hat: np.ndarray,
    y_obs: np.ndarray,
) -> dict:
    """
    Construct realized quarterly values replicated onto the 3 months of each quarter
    and horizon-specific monthly error series.

    y_obs is expected to be non-NaN only at Mar/Jun/Sep/Dec (MM index).
    """
    T = len(dates)
    y_realized_for_q = np.full(T, np.nan, dtype=float)
    err_m1 = np.full(T, np.nan, dtype=float)
    err_m2 = np.full(T, np.nan, dtype=float)
    err_m3 = np.full(T, np.nan, dtype=float)

    qend_mask = np.isfinite(y_obs) & np.isin(dates.month, [3, 6, 9, 12])
    qend_idx = np.where(qend_mask)[0]

    for j in qend_idx:
        yq = float(y_obs[j])
        if j - 2 >= 0:
            y_realized_for_q[j] = yq
            y_realized_for_q[j - 1] = yq
            y_realized_for_q[j - 2] = yq

    for t in range(T):
        if not np.isfinite(y_realized_for_q[t]) or not np.isfinite(y_hat[t]):
            continue
        miq = _month_in_quarter_mm(int(dates.month[t]))
        if miq == 1:
            err_m1[t] = y_hat[t] - y_realized_for_q[t]
        elif miq == 2:
            err_m2[t] = y_hat[t] - y_realized_for_q[t]
        else:
            err_m3[t] = y_hat[t] - y_realized_for_q[t]

    return {
        "y_q_realized_for_quarter": y_realized_for_q,
        "error_m1": err_m1,
        "error_m2": err_m2,
        "error_m3": err_m3,
    }


def compute_window_metrics_mm(
    dates: pd.DatetimeIndex,
    y_hat: np.ndarray,
    y_obs: np.ndarray,
    window_mask: np.ndarray,
) -> WindowMetrics:
    """
    Compute RMSE_m1/m2/m3 and pooled MSE over all horizons for a given window.

    IMPORTANT: To avoid cross-window mixing at boundaries, the evaluation set is defined
    by quarter-end months j such that:
      - window_mask[j] is True
      - y_obs[j] is finite
      - j is a quarter-end month (Mar/Jun/Sep/Dec)

    For each such quarter-end j, errors are evaluated at months j-2 (m1), j-1 (m2), j (m3),
    but only if the month index is also inside the window (window_mask[idx] True).
    """
    T = len(dates)

    e1 = []
    e2 = []
    e3 = []
    epool = []

    qend_idx = np.where(
        window_mask
        & np.isfinite(y_obs)
        & np.isin(dates.month, [3, 6, 9, 12])
    )[0]

    for j in qend_idx:
        yq = float(y_obs[j])

        # m1
        t1 = j - 2
        if t1 >= 0 and window_mask[t1] and np.isfinite(y_hat[t1]):
            err = float(y_hat[t1] - yq)
            e1.append(err)
            epool.append(err)

        # m2
        t2 = j - 1
        if t2 >= 0 and window_mask[t2] and np.isfinite(y_hat[t2]):
            err = float(y_hat[t2] - yq)
            e2.append(err)
            epool.append(err)

        # m3
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

    return WindowMetrics(
        rmse_m1=rmse_m1,
        rmse_m2=rmse_m2,
        rmse_m3=rmse_m3,
        pooled_mse=pooled_mse,
        pooled_rmse=pooled_rmse,
        n_m1=len(e1),
        n_m2=len(e2),
        n_m3=len(e3),
        n_pool=len(epool),
    )


@dataclass
class CVResult:
    r: int
    p: int
    rmse_val_m1: float
    rmse_val_m2: float
    rmse_val_m3: float
    pooled_mse_val: float
    pooled_rmse_val: float
    n_val_m1: int
    n_val_m2: int
    n_val_m3: int
    n_val_pool: int


def _fit_one_config(job) -> CVResult:
    (
        r,
        p,
        panel_values,
        y_values,
        dates_values,
        train_mask,
        val_mask,
        max_iter,
        tol,
        n,
        sigma_x_meas2,
    ) = job

    dates = pd.DatetimeIndex(dates_values)

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
            use_tqdm=False,
            sigma_x_meas2=float(sigma_x_meas2),
        )

        # 2) Build state matrices
        T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0 = build_state_matrices_from_params(
            params_train,
            n=n,
            r=r,
            p=p,
        )

        # 3) Filter full sample with y_q masked after train_end
        Y = np.column_stack([panel_values, y_values])
        Y[~train_mask, -1] = np.nan

        alpha_filt, _ = kalman_filter_only(
            Y=Y,
            T=T_mat,
            Q=Q_mat,
            C=C_mat,
            R_meas=R_meas,
            a0=a0,
            P0=P0,
        )

        # 4) Nowcast series
        y_m_hat_all = alpha_filt[:, idx_y0]
        y_q_hat_all = mariano_murasawa_from_monthly(y_m_hat_all)

        # 5) Validation metrics (m1/m2/m3 + pooled over horizons)
        m_val = compute_window_metrics_mm(
            dates=dates,
            y_hat=y_q_hat_all,
            y_obs=y_values,
            window_mask=val_mask,
        )

        return CVResult(
            r=r,
            p=p,
            rmse_val_m1=m_val.rmse_m1,
            rmse_val_m2=m_val.rmse_m2,
            rmse_val_m3=m_val.rmse_m3,
            pooled_mse_val=m_val.pooled_mse,
            pooled_rmse_val=m_val.pooled_rmse,
            n_val_m1=m_val.n_m1,
            n_val_m2=m_val.n_m2,
            n_val_m3=m_val.n_m3,
            n_val_pool=m_val.n_pool,
        )

    except Exception:
        return CVResult(
            r=r,
            p=p,
            rmse_val_m1=np.nan,
            rmse_val_m2=np.nan,
            rmse_val_m3=np.nan,
            pooled_mse_val=np.nan,
            pooled_rmse_val=np.nan,
            n_val_m1=0,
            n_val_m2=0,
            n_val_m3=0,
            n_val_pool=0,
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Fast train/validation/test for mixed-frequency DFM (Mariano–Murasawa): "
            "grid over (r, p), select on validation RMSE, then test OOS. "
            "Also reports within-quarter RMSE_m1/m2/m3 and pooled MSE over all horizons."
        )
    )
    parser.add_argument("--panel-csv", required=True, type=str)
    parser.add_argument("--target-csv", required=True, type=str)

    parser.add_argument("--r-grid", type=str, default="1,2,3")
    parser.add_argument("--p-grid", type=str, default="1,2")

    parser.add_argument("--train-end", type=str, default="2005-12-01")
    parser.add_argument("--val-end", type=str, default="2015-12-01")

    parser.add_argument("--max-iter", type=int, default=100)
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

    train_mask = np.asarray(dates <= train_end, dtype=bool)
    val_mask = np.asarray((dates > train_end) & (dates <= val_end), dtype=bool)
    test_mask = np.asarray(dates > val_end, dtype=bool)

    panel_values = panel.to_numpy(dtype=float)
    y_values = y_q.to_numpy(dtype=float)

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    # Helpful counts: number of quarter-end y points in each window (drives m1/m2/m3 sample sizes)
    qend_all = np.isfinite(y_values) & np.isin(dates.month, [3, 6, 9, 12])
    n_qend_val = int(np.sum(qend_all & val_mask))
    n_qend_test = int(np.sum(qend_all & test_mask))
    print(f"Quarter-end observed y points: val={n_qend_val}, test={n_qend_test}")

    jobs = []
    for r in r_grid:
        for p in p_grid:
            jobs.append(
                (
                    r,
                    p,
                    panel_values,
                    y_values,
                    dates.to_numpy(),
                    train_mask,
                    val_mask,
                    args.max_iter,
                    args.tol,
                    n,
                    float(args.sigma_x_meas2),
                )
            )

    cv_rows = []
    print("\n=== Grid search over (r, p) ===")
    with cf.ProcessPoolExecutor() as ex:
        for res in ex.map(_fit_one_config, jobs):
            print(
                f"(r={res.r}, p={res.p}) -> "
                f"RMSE_val_m1={res.rmse_val_m1}, "
                f"RMSE_val_m2={res.rmse_val_m2}, "
                f"RMSE_val_m3={res.rmse_val_m3}, "
                f"Pooled_MSE_val={res.pooled_mse_val}"
            )
            cv_rows.append(
                {
                    "r": res.r,
                    "p": res.p,
                    "rmse_val_m1": res.rmse_val_m1,
                    "rmse_val_m2": res.rmse_val_m2,
                    "rmse_val_m3": res.rmse_val_m3,
                    "pooled_mse_val": res.pooled_mse_val,
                    "pooled_rmse_val": res.pooled_rmse_val,
                    "n_val_m1": res.n_val_m1,
                    "n_val_m2": res.n_val_m2,
                    "n_val_m3": res.n_val_m3,
                    "n_val_pool": res.n_val_pool,
                    # keep backward-compatible selection metric
                    "rmse_val": res.rmse_val_m3,
                }
            )

    cv_df = pd.DataFrame(cv_rows)
    cv_path = results_dir / f"mf_dfm_cv_summary_{label}.csv"
    cv_df.to_csv(cv_path, index=False)
    print(f"\nSaved CV summary to: {cv_path}")

    # Selection remains on m3 by default (backward compatible)
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

    Y = np.column_stack([panel_values, y_values])
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

    # Test metrics (same definitions as validation)
    m_test = compute_window_metrics_mm(
        dates=dates,
        y_hat=y_q_hat_all,
        y_obs=y_values,
        window_mask=test_mask,
    )

    print(
        "Test metrics (> val_end): "
        f"RMSE_m1={m_test.rmse_m1:.6g}, RMSE_m2={m_test.rmse_m2:.6g}, RMSE_m3={m_test.rmse_m3:.6g}, "
        f"Pooled_MSE={m_test.pooled_mse:.6g}"
    )

    # Save OOS series and error decomposition
    wq_all = compute_within_quarter_errors_mm(dates=dates, y_hat=y_q_hat_all, y_obs=y_values)

    out_df = pd.DataFrame(
        {
            "y_q": y_values,  # observed only at Mar/Jun/Sep/Dec in MM-index target
            "y_q_hat": y_q_hat_all,  # nowcast each month
            "y_m_hat": y_m_hat_all,
            "y_q_realized_for_quarter": wq_all["y_q_realized_for_quarter"],  # replicated onto 3 months
            "error_m1": wq_all["error_m1"],
            "error_m2": wq_all["error_m2"],
            "error_m3": wq_all["error_m3"],
        },
        index=dates,
    )
    out_path = results_dir / f"mf_dfm_oos_{label}_r{best_r}_p{best_p}.csv"
    out_df.to_csv(out_path)
    print(f"Saved OOS nowcast series for best model to: {out_path}")


if __name__ == "__main__":
    main()
