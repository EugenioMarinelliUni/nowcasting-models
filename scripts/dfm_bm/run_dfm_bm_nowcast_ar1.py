#!/usr/bin/env python
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_dyn.em_dfm_ar1 import em_dfm_full_ar1

try:
    from tqdm import trange
except Exception:  # fallback if tqdm is not installed
    trange = range


def rolling_dfm_nowcast_ar1(
    X: np.ndarray,
    y: np.ndarray,
    r: int,
    p: int,
    start_idx: int,
    max_iter_em: int,
    tol_em: float,
    use_tqdm: bool,
) -> np.ndarray:
    """
    Rolling BM-style nowcast using the AR(1)-idios DFM (em_dfm_full_ar1).

    For each t >= start_idx:
        - run EM-DFM on X[0:t+1]
        - regress y[0:t+1] on factors
        - produce y_hat[t]

    Parameters
    ----------
    X : (T, n)
        Standardized panel.
    y : (T,)
        Standardized target (quarterly GDP mapped to monthly grid; NaNs allowed).
    r : int
        Number of factors.
    p : int
        VAR order.
    start_idx : int
        First time index at which to produce a nowcast.
    max_iter_em : int
        Max EM iterations per window.
    tol_em : float
        EM convergence tolerance.
    use_tqdm : bool
        If True, show rolling progress bar.

    Returns
    -------
    y_hat : (T,)
        Monthly nowcasts (NaN before start_idx or if regression not feasible).
    """
    Tn, n = X.shape
    y_hat = np.full(Tn, np.nan)

    iterator = trange(start_idx, Tn) if use_tqdm else range(start_idx, Tn)

    for t in iterator:
        X_win = X[: t + 1, :]
        y_win = y[: t + 1]

        # Estimate DFM with AR(1) idios on window [0..t]
        params = em_dfm_full_ar1(
            X_win,
            r=r,
            p=p,
            max_iter=max_iter_em,
            tol=tol_em,
            verbose=False,
            use_tqdm=False,
        )
        F_win = params.factors_smooth  # (t+1, r)

        # OLS regression of y on factors (with intercept), using non-NaN y
        mask = ~np.isnan(y_win)
        if mask.sum() <= r + 1:
            # not enough points to estimate bridge regression
            continue

        F_reg = F_win[mask, :]        # (N_eff, r)
        y_reg = y_win[mask]           # (N_eff,)

        X_reg = np.column_stack([np.ones(F_reg.shape[0]), F_reg])  # (N_eff, r+1)
        beta, _, _, _ = np.linalg.lstsq(X_reg, y_reg, rcond=None)

        f_t = F_win[-1, :]  # factors at time t
        y_hat[t] = beta[0] + f_t @ beta[1:]

    return y_hat


def compute_within_quarter_rmse(
    df: pd.DataFrame,
    eval_start: str | None = None,
) -> tuple[dict[int, float], dict[int, int]]:
    """
    Compute RMSE for 1st-, 2nd-, 3rd-month nowcasts of quarterly GDP.

    df:
        DataFrame with index = monthly dates, columns must include
        "y" and "y_hat".

        - y      = quarterly GDP mapped to first month of quarter (NaN otherwise),
                   standardized.
        - y_hat  = monthly nowcasts from the DFM (standardized).

    eval_start:
        Optional YYYY-MM-DD string. If given, drop dates before this.

    Returns
    -------
    rmses : dict[int, float]
        {1: RMSE for 1st-month nowcasts,
         2: RMSE for 2nd-month nowcasts,
         3: RMSE for 3rd-month nowcasts}
        in standardized units.

    counts : dict[int, int]
        Number of quarters contributing to each horizon RMSE.
    """
    df = df.copy()
    df.index = pd.to_datetime(df.index)

    if eval_start is not None:
        df = df[df.index >= eval_start]

    dot_mask = df["y"].notna()
    dot_dates = df.index[dot_mask]
    y_q = df.loc[dot_mask, "y"]

    errors_by_h: dict[int, list[float]] = {1: [], 2: [], 3: []}

    for date_dot, y_val in zip(dot_dates, y_q):
        for h, months_ahead in enumerate([0, 1, 2], start=1):
            date_h = date_dot + pd.DateOffset(months=months_ahead)
            if date_h not in df.index:
                continue
            y_hat_val = df.at[date_h, "y_hat"]
            if not np.isfinite(y_hat_val):
                continue
            err = float(y_val - y_hat_val)
            errors_by_h[h].append(err)

    rmses: dict[int, float] = {}
    counts: dict[int, int] = {}
    for h, errs in errors_by_h.items():
        counts[h] = len(errs)
        if counts[h] == 0:
            rmses[h] = np.nan
        else:
            arr = np.asarray(errs, float)
            rmses[h] = float(np.sqrt(np.mean(arr**2)))

    return rmses, counts


def run_nowcast_ar1(
    panel_path: Path,
    target_path: Path,
    start_date: str,
    r: int,
    p: int,
    max_iter_em: int,
    tol_em: float,
    out_dir: Path,
    use_tqdm: bool,
    label: str | None,
) -> None:
    print(f"Panel CSV:  {panel_path}")
    print(f"Target CSV: {target_path}")

    X_df = pd.read_csv(panel_path, index_col=0, parse_dates=True)
    y_df = pd.read_csv(target_path, index_col=0, parse_dates=True)

    df_joined = X_df.join(y_df, how="inner")
    if df_joined.empty:
        raise ValueError("No overlapping dates between panel and target.")

    X_df_aligned = df_joined[X_df.columns]
    target_col = y_df.columns[0]
    y_series = df_joined[target_col]

    X = X_df_aligned.to_numpy(float)  # (T, n)
    y = y_series.to_numpy(float)      # (T,)
    dates = X_df_aligned.index.to_numpy()

    start_date_np = np.datetime64(start_date)
    idx_candidates = np.where(dates >= start_date_np)[0]
    if idx_candidates.size == 0:
        raise ValueError(f"No dates >= {start_date} in aligned panel/target.")
    start_idx = int(idx_candidates[0])

    print(f"Start date: {start_date} -> start_idx={start_idx}, calendar date={dates[start_idx]}")
    print(f"AR(1) DFM spec: r={r}, p={p}, max_iter_em={max_iter_em}, tol_em={tol_em}")

    y_hat = rolling_dfm_nowcast_ar1(
        X=X,
        y=y,
        r=r,
        p=p,
        start_idx=start_idx,
        max_iter_em=max_iter_em,
        tol_em=tol_em,
        use_tqdm=use_tqdm,
    )

    mask_eval = ~np.isnan(y_hat) & ~np.isnan(y)
    n_eval = int(mask_eval.sum())
    if n_eval == 0:
        print("No overlapping non-NaN observations in evaluation window.")
    else:
        errors = y[mask_eval] - y_hat[mask_eval]
        rmse_overall = float(np.sqrt(np.mean(errors**2)))
        print(f"Overall nowcast RMSE (standardized target) over eval window: {rmse_overall:.4f}")
        print(f"Number of evaluated points (overall): {n_eval}")

    out_dir.mkdir(parents=True, exist_ok=True)
    if not label:
        label = panel_path.stem

    out_path = out_dir / f"dfm_nowcast_ar1_r{r}_p{p}_{label}.csv"

    out_df = pd.DataFrame(
        {
            "y": y,
            "y_hat": y_hat,
            "error": y - y_hat,
        },
        index=X_df_aligned.index,
    )
    out_df.to_csv(out_path)

    print(f"Saved AR(1) DFM nowcast results to: {out_path}")

    eval_df = pd.DataFrame(
        {
            "y": y,
            "y_hat": y_hat,
        },
        index=X_df_aligned.index,
    )

    rmses_h, counts_h = compute_within_quarter_rmse(eval_df, eval_start=start_date)

    print("Within-quarter RMSEs (standardized target, AR(1) DFM):")
    for h in (1, 2, 3):
        rmse_h = rmses_h[h]
        n_h = counts_h[h]
        if np.isnan(rmse_h):
            print(f"  h={h} (month {h} of quarter): RMSE = nan (N={n_h})")
        else:
            print(f"  h={h} (month {h} of quarter): RMSE = {rmse_h:.4f} (N={n_h})")


def main():
    parser = argparse.ArgumentParser(
        description="Run Bańbura–Modugno-style DFM with AR(1) idiosyncratics on standardized panels."
    )
    parser.add_argument(
        "--panel-csv",
        type=str,
        required=True,
        help="Path to standardized explanatory panel CSV (rows=time, cols=series).",
    )
    parser.add_argument(
        "--target-csv",
        type=str,
        required=True,
        help="Path to standardized target CSV (single column, same index as panel).",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="First calendar date (YYYY-MM-DD) at which to produce nowcasts.",
    )
    parser.add_argument(
        "--r",
        type=int,
        default=2,
        help="Number of dynamic factors.",
    )
    parser.add_argument(
        "--p",
        type=int,
        default=1,
        help="VAR order for factors.",
    )
    parser.add_argument(
        "--max-iter-em",
        type=int,
        default=30,
        help="Max EM iterations per estimation window.",
    )
    parser.add_argument(
        "--tol-em",
        type=float,
        default=1e-4,
        help="EM convergence tolerance on log-likelihood.",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="results/dfm_bm_nowcasts_ar1",
        help="Directory where result CSV is written.",
    )
    parser.add_argument(
        "--label",
        type=str,
        default="",
        help="Optional label for output filename.",
    )
    parser.add_argument(
        "--no-tqdm",
        action="store_true",
        help="Disable tqdm progress bars.",
    )

    args = parser.parse_args()

    run_nowcast_ar1(
        panel_path=Path(args.panel_csv),
        target_path=Path(args.target_csv),
        start_date=args.start_date,
        r=args.r,
        p=args.p,
        max_iter_em=args.max_iter_em,
        tol_em=args.tol_em,
        out_dir=Path(args.out_dir),
        use_tqdm=not args.no_tqdm,
        label=args.label,
    )


if __name__ == "__main__":
    main()
