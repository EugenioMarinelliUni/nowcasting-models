#!/usr/bin/env python
import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_dyn.em_dfm_mf_gdp import em_dfm_mf_gdp_full


def compute_mm_quarterly_from_monthly(y_m: np.ndarray) -> np.ndarray:
    """
    Compute Mariano–Murasawa quarterly growth approximation from monthly
    GDP growth y_m (length T).

    Quarterly growth at month t (quarter-end) is:

        g_q(t) ≈ (1/3) y_t + (2/3) y_{t-1} + 1*y_{t-2}
                 + (2/3) y_{t-3} + (1/3) y_{t-4}

    We return an array of length T with NaNs where not enough lags exist.
    """
    Tn = len(y_m)
    y_q_hat = np.full(Tn, np.nan, dtype=float)
    if Tn < 5:
        return y_q_hat

    for t in range(4, Tn):
        y0 = y_m[t]
        y1 = y_m[t - 1]
        y2 = y_m[t - 2]
        y3 = y_m[t - 3]
        y4 = y_m[t - 4]
        y_q_hat[t] = (
            (1.0 / 3.0) * y0
            + (2.0 / 3.0) * y1
            + 1.0 * y2
            + (2.0 / 3.0) * y3
            + (1.0 / 3.0) * y4
        )
    return y_q_hat


def run_mf_dfm_mm(
    panel_path: Path,
    target_path: Path,
    r: int,
    p: int,
    max_iter_em: int,
    tol_em: float,
    out_dir: Path,
    label: str | None,
    use_tqdm: bool,
    verbose: bool,
) -> None:
    print(f"Panel CSV (monthly X):  {panel_path}")
    print(f"Target CSV (quarterly GDP growth on monthly grid): {target_path}")

    X_df = pd.read_csv(panel_path, index_col=0, parse_dates=True)
    y_df = pd.read_csv(target_path, index_col=0, parse_dates=True)

    df_joined = X_df.join(y_df, how="inner")
    if df_joined.empty:
        raise ValueError("No overlapping dates between panel and quarterly target.")

    X_df_aligned = df_joined[X_df.columns]
    target_col = y_df.columns[0]
    y_series = df_joined[target_col]

    X = X_df_aligned.to_numpy(float)  # (T, n)
    y_q = y_series.to_numpy(float)    # (T,)
    dates = X_df_aligned.index.to_numpy()

    Tn, n = X.shape
    print(f"Aligned sample length T={Tn}, n={n}")

    print(f"MF DFM MM spec: r={r}, p={p}, max_iter_em={max_iter_em}, tol_em={tol_em}")

    params = em_dfm_mf_gdp_full(
        X=X,
        y_q=y_q,
        r=r,
        p=p,
        max_iter=max_iter_em,
        tol=tol_em,
        verbose=verbose,
        use_tqdm=use_tqdm,
    )

    y_m_hat = params.monthly_gdp_smooth  # (T,)
    y_q_hat = compute_mm_quarterly_from_monthly(y_m_hat)

    # Evaluate in-sample only on months where y_q is observed and y_q_hat is defined
    mask_eval = ~np.isnan(y_q) & ~np.isnan(y_q_hat)
    n_eval = int(mask_eval.sum())
    if n_eval == 0:
        print("No overlapping non-NaN observations for quarterly evaluation.")
        rmse = np.nan
    else:
        errors = y_q[mask_eval] - y_q_hat[mask_eval]
        rmse = float(np.sqrt(np.mean(errors**2)))
        print(f"In-sample quarterly RMSE (standardized target): {rmse:.4f}")
        print(f"Number of evaluated quarter-end months: {n_eval}")

    out_dir.mkdir(parents=True, exist_ok=True)
    if not label:
        label = panel_path.stem

    out_path = out_dir / f"mf_dfm_mm_r{r}_p{p}_{label}.csv"

    out_df = pd.DataFrame(
        {
            "y_q": y_q,
            "y_q_hat": y_q_hat,
            "error": y_q - y_q_hat,
            "y_m_hat": y_m_hat,
        },
        index=X_df_aligned.index,
    )
    out_df.to_csv(out_path)
    print(f"Saved mixed-frequency MM DFM results to: {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run mixed-frequency DFM with Mariano–Murasawa mapping "
            "for quarterly GDP growth."
        )
    )
    parser.add_argument(
        "--panel-csv",
        type=str,
        required=True,
        help="Path to standardized monthly panel CSV (rows=time, cols=series).",
    )
    parser.add_argument(
        "--target-csv",
        type=str,
        required=True,
        help=(
            "Path to standardized quarterly GDP growth CSV, "
            "mapped to the monthly index (values only at quarter-end months)."
        ),
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
        default=50,
        help="Max EM iterations.",
    )
    parser.add_argument(
        "--tol-em",
        type=float,
        default=1e-4,
        help="EM convergence tolerance on relative log-likelihood change.",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="results/dfm_mf_mm",
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
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print EM log-likelihood per iteration.",
    )

    args = parser.parse_args()

    run_mf_dfm_mm(
        panel_path=Path(args.panel_csv),
        target_path=Path(args.target_csv),
        r=args.r,
        p=args.p,
        max_iter_em=args.max_iter_em,
        tol_em=args.tol_em,
        out_dir=Path(args.out_dir),
        label=args.label,
        use_tqdm=not args.no_tqdm,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
