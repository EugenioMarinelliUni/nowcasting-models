#!/usr/bin/env python
# scripts/dfm_tvp/run_tvp_on_dfm.py

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_tvp.tvp_regression import em_tvp_regression


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Run a small TVP regression on top of the mixed-frequency DFM output.\n"
            "The dependent variable is quarterly GDP (y_q), aligned on a monthly index,\n"
            "and the regressor is by default the monthly GDP component y_m_hat at the same dates.\n"
            "Optionally, a crisis dummy defines a high-variance regime for the observation noise."
        )
    )
    parser.add_argument(
        "--dfm-oos-csv",
        type=str,
        required=True,
        help="Path to mf_dfm_oos_*.csv produced by run_mf_dfm_cv_oos(_fast).",
    )
    parser.add_argument(
        "--regressor",
        type=str,
        default="y_m_hat",
        choices=["y_m_hat", "y_q_hat"],
        help="Which column to use as regressor in the TVP regression.",
    )
    parser.add_argument(
        "--include-const",
        action="store_true",
        help="Include a constant term in the TVP regression (intercept).",
    )
    parser.add_argument(
        "--crisis-start",
        type=str,
        default=None,
        help="Optional crisis start date (YYYY-MM-DD) for high-variance regime.",
    )
    parser.add_argument(
        "--crisis-end",
        type=str,
        default=None,
        help="Optional crisis end date (YYYY-MM-DD) for high-variance regime.",
    )
    parser.add_argument(
        "--max-iter",
        type=int,
        default=100,
        help="Max EM iterations for the TVP regression.",
    )
    parser.add_argument(
        "--tol",
        type=float,
        default=1e-4,
        help="EM convergence tolerance on relative log-likelihood.",
    )
    parser.add_argument(
        "--q-beta",
        type=float,
        default=0.01,
        help="State noise scale for beta_t (Q_beta = q_beta * I).",
    )
    parser.add_argument(
        "--out-csv",
        type=str,
        required=True,
        help="Output CSV path for TVP-adjusted nowcasts.",
    )

    args = parser.parse_args()

    dfm_oos_path = Path(args.dfm_oos_csv)
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(dfm_oos_path, index_col=0, parse_dates=True)

    if "y_q" not in df.columns:
        raise ValueError("Input dfm_oos CSV must contain 'y_q' column.")
    if args.regressor not in df.columns:
        raise ValueError(f"Input dfm_oos CSV must contain '{args.regressor}' column.")

    dates = df.index
    y_q = df["y_q"]
    x_base = df[args.regressor]

    # Work on the full monthly index, but TVP uses all dates where y_q is observed
    # (quarter-end months after the Mariano–Murasawa mapping).
    # Build X with optional constant.
    if args.include_const:
        X = pd.DataFrame(
            {
                "const": 1.0,
                args.regressor: x_base,
            },
            index=dates,
        )
    else:
        X = x_base.to_frame()

    # Crisis dummy (0 / 1) if requested
    if args.crisis_start is not None and args.crisis_end is not None:
        crisis_start = pd.to_datetime(args.crisis_start)
        crisis_end = pd.to_datetime(args.crisis_end)
        d_crisis = ((dates >= crisis_start) & (dates <= crisis_end)).astype(int).to_numpy()
    else:
        d_crisis = None

    # Run TVP EM on the full series (y_q may have NaNs; handled inside)
    res = em_tvp_regression(
        y=y_q,
        X=X,
        d_crisis=d_crisis,
        max_iter=args.max_iter,
        tol=args.tol,
        q_beta=args.q_beta,
        verbose=True,
    )

    # Fitted quarterly GDP from TVP regression
    y_q_hat_tvp = pd.Series(res.y_hat, index=dates, name="y_q_hat_tvp")

    # Error vs actual quarterly GDP (only meaningful where y_q is observed)
    err_tvp = y_q_hat_tvp - y_q
    err_tvp.name = "error_tvp"

    # RMSE on available quarters (in-sample fit)
    mask_eval = y_q.notna() & np.isfinite(y_q_hat_tvp.to_numpy())
    if mask_eval.any():
        rmse_tvp = float(
            np.sqrt(np.mean((err_tvp[mask_eval].to_numpy()) ** 2))
        )
        print(f"TVP regression RMSE on observed quarters (standardized): {rmse_tvp:.4f}")
    else:
        print("No overlapping non-NaN observations for RMSE computation.")

    # Add TVP outputs to the original DFM OOS dataframe
    df_out = df.copy()
    df_out["y_q_hat_tvp"] = y_q_hat_tvp
    df_out["error_tvp"] = err_tvp

    # If you want, also store the time-varying coefficients (flattened)
    # For k up to, say, 2 or 3 this is manageable.
    k = res.beta_smooth.shape[1]
    for j in range(k):
        col_name = f"beta_tvp_{j}"
        df_out[col_name] = res.beta_smooth[:, j]

    df_out.to_csv(out_path)
    print(f"Saved TVP-adjusted nowcasts to: {out_path}")


if __name__ == "__main__":
    main()
