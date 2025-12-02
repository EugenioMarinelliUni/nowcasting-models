# src/dfm_pipeline/dfm_simple/nowcast.py
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_simple.em_pca import em_pca
from dfm_pipeline.dfm_simple.var_kalman import (
    fit_var_ols,
    build_var_state_matrices,
    kalman_filter,
    kalman_smoother,
)


def _quarter_id_from_month(idx: pd.DatetimeIndex) -> pd.MultiIndex:
    """
    Build a quarter identifier for each monthly date,
    e.g. (year, quarter) for grouping.
    """
    years = idx.year
    quarters = ((idx.month - 1) // 3) + 1
    return pd.MultiIndex.from_arrays([years, quarters], names=["year", "quarter"])


def nowcast_current_quarter(
    X: pd.DataFrame,
    y: pd.Series,
    n_factors: int,
    p_var: int,
    T_vintage: pd.Timestamp,
    use_smoothed_factors: bool = True,
    min_quarters_for_reg: int = 40,
) -> Optional[float]:
    """
    End-to-end nowcast of current-quarter GDP at a vintage T_vintage.

    Assumes:
      - X, y are standardized.
      - y has one non-NaN observation per quarter (mapped to a month).

    Returns:
      y_now : float or None
    """
    # 1) Truncate to vintage
    X_trunc = X.loc[:T_vintage].copy()
    y_trunc = y.loc[:T_vintage].copy()

    if X_trunc.empty:
        return None

    # 2) EM-PCA on truncated X
    F_df, Lambda_df, X_hat_df = em_pca(X_trunc, n_factors=n_factors)

    # 3) Optional VAR+Kalman smoothing
    if p_var > 0 and use_smoothed_factors:
        A_list, Sigma_u = fit_var_ols(F_df, p=p_var)
        T_mat, R_mat, Q_mat, Z_mat = build_var_state_matrices(A_list, Sigma_u)
        H = 1e-4 * np.eye(F_df.shape[1])  # small measurement noise
        n_state = T_mat.shape[0]
        a1 = np.zeros(n_state)
        P1 = np.eye(n_state)

        alpha_filt, P_filt = kalman_filter(
            F_df.to_numpy(), Z_mat, T_mat, R_mat, Q_mat, H, a1, P1
        )
        alpha_smooth = kalman_smoother(alpha_filt, P_filt, T_mat, R_mat, Q_mat)
        F_used = alpha_smooth[:, :F_df.shape[1]]
        F_used_df = pd.DataFrame(F_used, index=F_df.index, columns=F_df.columns)
    else:
        F_used_df = F_df

    # 4) Aggregate factors to quarterly summaries
    idx = F_used_df.index
    qid = _quarter_id_from_month(idx)

    # Quarterly factor summary: mean over months in quarter
    F_q = F_used_df.groupby(qid).mean()

    # Quarterly y: pick non-NaN month per quarter
    y_q = (
        y_trunc.groupby(qid)
        .apply(lambda s: s.dropna().iloc[0] if not s.dropna().empty else np.nan)
        .astype(float)
    )

    # Align and drop quarters with missing y
    common_idx = F_q.index.intersection(y_q.index)
    F_q = F_q.loc[common_idx]
    y_q = y_q.loc[common_idx]

    mask = ~y_q.isna()
    if mask.sum() < min_quarters_for_reg:
        return None

    y_reg = y_q[mask].to_numpy()
    X_reg = F_q[mask].to_numpy()  # (n_q x r)

    # Add constant
    X_reg_ext = np.column_stack([np.ones(X_reg.shape[0]), X_reg])

    # OLS: y = beta0 + beta' F_q + eps
    beta, _, _, _ = np.linalg.lstsq(X_reg_ext, y_reg, rcond=None)

    # 5) Build current-quarter factor summary at vintage T_vintage
    q_vintage = _quarter_id_from_month(pd.DatetimeIndex([T_vintage]))[0]

    q_mask = (qid == q_vintage)
    if not q_mask.any():
        return None

    F_current_quarter = (
        F_used_df.loc[q_mask & (idx <= T_vintage)]
        .mean(axis=0)
        .to_numpy()
    )
    if np.any(np.isnan(F_current_quarter)):
        return None

    X_now = np.concatenate([[1.0], F_current_quarter])  # add constant
    y_now = float(X_now @ beta)

    return y_now
