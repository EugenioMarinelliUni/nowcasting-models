# src/dfm_pipeline/dfm_dyn/nowcast.py

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from dfm_pipeline.dfm_dyn.em_dfm import em_dfm_full, DynDFMParams
from dfm_pipeline.dfm_dyn.state_space import kalman_filter_smoother
from dfm_pipeline.dfm_simple.backtest import (
    build_quarterly_target_from_monthly,
    quarter_id_from_timestamp,
)


def nowcast_current_quarter_dyn(
    X: pd.DataFrame,
    y: pd.Series,
    q: int,
    r: int,
    p: int,
    T_vintage: pd.Timestamp,
    n_em_iter: int = 30,
    tol: float = 1e-3,
    min_obs: int = 60,
) -> Optional[float]:
    """
    Dynamic DFM nowcast of *current quarter* at vintage T_vintage.

    This uses the same ingredients as the dynamic backtest:

      1) Restrict X, y to data up to and including T_vintage.
      2) Estimate a dynamic DFM on X_T via EM (em_dfm_full) with q factors.
         (r, p are currently ignored here and reserved for future extensions.)
      3) Run the Kalman smoother to obtain smoothed factors f_t.
      4) Build a quarterly target y_Q (one obs per quarter).
      5) Regress y_Q on f_t (OLS on the smoothed factors).
      6) Return the nowcast of y_Q(T_vintage) using the smoothed factor at T_vintage.

    Parameters
    ----------
    X : DataFrame
        Monthly standardized predictor panel with a DatetimeIndex.
    y : Series
        Monthly standardized target with the same index as X (after alignment).
    q : int
        Number of dynamic factors used in the EM / state-space model.
        (Currently the only parameter actually used in estimation.)
    r : int
        Placeholder for dynamic dimension (not used yet).
    p : int
        Placeholder for VAR order in the state equation (not used yet).
    T_vintage : Timestamp
        Evaluation/nowcast month.
    n_em_iter : int
        Maximum EM iterations.
    tol : float
        EM convergence tolerance.
    min_obs : int
        Minimum number of time observations required to run the DFM.

    Returns
    -------
    float or None
        Nowcast of the quarterly target for the quarter containing T_vintage,
        or None if not enough data / quarter not observed / estimation failed.
    """
    # 1) Restrict sample to [start, T_vintage]
    X_T = X.loc[:T_vintage]
    y_T = y.loc[:T_vintage]

    if X_T.shape[0] < min_obs:
        return None

    # 2) Build quarterly target on the restricted sample
    y_q_T = build_quarterly_target_from_monthly(y_T)

    # Quarter containing the current vintage
    q_id_T = quarter_id_from_timestamp(T_vintage)
    if q_id_T not in y_q_T.index:
        return None
    if np.isnan(float(y_q_T.loc[q_id_T])):
        return None

    # 3) Estimate dynamic DFM parameters on X_T
    #    em_dfm_full is assumed to accept X, q, n_iter, tol (no r/p keywords).
    params: DynDFMParams = em_dfm_full(
        X_T,
        q=q,
        n_iter=n_em_iter,
        tol=tol,
    )

    # 4) Run Kalman smoother to get smoothed factors f_t
    X_np = X_T.to_numpy(dtype=float)
    ks_res = kalman_filter_smoother(
        Y=X_np,
        C=params.Lambda,
        T_mat=params.A,
        Q=params.Q,
        R=params.R,
        a0=params.a0,
        P0=params.P0,
    )
    # Expect ks_res.a_smooth shape: (T_eff, q)
    f_sm = ks_res.a_smooth
    dates_T = list(X_T.index)

    # 5) Build regression sample: quarterly y_Q vs smoothed factors
    y_reg: list[float] = []
    F_reg: list[np.ndarray] = []
    for idx, date in enumerate(dates_T):
        qid = quarter_id_from_timestamp(date)
        if qid not in y_q_T.index:
            continue
        y_val = float(y_q_T.loc[qid])
        if np.isnan(y_val):
            continue
        y_reg.append(y_val)
        F_reg.append(f_sm[idx, :])

    if len(y_reg) < q + 2:
        return None

    y_reg_arr = np.asarray(y_reg, dtype=float)
    F_reg_arr = np.asarray(F_reg, dtype=float)

    # OLS with intercept: y = b0 + b' f
    X_reg = np.column_stack([np.ones(len(y_reg_arr)), F_reg_arr])  # (n, 1+q)
    beta, *_ = np.linalg.lstsq(X_reg, y_reg_arr, rcond=None)

    # 6) Nowcast for the quarter containing T_vintage using f_T
    try:
        idx_T = dates_T.index(T_vintage)
    except ValueError:
        return None

    f_T = f_sm[idx_T, :]
    y_hat_T = beta[0] + f_T @ beta[1:]

    return float(y_hat_T)
