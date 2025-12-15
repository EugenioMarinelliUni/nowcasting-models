# src/dfm_pipeline/dfm_tvp/tvp_regression.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import numpy as np
import pandas as pd


@dataclass
class TVPRegressionResult:
    """
    Results for a univariate time-varying-parameter regression:

        y_t = x_t' beta_t + e_t
        beta_t = beta_{t-1} + w_t

    with optional two-regime variance switching for e_t.
    """

    beta_smooth: np.ndarray        # (T, k) smoothed state E[beta_t | y_1:T]
    P_smooth: np.ndarray           # (T, k, k) smoothed covariances Var[beta_t | y_1:T]
    y_hat: np.ndarray              # (T,) fitted y_t = x_t' beta_t
    Q_beta: np.ndarray             # (k, k) state noise covariance
    R_normal: float                # observation variance (normal periods)
    R_crisis: Optional[float]      # observation variance (crisis periods), or None
    loglik_history: Sequence[float]  # log-likelihood per EM iteration
    index: Optional[pd.Index]      # optional time index (for convenience)


def _kalman_filter_tvp(
    y: np.ndarray,
    X: np.ndarray,
    d_crisis: Optional[np.ndarray],
    Q_beta: np.ndarray,
    R_normal: float,
    R_crisis: Optional[float],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Kalman filter for:

        beta_t = beta_{t-1} + w_t
        y_t    = x_t' beta_t + e_t

    y may contain NaNs (missing). If d_crisis is given (0/1), observation
    variance is R_normal when d=0 and R_crisis when d=1.

    Returns
    -------
    beta_filt : (T, k)
    P_filt    : (T, k, k)
    a_pred    : (T, k)     predicted state before update
    P_pred    : (T, k, k)  predicted covariance before update
    loglik    : (T,)       log-likelihood contributions per t (0 if y_t NaN)
    """
    y = np.asarray(y, float)
    X = np.asarray(X, float)
    T, k = X.shape

    if d_crisis is not None:
        d_crisis = np.asarray(d_crisis, int)
        if d_crisis.shape[0] != T:
            raise ValueError("d_crisis must have length T if provided.")

    # State transition is identity
    F = np.eye(k)
    Q = Q_beta

    beta_filt = np.zeros((T, k), dtype=float)
    P_filt = np.zeros((T, k, k), dtype=float)
    a_pred = np.zeros((T, k), dtype=float)
    P_pred = np.zeros((T, k, k), dtype=float)
    loglik = np.zeros(T, dtype=float)

    # diffuse-ish prior
    beta_prev = np.zeros(k, dtype=float)
    P_prev = np.eye(k) * 1e3

    for t in range(T):
        # prediction
        a_t = F @ beta_prev
        P_t = F @ P_prev @ F.T + Q

        a_pred[t] = a_t
        P_pred[t] = P_t

        y_t = y[t]
        x_t = X[t]

        if not np.isfinite(y_t):
            # missing observation: no update
            beta_filt[t] = a_t
            P_filt[t] = P_t
            loglik[t] = 0.0
            beta_prev = beta_filt[t]
            P_prev = P_filt[t]
            continue

        # observation matrix (1 x k)
        H_t = x_t.reshape(1, -1)

        # observation variance
        if d_crisis is not None and R_crisis is not None and d_crisis[t] == 1:
            R_t = R_crisis
        else:
            R_t = R_normal

        # scalar S_t
        S_t = float(H_t @ P_t @ H_t.T + R_t)
        if S_t <= 0.0:
            # small jitter for numerical safety
            S_t = float(abs(S_t) + 1e-8)

        # Kalman gain (k x 1)
        K_t = (P_t @ H_t.T) / S_t

        # innovation
        v_t = float(y_t - H_t @ a_t)

        # update
        beta_upd = a_t + (K_t.flatten() * v_t)
        P_upd = P_t - K_t @ H_t @ P_t

        beta_filt[t] = beta_upd
        P_filt[t] = P_upd

        # log-likelihood contribution
        loglik[t] = -0.5 * (np.log(2.0 * np.pi * S_t) + (v_t**2) / S_t)

        beta_prev = beta_upd
        P_prev = P_upd

    return beta_filt, P_filt, a_pred, P_pred, loglik


def _kalman_smoother_tvp(
    beta_filt: np.ndarray,
    P_filt: np.ndarray,
    P_pred: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Rauch-Tung-Striebel smoother for the random-walk state:

        beta_t = beta_{t-1} + w_t

    With F = I, Q arbitrary.

    Parameters
    ----------
    beta_filt : (T, k)
        Filtered state estimates.
    P_filt : (T, k, k)
        Filtered covariances.
    P_pred : (T, k, k)
        Predicted covariances (before measurement update).

    Returns
    -------
    beta_smooth : (T, k)
    P_smooth    : (T, k, k)
    """
    T, k = beta_filt.shape
    beta_smooth = np.zeros((T, k), dtype=float)
    P_smooth = np.zeros((T, k, k), dtype=float)

    # Initialize at last time
    beta_smooth[-1] = beta_filt[-1]
    P_smooth[-1] = P_filt[-1]

    for t in range(T - 2, -1, -1):
        P_f = P_filt[t]
        P_p_next = P_pred[t + 1]

        # smoothing gain J_t = P_filt[t] * F' * (P_pred[t+1])^{-1}; F = I
        # use solve instead of explicit inverse
        J_t = np.linalg.solve(P_p_next.T, P_f.T).T

        beta_smooth[t] = beta_filt[t] + J_t @ (beta_smooth[t + 1] - beta_filt[t])
        P_smooth[t] = P_f + J_t @ (P_smooth[t + 1] - P_p_next) @ J_t.T

    return beta_smooth, P_smooth


def em_tvp_regression(
    y: pd.Series | np.ndarray,
    X: pd.DataFrame | np.ndarray,
    d_crisis: Optional[Sequence[int]] = None,
    max_iter: int = 100,
    tol: float = 1e-4,
    q_beta: float = 0.01,
    verbose: bool = False,
) -> TVPRegressionResult:
    """
    EM for univariate TVP regression with optional two-regime observation variance.

    Model
    -----
        beta_t = beta_{t-1} + w_t,   w_t ~ N(0, Q_beta)
        y_t    = x_t' beta_t + e_t,  e_t ~ N(0, R_normal) if d_t=0
                                      e_t ~ N(0, R_crisis) if d_t=1

    If d_crisis is None, a single variance R_normal is estimated.

    Parameters
    ----------
    y : array-like (T,)
        Dependent variable (e.g., standardized quarterly GDP).
        May contain NaNs (missing).
    X : array-like (T, k)
        Regressor(s), typically low-dimensional (k small).
    d_crisis : sequence of {0,1}, optional
        Crisis dummy per time t. If provided, estimate R_normal and R_crisis separately.
        If None, only R_normal is used.
    max_iter : int
        Maximum EM iterations.
    tol : float
        Relative log-likelihood tolerance for EM convergence.
    q_beta : float
        Scalar multiplier for state innovation covariance Q_beta = q_beta * I_k.
        This controls how "fast" beta_t is allowed to move.
    verbose : bool
        If True, prints log-likelihood by iteration.

    Returns
    -------
    TVPRegressionResult
    """
    # Convert to pandas Series / DataFrame to preserve index if possible
    if isinstance(y, pd.Series):
        index = y.index
        y_values = y.astype(float).to_numpy()
    else:
        y_values = np.asarray(y, float)
        index = None

    if isinstance(X, pd.DataFrame):
        X_values = X.astype(float).to_numpy()
    else:
        X_values = np.asarray(X, float)

    if y_values.ndim != 1:
        raise ValueError("y must be 1D array or Series.")
    if X_values.ndim != 2:
        raise ValueError("X must be 2D array or DataFrame.")
    if y_values.shape[0] != X_values.shape[0]:
        raise ValueError("y and X must have the same length.")

    T, k = X_values.shape

    if d_crisis is not None:
        d_arr = np.asarray(d_crisis, int)
        if d_arr.shape[0] != T:
            raise ValueError("d_crisis must have length T if provided.")
    else:
        d_arr = None

    # Initial OLS (ridge-regularized) on observed points
    obs_mask = np.isfinite(y_values) & np.all(np.isfinite(X_values), axis=1)
    if not np.any(obs_mask):
        raise ValueError("No finite observations in y/X for initialization.")

    X_obs = X_values[obs_mask]
    y_obs = y_values[obs_mask]

    # (X'X + λI)^(-1) X'y
    lam_ridge = 1e-4
    XtX = X_obs.T @ X_obs
    beta_ols = np.linalg.solve(
        XtX + lam_ridge * np.eye(k),
        X_obs.T @ y_obs,
    )

    # Initial Q_beta and observation variance(s)
    Q_beta = q_beta * np.eye(k)

    resid = y_obs - X_obs @ beta_ols
    var_resid = float(np.var(resid)) if resid.size > 1 else float(resid**2 + 1e-4)
    R_normal = max(var_resid, 1e-6)

    if d_arr is not None and np.any(d_arr[obs_mask] == 1):
        # crude split by regime for initialization
        crisis_mask_obs = obs_mask & (d_arr == 1)
        normal_mask_obs = obs_mask & (d_arr == 0)

        if np.any(normal_mask_obs):
            rn_resid = y_values[normal_mask_obs] - X_values[normal_mask_obs] @ beta_ols
            R_normal = max(float(np.var(rn_resid)), 1e-6)

        if np.any(crisis_mask_obs):
            rc_resid = y_values[crisis_mask_obs] - X_values[crisis_mask_obs] @ beta_ols
            R_crisis = max(float(np.var(rc_resid)), 1e-6)
        else:
            R_crisis = None
    else:
        R_crisis = None

    loglik_history: list[float] = []

    # Pre-initialize to keep static analyzers happy; they are overwritten in EM loop.
    beta_smooth = np.zeros((T, k), dtype=float)
    P_smooth = np.zeros((T, k, k), dtype=float)

    # EM loop
    for it in range(max_iter):
        # E-step: filter + smoother for beta_t
        beta_filt, P_filt, a_pred, P_pred, ll_vec = _kalman_filter_tvp(
            y=y_values,
            X=X_values,
            d_crisis=d_arr,
            Q_beta=Q_beta,
            R_normal=R_normal,
            R_crisis=R_crisis,
        )
        beta_smooth, P_smooth = _kalman_smoother_tvp(
            beta_filt=beta_filt,
            P_filt=P_filt,
            P_pred=P_pred,
        )

        loglik = float(np.sum(ll_vec))
        loglik_history.append(loglik)

        if verbose:
            print(
                f"[TVP EM] iter {it+1:3d}, loglik={loglik:.3f}, "
                f"R_normal={R_normal:.4g}, "
                f"R_crisis={R_crisis if R_crisis is not None else np.nan:.4g}"
            )

        # M-step: update R_normal / R_crisis via expected squared residuals
        # residual_t = y_t - x_t' beta_t, E[resid^2] = (mean)^2 + x' P x
        obs_mask = np.isfinite(y_values)
        if not np.any(obs_mask):
            break

        Enorm_sum = 0.0
        Enorm_count = 0
        Ecrisis_sum = 0.0
        Ecrisis_count = 0

        for t in range(T):
            if not obs_mask[t]:
                continue

            x_t = X_values[t]
            beta_t = beta_smooth[t]
            P_t = P_smooth[t]

            mean_resid = float(y_values[t] - x_t @ beta_t)
            var_pred = float(x_t @ P_t @ x_t)

            E_resid2 = mean_resid**2 + var_pred

            if d_arr is not None and R_crisis is not None and d_arr[t] == 1:
                Ecrisis_sum += E_resid2
                Ecrisis_count += 1
            else:
                Enorm_sum += E_resid2
                Enorm_count += 1

        # update variances with small floor
        if Enorm_count > 0:
            R_normal_new = max(Enorm_sum / Enorm_count, 1e-6)
        else:
            R_normal_new = R_normal

        if d_arr is not None and R_crisis is not None and Ecrisis_count > 0:
            R_crisis_new = max(Ecrisis_sum / Ecrisis_count, 1e-6)
        else:
            R_crisis_new = R_crisis

        # Check convergence on log-likelihood
        if it > 0:
            ll_old = loglik_history[-2]
            rel_impr = (loglik - ll_old) / (1.0 + abs(ll_old))
            if rel_impr < tol:
                R_normal = R_normal_new
                R_crisis = R_crisis_new
                break

        R_normal = R_normal_new
        R_crisis = R_crisis_new

    # final fitted values
    y_hat = np.einsum("tk,tk->t", X_values, beta_smooth)

    return TVPRegressionResult(
        beta_smooth=beta_smooth,
        P_smooth=P_smooth,
        y_hat=y_hat,
        Q_beta=Q_beta,
        R_normal=R_normal,
        R_crisis=R_crisis,
        loglik_history=loglik_history,
        index=index,
    )
