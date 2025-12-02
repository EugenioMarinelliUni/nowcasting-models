# src/dfm_pipeline/dfm_simple/var_kalman.py
from __future__ import annotations

from typing import List, Tuple

import numpy as np
import pandas as pd


def fit_var_ols(F: pd.DataFrame, p: int) -> Tuple[List[np.ndarray], np.ndarray]:
    """
    Fit a VAR(p) by OLS on factors F (T x r).

    Returns:
      A_list: list of p coefficient matrices A_i, each (r x r).
      Sigma_u: (r x r) covariance of residuals.
    """
    Fv = F.to_numpy(dtype=float)
    T_len, r = Fv.shape

    if T_len <= p:
        raise ValueError("Not enough observations to fit VAR(p).")

    Y = Fv[p:]            # (T-p x r), dependent
    X_list = []
    for lag in range(1, p + 1):
        X_list.append(Fv[p - lag:T_len - lag])
    # Stack lagged regressors horizontally: (T-p x (r*p))
    X = np.concatenate(X_list, axis=1)

    # No intercept
    B, _, _, _ = np.linalg.lstsq(X, Y, rcond=None)  # (r*p x r)

    # Unstack B into p matrices A_i
    A_list: List[np.ndarray] = []
    for i in range(p):
        Ai = B[i * r:(i + 1) * r, :].T  # (r x r)
        A_list.append(Ai)

    # Residuals and covariance
    U = Y - X @ B
    Sigma_u = (U.T @ U) / (U.shape[0] - r * p)

    return A_list, Sigma_u


def build_var_state_matrices(
    A_list: List[np.ndarray],
    Sigma_u: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    From VAR(p) coefficient matrices A_list and residual covariance Sigma_u,
    build the state-space matrices (T, R, Q, Z) for the companion-form VAR.

    State vector alpha_t = [F_t, F_{t-1}, ..., F_{t-p+1}]' (dim = r*p).

    Returns:
      T_mat : (rp x rp) state transition matrix
      R_mat : (rp x r)  selection matrix for shocks
      Q_mat : (r x r)   covariance of innovations eta_t (here = Sigma_u)
      Z_mat : (r x rp)  measurement matrix mapping state -> F_t
    """
    r = A_list[0].shape[0]
    p = len(A_list)
    rp = r * p

    # State transition T (companion matrix)
    T_mat = np.zeros((rp, rp), dtype=float)

    # First block row: [A1, A2, ..., Ap]
    T_mat[:r, :r * p] = np.hstack(A_list)

    # Subdiagonal identity blocks
    if p > 1:
        T_mat[r:, :-r] = np.eye(rp - r)

    # R matrix: only first r states get shocks
    R_mat = np.zeros((rp, r), dtype=float)
    R_mat[:r, :] = np.eye(r)

    # Q: innovation covariance for eta_t (same as Sigma_u)
    Q_mat = Sigma_u.copy()

    # Measurement matrix Z: picks F_t block
    Z_mat = np.zeros((r, rp), dtype=float)
    Z_mat[:, :r] = np.eye(r)

    return T_mat, R_mat, Q_mat, Z_mat


def kalman_filter(
    F_obs: np.ndarray,
    Z: np.ndarray,
    T_mat: np.ndarray,
    R_mat: np.ndarray,
    Q_mat: np.ndarray,
    H: np.ndarray,
    a1: np.ndarray,
    P1: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Standard Kalman filter for linear Gaussian state-space:

      alpha_t = T alpha_{t-1} + R eta_t,    eta_t ~ N(0, Q)
      F_obs_t = Z alpha_t + eps_t,          eps_t ~ N(0, H)

    Here:
      - F_obs: (T x r) observed factors
      - Z:     (r x n_state)
      - T_mat: (n_state x n_state)
      - R_mat: (n_state x r_shocks)
      - Q_mat: (r_shocks x r_shocks)
      - H:     (r x r) measurement noise covariance
      - a1:    (n_state,) initial state mean
      - P1:    (n_state x n_state) initial state covariance

    Returns:
      alpha_filt: (T x n_state) filtered state means alpha_{t|t}
      P_filt:    (T x n_state x n_state) filtered covariances P_{t|t}
    """
    T_len = F_obs.shape[0]
    n_state = T_mat.shape[0]

    alpha_filt = np.zeros((T_len, n_state))
    P_filt = np.zeros((T_len, n_state, n_state))

    a_pred = a1.copy()
    P_pred = P1.copy()

    for t in range(T_len):
        y_t = F_obs[t]  # (r,)

        # Predict
        if t > 0:
            a_pred = T_mat @ alpha_filt[t - 1]
            P_pred = T_mat @ P_filt[t - 1] @ T_mat.T + R_mat @ Q_mat @ R_mat.T

        # Innovation
        v_t = y_t - Z @ a_pred
        F_t = Z @ P_pred @ Z.T + H

        # Kalman gain
        K_t = P_pred @ Z.T @ np.linalg.inv(F_t)

        # Update
        a_upd = a_pred + K_t @ v_t
        P_upd = P_pred - K_t @ Z @ P_pred

        alpha_filt[t] = a_upd
        P_filt[t] = P_upd

    return alpha_filt, P_filt


def kalman_smoother(
    alpha_filt: np.ndarray,
    P_filt: np.ndarray,
    T_mat: np.ndarray,
    R_mat: np.ndarray,
    Q_mat: np.ndarray,
) -> np.ndarray:
    """
    Rauch-Tung-Striebel smoother to get alpha_{t|T} from alpha_{t|t}.

    Returns:
      alpha_smooth : (T x n_state)
    """
    T_len = alpha_filt.shape[0]
    n_state = alpha_filt.shape[1]

    alpha_smooth = np.zeros_like(alpha_filt)
    alpha_smooth[-1] = alpha_filt[-1]

    for t in range(T_len - 2, -1, -1):
        P_pred = T_mat @ P_filt[t] @ T_mat.T + R_mat @ Q_mat @ R_mat.T
        # Smoother gain
        J_t = P_filt[t] @ T_mat.T @ np.linalg.pinv(P_pred)

        alpha_smooth[t] = (
            alpha_filt[t]
            + J_t @ (alpha_smooth[t + 1] - T_mat @ alpha_filt[t])
        )

    return alpha_smooth
