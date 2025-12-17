#!/usr/bin/env python
"""
Numba-accelerated wrapper around dfm_pipeline.dfm_dyn.em_dfm_mf_gdp.

- Reuses the existing EM / parameter code from em_dfm_mf_gdp.py.
- Monkey-patches its _kalman_filter_smoother and kalman_filter_only
  with numba-jitted versions (when numba is available).

Import this module instead of em_dfm_mf_gdp in performance-critical scripts, e.g.:

    from dfm_pipeline.dfm_dyn.em_dfm_mf_gdp_numba import (
        em_dfm_mf_gdp_full,
        build_state_matrices_from_params,
        kalman_filter_only,
        mariano_murasawa_from_monthly,
    )
"""

import numpy as np

try:
    from numba import njit
    NUMBA_AVAILABLE = True
except ImportError:  # numba not installed; fall back to pure Python
    NUMBA_AVAILABLE = False

    def njit(*_args, **_kwargs):
        # no-op decorator if numba is not installed
        def wrapper(f):
            return f
        return wrapper


# import the baseline implementation
from dfm_pipeline.dfm_dyn import em_dfm_mf_gdp as base


@njit
def _build_obs_slices(y_t, C_mat, R_meas):
    """
    Helper for numba:

    Given y_t (length p) with NaNs for missing obs, build:

    - idx: integer indices of observed entries
    - y_obs: (k,) observed values
    - C_t: (k, m) observation matrix rows for observed entries
    - R_t: (k, k) submatrix of R_meas

    where k = number of observed entries.
    """
    p = y_t.shape[0]
    m = C_mat.shape[1]

    # count observed
    k = 0
    for i in range(p):
        if not np.isnan(y_t[i]):
            k += 1

    if k == 0:
        return np.empty(0, dtype=np.int64), np.empty(0), np.empty((0, m)), np.empty((0, 0))

    idx = np.empty(k, dtype=np.int64)
    pos = 0
    for i in range(p):
        if not np.isnan(y_t[i]):
            idx[pos] = i
            pos += 1

    # build y_obs
    y_obs = np.empty(k)
    for i in range(k):
        y_obs[i] = y_t[idx[i]]

    # build C_t (k x m)
    C_t = np.empty((k, m))
    for i in range(k):
        row_idx = idx[i]
        for j in range(m):
            C_t[i, j] = C_mat[row_idx, j]

    # build R_t (k x k)
    R_t = np.empty((k, k))
    for i in range(k):
        for j in range(k):
            R_t[i, j] = R_meas[idx[i], idx[j]]

    return idx, y_obs, C_t, R_t


@njit
def _regularize_symmetric(S, eps):
    """
    Symmetrize S and add eps to the diagonal (in-place).
    """
    k = S.shape[0]
    # symmetrize: S = (S + S.T) / 2
    for i in range(k):
        for j in range(i + 1, k):
            avg = 0.5 * (S[i, j] + S[j, i])
            S[i, j] = avg
            S[j, i] = avg
    # add ridge
    for i in range(k):
        S[i, i] = S[i, i] + eps


@njit
def _kalman_filter_smoother_numba(Y, T_mat, Q_mat, C_mat, R_meas, a0, P0):
    """
    Numba-jitted Kalman filter + RTS smoother.

    Y: (T, p) with NaNs for missing obs
    T_mat: (m, m)
    Q_mat: (m, m)
    C_mat: (p, m)
    R_meas: (p, p)
    a0: (m,)
    P0: (m, m)
    """
    T = Y.shape[0]
    p = Y.shape[1]
    m = a0.shape[0]

    alpha_pred = np.zeros((T, m))
    P_pred = np.zeros((T, m, m))
    alpha_filt = np.zeros((T, m))
    P_filt = np.zeros((T, m, m))

    loglik = 0.0

    a_prev = a0.copy()
    P_prev = P0.copy()

    eye_m = np.eye(m)
    eps_reg = 1e-6

    # forward filter (standard initialization)
    # t=0 uses prior (a0, P0) for alpha_0, then updates with y_0.
    for t in range(T):
        if t == 0:
            a_pred = a_prev
            P_pred_t = P_prev
        else:
            a_pred = T_mat @ a_prev
            P_pred_t = T_mat @ P_prev @ T_mat.T + Q_mat

        y_t = Y[t]

        # observed slices
        idx, y_obs, C_t, R_t = _build_obs_slices(y_t, C_mat, R_meas)
        k = idx.shape[0]

        if k > 0:
            v_t = y_obs - C_t @ a_pred
            S_t = C_t @ P_pred_t @ C_t.T + R_t

            # regularize S_t to be numerically PD
            _regularize_symmetric(S_t, eps_reg)

            L = np.linalg.cholesky(S_t)
            Linv = np.linalg.inv(L)
            S_inv = Linv.T @ Linv

            K_t = P_pred_t @ C_t.T @ S_inv

            a_filt_t = a_pred + K_t @ v_t
            # Joseph form is slower; keep the basic update but enforce symmetry.
            P_filt_t = (eye_m - K_t @ C_t) @ P_pred_t
            # symmetrize
            P_filt_t = 0.5 * (P_filt_t + P_filt_t.T)

            # loglik contribution
            diag_L = np.empty(k)
            for i in range(k):
                diag_L[i] = L[i, i]
            log_det_S = 2.0 * np.sum(np.log(diag_L))
            ll_t = -0.5 * (
                v_t @ (S_inv @ v_t)
                + log_det_S
                + k * np.log(2.0 * np.pi)
            )
            loglik += ll_t
        else:
            a_filt_t = a_pred
            P_filt_t = P_pred_t
            P_filt_t = 0.5 * (P_filt_t + P_filt_t.T)

        alpha_pred[t] = a_pred
        P_pred[t] = P_pred_t
        alpha_filt[t] = a_filt_t
        P_filt[t] = P_filt_t

        a_prev = a_filt_t
        P_prev = P_filt_t

    # backward RTS smoother
    alpha_smooth = np.zeros((T, m))
    P_smooth = np.zeros((T, m, m))
    P_lag = np.zeros((T, m, m))

    alpha_smooth[T - 1] = alpha_filt[T - 1]
    P_smooth[T - 1] = P_filt[T - 1]

    for t in range(T - 2, -1, -1):
        P_pred_next = P_pred[t + 1]

        # J_t = P_filt[t] T' P_pred[t+1]^{-1}, compute via solve instead of explicit inverse
        M = P_filt[t] @ T_mat.T
        # solve(P_pred_next.T, M.T).T is equivalent to M @ inv(P_pred_next)
        J_t = np.linalg.solve(P_pred_next.T, M.T).T

        alpha_smooth[t] = alpha_filt[t] + J_t @ (
            alpha_smooth[t + 1] - alpha_pred[t + 1]
        )
        P_smooth[t] = (
            P_filt[t]
            + J_t @ (P_smooth[t + 1] - P_pred[t + 1]) @ J_t.T
        )
        P_smooth[t] = 0.5 * (P_smooth[t] + P_smooth[t].T)
        P_lag[t + 1] = P_smooth[t + 1] @ J_t.T

    return alpha_smooth, P_smooth, P_lag, loglik


@njit
def _kalman_filter_only_numba(Y, T_mat, Q_mat, C_mat, R_meas, a0, P0):
    """
    Numba-jitted forward-only Kalman filter.

    Returns alpha_filt, P_filt.
    """
    T = Y.shape[0]
    p = Y.shape[1]
    m = a0.shape[0]

    alpha_filt = np.zeros((T, m))
    P_filt = np.zeros((T, m, m))

    a_prev = a0.copy()
    P_prev = P0.copy()

    eye_m = np.eye(m)
    eps_reg = 1e-6

    for t in range(T):
        if t == 0:
            a_pred = a_prev
            P_pred_t = P_prev
        else:
            a_pred = T_mat @ a_prev
            P_pred_t = T_mat @ P_prev @ T_mat.T + Q_mat

        y_t = Y[t]

        idx, y_obs, C_t, R_t = _build_obs_slices(y_t, C_mat, R_meas)
        k = idx.shape[0]

        if k > 0:
            v_t = y_obs - C_t @ a_pred
            S_t = C_t @ P_pred_t @ C_t.T + R_t

            # regularize S_t
            _regularize_symmetric(S_t, eps_reg)

            L = np.linalg.cholesky(S_t)
            Linv = np.linalg.inv(L)
            S_inv = Linv.T @ Linv

            K_t = P_pred_t @ C_t.T @ S_inv

            a_filt_t = a_pred + K_t @ v_t
            P_filt_t = (eye_m - K_t @ C_t) @ P_pred_t
            P_filt_t = 0.5 * (P_filt_t + P_filt_t.T)
        else:
            a_filt_t = a_pred
            P_filt_t = P_pred_t
            P_filt_t = 0.5 * (P_filt_t + P_filt_t.T)

        alpha_filt[t] = a_filt_t
        P_filt[t] = P_filt_t

        a_prev = a_filt_t
        P_prev = P_filt_t

    return alpha_filt, P_filt


def _kalman_filter_smoother_patched(
    Y,
    T_mat,
    Q_mat,
    C_mat,
    R_meas,
    a0,
    P0,
):
    """
    Wrapper used to monkey-patch base._kalman_filter_smoother.
    """
    Y_arr = np.asarray(Y, dtype=np.float64)
    T_arr = np.asarray(T_mat, dtype=np.float64)
    Q_arr = np.asarray(Q_mat, dtype=np.float64)
    C_arr = np.asarray(C_mat, dtype=np.float64)
    R_arr = np.asarray(R_meas, dtype=np.float64)
    a0_arr = np.asarray(a0, dtype=np.float64)
    P0_arr = np.asarray(P0, dtype=np.float64)

    return _kalman_filter_smoother_numba(
        Y_arr, T_arr, Q_arr, C_arr, R_arr, a0_arr, P0_arr
    )


def _kalman_filter_only_patched(
    Y,
    T,
    Q,
    C,
    R_meas,
    a0,
    P0,
):
    """
    Wrapper used to monkey-patch base.kalman_filter_only.
    """
    Y_arr = np.asarray(Y, dtype=np.float64)
    T_arr = np.asarray(T, dtype=np.float64)
    Q_arr = np.asarray(Q, dtype=np.float64)
    C_arr = np.asarray(C, dtype=np.float64)
    R_arr = np.asarray(R_meas, dtype=np.float64)
    a0_arr = np.asarray(a0, dtype=np.float64)
    P0_arr = np.asarray(P0, dtype=np.float64)

    return _kalman_filter_only_numba(
        Y_arr, T_arr, Q_arr, C_arr, R_arr, a0_arr, P0_arr
    )


# monkey-patch the baseline module so its EM uses the numba Kalman
base._kalman_filter_smoother = _kalman_filter_smoother_patched
base.kalman_filter_only = _kalman_filter_only_patched

# public API re-export
em_dfm_mf_gdp_full = base.em_dfm_mf_gdp_full
build_state_matrices_from_params = base.build_state_matrices_from_params
kalman_filter_only = base.kalman_filter_only
mariano_murasawa_from_monthly = base.mariano_murasawa_from_monthly
MixedFreqDFMParams = base.MixedFreqDFMParams
