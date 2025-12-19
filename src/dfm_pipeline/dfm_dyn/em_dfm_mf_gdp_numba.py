# src/dfm_pipeline/dfm_dyn/em_dfm_mf_gdp_numba.py
#!/usr/bin/env python
"""
Numba-accelerated wrapper around dfm_pipeline.dfm_dyn.em_dfm_mf_gdp.

Debug support:
- Set env var DFM_DISABLE_NUMBA=1 to force pure-Python execution (njit becomes a no-op).
  This is useful for readable tracebacks.

Example:
  DFM_DISABLE_NUMBA=1 python scripts/dfm_bm/run_mf_dfm_cv_oos_fast.py ... --no-parallel --debug --stop-on-error
"""

from __future__ import annotations

import os
import numpy as np

_FORCE_NO_NUMBA = os.environ.get("DFM_DISABLE_NUMBA", "0").strip() in ("1", "true", "True", "YES", "yes")

try:
    if _FORCE_NO_NUMBA:
        raise ImportError("Forced no-numba via DFM_DISABLE_NUMBA=1")

    from numba import njit  # type: ignore
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False

    def njit(*args, **kwargs):
        """
        No-op replacement for numba.njit that supports BOTH decorator styles:
          - @njit
          - @njit(...)
        """
        if len(args) == 1 and callable(args[0]) and not kwargs:
            # used as: @njit
            return args[0]

        # used as: @njit(...)
        def decorator(func):
            return func

        return decorator


# import the baseline implementation
from dfm_pipeline.dfm_dyn import em_dfm_mf_gdp as base


@njit
def _build_obs_slices(y_t, C_mat, R_meas):
    """
    Given y_t (length p) with NaNs for missing obs, build:
    - idx: integer indices of observed entries
    - y_obs: (k,) observed values
    - C_t: (k, m) observation matrix rows for observed entries
    - R_t: (k, k) submatrix of R_meas
    """
    p = y_t.shape[0]
    m = C_mat.shape[1]

    k = 0
    for i in range(p):
        if not np.isnan(y_t[i]):
            k += 1

    if k == 0:
        return (
            np.empty(0, dtype=np.int64),
            np.empty(0),
            np.empty((0, m)),
            np.empty((0, 0)),
        )

    idx = np.empty(k, dtype=np.int64)
    pos = 0
    for i in range(p):
        if not np.isnan(y_t[i]):
            idx[pos] = i
            pos += 1

    y_obs = np.empty(k)
    for i in range(k):
        y_obs[i] = y_t[idx[i]]

    C_t = np.empty((k, m))
    for i in range(k):
        row_idx = idx[i]
        for j in range(m):
            C_t[i, j] = C_mat[row_idx, j]

    R_t = np.empty((k, k))
    for i in range(k):
        for j in range(k):
            R_t[i, j] = R_meas[idx[i], idx[j]]

    return idx, y_obs, C_t, R_t


@njit
def _regularize_symmetric(S, eps):
    """
    Symmetrize S and add eps to diagonal (in-place).
    """
    k = S.shape[0]
    for i in range(k):
        for j in range(i + 1, k):
            avg = 0.5 * (S[i, j] + S[j, i])
            S[i, j] = avg
            S[j, i] = avg
    for i in range(k):
        S[i, i] = S[i, i] + eps


@njit
def _kalman_filter_smoother_numba(Y, T_mat, Q_mat, C_mat, R_meas, a0, P0):
    """
    Kalman filter + RTS smoother. Works in numba mode if available, otherwise pure python.
    """
    T = Y.shape[0]
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

            _regularize_symmetric(S_t, eps_reg)

            L = np.linalg.cholesky(S_t)
            Linv = np.linalg.inv(L)
            S_inv = Linv.T @ Linv

            K_t = P_pred_t @ C_t.T @ S_inv

            a_filt_t = a_pred + K_t @ v_t
            P_filt_t = (eye_m - K_t @ C_t) @ P_pred_t
            P_filt_t = 0.5 * (P_filt_t + P_filt_t.T)

            diag_L = np.empty(k)
            for i in range(k):
                diag_L[i] = L[i, i]
            log_det_S = 2.0 * np.sum(np.log(diag_L))
            ll_t = -0.5 * (v_t @ (S_inv @ v_t) + log_det_S + k * np.log(2.0 * np.pi))
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

    alpha_smooth = np.zeros((T, m))
    P_smooth = np.zeros((T, m, m))
    P_lag = np.zeros((T, m, m))

    alpha_smooth[T - 1] = alpha_filt[T - 1]
    P_smooth[T - 1] = P_filt[T - 1]

    for t in range(T - 2, -1, -1):
        P_pred_next = P_pred[t + 1]
        M = P_filt[t] @ T_mat.T
        J_t = np.linalg.solve(P_pred_next.T, M.T).T

        alpha_smooth[t] = alpha_filt[t] + J_t @ (alpha_smooth[t + 1] - alpha_pred[t + 1])
        P_smooth[t] = P_filt[t] + J_t @ (P_smooth[t + 1] - P_pred[t + 1]) @ J_t.T
        P_smooth[t] = 0.5 * (P_smooth[t] + P_smooth[t].T)
        P_lag[t + 1] = P_smooth[t + 1] @ J_t.T

    return alpha_smooth, P_smooth, P_lag, loglik


@njit
def _kalman_filter_only_numba(Y, T_mat, Q_mat, C_mat, R_meas, a0, P0):
    """
    Forward-only Kalman filter. Works in numba mode if available, otherwise pure python.
    """
    T = Y.shape[0]
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


def _kalman_filter_smoother_patched(Y, T_mat, Q_mat, C_mat, R_meas, a0, P0):
    Y_arr = np.asarray(Y, dtype=np.float64)
    T_arr = np.asarray(T_mat, dtype=np.float64)
    Q_arr = np.asarray(Q_mat, dtype=np.float64)
    C_arr = np.asarray(C_mat, dtype=np.float64)
    R_arr = np.asarray(R_meas, dtype=np.float64)
    a0_arr = np.asarray(a0, dtype=np.float64)
    P0_arr = np.asarray(P0, dtype=np.float64)

    return _kalman_filter_smoother_numba(Y_arr, T_arr, Q_arr, C_arr, R_arr, a0_arr, P0_arr)


def _kalman_filter_only_patched(Y, T, Q, C, R_meas, a0, P0):
    Y_arr = np.asarray(Y, dtype=np.float64)
    T_arr = np.asarray(T, dtype=np.float64)
    Q_arr = np.asarray(Q, dtype=np.float64)
    C_arr = np.asarray(C, dtype=np.float64)
    R_arr = np.asarray(R_meas, dtype=np.float64)
    a0_arr = np.asarray(a0, dtype=np.float64)
    P0_arr = np.asarray(P0, dtype=np.float64)

    return _kalman_filter_only_numba(Y_arr, T_arr, Q_arr, C_arr, R_arr, a0_arr, P0_arr)


# monkey-patch baseline EM to use these routines
base._kalman_filter_smoother = _kalman_filter_smoother_patched
base.kalman_filter_only = _kalman_filter_only_patched

# public API re-export
em_dfm_mf_gdp_full = base.em_dfm_mf_gdp_full
build_state_matrices_from_params = base.build_state_matrices_from_params
kalman_filter_only = base.kalman_filter_only
mariano_murasawa_from_monthly = base.mariano_murasawa_from_monthly
MixedFreqDFMParams = base.MixedFreqDFMParams
