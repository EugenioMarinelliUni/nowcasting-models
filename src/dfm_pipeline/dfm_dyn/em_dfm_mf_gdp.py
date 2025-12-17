#!/usr/bin/env python
"""Mixed-frequency Dynamic Factor Model for monthly indicators + quarterly GDP.

Model sketch (state-space):

  Factors (VAR(p)) in companion form:
    f_t = Phi_1 f_{t-1} + ... + Phi_p f_{t-p} + u_t,    u_t ~ N(0, Q_u)

  Idiosyncratic components (AR(1), diagonal):
    eps_{i,t} = phi_i eps_{i,t-1} + e_{i,t},            e_{i,t} ~ N(0, sigma_{e,i}^2)

  Monthly GDP latent (ARX(1) on factors + AR(1) on itself):
    y^m_t = gamma' f_t + phi_y y^m_{t-1} + eta_t,        eta_t ~ N(0, sigma_eta^2)

  Monthly indicator measurement (nearly exact, but configurable):
    x_{i,t} = lambda_i' f_t + eps_{i,t} + meas_noise

  Quarterly GDP measurement at month t (Mariano–Murasawa weights on monthly GDP lags):
    y^q_t = w0 y^m_t + w1 y^m_{t-1} + ... + w4 y^m_{t-4} + v_t,
    v_t ~ N(0, sigma_v^2)

Public entry points:
  - em_dfm_mf_gdp_full: EM-style estimation (approximate M-step).
  - build_state_matrices_from_params: rebuild (T,Q,C,R,a0,P0) from fitted params.
  - kalman_filter_only: forward-only filter for one-sided nowcasts.
  - mariano_murasawa_from_monthly: deterministic quarterly mapping from monthly GDP.

Notes:
  - This implementation allows NaNs in X and y_q; missing entries are handled by
    skipping measurement updates at those entries.
  - The M-step is intentionally approximate (OLS on smoothed states). This is
    consistent with the existing project design; it is not a full closed-form EM.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Optional, Union

import numpy as np

from .state_space import (
    StateSpaceParams,
    kalman_filter_smoother as _kalman_filter_smoother_shared,
    kalman_filter_only as _kalman_filter_only_shared,
)

# optional tqdm for EM progress
try:
    from tqdm.auto import trange as _trange
except Exception:  # noqa: BLE001
    _trange = range


ArrayLike = Union[np.ndarray]


@dataclass
class MixedFreqDFMParams:
    # Measurement-side parameters
    Lambda_x: np.ndarray            # (n, r) loadings for monthly indicators
    Phi_factors: List[np.ndarray]   # list of p (r, r) VAR coefficient matrices
    Q_u: np.ndarray                 # (r, r) factor innovations covariance

    Phi_eps_diag: np.ndarray        # (n,) AR(1) coefficients for idios
    R_e_diag: np.ndarray            # (n,) idios innovation variances

    gamma: np.ndarray               # (r,) loading from factors to monthly GDP
    phi_y: float                    # AR(1) coefficient for monthly GDP
    sigma_eta2: float               # variance of monthly GDP innovation
    sigma_v2: float                 # variance of quarterly measurement noise

    # Measurement variance for monthly indicators
    sigma_x_meas2: float            # scalar; R_meas[:n,:n] = sigma_x_meas2 * I

    # Smoothed states (from final EM iteration)
    factors_smooth: np.ndarray      # (T, r)
    monthly_gdp_smooth: np.ndarray  # (T,)

    # Loglikelihood history across EM iterations
    loglik_history: List[float]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _sym(A: np.ndarray) -> np.ndarray:
    return 0.5 * (A + A.T)


def _init_factors_pca(X: np.ndarray, r: int) -> Tuple[np.ndarray, np.ndarray]:
    """PCA initialization with column-mean imputation and centering.

    Returns:
      - Lambda_x_init: (n, r)
      - F_init:        (T, r)
    """
    X = np.asarray(X, float)
    X_filled = X.copy()

    col_means = np.nanmean(X_filled, axis=0)
    nan_pos = np.where(np.isnan(X_filled))
    X_filled[nan_pos] = np.take(col_means, nan_pos[1])

    Xc = X_filled - X_filled.mean(axis=0, keepdims=True)
    U, S, Vt = np.linalg.svd(Xc, full_matrices=False)

    F_init = U[:, :r] * S[:r]

    # Lambda via ridge-stabilized regression: Xc ≈ F_init Lambda'
    FtF = F_init.T @ F_init
    FtF = FtF + 1e-8 * np.eye(r)
    Lambda_x_init = np.linalg.solve(FtF, F_init.T @ Xc).T

    return Lambda_x_init, F_init


def _build_factor_companion(Phi_list: List[np.ndarray], Q_u: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Build companion-form transition (T_f, Q_f) for VAR(p) factors."""
    p = len(Phi_list)
    r = Phi_list[0].shape[0]

    if p == 0:
        return np.zeros((r, r)), Q_u.copy()

    T_f = np.zeros((r * p, r * p))
    for i, Phi_i in enumerate(Phi_list):
        T_f[:r, i * r : (i + 1) * r] = Phi_i

    if p > 1:
        for j in range(1, p):
            T_f[j * r : (j + 1) * r, (j - 1) * r : j * r] = np.eye(r)

    Q_f = np.zeros((r * p, r * p))
    Q_f[:r, :r] = Q_u
    return T_f, Q_f


def mariano_murasawa_from_monthly(y_m: np.ndarray) -> np.ndarray:
    """Mariano–Murasawa quarterly approximation from monthly GDP growth.

    For t >= 4:
      y_q_hat[t] = (1/3) y_t + (2/3) y_{t-1} + 1*y_{t-2} + (2/3) y_{t-3} + (1/3) y_{t-4}

    Returns (T,) array with NaNs for t < 4.
    """
    y_m = np.asarray(y_m, float)
    Tn = y_m.shape[0]
    y_q_hat = np.full(Tn, np.nan, dtype=float)
    if Tn < 5:
        return y_q_hat

    for t in range(4, Tn):
        y_q_hat[t] = (
            (1.0 / 3.0) * y_m[t]
            + (2.0 / 3.0) * y_m[t - 1]
            + 1.0 * y_m[t - 2]
            + (2.0 / 3.0) * y_m[t - 3]
            + (1.0 / 3.0) * y_m[t - 4]
        )

    return y_q_hat


# ---------------------------------------------------------------------------
# Kalman wrappers (kept as module-level functions so the numba wrapper can patch)
# ---------------------------------------------------------------------------


def _kalman_filter_smoother(
    Y: np.ndarray,
    T_mat: np.ndarray,
    Q_mat: np.ndarray,
    C_mat: np.ndarray,
    R_meas: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    """Filter + RTS smoother wrapper around dfm_dyn.state_space.

    Returns:
      alpha_smooth: (T, m)
      P_smooth:     (T, m, m)
      P_lag:        (T, m, m), where P_lag[t] = Cov(alpha_t, alpha_{t-1} | Y), t>=1
      loglik:       float
    """
    ss = StateSpaceParams(
        T=np.asarray(T_mat, float),
        Q=np.asarray(Q_mat, float),
        C=np.asarray(C_mat, float),
        R=np.asarray(R_meas, float),
        a0=np.asarray(a0, float),
        P0=np.asarray(P0, float),
    )
    ks = _kalman_filter_smoother_shared(np.asarray(Y, float), ss)
    return ks.a_smooth, ks.P_smooth, ks.P_lag_smooth, float(ks.loglik)


def kalman_filter_only(
    Y: np.ndarray,
    T: Optional[np.ndarray] = None,
    Q: Optional[np.ndarray] = None,
    C: Optional[np.ndarray] = None,
    R_meas: Optional[np.ndarray] = None,
    a0: Optional[np.ndarray] = None,
    P0: Optional[np.ndarray] = None,
    # Backward-compatible aliases
    T_mat: Optional[np.ndarray] = None,
    Q_mat: Optional[np.ndarray] = None,
    C_mat: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Forward-only Kalman filter wrapper around dfm_dyn.state_space.

    This signature is intentionally permissive because scripts in this repo use
    both positional and keyword argument styles.

    Required: Y and the 6 state-space objects (T,Q,C,R_meas,a0,P0).

    Returns:
      alpha_filt: (T, m)
      P_filt:     (T, m, m)
    """
    if T is None:
        T = T_mat
    if Q is None:
        Q = Q_mat
    if C is None:
        C = C_mat

    if T is None or Q is None or C is None or R_meas is None or a0 is None or P0 is None:
        raise ValueError("kalman_filter_only requires T, Q, C, R_meas, a0, P0")

    ss = StateSpaceParams(
        T=np.asarray(T, float),
        Q=np.asarray(Q, float),
        C=np.asarray(C, float),
        R=np.asarray(R_meas, float),
        a0=np.asarray(a0, float),
        P0=np.asarray(P0, float),
    )
    _a_pred, _P_pred, a_filt, P_filt, _ll = _kalman_filter_only_shared(np.asarray(Y, float), ss)
    return a_filt, P_filt


# ---------------------------------------------------------------------------
# EM for mixed-frequency DFM with Mariano–Murasawa quarterly GDP
# ---------------------------------------------------------------------------


def em_dfm_mf_gdp_full(
    X: np.ndarray,
    y_q: np.ndarray,
    r: int,
    p: int,
    max_iter: int = 30,
    tol: float = 1e-4,
    verbose: bool = False,
    use_tqdm: bool = False,
    sigma_x_meas2: float = 1e-6,
) -> MixedFreqDFMParams:
    """Estimate the mixed-frequency DFM.

    Inputs:
      X:   (T, n) standardized monthly indicators, NaNs allowed.
      y_q: (T,)   standardized quarterly GDP on the same monthly index,
                 NaN except at quarter-end months.

    Output:
      MixedFreqDFMParams with fitted parameters and final smoothed states.

    Important:
      - sigma_x_meas2 controls the indicator measurement noise (small but >0).
      - The M-step is approximate (OLS on smoothed states).
    """
    X = np.asarray(X, float)
    y_q = np.asarray(y_q, float)
    Tn, n = X.shape

    if y_q.shape[0] != Tn:
        raise ValueError("X and y_q must share the same time length")

    sigma_x_meas2 = float(sigma_x_meas2)
    if not np.isfinite(sigma_x_meas2) or sigma_x_meas2 <= 0.0:
        raise ValueError("sigma_x_meas2 must be finite and > 0")

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    # 1) PCA for factor loadings + factor paths
    Lambda_x, F_init = _init_factors_pca(X, r)

    # 2) Idios residuals and AR(1) init
    X_hat0 = F_init @ Lambda_x.T
    E_init = X - X_hat0

    Phi_eps_diag = np.zeros(n)
    R_e_diag = np.ones(n)

    for i in range(n):
        e_i = E_init[:, i]
        mask_i = ~np.isnan(e_i)
        e_i = e_i[mask_i]

        if e_i.size < 3:
            Phi_eps_diag[i] = 0.0
            R_e_diag[i] = float(np.nanvar(e_i)) if e_i.size > 1 else 1.0
            if not np.isfinite(R_e_diag[i]) or R_e_diag[i] <= 1e-8:
                R_e_diag[i] = 1.0
            continue

        e_lag = e_i[:-1]
        e_cur = e_i[1:]
        den = float(e_lag @ e_lag) + 1e-8
        phi_i = float((e_lag @ e_cur) / den)
        phi_i = float(np.clip(phi_i, -0.99, 0.99))

        resid = e_cur - phi_i * e_lag
        var_i = float(np.var(resid, ddof=1)) if resid.size > 1 else 1.0
        if not np.isfinite(var_i) or var_i <= 1e-10:
            var_i = 1.0

        Phi_eps_diag[i] = phi_i
        R_e_diag[i] = var_i

    # 3) Factor VAR(p) init
    Phi_factors: List[np.ndarray] = []
    if p > 0:
        F = F_init
        T_eff = Tn - p
        Y_var = F[p:]
        X_var = np.column_stack([F[p - j - 1 : Tn - j - 1] for j in range(p)])
        XtX = X_var.T @ X_var + 1e-8 * np.eye(r * p)
        XtY = X_var.T @ Y_var
        B = np.linalg.solve(XtX, XtY)  # (r*p, r)
        for j in range(p):
            Phi_factors.append(B[j * r : (j + 1) * r].T)
        U = Y_var - X_var @ B
        Q_u = (U.T @ U) / max(T_eff, 1)
        Q_u = _sym(Q_u)
        # floor diagonal
        Q_u = Q_u + np.diag(np.maximum(1e-10 - np.diag(Q_u), 0.0))
    else:
        Phi_factors = []
        Q_u = np.cov(F_init.T) if Tn > 1 else np.eye(r)
        Q_u = _sym(Q_u) + 1e-8 * np.eye(r)

    # 4) Monthly GDP init from first factor proxy
    gamma = np.zeros(r)
    phi_y = 0.0
    sigma_eta2 = 1.0
    sigma_v2 = 1.0

    f1 = F_init[:, 0]
    mask_q = ~np.isnan(y_q)
    if np.any(mask_q):
        f1_std = (f1 - np.mean(f1)) / (np.std(f1, ddof=1) + 1e-8)
        y_m_init = f1_std

        FtF = F_init.T @ F_init + 1e-8 * np.eye(r)
        gamma = np.linalg.solve(FtF, F_init.T @ y_m_init)

        y_lag = y_m_init[:-1]
        y_cur = y_m_init[1:]
        den = float(y_lag @ y_lag) + 1e-8
        phi_y = float(np.clip((y_lag @ y_cur) / den, -0.99, 0.99))
        resid_y = y_cur - phi_y * y_lag
        sigma_eta2 = float(np.var(resid_y, ddof=1)) if resid_y.size > 1 else 1.0
        if not np.isfinite(sigma_eta2) or sigma_eta2 <= 1e-10:
            sigma_eta2 = 1.0

        y_q_hat0 = mariano_murasawa_from_monthly(y_m_init)
        mask_q2 = mask_q & ~np.isnan(y_q_hat0)
        if np.any(mask_q2):
            err0 = y_q_hat0[mask_q2] - y_q[mask_q2]
            sigma_v2 = float(np.var(err0, ddof=1)) if err0.size > 1 else 1.0
        if not np.isfinite(sigma_v2) or sigma_v2 <= 1e-10:
            sigma_v2 = 1.0

    # State layout:
    #   alpha_t = [f_t (companion), eps_t (n), y^m_t, y^m_{t-1}, ..., y^m_{t-4}]'
    if p > 0:
        T_f, Q_f = _build_factor_companion(Phi_factors, Q_u)
    else:
        T_f = np.zeros((r, r))
        Q_f = Q_u.copy()

    m_f = T_f.shape[0]
    idx_eps_start = m_f
    idx_y0 = m_f + n

    m = m_f + n + 5
    a0 = np.zeros(m)
    P0 = np.eye(m) * 1e4

    def build_state_matrices(
        Lambda_x_loc: np.ndarray,
        Phi_factors_loc: List[np.ndarray],
        Q_u_loc: np.ndarray,
        Phi_eps_diag_loc: np.ndarray,
        R_e_diag_loc: np.ndarray,
        gamma_loc: np.ndarray,
        phi_y_loc: float,
        sigma_eta2_loc: float,
        sigma_v2_loc: float,
        sigma_x_meas2_loc: float,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        # Factors
        if p > 0:
            T_f_loc, Q_f_loc = _build_factor_companion(Phi_factors_loc, Q_u_loc)
        else:
            T_f_loc = np.zeros((r, r))
            Q_f_loc = Q_u_loc.copy()

        m_f_loc = T_f_loc.shape[0]
        m_loc = m_f_loc + n + 5

        idx_eps = m_f_loc
        idx_y0_loc = m_f_loc + n
        idx_y1_loc = idx_y0_loc + 1
        idx_y2_loc = idx_y0_loc + 2
        idx_y3_loc = idx_y0_loc + 3
        idx_y4_loc = idx_y0_loc + 4

        T_mat = np.zeros((m_loc, m_loc))
        Q_mat = np.zeros((m_loc, m_loc))

        # factor companion
        T_mat[:m_f_loc, :m_f_loc] = T_f_loc
        Q_mat[:m_f_loc, :m_f_loc] = Q_f_loc

        # idios AR(1)
        T_mat[idx_eps : idx_eps + n, idx_eps : idx_eps + n] = np.diag(Phi_eps_diag_loc)
        Q_mat[idx_eps : idx_eps + n, idx_eps : idx_eps + n] = np.diag(R_e_diag_loc)

        # monthly GDP state
        # y^m_t depends on contemporaneous f_t (first r states) and lagged y^m_{t-1}
        T_mat[idx_y0_loc, :r] = gamma_loc
        T_mat[idx_y0_loc, idx_y1_loc] = float(phi_y_loc)
        Q_mat[idx_y0_loc, idx_y0_loc] = float(sigma_eta2_loc)

        # GDP lags shift
        T_mat[idx_y1_loc, idx_y0_loc] = 1.0
        T_mat[idx_y2_loc, idx_y1_loc] = 1.0
        T_mat[idx_y3_loc, idx_y2_loc] = 1.0
        T_mat[idx_y4_loc, idx_y3_loc] = 1.0

        # measurement
        k_loc = n + 1
        C_mat = np.zeros((k_loc, m_loc))

        # monthly indicators
        C_mat[:n, :r] = Lambda_x_loc
        C_mat[:n, idx_eps : idx_eps + n] = np.eye(n)

        # quarterly GDP (MM weights)
        C_mat[n, idx_y0_loc] = 1.0 / 3.0
        C_mat[n, idx_y1_loc] = 2.0 / 3.0
        C_mat[n, idx_y2_loc] = 1.0
        C_mat[n, idx_y3_loc] = 2.0 / 3.0
        C_mat[n, idx_y4_loc] = 1.0 / 3.0

        R_meas = np.zeros((k_loc, k_loc))
        R_meas[:n, :n] = float(sigma_x_meas2_loc) * np.eye(n)
        R_meas[n, n] = float(sigma_v2_loc)

        return T_mat, Q_mat, C_mat, R_meas

    # Observation array
    Y = np.column_stack([X, y_q])

    loglik_history: List[float] = []
    prev_loglik = -np.inf

    it_range = _trange(max_iter) if use_tqdm else range(max_iter)

    for it in it_range:
        T_mat, Q_mat, C_mat, R_meas = build_state_matrices(
            Lambda_x,
            Phi_factors,
            Q_u,
            Phi_eps_diag,
            R_e_diag,
            gamma,
            phi_y,
            sigma_eta2,
            sigma_v2,
            sigma_x_meas2,
        )

        alpha_smooth, _P_smooth, _P_lag, loglik = _kalman_filter_smoother(
            Y=Y,
            T_mat=T_mat,
            Q_mat=Q_mat,
            C_mat=C_mat,
            R_meas=R_meas,
            a0=a0,
            P0=P0,
        )

        loglik_history.append(float(loglik))

        if verbose:
            print(f"EM iter {it:3d}: loglik = {loglik:.6f}")

        if it > 0:
            denom = abs(prev_loglik) + 1e-8
            if abs((loglik - prev_loglik) / denom) < tol:
                break
        prev_loglik = float(loglik)

        # Extract smoothed components
        factors_smooth = alpha_smooth[:, :r]
        monthly_gdp_smooth = alpha_smooth[:, idx_y0]

        # ------------------------------------------------------------------
        # M-step (approximate): update parameters from smoothed states
        # ------------------------------------------------------------------

        # (1) Factor VAR(p)
        if p > 0:
            F = factors_smooth
            T_eff = Tn - p
            Y_var = F[p:]
            X_var = np.column_stack([F[p - j - 1 : Tn - j - 1] for j in range(p)])
            XtX = X_var.T @ X_var + 1e-8 * np.eye(r * p)
            XtY = X_var.T @ Y_var
            B = np.linalg.solve(XtX, XtY)
            Phi_factors = [B[j * r : (j + 1) * r].T for j in range(p)]
            U = Y_var - X_var @ B
            Q_u = (U.T @ U) / max(T_eff, 1)
            Q_u = _sym(Q_u)
            Q_u = Q_u + np.diag(np.maximum(1e-10 - np.diag(Q_u), 0.0))
        else:
            Phi_factors = []
            Q_u = np.cov(factors_smooth.T) if Tn > 1 else np.eye(r)
            Q_u = _sym(Q_u) + 1e-8 * np.eye(r)

        # (2) Loadings Lambda_x via masked regressions X_i ~ factors
        Lambda_new = np.zeros_like(Lambda_x)
        for i in range(n):
            mask_i = ~np.isnan(X[:, i])
            Ti = int(mask_i.sum())
            if Ti <= r:
                Lambda_new[i] = Lambda_x[i]
                continue
            Fi = factors_smooth[mask_i]
            yi = X[mask_i, i]
            beta, _, _, _ = np.linalg.lstsq(Fi, yi, rcond=None)
            Lambda_new[i] = beta
        Lambda_x = Lambda_new

        # Residuals for idios updates
        X_hat = factors_smooth @ Lambda_x.T
        E_res = X - X_hat

        # (3) Idios AR(1)
        for i in range(n):
            e_i = E_res[:, i]
            mask_i = ~np.isnan(e_i)
            e_i = e_i[mask_i]
            if e_i.size < 3:
                continue
            e_lag = e_i[:-1]
            e_cur = e_i[1:]
            den = float(e_lag @ e_lag) + 1e-8
            phi_i = float(np.clip((e_lag @ e_cur) / den, -0.99, 0.99))
            resid = e_cur - phi_i * e_lag
            var_i = float(np.var(resid, ddof=1)) if resid.size > 1 else R_e_diag[i]
            if not np.isfinite(var_i) or var_i <= 1e-10:
                var_i = float(R_e_diag[i])
            Phi_eps_diag[i] = phi_i
            R_e_diag[i] = var_i

        # (4) Monthly GDP regression y^m_t ~ factors_{t-1} + y^m_{t-1}
        y_m = monthly_gdp_smooth
        y_lag = y_m[:-1]
        F_lag = factors_smooth[:-1]
        y_cur = y_m[1:]
        Z = np.column_stack([F_lag, y_lag])
        XtX = Z.T @ Z + 1e-8 * np.eye(r + 1)
        XtY = Z.T @ y_cur
        beta = np.linalg.solve(XtX, XtY)
        gamma = beta[:r]
        phi_y = float(np.clip(beta[-1], -0.99, 0.99))
        resid_y = y_cur - Z @ beta
        sigma_eta2 = float(np.var(resid_y, ddof=1)) if resid_y.size > 1 else sigma_eta2
        if not np.isfinite(sigma_eta2) or sigma_eta2 <= 1e-10:
            sigma_eta2 = 1.0

        # (5) Quarterly measurement variance sigma_v2
        y_q_hat_mm = mariano_murasawa_from_monthly(y_m)
        mask_q_mm = ~np.isnan(y_q) & ~np.isnan(y_q_hat_mm)
        if np.any(mask_q_mm):
            err_q = y_q_hat_mm[mask_q_mm] - y_q[mask_q_mm]
            sigma_v2 = float(np.var(err_q, ddof=1)) if err_q.size > 1 else sigma_v2
            if not np.isfinite(sigma_v2) or sigma_v2 <= 1e-10:
                sigma_v2 = 1.0

    # Final smoother pass (for returned smoothed states)
    T_mat, Q_mat, C_mat, R_meas = build_state_matrices(
        Lambda_x,
        Phi_factors,
        Q_u,
        Phi_eps_diag,
        R_e_diag,
        gamma,
        phi_y,
        sigma_eta2,
        sigma_v2,
        sigma_x_meas2,
    )

    alpha_smooth, _P_smooth, _P_lag, loglik_final = _kalman_filter_smoother(
        Y=Y,
        T_mat=T_mat,
        Q_mat=Q_mat,
        C_mat=C_mat,
        R_meas=R_meas,
        a0=a0,
        P0=P0,
    )

    factors_smooth = alpha_smooth[:, :r]
    monthly_gdp_smooth = alpha_smooth[:, idx_y0]
    loglik_history.append(float(loglik_final))

    return MixedFreqDFMParams(
        Lambda_x=Lambda_x,
        Phi_factors=Phi_factors,
        Q_u=Q_u,
        Phi_eps_diag=Phi_eps_diag,
        R_e_diag=R_e_diag,
        gamma=gamma,
        phi_y=float(phi_y),
        sigma_eta2=float(sigma_eta2),
        sigma_v2=float(sigma_v2),
        sigma_x_meas2=float(sigma_x_meas2),
        factors_smooth=factors_smooth,
        monthly_gdp_smooth=monthly_gdp_smooth,
        loglik_history=loglik_history,
    )


# ---------------------------------------------------------------------------
# Helpers for OOS evaluation
# ---------------------------------------------------------------------------


def build_state_matrices_from_params(
    params: MixedFreqDFMParams,
    n: int,
    r: int,
    p: int,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, int]:
    """Build (T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0) from trained params."""

    Lambda_x = params.Lambda_x
    Phi_list = params.Phi_factors
    Q_u = params.Q_u
    Phi_eps_diag = params.Phi_eps_diag
    sigma_e2 = params.R_e_diag
    gamma = params.gamma
    phi_y = float(params.phi_y)
    sigma_eta2 = float(params.sigma_eta2)
    sigma_v2 = float(params.sigma_v2)
    sigma_x_meas2 = float(params.sigma_x_meas2)

    if p > 0:
        T_f, Q_f = _build_factor_companion(Phi_list, Q_u)
    else:
        T_f = np.zeros((r, r))
        Q_f = Q_u.copy()

    m_f = T_f.shape[0]
    m = m_f + n + 5

    idx_eps_start = m_f
    idx_y0 = m_f + n
    idx_y1 = idx_y0 + 1
    idx_y2 = idx_y0 + 2
    idx_y3 = idx_y0 + 3
    idx_y4 = idx_y0 + 4

    T_mat = np.zeros((m, m))
    Q_mat = np.zeros((m, m))

    # factors
    T_mat[:m_f, :m_f] = T_f
    Q_mat[:m_f, :m_f] = Q_f

    # idios AR(1)
    T_mat[idx_eps_start : idx_eps_start + n, idx_eps_start : idx_eps_start + n] = np.diag(Phi_eps_diag)
    Q_mat[idx_eps_start : idx_eps_start + n, idx_eps_start : idx_eps_start + n] = np.diag(sigma_e2)

    # monthly GDP
    T_mat[idx_y0, :r] = gamma
    T_mat[idx_y0, idx_y1] = phi_y
    Q_mat[idx_y0, idx_y0] = sigma_eta2

    # lags
    T_mat[idx_y1, idx_y0] = 1.0
    T_mat[idx_y2, idx_y1] = 1.0
    T_mat[idx_y3, idx_y2] = 1.0
    T_mat[idx_y4, idx_y3] = 1.0

    # measurement
    k = n + 1
    C_mat = np.zeros((k, m))
    C_mat[:n, :r] = Lambda_x
    C_mat[:n, idx_eps_start : idx_eps_start + n] = np.eye(n)

    C_mat[n, idx_y0] = 1.0 / 3.0
    C_mat[n, idx_y1] = 2.0 / 3.0
    C_mat[n, idx_y2] = 1.0
    C_mat[n, idx_y3] = 2.0 / 3.0
    C_mat[n, idx_y4] = 1.0 / 3.0

    R_meas = np.zeros((k, k))
    R_meas[:n, :n] = sigma_x_meas2 * np.eye(n)
    R_meas[n, n] = sigma_v2

    a0 = np.zeros(m)
    P0 = np.eye(m) * 1e4

    return T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0
