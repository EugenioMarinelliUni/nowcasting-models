#!/usr/bin/env python
"""
Mixed-frequency Dynamic Factor Model for monthly indicators + quarterly GDP (Mariano–Murasawa).

Core entry point:
    em_dfm_mf_gdp_full(X, y_q, r, p, max_iter=30, tol=1e-4, ...)

Helpers for OOS evaluation:
    build_state_matrices_from_params(...)
    kalman_filter_only(...)
    mariano_murasawa_from_monthly(...)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
from numpy.linalg import LinAlgError

# optional tqdm for EM progress
try:
    from tqdm.auto import trange as _trange
except Exception:  # noqa: BLE001
    _trange = range


@dataclass
class MixedFreqDFMParams:
    # measurement-side parameters
    Lambda_x: np.ndarray          # (n, r) loadings for monthly indicators
    Phi_factors: List[np.ndarray] # list of p (r, r) VAR coefficient matrices
    Q_u: np.ndarray               # (r, r) factor innovations covariance

    Phi_eps_diag: np.ndarray      # (n,) AR(1) coefficients for idios
    R_e_diag: np.ndarray          # (n,) idios variance

    gamma: np.ndarray             # (r,) loading from factors to monthly GDP
    phi_y: float                  # AR(1) coefficient for monthly GDP
    sigma_eta2: float             # variance of monthly GDP innovation
    sigma_v2: float               # variance of quarterly measurement noise

    # smoothed states (from final EM iteration)
    factors_smooth: np.ndarray    # (T, r)
    monthly_gdp_smooth: np.ndarray  # (T,)

    # loglikelihood history across EM iterations
    loglik_history: List[float]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _init_factors_pca(X: np.ndarray, r: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    Simple PCA initialization ignoring missing values (columns-wise mean imputation).

    Returns:
        Lambda_x_init: (n, r)
        F_init:        (T, r)
    """
    X_filled = X.copy()
    # column-wise mean imputation for NaNs
    col_means = np.nanmean(X_filled, axis=0)
    idx_nan = np.where(np.isnan(X_filled))
    X_filled[idx_nan] = np.take(col_means, idx_nan[1])

    T, n = X_filled.shape
    # center
    Xc = X_filled - X_filled.mean(axis=0, keepdims=True)
    # SVD
    U, S, Vt = np.linalg.svd(Xc, full_matrices=False)
    # factors = first r columns of U * S
    F_init = U[:, :r] * S[:r]
    # loadings = regression Xc ~ F_init
    # (F'F)^{-1} F' Xc
    FtF_inv = np.linalg.inv(F_init.T @ F_init)
    Lambda_x_init = (FtF_inv @ F_init.T @ Xc).T  # (n, r)
    return Lambda_x_init, F_init


def _build_factor_companion(Phi_list: List[np.ndarray], Q_u: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build companion matrix for VAR(p) factors.

    Phi_list: list of p (r, r) matrices.
    Q_u:      (r, r) innovations covariance for f_t.

    Returns:
        T_f: (r*p, r*p)
        Q_f: (r*p, r*p)
    """
    p = len(Phi_list)
    r = Phi_list[0].shape[0]
    if p == 0:
        return np.zeros((r, r)), Q_u.copy()

    T_f = np.zeros((r * p, r * p))
    # top block row
    for i, Phi_i in enumerate(Phi_list):
        T_f[:r, i * r:(i + 1) * r] = Phi_i
    # sub-diagonal identity blocks
    if p > 1:
        for j in range(1, p):
            T_f[j * r:(j + 1) * r, (j - 1) * r:j * r] = np.eye(r)

    Q_f = np.zeros((r * p, r * p))
    Q_f[:r, :r] = Q_u
    return T_f, Q_f


def _kalman_filter_smoother(
    Y: np.ndarray,
    T_mat: np.ndarray,
    Q_mat: np.ndarray,
    C_mat: np.ndarray,
    R_meas: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, float]:
    """
    Generic Kalman filter + RTS smoother for state-space:

        alpha_t = T alpha_{t-1} + u_t,  u_t ~ N(0, Q)
        y_t     = C alpha_t     + eps_t, eps_t ~ N(0, R)

    Y:     (T, k) measurement array, with NaNs allowed.
    T_mat: (m, m)
    Q_mat: (m, m)
    C_mat: (k, m)
    R_meas:(k, k)
    a0:    (m,)
    P0:    (m, m)

    Returns:
        alpha_smooth: (T, m)
        P_smooth:     (T, m, m)
        P_lag:        (T, m, m) covariances Cov(alpha_t, alpha_{t-1} | Y)
        loglik:       scalar
    """
    Tn, k = Y.shape
    m = a0.shape[0]

    alpha_pred = np.zeros((Tn, m))
    P_pred = np.zeros((Tn, m, m))
    alpha_filt = np.zeros((Tn, m))
    P_filt = np.zeros((Tn, m, m))

    a_prev = a0.copy()
    P_prev = P0.copy()
    I_m = np.eye(m)
    loglik = 0.0

    for t in range(Tn):
        # prediction
        a_pred = T_mat @ a_prev
        P_pred_t = T_mat @ P_prev @ T_mat.T + Q_mat
        P_pred_t = 0.5 * (P_pred_t + P_pred_t.T)

        y_t = Y[t]
        mask = ~np.isnan(y_t)
        if not np.any(mask):
            # no observation
            alpha_pred[t] = a_pred
            P_pred[t] = P_pred_t
            alpha_filt[t] = a_pred
            P_filt[t] = P_pred_t
            a_prev, P_prev = a_pred, P_pred_t
            continue

        C_t = C_mat[mask]              # (k_t, m)
        R_t = R_meas[np.ix_(mask, mask)]
        y_obs = y_t[mask]

        v_t = y_obs - C_t @ a_pred
        S_t = C_t @ P_pred_t @ C_t.T + R_t
        S_t = 0.5 * (S_t + S_t.T)

        try:
            L = np.linalg.cholesky(S_t)
        except LinAlgError:
            S_t = S_t + 1e-6 * np.eye(S_t.shape[0])
            L = np.linalg.cholesky(S_t)

        # S^{-1} v via triangular solves
        z = np.linalg.solve(L, v_t)
        S_inv_v = np.linalg.solve(L.T, z)

        # loglik increment
        logdet_S = 2.0 * np.sum(np.log(np.diag(L)))
        k_t = y_obs.shape[0]
        ll_inc = -0.5 * (logdet_S + v_t @ S_inv_v + k_t * np.log(2.0 * np.pi))
        loglik += ll_inc

        # compute K_t using explicit inverse (can be optimized further)
        S_inv = np.linalg.inv(S_t)
        K_t = P_pred_t @ C_t.T @ S_inv

        a_filt = a_pred + K_t @ v_t
        P_filt_t = (I_m - K_t @ C_t) @ P_pred_t
        P_filt_t = 0.5 * (P_filt_t + P_filt_t.T)

        alpha_pred[t] = a_pred
        P_pred[t] = P_pred_t
        alpha_filt[t] = a_filt
        P_filt[t] = P_filt_t

        a_prev, P_prev = a_filt, P_filt_t

    # RTS smoother
    alpha_smooth = np.zeros_like(alpha_filt)
    P_smooth = np.zeros_like(P_filt)
    P_lag = np.zeros_like(P_filt)

    alpha_smooth[-1] = alpha_filt[-1]
    P_smooth[-1] = P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_filt_t = P_filt[t]
        P_pred_next = P_pred[t + 1]
        # smoothing gain
        try:
            P_pred_inv = np.linalg.inv(P_pred_next)
        except LinAlgError:
            P_pred_next = P_pred_next + 1e-8 * I_m
            P_pred_inv = np.linalg.inv(P_pred_next)
        J_t = P_filt_t @ T_mat.T @ P_pred_inv

        alpha_smooth[t] = alpha_filt[t] + J_t @ (alpha_smooth[t + 1] - alpha_pred[t + 1])
        P_smooth[t] = P_filt_t + J_t @ (P_smooth[t + 1] - P_pred_next) @ J_t.T
        P_smooth[t] = 0.5 * (P_smooth[t] + P_smooth[t].T)
        P_lag[t + 1] = J_t @ P_smooth[t + 1]

    P_lag[0] = np.zeros_like(P_lag[0])

    return alpha_smooth, P_smooth, P_lag, loglik


def mariano_murasawa_from_monthly(y_m: np.ndarray) -> np.ndarray:
    """
    Given monthly GDP growth y_m[t], compute Mariano–Murasawa quarterly approximation y_q_hat[t].

    y_q_hat[t] is defined for t >= 4 as:
      (1/3)*y_t + (2/3)*y_{t-1} + 1*y_{t-2} + (2/3)*y_{t-3} + (1/3)*y_{t-4}.
    """
    Tn = len(y_m)
    y_q_hat = np.full(Tn, np.nan, dtype=float)
    if Tn < 5:
        return y_q_hat

    for t in range(4, Tn):
        y_q_hat[t] = (
            (1.0 / 3.0) * y_m[t]
            + (2.0 / 3.0) * y_m[t - 1]
            + 1.0        * y_m[t - 2]
            + (2.0 / 3.0) * y_m[t - 3]
            + (1.0 / 3.0) * y_m[t - 4]
        )
    return y_q_hat


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
) -> MixedFreqDFMParams:
    """
    EM for mixed-frequency DFM with monthly indicators X and quarterly GDP y_q.

    X:   (T, n) standardized monthly indicators, NaNs allowed.
    y_q: (T,)   standardized quarterly GDP on the same monthly index,
                NaN except at quarter-end months (Mar/Jun/Sep/Dec).

    r:   number of factors
    p:   VAR order for factors

    Returns:
        MixedFreqDFMParams with smoothed factors, monthly GDP, and parameters.
    """
    X = np.asarray(X, float)
    y_q = np.asarray(y_q, float)
    Tn, n = X.shape

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    # 1) PCA for factor loadings + factor paths
    Lambda_x, F_init = _init_factors_pca(X, r)

    # 2) Idios residuals and AR(1) init
    # residuals: X - Lambda_x f_t
    F_design = np.nan_to_num(F_init)
    X_hat = F_design @ Lambda_x.T
    E_init = X - X_hat

    Phi_eps_diag = np.zeros(n)
    R_e_diag = np.zeros(n)
    for i in range(n):
        e_i = E_init[:, i]
        mask = ~np.isnan(e_i)
        e_i = e_i[mask]
        if e_i.size < 3:
            Phi_eps_diag[i] = 0.0
            R_e_diag[i] = np.nanvar(e_i) if e_i.size > 1 else 1.0
            continue
        e_lag = e_i[:-1]
        e_curr = e_i[1:]
        phi_i_num = np.dot(e_lag, e_curr)
        phi_i_den = np.dot(e_lag, e_lag) + 1e-8
        phi_i = phi_i_num / phi_i_den
        Phi_eps_diag[i] = np.clip(phi_i, -0.99, 0.99)
        resid = e_curr - phi_i * e_lag
        R_e_diag[i] = np.var(resid, ddof=1)

    # 3) Factor VAR(p) init (simple VAR)
    Phi_factors: List[np.ndarray] = []
    Q_u = np.eye(r)
    if p > 0:
        F = F_design
        T_eff = Tn - p
        Y_var = F[p:]
        X_var = np.column_stack([F[p - j - 1:Tn - j - 1] for j in range(p)])  # (T_eff, r*p)
        # OLS: Y_var = X_var B + U
        XtX = X_var.T @ X_var + 1e-8 * np.eye(r * p)
        XtY = X_var.T @ Y_var
        B = np.linalg.solve(XtX, XtY)  # (r*p, r)
        for j in range(p):
            Phi_factors.append(B[j * r:(j + 1) * r].T)
        U = Y_var - X_var @ B
        Q_u = (U.T @ U) / T_eff
    else:
        Phi_factors = []
        Q_u = np.cov(F_design.T)

    # 4) Monthly GDP init: simple regression of y_q (where observed) on first factor,
    #    then back-cast monthly GDP as y_m ~ first factor.
    y_m_init = np.zeros(Tn)
    gamma = np.zeros(r)
    phi_y = 0.0
    sigma_eta2 = 1.0
    sigma_v2 = 1.0

    # we use y_m_init ~ first factor as a proxy
    f1 = F_design[:, 0]
    mask_q = ~np.isnan(y_q)
    if np.any(mask_q):
        # approximate quarterly back-cast: y_m_init proportional to f1
        # just normalize f1 to have similar scale
        f1_std = (f1 - f1.mean()) / (f1.std(ddof=1) + 1e-8)
        y_m_init = f1_std
        # regression y_m_init ~ factors
        F_reg = F_design
        XtX = F_reg.T @ F_reg + 1e-8 * np.eye(r)
        XtY = F_reg.T @ y_m_init
        gamma = np.linalg.solve(XtX, XtY)
        # AR(1) for y_m
        y_lag = y_m_init[:-1]
        y_curr = y_m_init[1:]
        phi_y_num = np.dot(y_lag, y_curr)
        phi_y_den = np.dot(y_lag, y_lag) + 1e-8
        phi_y = np.clip(phi_y_num / phi_y_den, -0.99, 0.99)
        resid_y = y_curr - phi_y * y_lag
        sigma_eta2 = np.var(resid_y, ddof=1)
        # rough sigma_v2 init: variance of quarterly residuals (MM of y_m_init)
        y_q_hat0 = mariano_murasawa_from_monthly(y_m_init)
        mask_q2 = mask_q & ~np.isnan(y_q_hat0)
        if np.any(mask_q2):
            err_q0 = y_q_hat0[mask_q2] - y_q[mask_q2]
            sigma_v2 = np.var(err_q0, ddof=1)
        else:
            sigma_v2 = 1.0
    else:
        gamma = np.zeros(r)
        phi_y = 0.0
        sigma_eta2 = 1.0
        sigma_v2 = 1.0
        y_m_init[:] = 0.0

    # state dimension m = r*p_factors + n + 5 GDP lags
    if p > 0:
        T_f, Q_f = _build_factor_companion(Phi_factors, Q_u)
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

    # initial state mean and covariance
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
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        # factor block
        if p > 0:
            T_f_loc, Q_f_loc = _build_factor_companion(Phi_factors_loc, Q_u_loc)
        else:
            T_f_loc = np.zeros((r, r))
            Q_f_loc = Q_u_loc.copy()
        m_f_loc = T_f_loc.shape[0]
        m_loc = m_f_loc + n + 5

        T_mat = np.zeros((m_loc, m_loc))
        Q_mat = np.zeros((m_loc, m_loc))

        idx_eps = m_f_loc
        idx_y0_loc = m_f_loc + n
        idx_y1_loc = idx_y0_loc + 1
        idx_y2_loc = idx_y0_loc + 2
        idx_y3_loc = idx_y0_loc + 3
        idx_y4_loc = idx_y0_loc + 4

        # factors
        T_mat[:m_f_loc, :m_f_loc] = T_f_loc
        Q_mat[:m_f_loc, :m_f_loc] = Q_f_loc

        # idios AR(1)
        T_mat[idx_eps:idx_eps + n, idx_eps:idx_eps + n] = np.diag(Phi_eps_diag_loc)
        Q_mat[idx_eps:idx_eps + n, idx_eps:idx_eps + n] = np.diag(R_e_diag_loc)

        # monthly GDP
        T_mat[idx_y0_loc, :r] = gamma_loc
        T_mat[idx_y0_loc, idx_y1_loc] = phi_y_loc
        Q_mat[idx_y0_loc, idx_y0_loc] = sigma_eta2_loc

        # lags
        T_mat[idx_y1_loc, idx_y0_loc] = 1.0
        T_mat[idx_y2_loc, idx_y1_loc] = 1.0
        T_mat[idx_y3_loc, idx_y2_loc] = 1.0
        T_mat[idx_y4_loc, idx_y3_loc] = 1.0

        # measurement matrix
        k_loc = n + 1
        C_mat = np.zeros((k_loc, m_loc))
        # X block
        C_mat[:n, :r] = Lambda_x_loc
        C_mat[:n, idx_eps:idx_eps + n] = np.eye(n)
        # quarterly GDP (MM weights)
        C_mat[n, idx_y0_loc] = 1.0 / 3.0
        C_mat[n, idx_y1_loc] = 2.0 / 3.0
        C_mat[n, idx_y2_loc] = 1.0
        C_mat[n, idx_y3_loc] = 2.0 / 3.0
        C_mat[n, idx_y4_loc] = 1.0 / 3.0

        R_meas_loc = np.zeros((k_loc, k_loc))
        R_meas_loc[:n, :n] = 1e-6 * np.eye(n)
        R_meas_loc[n, n] = sigma_v2_loc

        return T_mat, Q_mat, C_mat, R_meas_loc

    # observation array Y: [X_t, y_q(t)]
    Y = np.column_stack([X, y_q])

    loglik_history: List[float] = []
    prev_loglik = -np.inf

    it_range = _trange(max_iter) if use_tqdm else range(max_iter)

    for it in it_range:
        # ------------------------------------------------------------------
        # E-step: build state matrices, run smoother
        # ------------------------------------------------------------------
        T_mat, Q_mat, C_mat, R_meas = build_state_matrices(
            Lambda_x, Phi_factors, Q_u, Phi_eps_diag, R_e_diag,
            gamma, phi_y, sigma_eta2, sigma_v2
        )

        alpha_smooth, P_smooth, P_lag, loglik = _kalman_filter_smoother(
            Y=Y,
            T_mat=T_mat,
            Q_mat=Q_mat,
            C_mat=C_mat,
            R_meas=R_meas,
            a0=a0,
            P0=P0,
        )
        loglik_history.append(loglik)

        if verbose:
            print(f"EM iter {it:3d}: loglik = {loglik:.6f}")

        # simple convergence check
        if it > 0:
            if abs((loglik - prev_loglik) / (abs(prev_loglik) + 1e-8)) < tol:
                if verbose:
                    print("Converged by relative loglik tolerance.")
                break
        prev_loglik = loglik

        # extract smoothed components
        # factors are the first r entries of the state
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
            X_var = np.column_stack([F[p - j - 1:Tn - j - 1] for j in range(p)])
            XtX = X_var.T @ X_var + 1e-8 * np.eye(r * p)
            XtY = X_var.T @ Y_var
            B = np.linalg.solve(XtX, XtY)  # (r*p, r)
            Phi_factors = []
            for j in range(p):
                Phi_factors.append(B[j * r:(j + 1) * r].T)
            U = Y_var - X_var @ B
            Q_u = (U.T @ U) / T_eff
        else:
            Phi_factors = []
            Q_u = np.cov(factors_smooth.T) + 1e-8 * np.eye(r)

        # (2) Loadings Lambda_x via regression X ~ factors
        F_design = factors_smooth
        XtX = (F_design.T @ F_design) + 1e-8 * np.eye(r)
        XtY = F_design.T @ np.nan_to_num(X)
        Lambda_x = (np.linalg.solve(XtX, XtY)).T  # (n, r)

        # residuals for idios
        X_hat = F_design @ Lambda_x.T
        E_res = X - X_hat

        # (3) Idios AR(1)
        for i in range(n):
            e_i = E_res[:, i]
            mask_i = ~np.isnan(e_i)
            e_i = e_i[mask_i]
            if e_i.size < 3:
                continue
            e_lag = e_i[:-1]
            e_curr = e_i[1:]
            phi_num = np.dot(e_lag, e_curr)
            phi_den = np.dot(e_lag, e_lag) + 1e-8
            phi_i = np.clip(phi_num / phi_den, -0.99, 0.99)
            Phi_eps_diag[i] = phi_i
            resid = e_curr - phi_i * e_lag
            R_e_diag[i] = np.var(resid, ddof=1)

        # (4) Monthly GDP regression y_m ~ factors + lagged y_m
        y_m = monthly_gdp_smooth
        y_lag = y_m[:-1]
        F_lag = F_design[:-1]
        y_curr = y_m[1:]
        Z = np.column_stack([F_lag, y_lag])
        XtX = Z.T @ Z + 1e-8 * np.eye(r + 1)
        XtY = Z.T @ y_curr
        beta = np.linalg.solve(XtX, XtY)
        gamma = beta[:r]
        phi_y = float(np.clip(beta[-1], -0.99, 0.99))
        resid_y = y_curr - Z @ beta
        sigma_eta2 = float(np.var(resid_y, ddof=1))

        # (5) Quarterly measurement variance sigma_v2
        y_q_hat_mm = mariano_murasawa_from_monthly(y_m)
        mask_q_mm = ~np.isnan(y_q) & ~np.isnan(y_q_hat_mm)
        if np.any(mask_q_mm):
            err_q = y_q_hat_mm[mask_q_mm] - y_q[mask_q_mm]
            sigma_v2 = float(np.var(err_q, ddof=1))
        else:
            sigma_v2 = float(sigma_v2)

    # after EM, run final smoother for output
    T_mat, Q_mat, C_mat, R_meas = build_state_matrices(
        Lambda_x, Phi_factors, Q_u, Phi_eps_diag, R_e_diag,
        gamma, phi_y, sigma_eta2, sigma_v2
    )
    alpha_smooth, P_smooth, P_lag, loglik_final = _kalman_filter_smoother(
        Y=Y,
        T_mat=T_mat,
        Q_mat=Q_mat,
        C_mat=C_mat,
        R_meas=R_meas,
        a0=a0,
        P0=P0,
    )
    loglik_history.append(loglik_final)
    factors_smooth = alpha_smooth[:, :r]
    monthly_gdp_smooth = alpha_smooth[:, idx_y0]

    return MixedFreqDFMParams(
        Lambda_x=Lambda_x,
        Phi_factors=Phi_factors,
        Q_u=Q_u,
        Phi_eps_diag=Phi_eps_diag,
        R_e_diag=R_e_diag,
        gamma=gamma,
        phi_y=phi_y,
        sigma_eta2=sigma_eta2,
        sigma_v2=sigma_v2,
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
    """
    Build (T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0) from trained parameters.

    Structure matches the EM state-space:

      alpha_t = [f_t (companion), eps_t, y_t^m, y_{t-1}^m, ..., y_{t-4}^m]'.
    """
    Lambda_x = params.Lambda_x
    Phi_list = params.Phi_factors
    Q_u = params.Q_u
    Phi_eps_diag = params.Phi_eps_diag
    sigma_e2 = params.R_e_diag
    gamma = params.gamma
    phi_y = params.phi_y
    sigma_eta2 = params.sigma_eta2
    sigma_v2 = params.sigma_v2

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
    if Phi_eps_diag is not None and len(Phi_eps_diag) == n:
        T_mat[idx_eps_start:idx_eps_start + n, idx_eps_start:idx_eps_start + n] = np.diag(
            Phi_eps_diag
        )
    if sigma_e2 is not None and len(sigma_e2) == n:
        Q_mat[idx_eps_start:idx_eps_start + n, idx_eps_start:idx_eps_start + n] = np.diag(
            sigma_e2
        )

    # monthly GDP
    T_mat[idx_y0, :r] = gamma
    T_mat[idx_y0, idx_y1] = phi_y
    Q_mat[idx_y0, idx_y0] = sigma_eta2

    # GDP monthly lags
    T_mat[idx_y1, idx_y0] = 1.0
    T_mat[idx_y2, idx_y1] = 1.0
    T_mat[idx_y3, idx_y2] = 1.0
    T_mat[idx_y4, idx_y3] = 1.0

    # measurement matrix
    k = n + 1
    C_mat = np.zeros((k, m))
    C_mat[:n, :r] = Lambda_x
    C_mat[:n, idx_eps_start:idx_eps_start + n] = np.eye(n)

    # Mariano–Murasawa weights for quarterly GDP
    C_mat[n, idx_y0] = 1.0 / 3.0
    C_mat[n, idx_y1] = 2.0 / 3.0
    C_mat[n, idx_y2] = 1.0
    C_mat[n, idx_y3] = 2.0 / 3.0
    C_mat[n, idx_y4] = 1.0 / 3.0

    R_meas = np.zeros((k, k))
    R_meas[:n, :n] = 1e-6 * np.eye(n)
    R_meas[n, n] = sigma_v2

    a0 = np.zeros(m)
    P0 = np.eye(m) * 1e4

    return T_mat, Q_mat, C_mat, R_meas, a0, P0, idx_y0


def kalman_filter_only(
    Y: np.ndarray,
    T_mat: np.ndarray,
    Q_mat: np.ndarray,
    C_mat: np.ndarray,
    R_meas: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Forward Kalman filter only (no RTS smoother), for one-sided nowcasts.

    Y: (T, k) measurements [X_t, y_q(t)] with NaNs.
    Returns:
        alpha_filt: (T, m)
        P_filt:     (T, m, m)
    """
    Tn, k = Y.shape
    m = a0.shape[0]

    alpha_filt = np.zeros((Tn, m))
    P_filt = np.zeros((Tn, m, m))

    a_prev = a0.copy()
    P_prev = P0.copy()
    I_m = np.eye(m)

    for t in range(Tn):
        # prediction
        a_pred = T_mat @ a_prev
        P_pred_t = T_mat @ P_prev @ T_mat.T + Q_mat
        P_pred_t = 0.5 * (P_pred_t + P_pred_t.T)

        y_t = Y[t]
        mask = ~np.isnan(y_t)

        if not np.any(mask):
            alpha_filt[t] = a_pred
            P_filt[t] = P_pred_t
            a_prev, P_prev = a_pred, P_pred_t
            continue

        C_t = C_mat[mask]
        R_t = R_meas[np.ix_(mask, mask)]
        y_obs = y_t[mask]

        v_t = y_obs - C_t @ a_pred
        S_t = C_t @ P_pred_t @ C_t.T + R_t
        S_t = 0.5 * (S_t + S_t.T)

        try:
            L = np.linalg.cholesky(S_t)
        except LinAlgError:
            S_t = S_t + 1e-6 * np.eye(S_t.shape[0])
            L = np.linalg.cholesky(S_t)

        # S^{-1} v
        z = np.linalg.solve(L, v_t)
        S_inv_v = np.linalg.solve(L.T, z)

        # explicit inverse for K_t (can be optimized)
        S_inv = np.linalg.inv(S_t)
        K_t = P_pred_t @ C_t.T @ S_inv

        a_filt = a_pred + K_t @ v_t
        P_filt_t = (I_m - K_t @ C_t) @ P_pred_t
        P_filt_t = 0.5 * (P_filt_t + P_filt_t.T)

        alpha_filt[t] = a_filt
        P_filt[t] = P_filt_t
        a_prev, P_prev = a_filt, P_filt_t

    return alpha_filt, P_filt
