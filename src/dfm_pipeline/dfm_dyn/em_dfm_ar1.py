"""
EM-based Dynamic Factor Model with AR(1) idiosyncratic components
in the spirit of Bańbura & Modugno (2010) and Linzenich & Meunier.

State:
    alpha_t = [f_t', f_{t-1}', ..., f_{t-p+1}', eps_t']'  (dim = r*p + n)

Transition:
    [f_t      ]   [Phi_1 ... Phi_p      0  ] [f_{t-1}    ]   [u_t     ]
    [f_{t-1}  ] = [I_r   ... 0          0  ] [f_{t-2}    ] + [0       ]
    [  ...    ]   [ ...                ... ] [  ...      ]   [ ...    ]
    [f_{t-p+1}]   [0     ... I_r        0  ] [f_{t-p}    ]   [0       ]
    [eps_t    ]   [0     ... 0         Phi ] [eps_{t-1}  ]   [e_t     ]

with u_t ~ N(0, Q_u), e_t ~ N(0, R_e), Phi=diag(phi_i), R_e=diag(sigma_ei^2).

Measurement:
    x_t = Lambda f_t + eps_t   (we treat measurement noise as negligible)

We estimate:
    - Lambda (n x r)
    - factor VAR(p): Phi_list (length p, r x r each) and Q_u (r x r)
    - idiosyncratic AR(1): Phi_eps_diag (n,), R_e_diag (n,)

via an EM algorithm using Kalman filter / smoother on the augmented state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np

try:
    from tqdm import trange
except Exception:  # pragma: no cover
    trange = range  # fallback


@dataclass
class DynDFMParamsAR1:
    factors_smooth: np.ndarray         # (T, r)
    states_smooth: np.ndarray          # (T, r*p + n)
    Lambda: np.ndarray                 # (n, r)
    Phi_factors: List[np.ndarray]      # list of length p, each (r, r)
    Q_u: np.ndarray                    # (r, r)
    Phi_eps_diag: np.ndarray           # (n,)
    R_e_diag: np.ndarray               # (n,)
    loglik_history: List[float]


# ---------------------------------------------------------------------------
# Kalman filter / smoother for general linear Gaussian state-space
# ---------------------------------------------------------------------------


def _kalman_filter_smoother(
    Y: np.ndarray,
    T: np.ndarray,
    Q: np.ndarray,
    C: np.ndarray,
    R_meas: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, float]:
    """
    Kalman filter + RTS smoother with missing data by row.

    Parameters
    ----------
    Y : (T, n)
        Observations (NaN allowed).
    T : (m, m)
        Transition matrix.
    Q : (m, m)
        Process noise covariance.
    C : (n, m)
        Measurement matrix.
    R_meas : (n, n)
        Measurement noise covariance (here typically tiny diagonal).
    a0 : (m,)
        Initial state mean.
    P0 : (m, m)
        Initial state covariance.

    Returns
    -------
    alpha_smooth : (T, m)
        Smoothed state means.
    P_smooth : (T, m, m)
        Smoothed state covariances.
    loglik : float
        Log-likelihood of the data given parameters.
    """
    Tn, n = Y.shape
    m = a0.shape[0]

    alpha_pred = np.zeros((Tn, m))
    P_pred = np.zeros((Tn, m, m))
    alpha_filt = np.zeros((Tn, m))
    P_filt = np.zeros((Tn, m, m))

    a_prev = a0.copy()
    P_prev = P0.copy()

    loglik = 0.0
    I_m = np.eye(m)

    for t in range(Tn):
        # Prediction
        a_pred = T @ a_prev
        P_pred_t = T @ P_prev @ T.T + Q
        # symmetrize
        P_pred_t = 0.5 * (P_pred_t + P_pred_t.T)

        y_t = Y[t]
        mask = ~np.isnan(y_t)
        if not np.any(mask):
            # no observation: no update
            alpha_pred[t] = a_pred
            P_pred[t] = P_pred_t
            alpha_filt[t] = a_pred
            P_filt[t] = P_pred_t
            a_prev = a_pred
            P_prev = P_pred_t
            continue

        C_t = C[mask]                      # (k, m)
        R_t = R_meas[np.ix_(mask, mask)]   # (k, k)
        y_obs = y_t[mask]                  # (k,)

        # Innovation
        v_t = y_obs - C_t @ a_pred         # (k,)
        S_t = C_t @ P_pred_t @ C_t.T + R_t # (k, k)
        S_t = 0.5 * (S_t + S_t.T)

        # Regularize S_t if needed
        try:
            # Cholesky for logdet and solve
            L = np.linalg.cholesky(S_t)
            # log det
            logdet_S = 2.0 * np.sum(np.log(np.diag(L)))
            # solve S_t^{-1} v_t via cho_solve type
            z = np.linalg.solve(L, v_t)
            S_inv_v = np.linalg.solve(L.T, z)
        except np.linalg.LinAlgError:
            # fallback: add jitter
            jitter = 1e-6 * np.eye(S_t.shape[0])
            S_t_j = S_t + jitter
            L = np.linalg.cholesky(S_t_j)
            logdet_S = 2.0 * np.sum(np.log(np.diag(L)))
            z = np.linalg.solve(L, v_t)
            S_inv_v = np.linalg.solve(L.T, z)

        # Kalman gain
        K_t = P_pred_t @ C_t.T
        # S_inv_v is (k,), we want (k, 1) for consistency
        S_inv = np.linalg.inv(S_t)  # small k; ok
        K_t = K_t @ S_inv           # (m, k)

        # Update
        a_filt = a_pred + K_t @ v_t
        P_filt_t = (I_m - K_t @ C_t) @ P_pred_t
        P_filt_t = 0.5 * (P_filt_t + P_filt_t.T)

        # Store
        alpha_pred[t] = a_pred
        P_pred[t] = P_pred_t
        alpha_filt[t] = a_filt
        P_filt[t] = P_filt_t

        # Update for next step
        a_prev = a_filt
        P_prev = P_filt_t

        # log-likelihood contribution
        ll_t = -0.5 * (logdet_S + v_t @ S_inv_v + len(y_obs) * np.log(2.0 * np.pi))
        loglik += ll_t

    # RTS smoother
    alpha_smooth = np.zeros_like(alpha_filt)
    P_smooth = np.zeros_like(P_filt)
    alpha_smooth[-1] = alpha_filt[-1]
    P_smooth[-1] = P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_f = P_filt[t]
        P_p_next = P_pred[t + 1]
        P_p_next = 0.5 * (P_p_next + P_p_next.T)
        # smoother gain
        try:
            J_t = P_f @ T.T @ np.linalg.inv(P_p_next)
        except np.linalg.LinAlgError:
            J_t = P_f @ T.T @ np.linalg.inv(P_p_next + 1e-6 * np.eye(m))

        alpha_smooth[t] = alpha_filt[t] + J_t @ (alpha_smooth[t + 1] - alpha_pred[t + 1])
        P_smooth[t] = P_f + J_t @ (P_smooth[t + 1] - P_p_next) @ J_t.T
        P_smooth[t] = 0.5 * (P_smooth[t] + P_smooth[t].T)

    return alpha_smooth, P_smooth, float(loglik)


# ---------------------------------------------------------------------------
# Helpers: build VAR(p) companion, init from PCA, etc.
# ---------------------------------------------------------------------------


def _build_factor_companion(
    Phi_list: List[np.ndarray],
    Q_u: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build VAR(p) companion transition and process covariance for factors.

    State_f = [f_t, f_{t-1}, ..., f_{t-p+1}]' of dimension r*p.
    """
    r = Phi_list[0].shape[0]
    p = len(Phi_list)
    m_f = r * p

    T_f = np.zeros((m_f, m_f))
    # top block row: [Phi_1 ... Phi_p]
    for j in range(p):
        T_f[:r, j * r:(j + 1) * r] = Phi_list[j]

    # subdiagonal identity blocks
    if p > 1:
        for j in range(1, p):
            T_f[j * r:(j + 1) * r, (j - 1) * r:j * r] = np.eye(r)

    # Process covariance: only the f_t block is shocked
    Q_f = np.zeros((m_f, m_f))
    Q_f[:r, :r] = Q_u

    return T_f, Q_f


def _init_from_pca_ar1(
    X: np.ndarray,
    r: int,
    p: int,
) -> Tuple[np.ndarray, List[np.ndarray], np.ndarray, np.ndarray, np.ndarray]:
    """
    PCA-based initialization with AR(1) idiosyncratics.

    Returns
    -------
    Lambda_init : (n, r)
    Phi_list_init : list of length p, each (r, r)
    Q_u_init : (r, r)
    Phi_eps_diag_init : (n,)
    R_e_diag_init : (n,)
    """
    Tn, n = X.shape

    # Fill missing with column means for PCA init only
    X_filled = X.copy()
    col_means = np.nanmean(X_filled, axis=0)
    inds = np.where(np.isnan(X_filled))
    X_filled[inds] = np.take(col_means, inds[1])

    # Static PCA via SVD
    U, s, Vt = np.linalg.svd(X_filled, full_matrices=False)
    F_init = U[:, :r] * s[:r]        # (T, r)
    Lambda_init = Vt[:r, :].T        # (n, r)

    # Factor VAR(p) init
    if p > 0:
        Z_rows = Tn - p
        if Z_rows <= 0:
            Phi_list_init = [np.eye(r) for _ in range(p)]
            Q_u_init = 0.1 * np.eye(r)
        else:
            Z = np.zeros((Z_rows, r * p))
            F_target = F_init[p:, :]  # (Z_rows, r)
            for k in range(Z_rows):
                row_elems = []
                for j in range(1, p + 1):
                    row_elems.append(F_init[p + k - j, :])
                Z[k, :] = np.concatenate(row_elems)
            B, _, _, _ = np.linalg.lstsq(Z, F_target, rcond=None)  # (r*p, r)
            Phi_list_init = []
            for j in range(p):
                Phi_j = B[j * r:(j + 1) * r, :].T  # (r, r)
                Phi_list_init.append(Phi_j)

            # Compute VAR residuals and Q_u
            U_eps = np.zeros((Z_rows, r))
            for k in range(Z_rows):
                f_hat = np.zeros(r)
                for j in range(1, p + 1):
                    Phi_j = Phi_list_init[j - 1]
                    f_hat += Phi_j @ F_init[p + k - j, :]
                U_eps[k, :] = F_target[k, :] - f_hat
            if Z_rows > r:
                Q_u_init = np.cov(U_eps.T)
            else:
                Q_u_init = 0.1 * np.eye(r)
    else:
        Phi_list_init = []
        Q_u_init = np.zeros((r, r))

    # Idiosyncratic residuals from static PCA reconstruction
    X_hat_init = F_init @ Lambda_init.T
    E_init = X_filled - X_hat_init  # (T, n)

    # AR(1) on E_init for each series
    Phi_eps_diag = np.zeros(n)
    sigma_e2 = np.zeros(n)

    for i in range(n):
        e_i = E_init[:, i]
        if np.all(np.isnan(e_i)):
            Phi_eps_diag[i] = 0.0
            sigma_e2[i] = 1e-6
            continue

        e_i = np.nan_to_num(e_i, nan=np.nanmean(e_i))
        if Tn <= 1:
            Phi_eps_diag[i] = 0.0
            sigma_e2[i] = max(np.var(e_i, ddof=1), 1e-6)
            continue

        e_i_t = e_i[1:]
        e_i_lag = e_i[:-1]
        denom = float(e_i_lag @ e_i_lag)
        if denom < 1e-8:
            Phi_eps_diag[i] = 0.0
            sigma_e2[i] = max(np.var(e_i, ddof=1), 1e-6)
            continue

        phi_i = float(e_i_lag @ e_i_t / denom)
        resid = e_i_t - phi_i * e_i_lag
        Phi_eps_diag[i] = np.clip(phi_i, -0.98, 0.98)
        sigma_e2[i] = max(float(np.mean(resid**2)), 1e-6)

    return Lambda_init, Phi_list_init, Q_u_init, Phi_eps_diag, sigma_e2


# ---------------------------------------------------------------------------
# EM algorithm with AR(1) idios
# ---------------------------------------------------------------------------


def em_dfm_full_ar1(
    X: np.ndarray,
    r: int,
    p: int,
    max_iter: int = 50,
    tol: float = 1e-4,
    verbose: bool = False,
    use_tqdm: bool = False,
) -> DynDFMParamsAR1:
    """
    EM-estimation of a dynamic factor model with AR(1) idiosyncratic components.

    Parameters
    ----------
    X : (T, n) np.ndarray
        Panel of observables (standardized; NaN allowed).
    r : int
        Number of dynamic factors.
    p : int
        VAR order for factors.
    max_iter : int
        Maximum number of EM iterations.
    tol : float
        Convergence tolerance on absolute log-likelihood change.
    verbose : bool
        If True, prints log-likelihood per iteration.
    use_tqdm : bool
        If True, wrap EM iterations in tqdm progress bar.

    Returns
    -------
    DynDFMParamsAR1
        Estimated parameters and smoothed factors/states.
    """
    X = np.asarray(X, float)
    Tn, n = X.shape
    assert r > 0
    assert p >= 0

    # Initialization from PCA + AR(1) idios
    Lambda, Phi_list, Q_u, Phi_eps_diag, sigma_e2 = _init_from_pca_ar1(X, r, p)

    # Build initial state-space
    if p > 0:
        T_f, Q_f = _build_factor_companion(Phi_list, Q_u)
    else:
        T_f = np.zeros((r, r))
        Q_f = Q_u.copy()
    m_f = T_f.shape[0]
    m = m_f + n

    # Transition and process noise
    T = np.zeros((m, m))
    Q = np.zeros((m, m))
    T[:m_f, :m_f] = T_f
    Q[:m_f, :m_f] = Q_f
    # AR(1) idios on eps_t
    T[m_f:, m_f:] = np.diag(Phi_eps_diag)
    Q[m_f:, m_f:] = np.diag(sigma_e2)

    # Measurement matrix: x_t = Lambda f_t + eps_t
    C = np.zeros((n, m))
    C[:, :r] = Lambda
    C[:, m_f:] = np.eye(n)

    # Small measurement noise (for numerical stability only)
    R_meas = 1e-6 * np.eye(n)

    # Initial state
    a0 = np.zeros(m)
    P0 = np.eye(m) * 1e4

    loglik_history: List[float] = []

    it_range = trange(max_iter) if use_tqdm else range(max_iter)

    for it in it_range:
        # E-step: Kalman filter + smoother
        alpha_smooth, P_smooth, loglik = _kalman_filter_smoother(
            X, T, Q, C, R_meas, a0, P0
        )
        loglik_history.append(loglik)

        if verbose:
            print(f"EM iter {it:3d}: loglik = {loglik:.6f}")

        # Convergence check
        if it > 0 and abs(loglik_history[-1] - loglik_history[-2]) < tol:
            if verbose:
                print("EM converged")
            break

        # M-step

        # 1) Update factor VAR from smoothed f_t (top r entries of state)
        F_hat = alpha_smooth[:, :r]  # (T, r)
        if p > 0:
            Z_rows = Tn - p
            if Z_rows <= 0:
                Phi_list_new = Phi_list
                Q_u_new = Q_u
            else:
                Z = np.zeros((Z_rows, r * p))
                F_target = F_hat[p:, :]
                for k in range(Z_rows):
                    row_elems = []
                    for j in range(1, p + 1):
                        row_elems.append(F_hat[p + k - j, :])
                    Z[k, :] = np.concatenate(row_elems)
                B, _, _, _ = np.linalg.lstsq(Z, F_target, rcond=None)  # (r*p, r)
                Phi_list_new: List[np.ndarray] = []
                for j in range(p):
                    Phi_j = B[j * r:(j + 1) * r, :].T
                    Phi_list_new.append(Phi_j)

                # residuals for Q_u
                U_eps = np.zeros((Z_rows, r))
                for k in range(Z_rows):
                    f_pred = np.zeros(r)
                    for j in range(1, p + 1):
                        Phi_j = Phi_list_new[j - 1]
                        f_pred += Phi_j @ F_hat[p + k - j, :]
                    U_eps[k, :] = F_target[k, :] - f_pred
                if Z_rows > r:
                    Q_u_new = np.cov(U_eps.T)
                else:
                    Q_u_new = Q_u
        else:
            Phi_list_new = []
            Q_u_new = Q_u

        # 2) Update Lambda by regressing X on F_hat (ignoring eps_hat in regression)
        Lambda_new = np.zeros_like(Lambda)
        for i in range(n):
            x_i = X[:, i]
            mask = ~np.isnan(x_i)
            if np.sum(mask) <= r:
                Lambda_new[i, :] = Lambda[i, :]
                continue
            F_i = F_hat[mask, :]
            x_i_obs = x_i[mask]
            beta_i, _, _, _ = np.linalg.lstsq(F_i, x_i_obs, rcond=None)
            Lambda_new[i, :] = beta_i

        # 3) Update AR(1) idios from smoothed eps_hat
        eps_hat = alpha_smooth[:, m_f:]  # (T, n)
        Phi_eps_diag_new = np.zeros_like(Phi_eps_diag)
        sigma_e2_new = np.zeros_like(sigma_e2)

        for i in range(n):
            eps_i = eps_hat[:, i]
            eps_i = np.nan_to_num(eps_i, nan=0.0)
            if Tn <= 1:
                Phi_eps_diag_new[i] = Phi_eps_diag[i]
                sigma_e2_new[i] = sigma_e2[i]
                continue
            eps_i_t = eps_i[1:]
            eps_i_lag = eps_i[:-1]
            denom = float(eps_i_lag @ eps_i_lag)
            if denom < 1e-8:
                Phi_eps_diag_new[i] = 0.0
                sigma_e2_new[i] = max(sigma_e2[i], 1e-6)
                continue
            phi_i = float(eps_i_lag @ eps_i_t / denom)
            resid = eps_i_t - phi_i * eps_i_lag
            Phi_eps_diag_new[i] = np.clip(phi_i, -0.98, 0.98)
            sigma_e2_new[i] = max(float(np.mean(resid**2)), 1e-6)

        # Assign updates
        Lambda = Lambda_new
        Phi_list = Phi_list_new
        Q_u = Q_u_new
        Phi_eps_diag = Phi_eps_diag_new
        sigma_e2 = sigma_e2_new

        # Rebuild state-space with updated parameters

        if p > 0:
            T_f, Q_f = _build_factor_companion(Phi_list, Q_u)
        else:
            T_f = np.zeros((r, r))
            Q_f = Q_u.copy()
        m_f = T_f.shape[0]
        m = m_f + n

        T = np.zeros((m, m))
        Q = np.zeros((m, m))
        T[:m_f, :m_f] = T_f
        Q[:m_f, :m_f] = Q_f
        T[m_f:, m_f:] = np.diag(Phi_eps_diag)
        Q[m_f:, m_f:] = np.diag(sigma_e2)

        C = np.zeros((n, m))
        C[:, :r] = Lambda
        C[:, m_f:] = np.eye(n)

        # keep R_meas as tiny diagonal
        a0 = alpha_smooth[0].copy()
        P0 = P_smooth[0].copy()

    # final E-step results are last alpha_smooth, P_smooth
    factors_smooth = alpha_smooth[:, :r]

    return DynDFMParamsAR1(
        factors_smooth=factors_smooth,
        states_smooth=alpha_smooth,
        Lambda=Lambda,
        Phi_factors=Phi_list,
        Q_u=Q_u,
        Phi_eps_diag=Phi_eps_diag,
        R_e_diag=sigma_e2,
        loglik_history=loglik_history,
    )
