# src/dfm_pipeline/dfm_dyn/em_dfm.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import numpy as np

from .state_space import (
    StateSpaceParams,
    build_dfm_state_space,
    kalman_filter_smoother,
)


@dataclass
class DynDFMParams:
    Lambda: np.ndarray        # (n, r)
    Phi_list: List[np.ndarray]  # length p, each (r,r)
    Q_u: np.ndarray           # (r, r)
    R: np.ndarray             # (n, n)
    a0: np.ndarray            # (rp,)
    P0: np.ndarray            # (rp, rp)
    r: int
    p: int


def _init_params(
    X: np.ndarray,
    r: int,
    p: int,
) -> DynDFMParams:
    """
    Crude initialization: static PCA for Lambda & factors, AR(1) for factors,
    diagonal idiosyncratic variance, identity VAR(p) companion as placeholder.
    """
    T, n = X.shape
    # simple mean-imputed
    X_filled = np.where(np.isnan(X), np.nanmean(X, axis=0, keepdims=True), X)

    # PCA via SVD
    U, S, Vt = np.linalg.svd(X_filled - X_filled.mean(0), full_matrices=False)
    F_init = U[:, :r] * S[:r]      # T x r
    Lambda_init = Vt[:r, :].T      # n x r

    # AR(1) on factors
    Phi1 = np.zeros((r, r))
    Q_u = np.eye(r)
    for j in range(r):
        f_j = F_init[:, j]
        f_tm1 = f_j[:-1]
        f_t   = f_j[1:]
        beta, *_ = np.linalg.lstsq(f_tm1[:, None], f_t, rcond=None)
        Phi1[j, j] = beta[0]
        resid = f_t - beta[0] * f_tm1
        Q_u[j, j] = np.var(resid)

    Phi_list = [Phi1] + [np.zeros((r, r)) for _ in range(p-1)]

    # idiosyncratic variance
    resid = X_filled - F_init @ Lambda_init.T
    R_diag = np.var(resid, axis=0)
    R_init = np.diag(R_diag + 1e-6)

    rp = r * p
    a0 = np.zeros(rp)
    P0 = np.eye(rp)

    return DynDFMParams(
        Lambda=Lambda_init,
        Phi_list=Phi_list,
        Q_u=Q_u,
        R=R_init,
        a0=a0,
        P0=P0,
        r=r,
        p=p,
    )


def em_dfm_full(
    X: np.ndarray,
    r: int,
    p: int,
    max_iter: int = 50,
    tol: float = 1e-4,
    verbose: bool = False,
) -> DynDFMParams:
    """
    EM estimation of a VAR(p) dynamic factor model in companion form.

    X : (T, n) panel (NaNs allowed)
    r : number of factors
    p : VAR order
    """
    T, n = X.shape

    # Simple missing mask
    mask = ~np.isnan(X)

    params = _init_params(X, r=r, p=p)
    prev_ll = -np.inf

    for it in range(max_iter):
        # E-step: build state space, run KF+KS
        ss = build_dfm_state_space(
            Lambda=params.Lambda,
            Phi_list=params.Phi_list,
            Q_u=params.Q_u,
            R=params.R,
            a0=params.a0,
            P0=params.P0,
        )
        ks = kalman_filter_smoother(Y=X, ss=ss, mask=~mask)

        # log-likelihood if you implement it; otherwise skip or approximate
        # ll = ...
        # if verbose:
        #     print(f"EM iter {it}: ll={ll:.3f}")
        # if abs(ll - prev_ll) < tol * (1 + abs(prev_ll)):
        #     break
        # prev_ll = ll

        # M-step:

        # 1) Update T, Q via unconstrained AR(1) on alpha and projection to VAR(p)
        rp = params.r * params.p
        S00 = np.zeros((rp, rp))
        S11 = np.zeros((rp, rp))
        S10 = np.zeros((rp, rp))
        for t in range(1, T):
            a_t = ks.a_smooth[t]
            a_tm1 = ks.a_smooth[t-1]
            P_t = ks.P_smooth[t]
            P_tm1 = ks.P_smooth[t-1]
            P_t_tm1 = ks.P_lag_smooth[t-1]

            S10 += P_t_tm1 + np.outer(a_t, a_tm1)
            S00 += P_tm1 + np.outer(a_tm1, a_tm1)
            S11 += P_t + np.outer(a_t, a_t)

        T_un = S10 @ np.linalg.inv(S00)
        Q_un = (S11 - T_un @ S10.T) / (T - 1)

        # project to VAR(p) companion
        r_ = params.r
        p_ = params.p
        top = T_un[0:r_, :]
        Phi_list_new: list[np.ndarray] = []
        for j in range(p_):
            Phi_j = top[:, j*r_:(j+1)*r_]
            Phi_list_new.append(Phi_j)

        Q_u_new = Q_un[0:r_, 0:r_]
        params.Phi_list = Phi_list_new
        params.Q_u = Q_u_new

        # 2) Update Lambda, R via quasi-ML regression of X on smoothed factors
        f_sm = ks.a_smooth[:, 0:r_]   # T x r
        Lambda_new = np.zeros_like(params.Lambda)
        R_diag_new = np.zeros(n)

        for i in range(n):
            x_i = X[:, i]
            m_i = ~np.isnan(x_i)
            if m_i.sum() < r_ + 1:
                continue
            F_i = f_sm[m_i, :]
            y_i = x_i[m_i]
            beta_i, *_ = np.linalg.lstsq(F_i, y_i, rcond=None)
            Lambda_new[i, :] = beta_i
            resid = y_i - F_i @ beta_i
            R_diag_new[i] = np.mean(resid**2)

        params.Lambda = Lambda_new
        params.R = np.diag(R_diag_new + 1e-6)

        # 3) Optional: update a0, P0 from first smoothed state
        params.a0 = ks.a_smooth[0].copy()
        params.P0 = ks.P_smooth[0].copy()

        if verbose:
            print(f"EM iter {it+1} completed")

    return params
