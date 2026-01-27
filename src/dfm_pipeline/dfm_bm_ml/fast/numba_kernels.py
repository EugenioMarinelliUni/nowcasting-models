from __future__ import annotations

import numpy as np

try:
    from numba import njit  # type: ignore

    NUMBA_AVAILABLE = True
except Exception:
    NUMBA_AVAILABLE = False

    def njit(*args, **kwargs):
        def _wrap(fn):
            return fn

        return _wrap


@njit(cache=True)
def accumulate_Sxx_Syx(Ezz, P_lag, a, f0_idx, lag_stack_idx):
    """
    Computes:
      S_yx = sum_t cross[f0, lag_stack]
      S_xx = sum_t Ezz[t-1][lag_stack, lag_stack]
    where cross[t] = P_lag[t] + a[t] a[t-1]'.
    """
    T = a.shape[0]
    rb = f0_idx.shape[0]
    k = lag_stack_idx.shape[0]

    S_xx = np.zeros((k, k), dtype=np.float64)
    S_yx = np.zeros((rb, k), dtype=np.float64)

    for t in range(1, T):
        Ell = Ezz[t - 1]
        for ii in range(k):
            ii_idx = lag_stack_idx[ii]
            for jj in range(k):
                jj_idx = lag_stack_idx[jj]
                S_xx[ii, jj] += Ell[ii_idx, jj_idx]

        Pl = P_lag[t]
        at = a[t]
        at1 = a[t - 1]
        for i in range(rb):
            fi = f0_idx[i]
            for j in range(k):
                lj = lag_stack_idx[j]
                S_yx[i, j] += Pl[fi, lj] + at[fi] * at1[lj]

    return S_xx, S_yx


@njit(cache=True)
def accumulate_Q_acc(Ezz, P_lag, a, f0_idx, lag_stack_idx, Phi_stack):
    """
    Computes:
      Q_acc = sum_t Eff - Phi*Efl' - Efl*Phi' + Phi*Ell*Phi'
    """
    T = a.shape[0]
    rb = f0_idx.shape[0]
    k = lag_stack_idx.shape[0]

    Q_acc = np.zeros((rb, rb), dtype=np.float64)
    count = 0

    for t in range(1, T):
        Eff = Ezz[t]
        Ell = Ezz[t - 1]
        Pl = P_lag[t]
        at = a[t]
        at1 = a[t - 1]

        Efl = np.zeros((rb, k), dtype=np.float64)
        for i in range(rb):
            fi = f0_idx[i]
            for j in range(k):
                lj = lag_stack_idx[j]
                Efl[i, j] = Pl[fi, lj] + at[fi] * at1[lj]

        Eff_ff = np.zeros((rb, rb), dtype=np.float64)
        for i in range(rb):
            fi = f0_idx[i]
            for j in range(rb):
                fj = f0_idx[j]
                Eff_ff[i, j] = Eff[fi, fj]

        Ell_ll = np.zeros((k, k), dtype=np.float64)
        for i in range(k):
            li = lag_stack_idx[i]
            for j in range(k):
                lj = lag_stack_idx[j]
                Ell_ll[i, j] = Ell[li, lj]

        term1 = Phi_stack @ Efl.T
        term2 = Efl @ Phi_stack.T
        term3 = Phi_stack @ (Ell_ll @ Phi_stack.T)

        for i in range(rb):
            for j in range(rb):
                Q_acc[i, j] += Eff_ff[i, j] - term1[i, j] - term2[i, j] + term3[i, j]

        count += 1

    return Q_acc, count


@njit(cache=True)
def update_rho_sig2_from_diag(Ezz_diag, P_lag_diag, a_diag, min_var=1e-8):
    """
    For one scalar state component z_t:
      Ezz_diag[t] = E[z_t^2]
      cross[t]    = E[z_t z_{t-1}] = P_lag_diag[t] + a_diag[t]*a_diag[t-1]
    Returns (rho, sig2).
    """
    T = a_diag.shape[0]
    num = 0.0
    den = 0.0

    for t in range(1, T):
        num += P_lag_diag[t] + a_diag[t] * a_diag[t - 1]
        den += Ezz_diag[t - 1]

    rho = 0.0
    if den > 0.0:
        rho = num / den

    s2 = 0.0
    for t in range(T):
        s2 += Ezz_diag[t]
    s2 /= T

    if s2 < min_var:
        s2 = min_var

    return rho, s2