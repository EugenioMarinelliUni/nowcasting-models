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
    Accumulate sufficient statistics for VAR(p) update in companion form.

    Returns:
      S_xx = sum_t E[z_{t-1} z_{t-1}'] for lag stack indices
      S_yx = sum_t E[f_t z_{t-1}'] for contemporaneous factor indices f0_idx
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
    Accumulate Q innovation covariance numerator for factor block update.

    Q_acc = sum_t (Eff - Phi*Efl' - Efl*Phi' + Phi*Ell*Phi')
    where Efl = E[f_t z_{t-1}'], Eff = E[f_t f_t'], Ell = E[z_{t-1} z_{t-1}'].
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
    Update AR(1) parameters for a scalar state z_t using smoothed moments.

    cross[t] = E[z_t z_{t-1}] = P_lag_diag[t] + a[t]*a[t-1]
    rho = sum cross / sum E[z_{t-1}^2]
    sig2 returned is the implied stationary variance q/(1-rho^2), where
    q is the expected AR(1) innovation variance.
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
    if rho > 0.999:
        rho = 0.999
    elif rho < -0.999:
        rho = -0.999

    q = 0.0
    for t in range(1, T):
        cross = P_lag_diag[t] + a_diag[t] * a_diag[t - 1]
        q += Ezz_diag[t] - 2.0 * rho * cross + rho * rho * Ezz_diag[t - 1]
    q /= max(T - 1, 1)

    one_minus_rho2 = 1.0 - rho * rho
    if one_minus_rho2 < 1e-12:
        one_minus_rho2 = 1e-12
    q_floor = min_var * one_minus_rho2
    if q < q_floor:
        q = q_floor
    s2 = q / one_minus_rho2
    if s2 < min_var:
        s2 = min_var

    return rho, s2
