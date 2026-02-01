"""
NEW (univariate) Kalman filter implementation for diagonal measurement noise.

This is algebraically equivalent to the multivariate update when H is diagonal:
  y_t = Z a_t + e_t,  e_t ~ N(0, H),  H diagonal

It replaces the kxk innovation Cholesky per time step with k scalar updates
(one per observed series), which is typically much faster when the cross-section
is large and the state dimension is moderate.

Notes:
  - If H is not diagonal (beyond a tolerance), we fall back to state_space_new.
  - Numerical results match the multivariate version up to floating-point noise
    (update order differs).
"""

from __future__ import annotations

import numpy as np

from . import state_space_new as ss_mv


def _as_diag(H: np.ndarray, tol: float) -> np.ndarray | None:
    """Return diag(H) if H is (approximately) diagonal; else None."""
    H = np.asarray(H)
    if H.ndim == 1:
        return H.astype(float, copy=False)
    if H.ndim != 2 or H.shape[0] != H.shape[1]:
        raise ValueError("H must be (n,n) or (n,)")

    off = H - np.diag(np.diag(H))
    if np.max(np.abs(off)) > tol:
        return None
    return np.diag(H).astype(float, copy=False)


def _build_obs_csr(Y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Build CSR-style index lists for non-missing observations per time t.

    Returns:
      indptr: shape (T+1,)
      indices: concatenated observed indices (int64)
    """
    Y = np.asarray(Y)
    if Y.ndim != 2:
        raise ValueError("Y must be 2D (T, n_obs)")

    obs_mask = ~np.isnan(Y)
    T, _n = obs_mask.shape
    counts = obs_mask.sum(axis=1).astype(np.int64)
    indptr = np.empty(T + 1, dtype=np.int64)
    indptr[0] = 0
    np.cumsum(counts, out=indptr[1:])

    indices = np.empty(indptr[-1], dtype=np.int64)
    pos = 0
    for t in range(T):
        idx = np.nonzero(obs_mask[t])[0].astype(np.int64)
        k = idx.size
        indices[pos : pos + k] = idx
        pos += k
    return indptr, indices


def kalman_filter_only(
    Y: np.ndarray,
    Z: np.ndarray,
    H: np.ndarray,
    Tm: np.ndarray,
    Q: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
    *,
    diag_H_tol: float = 1e-12,
    obs_indptr: np.ndarray | None = None,
    obs_indices: np.ndarray | None = None,
    symmetrize: bool = True,
) -> ss_mv.KalmanFilterResult:
    """
    Univariate Kalman filter (filtering only), for diagonal H.

    Signature matches state_space_new.kalman_filter_only.
    """
    Y = np.asarray(Y, dtype=float)
    Z = np.asarray(Z, dtype=float)
    Tm = np.asarray(Tm, dtype=float)
    Q = np.asarray(Q, dtype=float)
    a0 = np.asarray(a0, dtype=float)
    P0 = np.asarray(P0, dtype=float)

    H_diag = _as_diag(H, tol=diag_H_tol)
    if H_diag is None:
        # Fall back to multivariate implementation (full H).
        return ss_mv.kalman_filter_only(Y, Z, H, Tm, Q, a0, P0)

    Tn, _n_obs = Y.shape
    k_state = a0.shape[0]

    if obs_indptr is None or obs_indices is None:
        obs_indptr, obs_indices = _build_obs_csr(Y)

    a_filt = np.zeros((Tn, k_state))
    P_filt = np.zeros((Tn, k_state, k_state))
    a_pred = np.zeros((Tn, k_state))
    P_pred = np.zeros((Tn, k_state, k_state))
    loglik = 0.0

    a_prev = a0.copy()
    P_prev = P0.copy()

    for t in range(Tn):
        # predict
        a_pr = Tm @ a_prev
        P_pr = Tm @ P_prev @ Tm.T + Q

        a = a_pr.copy()
        P = P_pr.copy()

        # update: loop over observed series (scalar innovations)
        start = int(obs_indptr[t])
        end = int(obs_indptr[t + 1])
        for jj in range(start, end):
            i = int(obs_indices[jj])
            y = Y[t, i]

            c = Z[i, :]  # (k_state,)
            r = H_diag[i]

            v = y - float(c @ a)
            Pc = P @ c
            s = float(c @ Pc) + float(r)
            if s <= 0.0:
                # Numerical guard; fall back to multivariate for this run
                return ss_mv.kalman_filter_only(Y, Z, np.diag(H_diag), Tm, Q, a0, P0)

            inv_s = 1.0 / s
            K = Pc * inv_s

            a = a + K * v

            cP = c @ P
            P = P - np.outer(K, cP)

            loglik += -0.5 * (np.log(2.0 * np.pi) + np.log(s) + (v * v) * inv_s)

        if symmetrize:
            P = ss_mv._sym(P)

        a_filt[t] = a
        P_filt[t] = P
        a_pred[t] = a_pr
        P_pred[t] = P_pr

        a_prev, P_prev = a, P

    return ss_mv.KalmanFilterResult(
        a_filt=a_filt,
        P_filt=P_filt,
        a_pred=a_pred,
        P_pred=P_pred,
        loglik=float(loglik),
    )


def kalman_filter_smoother(
    Y: np.ndarray,
    Z: np.ndarray,
    H: np.ndarray,
    Tm: np.ndarray,
    Q: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
    *,
    diag_H_tol: float = 1e-12,
    obs_indptr: np.ndarray | None = None,
    obs_indices: np.ndarray | None = None,
    symmetrize: bool = True,
) -> ss_mv.KalmanSmootherResult:
    """
    Univariate filter + RTS smoother.

    Signature matches state_space_new.kalman_filter_smoother.
    """
    kf = kalman_filter_only(
        Y,
        Z,
        H,
        Tm,
        Q,
        a0,
        P0,
        diag_H_tol=diag_H_tol,
        obs_indptr=obs_indptr,
        obs_indices=obs_indices,
        symmetrize=symmetrize,
    )

    Tn, k_state = kf.a_filt.shape
    a_smooth = np.zeros_like(kf.a_filt)
    P_smooth = np.zeros_like(kf.P_filt)

    a_smooth[-1] = kf.a_filt[-1]
    P_smooth[-1] = kf.P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_f = kf.P_filt[t]
        P_pr_next = kf.P_pred[t + 1]

        # J = P_f T' (P_pr_next)^{-1}
        cf = np.linalg.cholesky(P_pr_next)
        rhs = P_f @ Tm.T
        tmp = np.linalg.solve(cf, rhs)
        J = np.linalg.solve(cf.T, tmp)

        a_smooth[t] = kf.a_filt[t] + J @ (a_smooth[t + 1] - kf.a_pred[t + 1])
        P_smooth[t] = ss_mv._sym(P_f + J @ (P_smooth[t + 1] - P_pr_next) @ J.T)

    return ss_mv.KalmanSmootherResult(
        a_filt=kf.a_filt,
        P_filt=kf.P_filt,
        a_pred=kf.a_pred,
        P_pred=kf.P_pred,
        a_smooth=a_smooth,
        P_smooth=P_smooth,
        loglik=kf.loglik,
    )
