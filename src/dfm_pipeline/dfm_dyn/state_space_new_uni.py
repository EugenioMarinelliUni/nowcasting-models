# src/dfm_pipeline/dfm_dyn/state_space_new_uni.py

from __future__ import annotations

import numpy as np

from . import state_space_new as ss_mv

# Re-export common API types so dispatchers/type-checkers see them.
StateSpaceParams = ss_mv.StateSpaceParams
KalmanSmootherResult = ss_mv.KalmanSmootherResult


def _build_obs_csr(Y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    Y = np.asarray(Y)
    if Y.ndim != 2:
        raise ValueError("Y must be 2D (T, n_obs)")
    obs_mask = ~np.isnan(Y)
    T, _ = obs_mask.shape
    counts = obs_mask.sum(axis=1).astype(np.int64)
    indptr = np.empty(T + 1, dtype=np.int64)
    indptr[0] = 0
    np.cumsum(counts, out=indptr[1:])
    indices = np.empty(indptr[-1], dtype=np.int64)
    pos = 0
    for t in range(T):
        idx = np.nonzero(obs_mask[t])[0].astype(np.int64)
        indices[pos : pos + idx.size] = idx
        pos += idx.size
    return indptr, indices


def _chol_pd(A: np.ndarray, *, base_jitter: float = 1e-10, max_tries: int = 12) -> np.ndarray:
    A = ss_mv._sym(np.asarray(A, dtype=float))
    n = A.shape[0]
    if n == 0:
        return A

    diag = np.diag(A)
    scale = float(np.max(diag)) if diag.size else 1.0
    if (not np.isfinite(scale)) or scale <= 0.0:
        scale = 1.0

    I = np.eye(n, dtype=float)
    j = 0.0
    for _ in range(max_tries):
        try:
            return np.linalg.cholesky(A + j * I)
        except np.linalg.LinAlgError:
            j = (base_jitter * scale) if j == 0.0 else (10.0 * j)

    w, V = np.linalg.eigh(A)
    w = np.maximum(w, base_jitter * scale)
    A_pd = ss_mv._sym((V * w) @ V.T)
    return np.linalg.cholesky(A_pd)


def _all_finite(*arrs: np.ndarray) -> bool:
    return all(np.isfinite(a).all() for a in arrs)


def _r_diag_from_R(R: np.ndarray, r_floor: float) -> np.ndarray:
    R = np.asarray(R, dtype=float)
    if R.ndim == 1:
        d = R.copy()
    elif R.ndim == 2 and R.shape[0] == R.shape[1]:
        d = np.diag(R).copy()
    else:
        raise ValueError("R must be (n,) or (n,n)")
    d = np.where(np.isfinite(d), d, r_floor)
    return np.maximum(d, r_floor)


def kalman_filter_only(
    Y: np.ndarray,
    ss: StateSpaceParams,
    *,
    r_floor: float = 1e-6,
    s_floor: float = 1e-6,
    obs_indptr: np.ndarray | None = None,
    obs_indices: np.ndarray | None = None,
    symmetrize: bool = True,
):
    """
    Stable univariate (sequential) Kalman filter for diagonal R.
    API matches state_space_new.kalman_filter_only(Y, ss).
    """
    Y = np.asarray(Y, dtype=float)
    C = np.asarray(ss.C, dtype=float)
    Tm = np.asarray(ss.T, dtype=float)
    Q = np.asarray(ss.Q, dtype=float)
    a0 = np.asarray(ss.a0, dtype=float)
    P0 = ss_mv._sym(np.asarray(ss.P0, dtype=float))

    R_diag = _r_diag_from_R(ss.R, r_floor=r_floor)

    Tn, _ = Y.shape
    m = a0.shape[0]

    if obs_indptr is None or obs_indices is None:
        obs_indptr, obs_indices = _build_obs_csr(Y)

    a_pred = np.zeros((Tn, m))
    P_pred = np.zeros((Tn, m, m))
    a_filt = np.zeros((Tn, m))
    P_filt = np.zeros((Tn, m, m))
    loglik = 0.0

    a_prev = a0.copy()
    P_prev = P0.copy()

    for t in range(Tn):
        a_pr = Tm @ a_prev
        P_pr = ss_mv._sym(Tm @ P_prev @ Tm.T + Q)

        a = a_pr.copy()
        P = P_pr.copy()

        start = int(obs_indptr[t])
        end = int(obs_indptr[t + 1])

        for jj in range(start, end):
            i = int(obs_indices[jj])
            y = float(Y[t, i])

            h = C[i]  # (m,)
            v = float(y - h @ a)

            Pc = P @ h
            s = float(h @ Pc + R_diag[i])
            if (not np.isfinite(s)) or s < s_floor:
                s = float(s_floor)

            inv_s = 1.0 / s
            a = a + Pc * (v * inv_s)
            P = P - np.outer(Pc, Pc) * inv_s

            if symmetrize:
                P = ss_mv._sym(P)

            loglik += -0.5 * (np.log(2.0 * np.pi) + np.log(s) + (v * v) * inv_s)

        a_pred[t] = a_pr
        P_pred[t] = P_pr
        a_filt[t] = a
        P_filt[t] = P

        a_prev, P_prev = a, P

    if not _all_finite(a_pred, P_pred, a_filt, P_filt):
        raise RuntimeError("Univariate filter produced non-finite outputs (a/P).")

    return a_pred, P_pred, a_filt, P_filt, float(loglik)


def kalman_filter_smoother(
    Y: np.ndarray,
    ss: StateSpaceParams,
    *,
    r_floor: float = 1e-6,
    s_floor: float = 1e-6,
    base_jitter: float = 1e-10,
    obs_indptr: np.ndarray | None = None,
    obs_indices: np.ndarray | None = None,
    symmetrize: bool = True,
) -> KalmanSmootherResult:
    """
    Univariate filter + RTS smoother. API matches state_space_new.
    """
    Y = np.asarray(Y, dtype=float)
    Tm = np.asarray(ss.T, dtype=float)

    a_pred, P_pred, a_filt, P_filt, loglik = kalman_filter_only(
        Y,
        ss,
        r_floor=r_floor,
        s_floor=s_floor,
        obs_indptr=obs_indptr,
        obs_indices=obs_indices,
        symmetrize=symmetrize,
    )

    Tn, m = a_filt.shape
    a_smooth = np.zeros_like(a_filt)
    P_smooth = np.zeros_like(P_filt)
    J = np.zeros((max(Tn - 1, 0), m, m))

    a_smooth[-1] = a_filt[-1]
    P_smooth[-1] = P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_pred_next = ss_mv._sym(P_pred[t + 1])
        cf = _chol_pd(P_pred_next, base_jitter=base_jitter)

        rhs = P_filt[t] @ Tm.T
        tmp = np.linalg.solve(cf, rhs)
        J_t = np.linalg.solve(cf.T, tmp)
        J[t] = J_t

        a_smooth[t] = a_filt[t] + J_t @ (a_smooth[t + 1] - a_pred[t + 1])
        P_smooth[t] = ss_mv._sym(P_filt[t] + J_t @ (P_smooth[t + 1] - P_pred_next) @ J_t.T)

    P_lag_smooth = np.zeros_like(P_smooth)
    for t in range(1, Tn):
        P_lag_smooth[t] = P_smooth[t] @ J[t - 1].T

    if not _all_finite(a_smooth, P_smooth, P_lag_smooth):
        raise RuntimeError("Univariate smoother produced non-finite outputs.")

    return ss_mv.KalmanSmootherResult(
        a_pred=a_pred,
        P_pred=P_pred,
        a_filt=a_filt,
        P_filt=P_filt,
        a_smooth=a_smooth,
        P_smooth=P_smooth,
        P_lag_smooth=P_lag_smooth,
        loglik=float(loglik),
    )