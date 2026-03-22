# src/dfm_pipeline/dfm_dyn/state_space_new_uni_numba.py

from __future__ import annotations

import numpy as np

from . import state_space_new as ss_mv
from . import state_space_new_uni as ss_uni

# Re-export common API types so dispatchers/type-checkers see them.
StateSpaceParams = ss_mv.StateSpaceParams
KalmanSmootherResult = ss_mv.KalmanSmootherResult

try:
    from numba import njit
except Exception:  # pragma: no cover
    njit = None


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


if njit is not None:

    @njit(cache=True, fastmath=False)
    def _kf_uni_numba(Y, C, Rdiag, Tm, Q, a0, P0, indptr, indices, s_floor):
        Tn, _ = Y.shape
        m = a0.shape[0]

        a_pred = np.zeros((Tn, m))
        P_pred = np.zeros((Tn, m, m))
        a_filt = np.zeros((Tn, m))
        P_filt = np.zeros((Tn, m, m))
        loglik = 0.0

        a_prev = a0.copy()
        P_prev = P0.copy()

        for t in range(Tn):
            a_pr = Tm @ a_prev
            P_pr = Tm @ P_prev @ Tm.T + Q

            a = a_pr.copy()
            P = P_pr.copy()

            start = int(indptr[t])
            end = int(indptr[t + 1])

            for jj in range(start, end):
                i = int(indices[jj])
                y = Y[t, i]

                # v = y - h a
                v = y
                for j in range(m):
                    v -= C[i, j] * a[j]

                # Pc = P h'
                Pc = np.empty(m, dtype=np.float64)
                for r in range(m):
                    sPc = 0.0
                    for c in range(m):
                        sPc += P[r, c] * C[i, c]
                    Pc[r] = sPc

                s = Rdiag[i]
                for j in range(m):
                    s += C[i, j] * Pc[j]
                if (not np.isfinite(s)) or (s < s_floor):
                    s = s_floor

                invs = 1.0 / s

                # a <- a + Pc * (v/s)
                scale = v * invs
                for j in range(m):
                    a[j] += Pc[j] * scale

                # P <- P - (Pc Pc')/s
                for r in range(m):
                    Pr = Pc[r] * invs
                    for c in range(m):
                        P[r, c] -= Pr * Pc[c]

                loglik += -0.5 * (np.log(2.0 * np.pi) + np.log(s) + (v * v) * invs)

            # symmetrize
            for r in range(m):
                for c in range(r + 1, m):
                    vrc = 0.5 * (P[r, c] + P[c, r])
                    P[r, c] = vrc
                    P[c, r] = vrc

            a_pred[t] = a_pr
            P_pred[t] = P_pr
            a_filt[t] = a
            P_filt[t] = P

            a_prev = a
            P_prev = P

        return a_pred, P_pred, a_filt, P_filt, loglik


def kalman_filter_only(
    Y: np.ndarray,
    ss: StateSpaceParams,
    *,
    r_floor: float = 1e-6,
    s_floor: float = 1e-6,
    symmetrize: bool = True,
):
    Y = np.asarray(Y, dtype=float)
    Rdiag = _r_diag_from_R(ss.R, r_floor=r_floor)
    indptr, indices = ss_uni._build_obs_csr(Y)

    if njit is None:
        return ss_uni.kalman_filter_only(
            Y,
            ss,
            r_floor=r_floor,
            s_floor=s_floor,
            obs_indptr=indptr,
            obs_indices=indices,
            symmetrize=symmetrize,
        )

    a_pred, P_pred, a_filt, P_filt, loglik = _kf_uni_numba(
        np.asarray(Y, dtype=float),
        np.asarray(ss.C, dtype=float),
        np.asarray(Rdiag, dtype=float),
        np.asarray(ss.T, dtype=float),
        np.asarray(ss.Q, dtype=float),
        np.asarray(ss.a0, dtype=float),
        ss_mv._sym(np.asarray(ss.P0, dtype=float)),
        np.asarray(indptr, dtype=np.int64),
        np.asarray(indices, dtype=np.int64),
        float(s_floor),
    )

    if not _all_finite(a_pred, P_pred, a_filt, P_filt):
        return ss_uni.kalman_filter_only(
            Y,
            ss,
            r_floor=r_floor,
            s_floor=s_floor,
            obs_indptr=indptr,
            obs_indices=indices,
            symmetrize=symmetrize,
        )

    return a_pred, P_pred, a_filt, P_filt, float(loglik)


def kalman_filter_smoother(
    Y: np.ndarray,
    ss: StateSpaceParams,
    *,
    r_floor: float = 1e-6,
    s_floor: float = 1e-6,
    base_jitter: float = 1e-10,
    symmetrize: bool = True,
) -> KalmanSmootherResult:
    a_pred, P_pred, a_filt, P_filt, loglik = kalman_filter_only(
        Y, ss, r_floor=r_floor, s_floor=s_floor, symmetrize=symmetrize
    )

    if not _all_finite(a_pred, P_pred, a_filt, P_filt):
        return ss_uni.kalman_filter_smoother(
            np.asarray(Y, dtype=float),
            ss,
            r_floor=r_floor,
            s_floor=s_floor,
            base_jitter=base_jitter,
            symmetrize=symmetrize,
        )

    Tm = np.asarray(ss.T, dtype=float)
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
        return ss_uni.kalman_filter_smoother(
            np.asarray(Y, dtype=float),
            ss,
            r_floor=r_floor,
            s_floor=s_floor,
            base_jitter=base_jitter,
            symmetrize=symmetrize,
        )

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