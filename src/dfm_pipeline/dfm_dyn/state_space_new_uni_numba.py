"""
NEW (univariate) Kalman filter with numba-accelerated measurement updates.

Requires:
  - H diagonal (else we fall back to state_space_new_uni / state_space_new)
  - numba installed (else we fall back to state_space_new_uni)

This keeps the same public API as state_space_new.kalman_filter_only / smoother.
"""

from __future__ import annotations

import numpy as np

from . import state_space_new as ss_mv
from . import state_space_new_uni as ss_uni

try:
    from numba import njit
except Exception:  # pragma: no cover
    njit = None


def _build_obs_csr(Y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    return ss_uni._build_obs_csr(Y)


if njit is not None:

    @njit(cache=True, fastmath=False)
    def _kf_univariate_numba(
        Y: np.ndarray,
        Z: np.ndarray,
        H_diag: np.ndarray,
        Tm: np.ndarray,
        Q: np.ndarray,
        a0: np.ndarray,
        P0: np.ndarray,
        obs_indptr: np.ndarray,
        obs_indices: np.ndarray,
        symmetrize: bool,
    ):
        Tn, _n_obs = Y.shape
        k_state = a0.shape[0]

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

            start = int(obs_indptr[t])
            end = int(obs_indptr[t + 1])

            for jj in range(start, end):
                i = int(obs_indices[jj])
                y = Y[t, i]

                # v = y - c @ a
                v = y
                for j in range(k_state):
                    v -= Z[i, j] * a[j]

                # Pc = P @ c'
                Pc = np.empty(k_state, dtype=np.float64)
                for r in range(k_state):
                    sPc = 0.0
                    for c in range(k_state):
                        sPc += P[r, c] * Z[i, c]
                    Pc[r] = sPc

                # s = c P c' + r
                s = H_diag[i]
                for j in range(k_state):
                    s += Z[i, j] * Pc[j]

                inv_s = 1.0 / s

                # K = Pc / s
                K = np.empty(k_state, dtype=np.float64)
                for j in range(k_state):
                    K[j] = Pc[j] * inv_s

                # a += K * v
                for j in range(k_state):
                    a[j] += K[j] * v

                # cP = c @ P
                cP = np.empty(k_state, dtype=np.float64)
                for j in range(k_state):
                    scP = 0.0
                    for c in range(k_state):
                        scP += Z[i, c] * P[c, j]
                    cP[j] = scP

                # P -= outer(K, cP)
                for r in range(k_state):
                    Kr = K[r]
                    for c in range(k_state):
                        P[r, c] -= Kr * cP[c]

                loglik += -0.5 * (np.log(2.0 * np.pi) + np.log(s) + (v * v) * inv_s)

            if symmetrize:
                for r in range(k_state):
                    for c in range(r + 1, k_state):
                        vrc = 0.5 * (P[r, c] + P[c, r])
                        P[r, c] = vrc
                        P[c, r] = vrc

            for j in range(k_state):
                a_filt[t, j] = a[j]
                a_pred[t, j] = a_pr[j]
                for c in range(k_state):
                    P_filt[t, j, c] = P[j, c]
                    P_pred[t, j, c] = P_pr[j, c]

            a_prev = a
            P_prev = P

        return a_filt, P_filt, a_pred, P_pred, loglik


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
    """Numba-accelerated univariate filter. Falls back gracefully."""
    H_diag = ss_uni._as_diag(H, tol=diag_H_tol)
    if H_diag is None:
        return ss_mv.kalman_filter_only(Y, Z, H, Tm, Q, a0, P0)

    if obs_indptr is None or obs_indices is None:
        obs_indptr, obs_indices = _build_obs_csr(np.asarray(Y, dtype=float))

    if njit is None:
        return ss_uni.kalman_filter_only(
            Y,
            Z,
            np.diag(H_diag),
            Tm,
            Q,
            a0,
            P0,
            diag_H_tol=diag_H_tol,
            obs_indptr=obs_indptr,
            obs_indices=obs_indices,
            symmetrize=symmetrize,
        )

    a_filt, P_filt, a_pred, P_pred, loglik = _kf_univariate_numba(
        np.asarray(Y, dtype=float),
        np.asarray(Z, dtype=float),
        np.asarray(H_diag, dtype=float),
        np.asarray(Tm, dtype=float),
        np.asarray(Q, dtype=float),
        np.asarray(a0, dtype=float),
        np.asarray(P0, dtype=float),
        np.asarray(obs_indptr, dtype=np.int64),
        np.asarray(obs_indices, dtype=np.int64),
        bool(symmetrize),
    )

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
    """Numba univariate filter + numpy RTS smoother."""
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

    Tn, _k_state = kf.a_filt.shape
    a_smooth = np.zeros_like(kf.a_filt)
    P_smooth = np.zeros_like(kf.P_filt)

    a_smooth[-1] = kf.a_filt[-1]
    P_smooth[-1] = kf.P_filt[-1]

    for t in range(Tn - 2, -1, -1):
        P_f = kf.P_filt[t]
        P_pr_next = kf.P_pred[t + 1]

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
