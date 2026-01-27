# src/dfm_pipeline/dfm_bm_ml/state_space_new_cached.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple, List

import numpy as np
import scipy.linalg as la


@dataclass
class MaskCacheEntry:
    idx: np.ndarray            # observed indices (int64)
    m: int                     # number observed
    Zm: np.ndarray             # Z[idx, :]  (m x k)
    Hm: np.ndarray             # H[idx, idx] (m x m)


class MeasurementMaskCache:
    """
    Caches per-missingness-pattern reduced measurement matrices.
    Keeps logic identical; avoids repeated slicing/allocations.
    """
    def __init__(self, Z: np.ndarray, H: np.ndarray):
        self.Z = np.asarray(Z, dtype=np.float64, order="C")
        self.H = np.asarray(H, dtype=np.float64, order="C")
        self._cache: Dict[bytes, MaskCacheEntry] = {}

    @staticmethod
    def _key_from_mask(mask: np.ndarray) -> bytes:
        # mask: boolean shape (n,)
        # Using raw bytes of uint8 mask is stable and fast.
        return np.asarray(mask, dtype=np.uint8, order="C").tobytes()

    def get(self, obs_mask: np.ndarray) -> MaskCacheEntry:
        key = self._key_from_mask(obs_mask)
        hit = self._cache.get(key)
        if hit is not None:
            return hit

        idx = np.flatnonzero(obs_mask).astype(np.int64)
        m = int(idx.size)
        Zm = np.ascontiguousarray(self.Z[idx, :], dtype=np.float64)
        # Avoid np.ix_ in hot loop; do it once per unique mask here.
        Hm = np.ascontiguousarray(self.H[np.ix_(idx, idx)], dtype=np.float64)

        ent = MaskCacheEntry(idx=idx, m=m, Zm=Zm, Hm=Hm)
        self._cache[key] = ent
        return ent


def kalman_filter_only_cached(
    Y: np.ndarray,
    Z: np.ndarray,
    H: np.ndarray,
    T: np.ndarray,
    Q: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, float]:
    """
    Drop-in replacement for state_space_new.kalman_filter_only.
    Handles missing Y by masking; caches measurement reductions.
    Logic unchanged; only reduces overhead and allocations.
    """
    Y = np.asarray(Y, dtype=np.float64, order="C")
    Z = np.asarray(Z, dtype=np.float64, order="C")
    H = np.asarray(H, dtype=np.float64, order="C")
    Tm = np.asarray(T, dtype=np.float64, order="C")
    Q = np.asarray(Q, dtype=np.float64, order="C")
    a = np.asarray(a0, dtype=np.float64, order="C").copy()
    P = np.asarray(P0, dtype=np.float64, order="C").copy()

    nT, n = Y.shape
    k = a.shape[0]

    a_filt = np.empty((nT, k), dtype=np.float64)
    P_filt = np.empty((nT, k, k), dtype=np.float64)

    cache = MeasurementMaskCache(Z=Z, H=H)

    # Preallocate maximum-sized work buffers (n is max m)
    v = np.empty(n, dtype=np.float64)
    S = np.empty((n, n), dtype=np.float64)
    K = np.empty((k, n), dtype=np.float64)

    loglik = 0.0
    I_k = np.eye(k, dtype=np.float64)

    for t in range(nT):
        yt = Y[t, :]
        obs_mask = np.isfinite(yt)
        ent = cache.get(obs_mask)
        m = ent.m

        # Predict
        a = Tm @ a
        P = Tm @ P @ Tm.T + Q

        if m == 0:
            a_filt[t] = a
            P_filt[t] = P
            continue

        idx = ent.idx
        Zm = ent.Zm         # (m x k)
        Hm = ent.Hm         # (m x m)

        # v_m = y_m - Zm a
        # (use preallocated v[:m])
        np.subtract(yt[idx], Zm @ a, out=v[:m])

        # S_m = Zm P Zm' + Hm
        # build into S[:m,:m] preallocated
        S_mm = S[:m, :m]
        S_mm[:] = Zm @ P @ Zm.T
        S_mm += Hm

        # Cholesky factorization of S_mm
        # Use overwrite_a to reduce allocations
        c, lower = la.cho_factor(S_mm, lower=True, overwrite_a=True, check_finite=False)

        # Solve for Kalman gain: K = P Zm' S^{-1}
        # First compute PZ' into K[:, :m], then right-solve
        K_km = K[:, :m]
        K_km[:] = P @ Zm.T
        # Solve S * X' = (PZ')' -> X = (PZ') * S^{-1}
        # cho_solve returns a new array; avoid by solving per column block via solve_triangular twice
        # Here we accept one allocation per t; if needed, implement in-place triangular solves.
        K_km[:] = (la.cho_solve((c, lower), K_km.T, check_finite=False).T)

        # Update a, P
        a = a + K_km @ v[:m]
        P = (I_k - K_km @ Zm) @ P

        # Log-likelihood contribution: -0.5*(m log(2pi) + log|S| + v'S^{-1}v)
        # Compute v'S^{-1}v using cho_solve
        Sinv_v = la.cho_solve((c, lower), v[:m], check_finite=False)
        quad = float(v[:m] @ Sinv_v)
        logdet = 2.0 * float(np.log(np.diag(c)).sum())
        loglik += -0.5 * (m * np.log(2.0 * np.pi) + logdet + quad)

        a_filt[t] = a
        P_filt[t] = P

    return a_filt, P_filt, loglik


def kalman_filter_smoother_cached(
    Y: np.ndarray,
    Z: np.ndarray,
    H: np.ndarray,
    T: np.ndarray,
    Q: np.ndarray,
    a0: np.ndarray,
    P0: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, float]:
    """
    Minimal “cached” version: run filter_cached then RTS smoother.
    Keep it structurally identical to your existing smoother to avoid logic drift.
    """
    a_filt, P_filt, loglik = kalman_filter_only_cached(Y, Z, H, T, Q, a0, P0)

    nT, k = a_filt.shape
    a_sm = np.empty_like(a_filt)
    P_sm = np.empty_like(P_filt)

    Tm = np.asarray(T, dtype=np.float64, order="C")
    Q = np.asarray(Q, dtype=np.float64, order="C")

    # Recompute predictions needed for RTS (to avoid storing all predicted states/covs)
    # If your current implementation stores predicted arrays, mirror that for exactness.
    a_pred = np.empty((nT, k), dtype=np.float64)
    P_pred = np.empty((nT, k, k), dtype=np.float64)

    a = np.asarray(a0, dtype=np.float64, order="C").copy()
    P = np.asarray(P0, dtype=np.float64, order="C").copy()

    for t in range(nT):
        a = Tm @ a
        P = Tm @ P @ Tm.T + Q
        a_pred[t] = a
        P_pred[t] = P

    a_sm[-1] = a_filt[-1]
    P_sm[-1] = P_filt[-1]

    for t in range(nT - 2, -1, -1):
        # J = P_filt[t] T' (P_pred[t+1])^{-1}
        # Use solve rather than explicit inverse
        Pt = P_filt[t]
        Pp = P_pred[t + 1]

        # Solve Pp * X = (T*Pt)' -> X = Pp^{-1} (T*Pt)'
        # Then J = (X')  (k x k)
        TP = Tm @ Pt
        X = la.solve(Pp, TP, assume_a="pos", check_finite=False)  # (k x k)
        J = X.T

        a_sm[t] = a_filt[t] + J @ (a_sm[t + 1] - a_pred[t + 1])
        P_sm[t] = Pt + J @ (P_sm[t + 1] - Pp) @ J.T

    return a_sm, P_sm, loglik
