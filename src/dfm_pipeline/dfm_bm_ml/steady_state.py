from __future__ import annotations

import numpy as np
from scipy.linalg import solve_discrete_lyapunov


def safe_sym(M: np.ndarray) -> np.ndarray:
    M = np.asarray(M, dtype=float)
    return 0.5 * (M + M.T)


def project_psd(M: np.ndarray, eps: float = 1e-10) -> np.ndarray:
    M = safe_sym(M)
    vals, vecs = np.linalg.eigh(M)
    vals = np.maximum(vals, eps)
    return safe_sym((vecs * vals) @ vecs.T)


def unconditional_covariance(T: np.ndarray, Q: np.ndarray, jitter: float = 1e-10) -> np.ndarray:
    """
    Solve P = T P T' + Q and project the result back to the PSD cone.
    This is the closest thing to the toolbox-style unconditional initialization
    without rewriting the whole BM state builder.
    """
    T = np.asarray(T, dtype=float)
    Q = safe_sym(np.asarray(Q, dtype=float))
    try:
        P = solve_discrete_lyapunov(T, Q)
    except Exception:
        # Fallback fixed-point iteration for near-singular cases.
        P = Q.copy()
        for _ in range(500):
            P_new = safe_sym(T @ P @ T.T + Q)
            if np.max(np.abs(P_new - P)) < 1e-10:
                P = P_new
                break
            P = P_new
    return project_psd(P, eps=max(jitter, 1e-12))


__all__ = ["safe_sym", "project_psd", "unconditional_covariance"]
