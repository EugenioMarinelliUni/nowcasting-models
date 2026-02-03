from __future__ import annotations

import numpy as np


def solve_discrete_lyapunov_vec(A: np.ndarray, Q: np.ndarray, *, jitter: float = 0.0) -> np.ndarray:
    """
    Solve P = A P A' + Q via vec operator:
        vec(P) = (I - kron(A, A))^{-1} vec(Q)

    Intended for small systems (e.g., factor companion blocks and 5x5 quarterly idio register).

    Falls back to least squares if the linear system is singular/ill-conditioned.
    """
    A = np.asarray(A, dtype=float)
    Q = np.asarray(Q, dtype=float)

    n = A.shape[0]
    if A.shape != (n, n) or Q.shape != (n, n):
        raise ValueError(f"A and Q must be square and same shape; got A={A.shape}, Q={Q.shape}")

    I = np.eye(n * n, dtype=float)
    K = np.kron(A, A)
    M = I - K
    b = Q.reshape(n * n, order="F")

    try:
        x = np.linalg.solve(M, b)
    except np.linalg.LinAlgError:
        x = np.linalg.lstsq(M, b, rcond=None)[0]

    P = x.reshape((n, n), order="F")
    P = 0.5 * (P + P.T)
    if jitter > 0.0:
        P = P + float(jitter) * np.eye(n, dtype=float)
    return P


def safe_psd(P: np.ndarray, *, eps: float = 0.0) -> np.ndarray:
    """
    Symmetrize and (optionally) floor eigenvalues.
    eps=0 keeps eigenvalues as-is (symmetrize only).
    """
    P = 0.5 * (P + P.T)
    if eps <= 0.0:
        return P
    w, V = np.linalg.eigh(P)
    w = np.maximum(w, float(eps))
    return (V * w) @ V.T
