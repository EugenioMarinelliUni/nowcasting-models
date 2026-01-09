from __future__ import annotations

import numpy as np


def safe_sym(A: np.ndarray) -> np.ndarray:
    return 0.5 * (A + A.T)


def steady_state_cov(A: np.ndarray, Q: np.ndarray, jitter: float = 1e-12) -> np.ndarray:
    """
    Solve for the unconditional covariance V of a stable linear system:
        x_t = A x_{t-1} + u_t,   u_t ~ (0, Q)
    V solves:  V = A V A' + Q

    Vectorized solution:
        vec(V) = (I - A ⊗ A)^{-1} vec(Q)

    Notes:
    - Assumes A is stable; if not, the solution may be non-finite or ill-conditioned.
    - Adds `jitter` to the linear system for numerical stability.
    """
    n = A.shape[0]
    if A.shape != (n, n):
        raise ValueError("A must be square.")
    if Q.shape != (n, n):
        raise ValueError("Q must have same shape as A.")

    K = np.eye(n * n, dtype=float) - np.kron(A, A)
    if jitter > 0.0:
        K = K + np.eye(n * n, dtype=float) * jitter

    v = np.linalg.solve(K, Q.reshape(-1, order="F"))
    V = v.reshape((n, n), order="F")
    return safe_sym(V)
