from __future__ import annotations

"""Numerically stable constrained least-squares helpers."""

from typing import Tuple

import numpy as np


def toolbox_R_mat() -> Tuple[np.ndarray, np.ndarray]:
    R = np.array(
        [
            [2.0, -1.0, 0.0, 0.0, 0.0],
            [3.0, 0.0, -1.0, 0.0, 0.0],
            [2.0, 0.0, 0.0, -1.0, 0.0],
            [1.0, 0.0, 0.0, 0.0, -1.0],
        ],
        dtype=float,
    )
    return R, np.zeros(4, dtype=float)


def kron_quarterly_constraints(R_mat: np.ndarray, r_total: int) -> np.ndarray:
    return np.kron(R_mat, np.eye(r_total, dtype=float))


def constrained_ls_fast(
    denom: np.ndarray,
    nom: np.ndarray,
    R_con: np.ndarray,
    q_con: np.ndarray,
) -> np.ndarray:
    """Solve the equality-constrained loading M-step with a KKT system."""
    D = np.asarray(denom, dtype=float)
    n = np.asarray(nom, dtype=float).reshape(-1)
    R = np.asarray(R_con, dtype=float)
    q = np.asarray(q_con, dtype=float).reshape(-1)

    if D.ndim != 2 or D.shape[0] != D.shape[1]:
        raise ValueError("denom must be square 2D.")
    if n.shape[0] != D.shape[0]:
        raise ValueError("nom dimension does not match denom.")
    if R.ndim != 2 or R.shape[1] != D.shape[0]:
        raise ValueError("R_con has incompatible shape.")
    if q.shape[0] != R.shape[0]:
        raise ValueError("q_con has incompatible shape.")

    k = R.shape[0]
    KKT = np.block([[D, R.T], [R, np.zeros((k, k), dtype=float)]])
    rhs = np.concatenate([n, q])
    try:
        sol = np.linalg.solve(KKT, rhs)
    except np.linalg.LinAlgError:
        sol, *_ = np.linalg.lstsq(KKT, rhs, rcond=None)
    c = sol[: D.shape[0]]

    violation = np.max(np.abs(R @ c - q)) if k else 0.0
    if not np.isfinite(violation) or violation > 1e-7:
        raise np.linalg.LinAlgError(
            f"Constrained LS failed to satisfy restrictions; max violation={violation:.3e}."
        )
    return c
