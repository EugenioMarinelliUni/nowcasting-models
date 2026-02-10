from __future__ import annotations

"""Fast helpers for constrained least squares in quarterly loading updates.

Public contract: constrained_ls_fast(denom, nom, R_con, q_con) -> C_con
No scaling or model logic belongs here.
"""

from typing import Tuple

import numpy as np


def toolbox_R_mat() -> Tuple[np.ndarray, np.ndarray]:
    """Toolbox proportionality constraints for 5-lag quarterly loadings."""
    R = np.array(
        [
            [2.0, -1.0, 0.0, 0.0, 0.0],
            [3.0, 0.0, -1.0, 0.0, 0.0],
            [2.0, 0.0, 0.0, -1.0, 0.0],
            [1.0, 0.0, 0.0, 0.0, -1.0],
        ],
        dtype=float,
    )
    q = np.zeros((4,), dtype=float)
    return R, q


def kron_quarterly_constraints(R_mat: np.ndarray, r_total: int) -> np.ndarray:
    """Kronecker expansion of R_mat over factor dimension r_total."""
    return np.kron(R_mat, np.eye(r_total, dtype=float))


def constrained_ls_fast(
    denom: np.ndarray,
    nom: np.ndarray,
    R_con: np.ndarray,
    q_con: np.ndarray,
) -> np.ndarray:
    """
    Constrained LS without explicit inverses.

    Unconstrained:
      C = denom^{-1} nom

    Projection:
      C_con = C - denom^{-1} R' (R denom^{-1} R')^{-1} (R C - q)
    """
    denom = np.asarray(denom, dtype=float)
    nom = np.asarray(nom, dtype=float)

    if denom.ndim != 2 or denom.shape[0] != denom.shape[1]:
        raise ValueError("denom must be square 2D.")
    if nom.ndim == 1:
        nom = nom.reshape(-1, 1)

    C = np.linalg.solve(denom, nom)

    X = np.linalg.solve(denom, R_con.T)
    middle = R_con @ X
    lam = np.linalg.solve(middle, (R_con @ C - q_con.reshape(-1, 1)))
    C_con = C - X @ lam

    return C_con.reshape(-1)
