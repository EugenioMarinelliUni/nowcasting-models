from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import numpy as np


def mm_weights(style: str = "toolbox") -> np.ndarray:
    """
    Mariano–Murasawa weights.

    - toolbox: integer weights [1,2,3,2,1]
    - scaled:  [1/3,2/3,1,2/3,1/3]
    """
    if style == "toolbox":
        return np.array([1.0, 2.0, 3.0, 2.0, 1.0])
    if style == "scaled":
        return np.array([1.0 / 3.0, 2.0 / 3.0, 1.0, 2.0 / 3.0, 1.0 / 3.0])
    raise ValueError(f"Unknown MM weight style: {style!r}")


def mm_sum_sq(style: str = "toolbox") -> float:
    """Return sum_k w_k^2 for the chosen MM weight style."""
    w = mm_weights(style)
    return float(np.sum(w * w))


def toolbox_R_mat() -> Tuple[np.ndarray, np.ndarray]:
    """
    Toolbox quarterly loading constraint matrix R_mat and q.

    Enforces c = a * [1,2,3,2,1] by linear equalities:
      2*c1 - c2 = 0
      3*c1 - c3 = 0
      2*c1 - c4 = 0
      1*c1 - c5 = 0
    """
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
    """Return kron(R_mat, I_r) as in the toolbox."""
    return np.kron(R_mat, np.eye(r_total, dtype=float))


def constrained_ls(
    denom: np.ndarray,
    nom: np.ndarray,
    R_con: np.ndarray,
    q_con: np.ndarray,
) -> np.ndarray:
    """
    Constrained LS using the toolbox-style projection step.

    Unconstrained solution: C = denom^{-1} nom
    Projection:
      C_con = C - denom^{-1} R' (R denom^{-1} R')^{-1} (R C - q)
    """
    C = np.linalg.solve(denom, nom)
    inv_d = np.linalg.inv(denom)
    middle = R_con @ inv_d @ R_con.T
    lam = np.linalg.solve(middle, (R_con @ C - q_con))
    C_constr = C - inv_d @ R_con.T @ lam
    return C_constr


@dataclass(frozen=True)
class BlockIndex:
    n_blocks: int
    mask_monthly: np.ndarray     # (n_blocks, r_total) on contemporaneous factors
    mask_quarterly: np.ndarray   # (n_blocks, 5*r_total) on factor stack
