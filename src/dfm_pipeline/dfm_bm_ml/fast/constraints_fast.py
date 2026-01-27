from __future__ import annotations

"""Fast drop-in helpers for :mod:`dfm_pipeline.dfm_bm_ml.constraints`.

The goal is to preserve the exact model logic while reducing runtime.

Key change:
  - Avoid explicit ``inv(denom)`` in constrained least squares.
    We use linear solves instead (same solution, typically faster and more stable).

The original module is kept unchanged for side-by-side comparison.
"""

from typing import Tuple

import numpy as np


def toolbox_R_mat() -> Tuple[np.ndarray, np.ndarray]:
    """Same as :func:`dfm_pipeline.dfm_bm_ml.constraints.toolbox_R_mat`."""
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
    """Same as :func:`dfm_pipeline.dfm_bm_ml.constraints.kron_quarterly_constraints`."""
    return np.kron(R_mat, np.eye(r_total, dtype=float))


def constrained_ls_fast(
    denom: np.ndarray,
    nom: np.ndarray,
    R_con: np.ndarray,
    q_con: np.ndarray,
) -> np.ndarray:
    """Constrained LS without explicit inverses.

    Original projection step:
      C = denom^{-1} nom
      C_con = C - denom^{-1} R' (R denom^{-1} R')^{-1} (R C - q)

    We compute ``denom^{-1} R'`` via a solve.
    """
    # Unconstrained solution
    C = np.linalg.solve(denom, nom)

    # X = denom^{-1} R'  (shape: (p, k) where p=len(C), k=#constraints)
    X = np.linalg.solve(denom, R_con.T)

    # middle = R denom^{-1} R' = R X
    middle = R_con @ X
    lam = np.linalg.solve(middle, (R_con @ C - q_con))

    # C_con = C - (denom^{-1} R') lam
    return C - X @ lam
