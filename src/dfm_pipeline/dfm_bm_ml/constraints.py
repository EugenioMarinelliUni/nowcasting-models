from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import numpy as np


def mm_weights(style: str = "toolbox") -> np.ndarray:
    """Return Mariano-Murasawa five-month aggregation weights."""
    if style == "toolbox":
        return np.array([1.0, 2.0, 3.0, 2.0, 1.0])
    if style == "scaled":
        return np.array([1.0 / 3.0, 2.0 / 3.0, 1.0, 2.0 / 3.0, 1.0 / 3.0])
    raise ValueError(f"Unknown MM weight style: {style!r}")


def mm_sum_sq(style: str = "toolbox") -> float:
    w = mm_weights(style)
    return float(np.sum(w * w))


def mm_proportionality_R_mat(style: str = "toolbox") -> Tuple[np.ndarray, np.ndarray]:
    """Return restrictions imposing loadings proportional to the MM weights.

    If ``c=(c_0,...,c_4)`` denotes the five factor-lag coefficients, the
    restrictions enforce ``c_k = (w_k / w_0) c_0``.  This works for both the
    unscaled ``[1,2,3,2,1]`` convention and any scalar-normalised equivalent,
    including the supported ``scaled`` weights.
    """
    w = mm_weights(style).astype(float)
    if w.shape != (5,):
        raise ValueError("Mariano-Murasawa weights must have shape (5,).")
    if not np.all(np.isfinite(w)) or abs(float(w[0])) <= np.finfo(float).eps:
        raise ValueError("The first Mariano-Murasawa weight must be finite and non-zero.")
    R = np.zeros((4, 5), dtype=float)
    for k in range(1, 5):
        R[k - 1, 0] = float(w[k] / w[0])
        R[k - 1, k] = -1.0
    return R, np.zeros(4, dtype=float)


def toolbox_R_mat() -> Tuple[np.ndarray, np.ndarray]:
    """Backward-compatible alias for toolbox-style MM restrictions."""
    return mm_proportionality_R_mat("toolbox")


def kron_quarterly_constraints(R_mat: np.ndarray, r_total: int) -> np.ndarray:
    return np.kron(R_mat, np.eye(r_total, dtype=float))


def constrained_ls(
    denom: np.ndarray,
    nom: np.ndarray,
    R_con: np.ndarray,
    q_con: np.ndarray,
) -> np.ndarray:
    """Solve equality-constrained quadratic least squares through a KKT system.

    This avoids explicit matrix inversion and solves

        min_c  1/2 c' D c - n'c,  subject to R c = q.

    The supplied ``denom`` should already include the desired regularisation.
    """
    D = np.asarray(denom, dtype=float)
    n = np.asarray(nom, dtype=float).reshape(-1)
    R = np.asarray(R_con, dtype=float)
    q = np.asarray(q_con, dtype=float).reshape(-1)

    if D.ndim != 2 or D.shape[0] != D.shape[1]:
        raise ValueError("denom must be square.")
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


@dataclass(frozen=True)
class BlockIndex:
    n_blocks: int
    mask_monthly: np.ndarray
    mask_quarterly: np.ndarray
