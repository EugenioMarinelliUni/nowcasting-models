from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

import numpy as np
from scipy.linalg import qr as scipy_qr

from .state_builder import BMParams


@dataclass(frozen=True)
class IdentificationInfo:
    mode: str
    signs: np.ndarray
    anchor_indices: np.ndarray
    rotations: Tuple[np.ndarray, ...] = ()


def _copy_params_with_block_rotations(
    params: BMParams,
    *,
    r_by_block: Sequence[int],
    rotations: Sequence[np.ndarray],
) -> BMParams:
    """Apply orthogonal factor rotations independently within each block.

    The convention is ``f_new = H' f_old`` and therefore
    ``Lambda_new = Lambda_old H``.  VAR and innovation-covariance parameters
    are transformed consistently, preserving the likelihood, fitted common
    component and forecasts.  Cross-block rotations are never allowed.
    """
    r_by_block = tuple(int(r) for r in r_by_block)
    rotations = tuple(np.asarray(H, dtype=float) for H in rotations)
    if len(rotations) != len(r_by_block):
        raise ValueError("One rotation matrix is required per factor block.")

    phi_new: list[list[np.ndarray]] = []
    q_new: list[np.ndarray] = []
    lambda_m_parts: list[np.ndarray] = []
    lambda_q_parts: list[np.ndarray] = []
    cursor = 0
    lam_m = np.asarray(params.Lambda_m, dtype=float)
    lam_q = np.asarray(params.Lambda_q, dtype=float)

    for b, rb in enumerate(r_by_block):
        H = rotations[b]
        if H.shape != (rb, rb):
            raise ValueError(f"Rotation for block {b} must have shape {(rb, rb)}.")
        if not np.allclose(H.T @ H, np.eye(rb), atol=1e-9, rtol=1e-9):
            raise ValueError(f"Rotation for block {b} is not orthogonal.")
        lambda_m_parts.append(lam_m[:, cursor : cursor + rb] @ H)
        lambda_q_parts.append(lam_q[:, cursor : cursor + rb] @ H)
        phi_new.append([H.T @ np.asarray(P, float) @ H for P in params.Phi_blocks[b]])
        q_new.append(H.T @ np.asarray(params.Q_f_blocks[b], float) @ H)
        cursor += rb

    return BMParams(
        Phi_blocks=phi_new,
        Q_f_blocks=q_new,
        rho_m=np.asarray(params.rho_m, float).copy(),
        sig2_m=np.asarray(params.sig2_m, float).copy(),
        rho_q=np.asarray(params.rho_q, float).copy(),
        sig2_q=np.asarray(params.sig2_q, float).copy(),
        Lambda_m=np.hstack(lambda_m_parts),
        Lambda_q=np.hstack(lambda_q_parts),
        R_diag_m=np.asarray(params.R_diag_m, float).copy(),
        R_diag_q=np.asarray(params.R_diag_q, float).copy(),
    )


def identify_signs(
    params: BMParams,
    *,
    r_by_block: Sequence[int],
    anchor_indices: Optional[Sequence[int]] = None,
) -> tuple[BMParams, IdentificationInfo]:
    """Choose factor signs so selected monthly anchor loadings are non-negative.

    Sign anchoring fully identifies only one-factor blocks.  For blocks with
    multiple factors, use :func:`identify_anchor_triangular`, which removes the
    within-block orthogonal rotational ambiguity as well as sign ambiguity.
    """
    lam = np.asarray(params.Lambda_m, dtype=float)
    r_by_block = tuple(int(r) for r in r_by_block)
    r_total = int(sum(r_by_block))
    if lam.ndim != 2 or lam.shape[1] != r_total:
        raise ValueError("Lambda_m shape is incompatible with r_by_block.")

    if anchor_indices is None:
        anchors = np.argmax(np.abs(lam), axis=0).astype(int)
    else:
        anchors = np.asarray(tuple(int(x) for x in anchor_indices), dtype=int)
        if anchors.shape != (r_total,):
            raise ValueError("anchor_indices must contain one index per factor.")
        if np.any(anchors < 0) or np.any(anchors >= lam.shape[0]):
            raise IndexError("identification anchor index is outside Lambda_m rows.")

    anchor_loadings = lam[anchors, np.arange(r_total)]
    signs = np.where(anchor_loadings < 0.0, -1.0, 1.0)
    rotations: list[np.ndarray] = []
    cursor = 0
    for rb in r_by_block:
        rotations.append(np.diag(signs[cursor : cursor + rb]))
        cursor += rb
    identified = _copy_params_with_block_rotations(
        params, r_by_block=r_by_block, rotations=rotations
    )
    return identified, IdentificationInfo(
        mode="sign_anchor",
        signs=signs,
        anchor_indices=anchors,
        rotations=tuple(rotations),
    )


def _automatic_anchor_rows(loadings: np.ndarray, rb: int) -> np.ndarray:
    """Select linearly independent anchor rows by pivoted QR."""
    if loadings.ndim != 2 or loadings.shape[1] != rb:
        raise ValueError("Block loading matrix has incompatible shape.")
    rank = int(np.linalg.matrix_rank(loadings))
    if rank < rb:
        raise ValueError(
            f"Cannot identify {rb} factors: the block loading matrix has rank {rank}."
        )
    _q, _r, piv = scipy_qr(loadings.T, mode="economic", pivoting=True)
    anchors = np.asarray(piv[:rb], dtype=int)
    if np.unique(anchors).size != rb:
        raise ValueError("Automatic identification did not find distinct anchor rows.")
    return anchors


def identify_anchor_triangular(
    params: BMParams,
    *,
    r_by_block: Sequence[int],
    anchor_indices: Optional[Sequence[int]] = None,
    rank_tolerance: float = 1e-10,
) -> tuple[BMParams, IdentificationInfo]:
    """Orthogonally identify every factor block by anchor-loadings restrictions.

    For a block containing ``r`` factors, ``r`` monthly anchor series are chosen
    (automatically by pivoted QR or explicitly by the caller).  An orthogonal
    within-block rotation makes their ``r x r`` loading submatrix lower
    triangular with a positive diagonal.  This removes permutation, sign and
    orthogonal rotational ambiguity while preserving the model likelihood and
    common component.  It is a statistical normalisation, not a structural
    economic identification.
    """
    lam = np.asarray(params.Lambda_m, dtype=float)
    r_by_block = tuple(int(r) for r in r_by_block)
    r_total = int(sum(r_by_block))
    if lam.ndim != 2 or lam.shape[1] != r_total:
        raise ValueError("Lambda_m shape is incompatible with r_by_block.")

    supplied: Optional[np.ndarray]
    if anchor_indices is None:
        supplied = None
    else:
        supplied = np.asarray(tuple(int(x) for x in anchor_indices), dtype=int)
        if supplied.shape != (r_total,):
            raise ValueError("anchor_indices must contain one monthly-series index per factor.")
        if np.any(supplied < 0) or np.any(supplied >= lam.shape[0]):
            raise IndexError("identification anchor index is outside Lambda_m rows.")

    rotations: list[np.ndarray] = []
    anchors_all: list[int] = []
    signs_all: list[float] = []
    cursor = 0
    for block_number, rb in enumerate(r_by_block):
        block_loadings = lam[:, cursor : cursor + rb]
        if supplied is None:
            anchors = _automatic_anchor_rows(block_loadings, rb)
        else:
            anchors = supplied[cursor : cursor + rb]
            if np.unique(anchors).size != rb:
                raise ValueError(
                    f"Identification anchors for block {block_number} must be distinct."
                )

        anchor_matrix = block_loadings[anchors, :]
        singular_values = np.linalg.svd(anchor_matrix, compute_uv=False)
        if singular_values.size != rb or float(singular_values[-1]) <= float(rank_tolerance):
            raise ValueError(
                f"Identification anchor loading matrix for block {block_number} is rank deficient."
            )

        # If M' = Q R, then M Q = R' is lower triangular.  Column signs are
        # subsequently selected so its diagonal is positive.
        Q, _R = np.linalg.qr(anchor_matrix.T)
        H = np.asarray(Q, dtype=float)
        triangular = anchor_matrix @ H
        diagonal = np.diag(triangular)
        if np.any(np.abs(diagonal) <= float(rank_tolerance)):
            raise ValueError(
                f"Identification anchor loading matrix for block {block_number} has a zero diagonal."
            )
        signs = np.where(diagonal < 0.0, -1.0, 1.0)
        H = H @ np.diag(signs)

        check = anchor_matrix @ H
        if np.max(np.abs(np.triu(check, k=1))) > 1e-8:
            raise RuntimeError("Triangular factor identification failed numerically.")
        if np.any(np.diag(check) <= 0.0):
            raise RuntimeError("Triangular factor identification failed to produce positive diagonal.")

        rotations.append(H)
        anchors_all.extend(int(x) for x in anchors)
        signs_all.extend(float(x) for x in signs)
        cursor += rb

    identified = _copy_params_with_block_rotations(
        params, r_by_block=r_by_block, rotations=rotations
    )
    return identified, IdentificationInfo(
        mode="anchor_triangular",
        signs=np.asarray(signs_all, dtype=float),
        anchor_indices=np.asarray(anchors_all, dtype=int),
        rotations=tuple(rotations),
    )


def align_procrustes(
    params: BMParams,
    reference_lambda_m: np.ndarray,
    *,
    r_by_block: Sequence[int],
) -> BMParams:
    """Orthogonally align factors block-by-block to reference monthly loadings.

    This reporting utility is useful across vintages or bootstrap replications.
    It does not impose structural economic identification, and cross-block
    rotations are deliberately excluded.
    """
    current = np.asarray(params.Lambda_m, dtype=float)
    reference = np.asarray(reference_lambda_m, dtype=float)
    if current.shape != reference.shape:
        raise ValueError("reference_lambda_m must have the same shape as Lambda_m.")

    rotations: list[np.ndarray] = []
    cursor = 0
    for rb in (int(r) for r in r_by_block):
        A = current[:, cursor : cursor + rb]
        B = reference[:, cursor : cursor + rb]
        U, _s, Vt = np.linalg.svd(A.T @ B, full_matrices=False)
        rotations.append(U @ Vt)
        cursor += rb
    return _copy_params_with_block_rotations(
        params, r_by_block=r_by_block, rotations=rotations
    )


__all__ = [
    "IdentificationInfo",
    "identify_signs",
    "identify_anchor_triangular",
    "align_procrustes",
]
