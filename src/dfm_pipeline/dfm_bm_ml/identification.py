from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np

from .state_builder import BMParams


@dataclass(frozen=True)
class IdentificationInfo:
    mode: str
    signs: np.ndarray
    anchor_indices: np.ndarray


def _copy_params_with_signs(
    params: BMParams,
    *,
    r_by_block: Sequence[int],
    signs: np.ndarray,
) -> BMParams:
    signs = np.asarray(signs, dtype=float).reshape(-1)
    r_by_block = tuple(int(r) for r in r_by_block)
    if signs.shape[0] != sum(r_by_block):
        raise ValueError("sign vector length must equal total number of factors.")
    if np.any(~np.isin(signs, [-1.0, 1.0])):
        raise ValueError("factor signs must be +/-1.")

    phi_new: list[list[np.ndarray]] = []
    q_new: list[np.ndarray] = []
    cursor = 0
    for b, rb in enumerate(r_by_block):
        S = np.diag(signs[cursor : cursor + rb])
        phi_new.append([S @ np.asarray(P, float) @ S for P in params.Phi_blocks[b]])
        q_new.append(S @ np.asarray(params.Q_f_blocks[b], float) @ S)
        cursor += rb

    S_all = np.diag(signs)
    return BMParams(
        Phi_blocks=phi_new,
        Q_f_blocks=q_new,
        rho_m=np.asarray(params.rho_m, float).copy(),
        sig2_m=np.asarray(params.sig2_m, float).copy(),
        rho_q=np.asarray(params.rho_q, float).copy(),
        sig2_q=np.asarray(params.sig2_q, float).copy(),
        Lambda_m=np.asarray(params.Lambda_m, float) @ S_all,
        Lambda_q=np.asarray(params.Lambda_q, float) @ S_all,
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

    If no anchors are supplied, each factor uses the monthly series with the
    largest absolute loading.  This removes arbitrary sign flips without
    changing the likelihood, fitted common component, or forecasts.
    """
    lam = np.asarray(params.Lambda_m, dtype=float)
    r_total = int(sum(int(r) for r in r_by_block))
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
    identified = _copy_params_with_signs(params, r_by_block=r_by_block, signs=signs)
    return identified, IdentificationInfo(mode="sign_anchor", signs=signs, anchor_indices=anchors)


def align_procrustes(
    params: BMParams,
    reference_lambda_m: np.ndarray,
    *,
    r_by_block: Sequence[int],
) -> BMParams:
    """Orthogonally align factors block-by-block to reference monthly loadings.

    This optional reporting utility is useful for comparing factors across
    vintages or bootstrap replications.  It does not impose a structural
    economic identification.  Cross-block rotations are deliberately excluded.
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

    phi_new: list[list[np.ndarray]] = []
    q_new: list[np.ndarray] = []
    lambda_m_parts: list[np.ndarray] = []
    lambda_q_parts: list[np.ndarray] = []
    cursor = 0
    for b, rb in enumerate(int(r) for r in r_by_block):
        H = rotations[b]
        # f_new = H' f_old; Lambda_new = Lambda_old H.
        lambda_m_parts.append(current[:, cursor : cursor + rb] @ H)
        lambda_q_parts.append(np.asarray(params.Lambda_q)[:, cursor : cursor + rb] @ H)
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


__all__ = ["IdentificationInfo", "identify_signs", "align_procrustes"]
