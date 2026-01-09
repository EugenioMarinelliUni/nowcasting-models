from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Sequence

import numpy as np

from .constraints import mm_weights
from .steady_state import steady_state_cov, safe_sym


@dataclass
class BMStateIndex:
    idx_factors: slice
    idx_idio_monthly: slice
    idx_idio_quarterly: slice

    r_by_block: Tuple[int, ...]
    ppC: int
    nM: int
    nQ: int

    # Indices into the full state vector:
    # contemporaneous factors concatenated by blocks
    f_t_idx: np.ndarray          # (r_total,)
    # factor stack [t,t-1,t-2,t-3,t-4] each is concatenated by blocks
    f_stack_idx: np.ndarray      # (5*r_total,)

    # Companion slices per block (full block companion, size r_b*ppC), within factor region
    factor_companion_slices: Tuple[slice, ...]


@dataclass
class BMParams:
    # Block-wise factor VAR(p): Phi_blocks[b][lag] is (r_b, r_b)
    Phi_blocks: list[list[np.ndarray]]
    # Block-wise factor innovation covariance: Q_f_blocks[b] is (r_b, r_b)
    Q_f_blocks: list[np.ndarray]

    # Monthly idio (diagonal AR(1))
    rho_m: np.ndarray            # (nM,)
    sig2_m: np.ndarray           # (nM,) stationary variance

    # Quarterly idio (per quarterly series), only first state AR(1) with shift register
    rho_q: np.ndarray            # (nQ,)
    sig2_q: np.ndarray           # (nQ,) stationary variance for the first state

    # Loadings
    Lambda_m: np.ndarray         # (nM, r_total) factors concatenated by blocks
    Lambda_q: np.ndarray         # (nQ, r_total) base loadings (per factor), constraints expand across lags via MM weights

    # Measurement noise (diagonal)
    R_diag_m: np.ndarray         # (nM,)
    R_diag_q: np.ndarray         # (nQ,)


def _companion_from_var_block(Phi_block: list[np.ndarray], ppC: int) -> np.ndarray:
    r = Phi_block[0].shape[0]
    p = len(Phi_block)
    A = np.zeros((r * ppC, r * ppC), dtype=float)
    for lag in range(p):
        A[:r, lag * r:(lag + 1) * r] = Phi_block[lag]
    for k in range(1, ppC):
        A[k * r:(k + 1) * r, (k - 1) * r:k * r] = np.eye(r, dtype=float)
    return A


def _Q_companion_block(Q_f: np.ndarray, ppC: int) -> np.ndarray:
    r = Q_f.shape[0]
    Q = np.zeros((r * ppC, r * ppC), dtype=float)
    Q[:r, :r] = Q_f
    return Q


def _quarterly_idio_transition(rho: float) -> np.ndarray:
    B = np.zeros((5, 5), dtype=float)
    B[0, 0] = rho
    B[1:, :-1] = np.eye(4, dtype=float)
    return B


def _quarterly_idio_Q(sig2: float, rho: float) -> np.ndarray:
    Q = np.zeros((5, 5), dtype=float)
    Q[0, 0] = (1.0 - rho * rho) * sig2
    return Q


def build_state_space(
    params: BMParams,
    nM: int,
    nQ: int,
    r_by_block: Sequence[int],
    p: int,
    ppC: int,
    mm_style: str,
    quarterly_meas_var_floor: float,
    jitter: float,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, BMStateIndex]:
    """
    Build toolbox-style MF-DFM state space with block-wise factor dynamics.

    State vector ordering:
      [ block1 factor companion (r1*ppC),
        block2 factor companion (r2*ppC),
        ...
        monthly idio states (nM),
        quarterly idio blocks (5*nQ) ]

    Observations:
      [ monthly series (nM),
        quarterly series (nQ) ]  (quarterly entries should be NaN in non-quarter-end months)
    """
    r_by_block = tuple(int(x) for x in r_by_block)
    if sum(r_by_block) <= 0:
        raise ValueError("r_by_block must sum to a positive integer.")
    if p < 0 or p > ppC:
        raise ValueError("p must satisfy 0 <= p <= ppC.")
    if ppC != 5:
        raise ValueError("Toolbox-parity MF constraints assume ppC=5.")

    # Dimensions
    r_total = int(sum(r_by_block))
    m_f = int(sum(rb * ppC for rb in r_by_block))
    m_m = int(nM)
    m_q = int(5 * nQ)
    m = m_f + m_m + m_q

    idx_f = slice(0, m_f)
    idx_m = slice(m_f, m_f + m_m)
    idx_q = slice(m_f + m_m, m)

    # Build factor transition and noise as block diagonal (block-wise VAR)
    T = np.zeros((m, m), dtype=float)
    Q = np.zeros((m, m), dtype=float)

    factor_companion_slices: list[slice] = []
    cursor = 0
    for b, rb in enumerate(r_by_block):
        if rb == 0:
            continue
        A_b = _companion_from_var_block(params.Phi_blocks[b], ppC)
        Q_b = _Q_companion_block(params.Q_f_blocks[b], ppC)
        sl = slice(cursor, cursor + rb * ppC)
        factor_companion_slices.append(sl)
        T[sl, sl] = A_b
        Q[sl, sl] = Q_b
        cursor += rb * ppC

    # Monthly idio AR(1)
    T[idx_m, idx_m] = np.diag(params.rho_m.astype(float))
    Q[idx_m, idx_m] = np.diag(((1.0 - params.rho_m ** 2) * params.sig2_m).astype(float))

    # Quarterly idio blocks
    for j in range(nQ):
        B = _quarterly_idio_transition(float(params.rho_q[j]))
        Qj = _quarterly_idio_Q(float(params.sig2_q[j]), float(params.rho_q[j]))
        sl = slice(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1))
        T[sl, sl] = B
        Q[sl, sl] = Qj

    # Measurement matrix
    n = nM + nQ
    C = np.zeros((n, m), dtype=float)

    # Precompute state indices for contemporaneous factors and 5-lag stack
    f_t_idx_list: list[int] = []
    f_stack_idx_list: list[int] = []

    # For each lag l=0..4, collect factor indices in the full state, concatenating blocks
    for lag in range(5):
        for b, rb in enumerate(r_by_block):
            if rb == 0:
                continue
            # start of block companion in state
            block_start = factor_companion_slices[b].start
            # within block companion, lag offset
            lag_start = block_start + lag * rb
            lag_idx = list(range(lag_start, lag_start + rb))
            if lag == 0:
                f_t_idx_list.extend(lag_idx)
            f_stack_idx_list.extend(lag_idx)

    f_t_idx = np.array(f_t_idx_list, dtype=int)
    f_stack_idx = np.array(f_stack_idx_list, dtype=int)

    # Monthly series measurement: y_i = lambda_i' f_t + e_i
    # Loadings go into scattered factor indices f_t_idx
    C[:nM, f_t_idx] = params.Lambda_m
    C[:nM, idx_m] = np.eye(nM, dtype=float)

    # Quarterly measurement: y_q = sum_{k=0}^4 w_k * (lambda_q' f_{t-k}) + sum_{k=0}^4 w_k * qidio_k
    w = mm_weights(mm_style)
    for j in range(nQ):
        row = nM + j
        # Factor stack coefficients are block-concatenated per lag; multiply each lag block by w[lag]
        # We fill by lag using f_stack_idx structure (lag-major).
        pos = 0
        for lag in range(5):
            # indices of factors at this lag in state
            lag_idx = f_stack_idx[pos:pos + r_total]
            C[row, lag_idx] = w[lag] * params.Lambda_q[j, :]
            pos += r_total
        # quarterly idio weights into its 5-state block
        C[row, idx_q.start + 5 * j: idx_q.start + 5 * (j + 1)] = w

    # Measurement noise (diagonal)
    R_diag = np.concatenate([params.R_diag_m, np.maximum(params.R_diag_q, quarterly_meas_var_floor)]).astype(float)
    R = np.diag(R_diag)

    # Initial mean
    a0 = np.zeros((m,), dtype=float)

    # Initial covariance: steady-state per block companion + stationary idio variances
    P0 = np.zeros((m, m), dtype=float)

    # Factor blocks: solve Lyapunov for each block companion
    for b, sl in enumerate(factor_companion_slices):
        A_b = T[sl, sl]
        Q_b = Q[sl, sl]
        V_b = steady_state_cov(A_b, Q_b, jitter=jitter)
        P0[sl, sl] = V_b

    # Monthly idio states: stationary variance is sig2_m by construction
    P0[idx_m, idx_m] = np.diag(params.sig2_m.astype(float))

    # Quarterly idio: steady-state for each 5x5 block
    for j in range(nQ):
        sl = slice(idx_q.start + 5 * j, idx_q.start + 5 * (j + 1))
        B = T[sl, sl]
        Qj = Q[sl, sl]
        Vq = steady_state_cov(B, Qj, jitter=jitter)
        P0[sl, sl] = Vq

    P0 = safe_sym(P0)

    idx = BMStateIndex(
        idx_factors=idx_f,
        idx_idio_monthly=idx_m,
        idx_idio_quarterly=idx_q,
        r_by_block=r_by_block,
        ppC=ppC,
        nM=nM,
        nQ=nQ,
        f_t_idx=f_t_idx,
        f_stack_idx=f_stack_idx,
        factor_companion_slices=tuple(factor_companion_slices),
    )
    return T, Q, C, R, a0, P0, idx
