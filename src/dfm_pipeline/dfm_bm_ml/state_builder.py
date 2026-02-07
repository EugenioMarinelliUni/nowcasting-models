from __future__ import annotations

"""
State-space builder for the Banbura–Modugno mixed-frequency DFM.

Implements:
- Consistent Mariano–Murasawa weights via constraints.mm_weights.
- Explicit idio specification switch (idio_ar1):
    * True  -> monthly idios are AR(1) states in the state vector.
    * False -> no monthly idio states; monthly noise is only in measurement R.
- Initial state covariance mode (P0_mode):
    * diffuse      -> P0 = 1e4 * I
    * steady_state -> blockwise unconditional covariance via discrete Lyapunov.
- Optional overrides for a0/P0 (for toolbox-style per-EM-iteration initial moment updates).
"""

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import numpy as np
from scipy.linalg import solve_discrete_lyapunov

from .constraints import mm_weights
from .steady_state import safe_sym


@dataclass(frozen=True)
class BMParams:
    Phi_blocks: List[List[np.ndarray]]  # per block: list of Phi_lags (each rb x rb)
    Q_f_blocks: List[np.ndarray]        # per block: (rb x rb)
    rho_m: np.ndarray                   # (nM,)
    sig2_m: np.ndarray                  # (nM,) stationary variance
    rho_q: np.ndarray                   # (nQ,)
    sig2_q: np.ndarray                  # (nQ,) stationary variance
    Lambda_m: np.ndarray                # (nM, r_total)
    Lambda_q: np.ndarray                # (nQ, r_total) base (lag-0) loadings
    R_diag_m: np.ndarray                # (nM,)
    R_diag_q: np.ndarray                # (nQ,)


@dataclass(frozen=True)
class StateIndex:
    idx_factors: slice
    idx_idio_monthly: slice
    idx_idio_quarterly: slice

    f_t_idx: np.ndarray         # (r_total,) indices of contemporaneous factor states
    f_stack_idx: np.ndarray     # (5*r_total,) indices of stacked factor states [t..t-4] in that order

    factor_companion_slices: Tuple[slice, ...]


def _Q_idio_innov_from_stationary(sig2: float, rho: float) -> float:
    # For stationary AR(1): sig2 = q / (1-rho^2) => q = sig2*(1-rho^2)
    return float(sig2) * float(max(1.0 - rho * rho, 0.0))


def _T_quarterly_shift_register(rho: float) -> np.ndarray:
    Tq = np.zeros((5, 5), dtype=float)
    Tq[0, 0] = float(rho)
    Tq[1, 0] = 1.0
    Tq[2, 1] = 1.0
    Tq[3, 2] = 1.0
    Tq[4, 3] = 1.0
    return Tq


def _Q_quarterly_shift_register(sig2: float, rho: float) -> np.ndarray:
    Qq = np.zeros((5, 5), dtype=float)
    Qq[0, 0] = _Q_idio_innov_from_stationary(sig2, rho)
    return Qq


def _T_factor_companion(Phi_list: List[np.ndarray], r: int, p: int, ppC: int) -> np.ndarray:
    r = int(r)
    p = int(p)
    ppC = int(ppC)

    T = np.zeros((r * ppC, r * ppC), dtype=float)

    # Top row: VAR(p)
    for lag in range(p):
        Phi = np.asarray(Phi_list[lag], dtype=float)
        if Phi.shape != (r, r):
            raise ValueError(f"Phi[{lag}] shape {Phi.shape} != {(r, r)}")
        T[:r, lag * r:(lag + 1) * r] = Phi

    # Shifts
    for lag in range(1, ppC):
        T[lag * r:(lag + 1) * r, (lag - 1) * r:lag * r] = np.eye(r, dtype=float)

    return T


def _Q_companion_block(Q_f: np.ndarray, ppC: int) -> np.ndarray:
    Q_f = np.atleast_2d(np.asarray(Q_f, dtype=float))
    if Q_f.ndim != 2 or Q_f.shape[0] != Q_f.shape[1]:
        raise ValueError(f"Q_f must be square, got {Q_f.shape}")
    r = int(Q_f.shape[0])
    Qb = np.zeros((r * int(ppC), r * int(ppC)), dtype=float)
    Qb[:r, :r] = Q_f
    return Qb


def _steady_state_block(A: np.ndarray, Q: np.ndarray, *, jitter: float) -> np.ndarray:
    A = np.asarray(A, dtype=float)
    Q = np.asarray(Q, dtype=float)
    if jitter > 0.0:
        Q = Q + float(jitter) * np.eye(Q.shape[0], dtype=float)
    P = solve_discrete_lyapunov(A, Q)
    return safe_sym(P)


def build_state_space(
    *,
    params: BMParams,
    nM: int,
    nQ: int,
    r_by_block: Sequence[int],
    p: int,
    ppC: int,
    mm_style: str,
    quarterly_meas_var_floor: float,
    idio_ar1: bool,
    jitter: float,
    P0_mode: str = "diffuse",
    a0_override: Optional[np.ndarray] = None,
    P0_override: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, StateIndex]:
    r_by_block = tuple(int(x) for x in r_by_block)
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be positive")
    if int(ppC) != 5:
        raise ValueError(f"ppC must be 5, got {ppC}")

    # Build factor block slices
    factor_slices: List[slice] = []
    cursor = 0
    for rb in r_by_block:
        sl = slice(cursor, cursor + rb * int(ppC))
        factor_slices.append(sl)
        cursor += rb * int(ppC)

    idx_factors = slice(0, cursor)

    if bool(idio_ar1):
        idx_idio_monthly = slice(cursor, cursor + int(nM))
        cursor += int(nM)
    else:
        idx_idio_monthly = slice(cursor, cursor)

    idx_idio_quarterly = slice(cursor, cursor + int(nQ) * 5)
    cursor += int(nQ) * 5

    m = int(cursor)
    n = int(nM + nQ)

    Tm = np.zeros((m, m), dtype=float)
    Qm = np.zeros((m, m), dtype=float)

    # Factors
    for b, rb in enumerate(r_by_block):
        rb = int(rb)
        if rb == 0:
            continue
        sl = factor_slices[b]
        Phi_list = params.Phi_blocks[b] if int(p) > 0 else []
        if int(p) > 0 and len(Phi_list) != int(p):
            raise ValueError(f"Block {b}: expected {p} Phi matrices, got {len(Phi_list)}")
        T_block = _T_factor_companion(Phi_list, r=rb, p=int(p), ppC=int(ppC))
        Q_block = _Q_companion_block(params.Q_f_blocks[b], ppC=int(ppC))
        Tm[sl, sl] = T_block
        Qm[sl, sl] = Q_block

    # Monthly idios (optional)
    if bool(idio_ar1):
        for i in range(int(nM)):
            s = idx_idio_monthly.start + i
            rho = float(params.rho_m[i])
            sig2 = float(params.sig2_m[i])
            Tm[s, s] = rho
            Qm[s, s] = _Q_idio_innov_from_stationary(sig2, rho)

    # Quarterly idios
    for j in range(int(nQ)):
        s0 = idx_idio_quarterly.start + 5 * j
        rho = float(params.rho_q[j])
        sig2 = float(params.sig2_q[j])
        Tq = _T_quarterly_shift_register(rho)
        Qq = _Q_quarterly_shift_register(sig2, rho)
        Tm[s0:s0 + 5, s0:s0 + 5] = Tq
        Qm[s0:s0 + 5, s0:s0 + 5] = Qq

    # Symmetrize Q
    if float(jitter) > 0.0:
        Qm = safe_sym(Qm) + float(jitter) * np.eye(m, dtype=float)
    else:
        Qm = safe_sym(Qm)

    # Factor index maps
    f_t_idx_list: List[int] = []
    for b, rb in enumerate(r_by_block):
        rb = int(rb)
        sl = factor_slices[b]
        base = int(sl.start)
        for k in range(rb):
            f_t_idx_list.append(base + k)
    f_t_idx = np.array(f_t_idx_list, dtype=int)

    # Stack idx: [t, t-1, t-2, t-3, t-4] each includes r_total entries in block order
    f_stack_idx_list: List[int] = []
    for lag in range(int(ppC)):
        for b, rb in enumerate(r_by_block):
            rb = int(rb)
            sl = factor_slices[b]
            base_lag = int(sl.start) + lag * rb
            for k in range(rb):
                f_stack_idx_list.append(base_lag + k)
    f_stack_idx = np.array(f_stack_idx_list, dtype=int)

    # Measurement matrix C
    C = np.zeros((n, m), dtype=float)

    # Monthly: x_i,t = Lambda_m[i]*f_t + idio_i,t (if idio_ar1)
    for i in range(int(nM)):
        C[i, f_t_idx] = params.Lambda_m[i, :].astype(float)
        if bool(idio_ar1):
            C[i, idx_idio_monthly.start + i] = 1.0

    # Quarterly: y_j,t = sum_l w_l * Lambda_q[j]*f_{t-l} + w' u_{j,t..t-4} + e
    w = mm_weights(mm_style).astype(float)
    if w.shape != (5,):
        raise ValueError("mm_weights must return shape (5,)")

    for j in range(int(nQ)):
        row = int(nM) + j
        pos = 0
        for lag in range(5):
            lag_idx = f_stack_idx[pos:pos + r_total]
            C[row, lag_idx] = w[lag] * params.Lambda_q[j, :].astype(float)
            pos += r_total
        s0 = idx_idio_quarterly.start + 5 * j
        C[row, s0:s0 + 5] = w

    # R (diagonal)
    R_diag = np.zeros((n,), dtype=float)
    R_diag[:int(nM)] = params.R_diag_m.astype(float)
    if int(nQ) > 0:
        R_diag[int(nM):] = np.maximum(params.R_diag_q.astype(float), float(quarterly_meas_var_floor))
    R = np.diag(R_diag)

    # a0, P0
    a0 = np.zeros((m,), dtype=float) if a0_override is None else np.asarray(a0_override, dtype=float).copy()
    if a0.shape != (m,):
        raise ValueError(f"a0_override must have shape {(m,)}, got {a0.shape}")

    if P0_override is not None:
        P0 = np.asarray(P0_override, dtype=float).copy()
        if P0.shape != (m, m):
            raise ValueError(f"P0_override must have shape {(m, m)}, got {P0.shape}")
        P0 = safe_sym(P0)
    else:
        if P0_mode == "diffuse":
            P0 = np.eye(m, dtype=float) * 1e4
        elif P0_mode == "steady_state":
            P0 = np.zeros((m, m), dtype=float)

            # Factor blocks
            for sl in factor_slices:
                A = Tm[sl, sl]
                Q = Qm[sl, sl]
                try:
                    P0[sl, sl] = _steady_state_block(A, Q, jitter=float(jitter))
                except Exception:
                    P0[sl, sl] = np.eye(A.shape[0], dtype=float) * 1e4

            # Monthly idios: stationary variances on diagonal
            if bool(idio_ar1):
                for i in range(int(nM)):
                    s = idx_idio_monthly.start + i
                    P0[s, s] = float(max(params.sig2_m[i], 0.0))

            # Quarterly idios: 5x5 Lyapunov per series
            for j in range(int(nQ)):
                s0 = idx_idio_quarterly.start + 5 * j
                A = Tm[s0:s0 + 5, s0:s0 + 5]
                Q = Qm[s0:s0 + 5, s0:s0 + 5]
                try:
                    P0[s0:s0 + 5, s0:s0 + 5] = _steady_state_block(A, Q, jitter=float(jitter))
                except Exception:
                    P0[s0:s0 + 5, s0:s0 + 5] = np.eye(5, dtype=float) * 1e4

            P0 = safe_sym(P0)
        else:
            raise ValueError(f"Unknown P0_mode={P0_mode!r}")

    idx = StateIndex(
        idx_factors=idx_factors,
        idx_idio_monthly=idx_idio_monthly,
        idx_idio_quarterly=idx_idio_quarterly,
        f_t_idx=f_t_idx,
        f_stack_idx=f_stack_idx,
        factor_companion_slices=tuple(factor_slices),
    )
    return Tm, Qm, C, R, a0, P0, idx
