# src/dfm_pipeline/dfm_bm_ml/state_builder.py
from __future__ import annotations

"""
State-space builder for the Banbura–Modugno mixed-frequency DFM.

Implements an idiosyncratic specification switch aligned with the toolbox:

- idio_ar1=True:
    monthly idiosyncratic components are explicit AR(1) states (one per monthly series),
    and monthly measurement noise is treated as near-zero (set/floored elsewhere).

- idio_ar1=False:
    monthly idiosyncratic components are iid measurement noise (no monthly idio states).
"""

from dataclasses import dataclass
from typing import List, Sequence, Tuple

import numpy as np

from .constraints import mm_weights
from .steady_state import safe_sym


@dataclass(frozen=True)
class BMParams:
    Phi_blocks: List[List[np.ndarray]]  # list over blocks; each block: list of Phi_lags
    Q_f_blocks: List[np.ndarray]        # list over blocks; each (rb, rb)
    rho_m: np.ndarray                   # (nM,)   (unused if idio_ar1=False)
    sig2_m: np.ndarray                  # (nM,)   stationary var (unused if idio_ar1=False)
    rho_q: np.ndarray                   # (nQ,)
    sig2_q: np.ndarray                  # (nQ,)   stationary var
    Lambda_m: np.ndarray                # (nM, r_total)
    Lambda_q: np.ndarray                # (nQ, r_total)
    R_diag_m: np.ndarray                # (nM,)
    R_diag_q: np.ndarray                # (nQ,)


@dataclass(frozen=True)
class StateIndex:
    idx_factors: slice
    idx_idio_monthly: slice   # may be empty if idio_ar1=False
    idx_idio_quarterly: slice

    f_t_idx: np.ndarray
    f_stack_idx: np.ndarray

    factor_companion_slices: Tuple[slice, ...]


def _T_companion_block(Phi_lags: List[np.ndarray], rb: int, ppC: int) -> np.ndarray:
    m = rb * ppC
    T = np.zeros((m, m), dtype=float)
    for lag, Phi in enumerate(Phi_lags, start=1):
        T[0:rb, (lag - 1) * rb: lag * rb] = Phi
    for k in range(1, ppC):
        T[k * rb: (k + 1) * rb, (k - 1) * rb: k * rb] = np.eye(rb, dtype=float)
    return T


def _Q_companion_block(Q_f: np.ndarray, rb: int, ppC: int) -> np.ndarray:
    """
    Build Q for a companion factor block; innovations hit only contemporaneous factors.

    Robust to accidental scalar / 0-d arrays by forcing at least 2d and checking shape.
    """
    Q_f = np.atleast_2d(np.asarray(Q_f, dtype=float))
    if Q_f.shape != (rb, rb):
        raise ValueError(f"Q_f must have shape ({rb},{rb}), got {Q_f.shape}")
    m = rb * ppC
    Q = np.zeros((m, m), dtype=float)
    Q[0:rb, 0:rb] = Q_f
    return Q


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
    monthly_meas_var_floor: float,
    idio_ar1: bool,
    jitter: float,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, StateIndex]:
    """
    Build (T, Q, C, R, a0, P0, idx) for mixed-frequency BM-DFM.

    State alpha_t ordering:
        [ factor_blocks (each rb*ppC),
          monthly_idio (nM)  (only if idio_ar1=True),
          quarterly_idio (nQ*5) ]

    Measurement Y_t ordering:
        [ monthly panel (nM),
          quarterly target(s) (nQ) ]
    """
    r_by_block = tuple(int(x) for x in r_by_block)
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be positive.")
    if int(ppC) != 5:
        raise ValueError(f"ppC must be 5 for this BM implementation, got {ppC}.")

    # --------------------------
    # Build factor blocks in state
    # --------------------------
    factor_slices: List[slice] = []
    cursor = 0
    for rb in r_by_block:
        rb = int(rb)
        sl = slice(cursor, cursor + rb * ppC)
        factor_slices.append(sl)
        cursor += rb * ppC

    idx_factors = slice(0, cursor)

    if bool(idio_ar1):
        idx_idio_monthly = slice(cursor, cursor + int(nM))
        cursor += int(nM)
    else:
        idx_idio_monthly = slice(cursor, cursor)  # empty

    idx_idio_quarterly = slice(cursor, cursor + int(nQ) * 5)
    cursor += int(nQ) * 5

    m = int(cursor)            # state dim
    n = int(nM + nQ)           # observation dim

    Tm = np.zeros((m, m), dtype=float)
    Qm = np.zeros((m, m), dtype=float)

    # --------------------------
    # Factor transitions/covariances (block diagonal)
    # --------------------------
    for b, rb in enumerate(r_by_block):
        rb = int(rb)
        sl = factor_slices[b]
        Phi_lags = params.Phi_blocks[b][: int(p)]
        Tb = _T_companion_block(Phi_lags, rb=rb, ppC=ppC)
        Qb = _Q_companion_block(params.Q_f_blocks[b], rb=rb, ppC=ppC)
        Tm[sl, sl] = Tb
        Qm[sl, sl] = Qb

    # --------------------------
    # Monthly idiosyncratic AR(1) states (optional)
    # --------------------------
    if bool(idio_ar1):
        for i in range(int(nM)):
            s = idx_idio_monthly.start + i
            rho = float(params.rho_m[i])
            sig2 = float(params.sig2_m[i])
            Tm[s, s] = rho
            Qm[s, s] = max((1.0 - rho * rho) * sig2, 0.0)

    # --------------------------
    # Quarterly idio 5-state shift register
    # --------------------------
    for j in range(int(nQ)):
        base = idx_idio_quarterly.start + 5 * j
        rho = float(params.rho_q[j])
        sig2 = float(params.sig2_q[j])
        # shift
        for k in range(1, 5):
            Tm[base + k, base + k - 1] = 1.0
        # AR(1) on first
        Tm[base + 0, base + 0] = rho
        Qm[base + 0, base + 0] = max((1.0 - rho * rho) * sig2, 0.0)

    Qm = safe_sym(Qm)

    # --------------------------
    # Measurement matrix
    # --------------------------
    C = np.zeros((n, m), dtype=float)

    # indices of contemporaneous factors within alpha_t
    f_t_idx: List[int] = []
    for b, rb in enumerate(r_by_block):
        rb = int(rb)
        sl = factor_slices[b]
        f_t_idx.extend(list(range(sl.start, sl.start + rb)))
    f_t_idx_arr = np.array(f_t_idx, dtype=int)

    # indices of [f_t, f_{t-1}, ..., f_{t-4}] stacked (lag-major)
    f_stack_idx: List[int] = []
    for lag in range(5):
        for b, rb in enumerate(r_by_block):
            rb = int(rb)
            sl = factor_slices[b]
            start = sl.start + lag * rb
            f_stack_idx.extend(list(range(start, start + rb)))
    f_stack_idx_arr = np.array(f_stack_idx, dtype=int)

    # monthly rows
    for i in range(int(nM)):
        C[i, f_t_idx_arr] = params.Lambda_m[i, :]
        if bool(idio_ar1):
            C[i, idx_idio_monthly.start + i] = 1.0

    # quarterly rows
    w = mm_weights(mm_style)
    for j in range(int(nQ)):
        row = int(nM) + j
        pos = 0
        for lag in range(5):
            lag_idx = f_stack_idx_arr[pos: pos + r_total]
            C[row, lag_idx] = w[lag] * params.Lambda_q[j, :]
            pos += r_total
        s0 = idx_idio_quarterly.start + 5 * j
        C[row, s0: s0 + 5] = w

    # --------------------------
    # Measurement noise covariance (diagonal)
    # --------------------------
    R_diag = np.zeros((n,), dtype=float)

    if bool(idio_ar1):
        R_diag[: int(nM)] = np.maximum(params.R_diag_m.astype(float), float(monthly_meas_var_floor))
    else:
        R_diag[: int(nM)] = np.maximum(params.R_diag_m.astype(float), 0.0)

    if int(nQ) > 0:
        R_diag[int(nM):] = np.maximum(params.R_diag_q.astype(float), float(quarterly_meas_var_floor))

    R = np.diag(R_diag)

    # --------------------------
    # Initial state moments (diffuse default)
    # --------------------------
    a0 = np.zeros((m,), dtype=float)
    P0 = np.eye(m, dtype=float) * 1e4

    if float(jitter) > 0.0:
        P0 = P0 + float(jitter) * np.eye(m, dtype=float)

    idx = StateIndex(
        idx_factors=idx_factors,
        idx_idio_monthly=idx_idio_monthly,
        idx_idio_quarterly=idx_idio_quarterly,
        f_t_idx=f_t_idx_arr,
        f_stack_idx=f_stack_idx_arr,
        factor_companion_slices=tuple(factor_slices),
    )

    return Tm, Qm, C, R, a0, P0, idx
