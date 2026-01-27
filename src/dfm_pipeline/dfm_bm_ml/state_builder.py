# src/dfm_pipeline/dfm_bm_ml/state_builder.py
from __future__ import annotations

"""
State-space builder for the Banbura–Modugno mixed-frequency DFM.

This file fixes a common failure mode:
    IndexError: tuple index out of range
coming from _Q_companion_block when Q_f accidentally becomes a scalar / 0-d array.

Core model logic is unchanged.
The only functional change is making _Q_companion_block robust via np.atleast_2d
(and basic shape checks), so the state-space can always be built.
"""

from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple

import numpy as np

from .steady_state import safe_sym


# -----------------------------------------------------------------------------
# Params container (imported by fast/numba fitters)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class BMParams:
    Phi_blocks: List[List[np.ndarray]]  # list over blocks; each block: list of Phi_lags
    Q_f_blocks: List[np.ndarray]        # list over blocks; each (rb, rb)
    rho_m: np.ndarray                   # (nM,)
    sig2_m: np.ndarray                  # (nM,) stationary var
    rho_q: np.ndarray                   # (nQ,)
    sig2_q: np.ndarray                  # (nQ,) stationary var
    Lambda_m: np.ndarray                # (nM, r_total)
    Lambda_q: np.ndarray                # (nQ, r_total)
    R_diag_m: np.ndarray                # (nM,)
    R_diag_q: np.ndarray                # (nQ,)


# -----------------------------------------------------------------------------
# Indices returned by build_state_space (consumed by EM code)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class StateIndex:
    # Slices in the full state vector alpha_t
    idx_factors: slice
    idx_idio_monthly: slice
    idx_idio_quarterly: slice

    # Factor selectors (used throughout EM)
    # - f_t_idx: indices of contemporaneous factors f_t within alpha_t
    # - f_stack_idx: indices of [f_t, f_{t-1}, ..., f_{t-4}] stacked, ordered lag-major
    f_t_idx: np.ndarray
    f_stack_idx: np.ndarray

    # For block-wise factor companion sub-states
    factor_companion_slices: Tuple[slice, ...]


# -----------------------------------------------------------------------------
# Mariano–Murasawa weights (5-month aggregation)
# -----------------------------------------------------------------------------
def _mm_weights(style: str) -> np.ndarray:
    """
    Returns weights w[0..4] applied to [t, t-1, t-2, t-3, t-4].
    This matches the usual Toolbox-style Mariano–Murasawa mapping.

    style:
      - "toolbox": (1,2,3,2,1)/3
      - "scaled" : same direction, different scaling (kept compatible)
    """
    if style not in {"toolbox", "scaled"}:
        raise ValueError(f"Unknown mm_style={style!r}. Expected 'toolbox' or 'scaled'.")

    w = np.array([1.0, 2.0, 3.0, 2.0, 1.0], dtype=float) / 3.0

    # "scaled" keeps compatibility with earlier codepaths that used a different
    # convention; leaving as toolbox weights is typically fine for nowcasting.
    # If you already had a different scaling elsewhere, adjust here consistently.
    if style == "scaled":
        return w

    return w


# -----------------------------------------------------------------------------
# Robust companion blocks
# -----------------------------------------------------------------------------
def _Q_companion_block(Q_f: np.ndarray, ppC: int) -> np.ndarray:
    """
    Embed factor innovation covariance Q_f into a ppC-companion state.

    State ordering for a block of size r:
        [f_t, f_{t-1}, ..., f_{t-ppC+1}]   (each is r-dim)

    Innovation hits only the first r components.
    """
    Q_f = np.atleast_2d(np.asarray(Q_f, dtype=float))

    # If Q_f somehow became 0-d (scalar), atleast_2d makes it (1,1).
    if Q_f.ndim != 2:
        raise ValueError(f"Q_f must be 2D after atleast_2d, got shape={Q_f.shape}.")

    if Q_f.shape[0] != Q_f.shape[1]:
        raise ValueError(f"Q_f must be square, got shape={Q_f.shape}.")

    r = int(Q_f.shape[0])
    m = r * int(ppC)

    Q = np.zeros((m, m), dtype=float)
    Q[:r, :r] = Q_f
    return Q


def _T_factor_companion(Phi_list: List[np.ndarray], r: int, p: int, ppC: int) -> np.ndarray:
    """
    Build ppC-companion transition for a factor VAR(p).

    alpha_t(block) = [f_t, f_{t-1}, ..., f_{t-ppC+1}]
    f_t = Phi_1 f_{t-1} + ... + Phi_p f_{t-p} + u_t

    In alpha_{t-1}, [f_{t-1}, f_{t-2}, ...] are exactly the first blocks.
    """
    m = r * ppC
    T = np.zeros((m, m), dtype=float)

    # Top row: f_t depends on [f_{t-1}, ..., f_{t-p}]
    # In alpha_{t-1}, these are located at offsets 0..(p-1)*r
    for lag in range(1, p + 1):
        Phi = np.asarray(Phi_list[lag - 1], dtype=float)
        if Phi.shape != (r, r):
            raise ValueError(f"Phi[{lag}] shape mismatch: expected {(r, r)}, got {Phi.shape}.")
        col0 = (lag - 1) * r
        T[:r, col0 : col0 + r] = Phi

    # Shift-down identity for remaining lags up to ppC
    for j in range(1, ppC):
        T[j * r : (j + 1) * r, (j - 1) * r : j * r] = np.eye(r, dtype=float)

    return T


def _T_idio_ar1(rho: float) -> np.ndarray:
    """1D AR(1) state transition."""
    return np.array([[float(rho)]], dtype=float)


def _Q_idio_ar1(sig2_stationary: float, rho: float) -> float:
    """
    Convert stationary variance sig2 to innovation variance q = sig2*(1-rho^2).
    Floors/clipping handled outside.
    """
    return float(sig2_stationary) * (1.0 - float(rho) ** 2)


def _T_quarterly_shift_register(rho: float) -> np.ndarray:
    """
    5D shift register:
        u_t   = rho*u_{t-1} + eta_t
        u_{t-1} becomes lag1, etc.
    """
    T = np.zeros((5, 5), dtype=float)
    T[0, 0] = float(rho)
    for j in range(1, 5):
        T[j, j - 1] = 1.0
    return T


def _Q_quarterly_shift_register(sig2_stationary: float, rho: float) -> np.ndarray:
    """Innovation variance only on first component."""
    q = _Q_idio_ar1(sig2_stationary, rho)
    Q = np.zeros((5, 5), dtype=float)
    Q[0, 0] = q
    return Q


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------
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
    jitter: float,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, StateIndex]:
    """
    Build (T, Q, C, R, a0, P0, idx) for mixed-frequency BM-DFM.

    State alpha_t is ordered as:
        [ factor_blocks (each rb*ppC),
          monthly_idio (nM),
          quarterly_idio (nQ*5) ]

    Measurement Y_t is ordered as:
        [ monthly panel (nM),
          quarterly target(s) (nQ) ]
    """
    r_by_block = tuple(int(x) for x in r_by_block)
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be positive.")
    if int(ppC) != 5:
        # your codebase assumes ppC=5 almost everywhere (MM weights)
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
    idx_idio_monthly = slice(cursor, cursor + int(nM))
    cursor += int(nM)
    idx_idio_quarterly = slice(cursor, cursor + int(nQ) * 5)
    cursor += int(nQ) * 5

    m = int(cursor)         # state dim
    n = int(nM + nQ)        # observation dim

    Tm = np.zeros((m, m), dtype=float)
    Qm = np.zeros((m, m), dtype=float)

    # Factor transitions/covariances (block diagonal)
    for b, rb in enumerate(r_by_block):
        rb = int(rb)
        sl = factor_slices[b]
        if rb == 0:
            continue

        Phi_list = params.Phi_blocks[b]
        if p == 0:
            Phi_list = []
        else:
            if len(Phi_list) != p:
                raise ValueError(f"Block {b}: expected {p} Phi matrices, got {len(Phi_list)}.")

        T_block = _T_factor_companion(Phi_list, r=rb, p=int(p), ppC=int(ppC))
        Q_block = _Q_companion_block(params.Q_f_blocks[b], ppC=int(ppC))

        Tm[sl, sl] = T_block
        Qm[sl, sl] = Q_block

    # Monthly idiosyncratic AR(1)
    for i in range(int(nM)):
        s = idx_idio_monthly.start + i
        rho = float(params.rho_m[i])
        sig2 = float(params.sig2_m[i])

        Tm[s, s] = rho
        Qm[s, s] = _Q_idio_ar1(sig2, rho)

    # Quarterly idiosyncratic shift registers (5 states each)
    for j in range(int(nQ)):
        s0 = idx_idio_quarterly.start + 5 * j
        rho = float(params.rho_q[j])
        sig2 = float(params.sig2_q[j])

        Tq = _T_quarterly_shift_register(rho)
        Qq = _Q_quarterly_shift_register(sig2, rho)

        Tm[s0 : s0 + 5, s0 : s0 + 5] = Tq
        Qm[s0 : s0 + 5, s0 : s0 + 5] = Qq

    # Small jitter on Q for numerical stability (optional)
    if float(jitter) > 0.0:
        Qm = safe_sym(Qm) + float(jitter) * np.eye(m, dtype=float)
    else:
        Qm = safe_sym(Qm)

    # --------------------------
    # Build measurement C and R
    # --------------------------
    C = np.zeros((n, m), dtype=float)

    # f_t indices: first lag (within each block slice, first rb entries)
    f_t_idx_list: List[int] = []
    # f_stack_idx lag-major ordering across blocks:
    # [lag0 all factors, lag1 all factors, ..., lag4 all factors]
    f_stack_idx_list: List[int] = []

    # For each block, collect its base indices per lag
    block_factor_offsets: List[List[int]] = []
    for b, rb in enumerate(r_by_block):
        rb = int(rb)
        sl = factor_slices[b]
        base = sl.start
        offs_per_lag = []
        for lag in range(ppC):
            offs_per_lag.append(base + lag * rb)
        block_factor_offsets.append(offs_per_lag)

        # lag0 indices for this block
        for k in range(rb):
            f_t_idx_list.append(base + k)

    # Build f_stack_idx in lag-major order across blocks
    for lag in range(ppC):
        for b, rb in enumerate(r_by_block):
            rb = int(rb)
            base_lag = block_factor_offsets[b][lag]
            for k in range(rb):
                f_stack_idx_list.append(base_lag + k)

    f_t_idx = np.array(f_t_idx_list, dtype=int)
    f_stack_idx = np.array(f_stack_idx_list, dtype=int)

    # Monthly measurement rows
    # y_i,t = Lambda_m[i,:] f_t + e_i,t
    # where e_i,t is the monthly idio state for series i
    for i in range(int(nM)):
        C[i, f_t_idx] = params.Lambda_m[i, :].astype(float)
        C[i, idx_idio_monthly.start + i] = 1.0

    # Quarterly measurement rows
    # y_q,t = sum_{lag=0..4} w[lag] * Lambda_q f_{t-lag} + sum_{lag} w[lag] * u_{t-lag}
    w = _mm_weights(mm_style).astype(float)
    for j in range(int(nQ)):
        row = int(nM) + j

        # Factor part: for each lag, apply w[lag] * Lambda_q
        pos = 0
        for lag in range(ppC):
            idx_lag = f_stack_idx[pos : pos + r_total]
            C[row, idx_lag] = w[lag] * params.Lambda_q[j, :].astype(float)
            pos += r_total

        # Quarterly idio part: 5-state shift register for this quarterly series
        s0 = idx_idio_quarterly.start + 5 * j
        C[row, s0 : s0 + 5] = w

    # Measurement noise R (diagonal)
    R_diag = np.zeros((n,), dtype=float)
    R_diag[: int(nM)] = params.R_diag_m.astype(float)
    if int(nQ) > 0:
        R_diag[int(nM) :] = np.maximum(params.R_diag_q.astype(float), float(quarterly_meas_var_floor))
    R = np.diag(R_diag)

    # --------------------------
    # Initial state (simple)
    # --------------------------
    a0 = np.zeros((m,), dtype=float)
    P0 = np.eye(m, dtype=float) * 1e4

    idx = StateIndex(
        idx_factors=idx_factors,
        idx_idio_monthly=idx_idio_monthly,
        idx_idio_quarterly=idx_idio_quarterly,
        f_t_idx=f_t_idx,
        f_stack_idx=f_stack_idx,
        factor_companion_slices=tuple(factor_slices),
    )

    return Tm, Qm, C, R, a0, P0, idx
