from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Literal

import numpy as np


@dataclass(frozen=True)
class BMDfmConfig:
    # Factors
    r_by_block: Sequence[int]                         # number of factors per block
    p: int                                            # factor VAR lags (must satisfy p<=5 for toolbox MF constraints)
    blocks: Optional[np.ndarray] = None               # shape (n_obs, n_blocks), binary; if None -> single block

    # Idiosyncratic components
    idio_ar1: bool = True
    rho_idio_init: float = 0.10

    # Mixed frequency (quarterly)
    n_quarterly: int = 1
    mm_weight_style: Literal["toolbox", "scaled"] = "toolbox"
    quarterly_meas_var_floor: float = 1e-4
    enforce_quarterly_loading_constraint: bool = True
    fix_quarterly_R: bool = True                      # toolbox effectively fixes quarterly R near zero

    # EM control
    max_iter: int = 200
    tol: float = 1e-6
    min_var: float = 1e-8
    jitter: float = 1e-12

    # Initialization
    pca_fill: Literal["mean", "ffill"] = "mean"
    standardize: bool = True

    # Numerical safeguards
    force_var_stability: bool = True
    var_stability_shrink: float = 0.98


@dataclass
class BMDfmResult:
    config: BMDfmConfig
    loglik_trace: list[float]

    # Final state-space params
    T: np.ndarray
    Q: np.ndarray
    C: np.ndarray
    R: np.ndarray
    a0: np.ndarray
    P0: np.ndarray

    # Smoothed outputs at optimum
    a_smooth: np.ndarray          # (T, m)
    P_smooth: np.ndarray          # (T, m, m)
    P_lag_smooth: np.ndarray      # (T, m, m)

    # Index helpers
    idx_factors: slice
    idx_idio_monthly: slice
    idx_idio_quarterly: slice
    f_t_idx: np.ndarray           # (r_total,) state indices for contemporaneous factors (concatenated by blocks)
    f_stack_idx: np.ndarray       # (5*r_total,) state indices for [t,t-1,t-2,t-3,t-4] factor stack
