from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence, Literal, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from .state_builder import BMParams
    from .scaling import PanelScaler


@dataclass(frozen=True)
class BMDfmConfig:
    # Factors
    r_by_block: Sequence[int]
    p: int
    blocks: Optional[np.ndarray] = None

    # Idiosyncratic components
    idio_ar1: bool = True
    rho_idio_init: float = 0.10

    # Mixed frequency (quarterly)
    n_quarterly: int = 1
    mm_weight_style: Literal["toolbox", "scaled"] = "toolbox"
    quarterly_meas_var_floor: float = 1e-4
    enforce_quarterly_loading_constraint: bool = True
    fix_quarterly_R: bool = True

    # Near-zero monthly measurement variance when idios are in the state (toolbox behavior)
    monthly_meas_var_floor: float = 1e-4

    # EM control
    max_iter: int = 200
    tol: float = 1e-6
    min_var: float = 1e-8
    jitter: float = 1e-12

    # Initialization
    pca_fill: Literal["mean", "ffill"] = "mean"

    # Scaling modes
    scaling_mode: Literal["external_frozen", "internal_per_run"] = "internal_per_run"

    # VAR stability
    force_var_stability: bool = True
    var_stability_shrink: float = 0.98

    # NEW: toolbox parity for initial state handling
    P0_mode: Literal["diffuse", "steady_state"] = "steady_state"
    update_initial_state_each_iter: bool = True


@dataclass
class BMDfmResult:
    config: BMDfmConfig
    loglik_trace: list[float]

    scaler: "PanelScaler"

    # Final state-space params
    T: np.ndarray
    Q: np.ndarray
    C: np.ndarray
    R: np.ndarray
    a0: np.ndarray
    P0: np.ndarray

    # Smoothed outputs at optimum
    a_smooth: np.ndarray
    P_smooth: np.ndarray
    P_lag_smooth: np.ndarray

    # Index helpers
    idx_factors: slice
    idx_idio_monthly: slice
    idx_idio_quarterly: slice
    f_t_idx: np.ndarray
    f_stack_idx: np.ndarray

    params_final: Optional["BMParams"] = None
