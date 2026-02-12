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
    blocks: Optional[np.ndarray] = None  # (nM+nQ, n_blocks) with 0/1 entries, or None

    # Idiosyncratic components
    idio_ar1: bool = True
    rho_idio_init: float = 0.10

    # Mixed frequency (quarterly)
    n_quarterly: int = 1
    mm_weight_style: Literal["toolbox", "scaled"] = "toolbox"
    quarterly_meas_var_floor: float = 1e-4
    enforce_quarterly_loading_constraint: bool = True
    fix_quarterly_R: bool = True

    # Toolbox behavior: near-zero monthly measurement variance when idios are in the state
    monthly_meas_var_floor: float = 1e-4

    # EM control
    max_iter: int = 200
    convergence_mode: Literal["abs", "toolbox_rel"] = "toolbox_rel"
    # tol meaning depends on convergence_mode:
    # - abs: |LL_t - LL_{t-1}| < tol
    # - toolbox_rel: |LL_t - LL_{t-1}| / ( (|LL_t| + |LL_{t-1}|) / 2 ) < tol
    tol: float = 1e-4
    min_var: float = 1e-8
    jitter: float = 1e-12

    # Initialization
    pca_fill: Literal["mean", "ffill"] = "mean"

    # Scaling modes
    scaling_mode: Literal["external_frozen", "internal_per_run"] = "internal_per_run"

    # VAR stability
    force_var_stability: bool = True
    var_stability_shrink: float = 0.98

    # Initial state handling (toolbox parity)
    P0_mode: Literal["diffuse", "steady_state"] = "steady_state"
    update_initial_state_each_iter: bool = True

    def validate(self) -> None:
        """Validate config invariants.

        These checks make correctness-critical assumptions explicit.
        """

        # Core dimensional assumptions for this BM-DFM variant
        if int(self.n_quarterly) != 1:
            raise ValueError("BM-DFM implementation expects n_quarterly=1 (single quarterly target).")

        r_by_block = tuple(int(x) for x in self.r_by_block)
        if len(r_by_block) == 0 or sum(r_by_block) <= 0:
            raise ValueError("r_by_block must be non-empty and sum(r_by_block) must be positive.")
        if any(x < 0 for x in r_by_block):
            raise ValueError("r_by_block entries must be non-negative.")

        if int(self.p) < 0:
            raise ValueError("p must be >= 0.")

        if self.mm_weight_style not in ("toolbox", "scaled"):
            raise ValueError(f"Unknown mm_weight_style={self.mm_weight_style!r}.")

        # Toolbox proportionality constraints assume integer weights [1,2,3,2,1].
        if bool(self.enforce_quarterly_loading_constraint) and self.mm_weight_style != "toolbox":
            raise ValueError('enforce_quarterly_loading_constraint requires mm_weight_style="toolbox".')

        if float(self.quarterly_meas_var_floor) <= 0.0:
            raise ValueError("quarterly_meas_var_floor must be > 0.")
        if bool(self.idio_ar1) and float(self.monthly_meas_var_floor) <= 0.0:
            raise ValueError("monthly_meas_var_floor must be > 0 when idio_ar1=True.")

        if bool(self.force_var_stability):
            sh = float(self.var_stability_shrink)
            if not (0.0 < sh < 1.0):
                raise ValueError("var_stability_shrink must be in (0,1) when force_var_stability=True.")

        if float(self.min_var) <= 0.0:
            raise ValueError("min_var must be > 0.")
        if float(self.jitter) < 0.0:
            raise ValueError("jitter must be >= 0.")

        if int(self.max_iter) <= 0:
            raise ValueError("max_iter must be > 0.")

        if self.convergence_mode not in ("abs", "toolbox_rel"):
            raise ValueError(f"Unknown convergence_mode={self.convergence_mode!r}.")

        tol = float(self.tol)
        if tol <= 0.0:
            raise ValueError("tol must be > 0.")
        if self.convergence_mode == "toolbox_rel" and not (tol < 1.0):
            raise ValueError('With convergence_mode="toolbox_rel", tol must be in (0,1).')


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