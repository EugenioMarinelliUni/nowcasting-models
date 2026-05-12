from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence


@dataclass
class BMDfmConfig:
    r_by_block: Sequence[int]
    p: int

    blocks: Optional[list[int]] = None
    n_quarterly: int = 1
    ppC: int = 5

    mm_weight_style: str = "toolbox"
    pca_fill: str = "mean"
    init_missing_method: str = "toolbox_spline"

    max_iter: int = 200
    tol: float = 1e-6
    convergence_mode: str = "toolbox_rel"

    rho_idio_init: float = 0.10
    min_var: float = 1e-8
    jitter: float = 1e-12

    monthly_meas_var_floor: float = 1e-4
    quarterly_meas_var_floor: float = 1e-4

    scaling_mode: str = "external_frozen"
    idio_ar1: bool = True
    enforce_quarterly_loading_constraint: bool = True
    fix_quarterly_R: bool = True

    force_var_stability: bool = True
    var_stability_shrink: float = 0.98

    # Econometric-spec options
    P0_mode: str = "steady_state"
    update_initial_state_each_iter: bool = True

    def validate(self) -> None:
        if not self.r_by_block:
            raise ValueError("r_by_block must be non-empty.")
        if any(int(r) <= 0 for r in self.r_by_block):
            raise ValueError("All entries of r_by_block must be positive integers.")
        if int(self.p) <= 0:
            raise ValueError("p must be a positive integer.")
        if int(self.n_quarterly) != 1:
            raise ValueError("This BM-DFM implementation expects n_quarterly == 1.")
        if int(self.ppC) < 5:
            raise ValueError("ppC must be at least 5 for the Mariano-Murasawa quarterly stack.")

        if self.mm_weight_style not in {"toolbox", "scaled"}:
            raise ValueError("mm_weight_style must be 'toolbox' or 'scaled'.")
        if self.pca_fill not in {"mean", "ffill"}:
            raise ValueError("pca_fill must be 'mean' or 'ffill'.")
        if self.init_missing_method not in {"legacy_mean", "legacy_ffill", "toolbox_spline", "linear_interp"}:
            raise ValueError(
                "init_missing_method must be one of: legacy_mean, legacy_ffill, toolbox_spline, linear_interp."
            )

        if self.scaling_mode not in {"external_frozen", "internal_per_run", "toolbox_vintage"}:
            raise ValueError(
                "scaling_mode must be one of: external_frozen, internal_per_run, toolbox_vintage."
            )
        if self.convergence_mode not in {"absolute_ll", "toolbox_rel"}:
            raise ValueError("convergence_mode must be 'absolute_ll' or 'toolbox_rel'.")
        if self.P0_mode not in {"diffuse", "steady_state", "steady_state_factor_diffuse_idio"}:
            raise ValueError(
                "P0_mode must be one of: diffuse, steady_state, steady_state_factor_diffuse_idio."
            )

        if float(self.min_var) <= 0.0:
            raise ValueError("min_var must be strictly positive.")
        if float(self.monthly_meas_var_floor) <= 0.0:
            raise ValueError("monthly_meas_var_floor must be strictly positive.")
        if float(self.quarterly_meas_var_floor) <= 0.0:
            raise ValueError("quarterly_meas_var_floor must be strictly positive.")
        if not (0.0 < float(self.var_stability_shrink) <= 1.0):
            raise ValueError("var_stability_shrink must lie in (0, 1].")

        if self.blocks is not None and len(self.blocks) == 0:
            raise ValueError("blocks must be None or a non-empty list[int].")


__all__ = ["BMDfmConfig"]
