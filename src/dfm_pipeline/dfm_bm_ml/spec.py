from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class BMDfmConfig:
    r_by_block: tuple[int, ...] = (4,)
    p: int = 1
    blocks: list[int] | None = None

    n_quarterly: int = 1

    mm_weight_style: Literal["toolbox", "scaled"] = "toolbox"

    # PCA init
    pca_fill: Literal["mean", "ffill", "toolbox_spline"] = "mean"
    pca_spline_k: int = 3
    pca_spline_trim_row_missing_frac: float = 0.8

    # EM loop control
    max_iter: int = 200          # hard cap (always enforced)
    tol: float = 1e-6            # used by convergence criterion
    convergence_mode: Literal["absolute_ll", "toolbox_rel"] = "toolbox_rel"
    # - absolute_ll: stop if |ll_k - ll_{k-1}| < tol
    # - toolbox_rel: stop if |ll_k - ll_{k-1}| / max(1, |ll_{k-1}|) < tol

    rho_idio_init: float = 0.10
    idio_ar1: bool = True

    # Initial state covariance (P0)
    P0_mode: Literal["diffuse", "steady_state"] = "diffuse"

    min_var: float = 1e-8
    jitter: float = 1e-12

    monthly_meas_var_floor: float = 1e-4
    quarterly_meas_var_floor: float = 1e-4

    scaling_mode: Literal["external_frozen", "internal_per_run", "toolbox_vintage"] = "internal_per_run"

    enforce_quarterly_loading_constraint: bool = True
    fix_quarterly_R: bool = True

    force_var_stability: bool = True
    var_stability_shrink: float = 0.98
    var_stability_max_iter: int = 50

    def validate(self) -> None:
        if int(self.n_quarterly) != 1:
            raise ValueError("BM-DFM implementation expects n_quarterly=1 (single quarterly target).")

        if self.scaling_mode not in ("external_frozen", "internal_per_run", "toolbox_vintage"):
            raise ValueError(f"Unknown scaling_mode={self.scaling_mode!r}.")

        if self.enforce_quarterly_loading_constraint and self.mm_weight_style != "toolbox":
            raise ValueError(
                "enforce_quarterly_loading_constraint=True requires mm_weight_style='toolbox' "
                "(toolbox proportional loading constraint assumes integer MM weights)."
            )

        if self.convergence_mode not in ("absolute_ll", "toolbox_rel"):
            raise ValueError(f"Unknown convergence_mode={self.convergence_mode!r}.")

        if self.P0_mode not in ("diffuse", "steady_state"):
            raise ValueError(f"Unknown P0_mode={self.P0_mode!r}.")

        if self.pca_fill not in ("mean", "ffill", "toolbox_spline"):
            raise ValueError(f"Unknown pca_fill={self.pca_fill!r}.")

        if self.pca_spline_k < 0:
            raise ValueError("pca_spline_k must be >= 0.")

        if not (0.0 < self.pca_spline_trim_row_missing_frac <= 1.0):
            raise ValueError("pca_spline_trim_row_missing_frac must be in (0,1].")

        if self.max_iter <= 0:
            raise ValueError("max_iter must be positive.")
        if self.tol <= 0:
            raise ValueError("tol must be positive.")

        if self.force_var_stability and not (0.0 < self.var_stability_shrink < 1.0):
            raise ValueError("var_stability_shrink must be in (0,1) when force_var_stability=True.")