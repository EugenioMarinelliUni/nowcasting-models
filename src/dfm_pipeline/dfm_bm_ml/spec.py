from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass
class BMDfmConfig:
    # Factor blocks: provide r per block. This implementation supports one block by default.
    r_by_block: tuple[int, ...] = (4,)
    p: int = 1
    blocks: list[int] | None = None  # optional block assignment for monthly series (len = n_monthly)

    # Quarterly target count (this variant supports a single quarterly target series).
    n_quarterly: int = 1

    # MM aggregation weights for quarterly measurement equation
    mm_weight_style: Literal["toolbox", "scaled"] = "toolbox"

    # PCA init
    pca_fill: Literal["mean", "ffill"] = "mean"

    # EM loop
    max_iter: int = 200
    tol: float = 1e-6

    # Convergence
    convergence_mode: Literal["absolute_ll", "toolbox_rel"] = "toolbox_rel"

    # Idiosyncratic AR(1)
    rho_idio_init: float = 0.10
    idio_ar1: bool = True

    # Variance floors / numerical stability
    min_var: float = 1e-8
    jitter: float = 1e-12

    # Toolbox-style measurement variance floors (important for parity)
    monthly_meas_var_floor: float = 1e-4
    quarterly_meas_var_floor: float = 1e-4

    # Scaling modes
    # - external_frozen: inputs already standardized externally (frozen scalers); fit must not rescale.
    # - toolbox_vintage: compute nanmean/nanstd on the current in-sample vintage and standardize once per fit call
    #   (toolbox-style per-vintage scaling). This is an alias of internal_per_run.
    # - internal_per_run: same behavior as toolbox_vintage; kept for backward compatibility.
    scaling_mode: Literal["external_frozen", "internal_per_run", "toolbox_vintage"] = "internal_per_run"

    # Quarterly loading constraint / R handling
    enforce_quarterly_loading_constraint: bool = True
    fix_quarterly_R: bool = True

    # VAR stability
    force_var_stability: bool = True
    var_stability_shrink: float = 0.98
    var_stability_max_iter: int = 50

    def validate(self) -> None:
        # Core dimensional assumptions for this BM-DFM variant
        if int(self.n_quarterly) != 1:
            raise ValueError("BM-DFM implementation expects n_quarterly=1 (single quarterly target).")

        if self.scaling_mode not in ("external_frozen", "internal_per_run", "toolbox_vintage"):
            raise ValueError(f"Unknown scaling_mode={self.scaling_mode!r}.")

        # Quarterly constraint vs MM weights must be toolbox-consistent
        if self.enforce_quarterly_loading_constraint and self.mm_weight_style != "toolbox":
            raise ValueError(
                "enforce_quarterly_loading_constraint=True requires mm_weight_style='toolbox' "
                "(toolbox proportional loading constraint assumes integer MM weights)."
            )

        # Convergence mode validation
        if self.convergence_mode not in ("absolute_ll", "toolbox_rel"):
            raise ValueError(f"Unknown convergence_mode={self.convergence_mode!r}.")

        if self.max_iter <= 0:
            raise ValueError("max_iter must be positive.")
        if self.tol <= 0:
            raise ValueError("tol must be positive.")

        if self.force_var_stability and not (0.0 < self.var_stability_shrink < 1.0):
            raise ValueError("var_stability_shrink must be in (0,1) when force_var_stability=True.")