from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass
class BMDfmConfig:
    """Configuration for the Bańbura-Modugno mixed-frequency DFM.

    The default estimator is a projected generalized-EM (GEM) algorithm.  The
    stability/PSD projections are followed by an observed-likelihood acceptance
    check, so accepted iterations are monotone up to ``gem_ll_tolerance``.

    ``exact_constrained_ml`` is intentionally reserved for a future optimiser
    that solves the stable-VAR constrained M-step directly.  Selecting it raises
    ``NotImplementedError`` rather than silently falling back to projected GEM.
    """

    r_by_block: Sequence[int]
    p: int

    # Optional block membership. None selects the standard single-block model.
    # When multiple factor blocks are requested, provide either a 1D zero-based
    # label vector or a 2D membership mask; shape validation occurs once nM is known.
    blocks: Optional[object] = None
    n_quarterly: int = 1
    ppC: int = 5

    mm_weight_style: str = "toolbox"
    pca_fill: str = "mean"
    init_missing_method: str = "toolbox_spline"

    max_iter: int = 200
    tol: float = 1e-6
    convergence_mode: str = "toolbox_rel"
    require_convergence: bool = False

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

    # Initial-state policy.  ``estimated`` is available for compatibility with
    # free initial moments; the stationary default is preferable for the
    # transformed stationary macro panel.
    P0_mode: str = "steady_state"
    update_initial_state_each_iter: bool = False

    # Estimation mode and GEM monotonicity guard.
    estimation_mode: str = "projected_gem"
    gem_likelihood_guard: bool = True
    gem_ll_tolerance: float = 1e-7
    gem_min_step: float = 1e-3
    gem_max_backtracks: int = 12

    # Optional post-estimation factor identification.  ``sign_anchor`` removes
    # arbitrary sign flips while preserving the likelihood and common component.
    identification_mode: str = "none"
    identification_anchor_indices: Optional[Sequence[int]] = None

    def validate(self) -> None:
        if not self.r_by_block:
            raise ValueError("r_by_block must be non-empty.")
        if any(int(r) <= 0 for r in self.r_by_block):
            raise ValueError("All entries of r_by_block must be positive integers.")
        if int(self.p) <= 0:
            raise ValueError("p must be a positive integer.")
        if int(self.p) > int(self.ppC):
            raise ValueError(
                f"p={self.p} exceeds ppC={self.ppC}. This implementation uses a "
                "five-month Mariano-Murasawa factor stack, so supported VAR orders are 1 through 5."
            )
        if int(self.n_quarterly) != 1:
            raise ValueError("This BM-DFM implementation expects n_quarterly == 1.")
        if int(self.ppC) != 5:
            raise ValueError("ppC must equal 5 for the Mariano-Murasawa quarterly stack.")

        if self.mm_weight_style not in {"toolbox", "scaled"}:
            raise ValueError("mm_weight_style must be 'toolbox' or 'scaled'.")
        if self.pca_fill not in {"mean", "ffill"}:
            raise ValueError("pca_fill must be 'mean' or 'ffill'.")
        if self.init_missing_method not in {
            "legacy_mean",
            "legacy_ffill",
            "toolbox_spline",
            "linear_interp",
        }:
            raise ValueError(
                "init_missing_method must be one of: legacy_mean, legacy_ffill, "
                "toolbox_spline, linear_interp."
            )

        if self.scaling_mode not in {"external_frozen", "internal_per_run", "toolbox_vintage"}:
            raise ValueError(
                "scaling_mode must be one of: external_frozen, internal_per_run, toolbox_vintage."
            )
        if self.convergence_mode not in {"absolute_ll", "toolbox_rel"}:
            raise ValueError("convergence_mode must be 'absolute_ll' or 'toolbox_rel'.")
        if int(self.max_iter) <= 0:
            raise ValueError("max_iter must be positive.")
        if float(self.tol) < 0.0:
            raise ValueError("tol must be non-negative.")

        valid_p0 = {"diffuse", "steady_state", "steady_state_factor_diffuse_idio", "estimated"}
        if self.P0_mode not in valid_p0:
            raise ValueError(f"P0_mode must be one of: {', '.join(sorted(valid_p0))}.")
        # Backward-compatible resolution of the former mixed policy: explicitly
        # requesting per-iteration initial moments now selects the coherent
        # freely-estimated initial-state mode.
        if bool(self.update_initial_state_each_iter) and self.P0_mode != "estimated":
            self.P0_mode = "estimated"
        if self.P0_mode == "estimated" and not bool(self.update_initial_state_each_iter):
            self.update_initial_state_each_iter = True

        if float(self.min_var) <= 0.0:
            raise ValueError("min_var must be strictly positive.")
        if float(self.monthly_meas_var_floor) <= 0.0:
            raise ValueError("monthly_meas_var_floor must be strictly positive.")
        if float(self.quarterly_meas_var_floor) <= 0.0:
            raise ValueError("quarterly_meas_var_floor must be strictly positive.")

        shrink = float(self.var_stability_shrink)
        if not (0.0 < shrink <= 1.0):
            raise ValueError("var_stability_shrink must lie in (0, 1].")
        if bool(self.force_var_stability) and shrink >= 1.0:
            raise ValueError(
                "var_stability_shrink must be strictly below 1 when force_var_stability=True."
            )

        if self.estimation_mode not in {"projected_gem", "unrestricted_em", "exact_constrained_ml"}:
            raise ValueError(
                "estimation_mode must be projected_gem, unrestricted_em, or exact_constrained_ml."
            )
        if self.estimation_mode == "unrestricted_em" and bool(self.force_var_stability):
            raise ValueError(
                "unrestricted_em requires force_var_stability=False; otherwise use projected_gem."
            )
        if float(self.gem_ll_tolerance) < 0.0:
            raise ValueError("gem_ll_tolerance must be non-negative.")
        if not (0.0 < float(self.gem_min_step) <= 1.0):
            raise ValueError("gem_min_step must lie in (0, 1].")
        if int(self.gem_max_backtracks) < 0:
            raise ValueError("gem_max_backtracks must be non-negative.")

        if self.identification_mode not in {"none", "sign_anchor"}:
            raise ValueError("identification_mode must be 'none' or 'sign_anchor'.")
        if self.identification_mode != "none" and self.P0_mode == "estimated":
            raise ValueError(
                "Automatic factor identification is not supported with freely estimated initial moments; "
                "use stationary initialization or apply identification as a reporting step."
            )
        if self.identification_anchor_indices is not None:
            anchors = tuple(int(x) for x in self.identification_anchor_indices)
            if len(anchors) != int(sum(int(r) for r in self.r_by_block)):
                raise ValueError(
                    "identification_anchor_indices must contain one monthly-series index per factor."
                )
            if any(x < 0 for x in anchors):
                raise ValueError("identification_anchor_indices must be non-negative.")

        if len(tuple(self.r_by_block)) > 1 and self.blocks is None:
            raise ValueError(
                "Multiple entries in r_by_block require an explicit blocks membership specification. "
                "Use blocks=None only for the optional standard single-block model."
            )
        if self.blocks is not None:
            try:
                if len(self.blocks) == 0:  # type: ignore[arg-type]
                    raise ValueError("blocks must be None or a non-empty label vector/membership mask.")
            except TypeError as exc:
                raise ValueError(
                    "blocks must be None, a 1D label vector, or a 2D membership mask."
                ) from exc


__all__ = ["BMDfmConfig"]
