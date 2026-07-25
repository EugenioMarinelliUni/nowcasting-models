from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence

import numpy as np

from dfm_pipeline.dfm_dyn.state_space import StateSpaceParams, kalman_filter_smoother

from .state_builder import BMParams, build_state_space
from .steady_state import project_psd
from .stability import _companion_from_var, spectral_radius


@dataclass(frozen=True)
class GEMAcceptance:
    params: BMParams
    loglik: float
    step_size: float
    backtracks: int
    accepted: bool
    a0_override: Optional[np.ndarray]
    P0_override: Optional[np.ndarray]


def blend_params(old: BMParams, new: BMParams, step: float, *, min_var: float) -> BMParams:
    """Convexly interpolate two feasible parameter objects."""
    g = float(step)
    if not (0.0 <= g <= 1.0):
        raise ValueError("step must lie in [0, 1].")

    def mix(a, b):
        return (1.0 - g) * np.asarray(a, dtype=float) + g * np.asarray(b, dtype=float)

    phi_blocks: list[list[np.ndarray]] = []
    q_blocks: list[np.ndarray] = []
    for old_block, new_block, old_q, new_q in zip(
        old.Phi_blocks, new.Phi_blocks, old.Q_f_blocks, new.Q_f_blocks
    ):
        phi_blocks.append([mix(a, b) for a, b in zip(old_block, new_block)])
        q_blocks.append(project_psd(mix(old_q, new_q), eps=float(min_var)))

    return BMParams(
        Phi_blocks=phi_blocks,
        Q_f_blocks=q_blocks,
        rho_m=np.clip(mix(old.rho_m, new.rho_m), -0.999, 0.999),
        sig2_m=np.maximum(mix(old.sig2_m, new.sig2_m), float(min_var)),
        rho_q=np.clip(mix(old.rho_q, new.rho_q), -0.999, 0.999),
        sig2_q=np.maximum(mix(old.sig2_q, new.sig2_q), float(min_var)),
        Lambda_m=mix(old.Lambda_m, new.Lambda_m),
        Lambda_q=mix(old.Lambda_q, new.Lambda_q),
        R_diag_m=np.maximum(mix(old.R_diag_m, new.R_diag_m), float(min_var)),
        R_diag_q=np.maximum(mix(old.R_diag_q, new.R_diag_q), float(min_var)),
    )



def factor_var_is_stable(
    params: BMParams,
    *,
    ppC: int,
    radius_target: float = 0.999,
) -> bool:
    """Return whether every factor-block VAR is stable.

    Backtracking between two stable VAR parameterisations is not guaranteed to
    remain inside the (generally non-convex) stability region for VAR(p).  The
    GEM acceptance step therefore checks stability explicitly rather than
    relying on the endpoint projection alone.
    """
    for phi_lags in params.Phi_blocks:
        try:
            radius = spectral_radius(_companion_from_var(phi_lags, ppC=int(ppC)))
        except (ValueError, np.linalg.LinAlgError):
            return False
        if (not np.isfinite(radius)) or radius > float(radius_target):
            return False
    return True

def _kalman_R(R: np.ndarray) -> np.ndarray:
    R = np.asarray(R, dtype=float)
    if R.ndim == 1:
        return R
    d = np.diag(R)
    if np.allclose(R, np.diag(d)):
        return d
    return R


def evaluate_params(
    Y: np.ndarray,
    params: BMParams,
    *,
    nM: int,
    nQ: int,
    r_by_block: Sequence[int],
    p: int,
    ppC: int,
    mm_style: str,
    quarterly_meas_var_floor: float,
    idio_ar1: bool,
    jitter: float,
    P0_mode: str,
    a0_override: Optional[np.ndarray] = None,
    P0_override: Optional[np.ndarray] = None,
):
    A, Q, C, R, a0, P0, idx = build_state_space(
        params=params,
        nM=int(nM),
        nQ=int(nQ),
        r_by_block=tuple(int(r) for r in r_by_block),
        p=int(p),
        ppC=int(ppC),
        mm_style=str(mm_style),
        quarterly_meas_var_floor=float(quarterly_meas_var_floor),
        idio_ar1=bool(idio_ar1),
        jitter=float(jitter),
        P0_mode=str(P0_mode),
        a0_override=a0_override,
        P0_override=P0_override,
    )
    ss = StateSpaceParams(T=A, Q=Q, C=C, R=_kalman_R(R), a0=a0, P0=P0)
    smooth = kalman_filter_smoother(np.asarray(Y, dtype=float), ss)
    return float(smooth.loglik), smooth, (A, Q, C, R, a0, P0, idx)


def accept_projected_gem_candidate(
    *,
    Y: np.ndarray,
    old_params: BMParams,
    candidate_params: BMParams,
    old_loglik: float,
    old_a0: Optional[np.ndarray],
    old_P0: Optional[np.ndarray],
    candidate_a0: Optional[np.ndarray],
    candidate_P0: Optional[np.ndarray],
    build_kwargs: dict[str, Any],
    likelihood_guard: bool,
    ll_tolerance: float,
    min_step: float,
    max_backtracks: int,
    min_var: float,
    require_stability: bool = False,
    stability_radius_target: float = 0.999,
) -> GEMAcceptance:
    """Accept a projected M-step only when observed likelihood is non-decreasing.

    If the full projected step decreases the likelihood, backtracking along the
    old-to-candidate parameter segment produces a valid generalized-EM step.
    """
    if not likelihood_guard:
        ll, _smooth, _mats = evaluate_params(
            Y, candidate_params, a0_override=candidate_a0, P0_override=candidate_P0, **build_kwargs
        )
        return GEMAcceptance(
            candidate_params, ll, 1.0, 0, True, candidate_a0, candidate_P0
        )

    step = 1.0
    for backtracks in range(int(max_backtracks) + 1):
        params_try = candidate_params if step == 1.0 else blend_params(
            old_params, candidate_params, step, min_var=float(min_var)
        )

        if old_a0 is None or candidate_a0 is None:
            a0_try = candidate_a0 if step == 1.0 else old_a0
        else:
            a0_try = (1.0 - step) * np.asarray(old_a0) + step * np.asarray(candidate_a0)

        if old_P0 is None or candidate_P0 is None:
            P0_try = candidate_P0 if step == 1.0 else old_P0
        else:
            P0_try = project_psd(
                (1.0 - step) * np.asarray(old_P0) + step * np.asarray(candidate_P0),
                eps=float(min_var),
            )

        if bool(require_stability) and not factor_var_is_stable(
            params_try,
            ppC=int(build_kwargs["ppC"]),
            radius_target=float(stability_radius_target),
        ):
            ll_try = float("-inf")
        else:
            try:
                ll_try, _smooth, _mats = evaluate_params(
                    Y, params_try, a0_override=a0_try, P0_override=P0_try, **build_kwargs
                )
            except (ValueError, np.linalg.LinAlgError, RuntimeError):
                ll_try = float("-inf")

        threshold = float(old_loglik) - float(ll_tolerance)
        if np.isfinite(ll_try) and ll_try >= threshold:
            return GEMAcceptance(
                params_try, float(ll_try), float(step), backtracks, True, a0_try, P0_try
            )

        step *= 0.5
        if step < float(min_step):
            break

    return GEMAcceptance(
        old_params,
        float(old_loglik),
        0.0,
        int(max_backtracks),
        False,
        old_a0,
        old_P0,
    )


__all__ = [
    "GEMAcceptance",
    "blend_params",
    "factor_var_is_stable",
    "evaluate_params",
    "accept_projected_gem_candidate",
]
