from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

import numpy as np
from tqdm.auto import tqdm

from .estimation import accept_projected_gem_candidate
from .state_builder import BMParams


@dataclass
class EMRunOutput:
    params: BMParams
    loglik_trace: list[float]
    converged: bool
    a0_override: Optional[np.ndarray]
    P0_override: Optional[np.ndarray]
    diagnostics: dict[str, Any]


def converged_trace(loglik_trace: list[float], tol: float, mode: str) -> bool:
    if len(loglik_trace) < 2:
        return False
    ll_new = float(loglik_trace[-1])
    ll_old = float(loglik_trace[-2])
    if not (np.isfinite(ll_new) and np.isfinite(ll_old)):
        return False
    diff = ll_new - ll_old
    if mode == "absolute_ll":
        return abs(diff) < float(tol)
    return abs(diff) / max(1.0, abs(ll_old)) < float(tol)


def run_em_loop(
    *,
    Y: np.ndarray,
    initial_params: BMParams,
    config: Any,
    em_step: Callable[..., tuple],
    em_kwargs: dict[str, Any],
    build_kwargs: dict[str, Any],
    description: str,
    verbose: bool,
) -> EMRunOutput:
    if str(config.estimation_mode) == "exact_constrained_ml":
        raise NotImplementedError(
            "exact_constrained_ml is reserved for a future direct optimiser of the stable-VAR "
            "constrained likelihood. Use estimation_mode='projected_gem' for the implemented "
            "likelihood-guarded constrained estimator."
        )

    params = initial_params
    update_initial = bool(config.update_initial_state_each_iter)
    a0_in: Optional[np.ndarray] = None
    P0_in: Optional[np.ndarray] = None

    trace: list[float] = []
    step_sizes: list[float] = []
    backtracks: list[int] = []
    rejected_steps = 0
    stalled = False
    converged = False
    stop_reason = "maximum_iterations"

    iterator = tqdm(
        range(int(config.max_iter)),
        desc=description,
        unit="iter",
        dynamic_ncols=True,
        disable=not bool(verbose),
    )

    for _iteration in iterator:
        old_params = params
        old_a0 = None if a0_in is None else np.asarray(a0_in, dtype=float).copy()
        old_P0 = None if P0_in is None else np.asarray(P0_in, dtype=float).copy()

        (
            candidate,
            old_loglik,
            _a_smooth,
            _P_smooth,
            _P_lag_smooth,
            candidate_a0,
            candidate_P0,
        ) = em_step(
            Y=Y,
            params=old_params,
            a0_in=old_a0,
            P0_in=old_P0,
            update_initial_state=update_initial,
            **em_kwargs,
        )

        if not update_initial:
            candidate_a0 = None
            candidate_P0 = None

        acceptance = accept_projected_gem_candidate(
            Y=Y,
            old_params=old_params,
            candidate_params=candidate,
            old_loglik=float(old_loglik),
            old_a0=old_a0,
            old_P0=old_P0,
            candidate_a0=candidate_a0,
            candidate_P0=candidate_P0,
            build_kwargs=build_kwargs,
            likelihood_guard=bool(config.gem_likelihood_guard),
            ll_tolerance=float(config.gem_ll_tolerance),
            min_step=float(config.gem_min_step),
            max_backtracks=int(config.gem_max_backtracks),
            min_var=float(config.min_var),
            require_stability=bool(config.force_var_stability),
        )

        params = acceptance.params
        if update_initial:
            a0_in = acceptance.a0_override
            P0_in = acceptance.P0_override

        trace.append(float(acceptance.loglik))
        step_sizes.append(float(acceptance.step_size))
        backtracks.append(int(acceptance.backtracks))
        if not acceptance.accepted:
            rejected_steps += 1
            stalled = True

        if verbose:
            iterator.set_postfix(
                ll=float(acceptance.loglik),
                step=float(acceptance.step_size),
                refresh=False,
            )

        # A rejected GEM proposal leaves the likelihood unchanged by construction.
        # Check rejection before convergence so that a duplicated likelihood cannot
        # be misclassified as successful convergence.
        if stalled:
            stop_reason = "rejected_candidate"
            break
        if converged_trace(trace, tol=float(config.tol), mode=str(config.convergence_mode)):
            converged = True
            stop_reason = "tolerance"
            break

    if bool(config.require_convergence) and not converged:
        reason = "GEM step stalled" if stalled else "maximum iterations reached"
        raise RuntimeError(
            f"BM-DFM did not converge ({reason}); iterations={len(trace)}, "
            f"last_loglik={trace[-1] if trace else float('nan'):.6g}."
        )

    increments = np.diff(np.asarray(trace, dtype=float)) if len(trace) > 1 else np.array([], dtype=float)
    diagnostics: dict[str, Any] = {
        "estimation_mode": str(config.estimation_mode),
        "likelihood_guard": bool(config.gem_likelihood_guard),
        "iterations": len(trace),
        "converged": bool(converged),
        "stalled": bool(stalled),
        "stop_reason": str(stop_reason),
        "rejected_steps": int(rejected_steps),
        "step_sizes": step_sizes,
        "backtracks": backtracks,
        "minimum_loglik_increment": float(np.min(increments)) if increments.size else float("nan"),
        "initial_loglik": float(trace[0]) if trace else float("nan"),
        "final_loglik": float(trace[-1]) if trace else float("nan"),
        "initial_state_policy": str(config.P0_mode),
    }
    return EMRunOutput(params, trace, converged, a0_in, P0_in, diagnostics)


__all__ = ["EMRunOutput", "converged_trace", "run_em_loop"]
