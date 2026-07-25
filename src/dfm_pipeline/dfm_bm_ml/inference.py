from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd

from .identification import align_procrustes
from .state_builder import BMParams
from .types import BMDfmResult


@dataclass(frozen=True)
class BootstrapConfig:
    n_replications: int = 200
    random_seed: int = 0
    preserve_missing_pattern: bool = True
    align_factors_to_reference: bool = True
    fail_fast: bool = False


def flatten_params(params: BMParams) -> tuple[np.ndarray, list[str]]:
    values: list[float] = []
    names: list[str] = []

    for b, block in enumerate(params.Phi_blocks):
        for lag, phi in enumerate(block, start=1):
            arr = np.asarray(phi, dtype=float)
            for i, j in np.ndindex(arr.shape):
                names.append(f"Phi_b{b}_lag{lag}_{i}_{j}")
                values.append(float(arr[i, j]))
    for b, q in enumerate(params.Q_f_blocks):
        arr = np.asarray(q, dtype=float)
        for i, j in np.ndindex(arr.shape):
            names.append(f"Qf_b{b}_{i}_{j}")
            values.append(float(arr[i, j]))

    for prefix, arr in [
        ("rho_m", params.rho_m),
        ("sig2_m", params.sig2_m),
        ("rho_q", params.rho_q),
        ("sig2_q", params.sig2_q),
        ("R_m", params.R_diag_m),
        ("R_q", params.R_diag_q),
    ]:
        for i, value in enumerate(np.asarray(arr, dtype=float).reshape(-1)):
            names.append(f"{prefix}_{i}")
            values.append(float(value))

    for prefix, arr in [("Lambda_m", params.Lambda_m), ("Lambda_q", params.Lambda_q)]:
        A = np.asarray(arr, dtype=float)
        for i, j in np.ndindex(A.shape):
            names.append(f"{prefix}_{i}_{j}")
            values.append(float(A[i, j]))

    return np.asarray(values, dtype=float), names


def simulate_state_space(
    result: BMDfmResult,
    n_periods: int,
    *,
    rng: np.random.Generator,
    missing_mask: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Simulate observations in the fitted model's observation scale."""
    A = np.asarray(result.A, dtype=float)
    Q = np.asarray(result.Q, dtype=float)
    C = np.asarray(result.C, dtype=float)
    R = np.asarray(result.R, dtype=float)
    a0 = np.asarray(result.a0, dtype=float)
    P0 = np.asarray(result.P0, dtype=float)
    R_full = np.diag(R) if R.ndim == 1 else R

    state = rng.multivariate_normal(a0, P0)
    Y = np.empty((int(n_periods), C.shape[0]), dtype=float)
    for t in range(int(n_periods)):
        if t > 0:
            state = A @ state + rng.multivariate_normal(np.zeros(A.shape[0]), Q)
        Y[t] = C @ state + rng.multivariate_normal(np.zeros(C.shape[0]), R_full)

    if missing_mask is not None:
        mask = np.asarray(missing_mask, dtype=bool)
        if mask.shape != Y.shape:
            raise ValueError("missing_mask must have the same shape as the simulated panel.")
        Y[mask] = np.nan
    return Y


def parametric_bootstrap(
    *,
    fitted_result: BMDfmResult,
    fit_fn: Callable[..., BMDfmResult],
    model_config: Any,
    observed_panel: np.ndarray,
    bootstrap_config: BootstrapConfig = BootstrapConfig(),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Optional parametric-bootstrap parameter inference.

    The bootstrap operates in the fitted model scale.  Replications are aligned
    block-by-block to the reference loadings before parameter summaries are
    calculated, preventing arbitrary factor sign/rotation changes from inflating
    uncertainty.  It is intentionally not run by the ordinary fitter.
    """
    Y_obs = np.asarray(observed_panel, dtype=float)
    if Y_obs.ndim != 2:
        raise ValueError("observed_panel must be a T x (nM+nQ) array.")
    if int(bootstrap_config.n_replications) <= 1:
        raise ValueError("n_replications must exceed one.")

    missing_mask = np.isnan(Y_obs) if bootstrap_config.preserve_missing_pattern else None
    rng = np.random.default_rng(int(bootstrap_config.random_seed))
    rows: list[dict[str, float]] = []

    # Simulations are already in model coordinates, so avoid restandardising them.
    try:
        cfg_boot = replace(model_config, scaling_mode="external_frozen")
    except TypeError:
        cfg_boot = model_config
        setattr(cfg_boot, "scaling_mode", "external_frozen")

    r_by_block = tuple(int(r) for r in getattr(model_config, "r_by_block"))
    nM = Y_obs.shape[1] - int(getattr(model_config, "n_quarterly", 1))

    for rep in range(int(bootstrap_config.n_replications)):
        Y_sim = simulate_state_space(
            fitted_result,
            Y_obs.shape[0],
            rng=rng,
            missing_mask=missing_mask,
        )
        try:
            boot_res = fit_fn(
                Y_monthly=Y_sim[:, :nM],
                y_quarterly=Y_sim[:, nM],
                config=cfg_boot,
                verbose=False,
            )
            params = boot_res.params
            if bootstrap_config.align_factors_to_reference:
                params = align_procrustes(
                    params,
                    fitted_result.params.Lambda_m,
                    r_by_block=r_by_block,
                )
            vec, names = flatten_params(params)
            row = {name: float(value) for name, value in zip(names, vec)}
            row["replication"] = float(rep)
            row["converged"] = float(bool(boot_res.converged))
            rows.append(row)
        except Exception:
            if bootstrap_config.fail_fast:
                raise

    estimates = pd.DataFrame(rows)
    if estimates.empty:
        raise RuntimeError("All bootstrap replications failed.")

    parameter_cols = [c for c in estimates.columns if c not in {"replication", "converged"}]
    summary = pd.DataFrame(
        {
            "mean": estimates[parameter_cols].mean(),
            "std_error": estimates[parameter_cols].std(ddof=1),
            "q025": estimates[parameter_cols].quantile(0.025),
            "median": estimates[parameter_cols].quantile(0.5),
            "q975": estimates[parameter_cols].quantile(0.975),
        }
    )
    summary.index.name = "parameter"
    return estimates, summary


__all__ = [
    "BootstrapConfig",
    "flatten_params",
    "simulate_state_space",
    "parametric_bootstrap",
]
