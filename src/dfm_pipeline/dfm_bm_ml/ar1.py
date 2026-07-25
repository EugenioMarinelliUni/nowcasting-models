from __future__ import annotations

import numpy as np


def update_ar1_stationary_moments(
    Ezz_diag: np.ndarray,
    P_lag_diag: np.ndarray,
    a_diag: np.ndarray,
    *,
    min_var: float = 1e-8,
    rho_cap: float = 0.999,
) -> tuple[float, float]:
    """Update a scalar AR(1) state from smoothed sufficient statistics.

    The state equation is ``z_t = rho * z_{t-1} + eta_t`` and BMParams stores
    the *stationary state variance*, not the innovation variance. The M-step
    first estimates the conditional innovation variance

        q = mean E[(z_t - rho z_{t-1})^2]

    and then converts it to ``Var(z_t) = q / (1-rho**2)``.
    """
    Ezz_diag = np.asarray(Ezz_diag, dtype=float).reshape(-1)
    P_lag_diag = np.asarray(P_lag_diag, dtype=float).reshape(-1)
    a_diag = np.asarray(a_diag, dtype=float).reshape(-1)
    if not (Ezz_diag.shape == P_lag_diag.shape == a_diag.shape):
        raise ValueError("AR(1) moment arrays must have the same shape.")
    if Ezz_diag.size < 2:
        return 0.0, float(min_var)

    cross = P_lag_diag[1:] + a_diag[1:] * a_diag[:-1]
    prev = Ezz_diag[:-1]
    curr = Ezz_diag[1:]

    den = float(np.sum(prev))
    rho = float(np.sum(cross) / den) if den > 0.0 else 0.0
    rho = float(np.clip(rho, -float(rho_cap), float(rho_cap)))

    q_terms = curr - 2.0 * rho * cross + (rho * rho) * prev
    q = float(np.mean(q_terms))
    one_minus_rho2 = max(1.0 - rho * rho, 1e-12)
    q = max(q, float(min_var) * one_minus_rho2)
    stationary_var = max(q / one_minus_rho2, float(min_var))
    return rho, float(stationary_var)


__all__ = ["update_ar1_stationary_moments"]
