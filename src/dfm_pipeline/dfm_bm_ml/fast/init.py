from __future__ import annotations

from typing import Literal, Tuple

import numpy as np

from ..constraints import mm_sum_sq
from ..spec import BMDfmConfig
from ..stability import enforce_var_stability
from ..state_builder import BMParams

PcaFill = Literal["mean", "ffill"]


def _fill_for_pca(Y: np.ndarray, method: PcaFill) -> np.ndarray:
    """
    Fill NaNs for PCA initialization.

    - mean: replace NaNs with column nan-mean (NaN-mean -> 0 if column all-NaN)
    - ffill: forward-fill within each column, then fill remaining NaNs with column nan-mean
    """
    Y = np.asarray(Y, dtype=float)
    X = Y.copy()

    col_means = np.nanmean(X, axis=0)
    col_means = np.where(np.isfinite(col_means), col_means, 0.0)

    if method == "mean":
        inds = np.where(np.isnan(X))
        X[inds] = col_means[inds[1]]
        return X

    if method == "ffill":
        # forward fill along time axis
        T, n = X.shape
        last = np.full(n, np.nan, dtype=float)
        for t in range(T):
            row = X[t]
            mask = np.isfinite(row)
            last[mask] = row[mask]
            X[t, ~mask] = last[~mask]

        # remaining NaNs -> column means
        inds = np.where(np.isnan(X))
        X[inds] = col_means[inds[1]]
        return X

    raise ValueError(f"Unsupported pca_fill={method!r}")


def pca_init_factors(Y_monthly: np.ndarray, r_total: int, fill: PcaFill) -> Tuple[np.ndarray, np.ndarray]:
    """
    PCA init on filled monthly panel.

    Returns:
      F0: (T, r_total) factor scores
      Lambda0: (nM, r_total) loadings
    """
    X = _fill_for_pca(Y_monthly, fill)
    Xc = X - X.mean(axis=0, keepdims=True)

    U, S, Vt = np.linalg.svd(Xc, full_matrices=False)
    r = int(r_total)
    r = max(1, min(r, U.shape[1]))

    F0 = U[:, :r] * S[:r]
    Lambda0 = Vt[:r, :].T
    return F0, Lambda0


def _fit_var_ols(F: np.ndarray, p: int) -> Tuple[list[np.ndarray], np.ndarray]:
    """
    OLS VAR(p) on factors F (T x r). Returns Phi_lags (list length p, each r x r) and Q (r x r).
    """
    F = np.asarray(F, dtype=float)
    T, r = F.shape
    p = int(p)

    if p <= 0:
        return [], np.eye(r)

    # Build lagged regressor matrix
    Y = F[p:, :]  # (T-p, r)
    X = []
    for lag in range(1, p + 1):
        X.append(F[p - lag : T - lag, :])
    X = np.concatenate(X, axis=1)  # (T-p, r*p)

    # Solve Y = X B + e
    B, *_ = np.linalg.lstsq(X, Y, rcond=None)  # (r*p, r)
    resid = Y - X @ B

    # Innovation covariance
    Q = np.cov(resid.T, bias=True)
    if Q.ndim == 0:
        Q = np.array([[float(Q)]], dtype=float)
    Q = (Q + Q.T) / 2.0

    # Split B into lag matrices (each r x r)
    Phi_lags = []
    for lag in range(p):
        Phi_lags.append(B[lag * r : (lag + 1) * r, :].T)
    return Phi_lags, Q


def init_params_pca(
    Y_scaled: np.ndarray,
    *,
    nM: int,
    config: BMDfmConfig,
) -> BMParams:
    """
    Build a full BMParams initialization from PCA (and simple OLS for the factor VAR and quarterly loading).
    Expects Y_scaled to be (T, nM + 1), with quarterly target in the last column (NaNs except quarter-ends).
    """
    Y_scaled = np.asarray(Y_scaled, dtype=float)
    if Y_scaled.ndim != 2:
        raise ValueError("Y_scaled must be 2D (T, nM+1).")
    if Y_scaled.shape[1] != nM + 1:
        raise ValueError(f"Expected Y_scaled.shape[1]==nM+1 ({nM+1}), got {Y_scaled.shape[1]}.")

    r_by_block = tuple(int(x) for x in config.r_by_block)
    r_total = int(sum(r_by_block))
    if r_total <= 0:
        raise ValueError("sum(r_by_block) must be > 0.")

    Y_m = Y_scaled[:, :nM]
    y_q = Y_scaled[:, nM]

    # PCA on monthly panel
    F0, Lambda_m0 = pca_init_factors(Y_m, r_total=r_total, fill=config.pca_fill)

    # Block split
    blocks = []
    s = 0
    for rb in r_by_block:
        blocks.append(F0[:, s : s + rb])
        s += rb

    # VAR(p) per block
    Phi_blocks: list[list[np.ndarray]] = []
    Q_f_blocks: list[np.ndarray] = []
    for bF in blocks:
        Phi_lags, Qb = _fit_var_ols(bF, p=config.p)
        if config.force_var_stability and config.p > 0:
            Phi_lags = enforce_var_stability(
                Phi_lags,
                shrink=config.var_stability_shrink,
                max_iter=config.var_stability_max_iter,
            )
        Phi_blocks.append(Phi_lags)
        Q_f_blocks.append(Qb)

    # Quarterly loading (simple regression on contemporaneous factors at observed quarter-ends)
    obs_q = np.isfinite(y_q)
    if obs_q.sum() >= r_total:
        Xq = F0[obs_q, :]
        yq = y_q[obs_q]
        beta, *_ = np.linalg.lstsq(Xq, yq, rcond=None)
        Lambda_q0 = beta.reshape(1, -1)
        yq_hat = Xq @ beta
        R_q0 = float(np.nanvar(yq - yq_hat))
    else:
        Lambda_q0 = np.zeros((1, r_total), dtype=float)
        Lambda_q0[0, 0] = 1.0
        R_q0 = 1.0

    R_q0 = max(R_q0, config.quarterly_meas_var_floor)

    # Monthly measurement variance: if idio states are in the state, pin R_m near floor (toolbox-style)
    if config.idio_ar1:
        R_m0 = np.full(nM, config.monthly_meas_var_floor, dtype=float)
    else:
        resid_m = Y_m - (F0 @ Lambda_m0.T)
        R_m0 = np.nanvar(resid_m, axis=0)
        R_m0 = np.where(np.isfinite(R_m0), R_m0, config.monthly_meas_var_floor)
        R_m0 = np.maximum(R_m0, config.monthly_meas_var_floor)

    # Idiosyncratic AR(1) init (monthly)
    rho_m0 = np.full(nM, float(config.rho_idio_init), dtype=float)
    sig2_m0 = np.full(nM, 1.0 - float(config.rho_idio_init) ** 2, dtype=float)
    sig2_m0 = np.maximum(sig2_m0, config.min_var)

    # Quarterly idio (shift register) init; toolbox uses /sum(w^2)=/19 scaling
    rho_q0 = 0.0
    sig2_q0 = 1.0 / float(mm_sum_sq(config.mm_weight_style))
    sig2_q0 = max(sig2_q0, config.min_var)

    return BMParams(
        Phi_blocks=Phi_blocks,
        Q_f_blocks=Q_f_blocks,
        rho_m=rho_m0,
        sig2_m=sig2_m0,
        rho_q=rho_q0,
        sig2_q=sig2_q0,
        Lambda_m=Lambda_m0,
        Lambda_q=Lambda_q0,
        R_diag_m=R_m0,
        R_diag_q=R_q0,
    )
