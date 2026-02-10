from __future__ import annotations

"""Fast helpers for PCA initialization.

This module must not apply its own scaling when used by the fast BM-DFM fitters.
Scaling is handled centrally (scaling_mode) before PCA/EM.
"""

from typing import Tuple, Literal

import numpy as np


def _ffill_2d_inplace(X: np.ndarray) -> None:
    """Forward-fill NaNs down each column in-place."""
    if X.ndim != 2:
        raise ValueError("X must be 2D")

    Tn, n = X.shape
    if Tn == 0 or n == 0:
        return

    mask = np.isnan(X)
    if not mask.any():
        return

    idx = np.where(~mask, np.arange(Tn, dtype=int)[:, None], 0)
    np.maximum.accumulate(idx, axis=0, out=idx)

    out = X[idx, np.arange(n)[None, :]]
    X[mask] = out[mask]


def fill_for_pca_fast(X: np.ndarray, mode: Literal["mean", "ffill"] = "mean") -> np.ndarray:
    """Fill NaNs for PCA initialization only (fast)."""
    Xf = X.copy()

    if mode == "mean":
        col_means = np.nanmean(Xf, axis=0)
        inds = np.where(np.isnan(Xf))
        Xf[inds] = np.take(col_means, inds[1])
        return Xf

    if mode == "ffill":
        _ffill_2d_inplace(Xf)
        if np.isnan(Xf).any():
            col_means = np.nanmean(Xf, axis=0)
            col_means = np.where(np.isfinite(col_means), col_means, 0.0)
            inds = np.where(np.isnan(Xf))
            Xf[inds] = np.take(col_means, inds[1])
        return Xf

    raise ValueError(f"Unknown fill mode: {mode!r}")


def standardize_panel(Y: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Per-run standardization helper (kept for backward compatibility)."""
    mu = np.nanmean(Y, axis=0)
    sd = np.nanstd(Y, axis=0, ddof=0)
    sd = np.where(sd == 0.0, 1.0, sd)
    Ys = (Y - mu) / sd
    return Ys, mu, sd


def pca_init_factors(
    Y_monthly: np.ndarray,
    r: int,
    fill_mode: Literal["mean", "ffill"] = "mean",
) -> Tuple[np.ndarray, np.ndarray]:
    """PCA init on already-scaled monthly panel."""
    Xf = fill_for_pca_fast(Y_monthly, mode=fill_mode)
    Xf = Xf - Xf.mean(axis=0, keepdims=True)
    U, s, Vt = np.linalg.svd(Xf, full_matrices=False)
    F = U[:, :r] * s[:r]
    Lambda = Vt[:r, :].T
    return F, Lambda
