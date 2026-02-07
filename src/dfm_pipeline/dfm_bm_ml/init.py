from __future__ import annotations

from typing import Tuple, Literal

import numpy as np

from .scaling import scale_panel


def fill_for_pca(X: np.ndarray, mode: Literal["mean", "ffill"] = "mean") -> np.ndarray:
    """Fill NaNs for PCA initialization only."""
    Xf = np.asarray(X, dtype=float).copy()

    if mode == "mean":
        col_means = np.nanmean(Xf, axis=0)
        bad = ~np.isfinite(col_means)
        if np.any(bad):
            col_means = col_means.copy()
            col_means[bad] = 0.0
        inds = np.where(np.isnan(Xf))
        Xf[inds] = np.take(col_means, inds[1])
        return Xf

    if mode == "ffill":
        for j in range(Xf.shape[1]):
            col = Xf[:, j]
            last = np.nan
            for t in range(len(col)):
                if np.isnan(col[t]):
                    col[t] = last
                else:
                    last = col[t]
            if np.isnan(col).any():
                m = np.nanmean(col)
                if not np.isfinite(m):
                    m = 0.0
                col[np.isnan(col)] = m
            Xf[:, j] = col
        return Xf

    raise ValueError(f"Unknown fill mode: {mode!r}")


def standardize_panel(Y: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Standardize each series using available observations.

    Kept for compatibility with existing code paths expecting (Ys, mu, sd).
    Prefer scaling.scale_panel(Y, mode="internal_per_run") for new code.
    """
    Ys, scaler = scale_panel(np.asarray(Y, dtype=float), mode="internal_per_run")
    return Ys, scaler.mu, scaler.sd


def pca_init_factors(
    Y_monthly: np.ndarray,
    r: int,
    fill_mode: Literal["mean", "ffill"] = "mean",
) -> Tuple[np.ndarray, np.ndarray]:
    """
    PCA initialization on a (already scaled) monthly panel.

    Inputs:
        Y_monthly: (T, nM) potentially with NaNs
        r: number of factors

    Outputs:
        F: (T, r)
        Lambda: (nM, r)
    """
    Xf = fill_for_pca(np.asarray(Y_monthly, dtype=float), mode=fill_mode)

    # Center filled data for SVD-based PCA
    Xf = Xf - Xf.mean(axis=0, keepdims=True)

    U, s, Vt = np.linalg.svd(Xf, full_matrices=False)
    rr = int(r)
    F = U[:, :rr] * s[:rr]
    Lambda = Vt[:rr, :].T
    return F, Lambda
