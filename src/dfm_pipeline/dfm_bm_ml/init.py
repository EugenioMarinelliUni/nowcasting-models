from __future__ import annotations

from typing import Tuple, Literal

import numpy as np


def fill_for_pca(X: np.ndarray, mode: Literal["mean", "ffill"] = "mean") -> np.ndarray:
    """Fill NaNs for PCA initialization only."""
    Xf = X.copy()
    if mode == "mean":
        col_means = np.nanmean(Xf, axis=0)
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
                col[np.isnan(col)] = m
            Xf[:, j] = col
        return Xf
    raise ValueError(f"Unknown fill mode: {mode!r}")


def standardize_panel(Y: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Standardize each series using available observations."""
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
    """
    Return initial (F, Lambda) from PCA on filled monthly panel.

    Y_monthly: (T, nM)
    F: (T, r), Lambda: (nM, r)
    """
    Xf = fill_for_pca(Y_monthly, mode=fill_mode)
    Xf = Xf - Xf.mean(axis=0, keepdims=True)
    U, s, Vt = np.linalg.svd(Xf, full_matrices=False)
    F = U[:, :r] * s[:r]
    Lambda = Vt[:r, :].T
    return F, Lambda
