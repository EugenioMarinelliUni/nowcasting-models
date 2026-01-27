from __future__ import annotations

"""Fast helpers for PCA initialization.

This module keeps the same public behavior as :mod:`dfm_pipeline.dfm_bm_ml.init`,
but speeds up NaN filling in ``mode='ffill'`` by avoiding Python-level loops.

The original module is kept unchanged for comparison.
"""

from typing import Tuple, Literal

import numpy as np


def _ffill_2d_inplace(X: np.ndarray) -> None:
    """Forward-fill NaNs down each column *in-place*.

    Notes
    -----
    - Leading NaNs remain NaN; caller can fill them with a column mean after.
    - This is a vectorized implementation based on cumulative maxima of last-valid indices.
    """
    if X.ndim != 2:
        raise ValueError("X must be 2D")

    Tn, n = X.shape
    if Tn == 0 or n == 0:
        return

    mask = np.isnan(X)
    if not mask.any():
        return

    # For each column, build index of most recent non-NaN observation.
    idx = np.where(~mask, np.arange(Tn, dtype=int)[:, None], 0)
    np.maximum.accumulate(idx, axis=0, out=idx)

    # Gather last valid values; this forward-fills *including* rows that are non-NaN,
    # but we only write into NaN positions.
    out = X[idx, np.arange(n)[None, :]]
    X[mask] = out[mask]


def fill_for_pca_fast(X: np.ndarray, mode: Literal["mean", "ffill"] = "mean") -> np.ndarray:
    """Fill NaNs for PCA initialization only (fast version)."""
    Xf = X.copy()

    if mode == "mean":
        col_means = np.nanmean(Xf, axis=0)
        inds = np.where(np.isnan(Xf))
        Xf[inds] = np.take(col_means, inds[1])
        return Xf

    if mode == "ffill":
        _ffill_2d_inplace(Xf)
        # Fill any remaining NaNs (leading NaNs or all-NaN columns) with column means.
        if np.isnan(Xf).any():
            col_means = np.nanmean(Xf, axis=0)
            # Columns that are all NaN will produce nan mean; fall back to 0.0 to match
            # the original behavior as closely as possible (original used nanmean on col).
            col_means = np.where(np.isfinite(col_means), col_means, 0.0)
            inds = np.where(np.isnan(Xf))
            Xf[inds] = np.take(col_means, inds[1])
        return Xf

    raise ValueError(f"Unknown fill mode: {mode!r}")


def standardize_panel(Y: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Same as :func:`dfm_pipeline.dfm_bm_ml.init.standardize_panel`."""
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
    """Same as original PCA init, but calls :func:`fill_for_pca_fast`."""
    Xf = fill_for_pca_fast(Y_monthly, mode=fill_mode)
    Xf = Xf - Xf.mean(axis=0, keepdims=True)
    U, s, Vt = np.linalg.svd(Xf, full_matrices=False)
    F = U[:, :r] * s[:r]
    Lambda = Vt[:r, :].T
    return F, Lambda
