# src/dfm_pipeline/dfm_simple/em_pca.py
from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd


def _pca_svd(X: np.ndarray, n_factors: int) -> tuple[np.ndarray, np.ndarray]:
    """
    Basic PCA via SVD on a (T x N) zero-mean matrix X.

    Returns:
      F : (T x r) factor scores (scaled PC scores)
      L : (N x r) loadings
    """
    # Center columns
    Xc = X - X.mean(axis=0, keepdims=True)

    # SVD: Xc = U S V'
    U, S, Vt = np.linalg.svd(Xc, full_matrices=False)

    # Keep top r components
    r = n_factors
    U_r = U[:, :r]
    S_r = S[:r]
    V_r = Vt[:r, :].T  # (N x r)

    # Factors: F = U_r * S_r  (T x r)
    F = U_r * S_r

    # Loadings: L = V_r      (N x r)
    L = V_r

    return F, L


def em_pca(
    X: pd.DataFrame,
    n_factors: int,
    max_iter: int = 100,
    tol: float = 1e-4,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    EM-PCA on a ragged panel.

    Algorithm:
      1) Initialize missing entries with column means; PCA -> F, Lambda.
      2) Iterate:
         - E-step: impute missing entries using current F, Lambda.
         - M-step: PCA on completed matrix -> updated F, Lambda.
         - Stop when change in F is small.

    Parameters
    ----------
    X : DataFrame (T x N), may contain NaNs.
    n_factors : int, number of static factors r.
    max_iter : int, maximum EM iterations.
    tol : float, relative tolerance on factor change.

    Returns
    -------
    F_df : DataFrame (T x r), factors.
    Lambda_df : DataFrame (N x r), loadings.
    X_hat_df : DataFrame (T x N), reconstructed panel F Lambda'.
    """
    if not isinstance(X, pd.DataFrame):
        raise TypeError("X must be a pandas DataFrame.")

    T, N = X.shape
    X_values = X.to_numpy(dtype=float)

    # Mask: True where original data is missing
    missing_mask = np.isnan(X_values)

    # Initialize missing with column means
    col_means = np.nanmean(X_values, axis=0)
    # Columns that are all-NaN: fallback to 0
    col_means = np.where(np.isnan(col_means), 0.0, col_means)
    X_init = np.where(missing_mask, col_means[None, :], X_values)

    # Initial PCA
    F, L = _pca_svd(X_init, n_factors=n_factors)
    prev_F = F.copy()

    for _ in range(max_iter):
        # E-step: reconstruct X_hat = F L'
        X_hat = F @ L.T

        # Impute missing entries with reconstructed values, keep observed
        X_complete = np.where(missing_mask, X_hat, X_values)

        # M-step: PCA on completed data
        F_new, L_new = _pca_svd(X_complete, n_factors=n_factors)

        # Check convergence on F
        num = np.linalg.norm(F_new - prev_F)
        den = np.linalg.norm(prev_F) + 1e-12
        rel_change = num / den

        F = F_new
        L = L_new
        prev_F = F_new

        if rel_change < tol:
            break

    # Final reconstruction
    X_hat_final = F @ L.T

    factor_cols = [f"F{i+1}" for i in range(n_factors)]
    F_df = pd.DataFrame(F, index=X.index, columns=factor_cols)
    Lambda_df = pd.DataFrame(L, index=X.columns, columns=factor_cols)
    X_hat_df = pd.DataFrame(X_hat_final, index=X.index, columns=X.columns)

    return F_df, Lambda_df, X_hat_df
