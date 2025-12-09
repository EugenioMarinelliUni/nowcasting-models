# src/dfm_pipeline/dfm_dyn/pca_init.py

from __future__ import annotations
from dataclasses import dataclass
import numpy as np
import pandas as pd

@dataclass
class PCAInitResult:
    factors: np.ndarray       # (T, q0)
    loadings: np.ndarray      # (N, q0)
    idio_var: np.ndarray      # (N,)
    mean: np.ndarray          # (N,)

def pca_static_factors(X: pd.DataFrame, q0: int) -> PCAInitResult:
    """
    Basic PCA init (no EM, no ragged handling).
    Assumes X is standardized and relatively balanced.
    """

    # T x N
    X_np = X.to_numpy(dtype=float)
    T, N = X_np.shape

    mu = X_np.mean(axis=0)
    Xc = X_np - mu

    # SVD on T x N
    U, s, Vt = np.linalg.svd(Xc, full_matrices=False)
    # factors: T x q0
    F = U[:, :q0] * s[:q0]
    # loadings: N x q0
    Lambda = Vt[:q0, :].T

    # idiosyncratic variances (approx)
    X_hat = F @ Lambda.T
    E = Xc - X_hat
    R_diag = (E ** 2).mean(axis=0)

    return PCAInitResult(
        factors=F,
        loadings=Lambda,
        idio_var=R_diag,
        mean=mu,
    )
