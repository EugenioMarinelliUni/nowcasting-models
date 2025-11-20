from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Literal

import numpy as np
import pandas as pd


# ==================== Options ====================

@dataclass
class GCTOptions:
    r_max: int = 12
    standardize: bool = False   # paper uses demeaned data; keep False by default
    demean: bool = True         # must be True to match paper
    detrend: Literal["none"] = "none"  # placeholder; paper does not require detrending
    variant: Literal["PCprop", "ICprop", "PCp1", "ICp1", "PCp2", "ICp2"] = "ICprop"
    s0: float = 0.5             # penalty scale parameter in g_orig
    c_star: float = 2.0         # switch threshold for ψ1/ψ_{rmax}
    save_factors: bool = True
    save_loadings: bool = True
    save_scores: bool = True
    write_scree: bool = True


# ==================== Helpers (preprocess) ====================

def _maybe_standardize(X: np.ndarray, standardize: bool) -> np.ndarray:
    if not standardize:
        return X
    mu = np.nanmean(X, axis=0)
    sd = np.nanstd(X, axis=0, ddof=0)
    sd[sd == 0.0] = 1.0
    return (X - mu) / sd

def _maybe_demean(X: np.ndarray, demean: bool) -> np.ndarray:
    if not demean:
        return X
    mu = np.nanmean(X, axis=0)
    return X - mu


# ==================== Core quantities (paper) ====================

def _compute_V_grid(X: np.ndarray, r_max: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    V_k = (1/(N*T)) * sum_{j>k} s_j^2 for k=0..r_max, where s_j are singular values of X.
    Also returns s, psi_XtX (= s^2), psi_XXt (= s^2).
    """
    T, N = X.shape
    U, s, Vt = np.linalg.svd(X, full_matrices=False)
    s2 = s ** 2
    r_cap = min(r_max, s2.shape[0])
    total = float(np.sum(s2, dtype=np.float64))

    cumsum = np.concatenate(([0.0], np.cumsum(s2)))
    V = np.zeros(r_cap + 1, dtype=float)
    for k in range(r_cap + 1):
        resid = total - float(cumsum[k])
        V[k] = resid / float(N * T)

    psi_XtX = s2.copy()
    psi_XXt = s2.copy()
    return V, s, psi_XtX, psi_XXt


def _sigma2_hat(V: np.ndarray) -> float:
    """σ̂² = V_{r_max}."""
    return float(V[-1])


# ==================== Penalties and criteria ====================

def _gp1(N: int, T: int, sigma2: float) -> float:
    # Bai–Ng gp1 = σ² * ((N+T)/(N*T)) * log( (N*T)/(N+T) )
    return float(sigma2 * (N + T) / (N * T) * np.log((N * T) / (N + T)))

def _gp2(N: int, T: int, sigma2: float) -> float:
    # Bai–Ng gp2 = σ² * ((N+T)/(N*T)) * log( min(N,T) )
    return float(sigma2 * (N + T) / (N * T) * np.log(min(N, T)))

def _gprop(N: int, T: int, sigma2: float, psi_XtX: np.ndarray, s0: float, c_star: float, r_max: int) -> float:
    """
    Proposed adaptive penalty:
      compute ratio ψ1 / ψ_{rmax}.
      if ratio <= c*, use gp2;
      else use g_orig = σ² * (N+T)/(N*T) * [ s0 * ψ1/(N*T) ] / (1 - s0).
    """
    if psi_XtX.size == 0:
        return _gp2(N, T, sigma2)
    psi1 = float(psi_XtX[0])
    idx = max(0, min(r_max, len(psi_XtX)) - 1)  # index of ψ_{rmax}
    psir = float(psi_XtX[idx])
    ratio = psi1 / max(psir, 1e-16)
    if ratio <= c_star:
        return _gp2(N, T, sigma2)
    g_orig = sigma2 * (N + T) / (N * T) * (s0 * (psi1 / (N * T)) / (1.0 - s0))
    return float(g_orig)

def _pc_value(Vk: float, k: int, gscalar: float) -> float:
    return float(Vk + k * gscalar)

def _ic_value(Vk: float, k: int, gscalar: float, sigma2: float) -> float:
    # ICprop: log(Vk) + k * g_prop / σ̂²
    return float(np.log(max(Vk, 1e-300)) + k * (gscalar / max(sigma2, 1e-300)))


def _select_k(V: np.ndarray,
              variant: str,
              N: int, T: int,
              sigma2: float,
              psi_XtX: np.ndarray,
              s0: float, c_star: float,
              r_max: int) -> Tuple[int, List[Dict[str, float]], float, Dict[str, float]]:
    """
    Evaluate criterion over k=0..r_max according to chosen variant.
    Return k*, full grid records, g_prop used, and a dict with penalty scalars.
    """
    grid: List[Dict[str, float]] = []
    g_p1 = _gp1(N, T, sigma2)
    g_p2 = _gp2(N, T, sigma2)
    g_prop = _gprop(N, T, sigma2, psi_XtX, s0, c_star, r_max=r_max)

    best_k = 0
    best_val = float("inf")

    for k in range(len(V)):  # len(V) = r_max+1
        Vk = float(V[k])
        if variant == "PCprop":
            val = _pc_value(Vk, k, g_prop)
        elif variant == "ICprop":
            val = _ic_value(Vk, k, g_prop, sigma2)
        elif variant == "PCp1":
            val = _pc_value(Vk, k, g_p1)
        elif variant == "ICp1":
            val = float(np.log(max(Vk, 1e-300)) + k * ((N + T) / (N * T) * np.log((N * T) / (N + T))))
        elif variant == "PCp2":
            val = _pc_value(Vk, k, g_p2)
        elif variant == "ICp2":
            val = float(np.log(max(Vk, 1e-300)) + k * ((N + T) / (N * T) * np.log(min(N, T))))
        else:
            raise ValueError("unknown variant")

        grid.append({"k": float(k), "V_k": Vk, "crit": float(val)})
        if val < best_val:
            best_val = val
            best_k = k

    pens = {"g_p1": g_p1, "g_p2": g_p2, "g_prop": g_prop, "sigma2": sigma2}
    return int(best_k), grid, float(g_prop), pens


# ==================== Factors/Loadings at k* ====================

def _pca_FL(X: np.ndarray, k: int) -> Tuple[np.ndarray, np.ndarray]:
    T, _ = X.shape
    if k == 0:
        return np.zeros((T, 0)), np.zeros((X.shape[1], 0))
    U, s, Vt = np.linalg.svd(X, full_matrices=False)
    Uq = U[:, :k]
    sq = s[:k]
    Vq = Vt[:k, :].T
    F = np.sqrt(T) * Uq
    L = Vq * sq  # N x k
    return F, L


# ==================== Public API ====================

def run_gct_selection(X_df: pd.DataFrame, opts: GCTOptions) -> Dict:
    # preprocessing
    X = X_df.values.astype(float)
    X = _maybe_demean(X, opts.demean)
    X = _maybe_standardize(X, opts.standardize)

    T, N = X.shape
    r_max = min(opts.r_max, min(T, N))

    # V_k and eigenvalues
    V, s, psi_XtX, psi_XXt = _compute_V_grid(X, r_max)
    sigma2 = _sigma2_hat(V)

    # select k
    k_star, grid, gprop_used, pens = _select_k(
        V, opts.variant, N, T, sigma2, psi_XtX, opts.s0, opts.c_star, r_max=r_max
    )

    # factors/loadings at k_star
    F_star, L_star = _pca_FL(X, k_star)
    eigvals = (s ** 2) / float(T)  # scree on XX'/T scale

    return {
        "k_star": int(k_star),
        "sigma2": float(sigma2),
        "gprop": float(gprop_used),
        "grid": grid,
        "F": F_star,
        "L": L_star,
        "eigvals": eigvals,
        "meta": {
            "T": int(T),
            "N": int(N),
            "variant": opts.variant,
            "s0": float(opts.s0),
            "c_star": float(opts.c_star),
            "r_max": int(r_max),
            "demean": bool(opts.demean),
            "standardize": bool(opts.standardize),
            "penalties": pens,
        },
    }


def write_gct_artifacts(
    out_dir: Path,
    X_df: pd.DataFrame,
    sel: Dict,
    write_scree: bool = True,
    save_factors: bool = True,
    save_loadings: bool = True,
    save_scores: bool = True,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # grid
    grid_df = pd.DataFrame(sel["grid"])
    (out_dir / "grid.csv").write_text(grid_df.to_csv(index=False))

    # summary
    summary = {
        "k_star": sel["k_star"],
        "sigma2": sel["sigma2"],
        "gprop_used": sel["gprop"],
        "meta": sel["meta"],
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    # eigen
    eig_df = pd.DataFrame({"eigenvalue": sel["eigvals"]})
    (out_dir / "eigen.csv").write_text(eig_df.to_csv(index=False))

    # factors (scores), loadings
    F: np.ndarray = sel["F"]
    L: np.ndarray = sel["L"]
    idx = X_df.index

    if save_factors and F.size:
        F_df = pd.DataFrame(F, index=idx, columns=[f"F{j+1}" for j in range(F.shape[1])])
        (out_dir / "factors.csv").write_text(F_df.to_csv())

    if save_loadings and L.size:
        L_df = pd.DataFrame(L, index=X_df.columns, columns=[f"F{j+1}" for j in range(L.shape[1])])
        (out_dir / "loadings.csv").write_text(L_df.to_csv())

    if save_scores and F.size:
        # in this PCA parameterization, factors==scores
        S_df = pd.DataFrame(F, index=idx, columns=[f"F{j+1}" for j in range(F.shape[1])])
        (out_dir / "scores.csv").write_text(S_df.to_csv())

    if write_scree:
        scree = pd.DataFrame(
            {"component": np.arange(1, sel["eigvals"].shape[0] + 1), "eigenvalue": sel["eigvals"]}
        )
        (out_dir / "scree.csv").write_text(scree.to_csv(index=False))
