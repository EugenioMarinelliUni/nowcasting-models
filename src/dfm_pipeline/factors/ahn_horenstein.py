# src/dfm_pipeline/factors/ahn_horenstein.py
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Literal, Optional, Dict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

Variant = Literal["ER", "GR"]


@dataclass
class AhnHorensteinResult:
    q_star: int
    variant: Variant
    grid: pd.DataFrame          # columns: k, eig, ratio, growth, cum_var, T, N
    eigenvalues: pd.Series      # length = min(T, N)
    scores: pd.DataFrame        # (optional) T x q_star (if requested)
    loadings: pd.DataFrame      # (optional) N x q_star (if requested)
    meta: dict


# ---------- Internal utilities ----------

def _complete_case(X: pd.DataFrame) -> pd.DataFrame:
    """Listwise deletion to ensure well-defined covariance/SVD."""
    return X.dropna(axis=0, how="any")


def _apply_preprocessing(
    X: pd.DataFrame,
    *,
    standardize: bool = True,
    demean: bool = False,
    detrend: str = "none"
) -> pd.DataFrame:
    """
    Apply optional preprocessing. For ER/GR we usually standardize (use correlation).
    detrend currently supports only 'none' (reserved for future).
    """
    if detrend not in {"none"}:
        raise ValueError("Only detrend='none' is implemented for Ahn–Horenstein at this time.")

    Z = X.copy()

    if demean:
        Z = Z - Z.mean(axis=0)

    if standardize:
        # z-score column-wise with sample std (ddof=1)
        mu = Z.mean(axis=0)
        sd = Z.std(axis=0, ddof=1).replace(0.0, np.nan)
        Z = (Z - mu) / sd

    return Z


def _pca_svd(Z: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    PCA via thin SVD on Z (T x N): Z = U diag(S) V'.
    Returns (U, S, Vt).
    """
    U, S, Vt = np.linalg.svd(Z, full_matrices=False)
    return U, S, Vt


def _cov_eigenvalues_from_svd(S: np.ndarray, T: int, scale: Literal["T", "T-1"] = "T-1") -> np.ndarray:
    """
    Convert singular values S (from SVD of Z) to eigenvalues of covariance matrix.
    If Z is standardized, these are eigenvalues of the correlation matrix.
    """
    if scale == "T":
        denom = T
    else:
        denom = max(T - 1, 1)
    return (S ** 2) / denom


def _build_er_gr_grid(eigs: np.ndarray, k_max: int) -> pd.DataFrame:
    """
    Build DataFrame with columns: k, eig, ratio (ER), growth (GR), cum_var.
    - k runs 1..k_max (1-indexed for readability)
    - eig sorted descending
    - ratio(k) = eig_k / eig_{k+1} for k < k_max; NaN at k_max
    - growth(k): a simple log-growth proxy = log(eig_k) - log(eig_{k+1}) for k < k_max; NaN at k_max
    """
    k_max = int(min(k_max, len(eigs)))
    lam = np.array(eigs[:k_max], dtype=float)
    ratio = np.full(k_max, np.nan)
    growth = np.full(k_max, np.nan)

    for k in range(k_max - 1):  # 0..k_max-2
        if lam[k + 1] > 0 and lam[k] > 0:
            ratio[k] = lam[k] / lam[k + 1]
            growth[k] = math.log(lam[k]) - math.log(lam[k + 1])
        else:
            ratio[k] = np.nan
            growth[k] = np.nan

    tot = lam.sum() if lam.sum() > 0 else np.nan
    cum_var = np.cumsum(lam) / tot if np.isfinite(tot) else np.full_like(lam, np.nan)

    grid = pd.DataFrame(
        {
            "k": np.arange(1, k_max + 1, dtype=int),
            "eig": lam,
            "ratio": ratio,
            "growth": growth,
            "cum_var": cum_var,
        }
    )
    return grid


def _select_q(grid: pd.DataFrame, variant: Variant) -> int:
    """
    Select q by maximizing 'ratio' (ER) or 'growth' (GR).
    Ties: pick the smallest k among maxima. If all-NaN, fallback q=1.
    """
    col = "ratio" if variant == "ER" else "growth"
    s = grid[col]
    if s.notna().any():
        # valid up to k_max-1; at k_max it's NaN by construction
        idx = s.idxmax()  # index in DataFrame (0-based)
        q = int(grid.loc[idx, "k"])
        return q
    return 1


def _plot_scree(eigs: np.ndarray, out_png: Path, title: str = "Scree plot (eigenvalues)") -> None:
    plt.figure(figsize=(7, 4.2))
    x = np.arange(1, len(eigs) + 1)
    plt.plot(x, eigs, marker="o")
    plt.xlabel("Component index k")
    plt.ylabel("Eigenvalue")
    plt.title(title)
    plt.grid(True, alpha=0.3)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()


# ---------- Public API ----------

def choose_q_ahn_horenstein(
    X: pd.DataFrame,
    *,
    r_max: int = 12,
    variant: Variant = "ER",
    standardize: bool = True,
    demean: bool = False,
    detrend: str = "none",
    # optional for artifact enrichments:
    want_scores: bool = False,
    want_loadings: bool = False,
) -> AhnHorensteinResult:
    """
    Full Ahn–Horenstein selector on a (T x N) panel X.

    Steps:
      - preprocessing (demean / standardize / detrend=none)
      - listwise deletion
      - SVD → eigenvalues of covariance (or correlation if standardized)
      - build ER/GR grid and select q*
      - optionally compute scores/loadings at q*
    """
    if variant not in {"ER", "GR"}:
        raise ValueError("variant must be 'ER' or 'GR'.")

    Z = _apply_preprocessing(X, standardize=standardize, demean=demean, detrend=detrend)
    Zc = _complete_case(Z)

    T, N = Zc.shape
    if T < 5 or N < 2:
        raise ValueError("Not enough complete rows/columns after listwise deletion.")

    k_cap = int(min(r_max, min(T, N)))
    if k_cap < 2:
        # degenerate, force q=1
        k_cap = int(min(T, N))

    # SVD once
    U, S, Vt = _pca_svd(Zc.values)

    # covariance eigenvalues (scale doesn't affect ER/GR)
    eigs = _cov_eigenvalues_from_svd(S, T, scale="T-1")

    # build grid up to k_cap
    grid = _build_er_gr_grid(eigs, k_cap)

    # choose q*
    q_star = _select_q(grid, variant=variant)

    # optional scores/loadings at q*
    if q_star > 0:
        scores = pd.DataFrame(U[:, :q_star] * S[:q_star], index=Zc.index, columns=[f"F{k}" for k in range(1, q_star + 1)])
        loadings = pd.DataFrame(Vt[:q_star, :].T, index=Zc.columns, columns=[f"F{k}" for k in range(1, q_star + 1)])
    else:
        scores = pd.DataFrame(index=Zc.index)
        loadings = pd.DataFrame(index=Zc.columns)

    result = AhnHorensteinResult(
        q_star=int(q_star),
        variant=variant,
        grid=grid.assign(T=int(T), N=int(N)),
        eigenvalues=pd.Series(eigs, index=[f"PC{k}" for k in range(1, len(eigs) + 1)], name="eigenvalue"),
        scores=scores if want_scores else pd.DataFrame(index=Zc.index),
        loadings=loadings if want_loadings else pd.DataFrame(index=Zc.columns),
        meta={
            "T": int(T),
            "N": int(N),
            "r_max": int(r_max),
            "variant": variant,
            "standardize": bool(standardize),
            "demean": bool(demean),
            "detrend": detrend,
            "dropped_rows": int(X.shape[0] - Zc.shape[0]),
            "dropped_cols": int(X.shape[1] - Zc.shape[1]),
        },
    )
    return result


def write_ahn_horenstein_artifacts(
    result: AhnHorensteinResult,
    paths: Dict[str, str | Path],
    *,
    write_scree: bool = False,
) -> Dict[str, str]:
    """
    Save outputs to disk. Expected keys in 'paths':
      - grid_csv, summary_json, eigen_csv
      - scores_csv (optional if result.scores not empty)
      - loadings_csv (optional if result.loadings not empty)
      - scree_png (optional when write_scree=True)
    """
    out: Dict[str, str] = {}

    # grid
    Path(paths["grid_csv"]).parent.mkdir(parents=True, exist_ok=True)
    result.grid.to_csv(paths["grid_csv"], index=False)
    out["grid_csv"] = str(paths["grid_csv"])

    # summary
    summary = {
        "variant": result.variant,
        "q_star": int(result.q_star),
        "meta": result.meta,
        "grid_argmax": {
            "ER": int(result.grid.loc[result.grid["ratio"].idxmax(), "k"]) if result.grid["ratio"].notna().any() else None,
            "GR": int(result.grid.loc[result.grid["growth"].idxmax(), "k"]) if result.grid["growth"].notna().any() else None,
        },
    }
    with open(paths["summary_json"], "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    out["summary_json"] = str(paths["summary_json"])

    # eigenvalues
    pd.DataFrame({"component": result.eigenvalues.index, "eigenvalue": result.eigenvalues.values}).to_csv(
        paths["eigen_csv"], index=False
    )
    out["eigen_csv"] = str(paths["eigen_csv"])

    # scores/loadings
    if "scores_csv" in paths and not result.scores.empty:
        result.scores.to_csv(paths["scores_csv"], index=True, index_label="Date")
        out["scores_csv"] = str(paths["scores_csv"])
    if "loadings_csv" in paths and not result.loadings.empty:
        result.loadings.to_csv(paths["loadings_csv"], index=True, index_label="series")
        out["loadings_csv"] = str(paths["loadings_csv"])

    # scree
    if write_scree and "scree_png" in paths:
        _plot_scree(result.eigenvalues.values, Path(paths["scree_png"]), title=f"Scree plot ({result.variant})")
        out["scree_png"] = str(paths["scree_png"])

    return out
