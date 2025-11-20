# src/dfm_pipeline/factors/baing.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal, Mapping, Union
from os import PathLike

import numpy as np
import pandas as pd


ICType = Literal["ICp1", "ICp2", "ICp3", "BIC3", "AIC3"]
DetrendType = Literal["none"]  # reserved for future (hp, linear, stb, etc.)


@dataclass
class BaiNgResult:
    r_star: int
    ic_type: ICType
    grid: pd.DataFrame          # columns: r, ICp1, ICp2, ICp3, BIC3, AIC3, SSE, T, N
    factors: pd.DataFrame       # T x r* (index aligned with input)
    loadings: pd.DataFrame      # N x r*
    eigenvalues: pd.Series      # length = min(T, N)
    meta: dict


def _standardize_columns(X: pd.DataFrame) -> pd.DataFrame:
    """Column-standardize (z-score) with mean/variance computed over available (non-NA) entries."""
    mu = X.mean(axis=0, skipna=True)
    sd = X.std(axis=0, ddof=1, skipna=True).replace(0.0, np.nan)
    return (X - mu) / sd


def _demean_columns(X: pd.DataFrame) -> pd.DataFrame:
    """Column demean with mean computed over available (non-NA) entries."""
    mu = X.mean(axis=0, skipna=True)
    return X - mu


def _complete_case(Z: pd.DataFrame) -> pd.DataFrame:
    """Simple listwise deletion for missing values (robust & reproducible)."""
    return Z.dropna(axis=0, how="any")


def _maybe_remap_index(current: pd.Index, mapper: object) -> pd.Index:
    """
    If `mapper` is a pandas Series/DataFrame with .loc, try to remap `current` via mapper.loc[current].
    Otherwise, return `current`.
    """
    try:
        loc = getattr(mapper, "loc")
    except AttributeError:
        return current
    try:
        remapped = loc[current]
        return pd.Index(remapped)
    except Exception:
        return current


def bai_ng_criteria(
    X: pd.DataFrame,
    r_max: int = 12,
    ic_type: ICType = "ICp2",
    *,
    detrend: DetrendType = "none",
    standardize: bool = True,
    demean: bool = False,
    index: object | None = None,      # accept Series/Index/None; robustly handled
    colnames: object | None = None,   # accept Series/Index/list/None; robustly handled
) -> BaiNgResult:
    """
    Compute Bai–Ng information criteria on a (possibly preprocessed) panel X.

    Preprocessing:
      - detrend: currently only "none" (placeholder for future options).
      - standardize: column z-score if True.
      - demean: column-mean removal if True (ignored if standardize=True).
    """
    # 1) detrend (placeholder; extend later)
    Z = X.copy()

    # 2) standardize / demean / none
    if standardize:
        Z = _standardize_columns(Z)
        std_flag = True
        dm_flag = False
    elif demean:
        Z = _demean_columns(Z)
        std_flag = False
        dm_flag = True
    else:
        std_flag = False
        dm_flag = False

    # 3) complete-case rows
    Zc = _complete_case(Z)
    if Zc.shape[0] < 5 or Zc.shape[1] < 2:
        raise ValueError("Not enough complete rows/columns after listwise deletion.")

    T, N = Zc.shape
    r_max = int(min(r_max, min(T, N)))

    # 4) SVD once on the processed matrix
    U, S, Vt = np.linalg.svd(Zc.values, full_matrices=False)  # Zc = U diag(S) V'

    # 5) SSE grid over r = 0..r_max
    sse_list = []
    for r in range(0, r_max + 1):
        if r == 0:
            sse = float(np.sum(Zc.values * Zc.values))
        else:
            U_r = U[:, :r]
            S_r = S[:r]
            V_r = Vt[:r, :].T
            Zhat = (U_r * S_r) @ V_r.T
            R = Zc.values - Zhat
            sse = float(np.sum(R * R))
        sse_list.append(sse)

    # 6) Information criteria
    rows = []
    for r, sse in enumerate(sse_list):
        sigma2 = sse / (T * N)
        ICp1 = np.log(sigma2) + r * ((T + N) / (T * N)) * np.log((T * N) / (T + N))
        ICp2 = np.log(sigma2) + r * ((T + N) / (T * N)) * np.log(min(T, N))
        ICp3 = np.log(sigma2) + r * (np.log(min(T, N)) / min(T, N))
        AIC3 = np.log(sigma2) + 2.0 * r * (T + N) / (T * N)
        BIC3 = np.log(sigma2) + np.log(T * N) * r * (T + N) / (T * N)

        rows.append(
            dict(r=r, SSE=sse, sigma2=sigma2, ICp1=ICp1, ICp2=ICp2, ICp3=ICp3, AIC3=AIC3, BIC3=BIC3, T=T, N=N)
        )
    grid = pd.DataFrame(rows).set_index("r")

    if ic_type not in {"ICp1", "ICp2", "ICp3", "AIC3", "BIC3"}:
        raise ValueError(f"ic_type '{ic_type}' not in {{ICp1, ICp2, ICp3, AIC3, BIC3}}")
    r_star = int(grid[ic_type].idxmin())

    # 7) Factors/loadings at r*
    if r_star == 0:
        F = pd.DataFrame(index=Zc.index)
        L = pd.DataFrame(index=Zc.columns)
    else:
        U_r = U[:, :r_star]
        S_r = S[:r_star]
        V_r = Vt[:r_star, :].T
        F = pd.DataFrame(U_r * S_r, index=Zc.index, columns=[f"F{k+1}" for k in range(r_star)])
        L = pd.DataFrame(V_r, index=Zc.columns, columns=[f"F{k+1}" for k in range(r_star)])

    eigen = pd.Series(S, index=[f"PC{k+1}" for k in range(len(S))], name="singular_value")

    meta = {
        "ic_type": ic_type,
        "r_star": r_star,
        "T": int(T),
        "N": int(N),
        "r_max": int(r_max),
        "dropped_rows": int(X.shape[0] - Zc.shape[0]),
        "dropped_cols": int(X.shape[1] - Zc.shape[1]),
        "preprocess": {
            "detrend": detrend,
            "standardize": std_flag,
            "demean": dm_flag,
        },
        "missing_handling": "listwise_deletion",
    }

    # optional remapping of index/column labels
    if not F.empty and index is not None:
        F.index = _maybe_remap_index(F.index, index)
    if not L.empty and colnames is not None:
        L.index = _maybe_remap_index(L.index, colnames)

    return BaiNgResult(
        r_star=r_star,
        ic_type=ic_type,
        grid=grid.reset_index(),
        factors=F,
        loadings=L,
        eigenvalues=eigen,
        meta=meta,
    )


def write_baing_artifacts(
    result: BaiNgResult,
    paths: Mapping[str, Union[str, PathLike[str]]],
) -> dict[str, str]:
    """
    Save outputs to disk using the provided paths dict.
    Expected keys:
      - required: 'grid_csv', 'summary_json'
      - optional: 'factors_csv', 'loadings_csv', 'eigen_csv'
    """
    written: dict[str, str] = {}

    # required
    result.grid.to_csv(paths["grid_csv"], index=False)
    written["grid_csv"] = str(paths["grid_csv"])

    summary = {
        "ic_type": result.ic_type,
        "r_star": result.r_star,
        "meta": result.meta,
        "grid_min": {
            k: float(result.grid.loc[result.grid[k].idxmin(), k]) if k in result.grid.columns and not result.grid.empty else None
            for k in ["ICp1", "ICp2", "ICp3", "AIC3", "BIC3"]
        },
    }
    with open(paths["summary_json"], "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    written["summary_json"] = str(paths["summary_json"])

    # optional
    if "factors_csv" in paths and not result.factors.empty:
        result.factors.to_csv(paths["factors_csv"], index=True, index_label="Date")
        written["factors_csv"] = str(paths["factors_csv"])

    if "loadings_csv" in paths and not result.loadings.empty:
        result.loadings.to_csv(paths["loadings_csv"], index=True, index_label="series")
        written["loadings_csv"] = str(paths["loadings_csv"])

    if "eigen_csv" in paths:
        pd.DataFrame(
            {"component": result.eigenvalues.index, "singular_value": result.eigenvalues.values}
        ).to_csv(paths["eigen_csv"], index=False)
        written["eigen_csv"] = str(paths["eigen_csv"])

    return written
