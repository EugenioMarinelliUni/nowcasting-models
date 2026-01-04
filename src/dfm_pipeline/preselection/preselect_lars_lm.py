# FILE: src/dfm_pipeline/preselection/preselect_lars_lm.py
#!/usr/bin/env python3
from __future__ import annotations

"""Faithful(ish) Linzenich–Meunier toolbox LARS preselection.

Implements the ranking logic from Variable_selection_vF.R:
  - Run LARS on the current batch.
  - For each variable, compute the number of zeros across the coefficient path.
    (Lower => enters earlier.)
  - Keep only variables with a unique zero-count, append them to the order, remove them
    from the batch, and repeat.
  - If no unique zero-count exists, append all remaining variables at the end.

This wrapper also outputs a selected subset compatible with the project's
gate -> rank -> de-dup(tau) -> bounds mechanics.
"""

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

try:
    from sklearn.linear_model import lars_path
except Exception:  # pragma: no cover
    lars_path = None  # type: ignore[assignment]


def _align_dropna(X: pd.DataFrame, y: pd.Series) -> Tuple[pd.DataFrame, pd.Series]:
    common = X.index.intersection(y.index)
    yc = y.loc[common].dropna()
    Xc = X.loc[yc.index]
    Xc = Xc.dropna(axis=1, how="all")
    return Xc, yc


def _abs_corr_all(X: pd.DataFrame, y: pd.Series) -> pd.Series:
    vals: Dict[str, float] = {}
    for c in X.columns:
        s = pd.concat([X[c], y], axis=1).dropna()
        if len(s) < 3:
            vals[c] = np.nan
        else:
            vals[c] = float(abs(s.iloc[:, 0].corr(s.iloc[:, 1])))
    return pd.Series(vals, dtype="float64")


def _dedup_by_corr(features: List[str], X: pd.DataFrame, tau: float) -> List[str]:
    kept: List[str] = []
    for f in features:
        accept = True
        xf = X[f]
        for k in kept:
            s = pd.concat([xf, X[k]], axis=1).dropna()
            if len(s) < 3:
                continue
            if abs(float(s.iloc[:, 0].corr(s.iloc[:, 1]))) >= tau:
                accept = False
                break
        if accept:
            kept.append(f)
    return kept


def _enforce_min_max(
    selected: List[str],
    ranked: List[str],
    min_features: int,
    max_features: int,
) -> List[str]:
    s = list(selected)
    if len(s) < min_features:
        for r in ranked:
            if r not in s:
                s.append(r)
                if len(s) >= min_features:
                    break
    if max_features > 0:
        s = s[:max_features]
    return s


def _center_cols(Xv: np.ndarray) -> np.ndarray:
    mu = np.nanmean(Xv, axis=0)
    return Xv - mu


def _center_vec(yv: np.ndarray) -> np.ndarray:
    return yv - float(np.nanmean(yv))


def _lars_unique_zero_count_order(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    zero_tol: float = 1e-12,
    max_iter: int | None = None,
) -> Tuple[List[str], Dict[str, int]]:
    """Return (ranked_vars, pecking_order_map) following the R toolbox logic."""
    if lars_path is None:
        raise RuntimeError("scikit-learn is required for Linzenich–Meunier LARS selection")

    X0, y0 = _align_dropna(X, y)
    Z = pd.concat([y0, X0], axis=1).dropna()
    if Z.empty:
        return [], {}

    yv_all = _center_vec(Z.iloc[:, 0].to_numpy(dtype=float))
    X_all = Z.iloc[:, 1:]
    cols_all = list(X_all.columns)
    Xv_all = _center_cols(X_all.to_numpy(dtype=float))

    remaining_cols = cols_all.copy()
    remaining_idx = list(range(len(cols_all)))
    ranked: List[str] = []
    peck: Dict[str, int] = {}

    while remaining_cols:
        Xv = Xv_all[:, remaining_idx]
        if Xv.shape[1] == 0:
            break

        # lars_path returns coefs shape (n_features, n_alphas)
        kwargs = {"method": "lar"}

        if max_iter is not None:

            kwargs["max_iter"] = int(max_iter)

        _, _, coefs = lars_path(Xv, yv_all, **kwargs)

        coefs = np.asarray(coefs, dtype=float)
        if coefs.ndim != 2 or coefs.shape[0] != len(remaining_cols):
            # Defensive fallback: append remaining as-is.
            for c in remaining_cols:
                if c not in peck:
                    peck[c] = int(coefs.shape[1]) if coefs.ndim == 2 else 0
            ranked.extend(remaining_cols)
            break

        is_zero = np.abs(coefs) <= float(zero_tol)
        zero_counts = is_zero.sum(axis=1).astype(int)

        # Mimic: summarise(sum(.==0)) %>% arrange(V1) %>% group_by(V1) %>% filter(n()==1)
        vc = pd.Series(zero_counts, index=remaining_cols)
        freq = vc.value_counts()
        uniq_vals = set(freq[freq == 1].index.tolist())

        if not uniq_vals:
            # Mimic the "special procedure": put all remaining at the end.
            ranked.extend(remaining_cols)
            for c in remaining_cols:
                peck.setdefault(c, 1)
            break

        uniq = vc[vc.isin(list(uniq_vals))].sort_values(ascending=True)
        picked = list(uniq.index)

        ranked.extend(picked)
        for c in picked:
            peck[c] = int(vc.loc[c])

        # Remove picked from remaining.
        picked_set = set(picked)
        keep_mask = [c not in picked_set for c in remaining_cols]
        remaining_cols = [c for c in remaining_cols if c not in picked_set]
        remaining_idx = [idx for idx, keep in zip(remaining_idx, keep_mask) if keep]

    # Ensure full ranking (no omissions).
    missing = [c for c in cols_all if c not in set(ranked)]
    if missing:
        ranked.extend(missing)
        for c in missing:
            peck.setdefault(c, 1)

    return ranked, peck


def run_lars_lm_preselection(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """Linzenich–Meunier-style LARS ranking + project selection mechanics.

    Expected params keys
    -------------------
    - min_features       : int (default 10)
    - max_features       : int (default 80)
    - dedup_tau          : float (default 0.97)
    - lars_gate_topn     : int (default 0 => no top-n gate)
    - lars_zero_tol      : float (default 1e-12)
    - lars_max_iter      : int | None (default None)
    """
    min_features = int(params.get("min_features", 10))
    max_features = int(params.get("max_features", 80))
    dedup_tau = float(params.get("dedup_tau", 0.97))
    gate_topn = int(params.get("lars_gate_topn", 0))
    zero_tol = float(params.get("lars_zero_tol", 1e-12))
    max_iter_raw = params.get("lars_max_iter", None)
    max_iter = None if max_iter_raw in (None, "", "none") else int(max_iter_raw)

    X, y = _align_dropna(X_rank, y_rank)

    ranked, peck = _lars_unique_zero_count_order(
        X,
        y,
        zero_tol=zero_tol,
        max_iter=max_iter,
    )

    # Gate (optional): keep only top-n of the LARS order.
    keep = ranked
    if gate_topn and gate_topn > 0:
        keep = ranked[:gate_topn]

    # De-dup + bounds
    dedup = _dedup_by_corr(keep, X, tau=dedup_tau)
    selected = _enforce_min_max(
        dedup, ranked, min_features=min_features, max_features=max_features
    )

    # Ranking table
    abs_corr = _abs_corr_all(X, y)
    rank_index = pd.Index(ranked, name="variable")
    rank_df = pd.DataFrame(index=rank_index)
    rank_df["rank"] = np.arange(1, len(rank_df) + 1, dtype=int)
    rank_df["pecking_order"] = [peck.get(v, np.nan) for v in rank_df.index]
    rank_df["abs_corr"] = abs_corr.reindex(rank_df.index)
    rank_df["selected"] = rank_df.index.isin(selected)

    meta: Dict[str, Any] = {
        "method": "lars_lm",
        "n_obs": int(len(X)),
        "n_features_in": int(X.shape[1]),
        "n_features_selected": int(len(selected)),
        "hyperparams": {
            "min_features": min_features,
            "max_features": max_features,
            "dedup_tau": dedup_tau,
            "lars_gate_topn": gate_topn,
            "lars_zero_tol": zero_tol,
            "lars_max_iter": max_iter,
        },
    }

    return rank_df, selected, meta
