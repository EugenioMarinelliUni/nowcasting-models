# FILE: src/dfm_pipeline/preselection/preselect_tstat_lm.py
#!/usr/bin/env python3
from __future__ import annotations

"""Faithful(ish) Linzenich–Meunier toolbox t-stat preselection.

Implements the ranking logic from Variable_selection_vF.R:
  - For each regressor x_j, run OLS:
        y_t = c + b * x_{j,t} + sum_{k=1..L} a_k * y_{t-k} + e_t
  - Score is |t(b)|.

Differences vs the R script:
  - This wrapper returns both a full ranking table and a selected subset compatible
    with the project's gate -> rank -> de-dup(tau) -> bounds mechanics.
  - Optional gating by p-value can be enabled (disabled by default).
"""

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

from dfm_pipeline.preselection.alignment import align_X_y_dropna

try:
    import statsmodels.api as sm
except Exception:  # pragma: no cover
    sm = None


def _align_dropna(X: pd.DataFrame, y: pd.Series | pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    return align_X_y_dropna(X, y)


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


def _ols_t_and_p(yv: np.ndarray, Xv: np.ndarray, coef_idx: int) -> Tuple[float, float]:
    """Return (t_stat, p_value) for coefficient at coef_idx in Xv.

    Xv is assumed to be WITHOUT constant; constant is added internally.
    coef_idx refers to the column index in Xv (0-based) for which we want stats.
    """
    if sm is not None:
        Xd = sm.add_constant(Xv, has_constant="add")
        res = sm.OLS(yv, Xd, missing="drop").fit()
        # +1 for constant
        t = float(res.tvalues[1 + coef_idx])
        p = float(res.pvalues[1 + coef_idx])
        return t, p

    # Minimal fallback: OLS + homoskedastic SE
    Xd = np.column_stack([np.ones(len(yv)), Xv])
    beta = np.linalg.lstsq(Xd, yv, rcond=None)[0]
    resid = yv - Xd @ beta
    n, p = Xd.shape
    dof = max(n - p, 1)
    s2 = float((resid @ resid) / dof)
    cov = s2 * np.linalg.inv(Xd.T @ Xd)
    se = float(np.sqrt(max(cov[1 + coef_idx, 1 + coef_idx], 1e-12)))
    t = float(beta[1 + coef_idx] / se)
    # Normal approximation for p-value
    from math import erf, sqrt

    pval = 2.0 * (1.0 - 0.5 * (1.0 + erf(abs(t) / sqrt(2.0))))
    return t, float(pval)


def run_tstat_lm_preselection(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """Linzenich–Meunier-style t-stat ranking + project selection mechanics.

    Expected params keys
    -------------------
    - min_features : int (default 10)
    - max_features : int (default 80)
    - dedup_tau    : float (default 0.97)
    - target_lags  : int (default 2; matches Variable_selection_vF.R)
    - tstat_alpha  : float | None (default None => no gating)
    - tstat_topn   : int (default 0 => keep all after gating)
    """
    min_features = int(params.get("min_features", 10))
    max_features = int(params.get("max_features", 80))
    dedup_tau = float(params.get("dedup_tau", 0.97))
    target_lags = int(params.get("target_lags", 2))

    alpha_raw = params.get("tstat_alpha", None)
    tstat_alpha = None if alpha_raw is None else float(alpha_raw)
    tstat_topn = int(params.get("tstat_topn", 0))

    X, y = _align_dropna(X_rank, y_rank)

    # Precompute y lags once.
    ylags = [y.shift(k) for k in range(1, max(target_lags, 0) + 1)]

    abs_t: Dict[str, float] = {}
    pvals: Dict[str, float] = {}

    for c in X.columns:
        cols = [y] + ylags + [X[c]]
        Z = pd.concat(cols, axis=1).dropna()
        if len(Z) < 8:
            abs_t[c] = np.nan
            pvals[c] = np.nan
            continue

        yv = Z.iloc[:, 0].to_numpy(dtype=float)
        Xv = Z.iloc[:, 1:].to_numpy(dtype=float)
        # last column is x_j, its index in Xv is (n_lags)
        coef_idx = len(ylags)
        t, p = _ols_t_and_p(yv, Xv, coef_idx=coef_idx)
        abs_t[c] = float(abs(t))
        pvals[c] = float(p)

    tser = pd.Series(abs_t, dtype="float64").sort_values(ascending=False)
    ranked = [c for c in tser.index if np.isfinite(tser[c])]

    # Gate
    keep = ranked
    if tstat_alpha is not None and np.isfinite(tstat_alpha) and tstat_alpha > 0:
        keep = [
            c
            for c in ranked
            if np.isfinite(pvals.get(c, np.nan)) and pvals[c] < tstat_alpha
        ]
    if tstat_topn and tstat_topn > 0:
        keep = keep[:tstat_topn]

    # De-dup + bounds
    dedup = _dedup_by_corr(keep, X, tau=dedup_tau)
    selected = _enforce_min_max(
        dedup, ranked, min_features=min_features, max_features=max_features
    )

    # Ranking table
    abs_corr = _abs_corr_all(X, y)
    rank_df = pd.DataFrame(
        {
            "tstat_abs": tser,
            "pvalue": pd.Series(pvals, dtype="float64"),
            "abs_corr": abs_corr,
        }
    )
    rank_df["selected"] = rank_df.index.isin(selected)
    rank_df = rank_df.sort_values("tstat_abs", ascending=False)

    meta: Dict[str, Any] = {
        "method": "tstat_lm",
        "n_obs": int(len(X)),
        "n_features_in": int(X.shape[1]),
        "n_features_selected": int(len(selected)),
        "hyperparams": {
            "min_features": min_features,
            "max_features": max_features,
            "dedup_tau": dedup_tau,
            "target_lags": target_lags,
            "tstat_alpha": tstat_alpha,
            "tstat_topn": tstat_topn,
        },
    }

    return rank_df, selected, meta
