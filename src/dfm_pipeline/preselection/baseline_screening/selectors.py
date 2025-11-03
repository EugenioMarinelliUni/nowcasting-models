from __future__ import annotations

from typing import List, Dict, Tuple
import numpy as np
import pandas as pd
from sklearn.linear_model import LassoLarsCV

from .io import load_X_y, write_selected_meta, write_preselected_panel


# ---------- Utilities ----------

def mask_train_rows(y: pd.Series) -> pd.Series:
    """Months where the quarterly target is observed (e.g., Jan/Apr/Jul/Oct)."""
    return y.notna()

def dedup_by_corr(X: pd.DataFrame, cols: List[str], tau: float) -> List[str]:
    """
    Greedy de-duplication: keep first occurrence, drop others whose |corr| >= tau with any kept.
    """
    if len(cols) <= 1:
        return cols
    C = X[cols].corr().abs()
    keep: List[str] = []
    seen: set[str] = set()
    for c in cols:
        if c in seen:
            continue
        keep.append(c)
        dup = C.index[(C[c] >= tau) & (C.index != c)]
        seen.update(dup)
    return keep


# ---------- SIS (Fan & Lv, 2008) ----------

def sis_select(X: pd.DataFrame, y: pd.Series, *,
               tau: float = 0.0, top_n: int = 0,
               min_features: int = 0, max_features: int = 0,
               dedup_tau: float = 0.98) -> List[str]:
    """
    Rank by absolute Pearson correlation on masked rows.
    """
    # correlations
    yy = y.values
    scores: Dict[str, float] = {}
    for c in X.columns:
        x = X[c].values
        m = np.isfinite(x) & np.isfinite(yy)
        if m.sum() < 10:
            scores[c] = 0.0
            continue
        xm = x[m] - x[m].mean(); ym = yy[m] - np.nanmean(yy[m])
        denom = float(np.sqrt((xm * xm).sum()) * np.sqrt((ym * ym).sum()))
        scores[c] = 0.0 if denom == 0.0 else float((xm @ ym) / denom)

    s = pd.Series(scores).abs().sort_values(ascending=False)

    sel = s.index.tolist()
    if tau and tau > 0:
        sel = s[s >= tau].index.tolist()
    if top_n and top_n > 0 and len(sel) > top_n:
        sel = sel[:top_n]

    # dedup + guardrails
    cols = dedup_by_corr(X, sel, dedup_tau)

    # top-up to min_features by following ranking
    if min_features and len(cols) < min_features:
        for c in s.index:
            if c in cols:
                continue
            cols.append(c)
            cols = dedup_by_corr(X, cols, dedup_tau)
            if len(cols) >= min_features:
                break

    if max_features and len(cols) > max_features:
        cols = cols[:max_features]
    return cols


# ---------- t-stat–based (Bair et al., 2006 style) ----------

def _t_stat_univariate(x: np.ndarray, y: np.ndarray) -> float:
    m = np.isfinite(x) & np.isfinite(y)
    n = int(m.sum())
    if n < 10:
        return 0.0
    x = x[m]; y = y[m]
    xx = float(x.T @ x)
    if xx <= 0:
        return 0.0
    beta = float((x.T @ y) / xx)
    resid = y - beta * x
    s2 = float((resid @ resid) / max(1, n - 1))
    se = (s2 / xx) ** 0.5
    if se == 0:
        return 0.0
    return beta / se

def tstat_select(X: pd.DataFrame, y: pd.Series, *,
                 alpha: float = 0.0, top_n: int = 0,
                 min_features: int = 0, max_features: int = 0,
                 dedup_tau: float = 0.98) -> List[str]:
    """
    Rank by |t-stat| of y ~ x (univariate) on masked rows. Optional p-value screen via normal approx.
    """
    yy = y.values
    stats: Dict[str, Tuple[float, float]] = {}  # var -> (|t|, p)
    for c in X.columns:
        t = abs(_t_stat_univariate(X[c].values, yy))
        # Normal approximation p-value
        from math import erf, sqrt
        z = float(t)
        p = 2 * (1 - 0.5 * (1 + erf(z / sqrt(2))))
        stats[c] = (t, p)

    T = (pd.DataFrame(stats, index=["abs_t", "p"]).T
           .sort_values("abs_t", ascending=False))

    sel = T.index.tolist()
    if alpha and alpha > 0:
        sel = T[T["p"] <= alpha].index.tolist()
    if top_n and top_n > 0 and len(sel) > top_n:
        sel = sel[:top_n]

    cols = dedup_by_corr(X, sel, dedup_tau)

    if min_features and len(cols) < min_features:
        for c in T.index:
            if c in cols:
                continue
            cols.append(c)
            cols = dedup_by_corr(X, cols, dedup_tau)
            if len(cols) >= min_features:
                break

    if max_features and len(cols) > max_features:
        cols = cols[:max_features]
    return cols


# ---------- LARS / Lasso (Efron et al., 2004) ----------

def lars_select(X: pd.DataFrame, y: pd.Series, *,
                cv: int = 10,
                min_features: int = 0, max_features: int = 0,
                dedup_tau: float = 0.98) -> List[str]:
    """
    LassoLarsCV with fit_intercept=False (data already standardized).
    Support = non-zero coefficients at CV-chosen alpha.
    Guardrails: de-dup; optional top-up by |corr(y)| if min_features>0; cap at max_features.
    """
    model = LassoLarsCV(cv=int(cv), fit_intercept=False).fit(X.values, y.values)
    coef = model.coef_
    cols_all = X.columns.to_list()
    cols = [cols_all[i] for i, b in enumerate(coef) if float(b) != 0.0]

    # de-dup
    cols = dedup_by_corr(X, cols, dedup_tau)

    # top-up if requested using |corr(y)|
    if min_features and len(cols) < min_features:
        rank = X.apply(lambda s: s.corr(y), axis=0).abs().sort_values(ascending=False)
        for c in rank.index:
            if c in cols:
                continue
            cols.append(c)
            cols = dedup_by_corr(X, cols, dedup_tau)
            if len(cols) >= min_features:
                break

    if max_features and len(cols) > max_features:
        cols = cols[:max_features]
    return cols


# ---------- Public wrappers (used by the CLI scripts) ----------

def _finalize(panel: str, tag: str, method: str, params: Dict,
              X_full: pd.DataFrame, cols: List[str]) -> None:
    meta = write_selected_meta(panel, tag, method, params, cols)
    outp = write_preselected_panel(panel, tag, method, X_full, cols)
    print(f"{panel} {tag} {method}: selected {len(cols)} vars; meta={meta}; panel={outp}")

def run_sis(panel: str, tag: str, *,
            tau: float = 0.0, top_n: int = 0,
            min_features: int = 0, max_features: int = 0,
            dedup_tau: float = 0.98) -> List[str]:
    X, y = load_X_y(panel, tag)
    m = mask_train_rows(y)
    Xs, ys = X.loc[m], y.loc[m]
    cols = sis_select(Xs, ys, tau=tau, top_n=top_n,
                      min_features=min_features, max_features=max_features,
                      dedup_tau=dedup_tau)
    _finalize(panel, tag, "sis",
              {"tau_sis": tau, "top_n": top_n,
               "min_features": min_features, "max_features": max_features,
               "dedup_tau": dedup_tau},
              X, cols)
    return cols

def run_tstat(panel: str, tag: str, *,
              alpha: float = 0.0, top_n: int = 0,
              min_features: int = 0, max_features: int = 0,
              dedup_tau: float = 0.98) -> List[str]:
    X, y = load_X_y(panel, tag)
    m = mask_train_rows(y)
    Xs, ys = X.loc[m], y.loc[m]
    cols = tstat_select(Xs, ys, alpha=alpha, top_n=top_n,
                        min_features=min_features, max_features=max_features,
                        dedup_tau=dedup_tau)
    _finalize(panel, tag, "tstat",
              {"alpha_t": alpha, "top_n": top_n,
               "min_features": min_features, "max_features": max_features,
               "dedup_tau": dedup_tau},
              X, cols)
    return cols

def run_lars(panel: str, tag: str, *,
             cv: int = 10,
             min_features: int = 0, max_features: int = 0,
             dedup_tau: float = 0.98) -> List[str]:
    X, y = load_X_y(panel, tag)
    m = mask_train_rows(y)
    Xs, ys = X.loc[m], y.loc[m]
    cols = lars_select(Xs, ys, cv=cv,
                       min_features=min_features, max_features=max_features,
                       dedup_tau=dedup_tau)
    _finalize(panel, tag, "lars",
              {"cv": int(cv),
               "min_features": min_features, "max_features": max_features,
               "dedup_tau": dedup_tau},
              X, cols)
    return cols
