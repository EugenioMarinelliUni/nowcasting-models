# FILE: src/dfm_pipeline/preselection/baseline_screening/selectors.py
#!/usr/bin/env python3
"""
Selectors: SIS, t-stat (HAC + optional AR(y) + optional X aggregation + optional ADL block),
LARS with TimeSeriesSplit.

Public API (dataframe in, list[str] out)
----------------------------------------
sis_select(X, y, *, min_features, max_features, dedup_tau, sis_tau=0.0, sis_topn=0)
tstat_select(X, y, *, min_features, max_features, dedup_tau, tstat_alpha=0.05, tstat_topn=0,
             hac_lags="auto", ar_lags=0, agg_rule_map=None, agg_rule_default=None,
             x_adl_lags=0, adl_score="fstat")
lars_select(X, y, *, min_features, max_features, dedup_tau, cv=10, one_se=False)
"""
from __future__ import annotations

from typing import Iterable, List, Tuple, Optional, Dict, cast
import numpy as np
import pandas as pd

# Optional deps
try:
    import statsmodels.api as sm
except Exception:  # pragma: no cover
    sm = None

try:
    from sklearn.linear_model import LassoLarsCV, LassoLars
    from sklearn.model_selection import TimeSeriesSplit
except Exception:  # pragma: no cover
    LassoLarsCV = None  # type: ignore[assignment]
    LassoLars = None    # type: ignore[assignment]
    TimeSeriesSplit = None  # type: ignore[assignment]

# ---------- utilities ----------
def _align_dropna(X: pd.DataFrame, y: pd.Series) -> Tuple[pd.DataFrame, pd.Series]:
    common = X.index.intersection(y.index)
    yc = y.loc[common].dropna()
    Xc = X.loc[yc.index]
    Xc = Xc.dropna(axis=1, how="all")
    return Xc, yc

def _abs_pearson(x: pd.Series, y: pd.Series) -> float:
    s = pd.concat([x, y], axis=1).dropna()
    if len(s) < 3:
        return np.nan
    return float(abs(s.iloc[:, 0].corr(s.iloc[:, 1])))

def _rank_features_by_abs_corr(X: pd.DataFrame, y: pd.Series) -> pd.Series:
    vals = {c: _abs_pearson(X[c], y) for c in X.columns}
    return pd.Series(vals, dtype="float64").dropna().sort_values(ascending=False)

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

def _enforce_min_max(selected: List[str], ranked: Iterable[str], min_features: int, max_features: int) -> List[str]:
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

def _hac_lags_auto(n: int) -> int:
    """
    HAC auto bandwidth: floor(4 * (n/100)^(2/9)), min 1.
    """
    if n <= 0:
        return 1
    lag = int(np.floor(4.0 * (n / 100.0) ** (2.0 / 9.0)))
    return max(lag, 1)

# ---------- aggregation helper ----------
import json
from pathlib import Path

def _aggregate_monthly_to_quarterly_matrix(
    X: pd.DataFrame,
    y: pd.Series,  # not used; kept for backward compatibility
    rule_default: str | None,
    rule_map_path: str | None,
) -> pd.DataFrame:
    """
    Build a quarterly-aligned matrix from monthly X using:
      - mean3m / sum3m (rolling 3) or last, each with shift(1) to avoid look-ahead.
    """
    # Precompute
    sum3 = X.rolling(3).sum().shift(1)
    mean3 = X.rolling(3).mean().shift(1)
    last1 = X.shift(1)

    if rule_map_path:
        rules = json.loads(Path(rule_map_path).read_text(encoding="utf-8")).get("series_rules", {})
        cols = []
        for c in X.columns:
            rule = str(rules.get(c, {}).get("rule", rule_default or "mean3m")).lower()
            if rule == "sum3m":
                cols.append(sum3[c])
            elif rule == "last":
                cols.append(last1[c])
            else:
                cols.append(mean3[c])
        Xq = pd.concat(cols, axis=1)
        Xq.columns = X.columns
        return Xq

    rule = (rule_default or "mean3m").lower()
    if rule == "sum3m":
        return sum3
    if rule == "last":
        return last1
    return mean3

# ---------- SIS ----------
def sis_select(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    min_features: int,
    max_features: int,
    dedup_tau: float,
    sis_tau: float = 0.0,
    sis_topn: int = 0,
) -> List[str]:
    X, y = _align_dropna(X, y)
    corr = _rank_features_by_abs_corr(X, y)
    ranked = list(corr.index)
    if sis_tau > 0:
        ranked = [c for c in ranked if corr[c] >= sis_tau]
    if sis_topn and sis_topn > 0:
        ranked = ranked[:sis_topn]
    dedup = _dedup_by_corr(ranked, X, tau=dedup_tau)
    selected = _enforce_min_max(dedup, ranked, min_features, max_features)
    return selected

# ---------- t-stat (+ AR(y) + optional aggregation + optional ADL block) ----------
def _fit_ols_and_stats(yv: np.ndarray, Xv: np.ndarray, hac_lags: Optional[int]):
    if sm is None:
        # Plain OLS (no HAC), with simple t-stats via last coefficient; F requires statsmodels
        Xd = np.column_stack([np.ones(len(yv)), Xv])
        beta = np.linalg.lstsq(Xd, yv, rcond=None)[0]
        resid = yv - Xd @ beta
        p = Xd.shape[1]
        s2 = (resid @ resid) / max(len(yv) - p, 1)
        cov = s2 * np.linalg.inv(Xd.T @ Xd)
        tvals = beta / np.sqrt(np.maximum(np.diag(cov), 1e-12))
        return beta, resid, cov, tvals, None  # no model object
    Xd = sm.add_constant(Xv, has_constant="add")
    mod = sm.OLS(yv, Xd, missing="drop")
    res = mod.fit() if hac_lags is None else mod.fit(cov_type="HAC", cov_kwds={"maxlags": int(hac_lags)})
    return res.params, res.resid, res.cov_params(), res.tvalues, res

def _p_from_t_norm(t: float) -> float:
    # Two-sided normal-approx p-value (used only if statsmodels is unavailable)
    from math import erf, sqrt
    return 2.0 * (1.0 - 0.5 * (1.0 + erf(abs(t) / sqrt(2.0))))

def tstat_select(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    min_features: int,
    max_features: int,
    dedup_tau: float,
    tstat_alpha: float = 0.05,
    tstat_topn: int = 0,
    hac_lags: str | int = "auto",
    ar_lags: int = 0,
    agg_rule_map: str | None = None,
    agg_rule_default: str | None = None,
    # NEW:
    x_adl_lags: int = 0,
    adl_score: str = "fstat",  # "fstat" or "max_t"
) -> List[str]:
    # Align + (optional) aggregate
    X, y = _align_dropna(X, y)
    if agg_rule_map or agg_rule_default:
        X_aggr = _aggregate_monthly_to_quarterly_matrix(X, y, agg_rule_default, agg_rule_map)
        X, y = _align_dropna(X_aggr, y)

    n = int(y.notna().sum())

    # HAC lags: accept "auto", an int, or a numeric string ("4")
    if isinstance(hac_lags, str):
        hl = hac_lags.strip().lower()
        if hl == "auto":
            hac = _hac_lags_auto(n)
        else:
            try:
                hac = int(hl)
            except ValueError:
                hac = None
    else:
        hac = int(hac_lags)

    # AR(y)
    ylags: List[pd.Series] = []
    if ar_lags and ar_lags > 0:
        for k in range(1, int(ar_lags) + 1):
            ylags.append(y.shift(k))

    scores: Dict[str, float] = {}
    pvals: Dict[str, float] = {}

    # For dedup later, correlations use the base X (no lags) on the aligned quarterly grid.
    X_base = X.copy()

    for c in X.columns:
        # Build regressors: AR(y) + X block
        block_cols: List[pd.Series] = []

        # Base series for X_j is already the chosen timing (raw, last, mean3m/sum3m via aggregation above)
        x0 = X[c]
        block_cols.append(x0)

        # Add K short lags of the *same base-timed* series.
        # NOTE: Since (X,y) are on quarter-start rows after alignment, shift(1) = one quarter lag.
        K = int(max(x_adl_lags, 0))
        if K > 0:
            for k in range(1, K + 1):
                block_cols.append(x0.shift(k))

        # Assemble design
        Z = pd.concat([y] + ylags + block_cols, axis=1).dropna()
        if len(Z) < max(8, 2 + len(ylags) + len(block_cols)):
            scores[c] = np.nan
            pvals[c] = np.nan
            continue

        yv = Z.iloc[:, 0].values
        Xv = Z.iloc[:, 1:].values

        params, resid, cov, tvalues, res = _fit_ols_and_stats(yv, Xv, hac)

        p_const = 1  # constant
        p_y = len(ylags)
        p_blk = len(block_cols)
        p_total = p_const + p_y + p_blk

        # Index of block coefficients within parameter vector:
        # order is [const] + ylags + block_cols
        blk_start = 1 + p_y
        blk_end = blk_start + p_blk  # exclusive

        if p_blk == 1:
            # Classic single-regressor: use t on the feature
            t_last = float(abs(tvalues[blk_end - 1]))
            if res is not None and hasattr(res, "pvalues"):
                p_last = float(res.pvalues[blk_end - 1])
            else:
                p_last = _p_from_t_norm(t_last)  # normal-approx fallback
            scores[c] = t_last if adl_score == "max_t" else t_last  # same in single-var case
            pvals[c] = p_last if tstat_alpha > 0 else np.nan
        else:
            # Block case: either F-test or max |t|
            if adl_score == "fstat" and res is not None:
                # Wald F-test for joint significance of the block
                R = np.zeros((p_blk, p_total))
                for i in range(p_blk):
                    R[i, blk_start + i] = 1.0
                ftest = res.f_test(R)
                fval = float(np.asarray(ftest.fvalue).ravel()[0])
                pval = float(np.asarray(ftest.pvalue).ravel()[0])
                scores[c] = fval
                pvals[c] = pval if tstat_alpha > 0 else np.nan
            else:
                # Rank by max |t| in the block; gate by min p in the block (if available)
                t_block = np.asarray(tvalues[blk_start:blk_end], dtype=float)
                scores[c] = float(np.nanmax(np.abs(t_block)))
                if res is not None and hasattr(res, "pvalues") and tstat_alpha > 0:
                    p_block = np.asarray(res.pvalues[blk_start:blk_end], dtype=float)
                    pvals[c] = float(np.nanmin(p_block))
                else:
                    pvals[c] = np.nan

    # Ranking & gating
    s = pd.Series(scores, dtype="float64").dropna().sort_values(ascending=False)
    ranked_all = list(s.index)

    if tstat_alpha and np.isfinite(tstat_alpha) and tstat_alpha > 0:
        keep = [c for c in ranked_all if np.isfinite(pvals.get(c, np.nan)) and pvals[c] < tstat_alpha]
    else:
        keep = ranked_all

    if tstat_topn and tstat_topn > 0:
        keep = keep[:tstat_topn]

    # Dedup on the base (non-lagged) X
    dedup = _dedup_by_corr(keep, X_base, tau=dedup_tau)
    selected = _enforce_min_max(dedup, ranked_all, min_features, max_features)
    return selected

# ---------- LARS ----------
def lars_select(
    X: pd.DataFrame,
    y: pd.Series,
    *,
    min_features: int,
    max_features: int,
    dedup_tau: float,
    cv: int = 10,
    one_se: bool = False,
) -> List[str]:
    if LassoLarsCV is None or TimeSeriesSplit is None:
        raise RuntimeError("scikit-learn is required for LARS selection")
    X, y = _align_dropna(X, y)
    Z = pd.concat([y, X], axis=1).dropna()
    yv = Z.iloc[:, 0].values
    Xv = Z.iloc[:, 1:].values
    cols = list(X.columns)
    if len(cols) == 0:
        return []
    tscv = TimeSeriesSplit(n_splits=int(cv))
    model = cast("LassoLarsCV", LassoLarsCV(cv=tscv, fit_intercept=False).fit(Xv, yv))  # type: ignore[call-arg]

    if one_se:
        if LassoLars is None:
            raise RuntimeError("scikit-learn (LassoLars) required for 1-SE refit")

        # Robust 1-SE selection across sklearn versions:
        # mse_path_ can be (n_alphas, n_folds) or (n_folds, n_alphas)
        mse_path = np.asarray(model.mse_path_)
        alphas = np.asarray(model.alphas_)

        if mse_path.ndim != 2:
            # Fallback: treat all along last axis as alphas
            axis_alphas, axis_folds = -1, 0
        elif mse_path.shape[0] == alphas.shape[0]:
            axis_alphas, axis_folds = 0, 1   # (A, K)
        elif mse_path.shape[1] == alphas.shape[0]:
            axis_alphas, axis_folds = 1, 0   # (K, A)
        else:
            axis_alphas, axis_folds = -1, 0  # fallback

        mse_mean = mse_path.mean(axis=axis_folds)
        mse_std  = mse_path.std(axis=axis_folds)
        K = int(mse_path.shape[axis_folds]) if mse_path.ndim == 2 else 1
        mse_se = mse_std / np.sqrt(max(K, 1))

        mse_mean = np.asarray(mse_mean).ravel()
        mse_se   = np.asarray(mse_se).ravel()

        i_min = int(np.nanargmin(mse_mean))
        thresh = float(mse_mean[i_min] + mse_se[i_min])

        candidates = [i for i, m in enumerate(mse_mean) if np.isfinite(m) and m <= thresh]
        i_choice = (max(candidates) if candidates else i_min)
        i_choice = max(0, min(i_choice, len(alphas) - 1))  # clamp

        coef = LassoLars(alpha=float(alphas[i_choice]), fit_intercept=False).fit(Xv, yv).coef_
    else:
        coef = getattr(model, "coef_", None)

    if coef is None:
        return []
    nz = np.where(np.abs(coef) > 1e-12)[0]
    selected_raw = [cols[i] for i in nz]
    selected_dedup = _dedup_by_corr(selected_raw, X, tau=dedup_tau)
    corr = _rank_features_by_abs_corr(X, y)
    ranked = list(corr.index)
    selected = _enforce_min_max(selected_dedup, ranked, min_features, max_features)
    return selected
