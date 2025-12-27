# src/dfm_pipeline/covid/methods.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .covid_make_delete_weights import apply_delete_nan
from .covid_make_outliers_iqd_nan import _matlab_round_to_index
from .masks import broadcast_row_mask, optional_window_mask
from .spec import CovidSpec


@dataclass(frozen=True)
class VariantResult:
    X: pd.DataFrame
    masks: Dict[str, pd.DataFrame]
    artifacts: Dict[str, pd.DataFrame]
    meta: Dict


def build_covid_window_mask(X: pd.DataFrame, spec: CovidSpec) -> pd.Series:
    s, e = spec.covid_bounds()
    m = (X.index >= s) & (X.index <= e)
    return pd.Series(m, index=X.index, name="covid_window")


def variant_delete_window(X: pd.DataFrame, spec: CovidSpec) -> VariantResult:
    row_mask = build_covid_window_mask(X, spec)
    replaced = broadcast_row_mask(row_mask, X.columns) & X.notna()
    X_out = apply_delete_nan(X, spec.covid_start, spec.covid_end, monthly_freq=spec.monthly_freq)
    meta = {"method": "delete_window", "covid": spec.to_dict()}
    return VariantResult(
        X=X_out,
        masks={"covid_window": broadcast_row_mask(row_mask, X.columns), "deleted": replaced},
        artifacts={},
        meta=meta,
    )


def _resolve_lm_dummy_months(mode: str, custom_months: Optional[List[str]]) -> List[pd.Timestamp]:
    if mode == "q2q3_2020":
        return [pd.Timestamp("2020-06-01"), pd.Timestamp("2020-09-01")]
    if mode == "q1q2_2020":
        return [pd.Timestamp("2020-03-01"), pd.Timestamp("2020-06-01")]
    if mode == "custom":
        if not custom_months:
            raise ValueError("custom dummy mode requires custom_months")
        return [pd.Timestamp(m) for m in custom_months]
    raise ValueError(f"Unknown dummy mode: {mode}")


def variant_sparse_dummies(
    X: pd.DataFrame,
    spec: CovidSpec,
    *,
    mode: str = "q2q3_2020",
    custom_months: Optional[List[str]] = None,
    ensure_min_nonmissing: int = 5,
    prefix: str = "lm_covid_dummy",
    standardize_dummies: bool = False,
    standardize_over: Optional[Tuple[str, str]] = None,
) -> VariantResult:
    if standardize_dummies and standardize_over is None:
        raise ValueError("standardize_over must be provided when standardize_dummies=True")
    months = _resolve_lm_dummy_months(mode, custom_months)

    # Non-block DFM integration:
    # emit D as exogenous regressors; do not append dummy columns to X.
    D_cols: List[str] = []
    activated: Dict[str, bool] = {}
    D = pd.DataFrame(index=X.index)

    for k, ts in enumerate(months, start=1):
        name = f"{prefix}_{mode}_m{k}"
        d = pd.Series(0.0, index=X.index, name=name)
        if ts in d.index:
            nonmissing = int(np.sum(~np.isnan(X.loc[ts].to_numpy(dtype=float))))
            if nonmissing >= ensure_min_nonmissing:
                d.loc[ts] = 1.0
                activated[name] = True
            else:
                activated[name] = False
        else:
            activated[name] = False

        if standardize_dummies:
            s, e = pd.Timestamp(standardize_over[0]), pd.Timestamp(standardize_over[1])
            m = (d.index >= s) & (d.index <= e)
            mu = float(np.nanmean(d.loc[m].to_numpy(dtype=float)))
            sd = float(np.nanstd(d.loc[m].to_numpy(dtype=float)))
            d = (d - mu) / sd if sd > 0 else d * 0.0

        D[name] = d
        D_cols.append(name)

    meta = {
        "method": "sparse_dummies_exog",
        "covid": spec.to_dict(),
        "dummy_spec": {
            "mode": mode,
            "custom_months": custom_months,
            "ensure_min_nonmissing": int(ensure_min_nonmissing),
            "prefix": prefix,
            "standardize_dummies": bool(standardize_dummies),
            "standardize_over": list(standardize_over) if standardize_over is not None else None,
        },
        "dummy_cols": D_cols,
        "activated": activated,
        "integration": {
            "intended_use": "exogenous_regressors",
            "note": "Do not append dummy columns to X for factor extraction in a non-block DFM.",
        },
    }

    return VariantResult(
        X=X,
        masks={"covid_window": broadcast_row_mask(build_covid_window_mask(X, spec), X.columns)},
        artifacts={"exog_dummies": D},
        meta=meta,
    )


def _iqd_thresholds_from_fit(
    X_fit: np.ndarray,
    *,
    c: float,
    min_obs: int,
    eps: float,
) -> Tuple[np.ndarray, np.ndarray]:
    ncols = X_fit.shape[1]
    med = np.full(ncols, np.nan, dtype=float)
    iqd = np.full(ncols, np.nan, dtype=float)

    for j in range(ncols):
        xj = X_fit[:, j]
        xj = xj[np.isfinite(xj)]
        ns = int(xj.size)
        if ns < min_obs:
            continue
        xs = np.sort(xj)
        idx_med = _matlab_round_to_index(ns, 0.5)
        idx_q20 = _matlab_round_to_index(ns, 0.2)
        med_j = float(xs[idx_med])
        q20_j = float(xs[idx_q20])
        iqd_j = float(abs(q20_j - med_j))
        if iqd_j <= eps:
            med[j] = med_j
            iqd[j] = np.nan
        else:
            med[j] = med_j
            iqd[j] = iqd_j
    return med, iqd


def variant_iqd_outliers_to_nan(
    X: pd.DataFrame,
    spec: CovidSpec,
    *,
    c: float = 4.0,
    min_obs: int = 20,
    fit_window: Optional[Tuple[str, str]] = None,
    apply_window: Optional[Tuple[str, str]] = None,
    eps: float = 1e-12,
) -> VariantResult:
    if not isinstance(X.index, pd.DatetimeIndex):
        raise TypeError("X must have a DatetimeIndex")

    if fit_window is None and not spec.allow_leakage:
        raise ValueError(
            "lm_outliers requires a leakage-safe fit_window (training window) "
            "or allow_leakage=True."
        )
    if apply_window is None:
        apply_window = (spec.covid_start, spec.covid_end)

    fit_mask = optional_window_mask(X.index, fit_window, monthly_freq=spec.monthly_freq).to_numpy(dtype=bool)
    app_mask = optional_window_mask(X.index, apply_window, monthly_freq=spec.monthly_freq).to_numpy(dtype=bool)

    cols = list(X.columns)
    X_out = X.copy()

    X_fit = X_out.loc[fit_mask, cols].to_numpy(dtype=float)
    X_apply = X_out.loc[app_mask, cols].to_numpy(dtype=float)

    med, iqd = _iqd_thresholds_from_fit(X_fit, c=c, min_obs=min_obs, eps=eps)

    outlier_mask_apply = np.zeros_like(X_apply, dtype=bool)
    for j in range(len(cols)):
        if not np.isfinite(med[j]) or not np.isfinite(iqd[j]):
            continue
        thr = float(c * iqd[j])
        xj = X_apply[:, j]
        m = np.isfinite(xj) & (np.abs(xj - med[j]) > thr)
        outlier_mask_apply[:, j] = m
        xj[m] = np.nan
        X_apply[:, j] = xj

    X_out.loc[app_mask, cols] = X_apply

    full_mask = np.zeros((len(X.index), len(cols)), dtype=bool)
    full_mask[app_mask, :] = outlier_mask_apply
    mask_df = pd.DataFrame(full_mask, index=X.index, columns=cols)

    meta = {
        "method": "iqd_outliers_to_nan",
        "covid": spec.to_dict(),
        "outlier_spec": {"c": float(c), "min_obs": int(min_obs), "eps": float(eps)},
        "fit_window": list(fit_window) if fit_window is not None else None,
        "apply_window": list(apply_window) if apply_window is not None else None,
    }

    return VariantResult(
        X=X_out,
        masks={
            "covid_window": broadcast_row_mask(build_covid_window_mask(X, spec), X.columns),
            "outliers": mask_df,
        },
        artifacts={},
        meta=meta,
    )
