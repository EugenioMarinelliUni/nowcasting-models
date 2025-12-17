# src/dfm_pipeline/covid/covid_make_outliers_iqd_nan.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IQDOutlierSpec:
    """
    Replicates the Linzenich–Meunier toolbox outlier rule used by:
      common_outliers(x, 0)

    For each series:
      - median = 50th percentile (on non-missing values)
      - q20 = value at ~20th percentile (MATLAB uses round(ns*1/5))
      - IQD = abs(q20 - median)
      - outlier if abs(x - median) > c * IQD
      - replace outliers by NaN

    Defaults match toolbox:
      c = 4
    """
    c: float = 4.0
    min_obs: int = 20  # skip series with too few observations in fit window
    eps: float = 1e-12  # treat IQD ~ 0 as zero


def _matlab_round_to_index(ns: int, frac: float) -> int:
    """
    MATLAB expression: xsort(round(ns*frac))
    with 1-based indexing; clamp to [1, ns].
    Return 0-based index for Python.
    """
    k1 = int(np.round(ns * frac))
    k1 = max(1, min(ns, k1))
    return k1 - 1


def apply_outliers_iqd_to_nan(
    X: pd.DataFrame,
    spec: IQDOutlierSpec = IQDOutlierSpec(),
    fit_window: Optional[Tuple[str, str]] = None,
    apply_window: Optional[Tuple[str, str]] = None,
) -> pd.DataFrame:
    """
    Apply IQD-based outlier-to-NaN correction.

    Parameters
    ----------
    X:
        Monthly panel (standardized or not), DatetimeIndex.
    spec:
        IQDOutlierSpec with c, min_obs.
    fit_window:
        (start, end) used to compute median and q20 per series. If None, use full sample.
        Recommended to avoid leakage: fit_window should be the training window.
    apply_window:
        (start, end) months where replacement is applied. If None, apply to full sample.
        Linzenich–Meunier apply globally (not window-restricted).

    Returns
    -------
    Corrected panel with outliers replaced by NaN.
    """
    if not isinstance(X.index, pd.DatetimeIndex):
        raise TypeError("X must have a DatetimeIndex")

    X_out = X.copy()
    cols = list(X_out.columns)

    if fit_window is None:
        fit_mask = np.ones(len(X_out.index), dtype=bool)
    else:
        fs, fe = pd.Timestamp(fit_window[0]), pd.Timestamp(fit_window[1])
        fit_mask = (X_out.index >= fs) & (X_out.index <= fe)

    if apply_window is None:
        apply_mask = np.ones(len(X_out.index), dtype=bool)
    else:
        as_, ae = pd.Timestamp(apply_window[0]), pd.Timestamp(apply_window[1])
        apply_mask = (X_out.index >= as_) & (X_out.index <= ae)

    X_fit = X_out.loc[fit_mask, cols].to_numpy(dtype=float)
    X_apply = X_out.loc[apply_mask, cols].to_numpy(dtype=float)

    # Precompute per-series thresholds from fit window
    med = np.full(len(cols), np.nan, dtype=float)
    iqd = np.full(len(cols), np.nan, dtype=float)

    for j in range(len(cols)):
        xj = X_fit[:, j]
        xj = xj[np.isfinite(xj)]
        ns = int(xj.size)
        if ns < spec.min_obs:
            continue
        xs = np.sort(xj)
        idx_med = _matlab_round_to_index(ns, 0.5)
        idx_q20 = _matlab_round_to_index(ns, 0.2)
        med_j = float(xs[idx_med])
        q20_j = float(xs[idx_q20])
        iqd_j = float(abs(q20_j - med_j))

        if iqd_j <= spec.eps:
            # constant-ish series in fit window; do not flag outliers
            med[j] = med_j
            iqd[j] = np.nan
        else:
            med[j] = med_j
            iqd[j] = iqd_j

    # Apply to requested window
    for j in range(len(cols)):
        if not np.isfinite(med[j]) or not np.isfinite(iqd[j]):
            continue
        thr = float(spec.c * iqd[j])
        xj = X_apply[:, j]
        mask = np.isfinite(xj) & (np.abs(xj - med[j]) > thr)
        xj[mask] = np.nan
        X_apply[:, j] = xj

    X_out.loc[apply_mask, cols] = X_apply
    return X_out
