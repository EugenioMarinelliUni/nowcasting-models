# src/dfm_pipeline/covid/covid_make_dummies_sparse.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SparseDummySpec:
    """
    Implements the Linzenich–Meunier toolbox "Covid dummy" idea:
      - add sparse dummy *series* (columns) that are 1 on specific months, 0 otherwise.

    Modes matching their toolbox:
      - q2q3_2020: dummy at 2020-06 (Q2 stamp month) and 2020-09 (Q3 stamp month)
      - q1q2_2020: dummy at 2020-03 (Q1 stamp month) and 2020-06 (Q2 stamp month)

    The toolbox also guards against adding a dummy if that month is too sparse
    (needs at least 5 non-missing series that month). We replicate this behavior.
    """

    mode: str = "q2q3_2020"  # {"q2q3_2020", "q1q2_2020", "custom"}
    custom_months: Optional[Sequence[str]] = None  # e.g. ["2020-06-01", "2020-09-01"]
    ensure_min_nonmissing: int = 5
    prefix: str = "covid_dummy"

    def resolve_months(self) -> List[pd.Timestamp]:
        if self.mode == "q2q3_2020":
            return [pd.Timestamp("2020-06-01"), pd.Timestamp("2020-09-01")]
        if self.mode == "q1q2_2020":
            return [pd.Timestamp("2020-03-01"), pd.Timestamp("2020-06-01")]
        if self.mode == "custom":
            if not self.custom_months:
                raise ValueError("mode='custom' requires custom_months")
            return [pd.Timestamp(m) for m in self.custom_months]
        raise ValueError(f"Unknown mode: {self.mode}")


def append_sparse_covid_dummies(
    X: pd.DataFrame,
    spec: SparseDummySpec,
    standardize_dummies: bool = False,
    standardize_over: Optional[Tuple[str, str]] = None,
) -> pd.DataFrame:
    """
    Append sparse dummy columns to X.

    Parameters
    ----------
    X:
        Monthly predictor panel with DatetimeIndex.
    spec:
        SparseDummySpec defining which months get dummy=1.
    standardize_dummies:
        If True, z-score each dummy using mean/std computed on `standardize_over`.
        If False, leave as 0/1.
    standardize_over:
        (start, end) date strings; required if standardize_dummies=True.

    Notes
    -----
    - The Linzenich–Meunier MATLAB toolbox standardizes after adding dummies.
      If your X is already standardized, you can either keep dummies as 0/1
      or standardize them on the train window for closer comparability.
    """
    if not isinstance(X.index, pd.DatetimeIndex):
        raise TypeError("X must have a DatetimeIndex")

    months = spec.resolve_months()
    X_out = X.copy()

    for k, ts in enumerate(months, start=1):
        name = f"{spec.prefix}_{spec.mode}_m{k}"
        d = pd.Series(0.0, index=X_out.index, name=name)

        if ts in d.index:
            # Guard: only add dummy if enough series are observed that month (toolbox logic)
            nonmissing = int(np.sum(~np.isnan(X_out.loc[ts].to_numpy(dtype=float))))
            if nonmissing >= spec.ensure_min_nonmissing:
                d.loc[ts] = 1.0
            else:
                # leave as all zeros; still append for determinism
                d.loc[ts] = 0.0
        # else: month not in sample → leave all zeros

        if standardize_dummies:
            if standardize_over is None:
                raise ValueError("standardize_over must be provided when standardize_dummies=True")
            start, end = (pd.Timestamp(standardize_over[0]), pd.Timestamp(standardize_over[1]))
            msk = (d.index >= start) & (d.index <= end)
            mu = float(np.nanmean(d.loc[msk].to_numpy(dtype=float)))
            sd = float(np.nanstd(d.loc[msk].to_numpy(dtype=float)))
            if sd > 0:
                d = (d - mu) / sd
            else:
                # constant dummy in the window; keep as-is
                d = d * 0.0

        X_out[name] = d

    return X_out
