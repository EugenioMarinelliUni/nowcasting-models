from __future__ import annotations

from typing import Dict, Literal, Optional, Tuple
import numpy as np
import pandas as pd

from .tcode import apply_tcode_transformations, LEADS_LOST

__all__ = ["prepare_panel_for_factors", "standardize"]


def _first_valid_iloc(s: pd.Series) -> Optional[int]:
    """
    Position (iloc) of first non-NaN, or None if all NaN.
    """
    a = s.to_numpy()
    mask = ~np.isnan(a)
    return int(mask.argmax()) if mask.any() else None


def _zscore(
    df: pd.DataFrame,
    ddof: int = 0,
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Column-wise standardization: (x - mean) / std. Zero std -> NaN.
    """
    mu = df.mean(skipna=True)
    sigma = df.std(ddof=ddof, skipna=True)
    sigma = sigma.replace({0.0: np.nan})
    z = (df - mu) / sigma
    return z, mu, sigma


def standardize(df: pd.DataFrame, ddof: int = 0) -> pd.DataFrame:
    """
    Convenience wrapper for z-scoring (mean 0, variance 1 by column).
    """
    z, _, _ = _zscore(df, ddof=ddof)
    return z


def prepare_panel_for_factors(
    df_raw: pd.DataFrame,
    tcode_map: Dict[str, int],
    *,
    balance: Literal["none", "initial", "all"] = "initial",
    standardize: bool = True,
    ddof: int = 0,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Full preprocessing pipeline for factor models:

      1) transform by tcode (Stock–Watson/FRED-MD),
      2) (optional) balance the panel,
      3) (optional) standardize (z-score).

    Parameters
    ----------
    df_raw : DataFrame
        Raw panel (rows=time, cols=series). Index should be sorted by time.
    tcode_map : dict[str, int]
        Mapping series -> tcode (1..7).
    balance : {"none","initial","all"}
        - "none": keep ragged/missing rows (good if Kalman/EM will handle missing).
        - "initial": trim *leading* rows so all columns start at a common first-valid point.
        - "all": after initial trim, drop rows with any NaN (fully balanced block).
    standardize : bool
        If True, z-score columns after balancing.
    ddof : int
        Degrees of freedom for std (0 recommended for population-style scaling).

    Returns
    -------
    df_out : DataFrame
        Transformed (and optionally balanced & standardized) panel.
    info : dict
        Metadata: dropped columns, first-valid positions, rows dropped, means/stds, etc.

    Notes
    -----
    - Tcode transforms do NOT guarantee stationarity—inspect as needed.
    - For PCA, use balance="initial" or "all". For EM/Kalman DFMs, balance="none" is fine.
    """
    # 1) Transform
    df_t = apply_tcode_transformations(df_raw, tcode_map)

    # 2) Balance
    first_locs = {col: _first_valid_iloc(df_t[col]) for col in df_t.columns}
    all_nan_cols = [c for c, loc in first_locs.items() if loc is None]
    if all_nan_cols:
        df_t = df_t.drop(columns=all_nan_cols)

    # Recompute after any drops
    first_locs = {col: _first_valid_iloc(df_t[col]) for col in df_t.columns}
    valid_locs = [loc for loc in first_locs.values() if loc is not None]
    global_first = max(valid_locs) if valid_locs else 0

    if balance == "none":
        df_b = df_t
        rows_dropped = []
    elif balance == "initial":
        df_b = df_t.iloc[global_first:].copy()
        rows_dropped = list(df_t.index[:global_first])
    elif balance == "all":
        df_b = df_t.iloc[global_first:].dropna(axis=0, how="any").copy()
        initially_dropped = list(df_t.index[:global_first])
        additionally_dropped = list(
            sorted(set(df_t.index[global_first:]).difference(set(df_b.index)))
        )
        rows_dropped = initially_dropped + additionally_dropped
    else:
        raise ValueError("balance must be one of {'none','initial','all'}")

    # 3) Standardize
    if standardize:
        df_z, mu, sigma = _zscore(df_b, ddof=ddof)
        df_out = df_z
    else:
        df_out = df_b
        mu, sigma = None, None

    info = {
        "all_nan_columns_dropped": all_nan_cols,
        "first_valid_iloc_by_col": first_locs,
        "global_first_valid_iloc": global_first,
        "rows_dropped": rows_dropped,
        "standardized": standardize,
        "means": mu,
        "stds": sigma,
        "leads_lost_by_tcode": LEADS_LOST,
    }
    return df_out, info
