# src/dfm_pipeline/preprocessing/tcode.py  (or wherever you keep it)
from typing import Dict, Tuple
import numpy as np
import pandas as pd

__all__ = ["apply_tcode_transformations", "standardize", "ALLOWED_TCODES", "LEADS_LOST"]

# Stock–Watson / FRED-MD t-codes
ALLOWED_TCODES: set[int] = {1, 2, 3, 4, 5, 6, 7}
# Leading obs lost by each code
LEADS_LOST: Dict[int, int] = {1: 0, 2: 1, 3: 2, 4: 0, 5: 1, 6: 2, 7: 2}


def _transform_series(x: pd.Series, code: int) -> pd.Series:
    """
    Robust single-series transform (assumes time index ascending).
      1: level; 2: Δx; 3: Δ²x; 4: log x; 5: Δ log x; 6: Δ² log x; 7: Δ(x_t/x_{t-1} − 1)
    """
    s = pd.to_numeric(x, errors="coerce").astype(float)  # coerce messy tokens → NaN

    if code == 1:
        y = s
    elif code == 2:
        y = s.diff()
    elif code == 3:
        y = s.diff().diff()
    elif code == 4:
        y = np.log(s.where(s > 0))              # domain-safe log (≤0 → NaN)
    elif code == 5:
        y = np.log(s.where(s > 0)).diff()
    elif code == 6:
        y = np.log(s.where(s > 0)).diff().diff()
    elif code == 7:
        y = s.pct_change().diff()               # Δ growth
    else:
        raise ValueError(f"Unknown tcode: {code}. Allowed: {sorted(ALLOWED_TCODES)}")

    # Clean up numeric artifacts
    y = y.replace([np.inf, -np.inf], np.nan)
    return y


def apply_tcode_transformations(df: pd.DataFrame, tcode_map: Dict[str, int]) -> pd.DataFrame:
    """
    Column-wise transforms per tcode_map. Same index as df; leading NaNs by design.
    """
    # Ensure time order (safe no-op if already sorted)
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    # Validate coverage and codes
    missing = [c for c in df.columns if c not in tcode_map]
    if missing:
        raise KeyError(f"No tcode provided for columns: {missing}")
    invalid = {k: v for k, v in tcode_map.items() if int(v) not in ALLOWED_TCODES}
    if invalid:
        raise ValueError(f"Invalid tcodes detected: {invalid}. Allowed: {sorted(ALLOWED_TCODES)}")

    transformed = {col: _transform_series(df[col], int(tcode_map[col])) for col in df.columns}
    return pd.DataFrame(transformed, index=df.index)


def standardize(
    df: pd.DataFrame, *, ddof: int = 0
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Z-score: (x − mean) / std computed column-wise on the given DataFrame.
    Returns (Z, means, stds). Zero std → NaN (prevents infs).
    """
    mu = df.mean(skipna=True)
    sigma = df.std(ddof=ddof, skipna=True).replace({0.0: np.nan})
    z = (df - mu) / sigma
    return z, mu, sigma

