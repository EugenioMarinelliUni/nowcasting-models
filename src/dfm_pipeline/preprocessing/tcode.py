from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd

__all__ = [
    "apply_tcode_transformations",
    "standardize",
    "ALLOWED_TCODES",
    "LEADS_LOST",
]

# Stock-Watson / FRED-MD transformation codes.
ALLOWED_TCODES: set[int] = {1, 2, 3, 4, 5, 6, 7}

# Number of leading observations mechanically lost on a fully observed,
# domain-valid series solely because of the transformation operator.
LEADS_LOST: Dict[int, int] = {
    1: 0,
    2: 1,
    3: 2,
    4: 0,
    5: 1,
    6: 2,
    7: 2,
}


def _transform_series(x: pd.Series, code: int) -> pd.Series:
    """
    Apply one FRED-MD transformation while preserving genuine missingness.

    Codes
    -----
    1 : level
    2 : first difference
    3 : second difference
    4 : log level
    5 : first difference of log
    6 : second difference of log
    7 : first difference of period-on-period growth

    Non-positive observations are outside the log domain for codes 4--6 and
    therefore become NaN. Code 7 explicitly disables pandas' historical
    forward-fill behaviour so missing values are never bridged implicitly.
    """
    s = pd.to_numeric(x, errors="coerce").astype(float)

    if code == 1:
        y = s
    elif code == 2:
        y = s.diff()
    elif code == 3:
        y = s.diff().diff()
    elif code == 4:
        y = np.log(s.where(s > 0))
    elif code == 5:
        y = np.log(s.where(s > 0)).diff()
    elif code == 6:
        y = np.log(s.where(s > 0)).diff().diff()
    elif code == 7:
        y = s.pct_change(fill_method=None).diff()
    else:
        raise ValueError(
            f"Unknown tcode: {code}. Allowed: {sorted(ALLOWED_TCODES)}"
        )

    return y.replace([np.inf, -np.inf], np.nan)


def apply_tcode_transformations(
    df: pd.DataFrame,
    tcode_map: Dict[str, int],
) -> pd.DataFrame:
    """
    Transform every column using its FRED-MD t-code.

    The returned DataFrame preserves the input index and the original column
    order. The function never fills missing observations.
    """
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    missing = [col for col in df.columns if col not in tcode_map]
    if missing:
        raise KeyError(f"No tcode provided for columns: {missing}")

    invalid: dict[str, object] = {}
    for key, value in tcode_map.items():
        try:
            code = int(value)
        except (TypeError, ValueError):
            invalid[key] = value
            continue
        if code not in ALLOWED_TCODES:
            invalid[key] = value

    if invalid:
        raise ValueError(
            "Invalid tcodes detected: "
            f"{invalid}. Allowed: {sorted(ALLOWED_TCODES)}"
        )

    transformed = {
        col: _transform_series(df[col], int(tcode_map[col]))
        for col in df.columns
    }
    return pd.DataFrame(transformed, index=df.index)


def standardize(
    df: pd.DataFrame,
    *,
    ddof: int = 0,
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    """
    Column-wise z-score using statistics computed on ``df``.

    Returns ``(z, means, stds)``. A zero standard deviation is replaced by NaN
    so downstream code cannot silently create infinities.
    """
    mu = df.mean(skipna=True)
    sigma = df.std(ddof=ddof, skipna=True).replace({0.0: np.nan})
    z = (df - mu) / sigma
    return z, mu, sigma
