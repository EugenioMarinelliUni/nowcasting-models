from __future__ import annotations

from typing import Dict, Tuple, List
import numpy as np


# Single source of truth for allowed Stock–Watson/FRED-MD transform codes
ALLOWED_TCODES: set[int] = {1, 2, 3, 4, 5, 6, 7}


def validate_tcode_map_against_columns(
    df_columns: List[str],
    tcode_map: Dict[str, int],
    *,
    date_col: str = "sasdate",
    require_all: bool = True,
) -> Tuple[Dict[str, int], Dict]:
    """
    Clean and validate a mapping {series -> tcode} against the CSV's header columns.

    Parameters
    ----------
    df_columns : list of str
        Column names from the CSV header (including the date column).
    tcode_map : dict
        Raw mapping from series names to transformation codes (values may be str or int).
    date_col : str
        Name of the date column to ignore during validation.
    require_all : bool
        If True, raise if any series column lacks a valid tcode.

    Returns
    -------
    cleaned_map : dict[str, int]
        Mapping filtered to present columns; codes coerced to int and restricted to ALLOWED_TCODES.
    info : dict
        Diagnostics: {"missing_in_tcodes": [...], "extra_in_tcodes": [...]}

    Raises
    ------
    KeyError
        If require_all=True and some columns are missing in the mapping.
    """
    cols = [c for c in df_columns if c != date_col]

    cleaned: Dict[str, int] = {}
    for k, v in tcode_map.items():
        try:
            iv = int(v)  # accept numeric strings too
        except (TypeError, ValueError):
            iv = np.nan
        if k in cols and iv in ALLOWED_TCODES:
            cleaned[k] = int(iv)

    missing = [c for c in cols if c not in cleaned]
    extra = [k for k in tcode_map if k not in cols and k != date_col]

    if require_all and missing:
        raise KeyError(f"No tcode for columns: {missing}")

    return cleaned, {"missing_in_tcodes": missing, "extra_in_tcodes": extra}
