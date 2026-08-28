"""
Deprecated compatibility wrappers for time-series transformations.

New code must import FRED-MD transformations from
``dfm_pipeline.preprocessing.tcode``.  This module is retained temporarily so
legacy callers do not break while there is only one transformation
implementation in the project.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd

from dfm_pipeline.preprocessing.tcode import (
    ALLOWED_TCODES,
    LEADS_LOST,
    apply_tcode_transformations as _apply_tcode_transformations,
    standardize as _standardize,
)

__all__ = [
    "apply_tcode_transformations",
    "standardize",
    "ALLOWED_TCODES",
    "LEADS_LOST",
]


def apply_tcode_transformations(
    df: pd.DataFrame,
    tcode_map: Dict[str, int],
) -> pd.DataFrame:
    """Compatibility wrapper around the canonical t-code implementation."""
    return _apply_tcode_transformations(df, tcode_map)


def standardize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preserve the legacy return type and ddof=1 convention.

    New code should call ``dfm_pipeline.preprocessing.tcode.standardize``
    directly and choose ``ddof`` explicitly.
    """
    z, _, _ = _standardize(df, ddof=1)
    return z
