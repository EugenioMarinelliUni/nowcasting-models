from __future__ import annotations

from typing import Optional

import pandas as pd

from .bm_pseudort_fast import (
    _apply_delay_mask,
    _apply_quarter_end_leakage_guard,
    _mask_quarterly_target_release,
)


def apply_delay_mask_public(
    X: pd.DataFrame,
    eval_date: pd.Timestamp,
    delay_style: str,
    delay_map: Optional[dict],
) -> pd.DataFrame:
    return _apply_delay_mask(
        X=X,
        eval_date=eval_date,
        delay_style=delay_style,
        delay_map=delay_map,
    )


def mask_quarterly_target_release_public(
    y: pd.Series,
    eval_date: pd.Timestamp,
    gdp_rel: int,
) -> pd.Series:
    return _mask_quarterly_target_release(
        y=y,
        eval_date=eval_date,
        gdp_rel=gdp_rel,
    )


def apply_quarter_end_leakage_guard_public(
    y: pd.Series,
    eval_date: pd.Timestamp,
    *,
    no_qe_leak: bool,
) -> pd.Series:
    return _apply_quarter_end_leakage_guard(
        y=y,
        eval_date=eval_date,
        no_qe_leak=no_qe_leak,
    )