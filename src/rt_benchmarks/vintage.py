from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from dfm_pipeline.eval_pseudort.fast.vintage_api import (
    apply_delay_mask_public,
    apply_quarter_end_leakage_guard_public,
    mask_quarterly_target_release_public,
)


@dataclass(frozen=True)
class VintageConfig:
    delay_style: str = "none"
    delay_map: dict[str, int] | None = None
    gdp_rel: int = 0
    no_qe_leak: bool = True


def _to_month_start(ts: Any) -> pd.Timestamp:
    out = pd.Timestamp(ts)
    if pd.isna(out):
        raise ValueError("eval_date/index value cannot be NaT")
    return out.to_period("M").to_timestamp(how="start")


def _normalize_monthly_frame_index(X: pd.DataFrame) -> pd.DataFrame:
    X = X.copy()
    idx = pd.DatetimeIndex(pd.to_datetime(X.index))
    if idx.hasnans:
        raise ValueError("X index contains NaT values")
    X.index = idx.to_period("M").to_timestamp(how="start")
    X = X.sort_index()
    return X


def _normalize_monthly_series_index(y: pd.Series) -> pd.Series:
    y = y.copy()
    idx = pd.DatetimeIndex(pd.to_datetime(y.index))
    if idx.hasnans:
        raise ValueError("y index contains NaT values")
    y.index = idx.to_period("M").to_timestamp(how="start")
    y = y.sort_index()
    return y


def apply_delay_mask(
    X: pd.DataFrame,
    eval_date: pd.Timestamp,
    delay_style: str = "none",
    delay_map: dict[str, int] | None = None,
) -> pd.DataFrame:
    """
    Return the monthly predictor vintage observable at eval_date.
    """
    eval_ms = _to_month_start(eval_date)
    Xv = _normalize_monthly_frame_index(X)
    Xv = Xv.loc[:eval_ms].copy()

    return apply_delay_mask_public(
        X=Xv,
        eval_date=eval_ms,
        delay_style=delay_style,
        delay_map=delay_map,
    )


def mask_quarterly_target_release(
    y: pd.Series,
    eval_date: pd.Timestamp,
    gdp_rel: int,
) -> pd.Series:
    """
    Return the quarterly target vintage observable at eval_date.
    """
    eval_ms = _to_month_start(eval_date)
    yv = _normalize_monthly_series_index(y)
    yv = yv.loc[:eval_ms].copy()

    return mask_quarterly_target_release_public(
        y=yv,
        eval_date=eval_ms,
        gdp_rel=int(gdp_rel),
    )


def apply_quarter_end_leakage_guard(
    y: pd.Series,
    eval_date: pd.Timestamp,
    no_qe_leak: bool = True,
) -> pd.Series:
    """
    Apply the same quarter-end leakage guard as the DFM fast pseudo-RT code.
    """
    eval_ms = _to_month_start(eval_date)
    yv = _normalize_monthly_series_index(y)

    return apply_quarter_end_leakage_guard_public(
        y=yv,
        eval_date=eval_ms,
        no_qe_leak=bool(no_qe_leak),
    )


def build_vintage_view(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    eval_date: pd.Timestamp,
    cfg: VintageConfig,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Build the pseudo-real-time information set available at eval_date.
    """
    eval_ms = _to_month_start(eval_date)

    Xv = apply_delay_mask(
        X=X_full,
        eval_date=eval_ms,
        delay_style=cfg.delay_style,
        delay_map=cfg.delay_map,
    )

    yv = mask_quarterly_target_release(
        y=y_full,
        eval_date=eval_ms,
        gdp_rel=cfg.gdp_rel,
    )

    yv = apply_quarter_end_leakage_guard(
        y=yv,
        eval_date=eval_ms,
        no_qe_leak=cfg.no_qe_leak,
    )

    return Xv, yv