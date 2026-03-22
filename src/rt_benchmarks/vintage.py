from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass
class VintageConfig:
    delay_style: str = "none"
    delay_map: dict | None = None
    gdp_rel: int = 0
    no_qe_leak: bool = True


def apply_delay_mask(
    X: pd.DataFrame,
    eval_date: pd.Timestamp,
    delay_style: str = "none",
    delay_map: dict | None = None,
) -> pd.DataFrame:
    Xv = X.loc[:pd.Timestamp(eval_date)].copy()

    if delay_style == "none" or delay_map is None:
        return Xv

    for col, delay in delay_map.items():
        if col not in Xv.columns:
            continue
        if delay <= 0:
            continue
        masked_idx = Xv.index[Xv.index > (pd.Timestamp(eval_date) - pd.DateOffset(months=int(delay)))]
        Xv.loc[masked_idx, col] = pd.NA

    return Xv


def mask_quarterly_target_release(
    y: pd.Series,
    eval_date: pd.Timestamp,
    gdp_rel: int,
) -> pd.Series:
    yv = y.loc[:pd.Timestamp(eval_date)].copy()

    if gdp_rel <= 0:
        return yv

    last_idx = yv.index.max() if len(yv.index) else None
    if last_idx is not None and pd.Timestamp(eval_date) < (pd.Timestamp(last_idx) + pd.DateOffset(months=int(gdp_rel))):
        yv.loc[last_idx] = pd.NA

    return yv


def apply_quarter_end_leakage_guard(
    y: pd.Series,
    eval_date: pd.Timestamp,
    no_qe_leak: bool = True,
) -> pd.Series:
    yv = y.copy()
    d = pd.Timestamp(eval_date)

    if not no_qe_leak:
        return yv

    is_quarter_end_month = d.month in (3, 6, 9, 12)
    if is_quarter_end_month and d in yv.index:
        yv.loc[d] = pd.NA

    return yv


def build_vintage_view(
    X_full: pd.DataFrame,
    y_full: pd.Series,
    eval_date: pd.Timestamp,
    cfg: VintageConfig,
) -> tuple[pd.DataFrame, pd.Series]:
    Xv = apply_delay_mask(
        X_full,
        eval_date=eval_date,
        delay_style=cfg.delay_style,
        delay_map=cfg.delay_map,
    )
    yv = mask_quarterly_target_release(
        y_full,
        eval_date=eval_date,
        gdp_rel=cfg.gdp_rel,
    )
    yv = apply_quarter_end_leakage_guard(
        yv,
        eval_date=eval_date,
        no_qe_leak=cfg.no_qe_leak,
    )
    return Xv, yv