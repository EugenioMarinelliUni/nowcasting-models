from __future__ import annotations

import pandas as pd


VALID_HORIZONS = {"bac", "now", "for"}


def _to_month_start(ts: pd.Timestamp) -> pd.Timestamp:
    out = pd.Timestamp(ts)
    if pd.isna(out):
        raise ValueError("timestamp cannot be NaT")
    return pd.Timestamp(year=out.year, month=out.month, day=1)


def _quarter_end_month_number(month: int) -> int:
    if month <= 3:
        return 3
    if month <= 6:
        return 6
    if month <= 9:
        return 9
    return 12


def month_of_quarter(date: pd.Timestamp) -> int:
    d = _to_month_start(date)
    return ((d.month - 1) % 3) + 1


def quarter_end_month(date: pd.Timestamp) -> pd.Timestamp:
    d = _to_month_start(date)
    q_end_month = _quarter_end_month_number(d.month)
    return pd.Timestamp(year=d.year, month=q_end_month, day=1)


def horizon_target_date(eval_date: pd.Timestamp, horizon: str) -> pd.Timestamp:
    d = _to_month_start(eval_date)

    if horizon not in VALID_HORIZONS:
        raise ValueError(
            f"Invalid horizon '{horizon}'. Expected one of {sorted(VALID_HORIZONS)}"
        )

    target = quarter_end_month(d)

    if horizon == "bac":
        target = _to_month_start(target - pd.DateOffset(months=3))
    elif horizon == "for":
        target = _to_month_start(target + pd.DateOffset(months=3))

    return target


def target_release_date(target_date: pd.Timestamp, gdp_rel: int) -> pd.Timestamp:
    td = _to_month_start(target_date)
    return _to_month_start(td + pd.DateOffset(months=int(gdp_rel)))


def is_target_released(
    eval_date: pd.Timestamp,
    target_date: pd.Timestamp,
    gdp_rel: int,
) -> bool:
    ed = _to_month_start(eval_date)
    rel = target_release_date(target_date, gdp_rel)
    return ed >= rel