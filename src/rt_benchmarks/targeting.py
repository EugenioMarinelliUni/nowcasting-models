from __future__ import annotations

import pandas as pd


VALID_HORIZONS = {"bac", "now", "for"}


def month_of_quarter(date: pd.Timestamp) -> int:
    d = pd.Timestamp(date)
    return ((d.month - 1) % 3) + 1


def quarter_end_month(date: pd.Timestamp) -> pd.Timestamp:
    d = pd.Timestamp(date)
    q = d.to_period("Q")
    return q.asfreq("M", "end").to_timestamp("MS")


def horizon_target_date(eval_date: pd.Timestamp, horizon: str) -> pd.Timestamp:
    d = pd.Timestamp(eval_date)

    if horizon not in VALID_HORIZONS:
        raise ValueError(f"Invalid horizon '{horizon}'. Expected one of {sorted(VALID_HORIZONS)}")

    current_q_end = quarter_end_month(d)

    if horizon == "bac":
        return (current_q_end - pd.offsets.QuarterEnd(n=1)).to_period("Q").asfreq("M", "end").to_timestamp("MS")

    if horizon == "now":
        return current_q_end

    return (current_q_end + pd.offsets.QuarterEnd(n=1)).to_period("Q").asfreq("M", "end").to_timestamp("MS")


def target_release_date(target_date: pd.Timestamp, gdp_rel: int) -> pd.Timestamp:
    td = pd.Timestamp(target_date)
    return td + pd.DateOffset(months=int(gdp_rel))


def is_target_released(eval_date: pd.Timestamp, target_date: pd.Timestamp, gdp_rel: int) -> bool:
    ed = pd.Timestamp(eval_date)
    rel = target_release_date(target_date, gdp_rel)
    return ed >= rel