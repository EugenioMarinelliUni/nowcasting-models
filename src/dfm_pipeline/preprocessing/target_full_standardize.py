# src/dfm_pipeline/preprocessing/target_full_standardize.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import pandas as pd

from dfm_pipeline.preprocessing.target_standardize import (
    read_quarterly_target,
    quarterly_to_monthly,
    standardize_target_on_window,
)


@dataclass(frozen=True)
class TargetStdStats:
    mean: float
    std: float
    start: pd.Timestamp
    end: pd.Timestamp
    nobs: int


def build_full_standardized_target(
    raw_quarterly_csv: Path,
    x_full_panel_csv: Path,
    *,
    train_start: str,
    train_end: str,
    monthly_freq: str = "MS",
    place: str = "end",
) -> Tuple[pd.Series, TargetStdStats]:
    yq = read_quarterly_target(
        raw_quarterly_csv,
        date_col="sasdate",
        value_col="gdp_qoq_saar",
    )

    ym_proto = quarterly_to_monthly(
        yq,
        monthly_freq=monthly_freq,
        place=place,
    )

    df_x = pd.read_csv(x_full_panel_csv)
    if "date" in df_x.columns:
        df_x["date"] = pd.to_datetime(df_x["date"])
    elif "Date" in df_x.columns:
        df_x = df_x.rename(columns={"Date": "date"})
        df_x["date"] = pd.to_datetime(df_x["date"])
    else:
        df_x = df_x.rename(columns={df_x.columns[0]: "date"})
        df_x["date"] = pd.to_datetime(df_x["date"])

    df_x = df_x.dropna(subset=["date"]).set_index("date").sort_index()
    idx = df_x.index

    ym = ym_proto.reindex(idx)

    yz, stats_obj = standardize_target_on_window(
        ym,
        start=train_start,
        end=train_end,
    )

    yz_trim = yz.loc[train_start:].copy()

    stats = TargetStdStats(
        mean=float(stats_obj.mean),
        std=float(stats_obj.std),
        start=pd.to_datetime(train_start),
        end=pd.to_datetime(train_end),
        nobs=int(stats_obj.nobs),
    )

    return yz_trim, stats
