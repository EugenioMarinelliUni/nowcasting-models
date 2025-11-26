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
    place: str = "start",  # "start" (Jan/Apr/Jul/Oct) or "end" (Mar/Jun/Sep/Dec)
) -> Tuple[pd.Series, TargetStdStats]:
    """
    Build a full standardized target series aligned to a full monthly X panel.

    Steps:
      1. Read raw quarterly target from raw_quarterly_csv.
      2. Convert quarterly -> monthly using (monthly_freq, place).
      3. Read the full X panel index from x_full_panel_csv ('date' column).
      4. Reindex monthly target to that index.
      5. Standardize on [train_start, train_end] and apply frozen μ,σ to the full index.
      6. Drop any rows before train_start, so the result covers [train_start, end-of-sample].

    Returns:
      - yz_trim: standardized monthly target (pd.Series) indexed by the X panel index.
      - stats:   TargetStdStats(mean, std, start, end, nobs) for the training window.
    """
    # 1) Load raw quarterly target
    #    This assumes your read_quarterly_target knows how to produce a 'gdp_qoq_saar'
    #    series from the raw A191RL1Q225SBEA_latest.csv.
    yq = read_quarterly_target(
        raw_quarterly_csv,
        date_col="sasdate",
        value_col="gdp_qoq_saar",
    )

    # 2) Quarter -> monthly mapping
    ym_proto = quarterly_to_monthly(
        yq,
        monthly_freq=monthly_freq,
        place=place,
    )

    # 3) Load X full panel index
    df_x = pd.read_csv(x_full_panel_csv, parse_dates=["date"])
    df_x = df_x.dropna(subset=["date"]).set_index("date").sort_index()
    idx = df_x.index

    # 4) Align monthly target to X index
    ym = ym_proto.reindex(idx)

    # 5) Standardize on training window using your existing helper.
    #    standardize_target_on_window is expected to compute μ,σ on [train_start, train_end]
    #    and apply them to the full ym series, returning yz with the same index.
    yz, stats_obj = standardize_target_on_window(
        ym,
        start=train_start,
        end=train_end,
    )

    # 6) Trim to [train_start, end-of-sample] to mirror the X_full behavior.
    yz_trim = yz.loc[train_start:].copy()

    # Wrap stats in a simple dataclass
    # standardize_target_on_window likely returns something with mean/std/nobs attributes.
    stats = TargetStdStats(
        mean=float(stats_obj.mean),
        std=float(stats_obj.std),
        start=pd.to_datetime(train_start),
        end=pd.to_datetime(train_end),
        nobs=int(stats_obj.nobs),
    )

    return yz_trim, stats
