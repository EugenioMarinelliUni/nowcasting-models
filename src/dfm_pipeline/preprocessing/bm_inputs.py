from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dfm_pipeline.preprocessing.fixed_window_standardize import standardize_full_panel_on_window
from dfm_pipeline.preprocessing.target_standardize import (
    TargetStdStats,
    make_monthly_target,
    read_quarterly_target,
    standardize_monthly_target_on_window,
)


@dataclass(frozen=True)
class BMInputPaths:
    x_raw_csv: str
    y_raw_monthly_csv: str
    x_z_csv: str
    y_z_monthly_csv: str
    scalers_json: str


def _ensure_month_start_index(idx: pd.DatetimeIndex) -> pd.DatetimeIndex:
    # Robustly coerce any date-like index to MS (month start).
    # This avoids subtle “month end vs month start” mismatches.
    return pd.DatetimeIndex(idx).to_period("M").to_timestamp("MS")


def _read_panel(panel_csv: str, date_col: str) -> pd.DataFrame:
    df = pd.read_csv(panel_csv, parse_dates=[date_col])
    df = df.set_index(date_col).sort_index()
    df.index = _ensure_month_start_index(df.index)

    # Keep only numeric columns; coerce to float.
    df = df.apply(pd.to_numeric, errors="coerce").astype(float)
    return df


def _write_with_date_col(df: pd.DataFrame | pd.Series, out_csv: str, date_col_out: str) -> None:
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if isinstance(df, pd.Series):
        out_df = df.to_frame()
    else:
        out_df = df

    out_df = out_df.copy()
    out_df.index = _ensure_month_start_index(pd.DatetimeIndex(out_df.index))
    out_df.insert(0, date_col_out, out_df.index)
    out_df.to_csv(out_path, index=False)


def build_bm_inputs_from_raw(
    *,
    panel_csv: str,
    quarterly_target_csv: str,
    date_col_panel: str,
    date_col_target: str,
    target_col: str,
    outdir: str,
    train_start: str,
    train_end: str,
    date_col_out: str = "sasdate",
    target_anchor: str = "quarter_end_month",
) -> BMInputPaths:
    """
    Builds BOTH configurations you want:

    1) Raw BM inputs (unstandardized):
       - X_panel__bm_raw.csv
       - y_target__bm_monthly.csv   (quarterly values placed on quarter-end month; other months NaN)

    2) Frozen-scaler BM inputs (standardized on [train_start, train_end]):
       - X_panel_z__bm.csv
       - y_target_z__bm_monthly.csv

    Notes
    - Panel is assumed to be already transformed (tcodes etc) but not standardized.
    - The target is assumed to be QUARTERLY in quarterly_target_csv.
    - Output indices are forced to Month-Start (MS) to match your BM-DFM machinery.
    """
    outdir = str(Path(outdir))
    outdir_p = Path(outdir)
    outdir_p.mkdir(parents=True, exist_ok=True)

    # 1) Read panel (raw transformed)
    X_raw = _read_panel(panel_csv, date_col_panel)

    # 2) Read quarterly target and map to monthly (BM convention: GDP on quarter-end month)
    y_q = read_quarterly_target(
        quarterly_target_csv,
        target_col=target_col,
        date_col=date_col_target,
    )
    y_m = make_monthly_target(y_q, anchor=target_anchor)
    y_m.index = _ensure_month_start_index(pd.DatetimeIndex(y_m.index))

    # 3) Align to common monthly index (intersection)
    idx = X_raw.index.intersection(y_m.index)
    X_raw = X_raw.loc[idx]
    y_m = y_m.loc[idx]

    # 4) Write raw BM inputs
    x_raw_out = str(outdir_p / "X_panel__bm_raw.csv")
    y_raw_out = str(outdir_p / "y_target__bm_monthly.csv")
    _write_with_date_col(X_raw, x_raw_out, date_col_out)
    _write_with_date_col(y_m.rename("y"), y_raw_out, date_col_out)

    # 5) Frozen-window standardization for X (reuse existing implementation)
    X_z, x_stats = standardize_full_panel_on_window(
        panel_csv=panel_csv,
        train_start=train_start,
        train_end=train_end,
        date_col=date_col_panel,
    )
    X_z.index = _ensure_month_start_index(pd.DatetimeIndex(X_z.index))
    X_z = X_z.loc[idx]  # enforce same aligned index used for BM y_m

    # 6) Frozen-window standardization for monthly BM y
    y_z, y_stats = standardize_monthly_target_on_window(
        y_m.rename("y"),
        train_start=train_start,
        train_end=train_end,
    )

    # 7) Write standardized BM inputs
    x_z_out = str(outdir_p / "X_panel_z__bm.csv")
    y_z_out = str(outdir_p / "y_target_z__bm_monthly.csv")
    _write_with_date_col(X_z, x_z_out, date_col_out)
    _write_with_date_col(y_z.rename("y"), y_z_out, date_col_out)

    # 8) Persist scalers for traceability
    scalers = {
        "panel": {
            "train_start": train_start,
            "train_end": train_end,
            "date_col_panel": date_col_panel,
            "mean": x_stats.mean.to_dict(),
            "std": x_stats.std.to_dict(),
        },
        "target_monthly_bm": {
            "train_start": train_start,
            "train_end": train_end,
            "anchor": target_anchor,
            "mean": float(y_stats.mean),
            "std": float(y_stats.std),
        },
        "meta": {
            "panel_csv": panel_csv,
            "quarterly_target_csv": quarterly_target_csv,
            "target_col": target_col,
            "date_col_target": date_col_target,
            "date_col_out": date_col_out,
        },
    }

    scalers_out = str(outdir_p / "bm_scalers.json")
    Path(scalers_out).write_text(pd.Series(scalers).to_json(), encoding="utf-8")

    return BMInputPaths(
        x_raw_csv=x_raw_out,
        y_raw_monthly_csv=y_raw_out,
        x_z_csv=x_z_out,
        y_z_monthly_csv=y_z_out,
        scalers_json=scalers_out,
    )