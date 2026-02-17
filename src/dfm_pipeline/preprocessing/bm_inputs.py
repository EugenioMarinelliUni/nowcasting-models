"""Build Bańbura–Modugno (BM) model inputs.

Produces two consistent variants:

1) Frozen scaling (fixed window):
   - Standardize using mean/std computed on a chosen window.
   - Run DFM with scaling_mode=external_frozen.

2) Toolbox-vintage / per-vintage rescaling:
   - Save raw (tcode-transformed but unstandardized) inputs.
   - Run DFM with scaling_mode=toolbox_vintage so scaling is recomputed per fit call.

Target mapping convention:
- Quarterly target is mapped to a monthly index by placing the quarterly value
  on the quarter-end month (Mar/Jun/Sep/Dec). If your quarterly series is dated
  at quarter-start (e.g., 1960-04-01), it is correctly placed at 1960-06-01.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from dfm_pipeline.preprocessing.fixed_window_standardize import (
    standardize_panel_on_window,
)
from dfm_pipeline.preprocessing.target_standardize import (
    read_quarterly_target,
    quarterly_to_monthly,
    standardize_target_on_window,
)


@dataclass(frozen=True)
class BMInputsPaths:
    X_raw: Path
    y_raw_monthly: Path
    X_frozen_z: Path
    y_frozen_z_monthly: Path
    frozen_panel_scaler_json: Path
    frozen_target_scaler_json: Path


def _infer_value_col(df: pd.DataFrame, date_col: str) -> str:
    cols = [c for c in df.columns if c != date_col]
    if "value" in cols:
        return "value"
    if "y" in cols and len(cols) == 1:
        return "y"
    if len(cols) == 1:
        return cols[0]
    raise ValueError(
        f"Target CSV has multiple candidate value columns {cols}. "
        f"Provide a single value column or rename one to 'value'."
    )


def _read_panel(panel_csv: str | Path, date_col: str) -> pd.DataFrame:
    df = pd.read_csv(panel_csv, parse_dates=[date_col]).set_index(date_col).sort_index()
    # ensure float and keep NaNs
    return df.apply(pd.to_numeric, errors="coerce")


def _build_monthly_target_for_panel(
    panel_index: pd.DatetimeIndex,
    quarterly_target_csv: str | Path,
    date_col: str,
    value_col: Optional[str] = None,
    place: str = "end",
) -> pd.Series:
    # We need to read the raw file once to infer the value column if not specified.
    raw = pd.read_csv(quarterly_target_csv)
    if value_col is None:
        value_col = _infer_value_col(raw, date_col=date_col)

    yq = read_quarterly_target(
        quarterly_target_csv,
        date_col=date_col,
        value_col=value_col,
    )
    ym = quarterly_to_monthly(yq, place=place, monthly_freq="MS")

    y_monthly = pd.Series(np.nan, index=panel_index, name="y", dtype=float)
    common = panel_index.intersection(ym.index)
    y_monthly.loc[common] = ym.loc[common].astype(float).values
    return y_monthly


def build_bm_inputs_from_raw(
    panel_csv: str | Path,
    quarterly_target_csv: str | Path,
    outdir: str | Path,
    cfg: Dict[str, Any],
    window: Tuple[str, str] = ("1960-01-01", "2015-12-01"),
    date_col_override: Optional[str] = None,
    emit_raw: bool = True,
    emit_frozen: bool = True,
) -> BMInputsPaths:
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    date_col = date_col_override or cfg.get("dates", {}).get("date_col", "sasdate")

    X_raw_df = _read_panel(panel_csv, date_col=date_col)

    # Build monthly target (quarter-end placement)
    target_anchor = cfg.get("labeling", {}).get("target_anchor", "quarter_end_month")
    place = "end" if target_anchor in ("quarter_end_month", "end", "quarter_end") else "end"

    y_raw_monthly = _build_monthly_target_for_panel(
        panel_index=X_raw_df.index,
        quarterly_target_csv=quarterly_target_csv,
        date_col=date_col,
        value_col=None,
        place=place,
    )

    # Paths
    X_raw_path = outdir / "X_panel__bm_raw.csv"
    y_raw_path = outdir / "y_target__bm_monthly_raw.csv"

    X_z_path = outdir / "X_panel_z__bm.csv"
    y_z_path = outdir / "y_target_z__bm_monthly.csv"

    panel_scaler_path = outdir / "scaler_panel_frozen.json"
    target_scaler_path = outdir / "scaler_target_frozen.json"

    # 1) Raw artifacts (toolbox-vintage mode inputs)
    if emit_raw:
        X_raw_df.to_csv(X_raw_path, index=True)
        y_raw_monthly.to_frame("y").to_csv(y_raw_path, index=True)

    # 2) Frozen standardized artifacts (external_frozen mode inputs)
    if emit_frozen:
        start, end = window
        X_z, panel_stats = standardize_panel_on_window(
            X_raw_df, window_start=start, window_end=end
        )

        y_z, target_stats = standardize_target_on_window(
            y_raw_monthly, window_start=start, window_end=end
        )

        X_z.to_csv(X_z_path, index=True)
        y_z.to_frame("y").to_csv(y_z_path, index=True)

        panel_payload = {
            "window_start": start,
            "window_end": end,
            "mean": panel_stats["mean"].to_dict(),
            "std": panel_stats["std"].to_dict(),
        }
        target_payload = {
            "window_start": start,
            "window_end": end,
            "mean": float(target_stats["mean"]),
            "std": float(target_stats["std"]),
        }
        panel_scaler_path.write_text(json.dumps(panel_payload, indent=2))
        target_scaler_path.write_text(json.dumps(target_payload, indent=2))

    return BMInputsPaths(
        X_raw=X_raw_path,
        y_raw_monthly=y_raw_path,
        X_frozen_z=X_z_path,
        y_frozen_z_monthly=y_z_path,
        frozen_panel_scaler_json=panel_scaler_path,
        frozen_target_scaler_json=target_scaler_path,
    )