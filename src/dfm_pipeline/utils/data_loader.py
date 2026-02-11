from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd

from dfm_pipeline.utils.config_loader import load_config


def _read_time_indexed_csv(path: Path) -> pd.DataFrame:
    """Read a CSV whose first column contains timestamps."""
    df = pd.read_csv(path)
    if df.empty:
        return df
    date_col = str(df.columns[0])
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
    return df


def load_data(
    stage: str = "raw",
    *,
    processed_key: str | None = None,
) -> Tuple[pd.DataFrame, pd.Series]:
    """Load data for a given stage.

    stage:
        "raw"      -> load raw FRED-MD CSV (includes a transformation-code row)
        "processed" -> load a processed panel CSV (does not include tcodes)

    processed_key:
        When stage="processed", selects which file to read. Resolution order:
          1) explicit `processed_key` (either a key in config["paths"] or a file path)
          2) config["paths"]["processed_filename"] (legacy)
          3) config["paths"]["panel_1965"] (default fallback)
    """
    config = load_config()
    paths = dict(config.get("paths", {}))

    if stage == "raw":
        raw_filename = paths.get("raw_filename")
        if not raw_filename:
            raise KeyError("config.paths.raw_filename is missing")

        path = Path("data") / "raw_data" / str(raw_filename)
        if not path.exists():
            raise FileNotFoundError(f"File not found at: {path}")

        transform_codes_df = pd.read_csv(path, nrows=1, skiprows=1)
        transform_codes = transform_codes_df.squeeze()
        if isinstance(transform_codes, pd.DataFrame):
            transform_codes = transform_codes.iloc[0]
        transform_codes = pd.Series(transform_codes)

        df = pd.read_csv(
            path,
            skiprows=[1],
            parse_dates=["sasdate"],
            index_col="sasdate",
        ).sort_index()

        return df, transform_codes

    if stage == "processed":
        key = processed_key or paths.get("processed_filename") or "panel_1965"

        if key in paths:
            path = Path(str(paths[key]))
        else:
            path = Path(str(key))

        if not path.exists():
            available = ", ".join(sorted(str(k) for k in paths.keys()))
            raise FileNotFoundError(
                f"Processed data file not found: {path}. "
                f"If you intended to use a config key, available config.paths keys are: {available}"
            )

        df = _read_time_indexed_csv(path)
        transform_codes = pd.Series(dtype="float64")
        return df, transform_codes

    raise ValueError("stage must be 'raw' or 'processed'")
