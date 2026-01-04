# src/dfm_pipeline/covid/io.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


# -------------------------
# Basic helpers
# -------------------------
def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    path = Path(path)
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def write_json(obj: Any, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


# -------------------------
# CSV IO (safe defaults)
# -------------------------
@dataclass(frozen=True)
class CsvIOOptions:
    """
    CSV IO safety defaults for panel/mask artifacts.

    Key ideas:
    - Always overwrite (never append) to avoid "header becomes a data row"
      when a job is re-run.
    - Atomic writes to avoid partial/corrupt outputs if interrupted.
    - Defensive reads that drop accidental repeated header rows, e.g. a row
      with date == "date".
    """

    date_col: str = "date"
    float_format: Optional[str] = None  # e.g. "%.10g" if you want compact floats


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _atomic_write_csv(df: pd.DataFrame, path: Path, *, float_format: Optional[str] = None) -> None:
    """
    Write CSV atomically:
      1) write to tmp file in the same directory
      2) replace target path

    Always overwrites, uses consistent Unix newlines.
    """
    _ensure_parent(path)
    tmp = path.with_suffix(path.suffix + ".tmp")

    df.to_csv(
        tmp,
        index=False,
        float_format=float_format,
        mode="w",
        header=True,
        lineterminator="\n",  # IMPORTANT: correct kwarg name in pandas
    )
    tmp.replace(path)


def _drop_stray_header_rows(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """
    If a CSV was accidentally appended-to, it's common to get a repeated header
    row inside the body where date_col == "date" (string). We drop those rows.
    Also drops completely empty date rows.
    """
    if date_col not in df.columns:
        return df

    s = df[date_col].astype(str)
    bad = (s.str.lower() == date_col.lower()) | (s.str.strip() == "")
    if bad.any():
        df = df.loc[~bad].copy()
    return df


# -------------------------
# Readers
# -------------------------
def read_panel_csv(path: str | Path, *, opts: CsvIOOptions = CsvIOOptions()) -> pd.DataFrame:
    """
    Read a panel CSV saved with a leading date column.
    Returns a DataFrame with the date column preserved (not set as index).
    """
    path = Path(path)
    df = pd.read_csv(path, low_memory=False)
    df = _drop_stray_header_rows(df, opts.date_col)
    return df


def load_monthly_panel_csv(path: str | Path) -> pd.DataFrame:
    """
    Load a monthly panel CSV produced by the pipeline.

    Accepts common date column names: date, Date, sasdate.
    Returns a DataFrame with:
      - DatetimeIndex named "date"
      - numeric columns only (series)
    """
    path = Path(path)
    df = pd.read_csv(path, low_memory=False)

    # Find date column
    date_col = None
    for cand in ("date", "Date", "sasdate"):
        if cand in df.columns:
            date_col = cand
            break

    if date_col is None:
        # fallback: first column if it parses like dates
        first = df.columns[0]
        s = pd.to_datetime(df[first], errors="coerce")
        if float(s.notna().mean()) > 0.9:
            date_col = first
        else:
            raise ValueError(
                f"No date column found in {path}. Columns start: {list(df.columns)[:10]}"
            )

    # Drop stray header rows if present
    df = _drop_stray_header_rows(df, date_col)

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).sort_values(date_col)

    df = df.set_index(date_col)
    df.index.name = "date"

    # Keep numeric only, preserve order
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    return df.loc[:, num_cols]


# -------------------------
# Writers
# -------------------------
def write_panel_csv(df: pd.DataFrame, path: str | Path, *, opts: CsvIOOptions = CsvIOOptions()) -> None:
    """
    Write a panel CSV with a 'date' column first.

    - If df has a DatetimeIndex, we insert it as opts.date_col
    - Otherwise we require an existing opts.date_col column.
    """
    path = Path(path)

    if isinstance(df.index, pd.DatetimeIndex):
        out = df.copy()
        out.insert(0, opts.date_col, out.index)
    else:
        out = df.copy()
        if opts.date_col not in out.columns:
            raise ValueError(f"write_panel_csv expected a DatetimeIndex or a '{opts.date_col}' column: {path}")
        cols = [opts.date_col] + [c for c in out.columns if c != opts.date_col]
        out = out.loc[:, cols]

    _atomic_write_csv(out, path, float_format=opts.float_format)


def write_mask_matrix(mask: pd.DataFrame, path: str | Path, *, opts: CsvIOOptions = CsvIOOptions()) -> None:
    """
    Write a 0/1 mask matrix to CSV with a 'date' column first.

    Accepts either:
    - DatetimeIndex (preferred)
    - or an existing opts.date_col column
    """
    path = Path(path)

    if isinstance(mask.index, pd.DatetimeIndex):
        out = mask.copy()
        out = out.astype(int)
        out.insert(0, opts.date_col, out.index)
    else:
        out = mask.copy()
        if opts.date_col not in out.columns:
            raise ValueError(f"write_mask_matrix expected a DatetimeIndex or a '{opts.date_col}' column: {path}")
        cols = [opts.date_col] + [c for c in out.columns if c != opts.date_col]
        out = out.loc[:, cols]
        for c in out.columns:
            if c == opts.date_col:
                continue
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)

    _atomic_write_csv(out, path, float_format=opts.float_format)


def describe_mask(mask_df: pd.DataFrame, *, date_col: str = "date") -> Dict[str, Any]:
    """
    Quick summary stats for a 0/1 mask matrix.

    Accepts either:
      - a wide mask DataFrame that includes a date column (default: 'date')
      - or a DataFrame with only mask columns (no date col)
    """
    if mask_df is None or getattr(mask_df, "empty", True):
        return {"n_rows": 0, "n_cols": 0, "total_flagged": 0, "share_flagged": 0.0}

    df = mask_df.copy()
    if date_col in df.columns:
        df = df.drop(columns=[date_col])

    vals = df.apply(pd.to_numeric, errors="coerce").fillna(0)
    total_flagged = int(vals.to_numpy().sum())

    n_rows = int(vals.shape[0])
    n_cols = int(vals.shape[1])
    denom = n_rows * n_cols
    share_flagged = float(total_flagged) / float(denom) if denom else 0.0

    return {
        "n_rows": n_rows,
        "n_cols": n_cols,
        "total_flagged": total_flagged,
        "share_flagged": share_flagged,
    }