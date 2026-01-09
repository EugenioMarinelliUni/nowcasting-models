"""
Time-series transformation utilities.

Contains:
1) apply_tcode_transformations / standardize:
   Stock–Watson / FRED-MD style per-series transformations and standardization.

2) extract_tcodes_to_json (+ helpers):
   Extract embedded FRED-MD t-codes row from a CSV (header + tcode row + data)
   and write a clean JSON mapping {series: tcode}.
"""

from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd


# Allowed Stock–Watson / FRED-MD transformation codes
_ALLOWED_TCODES = {1, 2, 3, 4, 5, 6, 7}


# ---------- helpers ----------

def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA-256 of a file (for provenance in metadata)."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _detect_tcode_row(
    csv_path: str | Path,
    *,
    date_col: str = "sasdate",
    max_scan_rows: int = 5,
) -> Optional[int]:
    """
    Detect which 0-based file row (including the header row) contains the embedded t-codes.
    Typical FRED-MD files -> row 1 (i.e., second line). Returns None if not found.
    """
    csv_path = Path(csv_path)
    cols = pd.read_csv(csv_path, nrows=0).columns.tolist()

    # Read a few rows after header as DATA (not header)
    block = pd.read_csv(
        csv_path, header=None, names=cols, skiprows=1, nrows=max_scan_rows, dtype=str
    )
    for i, (_, row) in enumerate(block.iterrows()):
        vals = row.drop(labels=[date_col], errors="ignore")
        nums = pd.to_numeric(vals, errors="coerce")
        # A bona fide tcode row has only integers in {1..7} on non-date columns
        if nums.notna().all() and set(nums.astype(int)).issubset(_ALLOWED_TCODES):
            return 1 + i
    return None


def _read_embedded_tcode_map(
    csv_path: str | Path, *, date_col: str, tcode_row: int
) -> Dict[str, int]:
    """
    Read the embedded t-code row and return {series_name: tcode}.
    Assumes header is at row 0 and tcode_row is 0-based (including header).
    """
    csv_path = Path(csv_path)
    cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
    row = pd.read_csv(
        csv_path, header=None, names=cols, skiprows=tcode_row, nrows=1, dtype=str
    ).iloc[0]
    series = pd.to_numeric(row.drop(labels=[date_col], errors="ignore"), errors="coerce")
    # Keep only valid codes in {1..7}
    return {k: int(v) for k, v in series.dropna().items() if int(v) in _ALLOWED_TCODES}


def _validate_tcode_map_against_columns(
    df_columns: list[str],
    tcode_map: Dict[str, int],
    *,
    date_col: str = "sasdate",
    require_all: bool = True,
) -> Tuple[Dict[str, int], Dict]:
    """
    Clean & check the map against actual data columns (without reading data rows).
    """
    cols = [c for c in df_columns if c != date_col]

    cleaned: Dict[str, int] = {}
    for k, v in tcode_map.items():
        try:
            iv = int(v)
        except (TypeError, ValueError):
            iv = np.nan
        if k in cols and iv in _ALLOWED_TCODES:
            cleaned[k] = int(iv)

    missing = [c for c in cols if c not in cleaned]
    extra = [k for k in tcode_map if k not in cols and k != date_col]

    if require_all and missing:
        raise KeyError(f"No tcode for columns: {missing}")

    return cleaned, {"missing_in_tcodes": missing, "extra_in_tcodes": extra}


# ---------- public API for Step A ----------

def extract_tcodes_to_json(
    csv_path: str | Path,
    out_json_path: str | Path,
    *,
    date_col: str = "sasdate",
    tcode_row: int | None = None,
    autodetect_tcode_row: bool = True,
    require_all_tcodes: bool = True,
    overwrite: bool = False,
    write_sidecar_metadata: bool = True,
) -> Tuple[Dict[str, int], Dict]:
    """
    Step A: Ingest & extract the transformation map (one-time per vintage).

    What it does
    ------------
    1) Reads the CSV header to get the true series names (including `date_col`).
    2) Finds the embedded t-code row (either provided or auto-detected).
    3) Reads only that row as data and builds {series -> tcode} (dropping `date_col`).
    4) Optionally checks that every series has a valid code (1..7).
    5) Writes a pure JSON mapping to `out_json_path`.
    6) Optionally writes a sidecar metadata JSON with provenance info.

    Returns
    -------
    tcode_map : dict[str, int]
    info : dict with diagnostics and provenance (tcode_row, csv_sha256, n_series, etc.)
    """
    csv_path = Path(csv_path)
    out_json_path = Path(out_json_path)

    # Read header to obtain the definitive list of columns
    header_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
    if date_col not in header_cols:
        raise ValueError(
            f"Expected date column '{date_col}' in CSV header; found {header_cols[:6]}..."
        )

    # Determine which row contains t-codes
    if tcode_row is None:
        if autodetect_tcode_row:
            tcode_row = _detect_tcode_row(csv_path, date_col=date_col) or 1
        else:
            tcode_row = 1  # conventional FRED-MD layout

    # Build tcode map from embedded row
    raw_map = _read_embedded_tcode_map(csv_path, date_col=date_col, tcode_row=tcode_row)

    # Validate map against header columns (no need to read the data block)
    tcode_map, check = _validate_tcode_map_against_columns(
        df_columns=header_cols,
        tcode_map=raw_map,
        date_col=date_col,
        require_all=require_all_tcodes,
    )

    # Handle file overwrite policy
    if out_json_path.exists() and not overwrite:
        raise FileExistsError(f"{out_json_path} already exists. Set overwrite=True to replace.")

    # Write the pure mapping JSON
    out_json_path.parent.mkdir(parents=True, exist_ok=True)
    with out_json_path.open("w", encoding="utf-8") as f:
        json.dump(tcode_map, f, indent=2, sort_keys=True)

    # Optional sidecar metadata (keeps mapping JSON clean for downstream code)
    info = {
        "tcode_row": tcode_row,
        "csv_path": str(csv_path),
        "csv_sha256": _sha256_file(csv_path),
        "n_series": len(tcode_map),
        "missing_in_tcodes": check["missing_in_tcodes"],
        "extra_in_tcodes": check["extra_in_tcodes"],
        "mapping_json_path": str(out_json_path),
    }

    if write_sidecar_metadata:
        meta_path = out_json_path.with_suffix(out_json_path.suffix + ".meta.json")
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(info, f, indent=2, sort_keys=True)
        info["metadata_json_path"] = str(meta_path)

    return tcode_map, info


# ---------- Stock–Watson/FRED-MD transformations ----------

def apply_tcode_transformations(df: pd.DataFrame, tcode_map: dict) -> pd.DataFrame:
    """
    Apply Stock–Watson-style transformations to each series in a DataFrame
    according to a tcode mapping.

    Parameters
    ----------
    df : DataFrame
        Raw data (rows=time, columns=series).
    tcode_map : dict
        Mapping series name -> transformation code in {1..7}.

    Returns
    -------
    DataFrame
        Transformed series aligned to df.index.
    """
    def transform_series(x: pd.Series, code: int) -> pd.Series:
        x = x.copy()
        if code == 1:
            return x
        if code == 2:
            return x.diff()
        if code == 3:
            return x.diff().diff()
        if code == 4:
            return np.log(x.replace(0, np.nan))
        if code == 5:
            return np.log(x.replace(0, np.nan)).diff()
        if code == 6:
            return np.log(x.replace(0, np.nan)).diff().diff()
        if code == 7:
            return (x / x.shift(1) - 1).diff()
        raise ValueError(f"Unknown tcode: {code}")

    transformed = {}
    for col in df.columns:
        tcode = tcode_map.get(col)
        if tcode is None:
            raise KeyError(f"No transformation code found for series: {col}")
        transformed[col] = transform_series(df[col], int(tcode))

    return pd.DataFrame(transformed, index=df.index)


def standardize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Demean and standardize each column to have zero mean and unit variance.
    """
    return (df - df.mean()) / df.std()
