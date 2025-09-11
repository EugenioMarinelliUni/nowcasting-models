from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from dfm_pipeline.utils.hashing import sha256_file
from dfm_pipeline.validation.tcode_map import (
    validate_tcode_map_against_columns,
    ALLOWED_TCODES,
)


def detect_tcode_row(
    csv_path: str | Path,
    *,
    date_col: str = "sasdate",
    max_scan_rows: int = 5,
) -> Optional[int]:
    """
    Detect the 0-based file row (including the header row) that contains the embedded t-codes.

    Heuristic: read the header to get column names, then scan the next few lines; the first line
    whose non-date entries are all integers in ALLOWED_TCODES is the t-code row.

    Returns
    -------
    row_idx : int | None
        0-based row index of the t-code row (header is row 0). None if not found.
    """
    csv_path = Path(csv_path)
    cols = pd.read_csv(csv_path, nrows=0).columns.tolist()

    block = pd.read_csv(
        csv_path,
        header=None,       # treat scanned lines as data
        names=cols,        # real header names
        skiprows=1,        # skip header row
        nrows=max_scan_rows,
        dtype=str
    )

    for i, (_, row) in enumerate(block.iterrows()):
        vals = row.drop(labels=[date_col], errors="ignore")
        nums = pd.to_numeric(vals, errors="coerce")
        if nums.notna().all():
            try:
                cand = set(nums.astype(int).tolist())
            except Exception:
                continue
            if cand.issubset(ALLOWED_TCODES):
                return 1 + i  # header row (0) + offset
    return None


def read_embedded_tcode_map(
    csv_path: str | Path, *, date_col: str, tcode_row: int
) -> Dict[str, int]:
    """
    Read ONLY the embedded t-code row from a FRED-MD-style CSV and build {series: tcode}.

    Parameters
    ----------
    csv_path : str | Path
        CSV path (header row + t-code row + data rows).
    date_col : str
        Name of the date column in the header.
    tcode_row : int
        0-based row index (including header) of the t-code row.

    Returns
    -------
    tcode_map : dict[str, int]
        Mapping for present series; codes restricted to ALLOWED_TCODES.
    """
    csv_path = Path(csv_path)
    cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
    row = pd.read_csv(
        csv_path,
        header=None,
        names=cols,
        skiprows=tcode_row,
        nrows=1,
        dtype=str
    ).iloc[0]
    series = pd.to_numeric(row.drop(labels=[date_col], errors="ignore"), errors="coerce")
    return {k: int(v) for k, v in series.dropna().items() if int(v) in ALLOWED_TCODES}


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
    Ingest & extract the transformation map (one-time per vintage), persisting a clean JSON.

    Steps
    -----
    1) Read CSV header to get series names (incl. `date_col`).
    2) Locate the embedded t-code row (given or auto-detected; default to row 1 if not found).
    3) Read that row only and build {series -> tcode}.
    4) Validate against header columns (coverage; allowed values).
    5) Write the pure mapping JSON; optionally write a sidecar metadata JSON.

    Parameters
    ----------
    csv_path : str | Path
        Original FRED-MD-style CSV (header + t-code row + data rows).
    out_json_path : str | Path
        Destination for {"SERIES": code, ...}.
    date_col : str
        Name of date column in the CSV.
    tcode_row : int | None
        0-based row index of the t-code row; if None, detect or default to 1.
    autodetect_tcode_row : bool
        Attempt to detect the t-code row if not provided.
    require_all_tcodes : bool
        Raise if any series column lacks a code.
    overwrite : bool
        Allow overwriting an existing mapping JSON.
    write_sidecar_metadata : bool
        If True, write `<out_json_path>.meta.json` with provenance.

    Returns
    -------
    tcode_map : dict[str, int]
        Clean map for present series (codes in ALLOWED_TCODES).
    info : dict
        Diagnostics and provenance (tcode_row, csv_sha256, counts, missing/extra).
    """
    csv_path = Path(csv_path)
    out_json_path = Path(out_json_path)

    # Header columns (no need to read data rows)
    header_cols = pd.read_csv(csv_path, nrows=0).columns.tolist()
    if date_col not in header_cols:
        raise ValueError(
            f"Expected date column '{date_col}' in CSV header; found: {header_cols[:6]}..."
        )

    # Find the t-code row
    if tcode_row is None:
        tcode_row = detect_tcode_row(csv_path, date_col=date_col) if autodetect_tcode_row else None
        if tcode_row is None:
            tcode_row = 1  # conventional default: header at row 0, tcodes at row 1

    # Build mapping from embedded row
    raw_map = read_embedded_tcode_map(csv_path, date_col=date_col, tcode_row=tcode_row)

    # Validate/clean against header
    tcode_map, check = validate_tcode_map_against_columns(
        df_columns=header_cols,
        tcode_map=raw_map,
        date_col=date_col,
        require_all=require_all_tcodes,
    )

    # Overwrite policy
    if out_json_path.exists() and not overwrite:
        raise FileExistsError(f"{out_json_path} already exists. Set overwrite=True to replace.")

    # Write mapping JSON
    out_json_path.parent.mkdir(parents=True, exist_ok=True)
    with out_json_path.open("w", encoding="utf-8") as f:
        json.dump(tcode_map, f, indent=2, sort_keys=True)

    # Provenance / diagnostics
    info = {
        "tcode_row": tcode_row,
        "csv_path": str(csv_path),
        "csv_sha256": sha256_file(csv_path),
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
