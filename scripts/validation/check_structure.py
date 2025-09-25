#!/usr/bin/env python3
"""
Structure-only checks for a monthly panel CSV.

Focus:
  • Date/index hygiene (sasdate parse, monotonic, monthly MS grid)
  • Column naming (clean + unique)
  • Data types (numeric columns)
  • Basic shape sanity

Deliberately ignores: missingness levels/positions (covered elsewhere).

Run:
  python scripts/validation/check_structure.py data/processed_data/panel_start_1965_keep_late.csv
"""

from __future__ import annotations
from pathlib import Path
import argparse
import sys
import pandas as pd

# If you created the utils as discussed:
try:
    from dfm_pipeline.utils import load_panel_csv, ensure_monthly_index, clean_columns
except Exception:
    load_panel_csv = None  # fallback to local robust loader if utils not available

DATE_COL = "sasdate"

def _load_csv(path: Path, date_fmt: str) -> pd.DataFrame:
    """Robust loader that works whether sasdate is index or column."""
    if load_panel_csv is not None:
        return load_panel_csv(path, date_fmt=date_fmt)

    # Fallback (kept minimal)
    try:
        df = pd.read_csv(path, parse_dates=[DATE_COL], index_col=DATE_COL)
    except Exception:
        df = pd.read_csv(path)
        if DATE_COL not in df.columns:
            raise ValueError(f"{path}: '{DATE_COL}' not found as index or column.")
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], format=date_fmt, errors="coerce")
        if df[DATE_COL].isna().any():
            bad = df.loc[df[DATE_COL].isna()].index[:5].tolist()
            raise ValueError(f"{path}: some dates failed to parse with {date_fmt}. First bad rows: {bad}")
        df = df.set_index(DATE_COL)
    df = df.sort_index()
    df.index.name = DATE_COL
    return df

def check_structure(csv_path: Path, date_fmt: str, enforce_monthly_grid: bool) -> int:
    """Return 0 if OK, 1 on structural failure."""
    problems: list[str] = []
    df = _load_csv(csv_path, date_fmt=date_fmt)

    # 1) Index hygiene
    if not isinstance(df.index, pd.DatetimeIndex):
        problems.append("Index is not a DatetimeIndex.")
    if df.index.has_duplicates:
        problems.append("Duplicate timestamps in index.")
    if not df.index.is_monotonic_increasing:
        problems.append("Index not sorted ascending.")

    # 2) Enforce monthly MS grid (or at least report gaps)
    idx = pd.DatetimeIndex(df.index).sort_values()
    full = pd.date_range(idx.min(), idx.max(), freq="MS")
    missing_rows = full.difference(idx)
    if enforce_monthly_grid:
        # Reindex; we only *report* here (no file writes)
        df = df.reindex(full)
        df.index.name = DATE_COL
        # After reindex, the grid is contiguous by construction
    if len(missing_rows) > 0:
        problems.append(f"Monthly grid gaps detected before reindex: {len(missing_rows)}")

    # 3) Column naming
    # Clean columns (if you have utils.clean_columns, this would be applied upstream in preprocessing)
    if clean_columns is not None:
        df.columns = [c.strip().replace(" ", "_") for c in df.columns]
    # Duplicates?
    dup = df.columns[df.columns.duplicated()]
    if len(dup) > 0:
        problems.append(f"Duplicate column names: {sorted(set(dup.tolist()))[:6]}{'...' if len(dup)>6 else ''}")

    # 4) Dtypes: numeric-only
    non_numeric = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    if non_numeric:
        problems.append(f"Non-numeric columns present: {non_numeric[:6]}{'...' if len(non_numeric)>6 else ''}")

    # 5) Basic shape
    if df.shape[0] == 0 or df.shape[1] == 0:
        problems.append(f"Empty table after load: rows={df.shape[0]}, cols={df.shape[1]}")

    # ---- Report ----
    print(f"\n== {csv_path} ==")
    print(f"rows={len(df):,} cols={df.shape[1]} "
          f"start={df.index.min().date() if len(df) else 'NA'} "
          f"end={df.index.max().date() if len(df) else 'NA'}")
    print(f"monthly_grid_gaps_pre_reindex={len(missing_rows)} "
          f"(enforced_contiguous={'yes' if enforce_monthly_grid else 'no'})")
    print(f"non_numeric_cols={len(non_numeric)} duplicate_col_names={'yes' if len(dup)>0 else 'no'}")

    if problems:
        print("\n[STRUCTURE FAIL]")
        for p in problems:
            print(" -", p)
        return 1

    print("\n[STRUCTURE OK]")
    return 0

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Structure-only checks for a monthly panel CSV")
    ap.add_argument("csv", type=Path, help="Path to panel CSV")
    ap.add_argument("--date-fmt", default="%m/%d/%Y", help="Date format used in the CSV (default mm/dd/yyyy)")
    ap.add_argument("--enforce-monthly-grid", action="store_true",
                    help="Reindex in-memory to a contiguous Month-Start grid before checks")
    return ap.parse_args()

def main():
    args = parse_args()
    code = check_structure(args.csv, date_fmt=args.date_fmt, enforce_monthly_grid=args.enforce_monthly_grid)
    sys.exit(code)

if __name__ == "__main__":
    main()
