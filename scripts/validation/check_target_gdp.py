#!/usr/bin/env python3
"""
Structure-only checks for a quarterly target CSV (e.g., BEA/FRED A191RL1Q225SBEA).

Validates:
  • 'sasdate' column parses to datetimes and is strictly increasing with unique timestamps
  • All dates correspond to quarter-end months (Mar, Jun, Sep, Dec)
  • Target value column exists and is numeric (coercible to float)
  • Prints a compact report; exits 0 on OK, 1 on failure

Run examples:
  python scripts/validation/check_target_gdp.py data/raw_data/targets/gdp/A191RL1Q225SBEA_latest.csv
  python scripts/validation/check_target_gdp.py data/raw_data/targets/gdp/A191RL1Q225SBEA_latest.csv --value-col A191RL1Q225SBEA
"""

from __future__ import annotations
from pathlib import Path
import argparse
import sys
import pandas as pd

DATE_COL = "sasdate"
QE_MONTHS = {3, 6, 9, 12}  # quarter-end months

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Check structure of a quarterly GDP target CSV")
    ap.add_argument("csv", type=Path, help="Path to target CSV (e.g., A191RL1Q225SBEA_latest.csv)")
    ap.add_argument("--value-col", help="Name of the value column (default: auto-detect: first non-sasdate column)")
    ap.add_argument("--date-fmt", default=None,
                    help="Optional explicit date format for 'sasdate' (e.g., %%Y-%%m-%%d). If omitted, pandas infers.")
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    p = args.csv

    # --- Load ---
    try:
        df = pd.read_csv(p)
    except Exception as e:
        print(f"[FAIL] Could not read CSV: {p}\n  -> {e}")
        sys.exit(1)

    problems: list[str] = []
    if DATE_COL not in df.columns:
        problems.append(f"Missing '{DATE_COL}' column.")
        _exit(p, df, None, None, None, problems)

    # Parse dates
    try:
        if args.date_fmt:
            df[DATE_COL] = pd.to_datetime(df[DATE_COL], format=args.date_fmt, errors="raise")
        else:
            df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="raise")
    except Exception as e:
        problems.append(f"Failed to parse '{DATE_COL}': {e}")

    # Determine value column
    value_cols = [c for c in df.columns if c != DATE_COL]
    if args.value_col:
        if args.value_col not in value_cols:
            problems.append(f"Value column '{args.value_col}' not found. Available: {value_cols}")
        value_col = args.value_col
    else:
        if len(value_cols) == 0:
            problems.append("No value column found besides 'sasdate'.")
            value_col = None
        elif len(value_cols) > 1:
            # Prefer explicitness if multiple present
            problems.append(f"Multiple value columns found: {value_cols}. Use --value-col to choose one.")
            value_col = value_cols[0]
        else:
            value_col = value_cols[0]

    if problems:
        _exit(p, df, None, None, value_cols, problems)

    # Sort and basic index checks
    df = df.sort_values(DATE_COL).reset_index(drop=True)

    if df[DATE_COL].duplicated().any():
        dup = df.loc[df[DATE_COL].duplicated(), DATE_COL].head(5).tolist()
        problems.append(f"Duplicate dates found. First duplicates: {dup}")

    if not df[DATE_COL].is_monotonic_increasing:
        problems.append("Dates are not strictly increasing.")

    # Quarter-end check
    q_end_months = df[DATE_COL].dt.to_period("Q").asfreq("M").dt.month
    if not q_end_months.isin(QE_MONTHS).all():
        bad_rows = df.loc[~q_end_months.isin(QE_MONTHS), DATE_COL].dt.strftime("%Y-%m-%d").head(5).tolist()
        problems.append(f"Found non-quarter-end dates (examples: {bad_rows}).")

    # Numeric values check
    non_numeric_count = None
    if value_col:
        # Coerce to numeric to detect issues like '.'
        coerced = pd.to_numeric(df[value_col], errors="coerce")
        non_numeric_count = int(coerced.isna().sum() - df[value_col].isna().sum())
        if non_numeric_count > 0:
            problems.append(f"Non-numeric entries in '{value_col}' after coercion: {non_numeric_count}")

    # --- Report ---
    rows = len(df)
    start = df[DATE_COL].min().strftime("%Y-%m-%d") if rows else "NA"
    end = df[DATE_COL].max().strftime("%Y-%m-%d") if rows else "NA"
    print(f"\n== {p} ==")
    print(f"rows={rows:,} value_col={value_col} start={start} end={end}")
    print(f"non_numeric_after_coercion={non_numeric_count if non_numeric_count is not None else 'n/a'}")

    if problems:
        print("\n[STRUCTURE FAIL]")
        for msg in problems:
            print(" -", msg)
        sys.exit(1)

    print("\n[STRUCTURE OK]")
    sys.exit(0)

def _exit(p: Path, df, start, end, value_cols, problems: list[str]) -> None:
    print(f"\n== {p} ==")
    if isinstance(df, pd.DataFrame) and DATE_COL in df:
        rows = len(df)
        s = df[DATE_COL].min().strftime("%Y-%m-%d") if rows else "NA"
        e = df[DATE_COL].max().strftime("%Y-%m-%d") if rows else "NA"
        print(f"rows={rows:,} start={s} end={e}")
    else:
        print("rows=?, start=?, end=? (failed before date parse)")

    if value_cols is not None:
        print(f"value_cols_detected={value_cols}")
    if problems:
        print("\n[STRUCTURE FAIL]")
        for msg in problems:
            print(" -", msg)
    else:
        print("\n[STRUCTURE FAIL] (unspecified)")

    sys.exit(1)

if __name__ == "__main__":
    main()
