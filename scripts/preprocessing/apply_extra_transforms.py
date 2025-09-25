#!/usr/bin/env python3
from pathlib import Path
import argparse
import sys
import yaml
import pandas as pd


def apply_transform(s: pd.Series, kind: str) -> pd.Series:
    """
    Apply a simple extra transform to a single series.

    Supported:
      - "diff": first difference (x_t - x_{t-1})
      - "yoy" : 12-month difference (x_t - x_{t-12}), preserves units
    """
    kind = (kind or "").strip().lower()
    x = pd.to_numeric(s, errors="coerce")
    if kind == "diff":
        return x.diff()
    elif kind == "yoy":
        return x - x.shift(12)
    else:
        raise ValueError(f"Unsupported transform '{kind}'. Supported: diff, yoy")


def load_config(path: Path) -> dict:
    """Load YAML config with UTF-8; fall back gracefully if needed."""
    if not path.exists():
        sys.exit(f"Config not found: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # Try UTF-8 with BOM, then cp1252 as a last resort
        try:
            text = path.read_text(encoding="utf-8-sig")
            print("[warn] Read config with UTF-8 BOM; consider re-saving as plain UTF-8.", file=sys.stderr)
        except UnicodeDecodeError:
            text = path.read_text(encoding="cp1252")
            print("[warn] Read config with cp1252; consider re-saving as UTF-8 to avoid surprises.", file=sys.stderr)
    try:
        return yaml.safe_load(text) or {}
    except Exception as e:
        sys.exit(f"Failed to parse YAML in {path}: {e}")


def main():
    ap = argparse.ArgumentParser(
        description="Apply per-series extra transforms (e.g., diff) from config.yaml."
    )
    ap.add_argument("--config", type=Path, default=Path("config.yaml"))
    ap.add_argument("--in-csv", type=Path,
                    help="Input panel CSV. If omitted, uses paths.panel_1965 from config.")
    ap.add_argument("--out-csv", type=Path,
                    help="Output CSV. If omitted, uses paths.panel_1965_fixhousing_delta from config.")
    ap.add_argument("--date-col", default=None,
                    help="Override date column name (defaults to dates.date_col in config).")
    ap.add_argument("--date-fmt", default=None,
                    help="Override CSV date format (defaults to dates.csv_format in config).")
    ap.add_argument("--drop-leading-na", dest="drop_leading_na", action="store_true",
                    help="Drop the first row if transforms introduce NaN(s) at the first timestamp.")
    args = ap.parse_args()

    cfg = load_config(args.config)

    # Resolve paths & date settings (with clear errors if keys missing)
    try:
        date_col = args.date_col or cfg["dates"]["date_col"]
        date_fmt = args.date_fmt or cfg["dates"]["csv_format"]
        in_csv   = args.in_csv or Path(cfg["paths"]["panel_1965"])
        out_csv  = args.out_csv or Path(cfg["paths"]["panel_1965_fixhousing_delta"])
    except KeyError as ke:
        sys.exit(f"Missing key in config.yaml: {ke}. Please define dates.date_col, dates.csv_format, "
                 f"paths.panel_1965, and paths.panel_1965_fixhousing_delta (or pass --in-csv/--out-csv).")

    extra = (cfg.get("preprocessing", {}) or {}).get("extra_transforms", {}) or {}
    if not extra:
        sys.exit("No preprocessing.extra_transforms found in config.yaml (nothing to do).")

    # Load panel
    try:
        df = pd.read_csv(in_csv, parse_dates=[date_col], index_col=date_col)
    except Exception as e:
        sys.exit(f"Failed to read input CSV {in_csv}: {e}")

    if df.empty:
        sys.exit(f"Input panel is empty: {in_csv}")

    changed = []

    # Apply transforms
    for col, kind in extra.items():
        if col in df.columns:
            try:
                df[col] = apply_transform(df[col], kind)
                changed.append(f"{col}:{kind}")
            except Exception as e:
                print(f"[warn] Failed to transform {col} ({kind}): {e}", file=sys.stderr)
        else:
            print(f"[warn] Column '{col}' not found in panel; skipping.", file=sys.stderr)

    # Optionally drop the first row if any transformed column became NaN at the first timestamp
    if args.drop_leading_na and not df.empty:
        first_ts = df.index.min()
        if any((c in df.columns) and pd.isna(df.loc[first_ts, c]) for c in extra.keys()):
            df = df.iloc[1:]

    # Write output
    try:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, date_format=date_fmt, float_format="%.10g")
    except Exception as e:
        sys.exit(f"Failed to write output CSV {out_csv}: {e}")

    print("Applied transforms:", ", ".join(changed) if changed else "(none)")
    print("IN :", in_csv)
    print("OUT:", out_csv)


if __name__ == "__main__":
    main()

