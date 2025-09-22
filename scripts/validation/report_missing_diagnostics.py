#!/usr/bin/env python3
from pathlib import Path
import argparse, json
import pandas as pd

from dfm_pipeline.validation.panel_missing_diagnostics import (
    compute_missingness_by_series,
    save_positions_jsonl,
    missing_positions_by_series,
    missing_runs_by_series,
)

DATE_FMT = "%m/%d/%Y"  # mm/dd/yyyy
DEFAULT_META_DIR = Path("data/metadata")
DEFAULT_QC_DIR   = Path("data/quality_checks")


def load_panel(p: Path) -> pd.DataFrame:
    """Load panel with DatetimeIndex on 'sasdate' from either CSV style."""
    # Try index-labeled style first
    try:
        return pd.read_csv(p, parse_dates=["sasdate"], index_col="sasdate").sort_index()
    except Exception:
        df = pd.read_csv(p)
        if "sasdate" not in df.columns:
            raise SystemExit(f"{p}: 'sasdate' not found as column or index.")
        df["sasdate"] = pd.to_datetime(df["sasdate"], format=DATE_FMT, errors="coerce")
        return df.set_index("sasdate").sort_index()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Run missing-data diagnostics and write QC + metadata artifacts."
    )
    ap.add_argument(
        "csvs",
        nargs="+",
        type=Path,
        help="Path(s) to panel CSV file(s)."
    )
    ap.add_argument(
        "--meta-dir",
        type=Path,
        default=DEFAULT_META_DIR,
        help=f"Directory for metadata JSONs (default: {DEFAULT_META_DIR})"
    )
    ap.add_argument(
        "--qc-dir",
        type=Path,
        default=DEFAULT_QC_DIR,
        help=f"Directory for QC CSVs/JSONL (default: {DEFAULT_QC_DIR})"
    )
    return ap.parse_args()


def run_one(in_csv: Path, meta_dir: Path, qc_dir: Path) -> None:
    df = load_panel(in_csv)
    stem = in_csv.stem.replace("_sasdate_column", "")  # normalize if needed

    meta_dir.mkdir(parents=True, exist_ok=True)
    qc_dir.mkdir(parents=True, exist_ok=True)

    # 1) Boundary-style summary
    by_series, summary = compute_missingness_by_series(df, date_format=DATE_FMT)
    by_series_path = qc_dir / f"{stem}_missing_by_series.csv"
    by_series.to_csv(by_series_path, float_format="%.10g")

    summary_path = meta_dir / f"{stem}_missing_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # 2) Exact missing dates
    pos_summary, positions = missing_positions_by_series(df, as_strings=True, date_format=DATE_FMT)
    pos_summary_path = qc_dir / f"{stem}_missing_positions_summary.csv"
    pos_summary.to_csv(pos_summary_path, index=True)
    pos_jsonl_path = save_positions_jsonl(positions, qc_dir / f"{stem}_missing_positions.jsonl")

    # 3) Contiguous runs
    runs = missing_runs_by_series(df, as_strings=True, date_format=DATE_FMT)
    runs_path = qc_dir / f"{stem}_missing_runs.csv"
    runs.to_csv(runs_path, index=False)

    # 4) Small catalog
    catalog = {
        "input_csv": str(in_csv),
        "shape": {"rows": int(df.shape[0]), "cols": int(df.shape[1])},
        "params": {"date_format": DATE_FMT},
        "boundaries": summary.get("boundaries", {}),
        "counts": summary.get("counts", {}),
        "artifacts": {
            "by_series_csv": str(by_series_path),
            "summary_json": str(summary_path),
            "positions_summary_csv": str(pos_summary_path),
            "positions_jsonl": str(pos_jsonl_path),
            "runs_csv": str(runs_path),
        },
    }
    catalog_path = meta_dir / f"{stem}_panel_missing_catalog.json"
    with catalog_path.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2)

    print(f"[{stem}] rows={df.shape[0]} cols={df.shape[1]} -> QC:{qc_dir}  META:{meta_dir}")


def main() -> None:
    args = parse_args()
    for p in args.csvs:
        if not p.exists():
            raise SystemExit(f"Input not found: {p}")
        run_one(p, args.meta_dir, args.qc_dir)


if __name__ == "__main__":
    main()

