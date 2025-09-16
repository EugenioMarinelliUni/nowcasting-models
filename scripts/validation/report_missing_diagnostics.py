# scripts/validation/report_missing_diagnostics.py
from pathlib import Path
import json, pandas as pd
from dfm_pipeline.validation.panel_missing_diagnostics import (
    compute_missingness_by_series, save_positions_jsonl,
    missing_positions_by_series, missing_runs_by_series,
)

IN_CSV = Path("data/processed_data/panel_transformed.csv")
META_DIR = Path("data/metadata")
QC_DIR   = Path("data/quality_checks")

def main():
    META_DIR.mkdir(parents=True, exist_ok=True)
    QC_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(IN_CSV, parse_dates=["sasdate"], index_col="sasdate")

    # 1) Boundary-style summary → metadata
    by_series, summary = compute_missingness_by_series(df)
    by_series_path = QC_DIR / "panel_missing_by_series.csv"
    by_series.to_csv(by_series_path, float_format="%.10g")

    summary_path = META_DIR / "panel_missing_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # 2) Exact missing dates → quality_checks
    pos_summary, positions = missing_positions_by_series(df, as_strings=True, date_format="%m/%d/%Y")
    pos_summary.to_csv(QC_DIR / "missing_positions_summary.csv", index=True)
    pos_jsonl = save_positions_jsonl(positions, QC_DIR / "missing_positions.jsonl")

    # 3) Contiguous runs → quality_checks
    runs = missing_runs_by_series(df, as_strings=True, date_format="%m/%d/%Y")
    runs_path = QC_DIR / "missing_runs.csv"
    runs.to_csv(runs_path, index=False)

    # 4) Small catalog → metadata
    catalog = {
        "input_csv": str(IN_CSV),
        "artifacts": {
            "by_series_csv": str(by_series_path),
            "summary_json": str(summary_path),
            "positions_jsonl": str(pos_jsonl),
            "runs_csv": str(runs_path),
        },
        "params": {"date_format": "%m/%d/%Y"},
    }
    with (META_DIR / "panel_missing_catalog.json").open("w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2)

    print("Diagnostics written. Summary & catalog in data/metadata; details in data/quality_checks.")

if __name__ == "__main__":
    main()
