# scripts/ingestion/build_transformed_panel.py
from pathlib import Path
import os
import json
import pandas as pd

from dfm_pipeline.ingestion.fred_md import detect_tcode_row
from dfm_pipeline.preprocessing.tcode import apply_tcode_transformations


def main() -> None:
    # ---- Inputs ----
    csv_path = Path("data/raw_data/fred_md_current.csv")
    json_path = Path("data/metadata/tcode_map.json")

    # ---- Read raw CSV, skip embedded t-code row, keep dates as-is ----
    tcode_row = detect_tcode_row(csv_path, date_col="sasdate") or 1
    df = pd.read_csv(
        csv_path,
        skiprows=[tcode_row],          # skip the row with t-codes
        parse_dates=["sasdate"],
        index_col="sasdate",
    ).sort_index()
    df.index.name = "sasdate"

    # ---- Load t-code map ----
    with open(json_path, "r", encoding="utf-8") as f:
        tcode_map = {k: int(v) for k, v in json.load(f).items()}

    # Sanity check: every column needs a t-code
    missing = [c for c in df.columns if c not in tcode_map]
    if missing:
        raise KeyError(f"No tcode provided for columns: {missing}")

    # ---- Transform (unbalanced/ragged panel) ----
    X = apply_tcode_transformations(df, tcode_map)

    # ---- Outputs (env-configurable) ----
    out_dir = Path(os.getenv("DFM_OUTDIR", "data/processed_data"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # Dry-run toggle to avoid writing files during tracing
    DRYRUN = os.getenv("DFM_DRYRUN") == "1"

    if DRYRUN:
        print("[DRY-RUN] Skipping writes.")
    else:
        # Try Parquet first (optional engine)
        try:
            X.to_parquet(out_dir / "panel_transformed.parquet")
        except Exception as e:
            print(f"[WARN] Parquet write skipped ({e}).")

        # Always write CSV (dates rendered as mm/dd/yyyy)
        X.to_csv(
            out_dir / "panel_transformed.csv",
            index=True,
            index_label="sasdate",
            date_format="%m/%d/%Y",
            float_format="%.10g",
            na_rep="",  # show empty cell for NaN; change to "NA" if preferred
        )
        print(f"Saved to: {out_dir}/panel_transformed.parquet (if engine available) and .csv")

    # Small summary
    print(f"Transformed shape: {X.shape}; first date: {X.index.min().date()} | last date: {X.index.max().date()}")


if __name__ == "__main__":
    main()

