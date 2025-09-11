from pathlib import Path
from dfm_pipeline.ingestion.fred_md import extract_tcodes_to_json

if __name__ == "__main__":
    csv_path = Path("data/raw_data/fred_md_current.csv")
    json_path = Path("data/metadata/tcode_map.json")

    extract_tcodes_to_json(
        csv_path=csv_path,
        out_json_path=json_path,
        date_col="sasdate",
        overwrite=True,            # set False if you want to protect an existing JSON
        autodetect_tcode_row=True,
        require_all_tcodes=True,
        write_sidecar_metadata=True,
    )
    print(f"tcode JSON written to {json_path}")
