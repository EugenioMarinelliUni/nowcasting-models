# scripts/data/run_preprocessing.py

from dfm_pipeline.utils.preprocess_fred_md_pipeline import transform_from_csv_and_json

# Define paths
csv_path = "data/raw_data/fred_md_current.csv"
tcode_json_path = "data/metadata/tcode_map.json"
save_path = "data/processed_data/fred_md_transformed.csv"

# Run transformation
transform_from_csv_and_json(
    csv_path=csv_path,
    tcode_json_path=tcode_json_path,
    save_path=save_path,
    standardize_data=True
)
