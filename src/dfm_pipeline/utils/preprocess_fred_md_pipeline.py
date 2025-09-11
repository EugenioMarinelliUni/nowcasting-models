import os
import json
import pandas as pd

from dfm_pipeline.utils.series_transformations import (
    apply_tcode_transformations, standardize
)

def transform_from_csv_and_json(
    csv_path: str,
    tcode_json_path: str,
    save_path: str | None = None,
    standardize_data: bool = True
) -> pd.DataFrame:
    """
    Load raw dataset and tcode map from files, apply transformations, and optionally standardize.

    Parameters:
    - csv_path (str): Path to the raw CSV file (with a redundant second row to skip)
    - tcode_json_path (str): Path to the JSON file containing the tcode mapping
    - save_path (str, optional): Where to save the resulting DataFrame (as CSV). If None, don't save.
    - standardize_data (bool): Whether to standardize the transformed data

    Returns:
    - df_final (pd.DataFrame): Transformed (and optionally standardized) DataFrame
    """

    # Load raw dataset (skip second row with transformation codes)
    df = pd.read_csv(csv_path, skiprows=[1], parse_dates=["sasdate"], index_col="sasdate")

    # Load transformation mapping
    with open(tcode_json_path, "r") as f:
        tcode_map = json.load(f)

    # Apply transformations
    df_transformed = apply_tcode_transformations(df, tcode_map)

    # Optionally standardize
    df_final = standardize(df_transformed) if standardize_data else df_transformed

    # Save if path provided
    if save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df_final.to_csv(save_path)
        print(f"✅ Transformed dataset saved to {save_path}")

    return df_final

