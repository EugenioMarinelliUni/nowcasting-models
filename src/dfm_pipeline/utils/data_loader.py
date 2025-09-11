# src/dfm_pipeline/utils/data_loader.py

import os
import pandas as pd
from dfm_pipeline.utils.config_loader import load_config


def load_data(stage: str = "raw") -> tuple[pd.DataFrame, pd.Series]:
    """
    Load dataset and transformation codes for a given stage.

    Returns:
        Tuple of:
        - df: time-indexed DataFrame of macroeconomic series
        - transform_codes: Series containing transformation codes per column
    """
    config = load_config()

    if stage == "raw":
        path = os.path.join("data", "raw_data", config["paths"]["raw_filename"])
    elif stage == "processed":
        path = os.path.join("data", "processed_data", config["paths"]["processed_filename"])
    else:
        raise ValueError("stage must be 'raw' or 'processed'")

    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found at: {path}")

    # Load transformation codes (row 2, after header row)
    transform_codes_df = pd.read_csv(path, nrows=1, skiprows=1)
    transform_codes = transform_codes_df.squeeze()  # Turn DataFrame into Series

    # Load actual data (starting from row 3), parse sasdate as datetime index
    df = pd.read_csv(
        path,
        skiprows=[1],  # skip the row with transformation codes
        parse_dates=["sasdate"],
        index_col="sasdate"
    )

    return df, transform_codes




