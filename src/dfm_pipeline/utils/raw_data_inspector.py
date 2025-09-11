# src/dfm_pipeline/utils/raw_inspector.py

import pandas as pd

def inspect_raw_csv(filepath: str = "data/raw_data/fred_md_current.csv"):
    """
    Load and inspect the raw CSV file without parsing or indexing.

    Args:
        filepath (str): Path to the raw CSV file.

    Returns:
        tuple: Number of columns, list of column names
    """
    df = pd.read_csv(filepath)
    num_columns = len(df.columns)
    column_names = df.columns.tolist()

    return num_columns, column_names
