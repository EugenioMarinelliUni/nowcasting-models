# src/dfm_pipeline/utils/metadata_tools.py

def extract_series_labels(df, output_path: str = "series_labels.txt") -> None:
    """
    Write a list of column names (series labels) from a DataFrame to a text file.

    Args:
        df (pd.DataFrame): DataFrame containing FRED-MD series.
        output_path (str): Path to the output .txt file.
    """
    series_labels = df.columns.tolist()

    with open(output_path, "w") as f:
        for label in series_labels:
            f.write(label + "\n")

    print(f"Exported {len(series_labels)} series labels to {output_path}")
