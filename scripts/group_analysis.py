import sys
import os
import logging
import argparse
import pandas as pd

# Add src/ to Python path
current_dir = os.path.dirname(__file__)
src_path = os.path.abspath(os.path.join(current_dir, "..", "src"))
sys.path.append(src_path)

from dfm_pipeline.utils.data_loader import load_data
from dfm_pipeline.utils.grouping_series import (
    assign_variable_groups,
    filter_by_group,
    create_metadata_df
)

def setup_logging():
    """Set up console logging format."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

def print_section_header(title: str):
    """Print a formatted header for clarity in console output."""
    print(f"\n{'=' * 60}\n{title.upper()}\n{'=' * 60}")

def parse_args():
    """Parse command-line arguments to choose the group dynamically."""
    parser = argparse.ArgumentParser(description="Analyze macroeconomic variable groups.")
    parser.add_argument(
        "--group", type=str, default="Labor Market",
        help="Name of the group to analyze (e.g., 'Prices', 'Labor Market')"
    )
    return parser.parse_args()

def main():
    setup_logging()
    args = parse_args()
    selected_group = args.group

    # Load raw dataset
    df, _ = load_data(stage="raw")
    logging.info(f"✅ Data loaded successfully: shape = {df.shape}")

    # Assign variable groups
    group_series = assign_variable_groups(df)

    print_section_header("Variable Group Distribution")
    print(group_series.value_counts().to_string())

    # Create and save metadata DataFrame
    metadata = create_metadata_df(df)

    print_section_header("Metadata Preview")
    print(metadata.head().to_string(index=False))

    os.makedirs("data/metadata", exist_ok=True)
    metadata_path = "data/metadata/variable_groups.csv"
    metadata.to_csv(metadata_path, index=False)
    logging.info(f"📄 Metadata saved to: {metadata_path}")

    # Filter and show selected group
    group_df = filter_by_group(df, selected_group)

    print_section_header(f"Subset Preview - {selected_group}")
    print(f"Shape: {group_df.shape}")
    print(group_df.iloc[:, :5].head())  # Display first 5 columns for brevity

if __name__ == "__main__":
    main()


