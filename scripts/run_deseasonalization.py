# scripts/run_deseasonalization.py

import logging
import pandas as pd
from scripts.bootstrap import add_src_to_path

# Add src/ to Python path
add_src_to_path()

from dfm_pipeline.preprocessing.deseasonalization import deseasonalize_x13, deseasonalize_stl

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    setup_logging()

    file_path = "data/raw/fred_md_current.csv"  # Adjust if needed
    label = "INDPRO"  # Choose the column/series label to test

    logging.info(f"📥 Loading dataset from {file_path}")
    df = pd.read_csv(file_path, index_col=0, parse_dates=True)

    # STL Deseasonalization
    logging.info(f"🔄 Performing STL deseasonalization for {label}")
    stl_result = deseasonalize_stl(df, label)
    print(f"\nSTL Deseasonalized Series (head):\n{stl_result.head()}")

    # X13 Deseasonalization
    logging.info(f"🔄 Performing X-13 deseasonalization for {label}")
    try:
        x13_result = deseasonalize_x13(df, label)
        print(f"\nX13 Deseasonalized Series (head):\n{x13_result.head()}")
    except Exception as e:
        logging.warning(f"X13 Deseasonalization failed: {e}")

if __name__ == "__main__":
    main()
