import sys
import os
import logging

# Add src/ to the Python module search path
current_dir = os.path.dirname(__file__)
src_path = os.path.abspath(os.path.join(current_dir, "..", "src"))
sys.path.append(src_path)

from dfm_pipeline.utils.config_loader import load_config
from dfm_pipeline.ingestion.downloader import download_csv

def setup_logging():
    """Configure logging to console."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def main():
    setup_logging()
    config = load_config()

    # Extract expected filename and build full path
    filename = config["paths"]["raw_filename"]
    raw_path = os.path.join("data", "raw_data", filename)

    # Step 1: Check if file exists
    if os.path.exists(raw_path):
        logging.info(f"Raw file already exists at {raw_path}. Skipping download.")
    else:
        logging.info("Raw file not found. Starting download...")
        try:
            download_csv(overwrite=False)
            logging.info("Download completed successfully.")
        except Exception as e:
            logging.error(f"Download failed: {e}")

if __name__ == "__main__":
    main()
