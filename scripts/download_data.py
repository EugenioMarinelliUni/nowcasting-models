# scripts/download_data.py

import logging
from bootstrap import add_src_to_path

# Ensure src/ is in the Python path
add_src_to_path()

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

    filename = config["paths"]["raw_filename"]
    raw_path = os.path.join("data", "raw_data", filename)

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
    import os  # Needed for raw_path logic
    main()
