# scripts/check_series_metadata.py

import logging
from scripts.bootstrap import add_src_to_path

# Add src/ to the Python path
add_src_to_path()

from dfm_pipeline.validation.check_frequency_and_seasonality import validate_series_metadata

def setup_logging():
    """Set up logging format for clarity."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

def main():
    setup_logging()
    txt_path = "series_labels.txt"  # Adjust this path if needed

    logging.info("Starting metadata validation for FRED-MD series...")
    invalid = validate_series_metadata(txt_path)

    if invalid:
        logging.warning("⚠️ The following series could not be verified automatically:")
        for series in invalid:
            logging.warning(f" - {series}")
    else:
        logging.info("✅ All series passed validation successfully!")

if __name__ == "__main__":
    main()

