import os
import requests
import logging
from dfm_pipeline.utils.config_loader import load_config

def download_csv(overwrite: bool = False) -> str:
    """
    Download a CSV file using URL and filename from config.yaml,
    and save it to the data/raw_data/ folder.

    If the folder does not exist, it is created.
    If the file exists and overwrite=False, the download is skipped.

    Args:
        overwrite (bool): Whether to overwrite the file if it already exists.

    Returns:
        str: Full path to the saved file.
    """
    config = load_config()
    url = config["paths"]["remote_url"]
    filename = config["paths"]["raw_filename"]

    save_dir = os.path.join("data", "raw_data")
    save_path = os.path.join(save_dir, filename)

    # Ensure the directory exists
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        logging.info(f"Created directory: {save_dir}")
    else:
        logging.info(f"Directory already exists: {save_dir}")

    # Check if file already exists
    if os.path.exists(save_path) and not overwrite:
        logging.info(f"File already exists at {save_path}. Skipping download.")
        return save_path

    # Download file
    logging.info(f"Downloading data from {url}...")
    response = requests.get(url)

    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"Download complete: {save_path}")
        return save_path
    else:
        raise Exception(f"Download failed with status code: {response.status_code}")
