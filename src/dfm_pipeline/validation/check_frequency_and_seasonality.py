from bs4 import BeautifulSoup
import os
import requests
import logging
from typing import List, Tuple
from time import sleep

HEADERS = {"User-Agent": "Mozilla/5.0"}
FRED_SERIES_BASE_URL = "https://fred.stlouisfed.org/series/"

def read_series_labels(file_path: str) -> List[str]:
    with open(file_path, "r") as f:
        labels = [line.strip() for line in f if line.strip()]
    return labels

def fetch_fred_metadata(label: str) -> Tuple[str, str]:
    url = FRED_SERIES_BASE_URL + label
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Could not fetch {label}: {e}")

    soup = BeautifulSoup(response.text, "html.parser")

    # Find all metadata spans (this is a fallback for a more robust capture of values)
    meta_values = soup.select("p.mb-2 span.series-meta-value")

    freq = seas = None
    for span in meta_values:
        text = span.get_text(strip=True)
        if text in ["Monthly", "Quarterly", "Annual"]:
            freq = text
        elif "Seasonally Adjusted" in text or "Not Seasonally Adjusted" in text:
            seas = text

    if not freq or not seas:
        raise Exception("Incomplete metadata")

    return freq, seas

def check_series_metadata(labels: List[str], delay: float = 1.0) -> List[str]:
    failed = []
    results = []

    for label in labels:
        try:
            freq, seas = fetch_fred_metadata(label)
            results.append((label, freq, seas))
            logging.info(f"{label}: Frequency = {freq}, Seasonality = {seas}")
        except Exception as e:
            logging.warning(f"Failed to fetch metadata for {label}: {e}")
            failed.append(label)
        sleep(delay)

    os.makedirs("data/metadata", exist_ok=True)
    with open("data/metadata/series_metadata_report.txt", "w") as f:
        for label, freq, seas in results:
            f.write(f"{label}: Frequency = {freq}, Seasonality = {seas}\n")

    if failed:
        with open("data/metadata/series_failed_to_check.txt", "w") as f:
            for label in failed:
                f.write(f"{label}\n")
        logging.warning(f"⚠️ Could not verify {len(failed)} series. See data/metadata/series_failed_to_check.txt")

    logging.info("✅ Series metadata verification completed.")
    return failed

def validate_series_metadata(file_path: str) -> List[str]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Series label file not found: {file_path}")
    labels = read_series_labels(file_path)
    return check_series_metadata(labels)



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    label_path = "series_labels.txt"
    try:
        invalid = validate_series_metadata(label_path)
    except FileNotFoundError as e:
        logging.error(e)
