from bootstrap import add_src_to_path
add_src_to_path()

from dfm_pipeline.utils.data_loader import load_data
from dfm_pipeline.utils.metadata_tools import extract_series_labels

def main():
    df, _ = load_data(stage="raw")
    extract_series_labels(df)

if __name__ == "__main__":
    main()

