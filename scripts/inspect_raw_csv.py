# scripts/inspect_raw_csv.py

from bootstrap import add_src_to_path
add_src_to_path()

from dfm_pipeline.utils.raw_data_inspector import inspect_raw_csv

def main():
    num_columns, column_names = inspect_raw_csv()

    print(f"🔢 Number of columns: {num_columns}")
    print("📋 Column names:")
    for col in column_names:
        print(f" - {col}")

if __name__ == "__main__":
    main()
