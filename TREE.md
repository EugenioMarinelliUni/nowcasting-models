## Project structure

```
.
|-- data
|   `-- metadata
|-- scripts
|   |-- data
|   |-- ingestion
|   |-- bootstrap.py
|   |-- check_series_metadata.py
|   |-- download_data.py
|   |-- eda.py
|   |-- export_series_labels.py
|   |-- generate_group_map.py
|   |-- generate_tcode_map.py
|   |-- group_analysis.py
|   |-- inspect_raw_csv.py
|   |-- run_deseasonalization.py
|   |-- run_pipeline.py
|   `-- run_preprocessing.py
|-- src
|   `-- dfm_pipeline
|-- .gitignore
|-- README.md
|-- config.yaml
|-- pyproject.toml
|-- requirements.txt
`-- series_labels.txt

7 directories, 18 files
```

### src (detailed)

```
src
`-- dfm_pipeline
    |-- eda
    |   |-- __init__.py
    |   `-- exploration.py
    |-- ingestion
    |   |-- __init__.py
    |   |-- downloader.py
    |   `-- fred_md.py
    |-- modelling
    |   `-- __init__.py
    |-- preprocessing
    |   |-- __init__.py
    |   |-- deseasonalization.py
    |   |-- factors.py
    |   `-- tcode.py
    |-- utils
    |   |-- __init__.py
    |   |-- config_loader.py
    |   |-- data_loader.py
    |   |-- grouping_series.py
    |   |-- hashing.py
    |   |-- metadata_tools.py
    |   |-- preprocess_fred_md_pipeline.py
    |   |-- raw_data_inspector.py
    |   `-- series_transformations.py
    |-- validation
    |   |-- __init__.py
    |   |-- check_frequency_and_seasonality.py
    |   `-- tcode_map.py
    `-- __init__.py

7 directories, 23 files
```
