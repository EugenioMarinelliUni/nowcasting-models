# scripts/explore_data.py

from bootstrap import add_src_to_path
add_src_to_path()

from dfm_pipeline.eda.exploration import main

if __name__ == "__main__":
    main()
