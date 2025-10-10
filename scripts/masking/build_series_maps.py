#!/usr/bin/env python3
# No emojis in output (per user request)
from pathlib import Path
import json, yaml, pandas as pd

# ---------------------------------------------------------------------
# Configurable paths
# ---------------------------------------------------------------------
SRC_1960 = Path("data/metadata/variable_group_map_1960_noVIX.json")
SRC_1965 = Path("data/metadata/variable_group_map_1965_withVIX.json")

OUT_DIR   = Path("data/metadata/series_maps")
OUT_G1960 = OUT_DIR / "variable_group_map_1960_noVIX.json"
OUT_L1960 = OUT_DIR / "lag_map_1960_noVIX.json"
OUT_G1965 = OUT_DIR / "variable_group_map_1965_withVIX.json"
OUT_L1965 = OUT_DIR / "lag_map_1965_withVIX.json"

# If you want to restrict maps to series that actually appear in your panels,
# set this to True (recommended).
FILTER_TO_PANEL_COLUMNS = True

# ---------------------------------------------------------------------
# Default publication lags by category (BM snapshot convention)
#   0 = available within-month (markets, FX, some surveys)
#   1 = available with ~1-month lag (CPI/PPI, payrolls, IP, etc.)
#   2 = available with ~2-month lag (housing starts/permits in many BM setups)
# Tweak as needed.
# ---------------------------------------------------------------------
DEFAULT_LAGS = {
    "Stock Market": 0,
    "Interest & Exchange Rates": 0,
    "Exchange Rates": 0,              # your uploaded maps place FX under Interest & Exch. Rates
    "Expectations": 0,

    "Prices": 1,
    "Labor Market": 1,
    "Orders & Inventories": 1,
    "Output & Income": 1,
    "Consumption": 1,                 # your uploaded maps use "Consumption" (not C&I)
    "Money & Credit": 1,

    "Housing": 2,

    # Fallback
    "Other": 1
}

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def load_yaml(path="config.yaml"):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def read_panel_columns(csv_path, date_col):
    df = pd.read_csv(csv_path, nrows=5)
    return [c for c in df.columns if c != date_col]

def filter_map_to_series(group_map: dict, series_list: list) -> dict:
    allowed = set(series_list)
    return {k: v for k, v in group_map.items() if k in allowed}

def build_lag_map(group_map: dict) -> dict:
    return {s: int(DEFAULT_LAGS.get(cat, DEFAULT_LAGS["Other"])) for s, cat in group_map.items()}

def write_json(path: Path, obj: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    if not SRC_1960.exists() or not SRC_1965.exists():
        raise FileNotFoundError(
            "Source group maps not found. Place them at:\n"
            f"  - {SRC_1960}\n  - {SRC_1965}\n"
            "These should be the series→category JSONs you provided."
        )

    gmap_1960 = json.loads(SRC_1960.read_text(encoding="utf-8"))
    gmap_1965 = json.loads(SRC_1965.read_text(encoding="utf-8"))

    # Optionally restrict to actual panel columns
    if FILTER_TO_PANEL_COLUMNS:
        yml = load_yaml()
        date_col = yml["dates"]["date_col"]
        p60 = yml["paths"]["panel_1960_fixhousing_delta"]
        p65 = yml["paths"]["panel_1965_fixhousing_delta"]

        cols_60 = read_panel_columns(p60, date_col)
        cols_65 = read_panel_columns(p65, date_col)

        gmap_1960 = filter_map_to_series(gmap_1960, cols_60)
        gmap_1965 = filter_map_to_series(gmap_1965, cols_65)

    # Build lag maps from category defaults
    lmap_1960 = build_lag_map(gmap_1960)
    lmap_1965 = build_lag_map(gmap_1965)

    # Write outputs
    write_json(OUT_G1960, gmap_1960)
    write_json(OUT_L1960, lmap_1960)
    write_json(OUT_G1965, gmap_1965)
    write_json(OUT_L1965, lmap_1965)

    print("Wrote:")
    print(f"  - {OUT_G1960}")
    print(f"  - {OUT_L1960}")
    print(f"  - {OUT_G1965}")
    print(f"  - {OUT_L1965}")

if __name__ == "__main__":
    main()
