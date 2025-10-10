#!/usr/bin/env python3
from pathlib import Path
import json

def write(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))

def cfg(panel, train_start, train_end, note):
    return {
        "schema_version":"1.1",
        "panel_id":panel,
        "index_freq":"MS",
        "time_spans":{"train":{"start":train_start,"end":train_end},
                      "eval":{"start":"1960-01" if "1960" in panel else "1965-01",
                              "end":"2025-03"}},
        "snapshot":{"rule":"BM"},
        "standardization":{"mode":"frozen",
          "mean_map_path":f"data/metadata/scalers/mean_map_{panel}_train{train_start[:4]}_{train_end[:4]}.json",
          "std_map_path": f"data/metadata/scalers/std_map_{panel}_train{train_start[:4]}_{train_end[:4]}.json"},
        "masking":{"mode":"constant_lag",
          "lag_map_path":f"data/metadata/series_maps/lag_map_{panel}.json"},
        "series_maps":{
          "group_map_path":f"data/metadata/series_maps/variable_group_map_{panel}.json",
          "tcode_map_path":"data/metadata/series_maps/tcode_map.json"
        },
        "covid_handling":{"variant":"baseline",
          "window":["2020-03","2020-09"],"winsor_clip_sigma":6.0,
          "dummies_path":None,"weights_path":None},
        "target":{"id":"A191RL1Q225SBEA",
          "aggregation":"quarterly_to_monthly_last_month","lag_quarters":1},
        "paths":{"dataset_root":f"dataset/{panel}",
          "panel_file":"baseline/X_panel_z.csv","target_file":"baseline/y_target_z.csv"},
        "evaluation":{"metrics":["RMSE","MAE"],"horizons":["backcast","nowcast","forecast"]},
        "notes":note
    }

root = Path("data/metadata/backtest")
configs = [
    ("1960_noVIX","1960-01","2015-12","BM; frozen scalers 1960–2015; no VIX."),
    ("1960_noVIX","1990-01","2025-03","BM; frozen scalers 1990–2025; no VIX."),
    ("1965_withVIX","1965-01","2015-12","BM; frozen scalers 1965–2015; VIX."),
    ("1965_withVIX","1990-01","2025-03","BM; frozen scalers 1990–2025; VIX.")
]
for panel, ts, te, note in configs:
    rel = f"{panel}/backtest_config_train{ts[:4]}_{te[:4]}.json"
    write(root/rel, cfg(panel, ts, te, note))
print("Backtest configs written.")
