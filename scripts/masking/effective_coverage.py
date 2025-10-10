#!/usr/bin/env python3
from pathlib import Path
import pandas as pd, json
from scripts.utils.io_paths import load_yaml, load_backtest_config, list_backtest_configs_from_yaml, panel_z_path_from_cfg, monthly_is_ms

def load_lag_map(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))

def latest_visible_period(snapshot_bm: pd.Timestamp, L: int) -> pd.Period:
    return snapshot_bm.to_period("M") - L

def ms_timestamp(period_M: pd.Period) -> pd.Timestamp:
    return period_M.to_timestamp("MS")

def main():
    yml = load_yaml()
    ms = monthly_is_ms(yml)
    cfg_paths = list_backtest_configs_from_yaml(yml)

    for cfgp in cfg_paths:
        cfg = load_backtest_config(str(cfgp))
        panel = cfg["panel_id"]
        X_path = panel_z_path_from_cfg(cfg, variant="baseline")
        lag_map_path = Path(f"data/metadata/series_maps/lag_map_{panel}.json")
        lag = load_lag_map(lag_map_path)

        X = pd.read_csv(X_path, parse_dates=["Date"]).set_index("Date").sort_index()
        X.index = X.index.to_period("M").to_timestamp("MS" if ms else "M")

        snaps = pd.date_range(X.index.min(), X.index.max(), freq="BM")
        cov = []
        for s in snaps:
            count = 0
            for col, L in lag.items():
                cutoff = (ms_timestamp(latest_visible_period(s, int(L))) if ms
                          else latest_visible_period(s, int(L)).to_timestamp("M"))
                if X.loc[:cutoff, col].notna().any():
                    count += 1
            cov.append((s, count))

        df = pd.DataFrame(cov, columns=["snapshot_bm","n_series"])
        out = Path(f"output/{panel}/coverage_bm.csv")
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        print(f"{panel}: wrote {out}")

if __name__ == "__main__":
    main()
