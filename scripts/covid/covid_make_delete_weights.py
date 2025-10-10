#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import json, yaml
import pandas as pd

PROCESS_ALL_CONFIGS = True
WRITE_OPTIONAL_NAN_PANEL = False  # set True only if you also want a NaN’d panel

def load_yaml(path: str | Path = "config.yaml") -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def list_cfgs(yml: dict) -> list[Path]:
    cfgs = yml.get("backtest", {}).get("configs", {})
    return [Path(p) for p in cfgs.values()]

def selected_cfg(yml: dict) -> Path:
    sel = yml["backtest"]["selected_config"]
    return Path(yml["backtest"]["configs"][sel])

def train_tag(cfg: dict) -> str:
    tr = cfg["time_spans"]["train"]
    return f"train{tr['start'][:4]}_{tr['end'][:4]}"

def baseline_path(cfg: dict) -> Path:
    panel = cfg["panel_id"]; tag = train_tag(cfg)
    p = Path(f"dataset/{panel}/baseline/X_panel_z__{panel}__{tag}.csv")
    if not p.exists(): raise FileNotFoundError(p)
    return p

def ts_month(x: str, ms: bool) -> pd.Timestamp:
    p = pd.Period(x, "M")
    return p.to_timestamp(how="start" if ms else "end")

def main() -> None:
    yml = load_yaml()
    ms = str(yml["dates"]["monthly_freq"]).upper() == "MS"
    start = yml["covid"]["window_start"]
    end   = yml["covid"]["window_end"]

    cfg_paths = list_cfgs(yml) if PROCESS_ALL_CONFIGS else [selected_cfg(yml)]

    s = ts_month(start, ms)
    e = ts_month(end,   ms)

    for cfgp in cfg_paths:
        cfg = json.loads(Path(cfgp).read_text(encoding="utf-8"))
        panel = cfg["panel_id"]; tag = train_tag(cfg)

        X = pd.read_csv(baseline_path(cfg), parse_dates=["Date"]).set_index("Date").sort_index()
        per = pd.PeriodIndex(pd.DatetimeIndex(X.index), freq="M")
        idx = pd.DatetimeIndex(per.start_time if ms else per.end_time)

        w = pd.Series(1, index=idx, name="w", dtype=int)
        w.loc[(idx >= s) & (idx <= e)] = 0

        outdir = Path(f"dataset/{panel}/covid_delete"); outdir.mkdir(parents=True, exist_ok=True)
        outW = outdir / f"W_estimation_weights__{panel}__{tag}.csv"
        w.to_frame().to_csv(outW, index_label="Date")
        print(f"{panel} {tag}: wrote {outW}")

        if WRITE_OPTIONAL_NAN_PANEL:
            X_nan = X.copy()
            mask = (idx >= s) & (idx <= e)
            X_nan.loc[mask, :] = float("nan")
            outX = outdir / f"X_panel_z__{panel}__{tag}__covid_delete_nan.csv"
            X_nan.to_csv(outX, index_label="Date")
            print(f"{panel} {tag}: wrote {outX}")

if __name__ == "__main__":
    main()
