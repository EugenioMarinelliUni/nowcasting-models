#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import json, yaml
import numpy as np
import pandas as pd

PROCESS_ALL_CONFIGS: bool = True

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
    if not p.exists():
        raise FileNotFoundError(p)
    return p

def coerce_monthly(idx: pd.Index, ms: bool) -> pd.DatetimeIndex:
    dt = pd.DatetimeIndex(idx)
    per = pd.PeriodIndex(dt, freq="M")
    return pd.DatetimeIndex(per.start_time if ms else per.end_time)

def ts_month(x: str, ms: bool) -> pd.Timestamp:
    p = pd.Period(x, "M")
    return p.to_timestamp(how="start" if ms else "end")

def in_window(index: pd.DatetimeIndex, start: str, end: str, ms: bool) -> pd.Series:
    s = ts_month(start, ms)
    e = ts_month(end,   ms)
    m = (index >= s) & (index <= e)
    return pd.Series(m, index=index, name="covid_window")

def main() -> None:
    yml = load_yaml()
    ms = str(yml["dates"]["monthly_freq"]).upper() == "MS"
    start = yml["covid"]["window_start"]
    end   = yml["covid"]["window_end"]
    clip  = float(yml["covid"].get("winsor_clip_sigma", 6.0))

    cfg_paths = list_cfgs(yml) if PROCESS_ALL_CONFIGS else [selected_cfg(yml)]

    for cfgp in cfg_paths:
        cfg = json.loads(Path(cfgp).read_text(encoding="utf-8"))
        panel = cfg["panel_id"]; tag = train_tag(cfg)

        X = pd.read_csv(baseline_path(cfg), parse_dates=["Date"]).set_index("Date").sort_index()
        X.index = coerce_monthly(X.index, ms=ms)

        w = in_window(X.index, start, end, ms=ms)
        Xw = X.copy()
        Xw.loc[w.values, :] = np.clip(Xw.loc[w.values, :], -clip, +clip)

        outdir = Path(f"dataset/{panel}/covid_winsor"); outdir.mkdir(parents=True, exist_ok=True)
        outX = outdir / f"X_panel_z__{panel}__{tag}__covid_winsor.csv"
        Xw.to_csv(outX, index_label="Date")
        print(f"{panel} {tag}: wrote {outX}")

if __name__ == "__main__":
    main()
