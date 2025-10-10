#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import json, yaml
import numpy as np
import pandas as pd

PROCESS_ALL_CONFIGS = True     # False -> only selected_config

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

def residualize_on_dummy(X: pd.DataFrame, start: str, end: str, ms: bool,
                         separate: bool=False) -> tuple[pd.DataFrame, pd.DataFrame]:
    X = X.copy()
    idx = X.index
    s = ts_month(start, ms)
    e = ts_month(end,   ms)

    if separate:
        months = pd.date_range(s, e, freq="MS" if ms else "M")
        D = pd.DataFrame(0.0, index=idx, columns=[f"covid_{d.strftime('%Y-%m')}" for d in months])
        for d, col in zip(months, D.columns):
            D.loc[idx == d, col] = 1.0
    else:
        D = pd.DataFrame({"covid": ((idx >= s) & (idx <= e)).astype(float)}, index=idx)

    Z = pd.concat([pd.Series(1.0, index=idx, name="const"), D], axis=1).to_numpy(dtype=float)

    X_adj = pd.DataFrame(index=idx, columns=X.columns, dtype=float)
    for sname in X.columns:
        y = X[sname].to_numpy(dtype=float)
        m = ~np.isnan(y)
        if m.sum() < 5:
            X_adj[sname] = X[sname]
            continue
        Zm, ym = Z[m], y[m]
        beta = np.linalg.pinv(Zm.T @ Zm) @ (Zm.T @ ym)
        D_all = Z[:, 1:]                      # drop const
        X_adj[sname] = y - (D_all @ beta[1:]) # subtract dummy component only

    D_out = pd.DataFrame(D, index=idx)
    return X_adj, D_out

def main() -> None:
    yml = load_yaml()
    ms = str(yml["dates"]["monthly_freq"]).upper() == "MS"
    start, end = yml["covid"]["window_start"], yml["covid"]["window_end"]

    cfg_paths = list_cfgs(yml) if PROCESS_ALL_CONFIGS else [selected_cfg(yml)]

    for cfgp in cfg_paths:
        cfg = json.loads(Path(cfgp).read_text(encoding="utf-8"))
        panel = cfg["panel_id"]; tag = train_tag(cfg)

        X = pd.read_csv(baseline_path(cfg), parse_dates=["Date"]).set_index("Date").sort_index()
        per = pd.PeriodIndex(pd.DatetimeIndex(X.index), freq="M")
        X.index = pd.DatetimeIndex(per.start_time if ms else per.end_time)

        X_adj, D = residualize_on_dummy(X, start, end, ms=ms, separate=False)

        out_dir = Path(f"dataset/{panel}/covid_dummies"); out_dir.mkdir(parents=True, exist_ok=True)
        outX = out_dir / f"X_panel_z__{panel}__{tag}__covid_dummies.csv"
        outD = out_dir / f"dummies_monthly__{panel}__{tag}.csv"
        X_adj.to_csv(outX, index_label="Date")
        D.to_csv(outD, index_label="Date")
        print(f"{panel} {tag}: wrote {outX} and {outD}")

if __name__ == "__main__":
    main()
