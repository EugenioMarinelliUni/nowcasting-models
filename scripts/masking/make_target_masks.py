#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import json
import yaml
import numpy as np
import pandas as pd
from typing import List

# Toggles
PROCESS_ALL_CONFIGS: bool = True
GENERATE_PRETRAIN_MASKS: bool = False
RELEASE_LAG_MONTHS: int = 1  # 1=advance, 2=second, 3=third

def load_yaml(path: str | Path = "config.yaml") -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def list_backtest_configs_from_yaml(yml: dict) -> List[Path]:
    cfgs = yml.get("backtest", {}).get("configs", {})
    if not cfgs:
        raise ValueError("No backtest.configs found in config.yaml")
    return [Path(p) for p in cfgs.values()]

def selected_backtest_config_path(yml: dict) -> Path:
    sel = yml["backtest"]["selected_config"]
    return Path(yml["backtest"]["configs"][sel])

def load_backtest_config(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))

def train_tag_from_cfg(cfg: dict) -> str:
    tr = cfg["time_spans"]["train"]
    return f"train{tr['start'][:4]}_{tr['end'][:4]}"

def target_z_path_from_cfg(cfg: dict, variant: str = "baseline") -> Path:
    panel_id = cfg["panel_id"]
    tag = train_tag_from_cfg(cfg)
    p = Path(f"dataset/{panel_id}/{variant}/y_target_z__{panel_id}__{tag}.csv")
    if not p.exists():
        raise FileNotFoundError(f"Target file not found:\n  {p}")
    return p

def coerce_monthly_ms(idx: pd.Index) -> pd.DatetimeIndex:
    """Coerce to monthly Month-Start timestamps via PeriodIndex -> DatetimeIndex."""
    dt = pd.DatetimeIndex(idx)
    per_m = pd.PeriodIndex(dt, freq="M")
    return pd.DatetimeIndex(per_m.start_time)

def main() -> None:
    yml = load_yaml()
    cfg_paths = list_backtest_configs_from_yaml(yml) if PROCESS_ALL_CONFIGS else [selected_backtest_config_path(yml)]

    for cfgp in cfg_paths:
        cfg = load_backtest_config(cfgp)
        panel_id = cfg["panel_id"]

        # 1) Load monthly target (z-scored; values only at quarter-end months)
        y_path = target_z_path_from_cfg(cfg, variant="baseline")
        y = pd.read_csv(y_path, parse_dates=["Date"]).set_index("Date").sort_index()
        y.index = coerce_monthly_ms(y.index)  # ensure MS monthly index
        target_col = str(y.columns[0])

        # 2) Decide snapshot range
        train_end = pd.Timestamp(cfg["time_spans"]["train"]["end"])
        default_start = y.index.min() if GENERATE_PRETRAIN_MASKS else (train_end + pd.offsets.MonthBegin(1))
        default_end = y.index.max()

        eval_span = cfg["time_spans"].get("eval", None)
        if eval_span:
            snap_start = max(pd.Timestamp(eval_span["start"]), default_start)
            snap_end   = min(pd.Timestamp(eval_span["end"]),   default_end)
        else:
            snap_start, snap_end = default_start, default_end

        if snap_start > snap_end:
            print(f"[SKIP] {panel_id}: empty snapshot range ({snap_start.date()} > {snap_end.date()})")
            continue

        snapshots = pd.date_range(snap_start, snap_end, freq="BME")  # Business Month End

        # 3) Precompute first-visible month for each quarter-end month
        dt_index = pd.DatetimeIndex(y.index)  # explicit type for linters
        months = dt_index.month
        qend_mask = np.isin(months, [3, 6, 9, 12])
        qend = pd.DatetimeIndex(dt_index[qend_mask])
        first_visible = {tq: (tq + pd.offsets.MonthBegin(RELEASE_LAG_MONTHS)) for tq in qend}

        # 4) Build & write masks
        outdir = Path(f"output/{panel_id}/masks_target"); outdir.mkdir(parents=True, exist_ok=True)
        written = 0
        for snap in snapshots:
            data = np.zeros(len(y.index), dtype=bool)                 # explicit bool array (silences type checkers)
            m = pd.Series(data, index=y.index, name=target_col)       # one-column mask over monthly index

            # mark quarter-end months observed if snapshot >= first_visible
            for tq, fv in first_visible.items():
                if snap >= fv:
                    m.loc[tq] = True

            fn = outdir / f"mask_{snap.strftime('%Y-%m-%d')}.csv"
            m.to_frame().to_csv(fn, index_label="Date")
            written += 1

        pd.Series([d.strftime("%Y-%m-%d") for d in snapshots], name="snapshot_bm").to_csv(outdir / "_manifest.csv", index=False)
        rel = {1: "advance", 2: "second", 3: "third"}.get(RELEASE_LAG_MONTHS, f"+{RELEASE_LAG_MONTHS}M")
        print(f"{panel_id}: wrote {written} TARGET masks ({rel}) to {outdir}")

if __name__ == "__main__":
    main()
