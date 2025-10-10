#!/usr/bin/env python3
"""
Build BME-snapshot availability masks for EXPLANATORY VARIABLES (predictors only).

Outputs (per panel):
  output/<panel_id>/masks_pred/mask_YYYY-MM-DD.csv
  output/<panel_id>/masks_pred/_manifest.csv

Behavior
--------
- Snapshots default to begin the MONTH AFTER the training end date (no masks in train).
- If an eval window exists in backtest JSON, it is intersected with [train_end+1M, X.max()].
- Uses series publication lags L (months) from: data/metadata/series_maps/lag_map_<panel_id>.json
- Availability = (lag condition satisfied by snapshot) AND (cell is non-NaN in the panel).
"""

from pathlib import Path
import json
import yaml
import pandas as pd

# ----------------------------
# Toggles
# ----------------------------
PROCESS_ALL_CONFIGS = True     # True: process all backtest.configs; False: only selected_config
GENERATE_PRETRAIN_MASKS = False  # True: allow snapshots inside training window; False: start after train end

# ----------------------------
# Minimal helpers (no project-internal imports required)
# ----------------------------

def load_yaml(path: str | Path = "config.yaml") -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def monthly_is_ms(yml: dict) -> bool:
    return str(yml["dates"]["monthly_freq"]).upper() == "MS"

def list_backtest_configs_from_yaml(yml: dict) -> list[Path]:
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

def panel_z_path_from_cfg(cfg: dict, variant: str = "baseline") -> Path:
    """
    Build standardized predictor panel path:
      dataset/<panel_id>/<variant>/X_panel_z__<panel_id>__trainYYYY_YYYY.csv
    Falls back to legacy untagged path in config.yaml if present and exists.
    """
    panel_id = cfg["panel_id"]
    tag = train_tag_from_cfg(cfg)
    tagged = Path(f"dataset/{panel_id}/{variant}/X_panel_z__{panel_id}__{tag}.csv")
    if tagged.exists():
        return tagged

    # Optional legacy fallback (kept for compatibility)
    yml = load_yaml()
    legacy_key = None
    if "1960" in panel_id:
        legacy_key = "panel_z_1960_baseline" if variant == "baseline" else None
    elif "1965" in panel_id:
        legacy_key = "panel_z_1965_baseline" if variant == "baseline" else None
    if legacy_key and yml["paths"].get(legacy_key):
        legacy = Path(yml["paths"][legacy_key])
        if legacy.exists():
            return legacy

    raise FileNotFoundError(f"Predictor panel not found:\n  {tagged}")

def load_lag_map(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))

def coerce_monthly_index(idx: pd.Index, ms: bool) -> pd.DatetimeIndex:
    """Coerce any index to monthly MS/ME timestamps via PeriodIndex → DatetimeIndex."""
    dt_idx = pd.DatetimeIndex(idx)
    per_m = pd.PeriodIndex(dt_idx, freq="M")
    out = per_m.start_time if ms else per_m.end_time
    return pd.DatetimeIndex(out)

def latest_visible_period(snapshot_bme: pd.Timestamp, L: int) -> pd.Period:
    """BME snapshot + lag → latest visible reference month (Period[M])."""
    return snapshot_bme.to_period("M") - int(L)

def mask_for_snapshot(x_index: pd.Index, lag_map: dict, snapshot_bme: pd.Timestamp, ms: bool) -> pd.DataFrame:
    """Boolean availability mask over x_index × series for a single snapshot."""
    x_index = pd.DatetimeIndex(x_index)
    cols = list(lag_map.keys())
    M = pd.DataFrame(False, index=x_index, columns=cols)
    for s, L in lag_map.items():
        last_vis_per = latest_visible_period(snapshot_bme, L)  # Period[M]
        cutoff = last_vis_per.start_time if ms else last_vis_per.end_time
        M.loc[:cutoff, s] = True  # available up to and including cutoff
    return M

# ----------------------------
# Main
# ----------------------------

def main():
    yml = load_yaml()
    ms = monthly_is_ms(yml)

    # Choose configs to process
    if PROCESS_ALL_CONFIGS:
        cfg_paths = list_backtest_configs_from_yaml(yml)
    else:
        cfg_paths = [selected_backtest_config_path(yml)]

    for cfgp in cfg_paths:
        cfg = load_backtest_config(cfgp)
        panel_id = cfg["panel_id"]

        # 1) Load predictor panel (z-scored, tagged)
        X_path = panel_z_path_from_cfg(cfg, variant="baseline")
        X = pd.read_csv(X_path, parse_dates=["Date"]).set_index("Date").sort_index()
        X.index = coerce_monthly_index(X.index, ms=ms)

        # 2) Load lag map for this panel
        lag_map_path = Path(f"data/metadata/series_maps/lag_map_{panel_id}.json")
        if not lag_map_path.exists():
            raise FileNotFoundError(f"Missing lag map: {lag_map_path}")
        lag_map = load_lag_map(lag_map_path)

        # Align lag map to X columns
        unknown = sorted(set(lag_map) - set(X.columns))
        missing = sorted(set(X.columns) - set(lag_map))
        if unknown:
            print(f"[WARN] {panel_id}: {len(unknown)} lag-map series not in panel (ignored), e.g. {unknown[:8]}")
        lag_map = {s: lag_map[s] for s in lag_map if s in X.columns}
        if missing:
            print(f"[WARN] {panel_id}: {len(missing)} panel series missing in lag map. Defaulting L=1 for a few: {missing[:8]}")
            for s in missing:
                lag_map[s] = 1

        # 3) Snapshot range: start AFTER train end by default; intersect with eval if present
        train = cfg["time_spans"]["train"]
        train_end = pd.Timestamp(train["end"])

        default_start = X.index.min() if GENERATE_PRETRAIN_MASKS else (train_end + pd.offsets.MonthBegin(1))
        default_end = X.index.max()

        eval_span = cfg["time_spans"].get("eval", None)
        if eval_span:
            snap_start = max(pd.Timestamp(eval_span["start"]), default_start)
            snap_end   = min(pd.Timestamp(eval_span["end"]),   default_end)
        else:
            snap_start, snap_end = default_start, default_end

        if snap_start > snap_end:
            print(f"[SKIP] {panel_id}: empty snapshot range ({snap_start.date()} > {snap_end.date()})")
            continue

        # Use Business Month End (BME); BM is deprecated
        snapshots = pd.date_range(snap_start, snap_end, freq="BME")

        # 4) Build masks and write
        outdir = Path(f"output/{panel_id}/masks_pred"); outdir.mkdir(parents=True, exist_ok=True)
        obs = X.notna()

        written = 0
        for snap in snapshots:
            M = mask_for_snapshot(X.index, lag_map, snap, ms=ms)
            M = M & obs.reindex_like(M).fillna(False)  # intersect with actual non-NaN
            fn = outdir / f"mask_{snap.strftime('%Y-%m-%d')}.csv"
            M.to_csv(fn, index_label="Date")
            written += 1

        # Manifest for convenience
        pd.Series([d.strftime("%Y-%m-%d") for d in snapshots], name="snapshot_bm") \
          .to_csv(outdir / "_manifest.csv", index=False)

        print(f"{panel_id}: wrote {written} predictor masks to {outdir}")

if __name__ == "__main__":
    main()
