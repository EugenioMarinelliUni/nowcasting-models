#!/usr/bin/env python3
from pathlib import Path
import json, yaml, pandas as pd, numpy as np

def parse_date(s, fmts):
    for f in fmts:
        try:
            return pd.to_datetime(s, format=f)
        except Exception:
            pass
    return pd.to_datetime(s)

def load_monthly_csv(path, date_col, parse_formats, monthly_freq):
    df = pd.read_csv(path)
    if date_col not in df.columns:
        raise ValueError(f"Date column '{date_col}' not found in {path}")
    df[date_col] = df[date_col].apply(lambda x: parse_date(x, parse_formats))
    df = df.set_index(date_col).sort_index()

    # Align to monthly timestamps without using unsupported "MS" freq
    dt_idx = pd.DatetimeIndex(df.index)
    if str(monthly_freq).upper() == "MS":
        df.index = dt_idx.to_period("M").start_time   # month-start timestamps
    else:
        df.index = dt_idx.to_period("M").end_time     # month-end timestamps

    df = df.apply(pd.to_numeric, errors="coerce")
    return df

def compute_frozen_scalers(X, start, end, min_obs=36):
    Xtr = X.loc[start:end]
    mu = Xtr.mean(skipna=True).fillna(0.0)
    sd = Xtr.std(skipna=True)
    n = Xtr.notna().sum()
    sd[(~np.isfinite(sd)) | (sd <= 0) | (n < min_obs)] = np.nan
    gmed = sd.dropna().median()
    if not np.isfinite(gmed) or gmed <= 0:
        gmed = 1.0
    sd = sd.fillna(gmed).replace(0, 1.0)
    return mu, sd

def run(panel_label, raw_csv, cfg_json, mean_out, std_out, yml):
    # load panel
    X = load_monthly_csv(
        raw_csv,
        yml["dates"]["date_col"],
        yml["dates"]["parse_formats"],
        yml["dates"]["monthly_freq"],
    )

    # load config (for train window & panel id)
    cfg = json.loads(Path(cfg_json).read_text(encoding="utf-8"))
    tr = cfg["time_spans"]["train"]
    panel_id = cfg["panel_id"]              # e.g. "1960_noVIX"
    train_tag = f"train{tr['start'][:4]}_{tr['end'][:4]}"

    # scalers
    mu, sd = compute_frozen_scalers(X, tr["start"], tr["end"])

    # save scalers
    Path(mean_out).parent.mkdir(parents=True, exist_ok=True)
    Path(std_out).parent.mkdir(parents=True, exist_ok=True)
    Path(mean_out).write_text(json.dumps(mu.to_dict(), indent=2))
    Path(std_out).write_text(json.dumps(sd.to_dict(), indent=2))

    # standardize & write tagged filename
    Xz = (X - mu) / sd
    outdir = Path(f"dataset/{panel_id}/baseline")
    outdir.mkdir(parents=True, exist_ok=True)
    fname = f"X_panel_z__{panel_id}__{train_tag}.csv"
    Xz_out = outdir / fname
    Xz.to_csv(Xz_out, index_label="Date")
    print(f"{panel_label}: wrote {Xz_out}")

if __name__ == "__main__":
    yml = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))
    p = yml["paths"]

    jobs = [
        (
            "1960_noVIX (train 1960–2015)",
            p["panel_1960_fixhousing_delta"],
            "data/metadata/backtest/1960_noVIX/backtest_config_train1960_2015.json",
            "data/metadata/scalers/mean_map_1960_noVIX_train1960_2015.json",
            "data/metadata/scalers/std_map_1960_noVIX_train1960_2015.json",
        ),
        (
            "1965_withVIX (train 1965–2015)",
            p["panel_1965_fixhousing_delta"],
            "data/metadata/backtest/1965_withVIX/backtest_config_train1965_2015.json",
            "data/metadata/scalers/mean_map_1965_withVIX_train1965_2015.json",
            "data/metadata/scalers/std_map_1965_withVIX_train1965_2015.json",
        ),
    ]

    for args in jobs:
        run(*args, yml=yml)
