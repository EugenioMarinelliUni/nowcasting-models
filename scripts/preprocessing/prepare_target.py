#!/usr/bin/env python3
from pathlib import Path
import yaml, json, pandas as pd, numpy as np

# Accept either of these for the date column in the raw CSV
DATE_COL_CANDIDATES = ["sasdate", "sasdata"]
# Value column name in the raw CSV
VALUE_COL = "gdp_qoq_saar"
# Raw filename expected under paths.targets_raw_dir
RAW_TARGET_NAME = "A191RL1Q225SBEA_latest.csv"

def parse_date(s, fmts):
    for f in fmts:
        try:
            return pd.to_datetime(s, format=f)
        except Exception:
            pass
    # fallback to pandas' parser
    return pd.to_datetime(s)

def load_yaml(path="config.yaml"):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def read_quarterly_target(raw_path: Path, parse_formats: list[str]) -> pd.Series:
    """
    Read the quarterly target CSV:
      - date column: one of DATE_COL_CANDIDATES
      - value column: VALUE_COL (gdp_qoq_saar)
    Returns a pandas Series indexed by DatetimeIndex (quarter timestamps).
    """
    if not raw_path.exists():
        raise FileNotFoundError(f"Target file not found: {raw_path}")
    df = pd.read_csv(raw_path)

    # locate date column
    date_col = None
    for c in DATE_COL_CANDIDATES:
        if c in df.columns:
            date_col = c
            break
    if date_col is None:
        raise ValueError(
            f"None of the date columns {DATE_COL_CANDIDATES} found in {raw_path.name}. "
            f"Columns present: {list(df.columns)}"
        )
    if VALUE_COL not in df.columns:
        raise ValueError(
            f"Value column '{VALUE_COL}' not found in {raw_path.name}. "
            f"Columns present: {list(df.columns)}"
        )

    df[date_col] = df[date_col].apply(lambda x: parse_date(str(x), parse_formats))
    df = df.sort_values(date_col)
    yq = pd.Series(
        df[VALUE_COL].astype(float).values,
        index=pd.DatetimeIndex(df[date_col].values),
        name="gdp_qoq_saar",
    )
    return yq

def build_monthly_index_from_panel(panel_csv: str,
                                   date_col: str,
                                   parse_formats: list[str],
                                   monthly_freq: str) -> pd.DatetimeIndex:
    """
    Build a canonical monthly DatetimeIndex from a panel CSV, aligned to MS or ME.
    """
    df = pd.read_csv(panel_csv)
    if date_col not in df.columns:
        raise ValueError(f"Date column '{date_col}' not found in {panel_csv}")
    df[date_col] = df[date_col].apply(lambda x: parse_date(x, parse_formats))
    df = df.set_index(date_col).sort_index()

    # Make index types explicit for linters
    dt_idx = pd.DatetimeIndex(df.index)
    per_m = pd.PeriodIndex(dt_idx, freq="M")

    if str(monthly_freq).upper() == "MS":
        idx = per_m.start_time   # month-start timestamps
    else:
        idx = per_m.end_time     # month-end timestamps

    # Ensure unique, sorted monthly index
    return pd.DatetimeIndex(pd.unique(idx)).sort_values()

def quarterly_to_monthly_last_month(yq: pd.Series, monthly_is_ms: bool = True) -> pd.Series:
    """
    Map each quarterly observation to the last month of its quarter on a monthly timestamp:
      - If monthly_is_ms=True, timestamp at month-start (MS) of Mar/Jun/Sep/Dec
      - Else, timestamp at month-end (ME)
    Non-quarter-end months remain NaN when later reindexed to the full monthly grid.
    """
    # Explicit types for linter
    dt_q = pd.DatetimeIndex(yq.index)
    per_q = pd.PeriodIndex(dt_q, freq="Q")

    # Quarter-end timestamps (Mar/Jun/Sep/Dec)
    qe_ts = per_q.to_timestamp(how="end")
    per_m = pd.PeriodIndex(qe_ts, freq="M")
    m_ts = per_m.start_time if monthly_is_ms else per_m.end_time

    ym = pd.Series(yq.values, index=m_ts, name="gdp_qoq_saar").sort_index()
    return ym

def standardize_target(ym: pd.Series, start: str, end: str):
    """
    Compute frozen (train-window) scalers and return the z-scored monthly target.
    """
    tr = ym.loc[start:end].dropna()
    mu = float(tr.mean()) if len(tr) else 0.0
    # Match PanelScaler/_nanmean_std: population standard deviation (ddof=0).
    sd = float(tr.std(ddof=0)) if len(tr) else 1.0
    if not np.isfinite(sd) or sd <= 0:
        sd = 1.0
    yz = (ym - mu) / sd
    return yz, mu, sd

def main():
    yml = load_yaml()
    p, d = yml["paths"], yml["dates"]

    # Resolve raw target path from config
    raw_dir = Path(p["targets_raw_dir"])  # e.g., data/raw_data/targets/gdp
    raw_path = raw_dir / RAW_TARGET_NAME
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Target file not found at {raw_path}. "
            f"Either place '{RAW_TARGET_NAME}' there or update paths.targets_raw_dir in config.yaml."
        )

    # 1) Read quarterly target
    yq = read_quarterly_target(raw_path, d["parse_formats"])

    # 2) Build master monthly index from the 1960 panel (ensures alignment with predictors)
    idx = build_monthly_index_from_panel(
        p["panel_1960_fixhousing_delta"],
        d["date_col"],
        d["parse_formats"],
        d["monthly_freq"],
    )
    monthly_is_ms = str(d["monthly_freq"]).upper() == "MS"

    # 3) Embed quarterly values onto monthly grid (only quarter-end months are non-NaN)
    ym = quarterly_to_monthly_last_month(yq, monthly_is_ms=monthly_is_ms)
    ym = ym.reindex(idx)  # align to the full monthly predictor timeline

    # 4) For each backtest config, z-score with frozen train scalers and write tagged output
    jobs = [
        ("1960_noVIX", "data/metadata/backtest/1960_noVIX/backtest_config_train1960_2015.json"),
        ("1965_withVIX", "data/metadata/backtest/1965_withVIX/backtest_config_train1965_2015.json"),
    ]
    for panel_id, cfgp in jobs:
        cfg = json.loads(Path(cfgp).read_text(encoding="utf-8"))
        tr = cfg["time_spans"]["train"]  # {"start": "1960-01-01", "end": "2015-12-01"}
        yz, mu, sd = standardize_target(ym, tr["start"], tr["end"])

        outdir = Path(f"dataset/{panel_id}/baseline")
        outdir.mkdir(parents=True, exist_ok=True)
        train_tag = f"train{tr['start'][:4]}_{tr['end'][:4]}"
        out = outdir / f"y_target_z__{panel_id}__{train_tag}.csv"
        yz.to_csv(out, index_label="Date", header=["y_target_z"])
        print(f"Wrote {out}")

if __name__ == "__main__":
    main()
