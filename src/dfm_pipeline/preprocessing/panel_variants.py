# src/dfm_pipeline/preprocessing/panel_variants.py
from __future__ import annotations

from pathlib import Path
import json
from typing import Dict, List, Tuple

import pandas as pd


# ---------- Defaults (tune here or override via function args) ----------

DATE_COL_DEFAULT = "sasdate"
DATE_FMT_DEFAULT = "%m/%d/%Y"    # your CSV dates are mm/dd/yyyy

# Column rules
MISS_SHARE_MAX_DEFAULT = 0.40    # drop if > 40% missing overall (after anchoring)
LEAD_LIMIT_DEFAULT     = 24      # months, leading NaNs since anchor_start
INTERIOR_MAX_DEFAULT   = 6       # months, longest interior NaN run
MIN_OBS_TRAIN_DEFAULT  = 36      # months, min observed count in training window

# Auto training-window selection (data-agnostic)
COV_THRESH_DEFAULT     = 0.75    # require >= 75% series observed to start training
TRAIN_YEARS_DEFAULT    = 30      # target training length (years)
HOLDOUT_MONTHS_DEFAULT = 60      # keep last N months for validation/nowcasting
MIN_RUN_CONSEC_DEFAULT = 12      # need at least this many consecutive months above coverage threshold


# ---------- Helpers ----------

def ensure_monthly_panel(
    in_csv: Path,
    date_col: str = DATE_COL_DEFAULT,
    date_fmt: str = DATE_FMT_DEFAULT,
) -> pd.DataFrame:
    """
    Load a CSV, parse the date column (mm/dd/yyyy), and reindex to a contiguous monthly index (Month Start).
    Returns a DataFrame with a DatetimeIndex of monthly stamps and original columns.
    """
    df = pd.read_csv(in_csv)
    if date_col not in df.columns:
        raise ValueError(f"Date column '{date_col}' not found in {in_csv}")
    df[date_col] = pd.to_datetime(df[date_col], format=date_fmt, errors="coerce")
    if df[date_col].isna().all():
        raise ValueError(f"Could not parse any dates in column '{date_col}' using format '{date_fmt}'.")
    df = df.set_index(date_col).sort_index()

    # Build a contiguous monthly index so “months” are counted correctly
    full_idx = pd.date_range(df.index.min(), df.index.max(), freq="MS")
    df = df.reindex(full_idx)
    df.index.name = date_col  # ensure a named index for clean CSVs

    return df


def leading_na_since(s: pd.Series, start_ts: pd.Timestamp) -> int:
    """Count initial NaN months from start_ts onward until the first non-NaN."""
    s2 = s.loc[start_ts:]
    return int(s2.isna().cumprod().sum())


def interior_max_gap(s: pd.Series) -> int:
    """Longest contiguous NaN run between first and last valid obs (after anchoring)."""
    if s.notna().sum() == 0:
        return len(s)
    a = s.loc[s.first_valid_index(): s.last_valid_index()]
    runs = (a.isna() != a.isna().shift()).cumsum()
    return int(a.isna().groupby(runs).sum().max())


def pick_training_window(
    df_monthly_anchored: pd.DataFrame,
    cov_thresh: float = COV_THRESH_DEFAULT,
    train_years: int = TRAIN_YEARS_DEFAULT,
    holdout_months: int = HOLDOUT_MONTHS_DEFAULT,
    min_run: int = MIN_RUN_CONSEC_DEFAULT,
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Choose (TRAIN_START, TRAIN_END) automatically from an anchored monthly panel.

    Strategy:
      • Month-by-month coverage = fraction of non-missing series.
      • TRAIN_START = earliest month where coverage >= cov_thresh for min_run consecutive months
                      (fallback: first month with coverage >= cov_thresh; else first month after min_run-1).
      • TRAIN_END   = last month minus holdout_months (reserve for validation/nowcasting).
      • Try to allocate train_years*12 months ending at TRAIN_END, but never before TRAIN_START.
      • If window would be empty, shrink holdout to provide at least min_run months.

    Returns: (TRAIN_START, TRAIN_END) as pandas Timestamps.
    """
    anchored = df_monthly_anchored
    if anchored.empty:
        raise ValueError("Panel is empty after anchoring.")

    # Work with an explicit DatetimeIndex for clearer typing
    idx: pd.DatetimeIndex = pd.DatetimeIndex(anchored.index)

    coverage = anchored.notna().mean(axis=1)  # 0..1 per month

    roll_ok = coverage.rolling(min_run, min_periods=min_run).min() >= cov_thresh
    if bool(roll_ok.any()):
        first_ok: pd.Timestamp = pd.Timestamp(roll_ok[roll_ok].index[0])
    else:
        ok = coverage >= cov_thresh
        if bool(ok.any()):
            first_ok = pd.Timestamp(ok[ok].index[0])
        else:
            # fallback: the min_run-th month (0-based) if available, else the last available
            fallback_pos = min(min_run - 1, max(0, len(idx) - 1))
            first_ok = pd.Timestamp(idx[fallback_pos])

    # End: leave a holdout buffer
    n = len(idx)
    end_idx = max(0, n - holdout_months - 1)
    train_end: pd.Timestamp = pd.Timestamp(idx[end_idx])

    # Start: aim for train_years*12 months ending at train_end, but not before first_ok
    desired_len = train_years * 12
    start_min_idx = int(idx.get_indexer([first_ok], method="nearest")[0])
    start_idx = max(start_min_idx, end_idx - desired_len + 1)
    train_start: pd.Timestamp = pd.Timestamp(idx[start_idx])

    # Ensure non-empty window; if needed, shrink holdout to guarantee at least min_run months
    if train_start > train_end:
        start_idx = start_min_idx
        end_idx = max(start_idx + min_run - 1, start_idx)
        train_end = pd.Timestamp(idx[min(end_idx, n - 1)])
        train_start = pd.Timestamp(idx[start_idx])

    return train_start, train_end


def build_variant(
    df_all: pd.DataFrame,
    name: str,
    anchor_start: str,
    *,
    out_dir_processed: Path = Path("data/processed_data"),
    out_dir_meta: Path = Path("data/metadata"),
    date_fmt: str = DATE_FMT_DEFAULT,
    # Column-rule thresholds:
    miss_share_max: float = MISS_SHARE_MAX_DEFAULT,
    lead_limit_months: int = LEAD_LIMIT_DEFAULT,
    interior_gap_max_months: int = INTERIOR_MAX_DEFAULT,
    min_obs_train_months: int = MIN_OBS_TRAIN_DEFAULT,
    # Training-window selection:
    cov_thresh: float = COV_THRESH_DEFAULT,
    train_years: int = TRAIN_YEARS_DEFAULT,
    holdout_months: int = HOLDOUT_MONTHS_DEFAULT,
    min_run_consec: int = MIN_RUN_CONSEC_DEFAULT,
    in_csv_path: Path | None = None,
) -> Dict:
    """
    Build one variant:
      1) Anchor by dropping rows before anchor_start.
      2) Auto-pick (TRAIN_START, TRAIN_END) from the anchored panel (coverage-based).
      3) Apply 4 column rules and drop violating series.
      4) Save CSVs (index-labeled and sasdate-as-column) + a JSON catalog of decisions.

    Returns a dict with metadata (also written to JSON).
    """
    out_dir_processed.mkdir(parents=True, exist_ok=True)
    out_dir_meta.mkdir(parents=True, exist_ok=True)

    anchor_ts = pd.to_datetime(anchor_start, format=date_fmt)
    df = df_all.loc[df_all.index >= anchor_ts].copy()

    train_start, train_end = pick_training_window(
        df,
        cov_thresh=cov_thresh,
        train_years=train_years,
        holdout_months=holdout_months,
        min_run=min_run_consec,
    )

    drop: Dict[str, Dict] = {}
    kept: List[str] = []

    for col in df.columns:
        s = df[col]
        st = s.loc[train_start:train_end]

        miss_share = float(s.isna().mean())
        lead_na = leading_na_since(s, anchor_ts)
        interior = interior_max_gap(s)
        nobs_train = int(st.notna().sum())

        why = None
        if miss_share > miss_share_max:
            why = f"missing_share>{miss_share_max:.2f}"
        elif lead_na > lead_limit_months:
            why = f"leading_na>{lead_limit_months}"
        elif interior > interior_gap_max_months:
            why = f"interior_gap>{interior_gap_max_months}"
        elif nobs_train < min_obs_train_months:
            why = f"nobs_train<{min_obs_train_months}"

        if why:
            drop[col] = {
                "missing_share": miss_share,
                "leading_na_since_anchor": lead_na,
                "interior_gap_months": interior,
                "nobs_train": nobs_train,
                "drop_reason": why,
            }
        else:
            kept.append(col)

    df_out = df[kept]

    # ---------- Write BOTH CSV styles ----------
    # A) Keep DatetimeIndex (label it 'sasdate' to avoid <anonymous>)
    df_out.index.name = DATE_COL_DEFAULT
    out_csv_index = out_dir_processed / f"{name}.csv"
    df_out.to_csv(out_csv_index, index_label=DATE_COL_DEFAULT, date_format=date_fmt, float_format="%.10g")

    # B) sasdate as a regular column (no index)
    df_out_reset = df_out.reset_index().rename(columns={df_out.index.name or "index": DATE_COL_DEFAULT})
    out_csv_col = out_dir_processed / f"{name}_sasdate_column.csv"
    df_out_reset.to_csv(out_csv_col, index=False, date_format=date_fmt, float_format="%.10g")

    # ---------- Metadata ----------
    ts_to_str = lambda ts: pd.Timestamp(ts).strftime(date_fmt)

    meta_path = out_dir_meta / f"{name}_catalog.json"
    meta = {
        "input_csv": str(in_csv_path) if in_csv_path else None,
        "variant": name,
        "anchor_start": anchor_start,
        "train_window": [ts_to_str(train_start), ts_to_str(train_end)],
        "rules": {
            "missing_share_max": miss_share_max,
            "leading_na_limit_months": lead_limit_months,
            "interior_gap_max_months": interior_gap_max_months,
            "min_obs_train_months": min_obs_train_months,
            "leading_ref": "anchor_start",
        },
        "training_window_selection": {
            "cov_thresh": cov_thresh,
            "train_years": train_years,
            "holdout_months": holdout_months,
            "min_run_consecutive": min_run_consec,
            "anchored_span": [ts_to_str(df.index.min()), ts_to_str(df.index.max())],
        },
        "n_rows": int(df_out.shape[0]),
        "n_series": int(df_out.shape[1]),
        "kept_series": kept,
        "dropped_series": drop,
        "artifacts": {
            "panel_csv_index": str(out_csv_index),
            "panel_csv_with_sasdate_col": str(out_csv_col),
            "catalog_json": str(meta_path),
        },
    }

    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return meta

