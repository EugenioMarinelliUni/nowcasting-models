# src/dfm_pipeline/preselection/io.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json
import pandas as pd

# ---------- Path helpers (X only from training_sets) ----------

def path_training_X(panel: str, tag: str) -> Path:
    # standardized training panel stored under training_sets
    return Path(f"dataset/{panel}/training_sets/{tag}/standardized_train__{panel}__{tag}.csv")

def path_baseline_y(panel: str, tag: str) -> Path:
    # standardized target built earlier (monthly stamps on quarter months)
    return Path(f"dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv")

def path_meta(panel: str, tag: str, method: str) -> Path:
    d = Path("data/metadata/variants")
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{panel}__{tag}__preselect_{method}.json"

def path_preselect_X(panel: str, tag: str, method: str) -> Path:
    d = Path(f"dataset/{panel}/preselect/{method}")
    d.mkdir(parents=True, exist_ok=True)
    return d / f"X_panel_z__{panel}__{tag}__preselect-{method}.csv"

# ---------- I/O helpers ----------

def _read_panel_csv(p: Path) -> pd.DataFrame:
    df = pd.read_csv(p, parse_dates=["Date"])
    if "Date" not in df.columns:
        raise ValueError(f"'Date' column not found in {p}")
    return df.set_index("Date").sort_index()

def load_X_y(panel: str, tag: str) -> Tuple[pd.DataFrame, pd.Series, Dict]:
    """
    Load X and y for {panel, tag}.

    - X: REQUIRED at dataset/{panel}/training_sets/{tag}/standardized_train__{panel}__{tag}.csv
    - y: REQUIRED at dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv

    Ensures indices match; returns (X, y, meta).
    """
    x_path = path_training_X(panel, tag)
    y_path = path_baseline_y(panel, tag)

    if not x_path.exists():
        raise FileNotFoundError(f"Missing training X: {x_path}")
    if not y_path.exists():
        raise FileNotFoundError(f"Missing target y: {y_path}")

    X = _read_panel_csv(x_path)
    y_df = _read_panel_csv(y_path)

    # first (or only) column is y
    y = y_df.iloc[:, 0].astype(float)

    if not X.index.equals(y.index):
        raise ValueError(
            "Index mismatch between X and y.\n"
            f"  X: {x_path}\n"
            f"  y: {y_path}\n"
            f"  X range: {X.index.min()} .. {X.index.max()}  (n={len(X)})\n"
            f"  y range: {y.index.min()} .. {y.index.max()}  (n={len(y)})\n"
            "Ensure both are on the same monthly index and same train-tag slice."
        )

    meta = {
        "panel": panel,
        "tag": tag,
        "x_path": str(x_path),
        "y_path": str(y_path),
        "n_rows": int(len(X)),
        "date_start": str(X.index.min().date()),
        "date_end": str(X.index.max().date()),
        "non_nan_y": int(y.notna().sum()),
    }
    return X, y, meta

def write_selected_meta(
    panel: str,
    tag: str,
    method: str,
    params: Dict,
    selected: List[str],
    scores: Optional[Dict[str, float]] = None,
) -> Path:
    p = path_meta(panel, tag, method)
    payload = {
        "method": method,
        "panel_id": panel,
        "train_tag": tag,
        "params": params,
        "selected": list(selected),
    }
    if scores is not None:
        payload["scores"] = {k: float(v) for k, v in scores.items()}
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return p

def write_preselected_panel(panel: str, tag: str, method: str, selected: List[str]) -> Path:
    """
    Reads the full training X from disk, writes the subset with the selected columns.
    """
    X_full = _read_panel_csv(path_training_X(panel, tag))
    missing = [c for c in selected if c not in X_full.columns]
    if missing:
        raise ValueError(f"Selected columns not found in X: {missing[:10]}{' ...' if len(missing)>10 else ''}")
    outp = path_preselect_X(panel, tag, method)
    X_full.loc[:, selected].to_csv(outp, index_label="Date")
    return outp
