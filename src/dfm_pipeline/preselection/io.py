from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple
import json
import pandas as pd


# ---------- Path helpers ----------

def path_baseline_X(panel: str, tag: str) -> Path:
    return Path(f"dataset/{panel}/baseline/X_panel_z__{panel}__{tag}.csv")

def path_baseline_y(panel: str, tag: str) -> Path:
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

def load_X_y(panel: str, tag: str) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Load baseline X and y for {panel, tag}, ensure a monthly Date index shared by both.
    """
    xp, yp = path_baseline_X(panel, tag), path_baseline_y(panel, tag)
    if not xp.exists():
        raise FileNotFoundError(f"Missing X panel: {xp}")
    if not yp.exists():
        raise FileNotFoundError(f"Missing y target: {yp}")

    X = pd.read_csv(xp, parse_dates=["Date"]).set_index("Date").sort_index()
    ydf = pd.read_csv(yp, parse_dates=["Date"]).set_index("Date").sort_index()

    # First (or only) column is the target
    y = ydf.iloc[:, 0].astype(float)

    if not X.index.equals(y.index):
        raise ValueError(
            f"Index mismatch between X ({xp}) and y ({yp}). "
            "Make sure both are on the same monthly MS/ME index."
        )
    return X, y


def write_selected_meta(panel: str, tag: str, method: str,
                        params: Dict, selected: List[str]) -> Path:
    p = path_meta(panel, tag, method)
    payload = {
        "method": method,
        "panel_id": panel,
        "train_tag": tag,
        "params": params,
        "selected": list(selected),
    }
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return p


def write_preselected_panel(panel: str, tag: str, method: str,
                            X_full: pd.DataFrame, cols: List[str]) -> Path:
    outp = path_preselect_X(panel, tag, method)
    X_full.loc[:, cols].to_csv(outp, index_label="Date")
    return outp
