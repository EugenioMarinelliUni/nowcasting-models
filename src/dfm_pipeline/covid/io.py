# src/dfm_pipeline/covid/io.py
from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_monthly_panel_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)

    date_col: Optional[str] = None
    for cand in ["date", "Date", "sasdate"]:
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        first = df.columns[0]
        dt = pd.to_datetime(df[first], errors="coerce")
        if dt.notna().mean() > 0.9:
            date_col = first
        else:
            raise ValueError(f"No obvious date column in {path}. Columns: {list(df.columns)[:8]}")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
    return df.select_dtypes(include="number")


def write_panel_csv(X: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = X.copy()
    df.insert(0, "date", df.index)
    df.to_csv(path, index=False)


def write_mask_matrix(mask: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out = mask.astype(int).copy()
    out.insert(0, "date", out.index)
    out.to_csv(path, index=False)


def write_json(obj: Dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def describe_mask(mask: pd.DataFrame) -> Dict[str, int]:
    n_total = int(mask.size)
    n_true = int(np.sum(mask.to_numpy(dtype=bool)))
    return {"n_total": n_total, "n_true": n_true}
