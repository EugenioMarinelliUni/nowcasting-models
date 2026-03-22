from __future__ import annotations

import json
from pathlib import Path
import pandas as pd


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_predictions(pred_df: pd.DataFrame, outdir: str | Path, filename: str = "predictions.csv") -> Path:
    outdir = ensure_dir(outdir)
    path = outdir / filename
    pred_df.to_csv(path, index=False)
    return path


def write_scores(scores: dict, outdir: str | Path, filename: str = "scores.json") -> Path:
    outdir = ensure_dir(outdir)
    path = outdir / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(scores, f, indent=2)
    return path


def write_run_config(run_config: dict, outdir: str | Path, filename: str = "run_config.json") -> Path:
    outdir = ensure_dir(outdir)
    path = outdir / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(run_config, f, indent=2, default=str)
    return path