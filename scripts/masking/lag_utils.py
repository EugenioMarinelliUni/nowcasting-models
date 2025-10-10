#!/usr/bin/env python3
from pathlib import Path
import json, pandas as pd

def load_lag_map(panel):
    p = Path(f"data/metadata/series_maps/lag_map_{panel}.json")
    return json.loads(p.read_text(encoding="utf-8"))

def latest_visible_period(snapshot_bm: pd.Timestamp, L: int) -> pd.Period:
    return snapshot_bm.to_period("M") - L  # monthly Period

def ms_timestamp(period_M: pd.Period) -> pd.Timestamp:
    return period_M.to_timestamp("MS")