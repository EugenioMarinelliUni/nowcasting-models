#!/usr/bin/env python3
# Location: scripts/preselection/preselect_lars.py
from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Dict, Literal

import argparse
import json
import yaml
import pandas as pd
import numpy as np
from sklearn.linear_model import LassoLarsCV

# ---------------------------
# Utilities (self-contained)
# ---------------------------
def load_yaml(path: str = "config.yaml") -> Dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def load_cfg(cfg_path: str) -> Dict:
    return json.loads(Path(cfg_path).read_text(encoding="utf-8"))

def ms_index(idx: pd.Index, monthly_freq: str) -> pd.DatetimeIndex:
    """
    Normalize any datetime-like index to month-start or month-end timestamps.

    Using a Literal for `how` silences type checkers that expect one of
    {"start","end","s","e"} instead of a generic str.
    """
    ms = str(monthly_freq).upper() == "MS"
    di = pd.DatetimeIndex(pd.to_datetime(idx, errors="coerce"))
    how: Literal["start", "end"] = "start" if ms else "end"
    return di.to_period("M").to_timestamp(how=how)

def load_Xy(panel_csv: Path, target_csv: Path, monthly_freq: str) -> Tuple[pd.DataFrame, pd.Series]:
    X = pd.read_csv(panel_csv, parse_dates=["Date"]).set_index("Date").sort_index()
    y = pd.read_csv(target_csv, parse_dates=["Date"]).set_index("Date").sort_index()
    X.index = ms_index(X.index, monthly_freq)
    X = X.apply(pd.to_numeric, errors="coerce")
    y.index = ms_index(y.index, monthly_freq)
    y_series = y.iloc[:, 0].astype(float)
    XY = X.join(y_series.to_frame("y"), how="inner")
    return XY.drop(columns=["y"]), XY["y"]

def slice_train(X: pd.DataFrame, y: pd.Series, start: str, end: str) -> Tuple[pd.DataFrame, pd.Series]:
    return X.loc[start:end], y.loc[start:end]

def train_impute(Xtr: pd.DataFrame, min_obs: int = 36) -> pd.DataFrame:
    ok = Xtr.notna().sum() >= int(min_obs)
    Xtr2 = Xtr.loc[:, ok]
    return Xtr2.fillna(Xtr2.mean())

def dedup_by_corr(Xtr: pd.DataFrame, cols: List[str], tau: float) -> List[str]:
    if len(cols) <= 1:
        return cols
    C = Xtr[cols].corr().abs()
    keep: List[str] = []
    seen: set[str] = set()
    for c in cols:
        if c in seen:
            continue
        keep.append(c)
        dup = C.index[(C[c] >= tau) & (C.index != c)]
        seen.update(dup)
    return keep

def write_meta(panel: str, tag: str, method: str, params: Dict, selected: List[str]) -> Path:
    out = Path("data/metadata/variants")
    out.mkdir(parents=True, exist_ok=True)
    p = out / f"{panel}__{tag}__preselect_{method}.json"
    p.write_text(json.dumps(
        {
            "method": method,
            "panel_id": panel,
            "train_tag": tag,
            "params": params,
            "selected": selected,
        },
        indent=2,
    ))
    return p

def write_panel(panel: str, tag: str, method: str, X_full: pd.DataFrame, cols: List[str]) -> Path:
    out = Path(f"dataset/{panel}/preselect/{method}")
    out.mkdir(parents=True, exist_ok=True)
    fp = out / f"X_panel_z__{panel}__{tag}__preselect-{method}.csv"
    X_full.loc[:, cols].to_csv(fp, index_label="Date")
    return fp

# ---------------------------
# Core
# ---------------------------
def run_lars(
    cfg_path: str,
    monthly_freq: str,
    cv: int = 10,
    max_features: int = 0,
    min_obs_train: int = 36,
    write_panel_flag: bool = True,
    min_features: int = 0,
    dedup_tau: float = 0.98,
) -> List[str]:

    cfg = load_cfg(cfg_path)
    panel = cfg["panel_id"]
    ts = cfg["time_spans"]["train"]["start"]
    te = cfg["time_spans"]["train"]["end"]
    tag = f"train{ts[:4]}_{te[:4]}"

    Xp = Path(f"dataset/{panel}/baseline/X_panel_z__{panel}__{tag}.csv")
    yp = Path(f"dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv")
    X_full, y_full = load_Xy(Xp, yp, monthly_freq)
    Xtr, ytr = slice_train(X_full, y_full, ts, te)

    m_y = ytr.notna()
    Xtr = Xtr.loc[m_y]
    ytr = ytr.loc[m_y]
    Xtr = train_impute(Xtr, min_obs=min_obs_train)
    ytr = ytr.loc[Xtr.index]

    # Fit LassoLarsCV; keep the typed estimator variable (do not use the return of .fit)
    estimator = LassoLarsCV(cv=int(cv), fit_intercept=False)
    estimator.fit(Xtr.values, ytr.values)

    # LARS path attributes (now type-check clean)
    alphas: np.ndarray = estimator.alphas_
    coef_path: np.ndarray = estimator.coef_path_.T  # shape (n_alphas, p)
    cols_all: List[str] = Xtr.columns.to_list()

    def support_at(i: int) -> List[str]:
        nz = np.flatnonzero(coef_path[i])
        return [cols_all[k] for k in nz]

    # index of chosen alpha (closest in the grid)
    i = int(np.argmin(np.abs(alphas - estimator.alpha_)))
    cols = support_at(i)

    # Enforce max/min by walking the path:
    # smaller alpha (move toward the beginning: smaller i) -> sparser (fewer vars)
    # larger alpha (toward the end: larger i) -> denser (more vars)
    if max_features and len(cols) > max_features:
        j = i
        while j > 0 and len(cols) > max_features:
            j -= 1
            cols = support_at(j)

    if min_features and len(cols) < min_features:
        j = i
        while j + 1 < len(alphas) and len(cols) < min_features:
            j += 1
            cols = support_at(j)

    # Deduplicate highly correlated; top-up with |corr(y)| if still below min_features
    cols = dedup_by_corr(Xtr, cols, dedup_tau)

    if min_features and len(cols) < min_features:
        # Use corrwith for a clear Series return type (type-checker friendly)
        rank = Xtr.corrwith(ytr).abs().sort_values(ascending=False)
        for c in rank.index:
            if c in cols:
                continue
            cols.append(c)
            cols = dedup_by_corr(Xtr, cols, dedup_tau)
            if len(cols) >= min_features:
                break

    if max_features and len(cols) > max_features:
        cols = cols[:max_features]

    meta = write_meta(
        panel,
        tag,
        "lars",
        {
            "cv": int(cv),
            "alpha_cv": float(estimator.alpha_),
            "path_len": int(len(alphas)),
            "min_features": min_features,
            "max_features": max_features,
            "dedup_tau": dedup_tau,
        },
        cols,
    )
    outp = write_panel(panel, tag, "lars", X_full, cols) if write_panel_flag else None
    msg = f"{panel} {tag} LARS: selected {len(cols)} vars; meta={meta}"
    if outp:
        msg += f"; panel={outp}"
    print(msg)
    return cols

# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cfg", default=None)
    ap.add_argument("--cv", type=int, default=10)
    ap.add_argument("--max_features", type=int, default=0)
    ap.add_argument("--min_features", type=int, default=0)
    ap.add_argument("--dedup_tau", type=float, default=0.98)
    ap.add_argument("--write_panel", action="store_true")
    args = ap.parse_args()

    yml = load_yaml()
    mf = yml["dates"]["monthly_freq"]
    min_obs = int(yml["preprocessing"]["min_obs_train_months"])
    cfgs = [args.cfg] if args.cfg else list(yml.get("backtest", {}).get("configs", {}).values())

    for c in cfgs:
        run_lars(
            c,
            monthly_freq=mf,
            cv=args.cv,
            max_features=args.max_features,
            min_obs_train=min_obs,
            write_panel_flag=args.write_panel,
            min_features=args.min_features,
            dedup_tau=args.dedup_tau,
        )


