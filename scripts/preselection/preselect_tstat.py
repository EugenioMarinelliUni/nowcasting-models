#!/usr/bin/env python3
# Location: scripts/preselection/preselect_tstat.py
from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Dict, Literal
import argparse, json, yaml
import pandas as pd
import numpy as np

try:
    from scipy.stats import t as tdist  # optional; falls back to normal approx
    _SCIPY = True
except Exception:
    _SCIPY = False

# ---------------------------
# Utilities (self-contained)
# ---------------------------
def load_yaml(path: str = "config.yaml") -> Dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def load_cfg(cfg_path: str) -> Dict:
    return json.loads(Path(cfg_path).read_text(encoding="utf-8"))

def ms_index(idx: pd.Index, monthly_freq: str) -> pd.DatetimeIndex:
    """Normalize any datetime-like index to month-start or month-end timestamps."""
    ms = str(monthly_freq).upper() == "MS"
    di = pd.DatetimeIndex(pd.to_datetime(idx, errors="coerce"))
    how: Literal["start", "end"] = "start" if ms else "end"
    return di.to_period("M").to_timestamp(how=how)

def load_Xy(panel_csv: Path, target_csv: Path, monthly_freq: str) -> Tuple[pd.DataFrame, pd.Series]:
    X = pd.read_csv(panel_csv, parse_dates=["Date"]).set_index("Date").sort_index()
    y = pd.read_csv(target_csv, parse_dates=["Date"]).set_index("Date").sort_index()
    X.index = ms_index(X.index, monthly_freq); X = X.apply(pd.to_numeric, errors="coerce")
    y.index = ms_index(y.index, monthly_freq); y_series = y.iloc[:, 0].astype(float)
    XY = X.join(y_series.to_frame("y"), how="inner")
    return XY.drop(columns=["y"]), XY["y"]

def slice_train(X: pd.DataFrame, y: pd.Series, start: str, end: str) -> Tuple[pd.DataFrame, pd.Series]:
    return X.loc[start:end], y.loc[start:end]

def train_impute(Xtr: pd.DataFrame, min_obs: int = 36) -> pd.DataFrame:
    ok = Xtr.notna().sum() >= int(min_obs)
    Xtr = Xtr.loc[:, ok]
    return Xtr.fillna(Xtr.mean())

def dedup_by_corr(Xtr: pd.DataFrame, cols: List[str], tau: float) -> List[str]:
    if len(cols) <= 1: return cols
    C = Xtr[cols].corr().abs()
    keep, seen = [], set()
    for c in cols:
        if c in seen: continue
        keep.append(c)
        dup = C.index[(C[c] >= tau) & (C.index != c)]
        seen.update(dup)
    return keep

def write_meta(panel: str, tag: str, method: str, params: Dict, selected: List[str]) -> Path:
    out = Path("data/metadata/variants"); out.mkdir(parents=True, exist_ok=True)
    p = out / f"{panel}__{tag}__preselect_{method}.json"
    p.write_text(json.dumps({"method": method,
                             "panel_id": panel,
                             "train_tag": tag,
                             "params": params,
                             "selected": selected}, indent=2))
    return p

def write_panel(panel: str, tag: str, method: str, X_full: pd.DataFrame, cols: List[str]) -> Path:
    out = Path(f"dataset/{panel}/preselect/{method}")
    out.mkdir(parents=True, exist_ok=True)
    fp = out / f"X_panel_z__{panel}__{tag}__preselect-{method}.csv"
    X_full.loc[:, cols].to_csv(fp, index_label="Date")
    return fp

def build_shifted_columns(X: pd.DataFrame, x_leads: int, x_lags: int) -> pd.DataFrame:
    out = {}
    for c in X.columns:
        out[c] = X[c]
        for k in range(1, int(x_lags)+1):
            out[f"{c}__L{k}"] = X[c].shift(k)
        for k in range(1, int(x_leads)+1):
            out[f"{c}__F{k}"] = X[c].shift(-k)
    return pd.DataFrame(out, index=X.index)

def add_y_ar_lags(y: pd.Series, p: int) -> pd.DataFrame:
    Y = pd.DataFrame({"y": y})
    for k in range(1, int(p)+1):
        Y[f"y_lag{k}"] = y.shift(k)
    return Y

# ---------------------------
# Core
# ---------------------------
def run_tstat(cfg_path: str,
              monthly_freq: str,
              alpha_t: float = 0.0,     # 0 => no p-value filter
              top_n: int = 0,           # 0 => no cap
              min_obs_train: int = 36,
              write_panel_flag: bool = True,
              min_features: int = 0,
              max_features: int = 0,
              dedup_tau: float = 0.98,
              y_ar_lags: int = 4,
              x_leads: int = 0,
              x_lags: int = 0) -> List[str]:

    cfg = load_cfg(cfg_path); panel = cfg["panel_id"]
    ts, te = cfg["time_spans"]["train"]["start"], cfg["time_spans"]["train"]["end"]
    tag = f"train{ts[:4]}_{te[:4]}"

    Xp = Path(f"dataset/{panel}/baseline/X_panel_z__{panel}__{tag}.csv")
    yp = Path(f"dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv")
    X_full, y_full = load_Xy(Xp, yp, monthly_freq)
    Xtr0, ytr0 = slice_train(X_full, y_full, ts, te)

    # keep only months with y and impute X on train (before shifting)
    m_y = ytr0.notna()
    Xtr0, ytr = Xtr0.loc[m_y], ytr0.loc[m_y]
    Xtr0 = train_impute(Xtr0, min_obs=min_obs_train)

    # build shifted X and AR lags of y; align indices (drop rows with missing lags)
    Xtr_sh = build_shifted_columns(Xtr0, x_leads=x_leads, x_lags=x_lags)
    Y_df = add_y_ar_lags(ytr, p=y_ar_lags).dropna()
    Xtr = Xtr_sh.loc[Y_df.index].dropna(how="all", axis=1)
    y_al = Y_df["y"].values
    Z = Y_df[[c for c in Y_df.columns if c != "y"]].values  # AR controls

    # compute t-stat for coef on each x in y ~ x + AR(p)
    stats = []
    for col in Xtr.columns:
        x = Xtr[col].values.reshape(-1, 1)
        M = np.column_stack([x, Z])
        if M.shape[0] < M.shape[1] + 5:  # guard: need some dof
            stats.append((col, 0.0, 1.0)); continue
        beta, *_ = np.linalg.lstsq(M, y_al, rcond=None)
        resid = y_al - M @ beta
        n, k = M.shape
        s2 = float((resid @ resid) / max(1, n - k))
        XtX = M.T @ M
        try:
            XtX_inv = np.linalg.inv(XtX)
        except np.linalg.LinAlgError:
            # ridge jitter
            lam = 1e-10
            XtX_inv = np.linalg.inv(XtX + lam * np.eye(k))
        se_x = float(np.sqrt(s2 * XtX_inv[0, 0]))
        tval = 0.0 if se_x == 0 else float(beta[0] / se_x)
        if _SCIPY:
            pval = 2 * (1 - tdist.cdf(abs(tval), df=max(1, n - k)))
        else:
            from math import erf
            z = abs(float(tval))
            pval = 2 * (1 - 0.5*(1 + erf(z / np.sqrt(2))))
        stats.append((col, tval, pval))

    T = pd.DataFrame(stats, columns=["var", "t", "p"]).set_index("var")
    T["abs_t"] = T["t"].abs()

    # p-value screen first (if requested)
    if alpha_t and alpha_t > 0:
        T = T[T["p"] <= alpha_t]

    # rank by |t|
    T = T.sort_values("abs_t", ascending=False)
    sel = T.index.tolist()

    # optional cap
    if top_n and top_n > 0 and len(sel) > top_n:
        sel = sel[:top_n]

    # dedup and guardrails (use train covariances of selected set)
    Xtr_sel = Xtr.loc[:, sel] if len(sel) else Xtr.iloc[:, :0]
    cols = dedup_by_corr(Xtr_sel, sel, dedup_tau)

    if min_features and len(cols) < min_features:
        for c in T.index:
            if c in cols: continue
            cols.append(c)
            cols = dedup_by_corr(Xtr.loc[:, cols], cols, dedup_tau)
            if len(cols) >= min_features: break

    if max_features and len(cols) > max_features:
        cols = cols[:max_features]

    # Build full panel with identical shifted columns for output
    X_full_sh = build_shifted_columns(X_full, x_leads=x_leads, x_lags=x_lags)
    X_out = X_full_sh.loc[:, cols]

    meta = write_meta(panel, tag, "tstat",
                      {"alpha_t": alpha_t, "top_n": top_n,
                       "min_features": min_features, "max_features": max_features,
                       "dedup_tau": dedup_tau, "y_ar_lags": int(y_ar_lags),
                       "x_leads": int(x_leads), "x_lags": int(x_lags)},
                      cols)
    outp = write_panel(panel, tag, "tstat", X_out, cols) if write_panel_flag else None
    msg = f"{panel} {tag} t-stat+AR({y_ar_lags}): selected {len(cols)} vars; meta={meta}"
    if outp: msg += f"; panel={outp}"
    print(msg)
    return cols

# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cfg", default=None)
    ap.add_argument("--alpha_t", type=float, default=0.0, help="two-sided p-value filter; 0 means no filter")
    ap.add_argument("--top_n", type=int, default=0, help="optional cap; 0 means no cap")
    ap.add_argument("--min_features", type=int, default=0)
    ap.add_argument("--max_features", type=int, default=0)
    ap.add_argument("--dedup_tau", type=float, default=0.98)
    ap.add_argument("--y_ar_lags", type=int, default=4)
    ap.add_argument("--x_leads", type=int, default=0)
    ap.add_argument("--x_lags", type=int, default=0)
    ap.add_argument("--write_panel", action="store_true")
    args = ap.parse_args()

    yml = load_yaml(); mf = yml["dates"]["monthly_freq"]
    min_obs = int(yml["preprocessing"]["min_obs_train_months"])
    cfgs = [args.cfg] if args.cfg else list(yml.get("backtest", {}).get("configs", {}).values())
    for c in cfgs:
        run_tstat(c, monthly_freq=mf, alpha_t=args.alpha_t, top_n=args.top_n,
                  min_obs_train=min_obs, write_panel_flag=args.write_panel,
                  min_features=args.min_features, max_features=args.max_features,
                  dedup_tau=args.dedup_tau, y_ar_lags=args.y_ar_lags,
                  x_leads=args.x_leads, x_lags=args.x_lags)

