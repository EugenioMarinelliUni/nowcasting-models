#!/usr/bin/env python3
# Location: scripts/preselection/run_preselection.py
from __future__ import annotations
from pathlib import Path
import argparse, yaml, sys

HERE = Path(__file__).resolve()
ROOT = HERE.parents[2]        # project root (…/dfm_project_final/)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

def load_yaml(path: str = "config.yaml"):
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def run_one(method: str, cfg_path: str, mfreq: str,
            sis_tau: float, sis_topn: int,
            tstat_alpha: float, tstat_topn: int,
            min_features: int, max_features: int, dedup_tau: float,
            cv: int, write_panel: bool, min_obs: int,
            # new:
            y_ar_lags: int, x_leads: int, x_lags: int):
    if method in ("sis", "all"):
        from scripts.preselection.preselect_sis import run_sis
        run_sis(cfg_path, monthly_freq=mfreq,
                tau_sis=sis_tau, top_n=sis_topn,
                min_obs_train=min_obs, write_panel_flag=write_panel,
                min_features=min_features, max_features=max_features, dedup_tau=dedup_tau,
                x_leads=x_leads, x_lags=x_lags)
    if method in ("tstat", "all"):
        from scripts.preselection.preselect_tstat import run_tstat
        run_tstat(cfg_path, monthly_freq=mfreq,
                  alpha_t=tstat_alpha, top_n=tstat_topn,
                  min_obs_train=min_obs, write_panel_flag=write_panel,
                  min_features=min_features, max_features=max_features, dedup_tau=dedup_tau,
                  y_ar_lags=y_ar_lags, x_leads=x_leads, x_lags=x_lags)
    if method in ("lars", "all"):
        from scripts.preselection.preselect_lars import run_lars
        run_lars(cfg_path, monthly_freq=mfreq, cv=cv,
                 max_features=max_features, min_obs_train=min_obs,
                 write_panel_flag=write_panel, min_features=min_features,
                 dedup_tau=dedup_tau)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--method", choices=["sis","tstat","lars","all"], default="all")
    ap.add_argument("--cfg", default=None, help="single backtest config; default: iterate all in config.yaml")
    ap.add_argument("--sis_tau", type=float, default=0.15)
    ap.add_argument("--sis_topn", type=int, default=0)
    ap.add_argument("--tstat_alpha", type=float, default=0.05)
    ap.add_argument("--tstat_topn", type=int, default=0)
    ap.add_argument("--min_features", type=int, default=30)
    ap.add_argument("--max_features", type=int, default=80)
    ap.add_argument("--dedup_tau", type=float, default=0.98)
    ap.add_argument("--cv", type=int, default=10)
    ap.add_argument("--y_ar_lags", type=int, default=4, help="AR lags of y used in t-stat screen")
    ap.add_argument("--x_leads", type=int, default=0, help="allow up to this many leads of x for SIS/t-stat")
    ap.add_argument("--x_lags", type=int, default=0, help="allow up to this many lags of x for SIS/t-stat")
    ap.add_argument("--write_panel", action="store_true")
    args = ap.parse_args()

    yml = load_yaml()
    mf = yml["dates"]["monthly_freq"]
    min_obs = int(yml["preprocessing"]["min_obs_train_months"])
    cfgs = [args.cfg] if args.cfg else list(yml.get("backtest", {}).get("configs", {}).values())

    for cfg in cfgs:
        run_one(args.method, cfg, mf,
                args.sis_tau, args.sis_topn,
                args.tstat_alpha, args.tstat_topn,
                args.min_features, args.max_features, args.dedup_tau,
                args.cv, args.write_panel, min_obs,
                args.y_ar_lags, args.x_leads, args.x_lags)

