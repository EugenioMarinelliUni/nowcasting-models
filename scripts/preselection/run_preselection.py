#!/usr/bin/env python3
"""
Run variable preselection (SIS / t-stat / LARS) for a single {panel, tag} specified
via a backtest config JSON.

Expected inputs (created earlier in your pipeline):
  dataset/{panel}/baseline/X_panel_z__{panel}__{tag}.csv
  dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv

Outputs (per method):
  data/metadata/variants/{panel}__{tag}__preselect_{METHOD}.json
  dataset/{panel}/preselect/{method}/X_panel_z__{panel}__{tag}__preselect-{method}.csv
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any

import pandas as pd

# Core logic lives in src/dfm_pipeline/preselection/selectors.py
from dfm_pipeline.preselection.selectors import (
    run_sis,
    run_tstat,
    run_lars,
)


def _read_cfg(cfg_path: Path) -> Dict[str, Any]:
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    try:
        return json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"Failed to parse JSON in {cfg_path}: {e}") from e


def _make_tag(cfg: Dict[str, Any]) -> str:
    ts = cfg["time_spans"]["train"]["start"]
    te = cfg["time_spans"]["train"]["end"]
    return f"train{ts[:4]}_{te[:4]}"


def _print_brief_header(panel: str, tag: str, cfg_path: Path) -> None:
    print(f"[preselection] panel={panel}  tag={tag}  cfg={cfg_path}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run SIS / t-stat / LARS preselection for one {panel, tag}."
    )
    ap.add_argument("--cfg", required=True, help="Backtest config JSON (with panel_id and train span)")
    ap.add_argument("--method", choices=["sis", "tstat", "lars", "all"], default="all")

    # Common guardrails
    ap.add_argument("--min_features", type=int, default=30)
    ap.add_argument("--max_features", type=int, default=80)
    ap.add_argument("--dedup_tau", type=float, default=0.98)

    # SIS knobs
    ap.add_argument("--sis_tau", type=float, default=0.0, help="Abs(corr) threshold; 0 means no threshold")
    ap.add_argument("--sis_topn", type=int, default=0, help="Cap; 0 means no cap")

    # t-stat knobs
    ap.add_argument("--tstat_alpha", type=float, default=0.0, help="Two-sided p-value filter; 0=no filter")
    ap.add_argument("--tstat_topn", type=int, default=0, help="Cap; 0 means no cap")

    # LARS knobs
    ap.add_argument("--cv", type=int, default=10, help="K-fold CV for LassoLarsCV")

    # Kept for backward CLI parity (selectors already write outputs when they run)
    ap.add_argument("--write_panel", action="store_true", help="(No-op; outputs are written by default)")

    args = ap.parse_args()

    cfg_path = Path(args.cfg)
    cfg = _read_cfg(cfg_path)
    panel = cfg["panel_id"]
    tag = _make_tag(cfg)

    _print_brief_header(panel, tag, cfg_path)

    # Optional quick existence sanity before calling selectors (nice UX)
    x_path = Path(f"dataset/{panel}/baseline/X_panel_z__{panel}__{tag}.csv")
    y_path = Path(f"dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv")
    for p in (x_path, y_path):
        if not p.exists():
            raise FileNotFoundError(f"Required file missing: {p}")

    # Light index-alignment sanity (fail fast with a clear error)
    try:
        X = pd.read_csv(x_path, parse_dates=["Date"]).set_index("Date").sort_index()
        y = pd.read_csv(y_path, parse_dates=["Date"]).set_index("Date").sort_index().iloc[:, 0]
        if not X.index.equals(y.index):
            raise ValueError("X and y indices differ. Make sure both are aligned on the same monthly index.")
        if y.notna().sum() == 0:
            raise ValueError("No non-NaN target values in the train index; cannot run preselection.")
    except Exception as e:
        raise RuntimeError(f"Sanity check failed for {panel} {tag}: {e}") from e

    common_kwargs = dict(
        min_features=int(args.min_features),
        max_features=int(args.max_features),
        dedup_tau=float(args.dedup_tau),
    )

    # Run the requested method(s)
    if args.method in ("sis", "all"):
        print("[SIS] running…")
        run_sis(
            panel,
            tag,
            tau=float(args.sis_tau),
            top_n=int(args.sis_topn),
            **common_kwargs,
        )
    if args.method in ("tstat", "all"):
        print("[t-stat] running…")
        run_tstat(
            panel,
            tag,
            alpha=float(args.tstat_alpha),
            top_n=int(args.tstat_topn),
            **common_kwargs,
        )
    if args.method in ("lars", "all"):
        print("[LARS] running…")
        run_lars(
            panel,
            tag,
            cv=int(args.cv),
            **common_kwargs,
        )

    print("[preselection] done.")


if __name__ == "__main__":
    main()
