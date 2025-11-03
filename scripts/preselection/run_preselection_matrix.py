#!/usr/bin/env python3
"""
Discover all training sets under dataset/{panel}/training_sets/* and run
scripts/preselection/run_preselection.py for each valid (panel, tag) pair.

Usage (from repo root):
  python scripts/preselection/run_preselection_matrix.py
  python scripts/preselection/run_preselection_matrix.py --panels 1960_noVIX 1965_withVIX --methods all
  python scripts/preselection/run_preselection_matrix.py --min_features 30 --max_features 80 --dedup_tau 0.98 --cv 10
"""

from __future__ import annotations
import argparse
import subprocess
from pathlib import Path
from typing import Iterable, List


def find_tags_for_panel(panel: str) -> List[str]:
    root = Path(f"dataset/{panel}/training_sets")
    if not root.exists():
        return []
    return sorted([p.name for p in root.iterdir() if p.is_dir() and p.name.startswith("train")])


def x_path(panel: str, tag: str) -> Path:
    return Path(f"dataset/{panel}/training_sets/{tag}/standardized_train__{panel}__{tag}.csv")


def y_path(panel: str, tag: str) -> Path:
    return Path(f"dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv")


def run_one(panel: str, tag: str, method: str,
            min_features: int, max_features: int, dedup_tau: float, cv: int,
            dry_run: bool = False) -> int:
    """
    Call the baseline runner script for a single (panel, tag).
    Returns the subprocess return code (0 = success).
    """
    cmd = [
        "python", "scripts/preselection/run_preselection.py",
        "--panel", panel, "--tag", tag,
        "--method", method,
        "--min_features", str(min_features),
        "--max_features", str(max_features),
        "--dedup_tau", str(dedup_tau),
        "--cv", str(cv),
    ]
    print("CMD:", " ".join(cmd))
    if dry_run:
        return 0
    return subprocess.call(cmd)


def main():
    ap = argparse.ArgumentParser(description="Matrix runner for baseline variable preselection.")
    ap.add_argument("--panels", nargs="*", default=["1960_noVIX", "1965_withVIX"],
                    help="Panel IDs to scan (default: 1960_noVIX 1965_withVIX)")
    ap.add_argument("--methods", default="all", choices=["all", "sis", "tstat", "lars"],
                    help="Which preselection method(s) to run (default: all)")
    ap.add_argument("--min_features", type=int, default=30)
    ap.add_argument("--max_features", type=int, default=80)
    ap.add_argument("--dedup_tau", type=float, default=0.98)
    ap.add_argument("--cv", type=int, default=10, help="Only used by LARS")
    ap.add_argument("--dry_run", action="store_true", help="Print commands without executing")
    args = ap.parse_args()

    total = 0
    skipped = 0
    failures = 0

    for panel in args.panels:
        tags = find_tags_for_panel(panel)
        if not tags:
            print(f"[WARN] No training_sets found for panel: {panel}")
            continue
        for tag in tags:
            X = x_path(panel, tag)
            Y = y_path(panel, tag)
            if not X.exists() or not Y.exists():
                print(f"[SKIP] {panel} {tag}")
                print(f"  X exists: {X.exists()}  -> {X}")
                print(f"  y exists: {Y.exists()}  -> {Y}")
                skipped += 1
                continue

            print(f">>> Running preselection for {panel} {tag} (method={args.methods})")
            rc = run_one(panel, tag, args.methods,
                         args.min_features, args.max_features, args.dedup_tau, args.cv,
                         dry_run=args.dry_run)
            total += 1
            if rc != 0:
                print(f"[ERROR] Command failed (rc={rc}) for {panel} {tag}")
                failures += 1

    print("\nSummary:")
    print(f"  attempted : {total}")
    print(f"  skipped   : {skipped}")
    print(f"  failures  : {failures}")
    if failures:
        exit(1)


if __name__ == "__main__":
    main()
