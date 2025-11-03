#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Tuple, Optional

import pandas as pd


def _eprint(msg: str) -> None:
    print(f"ERROR: {msg}")


def _wprint(msg: str) -> None:
    print(f"WARNING: {msg}")


def _okprint(msg: str) -> None:
    print(f"OK: {msg}")


# ---------- Heuristics & defaults ----------

# Fallback group -> (type, rule) if we have no tcode and no name hint
GROUP_DEFAULT: Dict[str, Tuple[str, str]] = {
    "Housing": ("flow", "sum3m"),
    "Prices": ("return", "sum3m"),  # many price series used as diffs/returns; adjust if levels
    "Interest Rates": ("level", "mean3m"),
    "Yield Spread": ("level", "mean3m"),
    "Stock Market": ("level", "mean3m"),
    "Exchange Rates": ("level", "mean3m"),
    "Labor Market": ("level", "mean3m"),
    "Money & Credit": ("level", "mean3m"),
    "Output & Income": ("level", "mean3m"),
    "Consumption / Inventories": ("level", "mean3m"),
}

# Name-based hints (regex -> (type, rule)); used only when tcode is missing
NAME_HINTS = [
    (re.compile(r"(HOUST|PERMIT|NEWORD|ACOGNO|AMDMNO|AMTMNO|AMTMVS)\b", re.I), ("flow", "sum3m")),
    (re.compile(r"(PAYEMS.*D1|PAYEMS_CHG|PAYNSA.*CHG)\b", re.I), ("flow", "sum3m")),
    (re.compile(r"(RET.*|PRS.*|SALES)\b", re.I), ("flow", "sum3m")),
    (re.compile(r"(DLOG|_RET|_CHG|_G|_GR|_DIFF)\b", re.I), ("return", "sum3m")),  # log/return-like
    (re.compile(r"(FEDFUNDS|GS\d+|T\d+Y\d+Y|BAA|AAA|SPREAD)\b", re.I), ("level", "mean3m")),
    (re.compile(r"(VIX)\b", re.I), ("level", "mean3m")),
]


def guess_type_rule(series: str, group: Optional[str], tcode: Optional[int]) -> Tuple[str, str]:
    """
    Decide (type, rule) with strict precedence:
      1) tcode in {2..7}  -> return/sum3m  (changes/log-changes add across months)
      2) tcode == 1       -> level/mean3m  (levels: quarter's typical level)
      3) name hints       -> as matched (when tcode missing)
      4) group defaults   -> fallback by economic group
      5) final fallback   -> level/mean3m
    """
    # 1) strict tcode handling when available
    if tcode in {2, 3, 4, 5, 6, 7}:
        return ("return", "sum3m")
    if tcode == 1:
        return ("level", "mean3m")

    # 2) name hints (only when tcode is missing)
    for pat, tr in NAME_HINTS:
        if pat.search(series):
            return tr

    # 3) group default
    if group and group in GROUP_DEFAULT:
        return GROUP_DEFAULT[group]

    # 4) final fallback
    return ("level", "mean3m")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build per-series aggregation rules (type + rule) for a panel.")
    ap.add_argument("--panel", required=True, help="Panel id, e.g., 1965_withVIX")
    ap.add_argument("--tag", required=True, help="Training tag, e.g., train1990_2019")

    # Optional overrides for metadata locations
    ap.add_argument("--group-map", default="data/metadata/variable_group_map.json",
                    help="Path to variable_group_map.json (optional).")
    ap.add_argument("--tcode-map", default="data/metadata/tcode_map.json",
                    help="Path to tcode_map.json (optional).")

    # Output directory
    ap.add_argument("--outdir", default="data/metadata/aggregation_rules",
                    help="Directory to write {panel}__aggregation_rule_map.json")

    args = ap.parse_args()
    panel, tag = args.panel, args.tag

    # ----- Required input: standardized X (to discover columns) -----
    x_path = Path(f"dataset/{panel}/training_sets/{tag}/standardized_train__{panel}__{tag}.csv")
    if not x_path.is_file():
        _eprint(f"Missing standardized X file: {x_path}")
        _eprint("Tip: run your standardization pipeline for this (panel, tag) first.")
        raise SystemExit(1)

    try:
        cols = pd.read_csv(x_path, nrows=0).columns.tolist()
    except Exception as e:
        _eprint(f"Failed to read columns from {x_path}: {e}")
        raise SystemExit(1)

    if not cols:
        _eprint(f"Standardized X has no columns: {x_path}")
        raise SystemExit(1)

    # --- Drop index-like columns (e.g., 'Date', 'Index', 'Unnamed: 0') ---
    DROP_NAMES = {"date", "index", "time", "period"}
    def _is_index_like(c: str) -> bool:
        s = str(c).strip()
        return (
            s == "" or
            s.lower() in DROP_NAMES or
            s.startswith("Unnamed:") or
            s.lower().startswith("unnamed:")
        )

    orig_n = len(cols)
    cols = [c for c in cols if not _is_index_like(c)]
    dropped_n = orig_n - len(cols)
    _okprint(f"Found {len(cols)} series in {x_path.name}" + (f" (dropped {dropped_n} index-like column(s))" if dropped_n else ""))

    # ----- Optional inputs: group map, tcode map -----
    group_map_path = Path(args.group_map)
    tcode_map_path = Path(args.tcode_map)

    group_map: Dict[str, str] = {}
    tcode_map: Dict[str, int] = {}

    if group_map_path.is_file():
        try:
            group_map = json.loads(group_map_path.read_text(encoding="utf-8"))
            if not isinstance(group_map, dict):
                _wprint(f"{group_map_path} is not a dict; ignoring.")
                group_map = {}
            else:
                _okprint(f"Loaded group map: {group_map_path}")
        except Exception as e:
            _wprint(f"Could not parse group map {group_map_path}: {e} (continuing without it)")
            group_map = {}
    else:
        _wprint(f"No group map found at {group_map_path} (continuing)")

    if tcode_map_path.is_file():
        try:
            tcode_map = json.loads(tcode_map_path.read_text(encoding="utf-8"))
            if not isinstance(tcode_map, dict):
                _wprint(f"{tcode_map_path} is not a dict; ignoring.")
                tcode_map = {}
            else:
                _okprint(f"Loaded tcode map: {tcode_map_path}")
        except Exception as e:
            _wprint(f"Could not parse tcode map {tcode_map_path}: {e} (continuing without it)")
            tcode_map = {}
    else:
        _wprint(f"No tcode map found at {tcode_map_path} (continuing)")

    # ----- Build rules -----
    out = {
        "_meta": {
            "panel": panel,
            "tag": tag,
            "standardized_x": str(x_path),
            "group_map": str(group_map_path) if group_map_path.is_file() else None,
            "tcode_map": str(tcode_map_path) if tcode_map_path.is_file() else None,
            "defaults": {"flow": "sum3m", "level": "mean3m", "return": "sum3m"},
        },
        "series_rules": {}
    }

    for s in cols:
        g = group_map.get(s)
        tc = tcode_map.get(s)
        typ, rule = guess_type_rule(s, g, tc)
        out["series_rules"][s] = {"type": typ, "rule": rule, "group": g}

    # ----- Write output -----
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / f"{panel}__aggregation_rule_map.json"
    try:
        out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    except Exception as e:
        _eprint(f"Failed to write output JSON to {out_path}: {e}")
        raise SystemExit(1)

    _okprint(f"Wrote aggregation rules: {out_path}")


if __name__ == "__main__":
    main()
