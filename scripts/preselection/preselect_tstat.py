#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
from dfm_pipeline.preselection.selectors import run_tstat

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cfg", required=True)
    ap.add_argument("--alpha_t", type=float, default=0.0)
    ap.add_argument("--top_n", type=int, default=0)
    ap.add_argument("--min_features", type=int, default=0)
    ap.add_argument("--max_features", type=int, default=0)
    ap.add_argument("--dedup_tau", type=float, default=0.98)
    ap.add_argument("--write_panel", action="store_true")
    args = ap.parse_args()

    cfg = json.loads(Path(args.cfg).read_text(encoding="utf-8"))
    panel = cfg["panel_id"]
    ts, te = cfg["time_spans"]["train"]["start"], cfg["time_spans"]["train"]["end"]
    tag = f"train{ts[:4]}_{te[:4]}"

    run_tstat(panel, tag,
              alpha=args.alpha_t, top_n=args.top_n,
              min_features=args.min_features, max_features=args.max_features,
              dedup_tau=args.dedup_tau)

if __name__ == "__main__":
    main()

