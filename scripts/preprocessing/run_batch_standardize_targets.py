#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

# add src/ to path
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.preprocessing.batch_target_standardize import build_targets_for_all  # noqa: E402


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=("Auto-discover all training X panels under dataset/*/training_sets/*/ "
                     "and build standardized targets y for each (aligned to each X index).")
    )
    ap.add_argument("--raw", required=True,
                    help="Raw quarterly target CSV (e.g., data/raw_data/targets/gdp/A191RL1Q225SBEA_latest.csv)")
    ap.add_argument("--monthly-freq", default="MS", choices=["MS", "ME"],
                    help="Normalize to month-start (MS) or month-end (ME). Default: MS.")
    ap.add_argument("--place", default="start", choices=["start", "end"],
                    help="Map quarterly to month at 'start' (Jan/Apr/Jul/Oct) or 'end' (Mar/Jun/Sep/Dec). Default: start.")
    ap.add_argument("--panels", nargs="*", default=None,
                    help="Optional filter: only process these panel names (e.g., 1960_noVIX 1965_withVIX).")
    ap.add_argument("--no-save-stats", action="store_true",
                    help="Do not write __train_stats CSV files.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    raw = Path(args.raw)
    if not raw.exists():
        raise FileNotFoundError(f"Raw target not found: {raw}")

    processed = build_targets_for_all(
        raw_quarterly_csv=raw,
        monthly_freq=args.monthly_freq,
        place=args.place,
        panels=args.panels,
        save_stats=not args.no_save_stats,
    )

    if not processed:
        print("No training sets discovered under dataset/*/training_sets/*/. Nothing to do.")
        return

    # Pretty summary
    print("\nBuilt standardized targets:")
    for r in sorted(processed, key=lambda z: (z.panel, z.tag)):
        print(f"  {r.panel:12}  {r.tag:14}  -> {r.out_path.name}")


if __name__ == "__main__":
    main()
