from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qrf_pipeline.delay_maps import (
    build_delay_map_from_group_map,
    load_group_map,
    write_delay_map,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Create a realistic default ragged-edge delay map.")
    p.add_argument("--group-map-path", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--default-delay", type=int, default=1)
    return p


def main() -> None:
    args = build_parser().parse_args()

    group_map = load_group_map(args.group_map_path)
    delay_map = build_delay_map_from_group_map(
        group_map,
        default_delay=args.default_delay,
    )

    out = write_delay_map(delay_map, args.out)

    print("Saved delay map to:", out)
    print("n variables:", len(delay_map))
    print("Default rule:")
    print("  real/activity/labor/housing/prices: 1-month publication delay")
    print("  financial/interest/spread/stock/exchange-rate series: 0-month delay")


if __name__ == "__main__":
    main()
