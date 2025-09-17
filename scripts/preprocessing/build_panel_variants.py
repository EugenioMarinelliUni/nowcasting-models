#!/usr/bin/env python3
# scripts/preprocessing/build_panel_variants.py

from pathlib import Path
import argparse

from dfm_pipeline.preprocessing.panel_variants import (
    ensure_monthly_panel,
    build_variant,
    DATE_COL_DEFAULT,
    DATE_FMT_DEFAULT,
)

DEFAULT_IN_CSV = Path("data/processed_data/panel_transformed.csv")

# Two default variants
DEFAULT_VARIANTS = [
    {"name": "panel_start_1965_keep_late", "anchor_start": "01/01/1965"},
    {"name": "panel_start_1960_drop_late", "anchor_start": "01/01/1960"},
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build panel variants from a monthly CSV (mm/dd/yyyy)."
    )
    p.add_argument(
        "--in-csv",
        type=Path,
        default=DEFAULT_IN_CSV,
        help=f"Input CSV with monthly data (default: {DEFAULT_IN_CSV})",
    )
    p.add_argument(
        "--date-col",
        default=DATE_COL_DEFAULT,
        help=f"Date column name (default: {DATE_COL_DEFAULT})",
    )
    p.add_argument(
        "--date-fmt",
        default=DATE_FMT_DEFAULT,
        help=f"Date format in CSV (default: {DATE_FMT_DEFAULT})",
    )
    p.add_argument(
        "--variants",
        nargs="*",
        metavar="NAME=ANCHOR",
        help=(
            "Optional overrides for variants, e.g.: "
            "--variants panelA=01/01/1965 panelB=01/01/1960"
        ),
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Load and enforce contiguous monthly index
    df_all = ensure_monthly_panel(args.in_csv, date_col=args.date_col, date_fmt=args.date_fmt)

    # Build the variant list (either defaults or user overrides)
    if args.variants:
        variants = []
        for item in args.variants:
            if "=" not in item:
                raise SystemExit(f"Invalid --variants item '{item}'. Use NAME=MM/DD/YYYY.")
            name, anchor = item.split("=", 1)
            variants.append({"name": name, "anchor_start": anchor})
    else:
        variants = DEFAULT_VARIANTS

    for v in variants:
        meta = build_variant(
            df_all=df_all,
            name=v["name"],
            anchor_start=v["anchor_start"],
            in_csv_path=args.in_csv,
        )

        arts = meta.get("artifacts", {})
        # Prefer new keys; fall back to old single-key if ever present
        out_idx = arts.get("panel_csv_index") or arts.get("panel_csv") or "<n/a>"
        out_col = arts.get("panel_csv_with_sasdate_col") or "<n/a>"

        # ASCII-only output to avoid Windows console encoding issues
        print(
            f"[{meta['variant']}] rows={meta['n_rows']} cols={meta['n_series']} "
            f"TRAIN=({meta['train_window'][0]} -> {meta['train_window'][1]}) "
            f"OUT_index={out_idx} OUT_sasdate_col={out_col}"
        )


if __name__ == "__main__":
    main()


