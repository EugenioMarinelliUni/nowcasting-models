#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

import pandas as pd


def load_selected_from_meta(meta_paths: List[Path]) -> list[str]:
    """
    Union of 'selected' names across one or more preselection metadata JSONs.
    Each JSON must have a top-level key 'selected' as a list of series names.
    """
    names: set[str] = set()
    for p in meta_paths:
        d = json.loads(p.read_text(encoding="utf-8"))
        for s in d.get("selected", []):
            names.add(str(s))
    return sorted(names)


def load_panel_with_date(path: Path) -> tuple[pd.DataFrame, str]:
    """
    Load a monthly panel CSV, detect a date column, and return (df, date_col_name).
    Does NOT set the index; keeps date as a column to mirror original format.
    """
    df = pd.read_csv(path, low_memory=False)
    date_col: str | None = None

    # Try common names first
    for cand in ["Date", "date", "sasdate"]:
        if cand in df.columns:
            date_col = cand
            break

    if date_col is None:
        # fallback: try first column as dates
        first = df.columns[0]
        dt = pd.to_datetime(df[first], errors="coerce")
        if dt.notna().mean() > 0.9:
            date_col = first
        else:
            raise ValueError(
                f"No obvious date column in {path}. "
                f"Columns: {list(df.columns)[:8]}"
            )

    return df, date_col


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Build a reduced full panel by restricting columns to a selection "
            "defined in one or more __selected.json metadata files."
        )
    )
    ap.add_argument(
        "--full-panel",
        required=True,
        help=(
            "Input full standardized panel CSV, e.g. "
            "dataset/1960_noVIX_TEST/full_panels/1960_noVIX_TEST__full_1990_01_2025_04.csv"
        ),
    )
    ap.add_argument(
        "--selection-meta",
        required=True,
        nargs="+",
        help=(
            "One or more JSON metadata files with a 'selected' list, e.g. "
            "data/metadata/variants_TEST_v2/direct_vote_...__selected.json"
        ),
    )
    ap.add_argument(
        "--out",
        default=None,
        help=(
            "Optional explicit output CSV. If set, this path is used exactly. "
            "If omitted, an automatic name is used based on --full-panel, "
            "--tag, and optionally --out-dir."
        ),
    )
    ap.add_argument(
        "--tag",
        default="selected",
        help=(
            "Tag to insert into the auto output name when --out is omitted, "
            "e.g. 'vote' -> <stem>__vote_reduced.csv. Default: 'selected'."
        ),
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Optional base directory for auto output. If set (and --out is not), "
            "the file is written under <out-dir>/<tag>/<stem>__<tag>_reduced.csv. "
            "If not set, the directory of --full-panel is used."
        ),
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    full_path = Path(args.full_panel)
    if not full_path.exists():
        raise FileNotFoundError(f"Full panel not found: {full_path}")

    meta_paths = [Path(p) for p in args.selection_meta]
    for p in meta_paths:
        if not p.exists():
            raise FileNotFoundError(f"Selection meta JSON not found: {p}")

    selected = load_selected_from_meta(meta_paths)
    if not selected:
        raise ValueError("No selected variables found in the provided metadata JSON(s).")

    df, date_col = load_panel_with_date(full_path)

    # Build list of columns to keep: date + selected series that exist in the panel
    keep_vars = [c for c in df.columns if c in selected]
    if not keep_vars:
        raise ValueError(
            "None of the selected variables are present in the full panel.\n"
            f"First few selected: {selected[:10]}\n"
            f"First few columns: {list(df.columns)[:10]}"
        )

    keep_cols = [date_col] + keep_vars

    df_red = df.loc[:, keep_cols]

    # Decide output path
    if args.out is not None:
        out_path = Path(args.out)
    else:
        stem = full_path.stem
        tag = args.tag.strip() or "selected"
        out_name = f"{stem}__{tag}_reduced.csv"

        if args.out_dir:
            base_dir = Path(args.out_dir) / tag
        else:
            base_dir = full_path.parent

        base_dir.mkdir(parents=True, exist_ok=True)
        out_path = base_dir / out_name

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_red.to_csv(out_path, index=False)

    print(
        f"[OK] wrote reduced panel: {out_path}\n"
        f"    original shape={df.shape}, reduced shape={df_red.shape}\n"
        f"    kept {len(keep_vars)} variables + date column"
    )


if __name__ == "__main__":
    main()
