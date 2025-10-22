#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import json

import pandas as pd

# allow `from dfm_pipeline...` when run from repo root
import sys
ROOT = Path(__file__).resolve().parents[2]  # .../dfm_project_final
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.preprocessing.rolling_standardize import (  # noqa: E402
    iter_realtime_rolling_standardized,
)


def load_panel(csv_path: Path, date_col: str = "sasdate") -> pd.DataFrame:
    """Load a panel with a date column, set monthly DateTimeIndex, keep numeric columns."""
    df = pd.read_csv(csv_path, low_memory=False)
    if date_col not in df.columns:
        # try first column as date
        first = df.columns[0]
        if pd.to_datetime(df[first], errors="coerce").notna().mean() > 0.9:
            date_col = first
        else:
            raise ValueError(f"Date column '{date_col}' not found in {csv_path}. "
                             f"Available: {list(df.columns)[:8]}")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
    # numeric only
    return df.select_dtypes(include="number")


def union_selected_from_meta(meta_paths: list[Path]) -> list[str]:
    """Union of 'selected' names across one or more preselection metadata JSONs."""
    names: set[str] = set()
    for p in meta_paths:
        d = json.loads(Path(p).read_text(encoding="utf-8"))
        for s in d.get("selected", []):
            names.add(s)
    return sorted(names)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Rolling-window z-score standardization in pseudo-real-time (monthly cutoffs)."
    )
    ap.add_argument("--csv", required=True,
                    help="Stationarized (NOT standardized) panel CSV (e.g., panel_start_1960_..._delta.csv).")
    ap.add_argument("--date-col", default="sasdate",
                    help="Date column name in CSV (default: sasdate).")
    ap.add_argument("--monthly-freq", default="MS", choices=["MS", "ME"],
                    help="Normalize index to month-start (MS) or month-end (ME). Default: MS.")
    ap.add_argument("--first-cutoff", required=True,
                    help="First cutoff month (YYYY-MM or YYYY-MM-01).")
    ap.add_argument("--last-cutoff", default=None,
                    help="Last cutoff month (YYYY-MM or YYYY-MM-01). If omitted, uses max date in panel.")
    ap.add_argument("--window-months", type=int, default=180,
                    help="Rolling window size in months (default: 180 = 15y).")
    ap.add_argument("--subset-meta", nargs="*", default=[],
                    help="Optional preselection metadata JSON(s); if provided, restrict columns to their union.")
    ap.add_argument("--start", default=None,
                    help="Optional start date for pre-slicing the panel (YYYY-MM-DD).")
    ap.add_argument("--end", default=None,
                    help="Optional end date for pre-slicing the panel (YYYY-MM-DD).")
    ap.add_argument("--return-full-hist", action="store_true",
                    help="If set, return history up to T standardized with rolling stats at T (else: just the window).")
    ap.add_argument("--out-dir", default=None,
                    help="If set, write per-cutoff outputs under this directory.")
    ap.add_argument("--save-what", default="none", choices=["none", "window", "history", "auto"],
                    help="What to save if --out-dir is set: "
                         "'window' (rolling slice), 'history' (full hist up to T), "
                         "'auto' (match --return-full-hist), or 'none' (print only). Default: none.")
    ap.add_argument("--save-stats", action="store_true",
                    help="If set with --out-dir, save a small CSV of mean/std/count per cutoff.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    csv_path = Path(args.csv)
    X = load_panel(csv_path, date_col=args.date_col)

    # optional column subset from preselection metadata
    if args.subset_meta:
        names = union_selected_from_meta([Path(p) for p in args.subset_meta])
        keep = [c for c in names if c in X.columns]
        if not keep:
            print("[warn] No overlap between metadata-selected names and panel columns; proceeding with all columns.")
        else:
            X = X.loc[:, keep]

    # optional date slice before iteration
    if args.start or args.end:
        X = X.loc[args.start:args.end]
        if X.empty:
            raise ValueError("Panel empty after date slicing; check --start/--end.")

    # prepare output
    out_base = None
    if args.out_dir:
        out_base = Path(args.out_dir)
        out_base.mkdir(parents=True, exist_ok=True)
        # make a subfolder per source panel
        out_base = out_base / csv_path.stem
        out_base.mkdir(parents=True, exist_ok=True)

    # choose what to save
    save_mode = args.save_what
    if save_mode == "auto":
        save_mode = "history" if args.return_full_hist else "window"

    # iterate over monthly cutoffs
    gen = iter_realtime_rolling_standardized(
        X_raw=X,
        monthly_freq=args.monthly_freq,
        first_cutoff=args.first_cutoff,
        last_cutoff=args.last_cutoff,
        window_months=int(args.window_months),
        return_full_hist_until_cutoff=bool(args.return_full_hist),
    )

    n = 0
    for cutoff, Z, stats in gen:
        n += 1
        # console summary (ASCII arrow for Windows)
        print(f"{csv_path.name}  cutoff={cutoff.date()}  window={stats.window_start.date()} -> {stats.cutoff.date()}  "
              f"shape={Z.shape}")

        if out_base and save_mode != "none":
            sub = out_base / cutoff.strftime("%Y%m")  # e.g., 201601
            sub.mkdir(parents=True, exist_ok=True)
            # Decide which matrix to save based on mode
            if save_mode == "window":
                fp = sub / f"Z_window_{cutoff.strftime('%Y%m')}.csv"
            elif save_mode == "history":
                fp = sub / f"Z_history_upto_{cutoff.strftime('%Y%m')}.csv"
            else:
                fp = sub / f"Z_{cutoff.strftime('%Y%m')}.csv"
            Z.to_csv(fp, index_label="Date")
            if args.save_stats:
                st = pd.DataFrame({"mean": stats.mean, "std": stats.std, "count": stats.count})
                st.to_csv(sub / f"stats_{cutoff.strftime('%Y%m')}.csv", index_label="series")

    if n == 0:
        print("[warn] no monthly cutoffs iterated; check --first-cutoff/--last-cutoff range.")


if __name__ == "__main__":
    main()
