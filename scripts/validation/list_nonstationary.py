#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import json

import pandas as pd

from dfm_pipeline.validation.stationarity import run_stationarity_tests_on_panel


def load_panel(csv_path: Path, date_col: str = "sasdate") -> pd.DataFrame:
    df = pd.read_csv(csv_path, low_memory=False)
    if date_col not in df.columns:
        # robust fallback: try first column
        first = df.columns[0]
        if pd.to_datetime(df[first], errors="coerce").notna().mean() > 0.9:
            date_col = first
        else:
            raise ValueError(f"Date column '{date_col}' not found in {csv_path}.")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
    # numeric only
    return df.select_dtypes(include="number")


def union_selected_from_meta(meta_paths: list[Path]) -> list[str]:
    names: set[str] = set()
    for mp in meta_paths:
        d = json.loads(Path(mp).read_text(encoding="utf-8"))
        for s in d.get("selected", []):
            names.add(s)
    return sorted(names)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="List names of non-stationary series (by ADF+KPSS rule) for one or more panels."
    )
    ap.add_argument(
        "--csvs",
        nargs="+",
        required=True,
        help="Panel CSVs (e.g., data/processed_data/stationarity/panel_start_1960_drop_late_fixhousing_delta.csv ...)",
    )
    ap.add_argument(
        "--subset-meta",
        nargs="*",
        default=[],
        help="Optional metadata JSON(s) to restrict columns (SIS/tstat/LARS). You can pass multiple; the union is used.",
    )
    ap.add_argument("--date-col", default="sasdate", help="Date column name (default: sasdate).")
    ap.add_argument("--start", default=None, help="Optional start date YYYY-MM-DD.")
    ap.add_argument("--end", default=None, help="Optional end date YYYY-MM-DD.")
    ap.add_argument(
        "--kpss-reg", choices=["c", "ct"], default="c",
        help="KPSS regression: 'c' level (default) or 'ct' level+trend."
    )
    ap.add_argument("--alpha", type=float, default=0.05, help="Significance level (default 0.05).")
    args = ap.parse_args()

    # Build optional subset (union across all provided meta files)
    subset_cols: list[str] | None = None
    if args.subset_meta:
        subset_cols = union_selected_from_meta([Path(m) for m in args.subset_meta])
        if not subset_cols:
            print("[warn] No 'selected' names found in provided metadata; proceeding with all columns.")

    for csv_path in args.csvs:
        p = Path(csv_path)
        X = load_panel(p, date_col=args.date_col)
        if args.start or args.end:
            X = X.loc[args.start:args.end]
        if subset_cols:
            keep = [c for c in subset_cols if c in X.columns]
            if not keep:
                print(f"[{p.name}] No overlap between subset and panel columns; skipping.")
                continue
            X = X.loc[:, keep]

        # Run tests (ADF+KPSS). Extras aren't needed here since we just want the ADF+KPSS decision.
        R = run_stationarity_tests_on_panel(
            X,
            kpss_reg=args.kpss_reg,
            alpha=args.alpha,
            run_pp=False,
            run_dfgls=False,
            run_za=False,
        )

        # Non-stationary by the rule: ADF not reject & KPSS reject
        nonstat = R.index[(~(R["adf_pvalue"] < args.alpha)) & (R["kpss_pvalue"] < args.alpha)].tolist()

        print(f"\n=== {p.name} ===")
        if args.start or args.end:
            idx = X.index
            # ASCII-safe arrow to avoid Windows cp1252 issues
            print(f"Window: {idx.min().date()} -> {idx.max().date()}  rows={len(X)}")
        total = X.shape[1]
        print(f"Series tested: {total}")
        print(f"Non-stationary (ADF not reject & KPSS reject) = {len(nonstat)}")
        if nonstat:
            for s in sorted(nonstat):
                print(s)
        else:
            print("(none)")


if __name__ == "__main__":
    main()
