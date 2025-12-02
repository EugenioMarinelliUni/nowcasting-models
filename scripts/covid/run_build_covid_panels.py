#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Make dfm_pipeline importable when running from repo root
ROOT = Path(__file__).resolve().parents[2]  # .../nowcasting-models
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.covid.covid_make_delete_weights import apply_delete_nan  # noqa: E402
from dfm_pipeline.covid.covid_make_dummies_resid import apply_dummies_resid  # noqa: E402
from dfm_pipeline.covid.covid_make_winsorized import apply_winsor_sigma  # noqa: E402


def load_monthly_panel(path: Path) -> pd.DataFrame:
    """
    Robust loader for a monthly panel:
    - detect a date column among ['Date','date','sasdate'] or first column
      if it looks like dates;
    - set it as DatetimeIndex;
    - keep only numeric columns.
    """
    df = pd.read_csv(path, low_memory=False)
    date_col: str | None = None

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

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(date_col).sort_index()
    df = df.select_dtypes(include="number")
    return df


def sanitize_suffix(start: str, end: str) -> str:
    """
    Build a short suffix based on Covid window, e.g.
    start='2020-03-01', end='2020-09-01' -> 'covid_2020M03_2020M09'
    """
    s = start[:7].replace("-", "M")
    e = end[:7].replace("-", "M")
    return f"covid_{s}_{e}"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Build Covid-adjusted panel variants (delete, dummy-resid, winsor) "
            "from a full standardized monthly panel."
        )
    )
    ap.add_argument(
        "--full-panel",
        required=True,
        help=(
            "Input full standardized panel CSV (e.g. "
            "dataset/1960_noVIX_TEST/full_panels/1960_noVIX_TEST__full_1990_01_2025_04.csv)"
        ),
    )
    ap.add_argument(
        "--covid-start",
        required=True,
        help="Covid window start (YYYY-MM or YYYY-MM-DD), e.g. 2020-03-01.",
    )
    ap.add_argument(
        "--covid-end",
        required=True,
        help="Covid window end (YYYY-MM or YYYY-MM-DD), e.g. 2020-09-01.",
    )
    ap.add_argument(
        "--monthly-freq",
        default="MS",
        choices=["MS", "ME"],
        help="Monthly index convention: MS=month-start (default), ME=month-end.",
    )
    ap.add_argument(
        "--clip-sigma",
        type=float,
        default=6.0,
        help="Winsorization threshold in std dev units for Covid window (default: 6.0).",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Optional output directory. If omitted, uses the directory of --full-panel."
        ),
    )
    ap.add_argument(
        "--suffix",
        default=None,
        help=(
            "Optional suffix to insert before .csv. If omitted, it is built from "
            "--covid-start/--covid-end, e.g. covid_2020M03_2020M09."
        ),
    )
    ap.add_argument(
        "--no-delete",
        action="store_true",
        help="If set, skip the 'covid_delete' variant.",
    )
    ap.add_argument(
        "--no-dummy-resid",
        action="store_true",
        help="If set, skip the 'covid_dummy_resid' variant (and dummy matrix).",
    )
    ap.add_argument(
        "--no-winsor",
        action="store_true",
        help="If set, skip the 'covid_winsor' variant.",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    full_path = Path(args.full_panel)
    if not full_path.exists():
        raise FileNotFoundError(f"Full panel not found: {full_path}")

    X_full = load_monthly_panel(full_path)

    covid_start = args.covid_start
    covid_end = args.covid_end
    monthly_freq = args.monthly_freq

    # Decide output dir and suffix
    out_dir = Path(args.out_dir) if args.out_dir is not None else full_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = args.suffix or sanitize_suffix(covid_start, covid_end)

    # Base stem, e.g. '1960_noVIX_TEST__full_1990_01_2025_04'
    stem = full_path.stem

    if not args.no_delete:
        X_delete = apply_delete_nan(
            X_full,
            covid_start=covid_start,
            covid_end=covid_end,
            monthly_freq=monthly_freq,
        )
        out_delete = out_dir / f"{stem}_covid_delete_{suffix}.csv"
        X_delete.to_csv(out_delete, index_label="Date")
        print(f"[OK] wrote covid_delete panel: {out_delete}")

    if not args.no_dummy_resid:
        X_dummy, D = apply_dummies_resid(
            X_full,
            covid_start=covid_start,
            covid_end=covid_end,
            separate=False,
            monthly_freq=monthly_freq,
        )
        out_dummy = out_dir / f"{stem}_covid_dummy_resid_{suffix}.csv"
        out_D = out_dir / f"{stem}_covid_dummy_matrix_{suffix}.csv"
        X_dummy.to_csv(out_dummy, index_label="Date")
        D.to_csv(out_D, index_label="Date")
        print(f"[OK] wrote covid_dummy_resid panel: {out_dummy}")
        print(f"[OK] wrote covid_dummy_matrix: {out_D}")

    if not args.no_winsor:
        X_winsor = apply_winsor_sigma(
            X_full,
            covid_start=covid_start,
            covid_end=covid_end,
            clip_sigma=float(args.clip_sigma),
            monthly_freq=monthly_freq,
        )
        out_winsor = out_dir / f"{stem}_covid_winsor_{suffix}.csv"
        X_winsor.to_csv(out_winsor, index_label="Date")
        print(f"[OK] wrote covid_winsor panel: {out_winsor}")


if __name__ == "__main__":
    main()
