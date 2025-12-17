# scripts/covid/run_build_covid_panels.py
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

# Make dfm_pipeline importable when running from repo root
ROOT = Path(__file__).resolve().parents[2]  # .../dfm_project_final (repo root)
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.covid.covid_make_delete_weights import apply_delete_nan  # noqa: E402
from dfm_pipeline.covid.covid_make_dummies_resid import apply_dummies_resid  # noqa: E402
from dfm_pipeline.covid.covid_make_winsorized import apply_winsor_sigma  # noqa: E402

# New Linzenich–Meunier-style variants
from dfm_pipeline.covid.covid_make_dummies_sparse import (  # noqa: E402
    SparseDummySpec,
    append_sparse_covid_dummies,
)
from dfm_pipeline.covid.covid_make_outliers_iqd_nan import (  # noqa: E402
    IQDOutlierSpec,
    apply_outliers_iqd_to_nan,
)


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
    start='2020-03-01', end='2020-09-01' -> 'covid_2020M03_2020M09'
    """
    s = start[:7].replace("-", "M")
    e = end[:7].replace("-", "M")
    return f"covid_{s}_{e}"


def _parse_window(s: Optional[str]) -> Optional[Tuple[str, str]]:
    if s is None:
        return None
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 2:
        raise ValueError("window must be 'YYYY-MM-DD,YYYY-MM-DD'")
    return parts[0], parts[1]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Build Covid-adjusted panel variants (delete, dummy-resid, winsor) "
            "from a full standardized monthly panel.\n"
            "Extended with Linzenich–Meunier-style (sparse dummies, IQD outliers->NaN)."
        )
    )
    ap.add_argument(
        "--full-panel",
        required=True,
        help="Input full (typically standardized) panel CSV.",
    )
    ap.add_argument(
        "--covid-start",
        required=True,
        help="Covid window start (YYYY-MM or YYYY-MM-DD), e.g. 2020-02-01.",
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
        help="Optional output directory. If omitted, uses the directory of --full-panel.",
    )
    ap.add_argument(
        "--suffix",
        default=None,
        help=(
            "Optional suffix to insert before .csv. If omitted, built from "
            "--covid-start/--covid-end, e.g. covid_2020M02_2020M09."
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

    # --- New: Linzenich–Meunier dummy approach (sparse one-month dummies) ---
    ap.add_argument(
        "--lm-dummies",
        action="store_true",
        help="If set, append sparse Covid quarter dummies (LM toolbox style).",
    )
    ap.add_argument(
        "--lm-dummy-mode",
        default="q2q3_2020",
        choices=["q2q3_2020", "q1q2_2020", "custom"],
        help="Sparse dummy pattern: q2q3_2020 => Jun+Sep 2020; q1q2_2020 => Mar+Jun 2020.",
    )
    ap.add_argument(
        "--lm-dummy-months",
        default=None,
        help="Used only if --lm-dummy-mode custom. Example: '2020-06-01,2020-09-01'.",
    )
    ap.add_argument(
        "--lm-dummy-min-nonmissing",
        type=int,
        default=5,
        help="Guard: only set dummy=1 if that month has at least this many observed series (LM toolbox guard).",
    )
    ap.add_argument(
        "--lm-dummy-standardize",
        action="store_true",
        help="If set, standardize dummy columns using --dummy-std-window.",
    )
    ap.add_argument(
        "--dummy-std-window",
        default=None,
        help="Required if --lm-dummy-standardize. Format: 'YYYY-MM-DD,YYYY-MM-DD' (train window).",
    )

    # --- New: Linzenich–Meunier outlier approach (IQD rule -> NaN) ---
    ap.add_argument(
        "--lm-outliers",
        action="store_true",
        help="If set, apply IQD outlier-to-NaN rule (LM toolbox common_outliers(...,0)).",
    )
    ap.add_argument(
        "--lm-outliers-c",
        type=float,
        default=4.0,
        help="Threshold multiplier c in abs(x-median) > c*IQD (LM toolbox default: 4).",
    )
    ap.add_argument(
        "--lm-outliers-min-obs",
        type=int,
        default=20,
        help="Minimum non-missing obs in fit window to compute thresholds for a series.",
    )
    ap.add_argument(
        "--lm-outliers-fit-window",
        default=None,
        help="Fit window for median/IQD to avoid leakage. Format: 'YYYY-MM-DD,YYYY-MM-DD'.",
    )
    ap.add_argument(
        "--lm-outliers-apply-window",
        default=None,
        help="Apply window for replacement. Omit to apply globally. Format: 'YYYY-MM-DD,YYYY-MM-DD'.",
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

    out_dir = Path(args.out_dir) if args.out_dir is not None else full_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = args.suffix or sanitize_suffix(covid_start, covid_end)

    stem = full_path.stem

    # --- Existing variants (backward compatible) ---
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

    # --- New variants: Linzenich–Meunier-style dummies ---
    if args.lm_dummies:
        custom_months = None
        if args.lm_dummy_mode == "custom":
            if args.lm_dummy_months is None:
                raise ValueError("--lm-dummy-months required when --lm-dummy-mode custom")
            custom_months = [m.strip() for m in args.lm_dummy_months.split(",") if m.strip()]

        spec = SparseDummySpec(
            mode=args.lm_dummy_mode,
            custom_months=custom_months,
            ensure_min_nonmissing=int(args.lm_dummy_min_nonmissing),
            prefix="lm_covid_dummy",
        )

        std_window = _parse_window(args.dummy_std_window) if args.lm_dummy_standardize else None
        X_lm_dum = append_sparse_covid_dummies(
            X_full,
            spec=spec,
            standardize_dummies=bool(args.lm_dummy_standardize),
            standardize_over=std_window,
        )
        out_lm_dum = out_dir / f"{stem}_lm_dummies_{args.lm_dummy_mode}_{suffix}.csv"
        X_lm_dum.to_csv(out_lm_dum, index_label="Date")
        print(f"[OK] wrote lm_dummies panel: {out_lm_dum}")

    # --- New variants: Linzenich–Meunier-style IQD outliers -> NaN ---
    if args.lm_outliers:
        fit_w = _parse_window(args.lm_outliers_fit_window)
        app_w = _parse_window(args.lm_outliers_apply_window)

        spec = IQDOutlierSpec(
            c=float(args.lm_outliers_c),
            min_obs=int(args.lm_outliers_min_obs),
        )
        X_lm_out = apply_outliers_iqd_to_nan(
            X_full,
            spec=spec,
            fit_window=fit_w,
            apply_window=app_w,
        )

        fit_tag = "fit_full" if fit_w is None else f"fit_{fit_w[0]}_{fit_w[1]}"
        app_tag = "apply_full" if app_w is None else f"apply_{app_w[0]}_{app_w[1]}"
        out_lm_out = out_dir / f"{stem}_lm_outliers_iqd_c{args.lm_outliers_c}_{fit_tag}_{app_tag}_{suffix}.csv"
        X_lm_out.to_csv(out_lm_out, index_label="Date")
        print(f"[OK] wrote lm_outliers panel: {out_lm_out}")


if __name__ == "__main__":
    main()
