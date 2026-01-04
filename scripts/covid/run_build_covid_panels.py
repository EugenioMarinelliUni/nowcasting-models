# scripts/covid/run_build_covid_panels.py
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple

# Make dfm_pipeline importable when running from repo root (MUST be before dfm_pipeline imports)
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.covid.io import (  # noqa: E402
    describe_mask,
    load_monthly_panel_csv,
    sha256_file,
    write_json,
    write_mask_matrix,
    write_panel_csv,
)
from dfm_pipeline.covid.methods import (  # noqa: E402
    variant_delete_window,
    variant_iqd_outliers_to_nan,
    variant_sparse_dummies,
)
from dfm_pipeline.covid.spec import CovidSpec, parse_window  # noqa: E402


def sanitize_suffix(start: str, end: str) -> str:
    s = start[:7].replace("-", "M")
    e = end[:7].replace("-", "M")
    return f"covid_{s}_{e}"


def _parse_window(s: Optional[str]) -> Optional[Tuple[str, str]]:
    return parse_window(s)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Build Covid-adjusted variants from a baseline full monthly panel.\n"
            "Kept variants:\n"
            "  - covid_delete: set Covid window to NaN\n"
            "  - lm_outliers: IQD outliers -> NaN (fit/apply windows)\n"
            "  - lm_dummies: sparse dummy matrix emitted as exogenous regressors (non-block safe)\n"
        )
    )

    ap.add_argument("--full-panel", required=True, help="Input baseline panel CSV (monthly; date column required).")
    ap.add_argument("--covid-start", required=True, help="Covid window start (YYYY-MM or YYYY-MM-DD).")
    ap.add_argument("--covid-end", required=True, help="Covid window end (YYYY-MM or YYYY-MM-DD).")
    ap.add_argument("--monthly-freq", default="MS", choices=["MS", "ME"], help="Monthly index convention (default MS).")

    ap.add_argument("--out-dir", default=None, help="Output directory. If omitted, uses the directory of --full-panel.")
    ap.add_argument(
        "--suffix",
        default=None,
        help="Optional suffix; if omitted, derived from covid-start/end (e.g., covid_2020M03_2020M09).",
    )

    # -------- delete-window variant --------
    ap.add_argument("--no-delete", action="store_true", help="Skip covid_delete variant.")

    # -------- LM sparse dummies (exogenous) --------
    ap.add_argument("--lm-dummies", action="store_true", help="Emit sparse dummy matrix D_exog (non-block safe).")
    ap.add_argument(
        "--lm-dummy-mode",
        default="q2q3_2020",
        choices=["q2q3_2020", "q1q2_2020", "custom"],
        help="q2q3_2020 => 2020-06 and 2020-09; q1q2_2020 => 2020-03 and 2020-06; custom => use --lm-dummy-months.",
    )
    ap.add_argument(
        "--lm-dummy-months",
        default=None,
        help="Custom months (comma-separated), only for --lm-dummy-mode custom. Example: '2020-06-01,2020-09-01'.",
    )
    ap.add_argument(
        "--lm-dummy-min-nonmissing",
        type=int,
        default=5,
        help="Guard: activate dummy only if that date has >= this many non-missing series.",
    )
    ap.add_argument("--lm-dummy-standardize", action="store_true", help="Standardize dummy columns over --dummy-std-window.")
    ap.add_argument("--dummy-std-window", default=None, help="Required if --lm-dummy-standardize. 'YYYY-MM-DD,YYYY-MM-DD'.")

    # -------- LM outliers (IQD -> NaN) --------
    ap.add_argument("--lm-outliers", action="store_true", help="Apply IQD outliers -> NaN variant.")
    ap.add_argument("--lm-outliers-c", type=float, default=4.0, help="c in abs(x-median) > c*IQD (default 4.0).")
    ap.add_argument("--lm-outliers-min-obs", type=int, default=20, help="Min obs in fit window (default 20).")
    ap.add_argument(
        "--lm-outliers-fit-window",
        default=None,
        help="Fit window for thresholds (leakage-safe). 'YYYY-MM-DD,YYYY-MM-DD'.",
    )
    ap.add_argument(
        "--lm-outliers-apply-window",
        default=None,
        help="Apply window for replacement. If omitted, defaults to Covid window. 'YYYY-MM-DD,YYYY-MM-DD'.",
    )
    ap.add_argument("--allow-leakage", action="store_true", help="Allow fitting outlier thresholds without an explicit fit window.")

    return ap.parse_args()


def main() -> None:
    args = parse_args()

    full_path = Path(args.full_panel)
    if not full_path.exists():
        raise FileNotFoundError(f"Full panel not found: {full_path}")

    X_full = load_monthly_panel_csv(full_path)

    covid_start = args.covid_start
    covid_end = args.covid_end
    monthly_freq = args.monthly_freq

    out_dir = Path(args.out_dir) if args.out_dir is not None else full_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    suffix = args.suffix or sanitize_suffix(covid_start, covid_end)
    stem = full_path.stem
    in_hash = sha256_file(full_path)

    base_meta = {
        "input": {"path": str(full_path), "sha256": in_hash},
        "covid_start": covid_start,
        "covid_end": covid_end,
        "monthly_freq": monthly_freq,
        "suffix": suffix,
    }

    covid_spec = CovidSpec(
        covid_start=covid_start,
        covid_end=covid_end,
        monthly_freq=monthly_freq,
        fit_window=_parse_window(args.lm_outliers_fit_window),
        apply_window=_parse_window(args.lm_outliers_apply_window),
        allow_leakage=bool(args.allow_leakage),
    )

    def _write_meta_and_masks(prefix: str, meta: dict, masks: dict) -> None:
        write_json(meta, out_dir / f"{prefix}__meta.json")
        for k, m in masks.items():
            write_mask_matrix(m, out_dir / f"{prefix}__mask_{k}.csv")

    # ---- covid_delete ----
    if not args.no_delete:
        res = variant_delete_window(X_full, covid_spec)
        out_csv = out_dir / f"{stem}_covid_delete_{suffix}.csv"
        write_panel_csv(res.X, out_csv)
        meta = {**base_meta, **res.meta, "masks": {k: describe_mask(v) for k, v in res.masks.items()}}
        _write_meta_and_masks(out_csv.with_suffix("").name, meta, res.masks)

    # ---- lm_exog_dummies ----
    if args.lm_dummies:
        custom_months = None
        if args.lm_dummy_mode == "custom":
            if args.lm_dummy_months is None:
                raise ValueError("--lm-dummy-months required when --lm-dummy-mode custom")
            custom_months = [m.strip() for m in args.lm_dummy_months.split(",") if m.strip()]

        std_window = _parse_window(args.dummy_std_window) if args.lm_dummy_standardize else None

        res = variant_sparse_dummies(
            X_full,
            covid_spec,
            mode=args.lm_dummy_mode,
            custom_months=custom_months,
            ensure_min_nonmissing=int(args.lm_dummy_min_nonmissing),
            prefix="lm_covid_dummy",
            standardize_dummies=bool(args.lm_dummy_standardize),
            standardize_over=std_window,
        )

        prefix = f"{stem}_lm_exog_dummies_{args.lm_dummy_mode}_{suffix}"
        out_D = out_dir / f"{prefix}__D_exog.csv"

        D = res.artifacts.get("exog_dummies")
        if D is None:
            raise KeyError("variant_sparse_dummies did not return 'exog_dummies' artifact")

        write_panel_csv(D, out_D)

        meta = {
            **base_meta,
            **res.meta,
            "x_panel": str(full_path),
            "exog_matrix": str(out_D),
            "exog_cols": res.meta.get("dummy_cols", []),
        }
        _write_meta_and_masks(prefix, meta, res.masks)

    # ---- lm_outliers (IQD -> NaN) ----
    if args.lm_outliers:
        fit_w = _parse_window(args.lm_outliers_fit_window)
        app_w = _parse_window(args.lm_outliers_apply_window)

        if fit_w is None and not args.allow_leakage:
            raise ValueError(
                "lm_outliers requires --lm-outliers-fit-window (training window) unless --allow-leakage is set."
            )

        res = variant_iqd_outliers_to_nan(
            X_full,
            covid_spec,
            c=float(args.lm_outliers_c),
            min_obs=int(args.lm_outliers_min_obs),
            fit_window=fit_w,
            apply_window=app_w,
        )

        fit_tag = "fit_full" if fit_w is None else f"fit_{fit_w[0]}_{fit_w[1]}"
        app_tag = "apply_covid" if app_w is None else f"apply_{app_w[0]}_{app_w[1]}"
        out_csv = out_dir / f"{stem}_lm_outliers_iqd_c{args.lm_outliers_c}_{fit_tag}_{app_tag}_{suffix}.csv"
        write_panel_csv(res.X, out_csv)

        meta = {**base_meta, **res.meta, "masks": {k: describe_mask(v) for k, v in res.masks.items()}}
        _write_meta_and_masks(out_csv.with_suffix("").name, meta, res.masks)


if __name__ == "__main__":
    main()