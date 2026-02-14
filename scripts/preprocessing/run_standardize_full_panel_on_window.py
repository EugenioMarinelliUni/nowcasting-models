# scripts/preprocessing/run_standardize_full_panel_on_window.py
#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
import pandas as pd

# add src to path when running from repo root
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.preprocessing.fixed_window_standardize import (  # noqa: E402
    standardize_full_panel_on_window,
)


def load_transformed_panel(csv_path: Path, date_col: str = "sasdate") -> pd.DataFrame:
    """
    Load a stationarized/transformed monthly panel; set DatetimeIndex;
    keep numeric columns only.
    """
    df = pd.read_csv(csv_path, low_memory=False)

    if date_col not in df.columns:
        first = df.columns[0]
        s_dates = pd.to_datetime(df[first], errors="coerce")
        valid_count: int = int(s_dates.notna().sum())
        total_count: int = int(len(s_dates))
        frac_valid: float = valid_count / total_count if total_count > 0 else 0.0

        if frac_valid > 0.9:
            date_col = first
        else:
            raise ValueError(
                f"Date column '{date_col}' not found in {csv_path}. "
                f"Available columns: {list(df.columns)[:8]}"
            )

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col]).set_index(df[date_col].name).sort_index()
    return df.select_dtypes(include="number")


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _tag_from_window(train_start: str, train_end: str, *, granularity: str) -> str:
    s = pd.to_datetime(train_start)
    e = pd.to_datetime(train_end)
    if granularity == "years":
        return f"train{s.year:04d}_{e.year:04d}"
    if granularity == "ym":
        return f"train{s.year:04d}_{s.month:02d}_{e.year:04d}_{e.month:02d}"
    if granularity == "ymd":
        return (
            f"train{s.year:04d}_{s.month:02d}_{s.day:02d}_"
            f"{e.year:04d}_{e.month:02d}_{e.day:02d}"
        )
    raise ValueError(f"Unsupported granularity={granularity!r}")


@dataclass(frozen=True)
class OutputPlan:
    out_full: Path
    out_train: Path | None
    out_oos: Path | None
    stats_path: Path | None


def _resolve_outputs(args: argparse.Namespace) -> OutputPlan:
    """
    Output logic:
    - If --out-dir is provided, the script constructs a subfolder based on train window
      and generates descriptive filenames. Parent dirs are created on write.
    - Otherwise, it uses --out-full/--out-train/--out-oos as provided.
    """
    if args.out_dir:
        out_dir = Path(args.out_dir)
        if not args.panel_id:
            raise ValueError("--panel-id is required when using --out-dir")

        tag = _tag_from_window(args.train_start, args.train_end, granularity=args.subdir_granularity)
        base = out_dir / tag

        out_full = base / f"X_panel_z__{args.panel_id}__{tag}.csv"

        out_train = None
        if args.write_splits:
            out_train = base / f"X_panel_z__{args.panel_id}__{tag}__train.csv"

        out_oos = None
        if args.write_splits:
            oos_start = pd.to_datetime(args.oos_start)
            oos_end = pd.to_datetime(args.oos_end) if args.oos_end else None
            if oos_end is None:
                oos_tag = f"oos{oos_start.year:04d}_{oos_start.month:02d}_end"
            else:
                oos_tag = f"oos{oos_start.year:04d}_{oos_start.month:02d}_{oos_end.year:04d}_{oos_end.month:02d}"
            out_oos = base / f"X_panel_z__{args.panel_id}__{tag}__{oos_tag}.csv"

        stats_path = None
        if args.save_stats:
            stats_path = base / f"X_panel_z__{args.panel_id}__{tag}__train_stats.csv"

        return OutputPlan(out_full=out_full, out_train=out_train, out_oos=out_oos, stats_path=stats_path)

    # Backward-compatible explicit outputs
    out_full = Path(args.out_full)
    out_train = Path(args.out_train) if args.out_train else None
    out_oos = Path(args.out_oos) if args.out_oos else None

    stats_path = None
    if args.save_stats:
        stats_path = out_full.with_suffix("").with_name(out_full.stem + "__train_stats.csv")

    return OutputPlan(out_full=out_full, out_train=out_train, out_oos=out_oos, stats_path=stats_path)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Standardize a full transformed panel (stationarized, NOT standardized) "
            "using μ,σ computed on a training window, and write a full standardized "
            "panel (from train-start onward) plus optional train/OOS subpanels."
        )
    )
    ap.add_argument(
        "--csv",
        required=True,
        help="Full transformed (tcodes-applied) panel CSV, e.g. data/processed_data/stationarity/panel_....csv",
    )
    ap.add_argument(
        "--date-col",
        default="sasdate",
        help="Date column name in input CSV (default: sasdate).",
    )
    ap.add_argument(
        "--date-col-out",
        default="date",
        help="Date column name in output CSVs (default: date). Use 'sasdate' to match your dataset convention.",
    )
    ap.add_argument(
        "--train-start",
        required=True,
        help="Training window start YYYY-MM-DD (e.g. 1990-01-01).",
    )
    ap.add_argument(
        "--train-end",
        required=True,
        help="Training window end YYYY-MM-DD (e.g. 2019-12-01).",
    )
    ap.add_argument(
        "--oos-start",
        required=True,
        help="OOS window start YYYY-MM-DD (e.g. 2020-01-01).",
    )
    ap.add_argument(
        "--oos-end",
        default=None,
        help="Optional OOS window end YYYY-MM-DD. If omitted, uses max date in panel.",
    )
    ap.add_argument(
        "--monthly-freq",
        default="MS",
        choices=["MS", "ME"],
        help="Normalize index to month-start (MS) or month-end (ME). Default: MS.",
    )
    ap.add_argument(
        "--min-obs-per-col",
        type=int,
        default=1,
        help="Drop columns with fewer than this many non-missing obs in the training window.",
    )

    # New: directory-based output mode (auto subfolder + descriptive names)
    ap.add_argument(
        "--out-dir",
        default="",
        help=(
            "If provided, store artifacts under --out-dir/<train-tag>/ with descriptive filenames. "
            "Overrides the need to manually create parent directories. Requires --panel-id."
        ),
    )
    ap.add_argument(
        "--panel-id",
        default="",
        help="Panel identifier used in generated filenames when using --out-dir (e.g. 1960_noVIX).",
    )
    ap.add_argument(
        "--subdir-granularity",
        default="ym",
        choices=["years", "ym", "ymd"],
        help="Train subfolder tag granularity: years|ym|ymd. Default: ym.",
    )
    ap.add_argument(
        "--write-splits",
        action="store_true",
        help="If set with --out-dir, also write train and OOS split files into the same subfolder.",
    )

    # Backward-compatible explicit outputs
    ap.add_argument(
        "--out-full",
        required=False,
        default="",
        help="Output CSV for full standardized panel (from train-start to end). Ignored if --out-dir is used.",
    )
    ap.add_argument(
        "--out-train",
        required=False,
        default="",
        help="Optional output CSV for standardized training subpanel. Ignored if --out-dir is used.",
    )
    ap.add_argument(
        "--out-oos",
        required=False,
        default="",
        help="Optional output CSV for standardized OOS subpanel. Ignored if --out-dir is used.",
    )
    ap.add_argument(
        "--save-stats",
        action="store_true",
        help="If set, write μ,σ,nobs CSV next to the outputs (or into the train-tag subfolder when using --out-dir).",
    )

    args = ap.parse_args()

    # Enforce output mode requirements
    if not args.out_dir:
        if not args.out_full:
            raise SystemExit("Either --out-dir (recommended) or --out-full must be provided.")

    if not args.date_col_out:
        raise SystemExit("--date-col-out must be a non-empty string.")
    return args


def main() -> None:
    args = parse_args()
    plan = _resolve_outputs(args)

    csv_path = Path(args.csv)
    X_raw = load_transformed_panel(csv_path, date_col=args.date_col)

    # 1) Full standardization with frozen train scalers
    Z_full_all, stats = standardize_full_panel_on_window(
        X_raw,
        start=args.train_start,
        end=args.train_end,
        monthly_freq=args.monthly_freq,
        min_obs_per_col=int(args.min_obs_per_col),
    )

    # 2) Trim to [train-start, end-of-sample]
    Z_full = Z_full_all.loc[args.train_start:].copy()

    # 3) Write full standardized panel
    df_full = Z_full.copy()
    df_full.insert(0, args.date_col_out, df_full.index)

    _ensure_parent(plan.out_full)
    df_full.to_csv(plan.out_full, index=False)
    print(f"[OK] wrote full standardized panel: {plan.out_full}  shape={df_full.shape}")

    # 4) Optional train/OOS splits
    oos_end_str: str = args.oos_end or str(Z_full.index.max().date())
    train_slice = slice(args.train_start, args.train_end)
    oos_slice = slice(args.oos_start, oos_end_str)

    if plan.out_train is not None:
        Z_train = Z_full.loc[train_slice].copy()
        df_train = Z_train.copy()
        df_train.insert(0, args.date_col_out, df_train.index)
        _ensure_parent(plan.out_train)
        df_train.to_csv(plan.out_train, index=False)
        print(f"[OK] wrote standardized training panel: {plan.out_train}  shape={df_train.shape}")

    if plan.out_oos is not None:
        Z_oos = Z_full.loc[oos_slice].copy()
        df_oos = Z_oos.copy()
        df_oos.insert(0, args.date_col_out, df_oos.index)
        _ensure_parent(plan.out_oos)
        df_oos.to_csv(plan.out_oos, index=False)
        print(f"[OK] wrote standardized OOS panel: {plan.out_oos}  shape={df_oos.shape}")

    # 5) Optional stats (μ,σ,nobs for the training window)
    if plan.stats_path is not None:
        _ensure_parent(plan.stats_path)
        pd.DataFrame({"mean": stats.mean, "std": stats.std, "nobs": stats.nobs}).to_csv(
            plan.stats_path, index_label="series"
        )
        print(f"[OK] wrote train stats: {plan.stats_path}")


if __name__ == "__main__":
    main()