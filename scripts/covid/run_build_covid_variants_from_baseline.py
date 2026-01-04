#!/usr/bin/env python3
"""
Build Covid-adjusted panel variants from existing baseline full standardized panels.

Default inputs discovered:
  dataset/*/baseline/*/X_panel_z__*.csv
(excludes *__train_stats.csv)

Outputs (per input panel):
  dataset/{panel}/baseline/{tag}/{out_subdir}/
    (files produced by scripts/covid/run_build_covid_panels.py)

This script is a thin runner around:
  python scripts/covid/run_build_covid_panels.py ...

Notes:
- Baseline panels are the full standardized panels (with a 'date' column) produced by
  run_standardize_full_panel_on_window.py.
- For lm_outliers, it is recommended to fit thresholds on the TRAIN window implied by the tag
  folder name (e.g. train1990_2019 -> 1990-02-01,2019-12-01). Use --lm-outliers-fit-window auto.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]


_TAG_RE = re.compile(r"^train(\d{4})_(\d{4})$")


def run_cmd(cmd: List[str], *, dry_run: bool = False) -> None:
    print("\n$ " + " ".join(cmd), flush=True)
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def default_inputs(roots: Optional[List[Path]] = None) -> List[Path]:
    """
    Find baseline panels:
      dataset/{panel}/baseline/{tag}/X_panel_z__{panel}__{tag}.csv

    Excludes:
      - *__train_stats.csv
    """
    base = roots if roots else [REPO_ROOT / "dataset"]
    out: List[Path] = []
    for b in base:
        b = Path(b)
        if b.name == "baseline":
            # user passed dataset/*/baseline
            candidates = b.glob("*/X_panel_z__*.csv")
        else:
            candidates = b.glob("*/baseline/*/X_panel_z__*.csv")

        for p in candidates:
            if p.name.endswith("__train_stats.csv"):
                continue
            out.append(p)

    return sorted(set(out))


def infer_fit_window_from_tag(tag: str) -> Optional[str]:
    """
    Infer fit window from tag folder name like 'train1990_2019'.
    Uses convention:
      start = YYYY-02-01
      end   = YYYY-12-01
    """
    m = _TAG_RE.match(tag)
    if not m:
        return None
    y0 = int(m.group(1))
    y1 = int(m.group(2))
    return f"{y0:04d}-02-01,{y1:04d}-12-01"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build Covid-adjusted panel variants from baseline full standardized panels."
    )
    ap.add_argument(
        "--roots",
        nargs="*",
        default=None,
        help=(
            "Optional baseline roots to scan. Examples:\n"
            "  dataset/1960_noVIX/baseline dataset/1965_withVIX/baseline\n"
            "If omitted, scans under dataset/*/baseline/*/X_panel_z__*.csv"
        ),
    )
    ap.add_argument(
        "--inputs",
        nargs="*",
        default=None,
        help=(
            "Optional explicit list of baseline panel CSVs. "
            "If provided, --roots is ignored."
        ),
    )

    ap.add_argument("--dry-run", action="store_true", help="Print commands but do not execute.")

    ap.add_argument(
        "--covid-start",
        default="2020-03-01",
        help="Covid window start (YYYY-MM-DD). Default: 2020-03-01",
    )
    ap.add_argument(
        "--covid-end",
        default="2020-09-01",
        help="Covid window end (YYYY-MM-DD). Default: 2020-09-01",
    )
    ap.add_argument(
        "--monthly-freq",
        choices=["MS", "ME"],
        default="MS",
        help="Monthly frequency convention. Default: MS",
    )
    ap.add_argument(
        "--out-subdir",
        default="covid_2020M03_2020M09",
        help="Subfolder name created under each baseline tag folder.",
    )

    # Pass-through: delete-window variant
    ap.add_argument(
        "--no-delete",
        action="store_true",
        help="Skip covid_delete variant (passes --no-delete).",
    )

    # Pass-through: LM dummies
    ap.add_argument(
        "--lm-dummies",
        action="store_true",
        help="Enable lm dummies (passes --lm-dummies and --lm-dummy-mode).",
    )
    ap.add_argument(
        "--lm-dummy-mode",
        default="q2q3_2020",
        choices=["q2q3_2020", "q1q2_2020", "custom"],
        help="Dummy mode (only if --lm-dummies).",
    )
    ap.add_argument(
        "--lm-dummy-months",
        default=None,
        help="Custom months, only for --lm-dummy-mode custom (comma-separated).",
    )

    # Pass-through: LM outliers (IQD -> NaN)
    ap.add_argument(
        "--lm-outliers",
        action="store_true",
        help="Enable lm outliers (IQD -> NaN).",
    )
    ap.add_argument(
        "--lm-outliers-c",
        default="6.0",
        help="Outlier cutoff constant (only if --lm-outliers). Default: 6.0",
    )
    ap.add_argument(
        "--lm-outliers-min-obs",
        default="20",
        help="Minimum obs for outlier fit (only if --lm-outliers). Default: 20",
    )
    ap.add_argument(
        "--lm-outliers-fit-window",
        default="auto",
        help=(
            "Fit window for outlier thresholds:\n"
            "  - 'auto' (default): infer from tag folder trainYYYY_YYYY\n"
            "  - or explicit 'YYYY-MM-DD,YYYY-MM-DD'\n"
        ),
    )

    return ap.parse_args()


def main() -> None:
    args = parse_args()

    if args.inputs:
        inputs = [Path(p) for p in args.inputs]
    else:
        roots = [Path(r) for r in args.roots] if args.roots else None
        inputs = default_inputs(roots=roots)

    if not inputs:
        raise SystemExit("No baseline panels found.")

    covid_builder = REPO_ROOT / "scripts" / "covid" / "run_build_covid_panels.py"
    if not covid_builder.exists():
        raise SystemExit(f"Missing covid builder script: {covid_builder}")

    py = sys.executable  # use venv python

    for full_panel in inputs:
        if not full_panel.exists():
            raise SystemExit(f"Missing input: {full_panel}")

        tag_dir = full_panel.parent
        tag = tag_dir.name

        out_dir = tag_dir / args.out_subdir
        out_dir.mkdir(parents=True, exist_ok=True)

        cmd: List[str] = [
            py,
            str(covid_builder),
            "--full-panel",
            str(full_panel),
            "--covid-start",
            args.covid_start,
            "--covid-end",
            args.covid_end,
            "--monthly-freq",
            args.monthly_freq,
            "--out-dir",
            str(out_dir),
        ]

        if args.no_delete:
            cmd += ["--no-delete"]

        if args.lm_dummies:
            cmd += ["--lm-dummies", "--lm-dummy-mode", str(args.lm_dummy_mode)]
            if args.lm_dummy_mode == "custom":
                if not args.lm_dummy_months:
                    raise SystemExit("--lm-dummy-months is required when --lm-dummy-mode custom")
                cmd += ["--lm-dummy-months", str(args.lm_dummy_months)]

        if args.lm_outliers:
            cmd += [
                "--lm-outliers",
                "--lm-outliers-c",
                str(args.lm_outliers_c),
                "--lm-outliers-min-obs",
                str(args.lm_outliers_min_obs),
            ]

            fw = args.lm_outliers_fit_window
            if fw == "auto":
                inferred = infer_fit_window_from_tag(tag)
                if inferred is None:
                    raise SystemExit(
                        f"Cannot infer fit window from tag '{tag}'. "
                        "Use --lm-outliers-fit-window YYYY-MM-DD,YYYY-MM-DD"
                    )
                fw = inferred

            if fw:
                cmd += ["--lm-outliers-fit-window", str(fw)]

        run_cmd(cmd, dry_run=bool(args.dry_run))


if __name__ == "__main__":
    main()