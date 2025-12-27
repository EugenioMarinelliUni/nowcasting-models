#!/usr/bin/env python3
"""
Build Covid-adjusted panel variants from existing baseline full standardized panels.

Inputs discovered by default:
  dataset/*/baseline/*/X_panel_z__*.csv

Outputs (per input panel):
  dataset/{panel}/baseline/{tag}/{out_subdir}/
    (files produced by scripts/covid/run_build_covid_panels.py)

This script is a thin runner around:
  python scripts/covid/run_build_covid_panels.py ...

Assumptions:
- Baseline panels have a 'date' column (lowercase) as produced by run_standardize_full_panel_on_window.py.
- The covid builder script accepts the CLI flags used below.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from typing import List


REPO_ROOT = Path(__file__).resolve().parents[2]


def run_cmd(cmd: List[str]) -> None:
    print("\n$ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def default_inputs() -> List[Path]:
    # Typical structure you created:
    # dataset/{panel}/baseline/{tag}/X_panel_z__{panel}__{tag}.csv
    return sorted((REPO_ROOT / "dataset").glob("*/baseline/*/X_panel_z__*.csv"))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build Covid-adjusted panel variants from baseline full standardized panels."
    )
    ap.add_argument(
        "--inputs",
        nargs="*",
        default=None,
        help=(
            "Optional explicit list of baseline panel CSVs. "
            "If omitted, scans dataset/*/baseline/*/X_panel_z__*.csv"
        ),
    )
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

    # Optional knobs (passed through if enabled)
    ap.add_argument(
        "--clip-sigma",
        default=None,
        help="If set, pass --clip-sigma to covid builder (e.g. 6.0).",
    )
    ap.add_argument(
        "--lm-dummies",
        action="store_true",
        help="If set, pass --lm-dummies (and related flags if provided).",
    )
    ap.add_argument(
        "--lm-dummy-mode",
        default="q2q3_2020",
        help="Dummy mode for LM dummies (only if --lm-dummies).",
    )
    ap.add_argument(
        "--lm-outliers",
        action="store_true",
        help="If set, pass --lm-outliers (and related flags if provided).",
    )
    ap.add_argument(
        "--lm-outliers-c",
        default="4.0",
        help="Outlier cutoff constant (only if --lm-outliers).",
    )
    ap.add_argument(
        "--lm-outliers-min-obs",
        default="20",
        help="Minimum obs for outlier fit (only if --lm-outliers).",
    )
    ap.add_argument(
        "--lm-outliers-fit-window",
        default=None,
        help=(
            "Fit window for outlier thresholds, as 'YYYY-MM-DD,YYYY-MM-DD'. "
            "If omitted, the covid builder's default is used."
        ),
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    if args.inputs:
        inputs = [Path(p) for p in args.inputs]
    else:
        inputs = default_inputs()

    if not inputs:
        raise SystemExit("No baseline panels found. Expected dataset/*/baseline/*/X_panel_z__*.csv")

    covid_builder = REPO_ROOT / "scripts" / "covid" / "run_build_covid_panels.py"
    if not covid_builder.exists():
        raise SystemExit(f"Missing covid builder script: {covid_builder}")

    for full_panel in inputs:
        if not full_panel.exists():
            raise SystemExit(f"Missing input: {full_panel}")

        # Output folder: sibling subdir under the tag folder
        tag_dir = full_panel.parent
        out_dir = tag_dir / args.out_subdir
        out_dir.mkdir(parents=True, exist_ok=True)

        cmd: List[str] = [
            "python",
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

        if args.clip_sigma is not None:
            cmd += ["--clip-sigma", str(args.clip_sigma)]

        if args.lm_dummies:
            cmd += ["--lm-dummies", "--lm-dummy-mode", str(args.lm_dummy_mode)]

        if args.lm_outliers:
            cmd += [
                "--lm-outliers",
                "--lm-outliers-c",
                str(args.lm_outliers_c),
                "--lm-outliers-min-obs",
                str(args.lm_outliers_min_obs),
            ]
            if args.lm_outliers_fit_window is not None:
                cmd += ["--lm-outliers-fit-window", str(args.lm_outliers_fit_window)]

        run_cmd(cmd)


if __name__ == "__main__":
    main()
