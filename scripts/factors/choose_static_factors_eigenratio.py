# scripts/factors/choose_static_factors_eigenratio.py
from __future__ import annotations

"""
Ahn–Horenstein (2013) factor-number selection (ER/GR variants).

Reads the preselected panel CSV and writes artifacts under:
  data/factors/{panel}/{tag}/{method}/{label}/anh_horenstein/{er|gr}/

You can override the trailing subfolder with --out-subdir "custom/path".
"""

import argparse
from pathlib import Path
import sys

import pandas as pd

# Ensure src/ is on path when running as a script
THIS = Path(__file__).resolve()
PROJECT_ROOT = THIS.parents[2]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from dfm_pipeline.factors.ahn_horenstein import (  # type: ignore
    choose_q_ahn_horenstein,
    write_ahn_horenstein_artifacts,
)


def discover_preselect_csv(panel: str, tag: str, method: str, label: str) -> Path:
    """
    Matches the preselection artifact naming used elsewhere:
    dataset/{panel}/preselect/{method}/X_panel_z__{panel}__{tag}__preselect-{method}__{label}.csv
    """
    rel = (
        f"dataset/{panel}/preselect/{method}/"
        f"X_panel_z__{panel}__{tag}__preselect-{method}__{label}.csv"
    )
    return PROJECT_ROOT / rel


def main():
    p = argparse.ArgumentParser(
        description="Choose number of static factors via Ahn–Horenstein ER/GR."
    )
    p.add_argument("--panel", required=True)
    p.add_argument("--tag", required=True)
    p.add_argument("--method", required=True, choices=["sis", "tstat", "lars"])
    p.add_argument("--label", required=True)

    # Method variant and limits
    p.add_argument("--variant", default="ER", choices=["ER", "GR"])
    p.add_argument("--r-max", type=int, default=12)

    # Input override
    p.add_argument(
        "--X-csv",
        dest="x_csv",
        default=None,
        help="Optional path to a panel CSV (bypasses discover_preselect_csv).",
    )

    # Preprocessing flags
    p.add_argument("--standardize", default="true", choices=["true", "false"])
    p.add_argument("--demean", default="false", choices=["true", "false"])
    p.add_argument("--detrend", default="none", choices=["none"])  # extend later

    # Artifacts
    p.add_argument("--scree", action="store_true", help="Save scree plot.")
    p.add_argument("--save-loadings", action="store_true")
    p.add_argument("--save-scores", action="store_true")

    # Custom output subfolder (relative to label/)
    p.add_argument(
        "--out-subdir",
        default=None,
        help='Optional extra subfolder under {label}/ (e.g., "custom/name"). '
             'If omitted, defaults to "anh_horenstein/{er|gr}".',
    )

    # Parity switch (currently unused)
    p.add_argument("--ignore-json", action="store_true", help="Reserved flag.")

    args = p.parse_args()

    # Parse booleans
    standardize = args.standardize.lower() == "true"
    demean = args.demean.lower() == "true"

    # Locate input panel CSV
    if args.x_csv:
        x_path = Path(args.x_csv)
    else:
        x_path = discover_preselect_csv(args.panel, args.tag, args.method, args.label)

    if not x_path.exists():
        raise FileNotFoundError(f"Panel CSV not found: {x_path}")

    # Load panel
    X = pd.read_csv(x_path, index_col=0)
    # If the index looks like dates, try to parse
    try:
        X.index = pd.to_datetime(X.index)
    except Exception:
        pass

    # Run selector
    result = choose_q_ahn_horenstein(
        X,
        r_max=args.r_max,
        variant=args.variant,        # "ER" or "GR"
        standardize=standardize,
        demean=demean,
        detrend=args.detrend,
        want_scores=args.save_scores,
        want_loadings=args.save_loadings,
    )

    # -------- Output location logic --------
    # Default: {label}/anh_horenstein/{er|gr}
    variant_lower = args.variant.lower()  # "er" or "gr"
    if args.out_subdir:
        suffix = Path(args.label) / args.out_subdir
    else:
        suffix = Path(args.label) / "anh_horenstein" / variant_lower

    base_dir = (
        PROJECT_ROOT
        / "data"
        / "factors"
        / args.panel
        / args.tag
        / args.method
        / suffix
    )
    base_dir.mkdir(parents=True, exist_ok=True)

    # We keep filenames as {ER|GR}.* inside base_dir
    stem = args.variant  # "ER" or "GR"
    paths = {
        "grid_csv": str(base_dir / f"{stem}.grid.csv"),
        "summary_json": str(base_dir / f"{stem}.summary.json"),
        "eigen_csv": str(base_dir / f"{stem}.eigen.csv"),
    }
    if args.save_scores:
        paths["scores_csv"] = str(base_dir / f"{stem}.scores.csv")
    if args.save_loadings:
        paths["loadings_csv"] = str(base_dir / f"{stem}.loadings.csv")
    if args.scree:
        paths["scree_png"] = str(base_dir / f"{stem}.scree.png")

    # Write artifacts
    written = write_ahn_horenstein_artifacts(
        result,
        paths,
        write_scree=args.scree,
    )

    # Console summary (ASCII-safe)
    print(
        f"[Ahn-Horenstein/{args.variant}] {args.panel} {args.tag} "
        f"{args.method}:{args.label} -> q*={result.q_star} (r_max={args.r_max})"
    )
    for k, v in written.items():
        print(f"  wrote: {k}: {v}")


if __name__ == "__main__":
    main()
