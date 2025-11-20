# scripts/factors/choose_static_factors_baing.py
from __future__ import annotations

"""
Bai–Ng (2002) information-criteria selector for the number of static factors.

Reads the preselected panel CSV and writes artifacts under:
  data/factors/{panel}/{tag}/{method}/{label}/bai_ng/{ic_lower}/

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

from dfm_pipeline.factors.baing import (  # type: ignore
    bai_ng_criteria,
    write_baing_artifacts,
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
        description="Choose number of static factors via Bai–Ng information criteria."
    )
    p.add_argument("--panel", required=True)
    p.add_argument("--tag", required=True)
    p.add_argument("--method", required=True, choices=["sis", "tstat", "lars"])
    p.add_argument("--label", required=True)

    # IC choice and rank cap
    p.add_argument("--ic", default="ICp2", choices=["ICp1", "ICp2", "ICp3", "AIC3", "BIC3"])
    p.add_argument("--r-max", type=int, default=12)

    # Optional direct input path
    p.add_argument(
        "--X-csv",
        dest="x_csv",
        default=None,
        help="Optional path to a panel CSV (bypasses discover_preselect_csv).",
    )

    # Preprocessing flags (accepted for CLI symmetry; Bai–Ng routine standardizes internally)
    p.add_argument("--standardize", default="true", choices=["true", "false"])
    p.add_argument("--demean", default="false", choices=["true", "false"])
    p.add_argument("--detrend", default="none", choices=["none"])  # reserved

    # Artifacts
    p.add_argument("--scree", action="store_true", help="Save scree plot.")
    p.add_argument("--save-loadings", action="store_true", help="Save factor loadings CSV.")
    p.add_argument("--save-scores", action="store_true", help="Save factor scores CSV.")

    # Custom output subfolder (relative to {label}/)
    p.add_argument(
        "--out-subdir",
        default=None,
        help='Optional extra subfolder under {label}/. '
             'If omitted, defaults to "bai_ng/{ic_lower}".',
    )

    # Parity flag, currently unused
    p.add_argument("--ignore-json", action="store_true", help="Reserved flag.")

    args = p.parse_args()

    # Parse booleans (not used by the computation, but we accept them for parity)
    _ = (args.standardize.lower() == "true")
    _ = (args.demean.lower() == "true")
    _ = args.detrend  # currently unused

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

    # Run Bai–Ng
    # Note: bai_ng_criteria always z-scores and does listwise deletion internally.
    result = bai_ng_criteria(
        X,
        r_max=args.r_max,
        ic_type=args.ic,
        index=X.index,         # friendly index if retained
        colnames=list(X.columns),
    )

    # -------- Output location logic --------
    # Default subdir: {label}/bai_ng/{ic_lower}
    ic_lower = args.ic.lower()
    if args.out_subdir:
        suffix = Path(args.label) / args.out_subdir
    else:
        suffix = Path(args.label) / "bai_ng" / ic_lower

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

    # We keep filenames as {IC}.* inside base_dir (e.g., ICp2.grid.csv)
    stem = args.ic
    paths = {
        "grid_csv":     str(base_dir / f"{stem}.grid.csv"),
        "summary_json": str(base_dir / f"{stem}.summary.json"),
        "eigen_csv":    str(base_dir / f"{stem}.eigen.csv"),
    }
    if args.save_scores:
        paths["factors_csv"] = str(base_dir / f"{stem}.factors.csv")
    if args.save_loadings:
        paths["loadings_csv"] = str(base_dir / f"{stem}.loadings.csv")
    if args.scree:
        paths["scree_png"] = str(base_dir / f"{stem}.scree.png")

    written = write_baing_artifacts(result, paths)

    # Scree plot is produced inside write_baing_artifacts? (Bai–Ng helper doesn’t draw.)
    # If you want a scree here, we can create it from eigenvalues:
    if args.scree and "scree_png" in paths:
        try:
            import matplotlib.pyplot as plt
            import numpy as np
            eig = result.eigenvalues.values
            x = range(1, len(eig) + 1)
            plt.figure(figsize=(7, 4.2))
            plt.plot(x, eig, marker="o")
            plt.xlabel("Component index k")
            plt.ylabel("Singular value")
            plt.title(f"Scree plot ({args.ic})")
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(paths["scree_png"], dpi=150)
            plt.close()
            written["scree_png"] = paths["scree_png"]
        except Exception:
            # Silent fallback if matplotlib missing in this environment
            pass

    # Console summary (ASCII-safe)
    print(
        f"[Bai-Ng/{args.ic}] {args.panel} {args.tag} "
        f"{args.method}:{args.label} -> r*={result.r_star} (r_max={args.r_max})"
    )
    for k, v in written.items():
        print(f"  wrote: {k}: {v}")


if __name__ == "__main__":
    main()
