from __future__ import annotations
import argparse
from pathlib import Path

import pandas as pd

from dfm_pipeline.factors.gct2023 import GCTOptions, run_gct_selection, write_gct_artifacts


def _discover_preselected_csv(panel: str, tag: str, method: str, label: str) -> Path:
    rel = f"dataset/{panel}/preselect/{method}/" \
          f"X_panel_z__{panel}__{tag}__preselect-{method}__{label}.csv"
    p = Path(rel)
    if not p.exists():
        raise FileNotFoundError(f"preselected panel not found: {p}")
    return p


def main() -> None:
    ap = argparse.ArgumentParser(description="Guo–Chen–Tang (2023) adaptive IC for static factor selection")
    ap.add_argument("--panel", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--method", required=True)
    ap.add_argument("--label", required=True)

    ap.add_argument("--variant", default="ICprop",
                    choices=["PCprop", "ICprop", "PCp1", "ICp1", "PCp2", "ICp2"])
    ap.add_argument("--r-max", type=int, default=12)

    ap.add_argument("--standardize", type=str, default="false")
    ap.add_argument("--demean", type=str, default="true")
    ap.add_argument("--detrend", type=str, default="none")

    ap.add_argument("--s0", type=float, default=0.5)
    ap.add_argument("--c-star", type=float, default=2.0)

    ap.add_argument("--run-label", required=True)

    ap.add_argument("--scree", action="store_true")
    ap.add_argument("--save-loadings", action="store_true")
    ap.add_argument("--save-scores", action="store_true")

    args = ap.parse_args()

    std = args.standardize.lower() == "true"
    dm = args.demean.lower() == "true"

    X_path = _discover_preselected_csv(args.panel, args.tag, args.method, args.label)
    X_df = pd.read_csv(X_path, index_col=0)

    opts = GCTOptions(
        r_max=args.r_max,
        standardize=std,
        demean=dm,
        detrend="none",
        variant=args.variant,
        s0=args.s0,
        c_star=args.c_star,
        save_factors=True,
        save_loadings=args.save_loadings,
        save_scores=args.save_scores,
        write_scree=args.scree,
    )

    sel = run_gct_selection(X_df, opts)

    out_dir = Path(
        f"data/factors/{args.panel}/{args.tag}/{args.method}/{args.label}/gct2023/{args.variant}/{args.run_label}"
    )
    write_gct_artifacts(
        out_dir,
        X_df,
        sel,
        write_scree=args.scree,
        save_factors=True,
        save_loadings=args.save_loadings,
        save_scores=args.save_scores,
    )

    print(
        f"[GCT2023] {args.panel} {args.tag} {args.method}:{args.label} "
        f"-> k*={sel['k_star']} (r_max={opts.r_max}), variant={args.variant}, "
        f"s0={opts.s0}, c*={opts.c_star}"
    )
    print(f"  wrote: grid_csv:     {out_dir/'grid.csv'}")
    print(f"  wrote: summary_json: {out_dir/'summary.json'}")
    print(f"  wrote: eigen_csv:    {out_dir/'eigen.csv'}")
    print(f"  wrote: factors_csv:  {out_dir/'factors.csv'}")
    print(f"  wrote: loadings_csv: {out_dir/'loadings.csv'}")
    print(f"  wrote: scores_csv:   {out_dir/'scores.csv'}")
    if args.scree:
        print(f"  wrote: scree_csv:    {out_dir/'scree.csv'}")


if __name__ == "__main__":
    main()
