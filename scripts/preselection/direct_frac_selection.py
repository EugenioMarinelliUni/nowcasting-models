#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def load_frac_table(path: str | Path) -> pd.DataFrame:
    """
    Load selection_comparison_..._frac.csv.
    Expected columns:
      - variable
      - sis_frac
      - tstat_frac
      - lars_frac
    Optional:
      - group
    """
    p = Path(path)
    df = pd.read_csv(p)

    required = ["variable", "sis_frac", "tstat_frac", "lars_frac"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"{p} is missing required column {col!r}.")

    if "group" not in df.columns:
        df["group"] = "unknown"

    # Ensure numeric fracs, fill missing with 0
    for col in ["sis_frac", "tstat_frac", "lars_frac"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def stability_rule(df: pd.DataFrame, stab_threshold: float) -> pd.DataFrame:
    """
    Minimal tuning method:
      stability = (sis_frac + tstat_frac + lars_frac) / 3
      keep if stability >= stab_threshold
    """
    df = df.copy()
    df["stability"] = (
        df["sis_frac"] + df["tstat_frac"] + df["lars_frac"]
    ) / 3.0

    mask = df["stability"] >= float(stab_threshold)
    sel = df[mask].copy()

    # sort by stability descending, then variable
    sel = sel.sort_values(["stability", "variable"], ascending=[False, True])
    return sel


def vote_rule(
    df: pd.DataFrame,
    sis_thr: float,
    tstat_thr: float,
    lars_thr: float,
    min_votes: int,
) -> pd.DataFrame:
    """
    Stricter agreement method:

      sel_sis   = sis_frac   >= sis_thr
      sel_tstat = tstat_frac >= tstat_thr
      sel_lars  = lars_frac  >= lars_thr

      votes = sel_sis + sel_tstat + sel_lars
      keep if votes >= min_votes

    Also computes 'stability' for inspection.
    """
    df = df.copy()

    sel_sis = df["sis_frac"] >= float(sis_thr)
    sel_tst = df["tstat_frac"] >= float(tstat_thr)
    sel_lrs = df["lars_frac"] >= float(lars_thr)

    df["sel_sis"] = sel_sis
    df["sel_tstat"] = sel_tst
    df["sel_lars"] = sel_lrs

    df["votes"] = (
        sel_sis.astype(int)
        + sel_tst.astype(int)
        + sel_lrs.astype(int)
    )

    df["stability"] = (
        df["sis_frac"] + df["tstat_frac"] + df["lars_frac"]
    ) / 3.0

    mask = df["votes"] >= int(min_votes)
    sel = df[mask].copy()

    sel = sel.sort_values(
        ["votes", "stability", "variable"],
        ascending=[False, False, True],
    )
    return sel


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Direct selection from *_frac.csv using either a stability threshold "
            "or a vote rule without top-K."
        )
    )

    ap.add_argument(
        "--comparison-csv",
        required=True,
        help="selection_comparison_..._frac.csv (with sis_frac, tstat_frac, lars_frac).",
    )
    ap.add_argument(
        "--out-csv",
        required=True,
        help="Where to write the selected subset.",
    )
    ap.add_argument(
        "--mode",
        required=True,
        choices=["stability", "vote"],
        help="stability = minimal tuning, vote = stricter agreement.",
    )

    # stability-mode hyperparameter
    ap.add_argument(
        "--stab-threshold",
        type=float,
        default=0.4,
        help="Stability threshold for mode=stability (default: 0.4).",
    )

    # vote-mode hyperparameters
    ap.add_argument(
        "--sis-threshold",
        type=float,
        default=0.5,
        help="sis_frac threshold for mode=vote (default: 0.5).",
    )
    ap.add_argument(
        "--tstat-threshold",
        type=float,
        default=0.5,
        help="tstat_frac threshold for mode=vote (default: 0.5).",
    )
    ap.add_argument(
        "--lars-threshold",
        type=float,
        default=0.5,
        help="lars_frac threshold for mode=vote (default: 0.5).",
    )
    ap.add_argument(
        "--min-votes",
        type=int,
        default=2,
        help="Minimum number of methods that must vote yes (default: 2).",
    )

    args = ap.parse_args()

    df = load_frac_table(args.comparison_csv)

    if args.mode == "stability":
        sel = stability_rule(df, stab_threshold=args.stab_threshold)
    else:
        sel = vote_rule(
            df,
            sis_thr=args.sis_threshold,
            tstat_thr=args.tstat_threshold,
            lars_thr=args.lars_threshold,
            min_votes=args.min_votes,
        )

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sel.to_csv(out_path, index=False)
    print(f"[direct_selection] mode={args.mode} -> {out_path} (n={len(sel)})")


if __name__ == "__main__":
    main()
