#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

# Ensure src/ is on path if needed later
ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "Compare simple DFM RMSE across three panels "
            "(full vs reduced-stability vs reduced-vote) "
            "for the same (q, r, p) grid."
        )
    )

    ap.add_argument(
        "--full-grid",
        required=True,
        help="CSV with grid results for FULL panel (dfm_simple_grid_results.csv).",
    )
    ap.add_argument(
        "--stab-grid",
        required=True,
        help="CSV with grid results for reduced STABILITY panel.",
    )
    ap.add_argument(
        "--vote-grid",
        required=True,
        help="CSV with grid results for reduced VOTE panel.",
    )
    ap.add_argument(
        "--out-csv",
        required=True,
        help="Output CSV with merged RMSE and best_panel / best_rmse.",
    )

    return ap.parse_args()


def _load_grid(path: Path, col_name: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # minimal columns needed
    required = {"q", "p", "rmse_nowcast"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} missing required columns: {sorted(missing)}")

    # r might be present; keep if so
    cols = ["q", "p", "rmse_nowcast"]
    if "r" in df.columns:
        cols.insert(2, "r")

    df = df[cols].copy().rename(columns={"rmse_nowcast": col_name})
    return df


def main() -> None:
    args = parse_args()

    full_path = Path(args.full_grid)
    stab_path = Path(args.stab_grid)
    vote_path = Path(args.vote_grid)
    out_path = Path(args.out_csv)

    for p in [full_path, stab_path, vote_path]:
        if not p.exists():
            raise FileNotFoundError(p)

    df_full = _load_grid(full_path, col_name="rmse_full")
    df_stab = _load_grid(stab_path, col_name="rmse_stab")
    df_vote = _load_grid(vote_path, col_name="rmse_vote")

    # Decide merge keys: (q,p) or (q,r,p) depending on presence of r
    if "r" in df_full.columns and "r" in df_stab.columns and "r" in df_vote.columns:
        keys = ["q", "r", "p"]
    else:
        keys = ["q", "p"]

    df = (
        df_full
        .merge(df_stab, on=keys, how="inner")
        .merge(df_vote, on=keys, how="inner")
    )

    if df.empty:
        raise RuntimeError("Merged comparison table is empty. Check that grids share the same (q,r,p).")

    # Compute best panel and best RMSE
    rmse_cols = ["rmse_full", "rmse_stab", "rmse_vote"]
    df["best_panel"] = df[rmse_cols].idxmin(axis=1)
    df["best_rmse"] = df[rmse_cols].min(axis=1)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    print(f"[OK] wrote comparison CSV: {out_path}  shape={df.shape}")
    print("\nTop 10 specs by best_rmse:")
    print(df.sort_values("best_rmse").head(10))


if __name__ == "__main__":
    main()
