#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def find_rmse_col(df: pd.DataFrame, label: str) -> str:
    cand = [c for c in df.columns if "rmse" in c.lower()]
    if not cand:
        raise SystemExit(f"No RMSE-like column in {label}: {df.columns.tolist()}")
    if len(cand) > 1:
        print(f"[warn] {label}: multiple RMSE-like columns {cand}, using {cand[0]}")
    return cand[0]


def find_spec_col(df: pd.DataFrame, label: str) -> str:
    cand = [c for c in df.columns if "best_spec" in c.lower()]
    if not cand:
        raise SystemExit(f"No best_spec-like column in {label}: {df.columns.tolist()}")
    if len(cand) > 1:
        print(f"[warn] {label}: multiple best_spec-like columns {cand}, using {cand[0]}")
    return cand[0]


def detect_key_monthly(old: pd.DataFrame, new: pd.DataFrame) -> list[str]:
    if "Date" in old.columns and "Date" in new.columns:
        old["Date"] = pd.to_datetime(old["Date"])
        new["Date"] = pd.to_datetime(new["Date"])
        return ["Date"]
    if {"year", "month"}.issubset(old.columns) and {"year", "month"}.issubset(new.columns):
        return ["year", "month"]
    raise SystemExit(
        f"No common monthly key; old={old.columns.tolist()}, new={new.columns.tolist()}"
    )


def detect_key_quarterly(old: pd.DataFrame, new: pd.DataFrame) -> list[str]:
    if {"year", "quarter"}.issubset(old.columns) and {"year", "quarter"}.issubset(new.columns):
        return ["year", "quarter"]
    if "Date" in old.columns and "Date" in new.columns:
        old["Date"] = pd.to_datetime(old["Date"])
        new["Date"] = pd.to_datetime(new["Date"])
        return ["Date"]
    raise SystemExit(
        f"No common quarterly key; old={old.columns.tolist()}, new={new.columns.tolist()}"
    )


def compare_best_specs(
    old_path: Path,
    new_path: Path,
    out_path: Path,
    key_kind: str,
) -> None:
    if not old_path.exists():
        raise SystemExit(f"OLD file not found: {old_path}")
    if not new_path.exists():
        raise SystemExit(f"NEW file not found: {new_path}")

    old = pd.read_csv(old_path)
    new = pd.read_csv(new_path)

    if key_kind == "monthly":
        key = detect_key_monthly(old, new)
    elif key_kind == "quarterly":
        key = detect_key_quarterly(old, new)
    else:
        raise SystemExit(f"Unknown key_kind={key_kind!r}")

    rmse_old_col = find_rmse_col(old, "old")
    rmse_new_col = find_rmse_col(new, "new")

    spec_old_col = find_spec_col(old, "old")
    spec_new_col = find_spec_col(new, "new")

    old_sub = old[key + [spec_old_col, rmse_old_col]].copy()
    old_sub = old_sub.rename(columns={spec_old_col: "spec_old", rmse_old_col: "rmse_old"})

    new_sub = new[key + [spec_new_col, rmse_new_col]].copy()
    new_sub = new_sub.rename(columns={spec_new_col: "spec_new", rmse_new_col: "rmse_new"})

    m = old_sub.merge(new_sub, on=key, how="inner")

    m["diff_new_minus_old"] = m["rmse_new"] - m["rmse_old"]

    def winner(row) -> str:
        if row["diff_new_minus_old"] < 0:
            return "new_better"
        elif row["diff_new_minus_old"] > 0:
            return "old_better"
        else:
            return "tie"

    m["which_model_better"] = m.apply(winner, axis=1)

    out_cols = key + [
        "spec_old",
        "rmse_old",
        "spec_new",
        "rmse_new",
        "diff_new_minus_old",
        "which_model_better",
    ]
    out = m[out_cols].copy().sort_values(key)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"[OK] wrote comparison CSV: {out_path}  shape={out.shape}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Compare old vs new best-spec RMSE, monthly and/or quarterly."
    )

    # monthly
    ap.add_argument(
        "--old-monthly",
        help="Old best-month CSV, e.g. dfm_simple_full_baseline_all_specs_eval2000_rmse_wide__best_month.csv",
    )
    ap.add_argument(
        "--new-monthly",
        help="New best-month CSV, e.g. ...mu1990_1999..._rmse_wide__best_month.csv",
    )
    ap.add_argument(
        "--out-monthly",
        help="Output CSV for monthly comparison.",
    )
    ap.add_argument(
        "--do-monthly",
        action="store_true",
        help="If set, perform monthly comparison (requires --old-monthly, --new-monthly, --out-monthly).",
    )

    # quarterly
    ap.add_argument(
        "--old-quarterly",
        help="Old best-quarter CSV, e.g. dfm_simple_full_baseline_all_specs_eval2000_rmse_wide__best_quarter.csv",
    )
    ap.add_argument(
        "--new-quarterly",
        help="New best-quarter CSV, e.g. ...mu1990_1999..._rmse_wide__best_quarter.csv",
    )
    ap.add_argument(
        "--out-quarterly",
        help="Output CSV for quarterly comparison.",
    )
    ap.add_argument(
        "--do-quarterly",
        action="store_true",
        help="If set, perform quarterly comparison (requires --old-quarterly, --new-quarterly, --out-quarterly).",
    )

    return ap.parse_args()


def main() -> None:
    args = parse_args()

    ran_any = False

    # monthly block
    if args.do_monthly:
        if not (args.old_monthly and args.new_monthly and args.out_monthly):
            raise SystemExit(
                "Monthly comparison requested (--do-monthly) but one of "
                "--old-monthly/--new-monthly/--out-monthly is missing."
            )
        compare_best_specs(
            old_path=Path(args.old_monthly),
            new_path=Path(args.new_monthly),
            out_path=Path(args.out_monthly),
            key_kind="monthly",
        )
        ran_any = True

    # quarterly block
    if args.do_quarterly:
        if not (args.old_quarterly and args.new_quarterly and args.out_quarterly):
            raise SystemExit(
                "Quarterly comparison requested (--do-quarterly) but one of "
                "--old-quarterly/--new-quarterly/--out-quarterly is missing."
            )
        compare_best_specs(
            old_path=Path(args.old_quarterly),
            new_path=Path(args.new_quarterly),
            out_path=Path(args.out_quarterly),
            key_kind="quarterly",
        )
        ran_any = True

    if not ran_any:
        raise SystemExit(
            "Nothing to do: set at least one of --do-monthly or --do-quarterly "
            "and provide the corresponding file arguments."
        )


if __name__ == "__main__":
    main()
