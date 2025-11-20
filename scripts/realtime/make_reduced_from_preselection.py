#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

import pandas as pd


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def fmt_span_from_index(idx: pd.Index) -> str:
    if idx.empty:
        return "NA_NA_NA_NA"
    dt_idx = pd.DatetimeIndex(idx)
    s = dt_idx.min()
    e = dt_idx.max()
    return f"{s.year:04d}_{s.month:02d}_{e.year:04d}_{e.month:02d}"


def load_selected_vars(path: str) -> List[str]:
    """
    Accepts the same formats as the preselection artifacts:
      - ["var1", "var2", ...]
      - {"selected": ["var1", "var2"]}
      - {"var1": true, "var2": false, ...}
    """
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(obj, list):
        return list(obj)
    if isinstance(obj, dict):
        if "selected" in obj and isinstance(obj["selected"], list):
            return list(obj["selected"])
        return [k for k, v in obj.items() if bool(v)]
    raise ValueError("selected_vars JSON must be list, {'selected': [...]}, or {var: bool}.")


def read_panel_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["Date"])
    df = df.set_index("Date").sort_index()
    return df


def read_target_csv(path: str) -> pd.Series:
    df = pd.read_csv(path, parse_dates=["Date"])
    df = df.set_index("Date").sort_index()
    # assume single column with target name
    cols = [c for c in df.columns if c.lower() not in ("date", "time", "timestamp")]
    if len(cols) != 1:
        raise ValueError(f"Target CSV {path} must have exactly one value column.")
    return df[cols[0]]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build reduced train/OOS panels from baseline outputs and selected_vars.json."
    )

    ap.add_argument("--panel-name", required=True, help="Logical panel id, e.g. 1960_noVIX.")
    ap.add_argument("--y-name", required=True, help="Logical target id, e.g. gdp_A191RL1Q225SBEA.")
    ap.add_argument(
        "--variant-name",
        required=True,
        help="Name of the preselection variant, used as suffix in output filenames.",
    )

    # baseline standardized artifacts (outputs of make_train_oos_from_raw.py)
    ap.add_argument(
        "--train-panel",
        required=True,
        help="Path to standardized train panel CSV (from make_train_oos_from_raw.py).",
    )
    ap.add_argument(
        "--oos-panel",
        default="",
        help="Path to standardized OOS panel CSV (from make_train_oos_from_raw.py). Optional if no OOS.",
    )
    ap.add_argument(
        "--y-train",
        required=True,
        help="Path to standardized train target CSV (from make_train_oos_from_raw.py).",
    )
    ap.add_argument(
        "--y-oos",
        default="",
        help="Path to standardized OOS target CSV (from make_train_oos_from_raw.py). Optional if no OOS.",
    )

    # selected variables
    ap.add_argument(
        "--vars-json",
        required=True,
        help="Path to selected_vars.json produced by preselection.",
    )

    args = ap.parse_args()

    # load baseline panels / targets
    X_train = read_panel_csv(args.train_panel)
    y_train = read_target_csv(args.y_train)

    has_oos_panel = bool(args.oos_panel)
    has_oos_target = bool(args.y_oos)

    if has_oos_panel:
        X_oos = read_panel_csv(args.oos_panel)
    else:
        X_oos = pd.DataFrame()

    if has_oos_target:
        y_oos = read_target_csv(args.y_oos)
    else:
        y_oos = pd.Series(dtype=float)

    # load selected variables
    selected = load_selected_vars(args.vars_json)

    # restrict to selected variables (intersect with existing columns to be robust)
    selected_in_panel = [c for c in selected if c in X_train.columns]
    if not selected_in_panel:
        raise ValueError(
            f"No selected variables found in train panel columns. "
            f"First few selected: {selected[:5]}"
        )

    X_train_red = X_train[selected_in_panel]
    if not X_oos.empty:
        X_oos_red = X_oos[selected_in_panel]
    else:
        X_oos_red = X_oos

    # derive spans from indexes
    train_span = fmt_span_from_index(X_train_red.index)
    oos_span = fmt_span_from_index(X_oos_red.index) if not X_oos_red.empty else None

    # output paths
    panel_root = Path("dataset") / args.panel_name
    train_dir = panel_root / "train_panels"
    oos_dir = panel_root / "oos_panels"
    ensure_dir(train_dir)
    ensure_dir(oos_dir)

    y_root = Path("data") / "targets" / args.y_name
    ensure_dir(y_root)

    train_panel_out = (
        train_dir / f"{args.panel_name}__train_{train_span}__{args.variant_name}.csv"
    )
    oos_panel_out = (
        oos_dir / f"{args.panel_name}__oos_{oos_span}__{args.variant_name}.csv"
        if oos_span is not None
        else None
    )

    y_train_out = (
        y_root / f"{args.y_name}__train_{train_span}__{args.variant_name}.csv"
    )
    y_oos_out = (
        y_root / f"{args.y_name}__oos_{oos_span}__{args.variant_name}.csv"
        if oos_span is not None and not y_oos.empty
        else None
    )

    # write reduced X
    X_train_red.to_csv(train_panel_out, index_label="Date")
    if oos_panel_out is not None and not X_oos_red.empty:
        X_oos_red.to_csv(oos_panel_out, index_label="Date")

    # write y copies (unchanged, but variant-labeled)
    y_train.to_frame(args.y_name).to_csv(y_train_out, index_label="Date")
    if y_oos_out is not None:
        y_oos.to_frame(args.y_name).to_csv(y_oos_out, index_label="Date")

    # simple log
    print(f"[train] X reduced: {train_panel_out}")
    print(
        f"[oos]   X reduced: {oos_panel_out if (oos_panel_out is not None and not X_oos_red.empty) else '(no OOS rows)'}"
    )
    print(f"[train] y (copied): {y_train_out}")
    print(
        f"[oos]   y (copied): {y_oos_out if (y_oos_out is not None and not y_oos.empty) else '(no OOS rows)'}"
    )
    print(f"[vars]  used: {args.vars_json}")


if __name__ == "__main__":
    main()
