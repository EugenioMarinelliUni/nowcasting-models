#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Grid runner for variable preselection with aggregation support.
#
# Works with the refactored run_preselection.py:
#   - Same aggregation semantics (none / quarterly).
#   - Same underlying method wrappers (run_sis, run_tstat, run_lars_tscv).
#
# Grid definition via --grid-spec:
#   JSON object mapping hyperparameter name -> values:
#
#   - Single value:
#       {"min_features": 40, "max_features": 80}
#
#   - Explicit list of values:
#       {"dedup_tau": [0.95, 0.97, 0.99]}
#
#   - Range [min, max, step] (numeric):
#       {"dedup_tau": [0.95, 0.99, 0.02]}
#     expands to: 0.95, 0.97, 0.99 (inclusive).
#
# Outputs:
#   {variants_root}/{variant_name}/{panel_name}/{train_start}_{train_end}/
#     ├─ grid_results.csv
#     ├─ selected_vars.json                # BEST (canonical winner; includes params)
#     ├─ best_summary.yml|json
#     ├─ selection_summary.csv             # var-level frequencies (+ group)
#     ├─ selection_details.csv             # per (trial, variable) with params (+ group)
#     ├─ selection_intersections.json      # intersections + per-variable trial/config info
#     ├─ trial_0/
#     │    ├─ selected_vars.json
#     │    ├─ selected_vars_with_group.json
#     │    ├─ params.yml|json
#     │    └─ rank.csv
#     ├─ trial_1/
#     │    └─ ...
#     └─ _VERSION
# -----------------------------------------------------------------------------

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import pathlib
import re
import subprocess
from datetime import datetime, UTC
from typing import Any, Dict, List, Tuple

import pandas as pd

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

# Reuse core pieces from run_preselection.py
from scripts.preselection.run_preselection import (
    load_panel,
    load_target,
    clip_window,
    apply_aggregation_for_ranking,
    run_sis,
    run_tstat,
    run_lars_tscv,
    load_group_map,
)


# ---------------------------------------------------------------------
# Small helpers: slug, JSON/YAML writers, git commit, sha256
# ---------------------------------------------------------------------


def slug(s: Any) -> str:
    """
    Make a filesystem-friendly slug from an arbitrary object/string.
    """
    txt = str(s)
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", txt)


def write_json(path: pathlib.Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def write_text(path: pathlib.Path, txt: str) -> None:
    path.write_text(txt, encoding="utf-8")


def maybe_git_commit() -> str | None:
    """
    Return current git commit hash if available, else None.
    """
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
        )
        return out.decode("utf-8").strip()
    except Exception:
        return None


def sha256_of_file(path: str) -> str:
    """
    Compute SHA-256 hash of a file (hex string). Used for provenance.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------
# Scoring: mean |corr(X_j, y)| over selected vars (smaller loss is worse)
# ---------------------------------------------------------------------


def score_trial(
    panel_rank: pd.DataFrame,
    y_rank: pd.Series,
    selected: list[str],
    mode: str = "corr_mean",
) -> float:
    """
    Return a numeric loss; smaller is better.

    mode = "corr_mean":
        loss = -mean(|corr(X_j, y)| over selected vars)
        (i.e. higher correlation -> more negative -> better).
    """
    if mode == "corr_mean":
        X, y = panel_rank.align(y_rank, join="inner", axis=0)
        if isinstance(y, pd.DataFrame):
            y = y.iloc[:, 0]
        vals: List[float] = []
        for v in selected:
            if v not in X.columns:
                continue
            c = X[v].corr(y)
            if pd.notna(c):
                vals.append(abs(float(c)))
        if not vals:
            return 1e9
        return -float(sum(vals) / len(vals))
    return 1e9


# ---------------------------------------------------------------------
# Grid expansion: JSON spec -> list of param dicts
# ---------------------------------------------------------------------


def _expand_single_value(name: str, v: Any) -> List[Any]:
    """
    Interpret one hyperparameter entry from --grid-spec.

    Rules:
      - non-list -> [v]
      - list of 3 numeric items [min, max, step] -> numeric range (inclusive)
      - other list -> used as explicit values
    """
    if not isinstance(v, list):
        return [v]

    # Range [min, max, step] for numeric hyperparams
    if len(v) == 3 and all(isinstance(x, (int, float)) for x in v):
        start, stop, step = v
        if step <= 0:
            raise ValueError(f"Invalid step <= 0 for hyperparameter {name!r}: {v}")
        vals: List[float] = []
        cur = float(start)
        eps = abs(step) * 1e-9
        while cur <= float(stop) + eps:
            vals.append(cur)
            cur += float(step)
        if all(isinstance(x, int) for x in (start, step)):
            return [int(round(x)) for x in vals]
        return vals

    # Explicit list of values
    return list(v)


def expand_grid_spec(grid_spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Turn a JSON spec mapping param -> (value | list | [min,max,step]) into
    a list of hyperparameter combinations (Cartesian product).
    """
    if not grid_spec:
        return [{}]

    names = sorted(grid_spec.keys())
    value_lists: List[List[Any]] = []
    for name in names:
        vals = _expand_single_value(name, grid_spec[name])
        if not vals:
            raise ValueError(f"Grid for {name!r} produced no values.")
        value_lists.append(vals)

    combos: List[Dict[str, Any]] = []
    for product_vals in itertools.product(*value_lists):
        params = {name: val for name, val in zip(names, product_vals)}
        combos.append(params)
    return combos


# ---------------------------------------------------------------------
# Method dispatch for a single trial
# ---------------------------------------------------------------------


def run_single_trial(
    method: str,
    panel_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """
    Call the appropriate preselection wrapper from run_preselection.py.

    method ∈ {"sis", "tstat", "lars_tscv"}.
    params: hyperparameters passed through to the method.
    """
    if method == "sis":
        return run_sis(panel_rank, y_rank, params)
    if method in {"tstat", "t-stat", "t_stat"}:
        return run_tstat(panel_rank, y_rank, params)
    if method in {"lars_tscv", "lars-tscv"}:
        return run_lars_tscv(panel_rank, y_rank, params)
    raise ValueError(f"Unknown method {method!r}.")


# ---------------------------------------------------------------------
# Progress bar helper
# ---------------------------------------------------------------------


def print_progress(current: int, total: int) -> None:
    """
    Simple text progress bar to stdout, updated in-place using carriage return.
    """
    if total <= 0:
        return
    frac = current / total
    bar_len = 30
    filled = int(bar_len * frac)
    bar = "#" * filled + "-" * (bar_len - filled)
    msg = f"[grid] Progress: [{bar}] {current}/{total} ({frac*100:5.1f}%)"
    print("\r" + msg, end="", flush=True)


# ---------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Variable preselection grid (aggregation-aware, works with run_preselection.py)."
    )

    # Core I/O
    ap.add_argument("--panel-csv", required=True, help="TRAIN panel CSV path.")
    ap.add_argument("--target-csv", required=True, help="TRAIN target CSV path.")
    ap.add_argument("--panel-name", required=True, help="Logical panel id, e.g. 1960_noVIX.")
    ap.add_argument("--train-start", required=True, help="YYYY-MM-01 (train start).")
    ap.add_argument("--train-end", required=True, help="YYYY-MM-01 (train end).")

    # Variant family + output root
    ap.add_argument(
        "--variant-name",
        required=True,
        help="Family name (e.g. sis_grid, tstat_grid); trials stored under this folder.",
    )
    ap.add_argument(
        "--variants-root",
        default="data/metadata/variants",
        help="Root for all variants (grid results will be under this).",
    )

    # Method
    ap.add_argument(
        "--method",
        required=True,
        choices=["sis", "tstat", "t-stat", "t_stat", "lars_tscv", "lars-tscv"],
        help="Preselection method to grid over.",
    )

    # Hyperparameter grid
    ap.add_argument(
        "--grid-spec",
        required=True,
        help=(
            "JSON object mapping hyperparameter names to values / lists / [min,max,step]. "
            "Example (SIS): "
            "'{\"min_features\": [30,40], \"max_features\": [60,80], "
            "  \"dedup_tau\": [0.95,0.99,0.02], \"sis_tau\": [0.0], \"sis_topn\": [0]}'"
        ),
    )

    # Aggregation (reuse semantics of run_preselection: none / quarterly)
    ap.add_argument(
        "--agg-mode",
        default="quarterly",
        choices=["none", "quarterly", "eoq"],
        help="Aggregation mode for X before preselection. 'eoq' is treated as 'quarterly'.",
    )
    ap.add_argument(
        "--agg-rule-path",
        default="",
        help="JSON with aggregation rules (Linzenich–Meunier style: has 'series_rules').",
    )
    ap.add_argument(
        "--agg-default-rule",
        default=None,
        help=(
            "Optional fallback aggregation rule (e.g. sum3m or mean3m) for series not in agg-rule-path. "
            "If omitted and a series is missing, an error is raised."
        ),
    )

    # Group map (for annotations and intersections)
    ap.add_argument(
        "--group-map-path",
        default="",
        help=(
            "Optional JSON mapping variable -> group "
            "(e.g. data/metadata/variable_group_map.json). "
            "Used to annotate trial artifacts and global selection summaries."
        ),
    )

    # Scoring
    ap.add_argument(
        "--score-mode",
        default="corr_mean",
        help="Score mode for trials (currently only 'corr_mean' is implemented).",
    )
    ap.add_argument(
        "--maximize",
        action="store_true",
        help="If set, higher score is better (internally we still minimize a loss).",
    )

    args = ap.parse_args()

    # -----------------------------------------------------------------
    # Load data + restrict to TRAIN window
    # -----------------------------------------------------------------
    panel_full = load_panel(args.panel_csv)
    y_full = load_target(args.target_csv)

    panel_train = clip_window(panel_full, args.train_start, args.train_end)
    y_train = clip_window(y_full, args.train_start, args.train_end)
    if isinstance(y_train, pd.DataFrame):
        y_train = y_train.iloc[:, 0]

    if panel_train.empty:
        raise ValueError("Training panel is empty after clipping.")
    if y_train.empty:
        raise ValueError("Training target is empty after clipping.")

    # -----------------------------------------------------------------
    # Aggregation map + (X_rank, y_rank)
    # -----------------------------------------------------------------
    agg_mode = args.agg_mode
    if agg_mode == "eoq":
        agg_mode = "quarterly"

    agg_map: Dict[str, Any] = {}
    agg_default_rule: str | None = args.agg_default_rule

    if agg_mode == "quarterly":
        if not args.agg_rule_path and agg_default_rule is None:
            raise ValueError(
                "quarterly agg-mode requires either --agg-rule-path or --agg-default-rule."
            )

        if args.agg_rule_path:
            cfg = json.loads(pathlib.Path(args.agg_rule_path).read_text(encoding="utf-8"))
            if "series_rules" in cfg:
                agg_map = cfg["series_rules"]
            else:
                agg_map = cfg
        else:
            agg_map = {}  # rely entirely on agg_default_rule

    elif agg_mode == "none":
        agg_map = {}

    panel_rank, y_rank = apply_aggregation_for_ranking(
        panel_train=panel_train,
        y_train=y_train,
        agg_map=agg_map,
        agg_default_rule=agg_default_rule,
        agg_mode=agg_mode,
    )

    # -----------------------------------------------------------------
    # Group map (optional)
    # -----------------------------------------------------------------
    group_map: Dict[str, str] | None = None
    if args.group_map_path:
        group_map = load_group_map(args.group_map_path)

    # -----------------------------------------------------------------
    # Parse grid spec and expand combinations
    # -----------------------------------------------------------------
    try:
        grid_spec = json.loads(args.grid_spec)
        if not isinstance(grid_spec, dict):
            raise ValueError("grid-spec must be a JSON object.")
    except Exception as e:
        raise ValueError(f"Invalid JSON for --grid-spec: {e}") from e

    param_combos = expand_grid_spec(grid_spec)
    if not param_combos:
        raise ValueError("Expanded grid is empty; check --grid-spec.")

    # -----------------------------------------------------------------
    # Output base directory
    # -----------------------------------------------------------------
    span = f"{args.train_start.replace('-', '_')}_{args.train_end.replace('-', '_')}"
    base_dir = (
        pathlib.Path(args.variants_root)
        / slug(args.variant_name)
        / slug(args.panel_name)
        / span
    )
    base_dir.mkdir(parents=True, exist_ok=True)

    trials: List[Dict[str, Any]] = []
    trial_id = 0

    # For global intersections
    selection_counts: Dict[str, int] = {}
    selection_trials: Dict[str, List[int]] = {}
    selection_configs: Dict[str, List[Dict[str, Any]]] = {}

    # For detailed table: one record per (trial, variable) when selected
    selection_detail_records: List[Dict[str, Any]] = []

    total_trials = len(param_combos)
    print(f"[grid] total trials: {total_trials}")

    # -----------------------------------------------------------------
    # Run grid (with progress bar)
    # -----------------------------------------------------------------
    for idx, params in enumerate(param_combos, start=1):
        print_progress(idx, total_trials)

        rank_df, selected_vars, meta = run_single_trial(
            method=args.method,
            panel_rank=panel_rank,
            y_rank=y_rank,
            params=params,
        )

        selected = list(selected_vars)
        raw_score = score_trial(panel_rank, y_rank, selected, mode=args.score_mode)
        loss = -raw_score if args.maximize else raw_score

        # Update global counts and detail records
        for v in selected:
            selection_counts[v] = selection_counts.get(v, 0) + 1
            selection_trials.setdefault(v, []).append(trial_id)
            selection_configs.setdefault(v, []).append(dict(params))

            rec: Dict[str, Any] = {
                "trial_id": trial_id,
                "variable": v,
                "group": group_map.get(str(v), None) if group_map is not None else None,
            }
            # Flatten params into columns param_<name>
            for k, val in params.items():
                rec[f"param_{k}"] = val
            selection_detail_records.append(rec)

        tdir = base_dir / f"trial_{trial_id}"
        tdir.mkdir(parents=True, exist_ok=True)

        # Per-trial artifacts
        # 1) selected_vars.json (legacy format)
        write_json(tdir / "selected_vars.json", {"selected": sorted(selected)})

        # 2) selected_vars_with_group.json (if group_map available)
        if group_map is not None:
            sel_groups = {v: group_map.get(str(v), None) for v in selected}
            write_json(tdir / "selected_vars_with_group.json", sel_groups)

        # 3) rank.csv (+ group column if group_map available)
        rank_to_save = rank_df.copy()
        if group_map is not None and not rank_to_save.empty:
            if "variable" in rank_to_save.columns:
                vars_series = rank_to_save["variable"].astype(str)
            else:
                vars_series = pd.Series(rank_to_save.index.astype(str), name="variable")
                rank_to_save.insert(0, "variable", vars_series)
            rank_to_save["group"] = [
                group_map.get(str(v), None) for v in vars_series
            ]
        rank_to_save.to_csv(tdir / "rank.csv", index=False)

        # 4) params + meta
        params_payload = {
            "method": args.method,
            "params": params,
            "meta": meta,
        }
        if yaml is not None:
            with open(tdir / "params.yml", "w", encoding="utf-8") as f:
                yaml.safe_dump(params_payload, f, sort_keys=True)
        else:
            write_json(tdir / "params.json", params_payload)

        trials.append(
            {
                "trial_id": trial_id,
                "method": args.method,
                "params": params,
                "loss": float(loss),
                "selected": selected,
                "rank_path": str(tdir / "rank.csv"),
            }
        )
        trial_id += 1

    # ensure progress line is terminated
    if total_trials > 0:
        print()  # newline after progress bar

    # -----------------------------------------------------------------
    # Grid table + best result
    # -----------------------------------------------------------------
    grid_df = pd.DataFrame(trials).sort_values("loss", ascending=True).reset_index(drop=True)
    grid_df.to_csv(base_dir / "grid_results.csv", index=False)

    best = grid_df.iloc[0].to_dict()
    best_selected = sorted(best["selected"])
    best_params = best["params"]

    # Canonical winner selected_vars.json (top level)
    best_payload = {
        "selected": best_selected,
        "from_trial": int(best["trial_id"]),
        "params": best_params,
    }
    write_json(base_dir / "selected_vars.json", best_payload)

    # -----------------------------------------------------------------
    # Global selection summary + intersections + detailed table
    # -----------------------------------------------------------------
    n_trials = len(trials)

    if selection_counts:
        # 1) selection_summary.csv
        rows = []
        for v, cnt in selection_counts.items():
            frac = cnt / n_trials
            g = group_map.get(str(v), None) if group_map is not None else None
            rows.append(
                {
                    "variable": v,
                    "count_selected": int(cnt),
                    "frac_selected": float(frac),
                    "group": g,
                }
            )
        summary_df = pd.DataFrame(rows).sort_values(
            ["count_selected", "variable"], ascending=[False, True]
        )
        summary_df.to_csv(base_dir / "selection_summary.csv", index=False)

        # 2) selection_details.csv (per trial, per selected variable, with params)
        details_df = pd.DataFrame(selection_detail_records)
        details_df.to_csv(base_dir / "selection_details.csv", index=False)

        # 3) selection_intersections.json with richer per-variable info
        intersection_all = sorted(
            v for v, cnt in selection_counts.items() if cnt == n_trials
        )
        intersection_any = sorted(selection_counts.keys())

        per_variable: Dict[str, Any] = {}
        for v in intersection_any:
            cnt = selection_counts[v]
            frac = cnt / n_trials
            g = group_map.get(str(v), None) if group_map is not None else None

            # Deduplicate configs for this variable (configs are small dicts)
            seen_cfgs: List[Dict[str, Any]] = []
            for cfg in selection_configs.get(v, []):
                if cfg not in seen_cfgs:
                    seen_cfgs.append(cfg)

            per_variable[v] = {
                "count_selected": int(cnt),
                "frac_selected": float(frac),
                "group": g,
                "trials": sorted(selection_trials.get(v, [])),
                "configs": seen_cfgs,
            }

        intersections_payload = {
            "n_trials": n_trials,
            "intersection_all": intersection_all,
            "intersection_any": intersection_any,
            "variables": per_variable,
        }
        write_json(base_dir / "selection_intersections.json", intersections_payload)

    # -----------------------------------------------------------------
    # Summary metadata
    # -----------------------------------------------------------------
    best_summary = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "panel": args.panel_name,
        "train_start": args.train_start,
        "train_end": args.train_end,
        "variant_family": args.variant_name,
        "method": args.method,
        "n_trials": int(len(grid_df)),
        "score_mode": args.score_mode,
        "maximize": bool(args.maximize),
        "best": {
            "trial_id": int(best["trial_id"]),
            "params": best_params,
            "loss": float(best["loss"]),
        },
        "agg_mode": agg_mode,
        "agg_rule_path": args.agg_rule_path or None,
        "agg_default_rule": args.agg_default_rule,
        "git_commit": maybe_git_commit(),
        "schema": {"layout": "variants_layout_v4", "selected_json": "selected_array_v2"},
        "inputs": {
            "panel_csv": args.panel_csv,
            "target_csv": args.target_csv,
            "panel_csv_sha256": sha256_of_file(args.panel_csv),
            "target_csv_sha256": sha256_of_file(args.target_csv),
        },
    }
    if yaml is not None:
        with open(base_dir / "best_summary.yml", "w", encoding="utf-8") as f:
            yaml.safe_dump(best_summary, f, sort_keys=False, allow_unicode=True)
    else:
        write_json(base_dir / "best_summary.json", best_summary)

    write_text(base_dir / "_VERSION", "variants_layout_v4\n")

    print(f"[grid] wrote results to: {base_dir}")
    print(f"[grid] BEST selected_vars.json -> {base_dir / 'selected_vars.json'}")


if __name__ == "__main__":
    main()
