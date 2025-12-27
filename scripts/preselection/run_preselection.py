#!/usr/bin/env python3
# Supports methods: sis, tstat, tstat_lm, lars_tscv, lars_lm
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, overload, Union, cast

import pandas as pd

from src.dfm_pipeline.preselection.preselect_sis import run_sis_preselection
from src.dfm_pipeline.preselection.preselect_tstat import run_tstat_preselection
from src.dfm_pipeline.preselection.preselect_lars import run_lars_tscv_preselection
from src.dfm_pipeline.preselection.preselect_tstat_lm import run_tstat_lm_preselection
from src.dfm_pipeline.preselection.preselect_lars_lm import run_lars_lm_preselection


# ---------------------------------------------------------------------
# Dataclasses and small helpers
# ---------------------------------------------------------------------


@dataclass
class PreselectionInfo:
    panel_name: str
    method: str
    variant_name: str
    train_start: str
    train_end: str
    agg_mode: str
    agg_rule_path: str | None
    agg_default_rule: str | None
    n_obs: int
    n_features_in: int
    n_features_selected: int
    method_kwargs: Dict[str, Any]


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def fmt_span_full(start: str, end: str) -> str:
    s = pd.to_datetime(start)
    e = pd.to_datetime(end)
    # YYYY_MM_DD_YYYY_MM_DD
    return (
        f"{s.year:04d}_{s.month:02d}_{s.day:02d}_"
        f"{e.year:04d}_{e.month:02d}_{e.day:02d}"
    )


@overload
def clip_window(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame: ...
@overload
def clip_window(df: pd.Series, start: str, end: str) -> pd.Series: ...


def clip_window(df: Union[pd.DataFrame, pd.Series], start: str, end: str):
    """
    Restrict DataFrame/Series to [start, end] on the index.

    Overloads let the type-checker infer the correct return type
    (DataFrame vs Series) based on the input.
    """
    start_ts = pd.to_datetime(start)
    end_ts = pd.to_datetime(end)
    return df.loc[(df.index >= start_ts) & (df.index <= end_ts)]


def load_panel(panel_csv: str) -> pd.DataFrame:
    df = pd.read_csv(panel_csv)
    first = df.columns[0]
    df[first] = pd.to_datetime(df[first])
    df = df.set_index(first).sort_index()
    return df


def load_target(target_csv: str) -> pd.Series:
    df = pd.read_csv(target_csv)
    first = df.columns[0]
    df[first] = pd.to_datetime(df[first])
    df = df.set_index(first).sort_index()
    ycols = [c for c in df.columns if c.lower() not in ("date", "time", "timestamp")]
    if not ycols:
        raise ValueError("Target CSV must contain a value column.")
    y = df[ycols[0]]
    if isinstance(y, pd.DataFrame):
        y = y.iloc[:, 0]
    return y


def load_group_map(path: str) -> Dict[str, str]:
    """
    Load a mapping var -> group from JSON.

    Accepts either:
      { "RPI": "Output & Income", ... }
    or:
      { "groups": { "RPI": "Output & Income", ... } }
    """
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(obj, dict):
        if "groups" in obj and isinstance(obj["groups"], dict):
            return {str(k): str(v) for k, v in obj["groups"].items()}
        # assume flat mapping var -> group
        return {str(k): str(v) for k, v in obj.items()}
    raise ValueError(f"Unsupported group map format in {path!r}")


# ---------------------------------------------------------------------
# Quarterly aggregation
# ---------------------------------------------------------------------


def aggregate_series_quarterly(
    series: pd.Series,
    rule: str,
) -> pd.Series:
    """
    Aggregate a monthly series to quarterly according to `rule`.

    Assumes:
    - series index is a DatetimeIndex with monthly dates (typically month-end).
    - rule ∈ {"sum3m", "mean3m"} in your current JSON.
    """
    if series.empty:
        return series.copy()

    idx = pd.DatetimeIndex(series.index)
    per = idx.to_period("Q")

    if rule == "sum3m":
        xq = series.groupby(per).sum()
    elif rule == "mean3m":
        xq = series.groupby(per).mean()
    else:
        raise ValueError(f"Unsupported aggregation rule: {rule!r}")

    # Cast index to PeriodIndex for type-checker, then to quarterly timestamps
    per_idx = cast("pd.PeriodIndex", xq.index)
    xq.index = per_idx.to_timestamp(how="start")

    return xq


def aggregate_panel_quarterly(
    panel_train: pd.DataFrame,
    agg_map: Dict[str, Any],
    agg_default_rule: str | None,
) -> pd.DataFrame:
    """
    Apply quarterly aggregation to each column in the monthly panel.

    Parameters
    ----------
    panel_train : monthly standardized panel (train), index = monthly dates.
    agg_map : mapping series_name -> rule info
        For your JSON, this is cfg["series_rules"], so each value is a dict
        like {"type": "return", "rule": "sum3m", "group": "..."}.
        If a value is a primitive string, it is taken as the rule itself.
    agg_default_rule : if not None, use this rule when a column is missing
        from agg_map; if None, raise if a column has no rule.

    Returns
    -------
    panel_q : DataFrame with quarterly aggregated series, index at quarter start.
    """
    if panel_train.empty:
        return panel_train.copy()

    out: Dict[str, pd.Series] = {}
    for col in panel_train.columns:
        info = agg_map.get(col)
        if info is None:
            if agg_default_rule is None:
                raise ValueError(
                    f"No aggregation rule for series '{col}' and no agg-default-rule provided."
                )
            rule = agg_default_rule
        else:
            if isinstance(info, dict):
                rule = info.get("rule")
                if rule is None:
                    if agg_default_rule is None:
                        raise ValueError(
                            f"Aggregation info for series '{col}' has no 'rule' field "
                            f"and no agg-default-rule is provided."
                        )
                    rule = agg_default_rule
            else:
                # assume flat mapping series -> rule
                rule = str(info)

        s = panel_train[col]
        out[col] = aggregate_series_quarterly(s, rule=rule)

    panel_q = pd.DataFrame(out).sort_index()
    return panel_q


def apply_aggregation_for_ranking(
    panel_train: pd.DataFrame,
    y_train: pd.Series,
    agg_map: Dict[str, Any],
    agg_default_rule: str | None,
    agg_mode: str,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare (X_rank, y_rank) for preselection.

    If agg_mode == "none":
        - Align monthly panel and (monthly) y on intersection of dates.

    If agg_mode == "quarterly":
        - Aggregate monthly panel to quarterly using agg_map / agg_default_rule.
        - Align quarterly panel and quarterly y on intersection of dates.
    """
    if agg_mode == "none":
        panel_align, y_align = panel_train.align(y_train, join="inner", axis=0)
        return panel_align, y_align

    if agg_mode != "quarterly":
        raise ValueError(f"Unsupported agg_mode={agg_mode!r}. Use 'none' or 'quarterly'.")

    # 1) Aggregate monthly X to quarterly
    panel_q = aggregate_panel_quarterly(panel_train, agg_map, agg_default_rule)

    # 2) Ensure y has DatetimeIndex; in your pipeline, target is already quarterly
    y_q = y_train.copy()
    y_q.index = pd.DatetimeIndex(y_q.index)

    # 3) Align on intersection of quarterly dates
    idx_common = panel_q.index.intersection(y_q.index)
    if idx_common.empty:
        raise ValueError("No overlapping quarterly dates between panel and target after aggregation.")

    X_rank = panel_q.loc[idx_common].sort_index()
    y_rank = y_q.loc[idx_common].sort_index()
    return X_rank, y_rank


# ---------------------------------------------------------------------
# Method dispatch (SIS / tstat / LARS)
# ---------------------------------------------------------------------


def run_sis(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """
    SIS preselection wrapper.

    Delegates to src.dfm_pipeline.preselection.preselect_sis.run_sis_preselection.
    """
    rank_df, selected_vars, meta = run_sis_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


def run_tstat(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """
    t-stat preselection wrapper (your original).

    Delegates to src.dfm_pipeline.preselection.preselect_tstat.run_tstat_preselection.
    """
    rank_df, selected_vars, meta = run_tstat_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


def run_tstat_lm(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """
    t-stat preselection wrapper (Linzenich–Meunier-style).

    Delegates to src.dfm_pipeline.preselection.preselect_tstat_lm.run_tstat_lm_preselection.
    """
    rank_df, selected_vars, meta = run_tstat_lm_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


def run_lars_tscv(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """
    LARS + time-series CV preselection wrapper (your original).

    Delegates to src.dfm_pipeline.preselection.preselect_lars.run_lars_tscv_preselection.
    """
    rank_df, selected_vars, meta = run_lars_tscv_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


def run_lars_lm(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    """
    LARS preselection wrapper (Linzenich–Meunier-style).

    Delegates to src.dfm_pipeline.preselection.preselect_lars_lm.run_lars_lm_preselection.
    """
    rank_df, selected_vars, meta = run_lars_lm_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


# ---------------------------------------------------------------------
# Artifact writing (with group info)
# ---------------------------------------------------------------------


def write_artifacts(
    panel_name: str,
    variant_name: str,
    variants_root: str,
    train_start: str,
    train_end: str,
    rank_df: pd.DataFrame,
    selected_vars: List[str],
    info: PreselectionInfo,
    group_map: Dict[str, str] | None = None,
) -> None:
    """
    Save selected_vars.json, rank.csv (+ group column if group_map given),
    info.json, and optional selected_vars_with_group.json under:

      {variants_root}/{variant_name}/{panel_name}/{train_start}_{train_end}/
    """
    span = fmt_span_full(train_start, train_end)
    outdir = Path(variants_root) / variant_name / panel_name / span
    ensure_dir(outdir)

    sel_path = outdir / "selected_vars.json"
    rank_path = outdir / "rank.csv"
    info_path = outdir / "info.json"

    # Selected vars: keep legacy list format
    sel_path.write_text(
        json.dumps(selected_vars, indent=2, sort_keys=True), encoding="utf-8"
    )

    # Rank: add group column if group_map provided
    rank_to_save = rank_df.copy()
    if group_map is not None and not rank_to_save.empty:
        rank_to_save["group"] = [
            group_map.get(str(var), None) for var in rank_to_save.index
        ]
    rank_to_save.to_csv(rank_path, index=True)

    # Info JSON
    info_path.write_text(
        json.dumps(asdict(info), indent=2, sort_keys=True), encoding="utf-8"
    )

    print(f"[preselection] wrote: {sel_path}")
    print(f"[preselection] rank:  {rank_path}")
    print(f"[preselection] info:  {info_path}")

    # Optional: selected_vars_with_group.json
    if group_map is not None:
        sel_groups = {var: group_map.get(str(var), None) for var in selected_vars}
        sel_with_group_path = outdir / "selected_vars_with_group.json"
        sel_with_group_path.write_text(
            json.dumps(sel_groups, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        print(f"[preselection] groups: {sel_with_group_path}")


# ---------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run variable preselection (SIS / tstat / LARS) on training panel."
    )

    # Core inputs
    ap.add_argument(
        "--panel-csv",
        required=True,
        help="Path to TRAIN panel CSV (e.g. dataset/...__train_1990_01_2019_12.csv).",
    )
    ap.add_argument(
        "--target-csv",
        required=True,
        help="Path to TRAIN target CSV (e.g. data/targets/...__train_1990_01_2019_12.csv).",
    )
    ap.add_argument("--panel-name", required=True, help="Logical panel id, e.g. 1960_noVIX.")
    ap.add_argument(
        "--train-start",
        required=True,
        help="Start of training window (YYYY-MM-01). Must match the TRAIN panel.",
    )
    ap.add_argument(
        "--train-end",
        required=True,
        help="End of training window (YYYY-MM-01). Must match the TRAIN panel.",
    )

    # Method choice
    ap.add_argument(
        "--method",
        choices=["sis", "tstat", "tstat_lm", "lars_tscv", "lars_lm"],
        required=True,
        help="Preselection method to run.",
    )

    # Aggregation
    ap.add_argument(
        "--agg-mode",
        choices=["none", "quarterly"],
        default="quarterly",
        help="Aggregation mode for X before preselection. "
             "'quarterly' for within-quarter aggregation vs quarterly y.",
    )
    ap.add_argument(
        "--agg-rule-path",
        default="",
        help="JSON file with aggregation rules (Linzenich–Meunier format with 'series_rules').",
    )
    ap.add_argument(
        "--agg-default-rule",
        default=None,
        help=(
            "Optional fallback aggregation rule (e.g. sum3m) for series not in agg-rule-path. "
            "If omitted and a series is missing, an error is raised."
        ),
    )

    # Group map (for annotations)
    ap.add_argument(
        "--group-map-path",
        default="",
        help="Optional JSON mapping variable -> group (e.g. data/metadata/variable_group_map.json). "
             "Used to annotate rank.csv and selected_vars_with_group.json.",
    )

    # Variant naming and output root
    ap.add_argument(
        "--variant-name",
        required=True,
        help="Name for this preselection variant (used in output directory).",
    )
    ap.add_argument(
        "--variants-root",
        default="data/metadata/variants",
        help="Root directory where variants are stored.",
    )

    # Method-specific kwargs as JSON string
    ap.add_argument(
        "--method-kwargs",
        default="",
        help="JSON string with method-specific hyperparameters (passed to SIS/tstat/LARS).",
    )

    args = ap.parse_args()

    # -----------------------------------------------------------------
    # Load panel and target (TRAIN artifacts, already standardized)
    # -----------------------------------------------------------------
    panel_full = load_panel(args.panel_csv)
    y_full = load_target(args.target_csv)

    # Restrict both to the TRAIN window (for safety, even if CSVs are already cropped)
    panel_train = clip_window(panel_full, args.train_start, args.train_end)
    y_train = clip_window(y_full, args.train_start, args.train_end)

    if panel_train.empty:
        raise ValueError("Training panel is empty after clipping to train window.")
    if y_train.empty:
        raise ValueError("Training target is empty after clipping to train window.")

    # -----------------------------------------------------------------
    # Aggregation map
    # -----------------------------------------------------------------
    agg_map: Dict[str, Any] = {}
    agg_default_rule: str | None = args.agg_default_rule

    if args.agg_mode == "quarterly":
        if not args.agg_rule_path and agg_default_rule is None:
            raise ValueError(
                "quarterly agg-mode requires either --agg-rule-path or --agg-default-rule."
            )

        if args.agg_rule_path:
            cfg = json.loads(Path(args.agg_rule_path).read_text(encoding="utf-8"))

            # EXPECTED STRUCTURE (Linzenich–Meunier style):
            # {
            #   "_meta": {...},
            #   "series_rules": {
            #       "RPI": { "type": "...", "rule": "sum3m", "group": "..." },
            #       ...
            #   }
            # }
            if "series_rules" in cfg:
                agg_map = cfg["series_rules"]
            else:
                # fallback: assume flat mapping series -> rule or series -> {rule: ...}
                agg_map = cfg
        else:
            agg_map = {}  # rely entirely on agg_default_rule for all series

    elif args.agg_mode == "none":
        agg_map = {}

    # -----------------------------------------------------------------
    # Build ranking panel (X_rank, y_rank)
    # -----------------------------------------------------------------
    X_rank, y_rank = apply_aggregation_for_ranking(
        panel_train=panel_train,
        y_train=y_train,
        agg_map=agg_map,
        agg_default_rule=agg_default_rule,
        agg_mode=args.agg_mode,
    )

    # -----------------------------------------------------------------
    # Parse method kwargs
    # -----------------------------------------------------------------
    if args.method_kwargs.strip():
        try:
            method_params: Dict[str, Any] = json.loads(args.method_kwargs)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON for --method-kwargs: {e}") from e
    else:
        method_params = {}

    # -----------------------------------------------------------------
    # Run chosen method
    # -----------------------------------------------------------------
    if args.method == "sis":
        rank_df, selected_vars, meta = run_sis(X_rank, y_rank, method_params)
    elif args.method == "tstat":
        rank_df, selected_vars, meta = run_tstat(X_rank, y_rank, method_params)
    elif args.method == "tstat_lm":
        rank_df, selected_vars, meta = run_tstat_lm(X_rank, y_rank, method_params)
    elif args.method == "lars_tscv":
        rank_df, selected_vars, meta = run_lars_tscv(X_rank, y_rank, method_params)
    elif args.method == "lars_lm":
        rank_df, selected_vars, meta = run_lars_lm(X_rank, y_rank, method_params)
    else:
        raise ValueError(f"Unknown method {args.method!r}.")

    # -----------------------------------------------------------------
    # Load group map (optional) for annotations
    # -----------------------------------------------------------------
    group_map: Dict[str, str] | None = None
    if args.group_map_path:
        group_map = load_group_map(args.group_map_path)

    # -----------------------------------------------------------------
    # Build info object and write artifacts
    # -----------------------------------------------------------------
    info = PreselectionInfo(
        panel_name=args.panel_name,
        method=args.method,
        variant_name=args.variant_name,
        train_start=args.train_start,
        train_end=args.train_end,
        agg_mode=args.agg_mode,
        agg_rule_path=args.agg_rule_path or None,
        agg_default_rule=args.agg_default_rule,
        n_obs=int(len(X_rank)),
        n_features_in=int(X_rank.shape[1]),
        n_features_selected=int(len(selected_vars)),
        method_kwargs=method_params,
    )

    write_artifacts(
        panel_name=args.panel_name,
        variant_name=args.variant_name,
        variants_root=args.variants_root,
        train_start=args.train_start,
        train_end=args.train_end,
        rank_df=rank_df,
        selected_vars=selected_vars,
        info=info,
        group_map=group_map,
    )


if __name__ == "__main__":
    main()
