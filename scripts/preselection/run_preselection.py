#!/usr/bin/env python3
# Supports methods: sis, tstat, tstat_lm, lars_tscv, lars_lm
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, overload, Union, cast

import pandas as pd

try:
    from dfm_pipeline.preselection.preselect_sis import run_sis_preselection
    from dfm_pipeline.preselection.preselect_tstat import run_tstat_preselection
    from dfm_pipeline.preselection.preselect_lars import run_lars_tscv_preselection
    from dfm_pipeline.preselection.preselect_tstat_lm import run_tstat_lm_preselection
    from dfm_pipeline.preselection.preselect_lars_lm import run_lars_lm_preselection
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from dfm_pipeline.preselection.preselect_sis import run_sis_preselection
    from dfm_pipeline.preselection.preselect_tstat import run_tstat_preselection
    from dfm_pipeline.preselection.preselect_lars import run_lars_tscv_preselection
    from dfm_pipeline.preselection.preselect_tstat_lm import run_tstat_lm_preselection
    from dfm_pipeline.preselection.preselect_lars_lm import run_lars_lm_preselection


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
    return (
        f"{s.year:04d}_{s.month:02d}_{s.day:02d}_"
        f"{e.year:04d}_{e.month:02d}_{e.day:02d}"
    )


@overload
def clip_window(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame: ...


@overload
def clip_window(df: pd.Series, start: str, end: str) -> pd.Series: ...


def clip_window(df: Union[pd.DataFrame, pd.Series], start: str, end: str):
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
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(obj, dict):
        if "groups" in obj and isinstance(obj["groups"], dict):
            return {str(k): str(v) for k, v in obj["groups"].items()}
        return {str(k): str(v) for k, v in obj.items()}
    raise ValueError(f"Unsupported group map format in {path!r}")


# ---------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------


def aggregate_series_quarterly(series: pd.Series, rule: str) -> pd.Series:
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

    per_idx = cast("pd.PeriodIndex", xq.index)
    xq.index = per_idx.to_timestamp(how="start")
    return xq


def aggregate_panel_quarterly(
    panel_train: pd.DataFrame,
    agg_map: Dict[str, Any],
    agg_default_rule: str | None,
) -> pd.DataFrame:
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
                            f"Aggregation info for series '{col}' has no 'rule' field and "
                            f"no agg-default-rule is provided."
                        )
                    rule = agg_default_rule
            else:
                rule = str(info)

        out[col] = aggregate_series_quarterly(panel_train[col], rule=rule)

    return pd.DataFrame(out).sort_index()


def _infer_month_anchor(index: pd.DatetimeIndex) -> str:
    if index.empty:
        return "start"
    days = index.day
    if float((days == 1).mean()) >= 0.8:
        return "start"
    return "end"


def _to_month_anchor(ts: pd.Timestamp, month_anchor: str) -> pd.Timestamp:
    per = ts.to_period("M")
    if month_anchor == "start":
        return per.to_timestamp(how="start")
    if month_anchor == "end":
        return per.to_timestamp(how="end")
    raise ValueError(f"Invalid month_anchor={month_anchor!r}.")


def _mm_weighted_sum(s: pd.Series) -> pd.Series:
    return (
        1.0 * s
        + 2.0 * s.shift(1)
        + 3.0 * s.shift(2)
        + 2.0 * s.shift(3)
        + 1.0 * s.shift(4)
    )


def aggregate_panel_mm_quarterly(panel_train: pd.DataFrame, target_index: pd.DatetimeIndex) -> pd.DataFrame:
    if panel_train.empty:
        return pd.DataFrame(index=target_index)
    if target_index.empty:
        return pd.DataFrame(index=target_index)

    month_anchor = _infer_month_anchor(pd.DatetimeIndex(panel_train.index))

    monthly_lookup = pd.DatetimeIndex([
        _to_month_anchor(pd.Timestamp(d), month_anchor) for d in target_index
    ])

    panel_idx = pd.DatetimeIndex(panel_train.index)

    out: Dict[str, pd.Series] = {}
    for col in panel_train.columns:
        s = panel_train[col]
        s.index = panel_idx
        mm = _mm_weighted_sum(s)

        sampled = pd.Series(index=target_index, dtype=float, name=col)
        common = monthly_lookup.intersection(pd.DatetimeIndex(mm.index))
        if not common.empty:
            q_map = {m: q for q, m in zip(target_index, monthly_lookup)}
            sampled.loc[[q_map[m] for m in common]] = mm.loc[common].to_numpy()
        out[col] = sampled

    return pd.DataFrame(out, index=target_index).sort_index()


def apply_aggregation_for_ranking(
    panel_train: pd.DataFrame,
    y_train: pd.Series,
    agg_map: Dict[str, Any],
    agg_default_rule: str | None,
    agg_mode: str,
) -> Tuple[pd.DataFrame, pd.Series]:
    if agg_mode == "none":
        panel_align, y_align = panel_train.align(y_train, join="inner", axis=0)
        return panel_align, y_align

    y_q = y_train.copy()
    y_q.index = pd.DatetimeIndex(y_q.index)

    if agg_mode == "quarterly":
        panel_q = aggregate_panel_quarterly(panel_train, agg_map, agg_default_rule)
        idx_common = panel_q.index.intersection(y_q.index)
        if idx_common.empty:
            raise ValueError("No overlapping quarterly dates between panel and target after aggregation.")
        X_rank = panel_q.loc[idx_common].sort_index()
        y_rank = y_q.loc[idx_common].sort_index()
        return X_rank, y_rank

    if agg_mode == "mm":
        panel_mm = aggregate_panel_mm_quarterly(panel_train, target_index=y_q.index)
        X_rank, y_rank = panel_mm.align(y_q, join="inner", axis=0)
        mask = (~y_rank.isna()) & (~X_rank.isna().any(axis=1))
        X_rank = X_rank.loc[mask].sort_index()
        y_rank = y_rank.loc[mask].sort_index()
        if X_rank.empty:
            raise ValueError("No usable observations after MM aggregation and NA removal.")
        return X_rank, y_rank

    raise ValueError(f"Unsupported agg_mode={agg_mode!r}. Use 'none', 'quarterly', or 'mm'.")


# ---------------------------------------------------------------------
# Method dispatch
# ---------------------------------------------------------------------


def run_sis(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    rank_df, selected_vars, meta = run_sis_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


def run_tstat(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    rank_df, selected_vars, meta = run_tstat_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


def run_tstat_lm(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    rank_df, selected_vars, meta = run_tstat_lm_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


def run_lars_tscv(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
    rank_df, selected_vars, meta = run_lars_tscv_preselection(X_rank, y_rank, params)
    return rank_df, selected_vars, meta


def run_lars_lm(
    X_rank: pd.DataFrame,
    y_rank: pd.Series,
    params: Dict[str, Any],
) -> Tuple[pd.DataFrame, List[str], Dict[str, Any]]:
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
    span = fmt_span_full(train_start, train_end)
    outdir = Path(variants_root) / variant_name / panel_name / span
    ensure_dir(outdir)

    sel_path = outdir / "selected_vars.json"
    rank_path = outdir / "rank.csv"
    info_path = outdir / "info.json"

    sel_path.write_text(json.dumps(selected_vars, indent=2, sort_keys=True), encoding="utf-8")

    rank_to_save = rank_df.copy()
    if group_map is not None and not rank_to_save.empty:
        rank_to_save["group"] = [group_map.get(str(var), None) for var in rank_to_save.index]
    rank_to_save.to_csv(rank_path, index=True)

    info_path.write_text(json.dumps(asdict(info), indent=2, sort_keys=True), encoding="utf-8")

    print(f"[preselection] wrote: {sel_path}")
    print(f"[preselection] rank:  {rank_path}")
    print(f"[preselection] info:  {info_path}")

    if group_map is not None:
        sel_groups = {var: group_map.get(str(var), None) for var in selected_vars}
        sel_with_group_path = outdir / "selected_vars_with_group.json"
        sel_with_group_path.write_text(json.dumps(sel_groups, indent=2, sort_keys=True), encoding="utf-8")
        print(f"[preselection] groups: {sel_with_group_path}")


# ---------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description="Run variable preselection (SIS / tstat / LARS) on training panel.")

    ap.add_argument("--panel-csv", required=True, help="Path to TRAIN panel CSV.")
    ap.add_argument("--target-csv", required=True, help="Path to TRAIN target CSV.")
    ap.add_argument("--panel-name", required=True, help="Logical panel id, e.g. 1960_noVIX.")
    ap.add_argument("--train-start", required=True, help="Start of training window (YYYY-MM-DD).")
    ap.add_argument("--train-end", required=True, help="End of training window (YYYY-MM-DD).")

    ap.add_argument(
        "--method",
        choices=["sis", "tstat", "tstat_lm", "lars_tscv", "lars_lm"],
        required=True,
        help="Preselection method to run.",
    )

    ap.add_argument(
        "--agg-mode",
        choices=["none", "quarterly", "mm"],
        default="quarterly",
        help=(
            "Aggregation mode for X before preselection. "
            "'quarterly' uses JSON within-quarter rules. "
            "'mm' applies Mariano–Murasawa 5-month filter then samples on y dates."
        ),
    )
    ap.add_argument(
        "--agg-rule-path",
        default="",
        help="JSON file with aggregation rules (expects 'series_rules'). Used only for agg-mode=quarterly.",
    )
    ap.add_argument(
        "--agg-default-rule",
        default=None,
        help="Optional fallback aggregation rule (e.g. sum3m). Used only for agg-mode=quarterly.",
    )

    ap.add_argument(
        "--group-map-path",
        default="",
        help="Optional JSON mapping variable -> group. Used to annotate rank.csv and selected_vars_with_group.json.",
    )

    ap.add_argument("--variant-name", required=True, help="Name for this preselection variant.")
    ap.add_argument(
        "--variants-root",
        default="data/metadata/variants",
        help="Root directory where variants are stored.",
    )

    ap.add_argument(
        "--method-kwargs",
        default="",
        help="JSON string with method-specific hyperparameters.",
    )

    args = ap.parse_args()

    panel_full = load_panel(args.panel_csv)
    y_full = load_target(args.target_csv)

    panel_train = clip_window(panel_full, args.train_start, args.train_end)
    y_train = clip_window(y_full, args.train_start, args.train_end)

    if panel_train.empty:
        raise ValueError("Training panel is empty after clipping to train window.")
    if y_train.empty:
        raise ValueError("Training target is empty after clipping to train window.")

    agg_map: Dict[str, Any] = {}
    agg_default_rule: str | None = args.agg_default_rule

    if args.agg_mode == "quarterly":
        if not args.agg_rule_path and agg_default_rule is None:
            raise ValueError("agg-mode=quarterly requires either --agg-rule-path or --agg-default-rule.")

        if args.agg_rule_path:
            cfg = json.loads(Path(args.agg_rule_path).read_text(encoding="utf-8"))
            if "series_rules" in cfg:
                agg_map = cfg["series_rules"]
            else:
                agg_map = cfg
        else:
            agg_map = {}

    X_rank, y_rank = apply_aggregation_for_ranking(
        panel_train=panel_train,
        y_train=y_train,
        agg_map=agg_map,
        agg_default_rule=agg_default_rule,
        agg_mode=args.agg_mode,
    )

    if args.method_kwargs.strip():
        try:
            method_params: Dict[str, Any] = json.loads(args.method_kwargs)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON for --method-kwargs: {e}") from e
    else:
        method_params = {}

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

    group_map: Dict[str, str] | None = None
    if args.group_map_path:
        group_map = load_group_map(args.group_map_path)

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
