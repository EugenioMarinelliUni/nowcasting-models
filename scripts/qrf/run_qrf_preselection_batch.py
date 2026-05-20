from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from tqdm import tqdm
except ImportError as exc:
    raise ImportError("Install tqdm first: python -m pip install tqdm") from exc


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRESELECTION_SCRIPT = PROJECT_ROOT / "scripts" / "preselection" / "run_preselection.py"

SUPPORTED_METHODS = {"sis", "tstat_lm", "lars_lm", "lars_tscv"}


def _parse_csv_list(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _safe_method_label(method: str) -> str:
    return method.replace("/", "_").replace("\\", "_").replace(" ", "_")


def _load_json_arg(value: str | None) -> dict[str, Any]:
    if value is None or not value.strip():
        return {}
    return json.loads(value)


def _read_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)


def _normalize_month_start_index(index: Any) -> pd.DatetimeIndex:
    idx = pd.DatetimeIndex(pd.to_datetime(index, errors="coerce"))
    if idx.hasnans:
        raise ValueError("Date index contains NaT values")
    return idx.to_period("M").to_timestamp(how="start")


def _find_date_column(df: pd.DataFrame) -> str | None:
    preferred = ["sasdate", "date", "Date", "DATE", "time", "timestamp"]

    for col in preferred:
        if col in df.columns:
            return col

    first = df.columns[0]
    parsed = pd.to_datetime(df[first], errors="coerce")
    if parsed.notna().mean() > 0.90:
        return first

    return None


def _load_panel_csv(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    df = pd.read_csv(path)

    date_col = _find_date_column(df)
    if date_col is not None:
        idx = pd.to_datetime(df.pop(date_col), errors="coerce")
        df.index = _normalize_month_start_index(idx)
    else:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.index = _normalize_month_start_index(df.index)

    df = df.apply(pd.to_numeric, errors="coerce")
    return df.sort_index()


def _load_target_csv(path: str | Path) -> pd.Series:
    path = Path(path)
    df = pd.read_csv(path)

    date_col = _find_date_column(df)
    if date_col is not None:
        idx = pd.to_datetime(df.pop(date_col), errors="coerce")
        df.index = _normalize_month_start_index(idx)
    else:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.index = _normalize_month_start_index(df.index)

    numeric_cols = df.select_dtypes(include="number").columns.tolist()

    if "y" in df.columns:
        y = df["y"]
    elif len(numeric_cols) == 1:
        y = df[numeric_cols[0]]
    elif df.shape[1] == 1:
        y = df.iloc[:, 0]
    else:
        raise ValueError(
            f"Cannot infer target column from {path}. Columns={df.columns.tolist()}"
        )

    y = pd.to_numeric(y, errors="coerce")
    y.name = "y"
    return y.sort_index()


def _train_xy_from_csv(
    x_path: str,
    y_path: str,
    train_start: str,
    train_end: str,
) -> tuple[pd.DataFrame, pd.Series]:
    X = _load_panel_csv(x_path)
    y = _load_target_csv(y_path)

    train_start_ts = pd.Timestamp(train_start).to_period("M").to_timestamp(how="start")
    train_end_ts = pd.Timestamp(train_end).to_period("M").to_timestamp(how="start")

    X = X.loc[(X.index >= train_start_ts) & (X.index <= train_end_ts)]
    y = y.loc[(y.index >= train_start_ts) & (y.index <= train_end_ts)]

    y = y.dropna()

    common_idx = X.index.intersection(y.index)
    X = X.loc[common_idx]
    y = y.loc[common_idx]

    X = X.dropna(axis=1, how="all")
    X = X.apply(pd.to_numeric, errors="coerce")

    valid_cols = []
    for col in X.columns:
        s = X[col]
        if s.notna().sum() >= 12 and np.nanstd(s.to_numpy(dtype=float)) > 1e-12:
            valid_cols.append(col)

    X = X[valid_cols]

    Z = pd.concat([y, X], axis=1).dropna()
    if Z.empty:
        raise ValueError("No usable aligned training rows after dropping NaNs")

    y_clean = Z.iloc[:, 0].astype(float)
    X_clean = Z.iloc[:, 1:].astype(float)

    return X_clean, y_clean


def _standardize_matrix(X: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    cols = list(X.columns)
    arr = X.to_numpy(dtype=float)

    mean = np.nanmean(arr, axis=0)
    std = np.nanstd(arr, axis=0)
    std = np.where(std <= 1e-12, 1.0, std)

    Xs = (arr - mean) / std
    Xs = np.nan_to_num(Xs, nan=0.0, posinf=0.0, neginf=0.0)

    return Xs, cols


def _standardize_vector(y: pd.Series) -> np.ndarray:
    arr = y.to_numpy(dtype=float)
    mu = float(np.nanmean(arr))
    sd = float(np.nanstd(arr))
    if sd <= 1e-12:
        sd = 1.0
    out = (arr - mu) / sd
    return np.nan_to_num(out, nan=0.0, posinf=0.0, neginf=0.0)


def _rank_sis(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    rows = []

    y_arr = y.to_numpy(dtype=float)

    for col in X.columns:
        x = X[col].to_numpy(dtype=float)
        mask = np.isfinite(x) & np.isfinite(y_arr)

        if mask.sum() < 12:
            score = np.nan
        elif np.nanstd(x[mask]) <= 1e-12 or np.nanstd(y_arr[mask]) <= 1e-12:
            score = np.nan
        else:
            score = float(np.corrcoef(x[mask], y_arr[mask])[0, 1])

        rows.append(
            {
                "variable": col,
                "score": score,
                "abs_score": abs(score) if np.isfinite(score) else np.nan,
                "method": "sis",
            }
        )

    rank_df = pd.DataFrame(rows)
    rank_df = rank_df.sort_values("abs_score", ascending=False, na_position="last")
    rank_df["rank"] = np.arange(1, len(rank_df) + 1)
    return rank_df


def _ols_tstat_single(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]

    n = len(y)
    if n < 12:
        return np.nan, np.nan, np.nan

    if np.std(x) <= 1e-12:
        return np.nan, np.nan, np.nan

    X_design = np.column_stack([np.ones(n), x])

    beta, *_ = np.linalg.lstsq(X_design, y, rcond=None)
    resid = y - X_design @ beta

    dof = max(n - X_design.shape[1], 1)
    sigma2 = float(resid @ resid / dof)

    xtx_inv = np.linalg.pinv(X_design.T @ X_design)
    se = np.sqrt(np.diag(sigma2 * xtx_inv))

    beta_x = float(beta[1])
    se_x = float(se[1])

    if se_x <= 1e-12:
        tstat = np.nan
    else:
        tstat = beta_x / se_x

    return beta_x, se_x, float(tstat)


def _rank_tstat_lm(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    rows = []
    y_arr = y.to_numpy(dtype=float)

    for col in X.columns:
        beta, se, tstat = _ols_tstat_single(X[col].to_numpy(dtype=float), y_arr)
        rows.append(
            {
                "variable": col,
                "coef": beta,
                "se": se,
                "tstat": tstat,
                "abs_tstat": abs(tstat) if np.isfinite(tstat) else np.nan,
                "method": "tstat_lm",
            }
        )

    rank_df = pd.DataFrame(rows)
    rank_df = rank_df.sort_values("abs_tstat", ascending=False, na_position="last")
    rank_df["rank"] = np.arange(1, len(rank_df) + 1)
    return rank_df


def _rank_lars_lm(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    try:
        from sklearn.linear_model import lars_path
    except ImportError as exc:
        raise ImportError("scikit-learn is required for LARS preselection") from exc

    Xs, cols = _standardize_matrix(X)
    ys = _standardize_vector(y)

    _alphas, active, coefs = lars_path(Xs, ys, method="lasso")

    rows = []

    for j, col in enumerate(cols):
        coef_path = coefs[j, :]
        nz = np.where(np.abs(coef_path) > 1e-10)[0]

        if len(nz) > 0:
            entry_step = int(nz[0])
            max_abs_coef = float(np.max(np.abs(coef_path)))
        else:
            entry_step = 10**9
            max_abs_coef = 0.0

        rows.append(
            {
                "variable": col,
                "entry_step": entry_step,
                "max_abs_coef": max_abs_coef,
                "method": "lars_lm",
            }
        )

    rank_df = pd.DataFrame(rows)

    if active:
        active_rank = {cols[idx]: i for i, idx in enumerate(active, start=1)}
        rank_df["_active_rank"] = rank_df["variable"].map(active_rank).fillna(10**9)
        rank_df = rank_df.sort_values(
            ["_active_rank", "entry_step", "max_abs_coef"],
            ascending=[True, True, False],
        ).drop(columns=["_active_rank"])
    else:
        rank_df = rank_df.sort_values(
            ["entry_step", "max_abs_coef"],
            ascending=[True, False],
        )

    rank_df["rank"] = np.arange(1, len(rank_df) + 1)
    return rank_df


def _rank_lars_tscv(X: pd.DataFrame, y: pd.Series) -> pd.DataFrame:
    try:
        from sklearn.linear_model import LassoLarsCV
        from sklearn.model_selection import TimeSeriesSplit
    except ImportError as exc:
        raise ImportError("scikit-learn is required for LARS-TSCV preselection") from exc

    Xs, cols = _standardize_matrix(X)
    ys = _standardize_vector(y)

    n = len(ys)
    if n < 20:
        return _rank_lars_lm(X, y)

    n_splits = min(5, max(2, n // 20))
    if n_splits >= n:
        n_splits = max(2, n - 1)

    cv = TimeSeriesSplit(n_splits=n_splits)

    model = LassoLarsCV(cv=cv, max_iter=min(500, Xs.shape[1] + 50))
    model.fit(Xs, ys)

    coefs = np.asarray(model.coef_, dtype=float)
    abs_coef = np.abs(coefs)

    rows = []

    for col, coef, score in zip(cols, coefs, abs_coef):
        rows.append(
            {
                "variable": col,
                "coef": float(coef),
                "abs_coef": float(score),
                "alpha": float(model.alpha_),
                "method": "lars_tscv",
            }
        )

    rank_df = pd.DataFrame(rows)
    rank_df = rank_df.sort_values("abs_coef", ascending=False, na_position="last")

    if float(rank_df["abs_coef"].max()) <= 1e-12:
        rank_df = _rank_lars_lm(X, y)
        rank_df["method"] = "lars_tscv_fallback_lars_path"

    rank_df["rank"] = np.arange(1, len(rank_df) + 1)
    return rank_df


def _standalone_preselect(
    method: str,
    args: argparse.Namespace,
    variant_name: str,
) -> tuple[pd.DataFrame, list[str], dict[str, Any]]:
    X, y = _train_xy_from_csv(
        x_path=args.x_path,
        y_path=args.y_path,
        train_start=args.train_start,
        train_end=args.train_end,
    )

    if method == "sis":
        rank_df = _rank_sis(X, y)
    elif method == "tstat_lm":
        rank_df = _rank_tstat_lm(X, y)
    elif method == "lars_lm":
        rank_df = _rank_lars_lm(X, y)
    elif method == "lars_tscv":
        rank_df = _rank_lars_tscv(X, y)
    else:
        raise ValueError(f"Unsupported method: {method}")

    selected_vars = (
        rank_df["variable"]
        .dropna()
        .astype(str)
        .head(args.top_k)
        .tolist()
    )

    meta = {
        "method": method,
        "source": "standalone_fallback",
        "variant_name": variant_name,
        "x_path": args.x_path,
        "y_path": args.y_path,
        "train_start": args.train_start,
        "train_end": args.train_end,
        "n_train_rows": int(len(y)),
        "n_candidate_predictors": int(X.shape[1]),
        "top_k": int(args.top_k),
    }

    return rank_df, selected_vars, meta


def _load_group_map(path: str | None) -> dict[str, str]:
    if not path:
        return {}

    p = Path(path)
    if not p.exists():
        return {}

    with open(p, "r", encoding="utf-8") as f:
        obj = json.load(f)

    if not isinstance(obj, dict):
        return {}

    return {str(k): str(v) for k, v in obj.items()}


def _write_standalone_preselection_artifacts(
    method: str,
    variant_name: str,
    rank_df: pd.DataFrame,
    selected_vars: list[str],
    meta: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, str]:
    train_label = (
        f"{pd.Timestamp(args.train_start).strftime('%Y_%m_%d')}_"
        f"{pd.Timestamp(args.train_end).strftime('%Y_%m_%d')}"
    )

    run_dir = (
        Path(args.variants_root)
        / variant_name
        / args.panel_name
        / train_label
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    selected_path = run_dir / "selected_vars.json"
    rank_path = run_dir / "rank.csv"
    info_path = run_dir / "info.json"
    grouped_path = run_dir / "selected_vars_with_group.json"

    _write_json(selected_path, selected_vars)
    rank_df.to_csv(rank_path, index=False)
    _write_json(info_path, meta)

    group_map = _load_group_map(args.group_map_path)
    grouped = [
        {
            "variable": v,
            "group": group_map.get(v, "Unknown"),
        }
        for v in selected_vars
    ]
    _write_json(grouped_path, grouped)

    return {
        "selected_vars_json": str(selected_path),
        "rank_csv": str(rank_path),
        "info_json": str(info_path),
        "selected_vars_with_group_json": str(grouped_path),
    }


def _extract_vars_from_json_obj(obj: Any) -> list[str]:
    if isinstance(obj, list):
        out = []
        for item in obj:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                for key in (
                    "variable",
                    "series",
                    "predictor",
                    "name",
                    "feature",
                    "fred",
                    "var",
                    "column",
                ):
                    if key in item and isinstance(item[key], str):
                        out.append(item[key])
                        break
        return list(dict.fromkeys(out))

    if isinstance(obj, dict):
        for key in (
            "selected_vars",
            "selected_variables",
            "variables",
            "predictors",
            "selected",
            "features",
            "selected_features",
            "selected_series",
        ):
            if key in obj:
                return _extract_vars_from_json_obj(obj[key])

        metadata_keys = {
            "method",
            "params",
            "metadata",
            "info",
            "train_start",
            "train_end",
            "panel",
            "panel_name",
            "source",
        }

        keys = [k for k in obj.keys() if k not in metadata_keys]
        if keys and all(isinstance(k, str) for k in keys):
            return list(dict.fromkeys(keys))

    return []


def _find_newest_file(
    root: Path,
    filename: str,
    required_path_token: str | None = None,
) -> Path | None:
    if not root.exists():
        return None

    candidates = []
    for path in root.rglob(filename):
        if required_path_token is not None and required_path_token not in str(path):
            continue
        candidates.append(path)

    if not candidates:
        return None

    return max(candidates, key=lambda p: p.stat().st_mtime)


def _read_rank_csv(path: Path) -> list[str]:
    df = pd.read_csv(path)

    if df.empty:
        return []

    cols = list(df.columns)

    if "rank" in cols:
        df = df.sort_values("rank", ascending=True)
    elif "abs_tstat" in cols:
        df = df.sort_values("abs_tstat", ascending=False)
    elif "t_abs" in cols:
        df = df.sort_values("t_abs", ascending=False)
    elif "tstat" in cols:
        df = df.assign(_abs_tstat=df["tstat"].abs()).sort_values(
            "_abs_tstat", ascending=False
        )
    elif "abs_score" in cols:
        df = df.sort_values("abs_score", ascending=False)
    elif "score" in cols:
        df = df.sort_values("score", ascending=False)
    elif "abs_corr" in cols:
        df = df.sort_values("abs_corr", ascending=False)
    elif "corr" in cols:
        df = df.assign(_abs_corr=df["corr"].abs()).sort_values(
            "_abs_corr", ascending=False
        )
    elif "abs_coef" in cols:
        df = df.sort_values("abs_coef", ascending=False)
    elif "entry_step" in cols:
        df = df.sort_values("entry_step", ascending=True)

    variable_col = None
    for candidate in (
        "variable",
        "series",
        "predictor",
        "name",
        "feature",
        "fred",
        "column",
        "var",
    ):
        if candidate in cols:
            variable_col = candidate
            break

    if variable_col is None:
        object_cols = [c for c in cols if df[c].dtype == "object"]
        if object_cols:
            variable_col = object_cols[0]
        else:
            variable_col = cols[0]

    values = [
        str(x).strip()
        for x in df[variable_col].tolist()
        if str(x).strip() and str(x).strip().lower() != "nan"
    ]

    return list(dict.fromkeys(values))


def _extract_selected_vars(
    variants_root: Path,
    variant_name: str,
    top_k: int,
) -> tuple[list[str], dict[str, str | None]]:
    selected_path = _find_newest_file(
        root=variants_root,
        filename="selected_vars.json",
        required_path_token=variant_name,
    )

    selected_group_path = _find_newest_file(
        root=variants_root,
        filename="selected_vars_with_group.json",
        required_path_token=variant_name,
    )

    rank_path = _find_newest_file(
        root=variants_root,
        filename="rank.csv",
        required_path_token=variant_name,
    )

    source = {
        "selected_vars_json": str(selected_path) if selected_path else None,
        "selected_vars_with_group_json": str(selected_group_path) if selected_group_path else None,
        "rank_csv": str(rank_path) if rank_path else None,
    }

    candidates: list[str] = []

    if rank_path is not None:
        try:
            candidates = _read_rank_csv(rank_path)
        except Exception:
            candidates = []

    if not candidates and selected_path is not None:
        try:
            candidates = _extract_vars_from_json_obj(_read_json(selected_path))
        except Exception:
            candidates = []

    if not candidates and selected_group_path is not None:
        try:
            candidates = _extract_vars_from_json_obj(_read_json(selected_group_path))
        except Exception:
            candidates = []

    selected = list(dict.fromkeys(candidates))[:top_k]

    return selected, source


def _method_kwargs_for(method: str, args: argparse.Namespace) -> dict[str, Any]:
    if method == "sis":
        return _load_json_arg(args.sis_kwargs)
    if method == "tstat_lm":
        return _load_json_arg(args.tstat_kwargs)
    if method == "lars_lm":
        return _load_json_arg(args.lars_kwargs)
    if method == "lars_tscv":
        return _load_json_arg(args.lars_tscv_kwargs)
    return {}


def _build_preselection_cmd(
    method: str,
    variant_name: str,
    method_kwargs: dict[str, Any],
    args: argparse.Namespace,
) -> list[str]:
    cmd = [
        sys.executable,
        str(PRESELECTION_SCRIPT),
        "--panel-csv",
        args.x_path,
        "--target-csv",
        args.y_path,
        "--panel-name",
        args.panel_name,
        "--train-start",
        args.train_start,
        "--train-end",
        args.train_end,
        "--method",
        method,
        "--agg-mode",
        args.agg_mode,
        "--variant-name",
        variant_name,
        "--variants-root",
        args.variants_root,
    ]

    if args.agg_rule_path:
        cmd.extend(["--agg-rule-path", args.agg_rule_path])

    if args.group_map_path:
        cmd.extend(["--group-map-path", args.group_map_path])

    if method_kwargs:
        cmd.extend(["--method-kwargs", json.dumps(method_kwargs)])

    return cmd


def _run_existing_preselection(
    method: str,
    variant_name: str,
    args: argparse.Namespace,
) -> int:
    if not PRESELECTION_SCRIPT.exists():
        return 999

    method_kwargs = _method_kwargs_for(method, args)

    cmd = _build_preselection_cmd(
        method=method,
        variant_name=variant_name,
        method_kwargs=method_kwargs,
        args=args,
    )

    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    return int(result.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run SIS, t-stat, LARS, and LARS-TSCV preselection for QRF predictor lists."
    )

    parser.add_argument("--x-path", required=True)
    parser.add_argument("--y-path", required=True)

    parser.add_argument("--panel-name", required=True)
    parser.add_argument("--tag", required=True)

    parser.add_argument("--train-start", required=True)
    parser.add_argument("--train-end", required=True)

    parser.add_argument(
        "--methods",
        default="sis,tstat_lm,lars_lm,lars_tscv",
        help="Comma-separated methods: sis,tstat_lm,lars_lm,lars_tscv",
    )

    parser.add_argument("--top-k", type=int, default=20)

    parser.add_argument(
        "--agg-mode",
        default="quarterly",
        choices=["none", "quarterly"],
    )

    parser.add_argument("--agg-rule-path", default=None)
    parser.add_argument("--group-map-path", default=None)

    parser.add_argument(
        "--variants-root",
        default="data/metadata/variants_qrf_preselection",
    )

    parser.add_argument(
        "--outdir",
        default=None,
        help="Output directory for QRF predictor lists. Default: data/metadata/qrf_predictors/{panel}/{tag}",
    )

    parser.add_argument("--sis-kwargs", default=None)
    parser.add_argument("--tstat-kwargs", default=None)
    parser.add_argument("--lars-kwargs", default=None)
    parser.add_argument("--lars-tscv-kwargs", default=None)

    parser.add_argument(
        "--force",
        action=argparse.BooleanOptionalAction,
        default=True,
    )

    parser.add_argument(
        "--use-existing-runner",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Try scripts/preselection/run_preselection.py first.",
    )

    parser.add_argument(
        "--standalone-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use standalone robust preselection if existing runner fails or returns no predictors.",
    )

    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.top_k <= 0:
        raise ValueError("--top-k must be positive")

    methods = _parse_csv_list(args.methods)
    bad_methods = [m for m in methods if m not in SUPPORTED_METHODS]
    if bad_methods:
        raise ValueError(f"Unsupported methods: {bad_methods}. Supported={sorted(SUPPORTED_METHODS)}")

    outdir = (
        Path(args.outdir)
        if args.outdir
        else Path("data/metadata/qrf_predictors") / args.panel_name / args.tag
    )
    outdir.mkdir(parents=True, exist_ok=True)

    variants_root = Path(args.variants_root)
    variants_root.mkdir(parents=True, exist_ok=True)

    summary_rows: list[dict[str, Any]] = []

    print(f"Methods: {methods}")
    print(f"Predictor output directory: {outdir}")
    print(f"Variants root: {variants_root}")
    print(f"Top k: {args.top_k}")
    print(f"Use existing runner: {args.use_existing_runner}")
    print(f"Standalone fallback: {args.standalone_fallback}")

    for method in tqdm(methods, desc="QRF predictor preselection", unit="method"):
        method_label = _safe_method_label(method)
        variant_name = (
            f"qrf_preselect__{args.panel_name}__{args.tag}"
            f"__{method_label}__top{args.top_k}"
        )

        predictors_json = outdir / f"{method_label}__top{args.top_k}.json"
        predictors_txt = outdir / f"{method_label}__top{args.top_k}.txt"

        if predictors_json.exists() and not args.force:
            selected = _extract_vars_from_json_obj(_read_json(predictors_json))
            summary_rows.append(
                {
                    "method": method,
                    "variant_name": variant_name,
                    "n_selected": len(selected),
                    "predictors_json": str(predictors_json),
                    "predictors_txt": str(predictors_txt),
                    "selected_vars_json": None,
                    "selected_vars_with_group_json": None,
                    "rank_csv": None,
                    "status": "skipped_existing",
                    "source": "existing_qrf_predictor_file",
                }
            )
            continue

        selected: list[str] = []
        source: dict[str, str | None] = {
            "selected_vars_json": None,
            "selected_vars_with_group_json": None,
            "rank_csv": None,
        }
        status = "not_run"
        selection_source = "none"

        if args.use_existing_runner:
            tqdm.write(f"RUN existing preselection: {method} -> {variant_name}")
            rc = _run_existing_preselection(method, variant_name, args)

            if rc == 0:
                selected, source = _extract_selected_vars(
                    variants_root=variants_root,
                    variant_name=variant_name,
                    top_k=args.top_k,
                )

                if selected:
                    status = "completed"
                    selection_source = "existing_preselection_runner"
                else:
                    status = "completed_but_no_predictors_extracted"
                    selection_source = "existing_preselection_runner"
            else:
                status = f"failed_returncode_{rc}"
                selection_source = "existing_preselection_runner"

        if (not selected) and args.standalone_fallback:
            tqdm.write(f"RUN standalone fallback: {method} -> {variant_name}")

            try:
                rank_df, selected_vars, meta = _standalone_preselect(
                    method=method,
                    args=args,
                    variant_name=variant_name,
                )

                artifact_paths = _write_standalone_preselection_artifacts(
                    method=method,
                    variant_name=variant_name,
                    rank_df=rank_df,
                    selected_vars=selected_vars,
                    meta=meta,
                    args=args,
                )

                selected = selected_vars[: args.top_k]
                source = {
                    "selected_vars_json": artifact_paths["selected_vars_json"],
                    "selected_vars_with_group_json": artifact_paths["selected_vars_with_group_json"],
                    "rank_csv": artifact_paths["rank_csv"],
                }
                status = "completed"
                selection_source = "standalone_fallback"

            except Exception as exc:
                status = f"{status}; standalone_fallback_failed: {exc}"
                selection_source = f"{selection_source}+standalone_fallback_failed"

        if selected:
            _write_json(predictors_json, selected)

            with open(predictors_txt, "w", encoding="utf-8") as f:
                for var in selected:
                    f.write(f"{var}\n")

            tqdm.write(f"WROTE {method}: {predictors_json}")
        else:
            tqdm.write(f"NO PREDICTORS WRITTEN for {method}: {status}")

        summary_rows.append(
            {
                "method": method,
                "variant_name": variant_name,
                "n_selected": len(selected),
                "predictors_json": str(predictors_json),
                "predictors_txt": str(predictors_txt),
                "selected_vars_json": source.get("selected_vars_json"),
                "selected_vars_with_group_json": source.get("selected_vars_with_group_json"),
                "rank_csv": source.get("rank_csv"),
                "status": status,
                "source": selection_source,
            }
        )

    summary_path = outdir / "preselection_summary.csv"

    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "method",
            "variant_name",
            "n_selected",
            "predictors_json",
            "predictors_txt",
            "selected_vars_json",
            "selected_vars_with_group_json",
            "rank_csv",
            "status",
            "source",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"\nSaved summary to: {summary_path}")

    print("\nProduced predictor files:")
    for row in summary_rows:
        print(
            {
                "method": row["method"],
                "n_selected": row["n_selected"],
                "predictors_json": row["predictors_json"],
                "status": row["status"],
                "source": row["source"],
            }
        )


if __name__ == "__main__":
    main()