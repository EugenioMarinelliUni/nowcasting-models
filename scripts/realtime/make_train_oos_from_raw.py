# scripts/realtime/make_train_oos_from_raw.py
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

try:
    from dfm_pipeline.covid.covid_make_delete_weights import apply_delete_nan
    from dfm_pipeline.covid.methods import variant_iqd_outliers_to_nan
    from dfm_pipeline.covid.spec import CovidSpec, parse_window
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from dfm_pipeline.covid.covid_make_delete_weights import apply_delete_nan
    from dfm_pipeline.covid.methods import variant_iqd_outliers_to_nan
    from dfm_pipeline.covid.spec import CovidSpec, parse_window


def read_panel(panel_csv: str) -> pd.DataFrame:
    df = pd.read_csv(panel_csv)
    first = df.columns[0]
    df[first] = pd.to_datetime(df[first])
    df = df.set_index(first)
    return df.sort_index()


def read_target(target_csv: str) -> pd.Series:
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


def clip_window(df: pd.DataFrame | pd.Series, start: str, end: str):
    start_ts = pd.to_datetime(start)
    end_ts = pd.to_datetime(end)
    return df.loc[(df.index >= start_ts) & (df.index <= end_ts)]


def fmt_span_from_strings(start: str, end: str) -> str:
    s = pd.to_datetime(start)
    e = pd.to_datetime(end)
    return f"{s.year:04d}_{s.month:02d}_{e.year:04d}_{e.month:02d}"


def fmt_span_from_index(idx: pd.Index) -> str:
    if idx.empty:
        return "NA_NA_NA_NA"
    s = pd.to_datetime(idx.min())
    e = pd.to_datetime(idx.max())
    return f"{s.year:04d}_{s.month:02d}_{e.year:04d}_{e.month:02d}"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_vars_json(path: str) -> list[str]:
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(obj, list):
        return list(obj)
    if isinstance(obj, dict):
        if "selected" in obj and isinstance(obj["selected"], list):
            return list(obj["selected"])
        return [k for k, v in obj.items() if bool(v)]
    raise ValueError("vars-json must be list, {'selected': [...]}, or {var: bool}.")


def compute_scaler(X: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    scaler: Dict[str, Dict[str, float]] = {}
    for col in X.columns:
        s = X[col]
        mu = float(s.mean())
        sigma = float(s.std(ddof=0))
        if sigma == 0.0 or pd.isna(sigma):
            sigma = 1.0
        scaler[col] = {"mean": mu, "std": sigma}
    return scaler


def apply_scaler(X: pd.DataFrame, scaler: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    Xs = pd.DataFrame(index=X.index, columns=X.columns, dtype=float)
    for col in X.columns:
        par = scaler.get(col)
        if par is None:
            Xs[col] = X[col]
            continue
        mu = par["mean"]
        sigma = par["std"]
        Xs[col] = (X[col] - mu) / sigma
    return Xs


@dataclass
class OOSInfo:
    panel_name: str
    y_name: str
    train_start: str
    train_end: str
    oos_start: str | None
    oos_end: str | None
    covid_policy: str
    covid_start: str
    covid_end: str
    monthly_freq: str
    lm_outliers_c: float
    lm_outliers_min_obs: int
    lm_outliers_fit_window: str


def _parse_window(s: Optional[str]) -> Optional[Tuple[str, str]]:
    return parse_window(s)


def apply_covid_policy_to_oos(
    X_oos: pd.DataFrame,
    policy: str,
    covid_start: str,
    covid_end: str,
    monthly_freq: str,
    lm_outliers_c: float,
    lm_outliers_min_obs: int,
    lm_outliers_fit_window: Tuple[str, str],
) -> pd.DataFrame:
    if X_oos.empty:
        return X_oos

    if policy == "none":
        return X_oos

    if policy == "covid_delete":
        return apply_delete_nan(X_oos, covid_start=covid_start, covid_end=covid_end)

    if policy == "lm_outliers":
        spec = CovidSpec(
            covid_start=covid_start,
            covid_end=covid_end,
            monthly_freq=monthly_freq,
            fit_window=lm_outliers_fit_window,
            apply_window=(covid_start, covid_end),
            allow_leakage=False,
        )
        res = variant_iqd_outliers_to_nan(
            X_oos,
            spec,
            c=float(lm_outliers_c),
            min_obs=int(lm_outliers_min_obs),
            fit_window=lm_outliers_fit_window,
            apply_window=(covid_start, covid_end),
        )
        return res.X

    raise ValueError(f"Unknown covid_policy={policy!r}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build standardized train/OOS panels with frozen scalers.")

    ap.add_argument("--panel-name", required=True, help="Logical panel id, e.g. 1960_noVIX.")
    ap.add_argument("--panel-csv", required=True, help="Path to raw/stationarity-treated panel CSV (monthly).")
    ap.add_argument("--y-name", required=True, help="Logical target id, e.g. gdp_A191RL1Q225SBEA.")
    ap.add_argument("--target-csv", required=True, help="Path to raw target CSV (quarterly or monthly).")

    ap.add_argument("--train-start", required=True, help="YYYY-MM-01")
    ap.add_argument("--train-end", required=True, help="YYYY-MM-01")

    ap.add_argument(
        "--vars-json",
        default="",
        help="Optional JSON with selected variables (list / {'selected': ...} / {var: bool}).",
    )

    ap.add_argument("--monthly-freq", default="MS", choices=["MS", "ME"], help="Monthly convention (default MS).")

    ap.add_argument(
        "--covid-policy",
        choices=["none", "covid_delete", "lm_outliers"],
        default="none",
        help="How to treat predictors in the Covid window in the OOS panel.",
    )
    ap.add_argument("--covid-start", default="2020-03-01", help="Start of Covid window.")
    ap.add_argument("--covid-end", default="2021-12-01", help="End of Covid window.")

    ap.add_argument("--lm-outliers-c", type=float, default=4.0, help="c in abs(x-median) > c*IQD.")
    ap.add_argument("--lm-outliers-min-obs", type=int, default=20, help="Min obs in fit window.")
    ap.add_argument(
        "--lm-outliers-fit-window",
        default="",
        help="Optional fit window 'YYYY-MM-DD,YYYY-MM-DD'. If omitted, defaults to train-start/train-end.",
    )

    args = ap.parse_args()

    panel = read_panel(args.panel_csv)
    y_raw = read_target(args.target_csv)

    if args.vars_json:
        sel = load_vars_json(args.vars_json)
        missing = [v for v in sel if v not in panel.columns]
        if missing:
            raise ValueError(f"vars-json references missing columns: {missing[:5]}...")
        panel = panel[sel]

    panel_train = clip_window(panel, args.train_start, args.train_end)
    y_train = clip_window(y_raw, args.train_start, args.train_end)

    train_end_ts = pd.to_datetime(args.train_end)
    panel_oos = panel.loc[panel.index > train_end_ts]
    y_oos = y_raw.loc[y_raw.index > train_end_ts]

    y_train_df = pd.DataFrame({args.y_name: y_train})
    y_oos_df = pd.DataFrame({args.y_name: y_oos})

    X_scaler = compute_scaler(panel_train)
    y_scaler = compute_scaler(y_train_df)

    X_train_std = apply_scaler(panel_train, X_scaler)
    X_oos_std = apply_scaler(panel_oos, X_scaler)
    y_train_std = apply_scaler(y_train_df, y_scaler).iloc[:, 0]
    y_oos_std = apply_scaler(y_oos_df, y_scaler).iloc[:, 0]

    fit_w = _parse_window(args.lm_outliers_fit_window) if args.lm_outliers_fit_window else None
    if fit_w is None:
        fit_w = (args.train_start, args.train_end)

    X_oos_std = apply_covid_policy_to_oos(
        X_oos_std,
        policy=args.covid_policy,
        covid_start=args.covid_start,
        covid_end=args.covid_end,
        monthly_freq=args.monthly_freq,
        lm_outliers_c=float(args.lm_outliers_c),
        lm_outliers_min_obs=int(args.lm_outliers_min_obs),
        lm_outliers_fit_window=fit_w,
    )

    train_span = fmt_span_from_strings(args.train_start, args.train_end)
    oos_span = fmt_span_from_index(panel_oos.index) if not panel_oos.empty else None

    panel_root = Path("dataset") / args.panel_name
    train_dir = panel_root / "train_panels"
    oos_dir = panel_root / "oos_panels"
    ensure_dir(train_dir)
    ensure_dir(oos_dir)

    y_root = Path("data") / "targets" / args.y_name
    ensure_dir(y_root)

    train_panel_path = train_dir / f"{args.panel_name}__train_{train_span}.csv"
    oos_panel_path = oos_dir / f"{args.panel_name}__oos_{oos_span}.csv" if oos_span is not None else None

    train_scaler_path = train_dir / f"{args.panel_name}__train_scaler.json"
    oos_info_path = oos_dir / f"{args.panel_name}__oos_info.json"

    y_train_path = y_root / f"{args.y_name}__train_{train_span}.csv"
    y_oos_path = y_root / f"{args.y_name}__oos_{oos_span}.csv" if oos_span is not None else None
    y_scaler_path = y_root / f"{args.y_name}__scaler.json"

    X_train_std.to_csv(train_panel_path, index_label="Date")
    if oos_panel_path is not None and not X_oos_std.empty:
        X_oos_std.to_csv(oos_panel_path, index_label="Date")

    y_train_std.to_frame(args.y_name).to_csv(y_train_path, index_label="Date")
    if y_oos_path is not None and not y_oos_std.empty:
        y_oos_std.to_frame(args.y_name).to_csv(y_oos_path, index_label="Date")

    Path(train_scaler_path).write_text(json.dumps(X_scaler, indent=2, sort_keys=True), encoding="utf-8")
    Path(y_scaler_path).write_text(json.dumps(y_scaler, indent=2, sort_keys=True), encoding="utf-8")

    if X_oos_std.empty:
        oos_start_str: str | None = None
        oos_end_str: str | None = None
    else:
        oos_start_ts = pd.to_datetime(X_oos_std.index.min())
        oos_end_ts = pd.to_datetime(X_oos_std.index.max())
        oos_start_str = oos_start_ts.strftime("%Y-%m-%d")
        oos_end_str = oos_end_ts.strftime("%Y-%m-%d")

    oos_info = OOSInfo(
        panel_name=args.panel_name,
        y_name=args.y_name,
        train_start=args.train_start,
        train_end=args.train_end,
        oos_start=oos_start_str,
        oos_end=oos_end_str,
        covid_policy=args.covid_policy,
        covid_start=args.covid_start,
        covid_end=args.covid_end,
        monthly_freq=args.monthly_freq,
        lm_outliers_c=float(args.lm_outliers_c),
        lm_outliers_min_obs=int(args.lm_outliers_min_obs),
        lm_outliers_fit_window=f"{fit_w[0]},{fit_w[1]}",
    )
    Path(oos_info_path).write_text(json.dumps(asdict(oos_info), indent=2, sort_keys=True), encoding="utf-8")

    print(f"[train] X: {train_panel_path}")
    print(f"[oos]   X: {oos_panel_path if (oos_panel_path is not None and not X_oos_std.empty) else '(no OOS rows)'}")
    print(f"[train] y: {y_train_path}")
    print(f"[oos]   y: {y_oos_path if (y_oos_path is not None and not y_oos_std.empty) else '(no OOS rows)'}")
    print(f"[info]  X scaler: {train_scaler_path}")
    print(f"[info]  y scaler: {y_scaler_path}")
    print(f"[info]  oos info: {oos_info_path}")


if __name__ == "__main__":
    main()
