# src/dfm_pipeline/validation/panel_missing_diagnostics.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, Dict, Any, List

import json
import numpy as np
import pandas as pd

__all__ = [
    "BoundaryDates",
    "compute_missingness_by_series",
    "save_missingness_report",
    "missing_positions_by_series",
    "missing_runs_by_series",
    "save_positions_jsonl",
]


# =============================== #
#   Index / boundary management   #
# =============================== #

@dataclass(frozen=True)
class BoundaryDates:
    first: pd.Timestamp
    second: pd.Timestamp
    last: pd.Timestamp
    notes: str = ""  # info about fallbacks if requested dates not found


def _coerce_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure a DatetimeIndex sorted ascending. Raise a clear error otherwise.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df = df.copy()
            df.index = pd.to_datetime(df.index, errors="raise")
        except Exception as e:
            raise TypeError(
                "DataFrame index must be a DatetimeIndex or convertible to datetime."
            ) from e
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()
    return df


def _resolve_boundaries(
    idx: pd.DatetimeIndex,
    *,
    first_date: Optional[str] = None,
    second_date: Optional[str] = None,
    last_date: Optional[str] = None,
    date_format: Optional[str] = None,  # e.g. "%m/%d/%Y"
) -> BoundaryDates:
    """
    Choose first/second/last timestamps to analyze. If explicit date strings are
    provided, use them if present in the index; otherwise fall back to positional
    rows and record a note. If not provided, use positional rows.
    """

    def _parse(s: str) -> pd.Timestamp:
        return pd.to_datetime(s, format=date_format) if date_format else pd.to_datetime(s)

    # Defaults from index positions
    first = idx[0]
    last = idx[-1]
    second = idx[1] if len(idx) > 1 else idx[0]

    # Optionally override with explicit dates (if present)
    notes = []
    if first_date:
        cand = _parse(first_date)
        if cand in idx:
            first = cand
        else:
            notes.append(
                f"[info] requested first_date={cand.date()} not in index; using first row {first.date()}"
            )
    if second_date:
        cand = _parse(second_date)
        if cand in idx:
            second = cand
        else:
            notes.append(
                f"[info] requested second_date={cand.date()} not in index; using second row {second.date()}"
            )
    if last_date:
        cand = _parse(last_date)
        if cand in idx:
            last = cand
        else:
            notes.append(
                f"[info] requested last_date={cand.date()} not in index; using last row {last.date()}"
            )

    return BoundaryDates(first=first, second=second, last=last, notes="\n".join(notes))


# ================================= #
#   Boundary-style missing report   #
# ================================= #

def compute_missingness_by_series(
    df: pd.DataFrame,
    *,
    first_date: Optional[str] = None,
    second_date: Optional[str] = None,
    last_date: Optional[str] = None,
    date_format: Optional[str] = "%m/%d/%Y",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Per-series flags and counts of missing values at:
      - first row, second row, both/any of first two
      - intermediate rows (strictly between second and last)
      - last row
    Also returns totals and percent missing.

    If first/second/last are not provided, they are inferred from the dataset.
    """
    df = _coerce_datetime_index(df)
    idx = df.index

    bounds = _resolve_boundaries(
        idx,
        first_date=first_date,
        second_date=second_date,
        last_date=last_date,
        date_format=date_format,
    )

    na = df.isna()

    # Safe accessors for boundary rows (fallback to positional)
    def _row_na(ts: pd.Timestamp, pos: int) -> pd.Series:
        return na.loc[ts] if ts in idx else na.iloc[pos]

    miss_first = _row_na(bounds.first, 0)
    miss_second = _row_na(bounds.second, 1 if len(idx) > 1 else 0)
    miss_last = _row_na(bounds.last, -1)

    # Intermediate = strictly between bounds.second and bounds.last
    if len(idx) >= 3:
        mask_mid = (idx > bounds.second) & (idx < bounds.last)
        na_mid = na.loc[mask_mid]
        if na_mid.empty:
            miss_mid_any = pd.Series(False, index=df.columns)
            n_mid = pd.Series(0, index=df.columns, dtype="int64")
        else:
            miss_mid_any = na_mid.any(axis=0)
            n_mid = na_mid.sum(axis=0).astype("int64")
    else:
        miss_mid_any = pd.Series(False, index=df.columns)
        n_mid = pd.Series(0, index=df.columns, dtype="int64")

    n_rows = len(df)
    n_missing_total = na.sum(axis=0).astype("int64")
    pct_missing = (n_missing_total / float(n_rows)) * 100.0

    by_series = pd.DataFrame(
        {
            "n_rows": n_rows,
            "n_missing": n_missing_total,
            "pct_missing": pct_missing,
            "miss_first": miss_first.astype(bool),
            "miss_second": miss_second.astype(bool),
            "miss_both_first_two": (miss_first & miss_second).astype(bool),
            "miss_any_first_two": (miss_first | miss_second).astype(bool),
            "miss_intermediate": miss_mid_any.astype(bool),
            "n_missing_intermediate": n_mid,
            "miss_last": miss_last.astype(bool),
        }
    ).sort_index()

    summary = {
        "n_series": int(by_series.shape[0]),
        "n_rows": int(n_rows),
        "boundaries": {
            "first": bounds.first.strftime("%Y-%m-%d"),
            "second": bounds.second.strftime("%Y-%m-%d"),
            "last": bounds.last.strftime("%Y-%m-%d"),
            "notes": bounds.notes,
        },
        "counts": {
            "missing_at_first": int(by_series["miss_first"].sum()),
            "missing_at_second": int(by_series["miss_second"].sum()),
            "missing_at_both_first_two": int(by_series["miss_both_first_two"].sum()),
            "missing_at_any_first_two": int(by_series["miss_any_first_two"].sum()),
            "missing_any_intermediate": int(by_series["miss_intermediate"].sum()),
            "missing_at_last": int(by_series["miss_last"].sum()),
        },
        "lists": {
            "series_missing_at_first": by_series.index[by_series["miss_first"]].tolist(),
            "series_missing_at_second": by_series.index[by_series["miss_second"]].tolist(),
            "series_missing_at_both_first_two": by_series.index[by_series["miss_both_first_two"]].tolist(),
            "series_missing_at_any_first_two": by_series.index[by_series["miss_any_first_two"]].tolist(),
            "series_missing_any_intermediate": by_series.index[by_series["miss_intermediate"]].tolist(),
            "series_missing_at_last": by_series.index[by_series["miss_last"]].tolist(),
        },
    }

    return by_series, summary


def save_missingness_report(
    by_series: pd.DataFrame,
    summary: Dict[str, Any],
    out_dir: Path | str = "data/quality_checks",
    *,
    stem: str = "missingness",
) -> Tuple[Path, Path]:
    """
    Save the per-series table as CSV and the summary as JSON (pretty-printed).
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{stem}_by_series.csv"
    json_path = out_dir / f"{stem}_summary.json"

    by_series.to_csv(csv_path, float_format="%.10g")
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return csv_path, json_path


# ======================================= #
#   NEW: exact missing positions / runs   #
# ======================================= #

def missing_positions_by_series(
    df: pd.DataFrame,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    date_format: Optional[str] = None,     # e.g., "%m/%d/%Y" for mm/dd/yyyy
    as_strings: bool = False,               # convert dates to strings
    limit_positions: Optional[int] = None,  # cap list length per series
) -> Tuple[pd.DataFrame, Dict[str, List[str | pd.Timestamp]]]:
    """
    For each series (column), return:
      - summary table with n_rows, n_missing, pct_missing, first_missing, last_missing,
        number of contiguous missing runs (n_runs), and longest_run;
      - dict mapping series -> list of missing dates (Timestamp or str).

    Use `start`/`end` to limit the window. If `as_strings` is True, dates are
    formatted using `date_format` (or ISO-8601 if None).
    """
    df = _coerce_datetime_index(df)
    if start or end:
        df = df.loc[start:end]

    idx = df.index
    na = df.isna()

    n_rows = len(df)
    n_missing = na.sum(axis=0).astype("int64")
    pct_missing = (n_missing / float(n_rows)) * 100.0

    positions: Dict[str, List[pd.Timestamp | str]] = {}
    first_missing: Dict[str, pd.Timestamp] = {}
    last_missing: Dict[str, pd.Timestamp] = {}
    n_runs: Dict[str, int] = {}
    longest_run: Dict[str, int] = {}

    def _runs_from_mask(mask: np.ndarray) -> List[tuple[int, int]]:
        where = np.flatnonzero(mask)
        if where.size == 0:
            return []
        splits = np.where(np.diff(where) > 1)[0] + 1
        groups = np.split(where, splits)
        return [(g[0], g[-1]) for g in groups]

    for col in df.columns:
        mask = na[col].to_numpy()
        if mask.any():
            where = np.flatnonzero(mask)
            pos_idx = idx[where]

            if as_strings:
                if date_format:
                    pos = [ts.strftime(date_format) for ts in pos_idx]
                else:
                    pos = [ts.isoformat() for ts in pos_idx]
            else:
                # Keep as Python datetime for JSON-serializable output via isoformat later if needed
                pos = list(pos_idx.to_pydatetime())

            if limit_positions is not None and len(pos) > limit_positions:
                pos = pos[:limit_positions]

            positions[col] = pos
            first_missing[col] = idx[where[0]]
            last_missing[col] = idx[where[-1]]

            runs = _runs_from_mask(mask)
            n_runs[col] = len(runs)
            longest_run[col] = 0 if not runs else max(e - s + 1 for s, e in runs)
        else:
            positions[col] = []
            first_missing[col] = pd.NaT
            last_missing[col] = pd.NaT
            n_runs[col] = 0
            longest_run[col] = 0

    summary_df = pd.DataFrame(
        {
            "n_rows": n_rows,
            "n_missing": n_missing,
            "pct_missing": (n_missing / float(n_rows)) * 100.0,
            "first_missing": pd.Series(first_missing),
            "last_missing": pd.Series(last_missing),
            "n_runs": pd.Series(n_runs, dtype="int64"),
            "longest_run": pd.Series(longest_run, dtype="int64"),
        }
    ).sort_index()

    return summary_df, positions


def missing_runs_by_series(
    df: pd.DataFrame,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    as_strings: bool = False,
    date_format: Optional[str] = None,
) -> pd.DataFrame:
    """
    List contiguous NA spans for each series as a long DataFrame with columns:
      ['series', 'start', 'end', 'length'].
    Useful when you prefer a compact artifact over every missing date.
    """
    df = _coerce_datetime_index(df)
    if start or end:
        df = df.loc[start:end]

    idx = df.index
    na = df.isna()
    rows: List[tuple[str, Any, Any, int]] = []

    for col in df.columns:
        mask = na[col].to_numpy()
        where = np.flatnonzero(mask)
        if where.size == 0:
            continue
        splits = np.where(np.diff(where) > 1)[0] + 1
        groups = np.split(where, splits)
        for g in groups:
            s_iloc, e_iloc = int(g[0]), int(g[-1])
            s, e = idx[s_iloc], idx[e_iloc]
            if as_strings:
                s = s.strftime(date_format) if date_format else s.isoformat()
                e = e.strftime(date_format) if date_format else e.isoformat()
            rows.append((col, s, e, e_iloc - s_iloc + 1))

    return pd.DataFrame(rows, columns=["series", "start", "end", "length"])


# ====================== #
#   Convenience savers   #
# ====================== #

def save_positions_jsonl(
    positions: Dict[str, List[pd.Timestamp | str]],
    path: Path | str,
) -> Path:
    """
    Save per-series missing positions as JSON Lines: one series per line:
      {"series": "<name>", "missing_dates": ["YYYY-MM-01", ...]}
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        for k, v in positions.items():
            # Ensure strings for stability
            vv = [ts if isinstance(ts, str) else pd.Timestamp(ts).isoformat() for ts in v]
            f.write(json.dumps({"series": k, "missing_dates": vv}) + "\n")
    return p
