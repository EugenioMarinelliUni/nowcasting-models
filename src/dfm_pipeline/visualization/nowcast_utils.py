from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import pandas as pd


def ensure_horizon_and_quarter_ts(df_long: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df_long has:
    - 'Date' as datetime
    - 'horizon_in_quarter' (1,2,3) based on month position within quarter
    - 'quarter_ts' as quarter-end timestamp
    """
    required_cols = {"Date", "year", "quarter"}
    missing = required_cols.difference(df_long.columns)
    if missing:
        raise ValueError(f"df_long is missing required columns: {sorted(missing)}")

    d = df_long.copy()

    if not pd.api.types.is_datetime64_any_dtype(d["Date"]):
        d["Date"] = pd.to_datetime(d["Date"])

    # Month-in-quarter: 1, 2, 3 (assuming quarterly evaluation)
    d["horizon_in_quarter"] = ((d["Date"].dt.month - 1) % 3) + 1

    # Quarter timestamp for x-axis: build "2000Q1" and convert
    q_str = d["year"].astype(str) + "Q" + d["quarter"].astype(str)
    quarter_periods = pd.PeriodIndex(q_str, freq="Q")
    d["quarter_ts"] = quarter_periods.to_timestamp("Q")

    return d


def compute_errors(
    df_long: pd.DataFrame,
    spec: str | None = None,
) -> pd.DataFrame:
    """
    Add forecast errors per horizon:
    error = y_real_Q - y_hat

    Optionally filter to a single spec.
    """
    required_cols = {"y_hat", "y_real_Q"}
    missing = required_cols.difference(df_long.columns)
    if missing:
        raise ValueError(f"df_long is missing required columns: {sorted(missing)}")

    d = ensure_horizon_and_quarter_ts(df_long)

    if spec is not None:
        if "spec" not in d.columns:
            raise ValueError("df_long has no 'spec' column but spec was provided.")
        d = d[d["spec"] == spec].copy()
        if d.empty:
            raise ValueError(f"No rows found for spec={spec!r} in df_long.")

    d["error"] = d["y_real_Q"] - d["y_hat"]
    return d


def parse_q_p_from_spec(spec: str) -> Tuple[int, int]:
    """
    Parse q and p from a spec string of the form 'q{q}_r{r}_p{p}'.

    Example:
        'q3_r1_p2' -> (3, 2)
    """
    parts = spec.split("_")
    q_val = None
    p_val = None
    for part in parts:
        if part.startswith("q"):
            q_val = int(part[1:])
        elif part.startswith("p"):
            p_val = int(part[1:])

    if q_val is None or p_val is None:
        raise ValueError(f"Could not parse q and p from spec={spec!r}")

    return q_val, p_val


@dataclass
class Subperiod:
    label: str
    start_year: int
    end_year: int
