from __future__ import annotations

import numpy as np
import pandas as pd


def load_bm_inputs_from_csv(
    path_monthly_csv: str,
    path_quarterly_csv: str,
    *,
    monthly_date_col: str = "date",
    quarterly_date_col: str = "date",
    quarterly_value_col: str = "y",
    date_format: str | None = None,
    quarterly_period: str = "Q-DEC",
    place_quarterly_on: str = "quarter_end",  # "quarter_end" or "quarter_start"
    join: str = "inner",  # "inner" or "outer"
    sort_index: bool = True,
) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex]:
    """
    Load monthly panel Y (T,nM) and target y (T,) from CSV paths and align them
    on a common monthly DatetimeIndex (month-start).

    Supports two target formats:
      (a) Quarterly-dated target (one obs per quarter): mapped to a month (quarter_end/quarter_start).
      (b) Monthly-dated target already on the monthly index (with NaNs in non-quarterly months):
          used as-is (no quarter remapping).
    """

    def _read_with_date_index(path: str, date_col: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], format=date_format)
            df = df.set_index(date_col)
        else:
            c0 = df.columns[0]
            dt = pd.to_datetime(df[c0], format=date_format)
            df = df.drop(columns=[c0])
            df.index = dt

        if sort_index:
            df = df.sort_index()

        # normalize to month-start (Option B)
        df.index = df.index.to_period("M").to_timestamp(how="start")
        return df

    # --- monthly ---
    Xm = _read_with_date_index(path_monthly_csv, monthly_date_col)
    Xm = Xm.apply(pd.to_numeric, errors="coerce")

    # --- target ---
    yq = _read_with_date_index(path_quarterly_csv, quarterly_date_col)
    if quarterly_value_col not in yq.columns:
        raise ValueError(
            f"quarterly_value_col '{quarterly_value_col}' not in target CSV columns: {list(yq.columns)}"
        )

    yq = yq[[quarterly_value_col]].apply(pd.to_numeric, errors="coerce")

    # Decide whether target is already monthly-indexed (case b) or truly quarterly-dated (case a)
    # If there are far more unique months than unique quarters, treat as monthly.
    n_unique_months = yq.index.to_period("M").nunique()
    n_unique_quarters = yq.index.to_period(quarterly_period).nunique()

    is_already_monthly = n_unique_months > int(1.5 * n_unique_quarters)

    if is_already_monthly:
        # Use as-is: already monthly timestamps (often with NaNs in non-quarterly months).
        yq_m = pd.Series(
            index=yq.index,
            data=yq[quarterly_value_col].to_numpy(dtype=float),
            dtype=float,
        )
    else:
        # Quarterly-dated: map each quarter to a specific month.
        qper = yq.index.to_period(quarterly_period)
        if place_quarterly_on == "quarter_end":
            q_month = qper.asfreq("M", how="end").to_timestamp(how="end")
        elif place_quarterly_on == "quarter_start":
            q_month = qper.asfreq("M", how="start").to_timestamp(how="start")
        else:
            raise ValueError("place_quarterly_on must be 'quarter_end' or 'quarter_start'")

        q_month = q_month.to_period("M").to_timestamp(how="start")

        yq_m = pd.Series(index=q_month, data=yq[quarterly_value_col].to_numpy(dtype=float), dtype=float)

    # Deduplicate defensively (keep last)
    if yq_m.index.has_duplicates:
        yq_m = yq_m.groupby(level=0).last()

    if sort_index:
        yq_m = yq_m.sort_index()

    # --- align on a common monthly index ---
    if join == "inner":
        idx = Xm.index
    elif join == "outer":
        idx = Xm.index.union(yq_m.index)
    else:
        raise ValueError("join must be 'inner' or 'outer'")

    if sort_index:
        idx = idx.sort_values()

    Xm_aligned = Xm.reindex(idx)
    y_aligned = yq_m.reindex(idx)

    Y_monthly = Xm_aligned.to_numpy(dtype=float)
    y_target = y_aligned.to_numpy(dtype=float)

    return Y_monthly, y_target, idx
