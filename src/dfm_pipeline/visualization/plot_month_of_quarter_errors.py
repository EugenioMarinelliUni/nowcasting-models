#!/usr/bin/env python
"""
Plot quarterly MM nowcast errors separately by month-of-quarter.

Idea:
- For each quarter Q, we have a single quarterly truth y_Q,
  observed at the quarter-end month (e.g. Mar/Jun/Sep/Dec).
- At each month t within that quarter (1st/2nd/3rd month),
  we have a one-sided MM nowcast y_q_hat[t].

We "fill" the quarterly truth y_Q to all three months of quarter Q,
then compute errors:

    err_t = y_q_hat[t] - y_Q

and plot three time series:
- Month 1 errors
- Month 2 errors
- Month 3 errors

Expected input CSV structure (mf_dfm_oos_*.csv):

index: monthly DatetimeIndex (e.g. MS)
columns:
    - y_q      : standardized quarterly GDP (NaN except at quarter-ends)
    - y_q_hat  : MM-implied quarterly nowcast
    - error    : y_q_hat - y_q (NaN except at quarter-ends)
    - y_m_hat  : monthly latent GDP

Usage example
-------------
from pathlib import Path
from dfm_pipeline.visualization.plot_month_of_quarter_errors import (
    plot_month_of_quarter_errors,
)

plot_month_of_quarter_errors(
    oos_csv_path=Path("results/dfm_mf_mm/mf_dfm_oos_...r3_p2.csv"),
    test_start="2016-01-01",
)
"""

from pathlib import Path
from typing import Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_month_of_quarter_errors(
    oos_csv_path: Union[str, Path],
    test_start: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    show: bool = True,
    title: Optional[str] = None,
) -> plt.Axes:
    """
    Plot quarterly MM nowcast errors by month-of-quarter.

    Parameters
    ----------
    oos_csv_path : str or Path
        Path to mf_dfm_oos_*.csv produced by the DFM OOS scripts.
    test_start : str, optional
        If given (e.g. "2016-01-01"), restrict the plot to dates >= test_start.
        The string is passed to pandas.to_datetime.
    ax : matplotlib.axes.Axes, optional
        Existing axes to plot on. If None, a new figure/axes is created.
    show : bool, default True
        If True and ax is None, calls plt.show() at the end.
    title : str, optional
        Plot title. If None, a default title is used.

    Returns
    -------
    ax : matplotlib.axes.Axes
        The axes with the plot.
    """
    oos_csv_path = Path(oos_csv_path)

    df = pd.read_csv(oos_csv_path, index_col=0, parse_dates=True)

    if "y_q" not in df.columns or "y_q_hat" not in df.columns:
        raise ValueError("Expected 'y_q' and 'y_q_hat' columns in OOS CSV.")

    dates = pd.DatetimeIndex(df.index)
    y_q = df["y_q"]
    y_q_hat = df["y_q_hat"]

    # Build a quarterly index and "fill" the true quarterly value
    quarters = dates.to_period("Q")

    # fill NaNs within each quarter, but keep NaNs if quarter has no observed y_q at all
    def _fill_quarter(s: pd.Series) -> pd.Series:
        filled = s.ffill().bfill()
        # if the entire quarter is NaN, keep it as NaN
        if s.notna().any():
            return filled
        return s

    y_q_quarter = y_q.groupby(quarters).transform(_fill_quarter)

    # month position within quarter: 1,2,3
    # assuming standard MS monthly index (Jan/Apr/Jul/Oct = first month of Q)
    month_in_q = ((dates.month - 1) % 3) + 1

    # base mask: finite truth and finite nowcast
    base_mask = np.isfinite(y_q_quarter.to_numpy()) & np.isfinite(
        y_q_hat.to_numpy()
    )

    if test_start is not None:
        ts = pd.to_datetime(test_start)
        time_mask = dates >= ts
    else:
        time_mask = np.ones(len(dates), dtype=bool)

    # month-specific masks
    mask_m1 = base_mask & time_mask & (month_in_q == 1)
    mask_m2 = base_mask & time_mask & (month_in_q == 2)
    mask_m3 = base_mask & time_mask & (month_in_q == 3)

    err_m1 = pd.Series(
        y_q_hat[mask_m1].to_numpy() - y_q_quarter[mask_m1].to_numpy(),
        index=dates[mask_m1],
        name="err_m1",
    )
    err_m2 = pd.Series(
        y_q_hat[mask_m2].to_numpy() - y_q_quarter[mask_m2].to_numpy(),
        index=dates[mask_m2],
        name="err_m2",
    )
    err_m3 = pd.Series(
        y_q_hat[mask_m3].to_numpy() - y_q_quarter[mask_m3].to_numpy(),
        index=dates[mask_m3],
        name="err_m3",
    )

    if ax is None:
        fig, ax = plt.subplots()

    if not err_m1.empty:
        ax.plot(err_m1.index, err_m1.values, label="Month 1")
    if not err_m2.empty:
        ax.plot(err_m2.index, err_m2.values, label="Month 2")
    if not err_m3.empty:
        ax.plot(err_m3.index, err_m3.values, label="Month 3")

    ax.axhline(0.0, linestyle="--", linewidth=1)

    if title is None:
        title = "Quarterly MM nowcast error by month-of-quarter"

    ax.set_title(title)
    ax.set_xlabel("Date (monthly index)")
    ax.set_ylabel("Error (standardized units)")
    ax.legend()
    ax.grid(True, axis="y", linestyle=":", linewidth=0.7)

    if show and ax.get_figure() is not None:
        ax.get_figure().tight_layout()
        plt.show()

    return ax
