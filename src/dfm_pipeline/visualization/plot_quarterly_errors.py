#!/usr/bin/env python
"""
Plot quarterly Mariano–Murasawa nowcast errors over time
(one point per quarter, at quarter-end months).

Expected input CSV structure (e.g. mf_dfm_oos_*.csv):

index: monthly DatetimeIndex
columns:
    - y_q      : standardized quarterly GDP (NaN except at quarter-ends)
    - y_q_hat  : MM-implied quarterly nowcast
    - error    : y_q_hat - y_q (NaN except at quarter-ends)
    - y_m_hat  : monthly latent GDP

Usage example
-------------
from pathlib import Path
from dfm_pipeline.visualization.plot_quarterly_errors import plot_quarterly_errors

plot_quarterly_errors(
    oos_csv_path=Path("results/dfm_mf_mm/mf_dfm_oos_...r3_p2.csv"),
    test_start="2016-01-01",
)
"""

from pathlib import Path
from typing import Optional, Union

import matplotlib.pyplot as plt
import pandas as pd


def plot_quarterly_errors(
    oos_csv_path: Union[str, Path],
    test_start: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    show: bool = True,
    title: Optional[str] = None,
) -> plt.Axes:
    """
    Plot quarterly MM nowcast errors (y_q_hat - y_q) over time.

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

    if "error" not in df.columns:
        raise ValueError("Expected 'error' column in OOS CSV (y_q_hat - y_q).")

    err_q = df["error"].dropna()

    if test_start is not None:
        ts = pd.to_datetime(test_start)
        err_q = err_q[err_q.index >= ts]

    if ax is None:
        fig, ax = plt.subplots()

    ax.plot(err_q.index, err_q.values)
    ax.axhline(0.0, linestyle="--", linewidth=1)

    if title is None:
        title = "Quarterly MM nowcast error (y_q_hat - y_q)"

    ax.set_title(title)
    ax.set_xlabel("Date (quarter-end months)")
    ax.set_ylabel("Error (standardized units)")

    ax.grid(True, axis="y", linestyle=":", linewidth=0.7)

    if show and ax.get_figure() is not None:
        ax.get_figure().tight_layout()
        plt.show()

    return ax
