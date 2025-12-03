from __future__ import annotations

from typing import Optional

import pandas as pd
import matplotlib.pyplot as plt

from dfm_pipeline.visualization.nowcast_utils import compute_errors, ensure_horizon_and_quarter_ts


def plot_horizon_error_boxplots(
    df_long: pd.DataFrame,
    spec: str,
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Boxplots of forecast errors e = y_real_Q - y_hat per horizon (1,2,3)
    for a given spec.
    """
    d = compute_errors(df_long, spec=spec)

    data_by_horizon = []
    labels = []
    for h in (1, 2, 3):
        sub = d[d["horizon_in_quarter"] == h]["error"].dropna()
        if not sub.empty:
            data_by_horizon.append(sub.values)
            labels.append(f"h{h}")

    if not data_by_horizon:
        raise ValueError(f"No errors available for spec={spec!r} at any horizon.")

    if ax is None:
        _, ax = plt.subplots(figsize=(6.0, 4.0))

    # Avoid 'labels=' arg to keep some older stubs happy; set tick labels separately.
    ax.boxplot(data_by_horizon, showfliers=False)
    ax.set_xticks(range(1, len(labels) + 1))
    ax.set_xticklabels(labels)

    ax.axhline(0.0, linestyle="--", linewidth=1.0)
    ax.set_title(f"Error distributions by horizon – spec {spec}")
    ax.set_xlabel("Horizon in quarter")
    ax.set_ylabel("Forecast error (y_real_Q - y_hat)")

    return ax


def plot_calibration_scatter(
    df_long: pd.DataFrame,
    spec: str,
    horizon: int,
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Scatter plot of actual vs nowcast at a given horizon (calibration plot).
    """
    if horizon not in (1, 2, 3):
        raise ValueError("horizon must be 1, 2, or 3.")

    d = ensure_horizon_and_quarter_ts(df_long)
    if "spec" not in d.columns:
        raise ValueError("df_long has no 'spec' column.")

    d = d[(d["spec"] == spec) & (d["horizon_in_quarter"] == horizon)].copy()
    d = d.dropna(subset=["y_hat", "y_real_Q"])
    if d.empty:
        raise ValueError(f"No data for spec={spec!r} at horizon={horizon}.")

    if ax is None:
        _, ax = plt.subplots(figsize=(5.0, 5.0))

    ax.scatter(d["y_hat"], d["y_real_Q"], alpha=0.7, s=25)

    min_val = min(d["y_hat"].min(), d["y_real_Q"].min())
    max_val = max(d["y_hat"].max(), d["y_real_Q"].max())
    ax.plot([min_val, max_val], [min_val, max_val], linestyle="--", linewidth=1.0)

    ax.set_xlabel("Nowcast (y_hat)")
    ax.set_ylabel("Actual (y_real_Q)")
    ax.set_title(f"Calibration – spec {spec}, horizon h{horizon}")
    ax.set_xlim(min_val, max_val)
    ax.set_ylim(min_val, max_val)
    ax.set_aspect("equal", adjustable="box")

    return ax


def plot_rolling_error(
    df_long: pd.DataFrame,
    spec: str,
    horizon: int,
    window: int = 12,
    metric: str = "rmse",
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Rolling error metric over time (per quarter) for a given spec and horizon.

    metric: 'rmse' or 'mae'.
    """
    if horizon not in (1, 2, 3):
        raise ValueError("horizon must be 1, 2, or 3.")
    if metric not in ("rmse", "mae"):
        raise ValueError("metric must be 'rmse' or 'mae'.")

    d = compute_errors(df_long, spec=spec)
    d = d[d["horizon_in_quarter"] == horizon].copy()
    d = d.dropna(subset=["quarter_ts", "error"])

    if d.empty:
        raise ValueError(f"No errors for spec={spec!r}, horizon={horizon}.")

    d = d.sort_values("quarter_ts").groupby("quarter_ts", as_index=False)["error"].first()

    if metric == "rmse":
        series = (d["error"] ** 2).rolling(window=window).mean().pow(0.5)
        y_label = f"Rolling RMSE (window={window})"
    else:
        series = d["error"].abs().rolling(window=window).mean()
        y_label = f"Rolling MAE (window={window})"

    if ax is None:
        _, ax = plt.subplots(figsize=(10.0, 4.0))

    ax.plot(d["quarter_ts"], series)
    ax.set_title(f"{y_label} – spec {spec}, horizon h{horizon}")
    ax.set_xlabel("Quarter")
    ax.set_ylabel(y_label)

    return ax
