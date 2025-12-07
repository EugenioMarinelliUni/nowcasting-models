# src/dfm_pipeline/visualization/nowcast_horizon_errors.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


HorizonMetric = Literal["error", "abs_error"]
PlotKind = Literal["box", "violin"]


@dataclass
class HorizonErrorConfig:
    """
    Configuration for horizon-specific error distribution plots.

    Attributes
    ----------
    panel_id : str
        Panel identifier, e.g. "1960_noVIX_full_mu1990_1999".
    spec : str
        DFM specification label, e.g. "q4_r1_p2".
    metric : {"error", "abs_error"}
        "error"     -> use e = y_hat - y_real_Q
        "abs_error" -> use |e|.
    kind : {"box", "violin"}
        "box"    -> boxplot per horizon
        "violin" -> violin plot per horizon.
    eval_start : Optional[str]
        Optional lower bound on Date (YYYY-MM-DD) to restrict evaluation window.
    eval_end : Optional[str]
        Optional upper bound on Date (YYYY-MM-DD) to restrict evaluation window.
    figsize : tuple[float, float]
        Figure size.
    """

    panel_id: str
    spec: str
    metric: HorizonMetric = "abs_error"
    kind: PlotKind = "box"
    eval_start: Optional[str] = None
    eval_end: Optional[str] = None
    figsize: tuple[float, float] = (8.0, 4.0)


def _detect_horizon_column(df: pd.DataFrame) -> str:
    """
    Detect the horizon column in the long forecasts panel.

    Prefers 'month_in_quarter'; falls back to 'horizon' if present.
    """
    if "month_in_quarter" in df.columns:
        return "month_in_quarter"
    if "horizon" in df.columns:
        return "horizon"
    raise ValueError(
        "Could not detect horizon column: expected 'month_in_quarter' "
        "or 'horizon' in the forecast DataFrame."
    )


def _prepare_horizon_error_df(
    df_long: pd.DataFrame,
    cfg: HorizonErrorConfig,
) -> pd.DataFrame:
    """
    Filter df_long to a single panel/spec and compute the chosen error metric.

    Returns a DataFrame with columns:
    - horizon (int: 1,2,3)
    - error or abs_error
    """
    required_cols = {"panel_id", "spec", "y_hat", "y_real_Q"}
    missing = required_cols.difference(df_long.columns)
    if missing:
        raise ValueError(
            f"df_long is missing required columns {sorted(missing)}. "
            "Expected at least: panel_id, spec, y_hat, y_real_Q."
        )

    d = df_long[(df_long["panel_id"] == cfg.panel_id) & (df_long["spec"] == cfg.spec)].copy()
    if d.empty:
        raise ValueError(
            f"No rows found for panel_id={cfg.panel_id!r}, spec={cfg.spec!r} "
            "in the forecast panel."
        )

    # Optional date filtering
    if "Date" in d.columns and (cfg.eval_start is not None or cfg.eval_end is not None):
        d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
        if cfg.eval_start is not None:
            d = d[d["Date"] >= pd.to_datetime(cfg.eval_start)]
        if cfg.eval_end is not None:
            d = d[d["Date"] <= pd.to_datetime(cfg.eval_end)]
        d = d.dropna(subset=["Date"])
        if d.empty:
            raise ValueError(
                f"After applying eval window [{cfg.eval_start}, {cfg.eval_end}] "
                f"no rows remain for panel_id={cfg.panel_id!r}, spec={cfg.spec!r}."
            )

    horizon_col = _detect_horizon_column(d)

    # Compute raw error if not present
    if "err" in d.columns:
        err = d["err"].to_numpy(dtype=float)
    else:
        err = (d["y_hat"] - d["y_real_Q"]).to_numpy(dtype=float)

    if cfg.metric == "error":
        metric_vals = err
        metric_name = "error"
        ylabel = "Forecast error (y_hat - y_real_Q)"
    else:
        metric_vals = np.abs(err)
        metric_name = "abs_error"
        ylabel = "|Forecast error|"

    horizons = d[horizon_col].astype(int).to_numpy()

    df_plot = pd.DataFrame(
        {
            "horizon": horizons,
            metric_name: metric_vals,
        }
    )

    # Drop NaNs in metric
    df_plot = df_plot.dropna(subset=[metric_name])

    if df_plot.empty:
        raise ValueError(
            f"No non-NaN {metric_name} values for panel_id={cfg.panel_id!r}, "
            f"spec={cfg.spec!r} in the selected window."
        )

    df_plot.attrs["metric_name"] = metric_name
    df_plot.attrs["ylabel"] = ylabel
    return df_plot


def plot_horizon_error_distribution(
    df_long: pd.DataFrame,
    cfg: HorizonErrorConfig,
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Plot horizon-specific error distributions (box or violin) for a single panel/spec.

    Parameters
    ----------
    df_long : DataFrame
        Long-format per-vintage forecast panel with columns including:
        - panel_id, spec, y_hat, y_real_Q
        - month_in_quarter or horizon
        - optionally Date (for eval window filtering).
    cfg : HorizonErrorConfig
        Plot configuration (panel_id, spec, metric, kind, eval window).
    ax : optional matplotlib Axes
        If provided, draw on this axis; otherwise create a new figure/axis.

    Returns
    -------
    ax : matplotlib Axes
    """
    df_plot = _prepare_horizon_error_df(df_long, cfg)
    metric_name = df_plot.attrs["metric_name"]
    ylabel = df_plot.attrs["ylabel"]

    if ax is None:
        _, ax = plt.subplots(figsize=cfg.figsize)

    # Collect data per horizon
    data_by_h = []
    horizons_sorted = sorted(df_plot["horizon"].unique())
    for h in horizons_sorted:
        vals = df_plot.loc[df_plot["horizon"] == h, metric_name].to_numpy()
        if vals.size == 0:
            data_by_h.append(np.array([], dtype=float))
        else:
            data_by_h.append(vals)

    # Positions and labels
    positions = list(range(1, len(horizons_sorted) + 1))
    labels = [f"m{int(h)}" for h in horizons_sorted]

    if cfg.kind == "box":
        ax.boxplot(
            data_by_h,
            positions=positions,
            widths=0.6,
            showfliers=True,
        )
    else:  # "violin"
        parts = ax.violinplot(
            data_by_h,
            positions=positions,
            widths=0.8,
            showmeans=False,
            showmedians=True,
            showextrema=True,
        )
        # Optional: slightly adjust violin appearance
        for pc in parts.get("bodies", []):
            pc.set_alpha(0.6)

    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    ax.set_xlabel("Month in quarter (horizon)")
    ax.set_ylabel(ylabel)

    title = (
        f"Horizon-specific {cfg.metric} distribution\n"
        f"panel={cfg.panel_id}, spec={cfg.spec}"
    )
    if cfg.eval_start or cfg.eval_end:
        title += f"\nwindow: [{cfg.eval_start or '-inf'}, {cfg.eval_end or '+inf'}]"
    ax.set_title(title)

    ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.5)

    return ax

def plot_horizon_error_distribution_three_panels_row(
    df_long: pd.DataFrame,
    panel_ids: list[str],
    spec: str,
    metric: HorizonMetric = "abs_error",
    kind: PlotKind = "box",
    eval_start: Optional[str] = None,
    eval_end: Optional[str] = None,
    figsize: tuple[float, float] = (12.0, 4.0),
) -> tuple[plt.Figure, list[plt.Axes]]:
    """
    Convenience helper: plot horizon-specific error distributions for
    three panels (e.g. full / reduced A / reduced B) in one row of subplots.

    Parameters
    ----------
    df_long : DataFrame
        Long-format nowcast panel with columns:
        - panel_id, spec, y_hat, y_real_Q
        - month_in_quarter or horizon
        - Date (optional) for eval window.
    panel_ids : list[str]
        Exactly three panel identifiers to compare.
    spec : str
        DFM specification label, e.g. "q4_r1_p2".
    metric : {"error", "abs_error"}
        Error metric.
    kind : {"box", "violin"}
        Plot type.
    eval_start, eval_end : str | None
        Optional evaluation window bounds.
    figsize : tuple[float, float]
        Figure size for the full row.

    Returns
    -------
    fig, axes
        Matplotlib Figure and list of three Axes.
    """
    if len(panel_ids) != 3:
        raise ValueError("panel_ids must contain exactly three entries.")

    fig, axes = plt.subplots(
        nrows=1,
        ncols=3,
        figsize=figsize,
        sharey=True,
    )

    for ax, pid in zip(axes, panel_ids):
        cfg = HorizonErrorConfig(
            panel_id=pid,
            spec=spec,
            metric=metric,
            kind=kind,
            eval_start=eval_start,
            eval_end=eval_end,
            figsize=(figsize[0] / 3.0, figsize[1]),
        )
        plot_horizon_error_distribution(df_long, cfg=cfg, ax=ax)
        # Replace the long default title with something compact
        ax.set_title(pid, fontsize=9)

    fig.tight_layout()
    return fig, list(axes)
