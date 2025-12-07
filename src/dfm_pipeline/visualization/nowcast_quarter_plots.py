# src/dfm_pipeline/visualization/nowcast_quarter_plots.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Optional progress bar
try:
    from tqdm.auto import tqdm
except ImportError:  # tqdm is optional
    tqdm = None


@dataclass
class NowcastPlotConfig:
    """
    Configuration for the four-dots-per-quarter nowcast plot.

    Attributes
    ----------
    spec : str
        Hyperparameter configuration label, e.g. "q4_r1_p1".
    title : Optional[str]
        Optional title for the plot. If None, a default will be used.
    figsize : tuple[float, float]
        Matplotlib figure size.
    jitter_days : dict[str, int]
        Optional horizontal jitter (in days) for each horizon label to reduce overlap.
        Keys should be among: "m1", "m2", "m3", "actual". Values are integer day offsets.
        Use zeros for no jitter.
    """

    spec: str
    title: Optional[str] = None
    figsize: tuple[float, float] = (10.0, 5.0)
    jitter_days: Optional[dict[str, int]] = None


def _prepare_plot_df_for_spec(df_long: pd.DataFrame, spec: str) -> pd.DataFrame:
    """
    Build a long table with 4 series per quarter (m1, m2, m3, actual) for a single spec.

    Parameters
    ----------
    df_long : DataFrame
        Long-format DFM forecast table. Must contain at least:
        - "Date": monthly datetime or string
        - "spec": hyperparameter configuration label
        - "year": integer year of target quarter
        - "quarter": integer quarter of year (1..4)
        - "y_hat": model nowcast for the quarter
        - "y_real_Q": realized quarterly target value
    spec : str
        Hyperparameter configuration label to filter on.

    Returns
    -------
    plot_df : DataFrame
        Columns: ["quarter_ts", "horizon_label", "value"].
        For each quarter, up to four rows:
        - horizon_label = "m1", "m2", "m3" from the three monthly nowcasts
        - horizon_label = "actual" from the realized value
    """
    if "Date" not in df_long.columns:
        raise ValueError("df_long must contain a 'Date' column.")

    if "spec" not in df_long.columns:
        raise ValueError("df_long must contain a 'spec' column.")

    required_cols = {"year", "quarter", "y_hat", "y_real_Q"}
    missing = required_cols.difference(df_long.columns)
    if missing:
        raise ValueError(f"df_long is missing required columns: {sorted(missing)}")

    d = df_long[df_long["spec"] == spec].copy()
    if d.empty:
        raise ValueError(f"No rows found for spec={spec!r} in the long forecasts table.")

    # Ensure Date is datetime
    if not pd.api.types.is_datetime64_any_dtype(d["Date"]):
        d["Date"] = pd.to_datetime(d["Date"])

    # Month-in-quarter: 1, 2, 3 (assuming quarterly evaluation)
    d["horizon_in_quarter"] = ((d["Date"].dt.month - 1) % 3) + 1

    # Quarter timestamp for x-axis (quarter end)
    q_str = d["year"].astype(str) + "Q" + d["quarter"].astype(str)
    quarter_periods = pd.PeriodIndex(q_str, freq="Q")
    d["quarter_ts"] = quarter_periods.to_timestamp("Q")

    # Nowcasts per quarter and horizon
    nowcasts = (
        d[["quarter_ts", "horizon_in_quarter", "y_hat"]]
        .rename(columns={"y_hat": "value"})
        .copy()
    )
    nowcasts["horizon_label"] = nowcasts["horizon_in_quarter"].map(
        {1: "m1", 2: "m2", 3: "m3"}
    )

    # Actuals: one per quarter
    actual = (
        d.groupby("quarter_ts", as_index=False)
        .agg({"y_real_Q": "first"})
        .rename(columns={"y_real_Q": "value"})
    )
    actual["horizon_label"] = "actual"

    plot_df = pd.concat(
        [nowcasts[["quarter_ts", "horizon_label", "value"]], actual],
        ignore_index=True,
    )

    plot_df = plot_df.dropna(subset=["quarter_ts", "horizon_label", "value"])

    return plot_df


def plot_four_dots_per_quarter(
    df_long: pd.DataFrame,
    cfg: NowcastPlotConfig,
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Plot four dots per quarter (three nowcasts + actual) for a given spec.
    """
    plot_df = _prepare_plot_df_for_spec(df_long, spec=cfg.spec)

    if ax is None:
        _, ax = plt.subplots(figsize=cfg.figsize)

    color_map = {
        "m1": "tab:blue",
        "m2": "tab:orange",
        "m3": "tab:green",
        "actual": "black",
    }
    marker_map = {
        "m1": "o",
        "m2": "s",
        "m3": "D",
        "actual": "x",
    }

    jitter_days = cfg.jitter_days or {
        "m1": -5,
        "m2": 0,
        "m3": 5,
        "actual": 0,
    }

    horizons = ["m1", "m2", "m3", "actual"]
    for horizon in horizons:
        sub = plot_df[plot_df["horizon_label"] == horizon].copy()
        if sub.empty:
            continue

        shift_days = jitter_days.get(horizon, 0)
        if shift_days != 0:
            sub["x"] = sub["quarter_ts"] + pd.to_timedelta(shift_days, unit="D")
        else:
            sub["x"] = sub["quarter_ts"]

        ax.scatter(
            sub["x"],
            sub["value"],
            label=horizon,
            color=color_map.get(horizon, "gray"),
            marker=marker_map.get(horizon, "o"),
            s=25,
        )

    title = cfg.title or f"DFM nowcasts – spec {cfg.spec}"
    ax.set_title(title)
    ax.set_xlabel("Quarter")
    ax.set_ylabel("Target value (same units as y_real_Q)")
    ax.legend()
    ax.figure.autofmt_xdate()

    return ax


def save_four_dots_plot_for_specs(
    df_long: pd.DataFrame,
    specs: Iterable[str],
    output_dir: str,
    base_figsize: tuple[float, float] = (10.0, 5.0),
) -> None:
    """
    Generate and save a PNG for each spec, with an optional progress bar.

    Parameters
    ----------
    df_long : DataFrame
        Long-format forecasts table.
    specs : iterable of str
        List of spec labels to plot.
    output_dir : str
        Directory where PNG files will be written.
    base_figsize : tuple[float, float]
        Figure size for each plot.
    """
    os.makedirs(output_dir, exist_ok=True)

    spec_list = list(specs)

    if tqdm is not None:
        iterator = tqdm(spec_list, desc="Plotting nowcasts", unit="spec")
    else:
        iterator = spec_list

    for spec in iterator:
        cfg = NowcastPlotConfig(
            spec=spec,
            title=f"DFM nowcasts – spec {spec}",
            figsize=base_figsize,
        )
        fig, ax = plt.subplots(figsize=cfg.figsize)
        try:
            plot_four_dots_per_quarter(df_long, cfg=cfg, ax=ax)
        except ValueError as e:
            plt.close(fig)
            print(f"Skipping spec {spec!r}: {e}")
            continue

        out_path = os.path.join(output_dir, f"nowcasts_four_dots_{spec}.png")
        fig.tight_layout()
        fig.savefig(out_path, dpi=150)
        plt.close(fig)


def plot_quarter_nowcasts_multi_panel(
    df_all: pd.DataFrame,
    *,
    model_col: str = "model",
    horizon_col: str = "month_in_quarter",
    y_real_col: str = "y_real_Q",
    y_hat_col: str = "y_hat",
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Plot, for each quarter, 3x3 nowcasts (3 models x 3 horizons) as dots,
    plus the actual value.

    Expected columns in df_all:
      - 'year', 'quarter'
      - model_col: identifies panel/model ('full', 'stab', 'vote', ...)
      - horizon_col: 1,2,3 (month-in-quarter or horizon index)
      - y_real_col: actual quarterly target (same for all rows of that quarter)
      - y_hat_col: nowcast

    df_all should already be filtered to a single (q,p) specification per model.
    """
    if ax is None:
        _, ax = plt.subplots(figsize=(12.0, 5.0))

    required = {"year", "quarter", model_col, horizon_col, y_real_col, y_hat_col}
    missing = required.difference(df_all.columns)
    if missing:
        raise ValueError(f"df_all is missing required columns: {missing}")

    df_all = df_all.copy()

    # Sort and build integer quarter index for x-axis
    df_all = df_all.sort_values(["year", "quarter", horizon_col, model_col])
    uq = df_all[["year", "quarter"]].drop_duplicates().reset_index(drop=True)
    uq["q_idx"] = np.arange(len(uq))
    df_all = df_all.merge(uq, on=["year", "quarter"], how="left")

    # Model and horizon ordering
    model_order = sorted(df_all[model_col].unique())
    horizons = sorted(df_all[horizon_col].unique())

    # Colors by model, markers by horizon
    default_colors = plt.rcParams["axes.prop_cycle"].by_key().get("color", [])
    color_map = {m: default_colors[i % len(default_colors)] for i, m in enumerate(model_order)}
    marker_map = {
        h: mk
        for h, mk in zip(
            horizons,
            ["o", "s", "^", "D", "v"],  # up to 5 horizons if ever needed
        )
    }

    # Offsets so points don't overlap exactly
    if len(model_order) > 1:
        base_offsets = np.linspace(-0.25, 0.25, len(model_order))
    else:
        base_offsets = [0.0]
    model_offset = {m: base_offsets[i] for i, m in enumerate(model_order)}

    horiz_jitter = {h: (h - np.mean(horizons)) * 0.03 for h in horizons}

    # Scatter for all model × horizon combos
    for m in model_order:
        for h in horizons:
            sub = df_all[(df_all[model_col] == m) & (df_all[horizon_col] == h)]
            if sub.empty:
                continue
            x = sub["q_idx"] + model_offset[m] + horiz_jitter[h]
            y = sub[y_hat_col]
            ax.scatter(
                x,
                y,
                s=20,
                color=color_map[m],
                marker=marker_map.get(h, "o"),
                alpha=0.8,
                label=f"{m}, h{h}",
            )

    # Actual values: one per quarter
    actual = (
        df_all[["q_idx", "year", "quarter", y_real_col]]
        .drop_duplicates(subset=["year", "quarter"])
        .sort_values("q_idx")
    )
    ax.scatter(
        actual["q_idx"],
        actual[y_real_col],
        s=30,
        color="black",
        marker="x",
        label="actual",
    )

    # x-axis tick labels as year-quarter (sparse)
    tick_idx = actual["q_idx"].to_numpy()
    tick_labels = actual.apply(
        lambda r: f"{int(r['year'])}Q{int(r['quarter'])}", axis=1
    ).to_numpy()

    if len(tick_idx) > 20:
        step = max(1, len(tick_idx) // 20)
    else:
        step = 1

    ax.set_xticks(tick_idx[::step])
    ax.set_xticklabels(tick_labels[::step], rotation=45, ha="right")

    ax.set_xlabel("Quarter")
    ax.set_ylabel("Standardized target")
    ax.set_title("Nowcasts (models x horizons) vs actual per quarter")

    # De-duplicate legend entries
    handles, labels = ax.get_legend_handles_labels()
    seen = set()
    new_handles, new_labels = [], []
    for h, lab in zip(handles, labels):
        if lab in seen:
            continue
        seen.add(lab)
        new_handles.append(h)
        new_labels.append(lab)
    ax.legend(new_handles, new_labels, loc="best", fontsize=8, ncol=3)

    ax.grid(True, axis="y", alpha=0.2)

    return ax
