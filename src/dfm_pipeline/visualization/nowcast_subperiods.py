from __future__ import annotations

from typing import Iterable, Optional, Sequence

import pandas as pd
import matplotlib.pyplot as plt

from dfm_pipeline.visualization.nowcast_utils import Subperiod, compute_errors


def _assign_subperiod(
    df: pd.DataFrame,
    subperiods: Sequence[Subperiod],
) -> pd.DataFrame:
    """
    Add a 'subperiod' column based on year and provided subperiod definitions.
    """
    if "year" not in df.columns:
        raise ValueError("df must contain a 'year' column.")

    df = df.copy()
    df["subperiod"] = None

    for sp in subperiods:
        mask = (df["year"] >= sp.start_year) & (df["year"] <= sp.end_year)
        df.loc[mask, "subperiod"] = sp.label

    return df


def compute_rmse_by_spec_and_subperiod(
    df_long: pd.DataFrame,
    horizon: int,
    subperiods: Sequence[Subperiod],
    specs: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Compute RMSE per (spec, subperiod) for a given horizon.

    Returns DataFrame with columns: spec, subperiod, rmse.
    """
    if horizon not in (1, 2, 3):
        raise ValueError("horizon must be 1, 2, or 3.")

    d = compute_errors(df_long, spec=None)
    d = d[d["horizon_in_quarter"] == horizon].copy()
    d = _assign_subperiod(d, subperiods)
    d = d.dropna(subset=["subperiod"])

    if specs is not None:
        spec_list = list(specs)
        d = d[d["spec"].isin(spec_list)]

    # Named aggregation to get a DataFrame with 'rmse' column
    grouped = (
        d.groupby(["spec", "subperiod"], as_index=False)
        .agg(rmse=("error", lambda x: (x**2).mean() ** 0.5))
    )
    return grouped


def plot_subperiod_rmse_bars(
    df_long: pd.DataFrame,
    horizon: int,
    subperiods: Sequence[Subperiod],
    specs: Iterable[str],
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Grouped bar plot: RMSE by subperiod for given specs at a given horizon.
    """
    specs_list = list(specs)
    rmse_df = compute_rmse_by_spec_and_subperiod(
        df_long=df_long,
        horizon=horizon,
        subperiods=subperiods,
        specs=specs_list,
    )

    if rmse_df.empty:
        raise ValueError("No RMSE data for given specs and subperiods.")

    rmse_df["spec"] = pd.Categorical(rmse_df["spec"], categories=specs_list, ordered=True)
    sub_labels = [sp.label for sp in subperiods]
    rmse_df["subperiod"] = pd.Categorical(rmse_df["subperiod"], categories=sub_labels, ordered=True)

    pivot = rmse_df.pivot(index="spec", columns="subperiod", values="rmse")

    if ax is None:
        _, ax = plt.subplots(figsize=(10.0, 5.0))

    x = range(len(pivot.index))
    width = 0.8 / max(1, len(pivot.columns))

    for i, sub_label in enumerate(pivot.columns):
        y = pivot[sub_label].values
        positions = [pos + (i - len(pivot.columns) / 2) * width + width / 2 for pos in x]
        ax.bar(positions, y, width=width, label=sub_label)

    ax.set_xticks(list(x))
    ax.set_xticklabels(list(pivot.index), rotation=45, ha="right")
    ax.set_ylabel(f"RMSE (horizon h{horizon})")
    ax.set_title(f"Subperiod RMSE by spec (horizon h{horizon})")
    ax.legend()

    return ax


def plot_subperiod_rmse_heatmap(
    df_long: pd.DataFrame,
    horizon: int,
    subperiods: Sequence[Subperiod],
    specs: Iterable[str],
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Heatmap: rows = specs, columns = subperiods, values = RMSE.
    """
    specs_list = list(specs)
    rmse_df = compute_rmse_by_spec_and_subperiod(
        df_long=df_long,
        horizon=horizon,
        subperiods=subperiods,
        specs=specs_list,
    )

    if rmse_df.empty:
        raise ValueError("No RMSE data for given specs and subperiods.")

    rmse_df["spec"] = pd.Categorical(rmse_df["spec"], categories=specs_list, ordered=True)
    sub_labels = [sp.label for sp in subperiods]
    rmse_df["subperiod"] = pd.Categorical(rmse_df["subperiod"], categories=sub_labels, ordered=True)

    pivot = rmse_df.pivot(index="spec", columns="subperiod", values="rmse")
    data = pivot.values

    if ax is None:
        _, ax = plt.subplots(figsize=(10.0, 6.0))

    im = ax.imshow(data, aspect="auto")

    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(list(pivot.index))
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(list(pivot.columns), rotation=45, ha="right")

    ax.set_title(f"Subperiod RMSE heatmap (horizon h{horizon})")
    ax.set_xlabel("Subperiod")
    ax.set_ylabel("Spec")

    n_rows, n_cols = data.shape
    for i in range(n_rows):
        for j in range(n_cols):
            value = data[i, j]
            text = "" if pd.isna(value) else f"{value:.2f}"
            ax.text(j, i, text, ha="center", va="center", fontsize=8)

    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    return ax
