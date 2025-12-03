from __future__ import annotations

from typing import Dict, Optional

import pandas as pd
import matplotlib.pyplot as plt

from dfm_pipeline.visualization.nowcast_utils import compute_errors, parse_q_p_from_spec


def compute_rmse_by_spec(
    df_long: pd.DataFrame,
    horizon: int,
) -> pd.DataFrame:
    """
    Compute overall RMSE per spec for a given horizon.

    Returns DataFrame with columns: spec, rmse.
    """
    if horizon not in (1, 2, 3):
        raise ValueError("horizon must be 1, 2, or 3.")

    d = compute_errors(df_long, spec=None)
    d = d[d["horizon_in_quarter"] == horizon].copy()
    d = d.dropna(subset=["spec", "error"])

    grouped = (
        d.groupby("spec", as_index=False)
        .agg(rmse=("error", lambda x: (x**2).mean() ** 0.5))
    )
    return grouped


def plot_complexity_vs_rmse(
    df_long: pd.DataFrame,
    horizon: int,
    ax: Optional[plt.Axes] = None,
) -> plt.Axes:
    """
    Scatter plot of model complexity vs RMSE (per spec) for a given horizon.

    Complexity = q * p, parsed from spec name 'q{q}_r{r}_p{p}'.
    """
    rmse_df = compute_rmse_by_spec(df_long, horizon=horizon)
    if rmse_df.empty:
        raise ValueError("No RMSE data available for any spec.")

    complexities: Dict[str, int] = {}
    for spec in rmse_df["spec"]:
        q_val, p_val = parse_q_p_from_spec(spec)
        complexities[spec] = q_val * p_val

    rmse_df["complexity"] = rmse_df["spec"].map(complexities)

    if ax is None:
        _, ax = plt.subplots(figsize=(7.0, 5.0))

    ax.scatter(
        rmse_df["complexity"],
        rmse_df["rmse"],
        s=25,
    )

    ax.set_xlabel("Complexity (q * p)")
    ax.set_ylabel(f"RMSE (horizon h{horizon})")
    ax.set_title("Complexity vs performance")

    return ax
