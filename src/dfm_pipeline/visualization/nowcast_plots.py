from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import matplotlib.pyplot as plt


REQUIRED_COLS = {"eval_date", "moq", "horizon", "target_date", "pred", "actual"}


def read_predictions_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    miss = REQUIRED_COLS - set(df.columns)
    if miss:
        raise ValueError(f"missing columns in {path}: {sorted(miss)}")

    df = df.copy()
    df["eval_date"] = pd.to_datetime(df["eval_date"])
    df["target_date"] = pd.to_datetime(df["target_date"])
    df["moq"] = pd.to_numeric(df["moq"], errors="coerce")
    df["pred"] = pd.to_numeric(df["pred"], errors="coerce")
    df["actual"] = pd.to_numeric(df["actual"], errors="coerce")

    df = df.dropna(subset=["eval_date", "target_date", "moq", "pred", "actual"])
    df["moq"] = df["moq"].astype(int)
    return df


def last_pred_per_target_moq(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["target_date", "moq", "eval_date"])
    return df.groupby(["target_date", "moq"], as_index=False).tail(1)


def _draw_vertical_lines(x_index: pd.DatetimeIndex, y_min: float, y_max: float) -> None:
    # Thin vertical reference lines at each target_date (helps visually separate quarters)
    for x in x_index:
        plt.vlines(x, y_min, y_max, linewidth=0.5, alpha=0.25)


def plot_nowcasts_moq123_vs_actual(
    df: pd.DataFrame,
    outpath: Path,
    horizon: str = "now",
    title: Optional[str] = None,
    *,
    marker_only: bool = False,
    colors: Optional[dict[str, str]] = None,
    show_vlines: bool = True,
    dot_size: int = 18,
) -> None:
    d = df[df["horizon"] == horizon].copy()
    if d.empty:
        raise ValueError(f"no rows for horizon={horizon}")

    # Keep last (eval_date) per (target_date, moq)
    last = last_pred_per_target_moq(d)

    # Wide preds: columns are moq=1/2/3; index is target_date
    wide = last.pivot(index="target_date", columns="moq", values="pred").sort_index()

    # Actual per target_date (same across moq; take last)
    actual = last.groupby("target_date")["actual"].last().sort_index()

    # Default colors: 3 nowcast months + actual (4 distinct colors)
    if colors is None:
        colors = {
            "moq1": "tab:blue",
            "moq2": "tab:orange",
            "moq3": "tab:green",
            "actual": "tab:red",
        }

    # Compute y-range for vertical reference lines
    y_vals = []
    for m in (1, 2, 3):
        if m in wide.columns:
            y_vals.append(wide[m].to_numpy())
    y_vals.append(actual.to_numpy())
    y_all = pd.Series(pd.concat([pd.Series(v) for v in y_vals], ignore_index=True)).dropna()
    y_min = float(y_all.min()) if not y_all.empty else 0.0
    y_max = float(y_all.max()) if not y_all.empty else 1.0

    plt.figure()

    if show_vlines:
        _draw_vertical_lines(wide.index, y_min, y_max)

    # Plot predictions for each moq at the SAME x (target_date) => vertical alignment per date
    for m, key in [(1, "moq1"), (2, "moq2"), (3, "moq3")]:
        if m in wide.columns:
            x = wide.index
            y = wide[m].to_numpy()
            if marker_only:
                plt.scatter(x, y, s=dot_size, color=colors[key], label=f"nowcast moq={m}")
            else:
                plt.plot(x, y, color=colors[key], label=f"nowcast moq={m}")

    # Plot actuals as dots too when marker_only=True
    if marker_only:
        plt.scatter(actual.index, actual.to_numpy(), s=dot_size, color=colors["actual"], label="actual")
    else:
        plt.plot(actual.index, actual.to_numpy(), color=colors["actual"], label="actual")

    plt.xlabel("Quarter (target_date)")
    plt.ylabel("Value")
    plt.title(title or f"{horizon.upper()} nowcasts vs actual (moq=1/2/3)")
    plt.legend()
    plt.tight_layout()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outpath, dpi=150)
    plt.close()


def plot_nowcast_moq_vs_actual(
    df: pd.DataFrame,
    outpath: Path,
    horizon: str = "now",
    moq: int = 3,
    title: Optional[str] = None,
    *,
    marker_only: bool = False,
    colors: Optional[dict[str, str]] = None,
    show_vlines: bool = True,
    dot_size: int = 18,
) -> None:
    if moq not in (1, 2, 3):
        raise ValueError("moq must be 1, 2, or 3")

    d = df[(df["horizon"] == horizon) & (df["moq"] == moq)].copy()
    if d.empty:
        raise ValueError(f"no rows for horizon={horizon} moq={moq}")

    # keep last per target_date
    d = d.sort_values(["target_date", "eval_date"]).groupby("target_date", as_index=False).tail(1)
    d = d.sort_values("target_date")

    if colors is None:
        colors = {
            "pred": "tab:blue",
            "actual": "tab:red",
        }

    y_all = pd.concat([d["pred"], d["actual"]], axis=0).dropna()
    y_min = float(y_all.min()) if not y_all.empty else 0.0
    y_max = float(y_all.max()) if not y_all.empty else 1.0

    plt.figure()

    if show_vlines:
        _draw_vertical_lines(pd.DatetimeIndex(d["target_date"]), y_min, y_max)

    if marker_only:
        plt.scatter(d["target_date"], d["pred"], s=dot_size, color=colors["pred"], label=f"nowcast (moq={moq})")
        plt.scatter(d["target_date"], d["actual"], s=dot_size, color=colors["actual"], label="actual")
    else:
        plt.plot(d["target_date"], d["pred"], color=colors["pred"], label=f"nowcast (moq={moq})")
        plt.plot(d["target_date"], d["actual"], color=colors["actual"], label="actual")

    plt.xlabel("Quarter (target_date)")
    plt.ylabel("Value")
    plt.title(title or f"{horizon.upper()} nowcast vs actual (moq={moq})")
    plt.legend()
    plt.tight_layout()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outpath, dpi=150)
    plt.close()