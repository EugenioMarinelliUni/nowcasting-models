from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

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
    # If duplicates exist, keep last by eval_date
    df = df.sort_values(["target_date", "moq", "eval_date"])
    return df.groupby(["target_date", "moq"], as_index=False).tail(1)


def plot_nowcasts_moq123_vs_actual(
    df: pd.DataFrame,
    outpath: Path,
    horizon: str = "now",
    title: Optional[str] = None,
) -> None:
    d = df[df["horizon"] == horizon].copy()
    if d.empty:
        raise ValueError(f"no rows for horizon={horizon}")

    last = last_pred_per_target_moq(d)
    wide = last.pivot(index="target_date", columns="moq", values="pred").sort_index()
    actual = last.groupby("target_date")["actual"].last().sort_index()

    plt.figure()
    for m in (1, 2, 3):
        if m in wide.columns:
            plt.plot(wide.index, wide[m].to_numpy(), label=f"nowcast moq={m}")
    plt.plot(actual.index, actual.to_numpy(), label="actual")

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
) -> None:
    if moq not in (1, 2, 3):
        raise ValueError("moq must be 1, 2, or 3")

    d = df[(df["horizon"] == horizon) & (df["moq"] == moq)].copy()
    if d.empty:
        raise ValueError(f"no rows for horizon={horizon} moq={moq}")

    d = d.sort_values(["target_date", "eval_date"]).groupby("target_date", as_index=False).tail(1)
    d = d.sort_values("target_date")

    plt.figure()
    plt.plot(d["target_date"], d["pred"], label=f"nowcast (moq={moq})")
    plt.plot(d["target_date"], d["actual"], label="actual")
    plt.xlabel("Quarter (target_date)")
    plt.ylabel("Value")
    plt.title(title or f"{horizon.upper()} nowcast vs actual (moq={moq})")
    plt.legend()
    plt.tight_layout()
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outpath, dpi=150)
    plt.close()
