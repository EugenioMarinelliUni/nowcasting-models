from __future__ import annotations

from pathlib import Path
import re

import numpy as np
import pandas as pd


def extract_feature_importance(model: object, feature_columns: list[str]) -> pd.DataFrame:
    estimator = getattr(model, "estimator", model)

    if not hasattr(estimator, "feature_importances_"):
        return pd.DataFrame(columns=["feature", "importance"])

    imp = np.asarray(getattr(estimator, "feature_importances_"), dtype=float)
    if len(imp) != len(feature_columns):
        return pd.DataFrame(columns=["feature", "importance"])

    return (
        pd.DataFrame({"feature": feature_columns, "importance": imp})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )


def predictor_from_feature(feature: str) -> str:
    f = str(feature)
    for suffix in ("_last", "_ma3", "_chg3"):
        if f.endswith(suffix):
            return f[: -len(suffix)]
    f = re.sub(r"_lag\d+$", "", f)
    f = re.sub(r"__xlag\d+$", "", f)
    return f


def aggregate_importance_to_predictor(importance_df: pd.DataFrame) -> pd.DataFrame:
    if importance_df.empty:
        return pd.DataFrame(columns=["predictor", "importance"])

    rows = []
    for _, row in importance_df.iterrows():
        rows.append(
            {
                "predictor": predictor_from_feature(str(row["feature"])),
                "importance": float(row["importance"]),
            }
        )

    return (
        pd.DataFrame(rows)
        .groupby("predictor", as_index=False)["importance"]
        .sum()
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )


def write_importance_outputs(
    feature_importance: pd.DataFrame,
    outdir: str | Path,
    *,
    prefix: str = "feature_importance",
) -> tuple[Path, Path]:
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    feature_path = outdir / f"{prefix}.csv"
    predictor_path = outdir / f"{prefix}_by_predictor.csv"

    feature_importance.to_csv(feature_path, index=False)
    aggregate_importance_to_predictor(feature_importance).to_csv(predictor_path, index=False)

    return feature_path, predictor_path
