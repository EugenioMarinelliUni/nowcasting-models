from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def load_delay_map(path: str | Path | None) -> dict[str, int] | None:
    if path is None:
        return None

    p = Path(path)
    with open(p, "r", encoding="utf-8") as f:
        obj = json.load(f)

    if not isinstance(obj, dict):
        raise ValueError(f"Delay map at {p} must be a JSON object")

    out: dict[str, int] = {}
    for key, val in obj.items():
        if isinstance(val, dict):
            if "delay" not in val:
                raise ValueError(f"Delay map entry for {key!r} must contain a 'delay' field")
            out[str(key)] = int(val["delay"])
        else:
            out[str(key)] = int(val)

    return out


def load_predictor_list(
    path: str | Path,
    *,
    column_name: str | None = None,
) -> list[str]:
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix == ".json":
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)

        if isinstance(obj, list):
            return [str(x) for x in obj]

        if isinstance(obj, dict):
            for key in ("predictors", "selected_vars", "variables", "features"):
                val = obj.get(key)
                if isinstance(val, list):
                    return [str(x) for x in val]

        raise ValueError(f"Unsupported JSON predictor-list structure in {p}")

    if suffix in {".txt", ".lst"}:
        lines = p.read_text(encoding="utf-8").splitlines()
        return [line.strip() for line in lines if line.strip()]

    df = pd.read_csv(p)

    if df.empty:
        return []

    if column_name is not None:
        if column_name not in df.columns:
            raise ValueError(f"Column {column_name!r} not found in {p}")
        series = df[column_name]
        return [str(x) for x in series.dropna().tolist()]

    for candidate in ("variable", "predictor", "series", "feature", "name"):
        if candidate in df.columns:
            return [str(x) for x in df[candidate].dropna().tolist()]

    return [str(x) for x in df.iloc[:, 0].dropna().tolist()]


def select_predictors(
    X_columns: list[str],
    *,
    predictors: list[str] | None,
    predictors_path: str | Path | None,
    predictors_col: str | None,
    n_predictors: int,
) -> list[str]:
    if predictors is not None:
        chosen = predictors
    elif predictors_path is not None:
        chosen = load_predictor_list(
            predictors_path,
            column_name=predictors_col,
        )
    else:
        if n_predictors <= 0:
            raise ValueError("n_predictors must be >= 1 when no explicit predictor list is provided")
        chosen = list(X_columns[:n_predictors])

    if n_predictors > 0:
        chosen = chosen[:n_predictors]

    missing = [c for c in chosen if c not in X_columns]
    if missing:
        raise ValueError(f"Requested predictors not found in X: {missing}")

    return chosen