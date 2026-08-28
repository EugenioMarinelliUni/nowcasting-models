from __future__ import annotations

from typing import Literal

import pandas as pd

from dfm_pipeline.preprocessing.tcode import ALLOWED_TCODES

SelectionScope = Literal[
    "ordinary",
    "all-stable",
    "rt-stable-candidates",
]

TRUE_VALUES = {"true", "1", "yes", "y"}
FALSE_VALUES = {"false", "0", "no", "n", ""}

REQUIRED_SELECTION_COLUMNS = [
    "raw_series",
    "candidate_rt_stable",
    "review_status",
    "include_rt_stable",
    "include_rt_canonical",
    "present_all_stable_window",
    "tcodes_seen",
    "n_distinct_tcodes",
    "n_tcode_changes",
]


def require_selection_columns(registry: pd.DataFrame) -> None:
    """Raise if the curated registry lacks fields required for panel selection."""
    missing = sorted(set(REQUIRED_SELECTION_COLUMNS) - set(registry.columns))
    if missing:
        raise ValueError(
            "Registry is missing required panel-selection columns: "
            + ", ".join(missing)
        )


def parse_registry_bool(
    value: object,
    *,
    field: str,
    raw_series: str,
) -> bool:
    """Parse one registry Boolean without relying on pandas dtype inference."""
    text = str(value).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    raise ValueError(
        f"{raw_series}: cannot parse {field}={value!r} as Boolean."
    )


def registry_bool_series(
    df: pd.DataFrame,
    field: str,
) -> pd.Series:
    """Parse a registry Boolean column row by row with useful error messages."""
    values: list[bool] = []
    for _, row in df.iterrows():
        values.append(
            parse_registry_bool(
                row[field],
                field=field,
                raw_series=str(row["raw_series"]),
            )
        )
    return pd.Series(values, index=df.index, dtype=bool)


def single_stable_tcode(
    tcodes_seen: object,
    n_distinct_tcodes: object,
) -> int | None:
    """Return the unique valid FRED-MD t-code recorded for a stable series."""
    try:
        n_distinct = int(float(n_distinct_tcodes))
    except (TypeError, ValueError):
        return None

    if n_distinct != 1:
        return None

    text = str(tcodes_seen).strip()
    if not text:
        return None

    parts = [part.strip() for part in text.split("|") if part.strip()]
    if len(parts) != 1:
        return None

    try:
        code = int(parts[0])
    except ValueError:
        return None

    return code if code in ALLOWED_TCODES else None


def normalize_registry_selection_fields(
    registry: pd.DataFrame,
) -> pd.DataFrame:
    """
    Return a copy with deterministic selection dtypes and derived t-code.

    Empty include flags on unreviewed rows remain ordinary False seed values;
    their interpretation is controlled by :func:`select_panel_candidates`.
    """
    require_selection_columns(registry)
    x = registry.copy()
    x["raw_series"] = x["raw_series"].astype(str)
    x["review_status"] = (
        x["review_status"].astype(str).str.strip().str.lower()
    )

    for field in (
        "candidate_rt_stable",
        "include_rt_stable",
        "include_rt_canonical",
        "present_all_stable_window",
    ):
        x[field] = registry_bool_series(x, field)

    x["tcode"] = [
        single_stable_tcode(
            row["tcodes_seen"],
            row["n_distinct_tcodes"],
        )
        for _, row in x.iterrows()
    ]
    return x


def select_panel_candidates(
    registry: pd.DataFrame,
    *,
    scope: SelectionScope,
) -> pd.DataFrame:
    """
    Select a deterministic FRED-MD candidate universe from the curated registry.

    Scopes
    ------
    ordinary
        Mechanically stable candidates whose review status is still
        ``unreviewed``. This is the Stage-1 ordinary universe.

    all-stable
        Every mechanically stable candidate, including manually reviewed
        special cases. This is useful for diagnostics.

    rt-stable-candidates
        Provisional model-facing stable universe: all ordinary unreviewed
        mechanically stable candidates plus reviewed stable candidates that
        were explicitly retained with ``include_rt_stable=True``. Reviewed
        exclusions are not selected.
    """
    x = normalize_registry_selection_fields(registry)

    candidate = x["candidate_rt_stable"]
    reviewed = x["review_status"].eq("reviewed")
    unreviewed = x["review_status"].eq("unreviewed")

    if scope == "ordinary":
        mask = candidate & unreviewed
    elif scope == "all-stable":
        mask = candidate
    elif scope == "rt-stable-candidates":
        mask = candidate & (
            unreviewed | (reviewed & x["include_rt_stable"])
        )
    else:  # pragma: no cover - Literal protects normal callers.
        raise ValueError(f"Unknown scope: {scope!r}")

    out = x.loc[mask].copy()
    if out.empty:
        raise ValueError(f"No rows selected for scope={scope!r}.")

    out["selection_class"] = ""
    out.loc[
        out["review_status"].eq("unreviewed"),
        "selection_class",
    ] = "ordinary_stable"
    out.loc[
        out["review_status"].eq("reviewed")
        & out["include_rt_stable"],
        "selection_class",
    ] = "reviewed_stable_keep"
    out.loc[
        out["review_status"].eq("reviewed")
        & ~out["include_rt_stable"],
        "selection_class",
    ] = "reviewed_stable_exclude"

    return out.sort_values("raw_series").reset_index(drop=True)


__all__ = [
    "SelectionScope",
    "TRUE_VALUES",
    "FALSE_VALUES",
    "REQUIRED_SELECTION_COLUMNS",
    "require_selection_columns",
    "parse_registry_bool",
    "registry_bool_series",
    "single_stable_tcode",
    "normalize_registry_selection_fields",
    "select_panel_candidates",
]
