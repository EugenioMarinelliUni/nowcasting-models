from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


REVIEW_COLUMNS = [
    "raw_series",
    "canonical_id",
    "relationship_type",
    "equivalence_level",
    "predecessor",
    "successor",
    "mapping_action",
    "include_rt_stable",
    "include_rt_canonical",
    "documented_change_vintage",
    "documented_change_type",
    "fred_md_change_log_reference",
    "fred_series_url",
    "source_agency_reference",
    "review_status",
    "notes",
]

ALLOWED_RELATIONSHIP_TYPES = {
    "unreviewed",
    "unchanged",
    "exact_rename",
    "official_successor",
    "related_non_equivalent",
    "new_unmatched",
    "retired_unmatched",
    "excluded",
}

ALLOWED_MAPPING_ACTIONS = {
    "unreviewed",
    "direct",
    "switch_by_vintage",
    "keep_separate",
    "exclude",
}

ALLOWED_REVIEW_STATUS = {
    "unreviewed",
    "in_review",
    "reviewed",
}

BOOLEAN_COLUMNS = {
    "include_rt_stable",
    "include_rt_canonical",
}


def _require_columns(
    df: pd.DataFrame,
    required: Iterable[str],
    *,
    name: str,
) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(
            f"{name} is missing required columns: {', '.join(missing)}"
        )


def _parse_optional_bool(value: object) -> bool | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip().lower()
    if text == "":
        return None
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False

    raise ValueError(f"Invalid boolean review value: {value!r}")


def build_review_template(
    seed_registry: pd.DataFrame,
    *,
    extra_series: Iterable[str] = (),
) -> pd.DataFrame:
    _require_columns(
        seed_registry,
        [
            "raw_series",
            "needs_stable_window_transition_review",
        ],
        name="seed_registry",
    )

    extras = {str(x).strip() for x in extra_series if str(x).strip()}
    known = set(seed_registry["raw_series"].astype(str))
    unknown = sorted(extras - known)
    if unknown:
        raise ValueError(
            f"Unknown extra series requested for review: {unknown}"
        )

    mask = seed_registry[
        "needs_stable_window_transition_review"
    ].astype(bool) | seed_registry["raw_series"].astype(str).isin(extras)

    selected = (
        seed_registry.loc[mask, "raw_series"]
        .astype(str)
        .sort_values()
        .tolist()
    )

    out = pd.DataFrame("", index=range(len(selected)), columns=REVIEW_COLUMNS)
    out["raw_series"] = selected
    out["review_status"] = "unreviewed"
    return out


def read_reviews_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
    )
    _require_columns(df, REVIEW_COLUMNS, name="reviews")
    return df[REVIEW_COLUMNS].copy()


def validate_reviews(
    seed_registry: pd.DataFrame,
    reviews: pd.DataFrame,
) -> None:
    _require_columns(seed_registry, ["raw_series"], name="seed_registry")
    _require_columns(reviews, REVIEW_COLUMNS, name="reviews")

    if reviews["raw_series"].duplicated().any():
        dup = sorted(
            reviews.loc[
                reviews["raw_series"].duplicated(keep=False),
                "raw_series",
            ].unique()
        )
        raise ValueError(f"Duplicate review rows for raw_series: {dup}")

    seed_series = set(seed_registry["raw_series"].astype(str))
    review_series = set(reviews["raw_series"].astype(str))
    unknown = sorted(review_series - seed_series)
    if unknown:
        raise ValueError(f"Reviews contain unknown raw_series: {unknown}")

    for _, row in reviews.iterrows():
        raw_series = row["raw_series"]

        relationship = row["relationship_type"].strip()
        if relationship and relationship not in ALLOWED_RELATIONSHIP_TYPES:
            raise ValueError(
                f"{raw_series}: invalid relationship_type={relationship!r}"
            )

        action = row["mapping_action"].strip()
        if action and action not in ALLOWED_MAPPING_ACTIONS:
            raise ValueError(
                f"{raw_series}: invalid mapping_action={action!r}"
            )

        status = row["review_status"].strip()
        if status and status not in ALLOWED_REVIEW_STATUS:
            raise ValueError(
                f"{raw_series}: invalid review_status={status!r}"
            )

        for col in BOOLEAN_COLUMNS:
            _parse_optional_bool(row[col])

        predecessor = row["predecessor"].strip()
        successor = row["successor"].strip()

        if predecessor and predecessor not in seed_series:
            raise ValueError(
                f"{raw_series}: unknown predecessor={predecessor!r}"
            )
        if successor and successor not in seed_series:
            raise ValueError(
                f"{raw_series}: unknown successor={successor!r}"
            )

        if predecessor == raw_series or successor == raw_series:
            raise ValueError(
                f"{raw_series}: predecessor/successor cannot equal itself."
            )

        if status == "reviewed":
            if not relationship:
                raise ValueError(
                    f"{raw_series}: reviewed row requires relationship_type."
                )
            if not action:
                raise ValueError(
                    f"{raw_series}: reviewed row requires mapping_action."
                )


def apply_review_overlay(
    seed_registry: pd.DataFrame,
    reviews: pd.DataFrame,
) -> pd.DataFrame:
    validate_reviews(seed_registry, reviews)

    out = seed_registry.copy()
    out["raw_series"] = out["raw_series"].astype(str)

    # CSV columns that are entirely empty can be inferred by pandas as
    # float64 (all-NaN). Review overlays subsequently assign strings to
    # these fields, which currently raises a FutureWarning and will become
    # an error in a future pandas release. Cast only review text fields to
    # object; preserve Boolean and mechanical/numeric registry columns.
    for col in REVIEW_COLUMNS:
        if col == "raw_series" or col in BOOLEAN_COLUMNS:
            continue
        if col in out.columns:
            out[col] = out[col].astype("object")

    out = out.set_index("raw_series", drop=False)

    for _, review in reviews.iterrows():
        raw_series = review["raw_series"]

        for col in REVIEW_COLUMNS:
            if col == "raw_series":
                continue

            value = review[col]
            if value is None or pd.isna(value):
                continue

            text = str(value).strip()
            if text == "":
                continue

            if col in BOOLEAN_COLUMNS:
                parsed = _parse_optional_bool(value)
                if parsed is not None:
                    out.at[raw_series, col] = parsed
            else:
                out.at[raw_series, col] = text

    return out.reset_index(drop=True)


def build_review_summary(
    registry: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(
        registry,
        [
            "raw_series",
            "review_status",
            "relationship_type",
            "include_rt_stable",
            "include_rt_canonical",
        ],
        name="registry",
    )

    return pd.DataFrame(
        [
            {
                "n_raw_series": int(len(registry)),
                "n_reviewed": int(
                    (registry["review_status"] == "reviewed").sum()
                ),
                "n_in_review": int(
                    (registry["review_status"] == "in_review").sum()
                ),
                "n_unreviewed": int(
                    (registry["review_status"] == "unreviewed").sum()
                ),
                "n_include_rt_stable": int(
                    registry["include_rt_stable"].astype(bool).sum()
                ),
                "n_include_rt_canonical": int(
                    registry["include_rt_canonical"].astype(bool).sum()
                ),
            }
        ]
    )


__all__ = [
    "REVIEW_COLUMNS",
    "ALLOWED_RELATIONSHIP_TYPES",
    "ALLOWED_MAPPING_ACTIONS",
    "ALLOWED_REVIEW_STATUS",
    "build_review_template",
    "read_reviews_csv",
    "validate_reviews",
    "apply_review_overlay",
    "build_review_summary",
]
