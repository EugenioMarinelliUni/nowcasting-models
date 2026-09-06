from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from dfm_pipeline.ingestion.fred_md_registry_reviews import (
    read_reviews_csv,
)
from dfm_pipeline.ingestion.fred_md_rt_canonical_review import (
    load_transition_review_spec,
)


DOCUMENTARY_COLUMNS = [
    "canonical_id",
    "transition_vintage",
    "old_source",
    "new_source",
    "aux_reference_series",
    "relationship_type",
    "equivalence_level",
    "old_title",
    "new_title",
    "old_source_agency",
    "new_source_agency",
    "old_frequency",
    "new_frequency",
    "old_units",
    "new_units",
    "old_seasonal_adjustment",
    "new_seasonal_adjustment",
    "official_fred_md_replacement",
    "source_agency_change",
    "methodology_change",
    "official_successor_relation",
    "construction_or_splice_note",
    "documentary_assessment",
    "methodology_reference",
    "review_status",
    "notes",
]

BOOLEAN_DOCUMENTARY_COLUMNS = {
    "official_fred_md_replacement",
    "source_agency_change",
    "methodology_change",
}

ALLOWED_DOCUMENTARY_ASSESSMENTS = {
    "official_non_exact_successor",
    "official_fred_md_splice",
    "needs_further_documentary_review",
}

ALLOWED_REVIEW_STATUS = {
    "in_review",
    "reviewed",
}

_MONTH_PATTERN = re.compile(
    r"^\d{4}-(0[1-9]|1[0-2])$"
)

C3_REQUIRED_COLUMNS = [
    "canonical_id",
    "transition_vintage",
    "old_source",
    "new_source",
    "relationship_type",
    "equivalence_level",
    "n_common_valid",
    "first_common_date",
    "last_common_date",
    "pearson_corr",
    "spearman_corr",
    "std_ratio_new_old",
    "nrmse_old_std",
    "sign_agreement",
    "ols_beta",
    "ols_r2",
    "diagnostic_status",
    "diagnostic_flags",
]

C4_FULL_METRICS = [
    "n_common_valid",
    "first_common_date",
    "last_common_date",
    "pearson_corr",
    "spearman_corr",
    "std_ratio_new_old",
    "nrmse_old_std",
    "sign_agreement",
    "ols_beta",
    "ols_r2",
    "diagnostic_status",
]

C4_ROLLING_METRICS = [
    "window_start_date",
    "window_end_date",
    "n_common_valid",
    "pearson_corr",
    "std_ratio_new_old",
    "nrmse_old_std",
    "diagnostic_status",
]


def _require_columns(
    frame: pd.DataFrame,
    columns: Iterable[str],
    *,
    name: str,
) -> None:
    missing = [
        column
        for column in columns
        if column not in frame.columns
    ]

    if missing:
        raise ValueError(
            f"{name} is missing required columns: "
            + ", ".join(missing)
        )


def _clean_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _parse_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value

    text = _clean_text(value).lower()

    if text in {
        "true",
        "1",
        "yes",
        "y",
    }:
        return True

    if text in {
        "false",
        "0",
        "no",
        "n",
    }:
        return False

    raise ValueError(
        f"Invalid boolean value: {value!r}"
    )


def _month_timestamp(
    value: object,
) -> pd.Timestamp:
    text = _clean_text(value)

    if not _MONTH_PATTERN.fullmatch(text):
        raise ValueError(
            f"Invalid transition vintage: {value!r}. "
            "Expected YYYY-MM."
        )

    return pd.Timestamp(
        f"{text}-01"
    )


def _months_between(
    transition_date: pd.Timestamp,
    evidence_date: pd.Timestamp,
) -> int:
    return (
        (
            transition_date.year
            - evidence_date.year
        )
        * 12
        + transition_date.month
        - evidence_date.month
    )


def validate_documentary_review(
    documentary: pd.DataFrame,
    *,
    c3: pd.DataFrame,
    review_spec: pd.DataFrame,
    series_reviews: pd.DataFrame,
    require_complete: bool = True,
) -> pd.DataFrame:
    """
    Validate the transition-level documentary review.

    The documentary file is not a second source registry. It must agree
    with:

      * the C3 transition identities;
      * the C4 review specification;
      * the existing source-level fred_md_series_reviews.csv registry.

    No quantitative acceptance threshold or final RT_CANONICAL decision
    is applied here.
    """
    _require_columns(
        documentary,
        DOCUMENTARY_COLUMNS,
        name="documentary review",
    )

    _require_columns(
        c3,
        [
            "canonical_id",
            "transition_vintage",
            "old_source",
            "new_source",
            "relationship_type",
            "equivalence_level",
        ],
        name="C3 comparability report",
    )

    _require_columns(
        review_spec,
        [
            "canonical_id",
            "old_reference_series",
            "new_reference_series",
            "aux_reference_series",
            "canonical_tcode",
            "review_mode",
        ],
        name="C4 review specification",
    )

    _require_columns(
        series_reviews,
        [
            "raw_series",
            "canonical_id",
            "relationship_type",
            "equivalence_level",
            "predecessor",
            "successor",
            "include_rt_canonical",
            "documented_change_vintage",
            "fred_md_change_log_reference",
            "fred_series_url",
            "source_agency_reference",
            "review_status",
        ],
        name="series reviews",
    )

    out = documentary.loc[
        :,
        DOCUMENTARY_COLUMNS,
    ].copy()

    text_columns = [
        column
        for column in DOCUMENTARY_COLUMNS
        if column
        not in BOOLEAN_DOCUMENTARY_COLUMNS
    ]

    for column in text_columns:
        out[column] = out[column].map(
            _clean_text
        )

    for column in BOOLEAN_DOCUMENTARY_COLUMNS:
        out[column] = out[column].map(
            _parse_bool
        )

    if out.empty:
        raise ValueError(
            "Documentary review must contain at least one row"
        )

    if out["canonical_id"].eq("").any():
        raise ValueError(
            "Documentary review contains blank canonical_id"
        )

    duplicated = (
        out["canonical_id"]
        .duplicated(
            keep=False
        )
    )

    if duplicated.any():
        values = sorted(
            out.loc[
                duplicated,
                "canonical_id",
            ]
            .unique()
            .tolist()
        )

        raise ValueError(
            "Duplicate documentary rows for canonical_id: "
            f"{values}"
        )

    for value in out[
        "transition_vintage"
    ]:
        _month_timestamp(
            value
        )

    invalid_assessments = sorted(
        set(
            out[
                "documentary_assessment"
            ]
        )
        - ALLOWED_DOCUMENTARY_ASSESSMENTS
    )

    if invalid_assessments:
        raise ValueError(
            "Unsupported documentary_assessment values: "
            f"{invalid_assessments}"
        )

    invalid_status = sorted(
        set(
            out[
                "review_status"
            ]
        )
        - ALLOWED_REVIEW_STATUS
    )

    if invalid_status:
        raise ValueError(
            "Unsupported documentary review_status values: "
            f"{invalid_status}"
        )

    if c3[
        "canonical_id"
    ].duplicated().any():
        raise ValueError(
            "C3 comparability report must contain one row "
            "per canonical_id"
        )

    expected_ids = set(
        c3[
            "canonical_id"
        ].astype(str)
    )

    actual_ids = set(
        out[
            "canonical_id"
        ]
    )

    missing = sorted(
        expected_ids
        - actual_ids
    )

    extra = sorted(
        actual_ids
        - expected_ids
    )

    if missing:
        raise ValueError(
            "Documentary review is missing canonical concepts: "
            f"{missing}"
        )

    if extra:
        raise ValueError(
            "Documentary review contains unexpected concepts: "
            f"{extra}"
        )

    c3_by_id = c3.set_index(
        "canonical_id"
    )

    for _, row in out.iterrows():
        canonical_id = row[
            "canonical_id"
        ]

        c3_row = c3_by_id.loc[
            canonical_id
        ]

        comparisons = {
            "transition_vintage": (
                row[
                    "transition_vintage"
                ],
                _clean_text(
                    c3_row[
                        "transition_vintage"
                    ]
                ),
            ),
            "old_source": (
                row[
                    "old_source"
                ],
                _clean_text(
                    c3_row[
                        "old_source"
                    ]
                ),
            ),
            "new_source": (
                row[
                    "new_source"
                ],
                _clean_text(
                    c3_row[
                        "new_source"
                    ]
                ),
            ),
            "relationship_type": (
                row[
                    "relationship_type"
                ],
                _clean_text(
                    c3_row[
                        "relationship_type"
                    ]
                ),
            ),
            "equivalence_level": (
                row[
                    "equivalence_level"
                ],
                _clean_text(
                    c3_row[
                        "equivalence_level"
                    ]
                ),
            ),
        }

        for field, (
            documentary_value,
            c3_value,
        ) in comparisons.items():
            if documentary_value != c3_value:
                raise ValueError(
                    f"{canonical_id}: documentary {field} "
                    f"does not match C3: "
                    f"{documentary_value!r} "
                    f"!= {c3_value!r}"
                )

    if review_spec[
        "canonical_id"
    ].duplicated().any():
        raise ValueError(
            "C4 review specification must contain one row "
            "per canonical_id"
        )

    spec_by_id = (
        review_spec
        .set_index(
            "canonical_id"
        )
    )

    if set(
        spec_by_id.index
    ) != expected_ids:
        raise ValueError(
            "C4 review specification canonical_id set "
            "does not match C3"
        )

    for _, row in out.iterrows():
        canonical_id = row[
            "canonical_id"
        ]

        spec_row = spec_by_id.loc[
            canonical_id
        ]

        expected_aux = _clean_text(
            spec_row[
                "aux_reference_series"
            ]
        )

        if (
            row[
                "aux_reference_series"
            ]
            != expected_aux
        ):
            raise ValueError(
                f"{canonical_id}: documentary "
                "aux_reference_series does not match "
                "C4 review specification: "
                f"{row['aux_reference_series']!r} "
                f"!= {expected_aux!r}"
            )

    if series_reviews[
        "raw_series"
    ].duplicated().any():
        raise ValueError(
            "series_reviews contains duplicate raw_series rows"
        )

    reviews_by_series = (
        series_reviews
        .set_index(
            "raw_series"
        )
    )

    for _, row in out.iterrows():
        canonical_id = row[
            "canonical_id"
        ]

        old_source = row[
            "old_source"
        ]

        new_source = row[
            "new_source"
        ]

        for series_id in (
            old_source,
            new_source,
        ):
            if (
                series_id
                not in reviews_by_series.index
            ):
                raise ValueError(
                    f"{canonical_id}: source "
                    f"{series_id!r} is absent from "
                    "fred_md_series_reviews.csv"
                )

            review = (
                reviews_by_series.loc[
                    series_id
                ]
            )

            if (
                _clean_text(
                    review[
                        "canonical_id"
                    ]
                )
                != canonical_id
            ):
                raise ValueError(
                    f"{canonical_id}: registry canonical_id "
                    f"mismatch for {series_id!r}"
                )

            if (
                _clean_text(
                    review[
                        "review_status"
                    ]
                )
                != "reviewed"
            ):
                raise ValueError(
                    f"{canonical_id}: source "
                    f"{series_id!r} has not completed "
                    "registry review"
                )

            if not _parse_bool(
                review[
                    "include_rt_canonical"
                ]
            ):
                raise ValueError(
                    f"{canonical_id}: source "
                    f"{series_id!r} is not marked "
                    "include_rt_canonical"
                )

        old_review = (
            reviews_by_series.loc[
                old_source
            ]
        )

        new_review = (
            reviews_by_series.loc[
                new_source
            ]
        )

        if (
            _clean_text(
                old_review[
                    "successor"
                ]
            )
            != new_source
        ):
            raise ValueError(
                f"{canonical_id}: registry successor for "
                f"{old_source!r} is not {new_source!r}"
            )

        if (
            _clean_text(
                new_review[
                    "predecessor"
                ]
            )
            != old_source
        ):
            raise ValueError(
                f"{canonical_id}: registry predecessor for "
                f"{new_source!r} is not {old_source!r}"
            )

        if (
            _clean_text(
                old_review[
                    "documented_change_vintage"
                ]
            )
            != row[
                "transition_vintage"
            ]
        ):
            raise ValueError(
                f"{canonical_id}: registry/documentary "
                "transition vintage mismatch"
            )

        if (
            _clean_text(
                old_review[
                    "relationship_type"
                ]
            )
            != row[
                "relationship_type"
            ]
        ):
            raise ValueError(
                f"{canonical_id}: registry/documentary "
                "relationship_type mismatch"
            )

        if (
            _clean_text(
                old_review[
                    "equivalence_level"
                ]
            )
            != row[
                "equivalence_level"
            ]
        ):
            raise ValueError(
                f"{canonical_id}: registry/documentary "
                "equivalence_level mismatch"
            )

    if require_complete:
        incomplete = out.loc[
            out[
                "review_status"
            ].ne(
                "reviewed"
            ),
            "canonical_id",
        ].tolist()

        if incomplete:
            raise ValueError(
                "Documentary review is incomplete for: "
                f"{incomplete}"
            )

        required_text = [
            "official_successor_relation",
            "documentary_assessment",
            "methodology_reference",
            "notes",
        ]

        for column in required_text:
            blank = out[
                column
            ].eq("")

            if blank.any():
                bad = out.loc[
                    blank,
                    "canonical_id",
                ].tolist()

                raise ValueError(
                    "Completed documentary review requires "
                    f"{column} for: {bad}"
                )

    return out.reset_index(
        drop=True
    )


def load_documentary_review(
    path: str | Path,
    *,
    c3: pd.DataFrame,
    review_spec: pd.DataFrame,
    series_reviews: pd.DataFrame,
    require_complete: bool = True,
) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
    )

    return validate_documentary_review(
        frame,
        c3=c3,
        review_spec=review_spec,
        series_reviews=series_reviews,
        require_complete=require_complete,
    )


def _prefix_metrics(
    frame: pd.DataFrame,
    *,
    pair_role: str,
    metrics: list[str],
    prefix: str,
) -> pd.DataFrame:
    _require_columns(
        frame,
        [
            "canonical_id",
            "pair_role",
            *metrics,
        ],
        name=f"C4 {prefix} input",
    )

    part = frame.loc[
        frame[
            "pair_role"
        ].eq(
            pair_role
        ),
        [
            "canonical_id",
            *metrics,
        ],
    ].copy()

    if part[
        "canonical_id"
    ].duplicated().any():
        raise ValueError(
            f"C4 pair_role={pair_role!r} contains "
            "duplicate canonical_id rows"
        )

    return part.rename(
        columns={
            metric: (
                f"{prefix}_{metric}"
            )
            for metric in metrics
        }
    )


def select_near_transition_diagnostics(
    rolling: pd.DataFrame,
    *,
    c3: pd.DataFrame,
    pair_role: str,
    prefix: str,
) -> pd.DataFrame:
    """
    Select the final available rolling diagnostic window whose end date
    precedes the actual C3 transition vintage.

    Optional pair roles, such as old_vs_aux and aux_vs_new, may not exist
    for ordinary pairwise concepts. In that case this function returns an
    empty DataFrame with a stable, mergeable schema rather than a
    schema-less DataFrame.
    """
    _require_columns(
        rolling,
        [
            "canonical_id",
            "pair_role",
            *C4_ROLLING_METRICS,
        ],
        name="C4 rolling report",
    )

    _require_columns(
        c3,
        [
            "canonical_id",
            "transition_vintage",
        ],
        name="C3 comparability report",
    )

    work = rolling.loc[
        rolling[
            "pair_role"
        ].eq(
            pair_role
        )
    ].copy()

    work[
        "window_start_date"
    ] = pd.to_datetime(
        work[
            "window_start_date"
        ],
        errors="coerce",
    )

    work[
        "window_end_date"
    ] = pd.to_datetime(
        work[
            "window_end_date"
        ],
        errors="coerce",
    )

    if (
        work[
            "window_start_date"
        ].isna().any()
        or work[
            "window_end_date"
        ].isna().any()
    ):
        raise ValueError(
            "C4 rolling report contains invalid dates"
        )

    rows: list[
        dict[str, object]
    ] = []

    for _, transition in (
        c3.iterrows()
    ):
        canonical_id = str(
            transition[
                "canonical_id"
            ]
        )

        transition_date = (
            _month_timestamp(
                transition[
                    "transition_vintage"
                ]
            )
        )

        candidates = work.loc[
            work[
                "canonical_id"
            ].eq(
                canonical_id
            )
            & work[
                "window_end_date"
            ].lt(
                transition_date
            )
        ]

        if candidates.empty:
            continue

        row = (
            candidates
            .sort_values(
                "window_end_date"
            )
            .iloc[-1]
        )

        window_end = pd.Timestamp(
            row[
                "window_end_date"
            ]
        )

        result: dict[
            str,
            object,
        ] = {
            "canonical_id": (
                canonical_id
            ),
            (
                f"{prefix}_"
                "months_to_transition"
            ): _months_between(
                transition_date,
                window_end,
            ),
        }

        for metric in (
            C4_ROLLING_METRICS
        ):
            value = row[
                metric
            ]

            if metric in {
                "window_start_date",
                "window_end_date",
            }:
                value = (
                    pd.Timestamp(
                        value
                    )
                    .date()
                    .isoformat()
                )

            result[
                f"{prefix}_{metric}"
            ] = value

        rows.append(
            result
        )

    # IMPORTANT:
    # Even if no rows are available for an optional pair role, preserve
    # the complete output schema. This allows a left merge on canonical_id
    # without raising KeyError and leaves the unavailable evidence fields
    # as NaN for non-triangulation concepts.
    output_columns = [
        "canonical_id",
        (
            f"{prefix}_"
            "months_to_transition"
        ),
        *[
            f"{prefix}_{metric}"
            for metric
            in C4_ROLLING_METRICS
        ],
    ]

    return pd.DataFrame(
        rows,
        columns=output_columns,
    )


def _build_registry_provenance(
    documentary: pd.DataFrame,
    series_reviews: pd.DataFrame,
) -> pd.DataFrame:
    reviews = (
        series_reviews
        .set_index(
            "raw_series"
        )
    )

    rows: list[
        dict[str, object]
    ] = []

    for _, documentary_row in (
        documentary.iterrows()
    ):
        canonical_id = (
            documentary_row[
                "canonical_id"
            ]
        )

        old_source = (
            documentary_row[
                "old_source"
            ]
        )

        new_source = (
            documentary_row[
                "new_source"
            ]
        )

        old = reviews.loc[
            old_source
        ]

        new = reviews.loc[
            new_source
        ]

        rows.append(
            {
                "canonical_id": (
                    canonical_id
                ),
                "fred_md_change_log_reference": (
                    old[
                        "fred_md_change_log_reference"
                    ]
                ),
                "registry_old_fred_series_url": (
                    old[
                        "fred_series_url"
                    ]
                ),
                "registry_new_fred_series_url": (
                    new[
                        "fred_series_url"
                    ]
                ),
                (
                    "registry_old_"
                    "source_agency_reference"
                ): (
                    old[
                        "source_agency_reference"
                    ]
                ),
                (
                    "registry_new_"
                    "source_agency_reference"
                ): (
                    new[
                        "source_agency_reference"
                    ]
                ),
            }
        )

    return pd.DataFrame(
        rows
    )


def build_rt_canonical_evidence(
    *,
    documentary: pd.DataFrame,
    c3: pd.DataFrame,
    c4_summary: pd.DataFrame,
    c4_rolling: pd.DataFrame,
    review_spec: pd.DataFrame,
    series_reviews: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build one evidence row per RT_CANONICAL source-transition concept.

    Combines:

      * transition-level documentary evidence;
      * C3 adjacent-vintage diagnostics;
      * C4 full-overlap external-reference diagnostics;
      * C4 final rolling diagnostic available before the transition;
      * monetary-base auxiliary triangulation when available;
      * existing source-registry provenance.

    This function deliberately creates no acceptance/rejection decision.
    """
    documentary = (
        validate_documentary_review(
            documentary,
            c3=c3,
            review_spec=review_spec,
            series_reviews=series_reviews,
            require_complete=True,
        )
    )

    _require_columns(
        c3,
        C3_REQUIRED_COLUMNS,
        name="C3 comparability report",
    )

    c3_part = c3.loc[
        :,
        C3_REQUIRED_COLUMNS,
    ].copy()

    c3_part = (
        c3_part.rename(
            columns={
                column: (
                    f"c3_{column}"
                )
                for column
                in C3_REQUIRED_COLUMNS
                if column
                not in {
                    "canonical_id",
                    "transition_vintage",
                    "old_source",
                    "new_source",
                    "relationship_type",
                    "equivalence_level",
                }
            }
        )
    )

    c3_part = (
        c3_part.drop(
            columns=[
                "transition_vintage",
                "old_source",
                "new_source",
                "relationship_type",
                "equivalence_level",
            ]
        )
    )

    _require_columns(
        review_spec,
        [
            "canonical_id",
            "old_reference_series",
            "new_reference_series",
            "aux_reference_series",
            "canonical_tcode",
            "review_mode",
        ],
        name="C4 review specification",
    )

    spec_part = (
        review_spec.loc[
            :,
            [
                "canonical_id",
                "old_reference_series",
                "new_reference_series",
                "aux_reference_series",
                "canonical_tcode",
                "review_mode",
            ],
        ]
        .rename(
            columns={
                "old_reference_series": (
                    "c4_old_reference_series"
                ),
                "new_reference_series": (
                    "c4_new_reference_series"
                ),
                "aux_reference_series": (
                    "c4_aux_reference_series"
                ),
                "canonical_tcode": (
                    "c4_canonical_tcode"
                ),
                "review_mode": (
                    "c4_review_mode"
                ),
            }
        )
    )

    c4_full = (
        _prefix_metrics(
            c4_summary,
            pair_role="old_vs_new",
            metrics=C4_FULL_METRICS,
            prefix="c4_full",
        )
    )

    c4_old_vs_aux_full = (
        _prefix_metrics(
            c4_summary,
            pair_role="old_vs_aux",
            metrics=C4_FULL_METRICS,
            prefix=(
                "c4_old_vs_aux_full"
            ),
        )
    )

    c4_aux_vs_new_full = (
        _prefix_metrics(
            c4_summary,
            pair_role="aux_vs_new",
            metrics=C4_FULL_METRICS,
            prefix=(
                "c4_aux_vs_new_full"
            ),
        )
    )

    c4_near = (
        select_near_transition_diagnostics(
            c4_rolling,
            c3=c3,
            pair_role="old_vs_new",
            prefix="c4_near",
        )
    )

    c4_old_vs_aux_near = (
        select_near_transition_diagnostics(
            c4_rolling,
            c3=c3,
            pair_role="old_vs_aux",
            prefix=(
                "c4_old_vs_aux_near"
            ),
        )
    )

    c4_aux_vs_new_near = (
        select_near_transition_diagnostics(
            c4_rolling,
            c3=c3,
            pair_role="aux_vs_new",
            prefix=(
                "c4_aux_vs_new_near"
            ),
        )
    )

    registry_provenance = (
        _build_registry_provenance(
            documentary,
            series_reviews,
        )
    )

    evidence = (
        documentary.copy()
    )

    parts = [
        spec_part,
        registry_provenance,
        c3_part,
        c4_full,
        c4_old_vs_aux_full,
        c4_aux_vs_new_full,
        c4_near,
        c4_old_vs_aux_near,
        c4_aux_vs_new_near,
    ]

    for part in parts:
        _require_columns(
            part,
            [
                "canonical_id",
            ],
            name=(
                "evidence merge part"
            ),
        )

        evidence = (
            evidence.merge(
                part,
                on="canonical_id",
                how="left",
                validate="one_to_one",
            )
        )

    evidence[
        "documentary_review_complete"
    ] = (
        evidence[
            "review_status"
        ].eq(
            "reviewed"
        )
    )

    forbidden_decision_columns = {
        "decision",
        "review_decision",
        "accepted",
        "rejected",
        "accept",
        "reject",
    }

    overlap = (
        forbidden_decision_columns
        & set(
            evidence.columns
        )
    )

    if overlap:
        raise RuntimeError(
            "Evidence builder must not create decision "
            f"columns: {sorted(overlap)}"
        )

    if len(evidence) != len(
        documentary
    ):
        raise RuntimeError(
            "Evidence merge changed the number of "
            "canonical concepts"
        )

    if evidence[
        "canonical_id"
    ].duplicated().any():
        raise RuntimeError(
            "Evidence output contains duplicate canonical_id rows"
        )

    return evidence


def write_evidence_csv(
    evidence: pd.DataFrame,
    path: str | Path,
    *,
    overwrite: bool = False,
) -> Path:
    path = Path(
        path
    )

    if (
        path.exists()
        and not overwrite
    ):
        raise FileExistsError(
            "Refusing to overwrite existing evidence file: "
            f"{path}"
        )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = (
        path.with_name(
            f".{path.name}.tmp"
        )
    )

    try:
        evidence.to_csv(
            temporary,
            index=False,
            float_format="%.15g",
        )

        temporary.replace(
            path
        )

    finally:
        if temporary.exists():
            temporary.unlink()

    return path


def build_rt_canonical_evidence_from_paths(
    *,
    documentary_path: str | Path,
    c3_path: str | Path,
    c4_summary_path: str | Path,
    c4_rolling_path: str | Path,
    review_spec_path: str | Path,
    series_reviews_path: str | Path,
) -> pd.DataFrame:
    c3 = pd.read_csv(
        c3_path,
        dtype={
            "transition_vintage": str,
        },
    )

    c4_summary = pd.read_csv(
        c4_summary_path
    )

    c4_rolling = pd.read_csv(
        c4_rolling_path
    )

    review_spec = (
        load_transition_review_spec(
            review_spec_path
        )
    )

    series_reviews = (
        read_reviews_csv(
            series_reviews_path
        )
    )

    documentary = pd.read_csv(
        documentary_path,
        dtype=str,
        keep_default_na=False,
    )

    return build_rt_canonical_evidence(
        documentary=documentary,
        c3=c3,
        c4_summary=c4_summary,
        c4_rolling=c4_rolling,
        review_spec=review_spec,
        series_reviews=series_reviews,
    )


__all__ = [
    "DOCUMENTARY_COLUMNS",
    "BOOLEAN_DOCUMENTARY_COLUMNS",
    "ALLOWED_DOCUMENTARY_ASSESSMENTS",
    "validate_documentary_review",
    "load_documentary_review",
    "select_near_transition_diagnostics",
    "build_rt_canonical_evidence",
    "build_rt_canonical_evidence_from_paths",
    "write_evidence_csv",
]