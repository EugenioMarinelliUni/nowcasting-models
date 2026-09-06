from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd


DECISION_COLUMNS = [
    "canonical_id",
    "documentary_review_complete",
    "review_decision",
    "rationale",
    "reviewer",
    "review_date",
]

ALLOWED_REVIEW_DECISIONS = {
    "accept_non_exact",
    "accept_with_break_note",
    "needs_further_review",
    "exclude_from_rt_canonical",
}

ACCEPTED_REVIEW_DECISIONS = {
    "accept_non_exact",
    "accept_with_break_note",
}

UNRESOLVED_REVIEW_DECISIONS = {
    "needs_further_review",
}

EXCLUDED_REVIEW_DECISIONS = {
    "exclude_from_rt_canonical",
}

_TRUE_VALUES = {
    "true",
    "1",
    "yes",
    "y",
}

_FALSE_VALUES = {
    "false",
    "0",
    "no",
    "n",
}

_DATE_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}$"
)


class RTCanonicalDecisionValidationError(
    ValueError
):
    """The RT_CANONICAL decision artifact is invalid."""


class RTCanonicalNotFreezableError(
    RTCanonicalDecisionValidationError
):
    """The reviewed RT_CANONICAL candidate is not yet freezable."""


def _clean_text(
    value: object,
) -> str:
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


def _parse_bool(
    value: object,
    *,
    field_name: str,
) -> bool:
    if isinstance(value, bool):
        return value

    text = _clean_text(value).lower()

    if text in _TRUE_VALUES:
        return True

    if text in _FALSE_VALUES:
        return False

    raise RTCanonicalDecisionValidationError(
        f"Invalid boolean value for {field_name}: "
        f"{value!r}"
    )


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
        raise RTCanonicalDecisionValidationError(
            f"{name} is missing required columns: "
            + ", ".join(missing)
        )


def _require_exact_decision_schema(
    frame: pd.DataFrame,
) -> None:
    actual = list(frame.columns)

    if actual == DECISION_COLUMNS:
        return

    expected_set = set(
        DECISION_COLUMNS
    )

    actual_set = set(
        actual
    )

    missing = sorted(
        expected_set
        - actual_set
    )

    extra = sorted(
        actual_set
        - expected_set
    )

    details: list[str] = []

    if missing:
        details.append(
            f"missing={missing}"
        )

    if extra:
        details.append(
            f"extra={extra}"
        )

    if (
        not missing
        and not extra
        and actual != DECISION_COLUMNS
    ):
        details.append(
            "column order differs from the required schema"
        )

    suffix = (
        "; ".join(details)
        if details
        else "schema mismatch"
    )

    raise RTCanonicalDecisionValidationError(
        "RT_CANONICAL decisions must have exactly "
        f"these columns, in this order: "
        f"{DECISION_COLUMNS}. {suffix}"
    )


def _validate_review_date(
    value: object,
) -> str:
    text = _clean_text(
        value
    )

    if not _DATE_PATTERN.fullmatch(
        text
    ):
        raise RTCanonicalDecisionValidationError(
            "Invalid review_date "
            f"{value!r}; expected YYYY-MM-DD"
        )

    try:
        datetime.strptime(
            text,
            "%Y-%m-%d",
        )
    except ValueError as exc:
        raise RTCanonicalDecisionValidationError(
            "Invalid calendar date in review_date: "
            f"{value!r}"
        ) from exc

    return text


def _normalize_evidence(
    evidence: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(
        evidence,
        [
            "canonical_id",
            "documentary_review_complete",
        ],
        name="RT_CANONICAL evidence",
    )

    if evidence.empty:
        raise RTCanonicalDecisionValidationError(
            "RT_CANONICAL evidence is empty"
        )

    out = evidence.loc[
        :,
        [
            "canonical_id",
            "documentary_review_complete",
        ],
    ].copy()

    out[
        "canonical_id"
    ] = out[
        "canonical_id"
    ].map(
        _clean_text
    )

    if out[
        "canonical_id"
    ].eq("").any():
        raise RTCanonicalDecisionValidationError(
            "RT_CANONICAL evidence contains "
            "blank canonical_id values"
        )

    duplicated = out[
        "canonical_id"
    ].duplicated(
        keep=False
    )

    if duplicated.any():
        duplicate_ids = sorted(
            out.loc[
                duplicated,
                "canonical_id",
            ]
            .unique()
            .tolist()
        )

        raise RTCanonicalDecisionValidationError(
            "RT_CANONICAL evidence contains "
            "duplicate canonical_id values: "
            f"{duplicate_ids}"
        )

    out[
        "documentary_review_complete"
    ] = out[
        "documentary_review_complete"
    ].map(
        lambda value: _parse_bool(
            value,
            field_name=(
                "evidence."
                "documentary_review_complete"
            ),
        )
    )

    return out.reset_index(
        drop=True
    )


def validate_rt_canonical_decisions(
    decisions: pd.DataFrame,
    *,
    evidence: pd.DataFrame,
) -> pd.DataFrame:
    """
    Validate the RT_CANONICAL manual decision artifact.

    This validates the structure and semantic consistency of the
    decisions file against the combined evidence artifact.

    It does NOT require the candidate to be freezable. In particular,
    ``needs_further_review`` and ``exclude_from_rt_canonical`` are valid
    decision values at this validation stage.

    Use ``assert_rt_canonical_freezable`` for the stronger final gate.
    """
    _require_exact_decision_schema(
        decisions
    )

    if decisions.empty:
        raise RTCanonicalDecisionValidationError(
            "RT_CANONICAL decisions file is empty"
        )

    out = decisions.loc[
        :,
        DECISION_COLUMNS,
    ].copy()

    text_columns = [
        "canonical_id",
        "review_decision",
        "rationale",
        "reviewer",
        "review_date",
    ]

    for column in text_columns:
        out[
            column
        ] = out[
            column
        ].map(
            _clean_text
        )

    out[
        "documentary_review_complete"
    ] = out[
        "documentary_review_complete"
    ].map(
        lambda value: _parse_bool(
            value,
            field_name=(
                "decisions."
                "documentary_review_complete"
            ),
        )
    )

    required_nonblank = [
        "canonical_id",
        "review_decision",
        "rationale",
        "reviewer",
        "review_date",
    ]

    for column in required_nonblank:
        blank = out[
            column
        ].eq("")

        if blank.any():
            rows = (
                out.index[
                    blank
                ]
                .tolist()
            )

            raise RTCanonicalDecisionValidationError(
                f"Blank values in required field "
                f"{column!r} at rows {rows}"
            )

    duplicated = out[
        "canonical_id"
    ].duplicated(
        keep=False
    )

    if duplicated.any():
        duplicate_ids = sorted(
            out.loc[
                duplicated,
                "canonical_id",
            ]
            .unique()
            .tolist()
        )

        raise RTCanonicalDecisionValidationError(
            "Duplicate RT_CANONICAL decision rows "
            f"for canonical_id: {duplicate_ids}"
        )

    invalid_decisions = sorted(
        set(
            out[
                "review_decision"
            ]
        )
        - ALLOWED_REVIEW_DECISIONS
    )

    if invalid_decisions:
        raise RTCanonicalDecisionValidationError(
            "Unsupported review_decision values: "
            f"{invalid_decisions}. "
            "Allowed values are: "
            f"{sorted(ALLOWED_REVIEW_DECISIONS)}"
        )

    out[
        "review_date"
    ] = out[
        "review_date"
    ].map(
        _validate_review_date
    )

    normalized_evidence = (
        _normalize_evidence(
            evidence
        )
    )

    decision_ids = set(
        out[
            "canonical_id"
        ]
    )

    evidence_ids = set(
        normalized_evidence[
            "canonical_id"
        ]
    )

    missing_decisions = sorted(
        evidence_ids
        - decision_ids
    )

    unexpected_decisions = sorted(
        decision_ids
        - evidence_ids
    )

    if missing_decisions:
        raise RTCanonicalDecisionValidationError(
            "Decision artifact is missing evidence "
            f"concepts: {missing_decisions}"
        )

    if unexpected_decisions:
        raise RTCanonicalDecisionValidationError(
            "Decision artifact contains concepts not "
            "present in the evidence artifact: "
            f"{unexpected_decisions}"
        )

    evidence_complete = (
        normalized_evidence
        .set_index(
            "canonical_id"
        )[
            "documentary_review_complete"
        ]
    )

    mismatch_ids: list[str] = []

    for _, row in out.iterrows():
        canonical_id = row[
            "canonical_id"
        ]

        expected_complete = bool(
            evidence_complete.loc[
                canonical_id
            ]
        )

        decision_complete = bool(
            row[
                "documentary_review_complete"
            ]
        )

        if (
            decision_complete
            != expected_complete
        ):
            mismatch_ids.append(
                canonical_id
            )

    if mismatch_ids:
        raise RTCanonicalDecisionValidationError(
            "documentary_review_complete differs "
            "between decisions and evidence for: "
            f"{sorted(mismatch_ids)}"
        )

    accepted_but_incomplete = out.loc[
        out[
            "review_decision"
        ].isin(
            ACCEPTED_REVIEW_DECISIONS
        )
        & ~out[
            "documentary_review_complete"
        ],
        "canonical_id",
    ].tolist()

    if accepted_but_incomplete:
        raise RTCanonicalDecisionValidationError(
            "Accepted RT_CANONICAL transitions must "
            "have completed documentary review. "
            "Incomplete accepted concepts: "
            f"{sorted(accepted_but_incomplete)}"
        )

    return out.reset_index(
        drop=True
    )


def assert_rt_canonical_freezable(
    decisions: pd.DataFrame,
    *,
    evidence: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply the final decision-level gate before freezing RT_CANONICAL.

    A candidate is freezable only if:

      * the decision artifact itself is valid;
      * every documentary review is complete;
      * no transition remains ``needs_further_review``;
      * no transition is marked ``exclude_from_rt_canonical``.

    This function does not modify or freeze any panel.
    """
    validated = (
        validate_rt_canonical_decisions(
            decisions,
            evidence=evidence,
        )
    )

    reasons: list[str] = []

    incomplete = validated.loc[
        ~validated[
            "documentary_review_complete"
        ],
        "canonical_id",
    ].tolist()

    if incomplete:
        reasons.append(
            "incomplete documentary review: "
            f"{sorted(incomplete)}"
        )

    unresolved = validated.loc[
        validated[
            "review_decision"
        ].isin(
            UNRESOLVED_REVIEW_DECISIONS
        ),
        "canonical_id",
    ].tolist()

    if unresolved:
        reasons.append(
            "needs further review: "
            f"{sorted(unresolved)}"
        )

    excluded = validated.loc[
        validated[
            "review_decision"
        ].isin(
            EXCLUDED_REVIEW_DECISIONS
        ),
        "canonical_id",
    ].tolist()

    if excluded:
        reasons.append(
            "excluded from current RT_CANONICAL "
            f"candidate: {sorted(excluded)}"
        )

    if reasons:
        raise RTCanonicalNotFreezableError(
            "RT_CANONICAL candidate is not "
            "freezable: "
            + "; ".join(
                reasons
            )
        )

    return validated


def load_rt_canonical_decisions(
    decisions_path: str | Path,
    *,
    evidence_path: str | Path,
) -> pd.DataFrame:
    decisions = pd.read_csv(
        decisions_path,
        dtype=str,
        keep_default_na=False,
    )

    evidence = pd.read_csv(
        evidence_path,
        dtype=str,
        keep_default_na=False,
    )

    return validate_rt_canonical_decisions(
        decisions,
        evidence=evidence,
    )


def assert_rt_canonical_freezable_from_paths(
    decisions_path: str | Path,
    *,
    evidence_path: str | Path,
) -> pd.DataFrame:
    decisions = pd.read_csv(
        decisions_path,
        dtype=str,
        keep_default_na=False,
    )

    evidence = pd.read_csv(
        evidence_path,
        dtype=str,
        keep_default_na=False,
    )

    return assert_rt_canonical_freezable(
        decisions,
        evidence=evidence,
    )


__all__ = [
    "DECISION_COLUMNS",
    "ALLOWED_REVIEW_DECISIONS",
    "ACCEPTED_REVIEW_DECISIONS",
    "UNRESOLVED_REVIEW_DECISIONS",
    "EXCLUDED_REVIEW_DECISIONS",
    "RTCanonicalDecisionValidationError",
    "RTCanonicalNotFreezableError",
    "validate_rt_canonical_decisions",
    "assert_rt_canonical_freezable",
    "load_rt_canonical_decisions",
    "assert_rt_canonical_freezable_from_paths",
]