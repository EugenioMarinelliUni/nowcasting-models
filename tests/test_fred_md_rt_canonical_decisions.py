from __future__ import annotations

import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_rt_canonical_decisions import (
    DECISION_COLUMNS,
    RTCanonicalDecisionValidationError,
    RTCanonicalNotFreezableError,
    assert_rt_canonical_freezable,
    validate_rt_canonical_decisions,
)


def _evidence() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "SERIES_A",
                "documentary_review_complete": True,
            },
            {
                "canonical_id": "SERIES_B",
                "documentary_review_complete": True,
            },
        ]
    )


def _decisions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "SERIES_A",
                "documentary_review_complete": True,
                "review_decision": "accept_non_exact",
                "rationale": (
                    "Documented non-exact successor "
                    "supported by the review evidence."
                ),
                "reviewer": "Reviewer",
                "review_date": "2026-09-06",
            },
            {
                "canonical_id": "SERIES_B",
                "documentary_review_complete": True,
                "review_decision": "accept_with_break_note",
                "rationale": (
                    "Documented successor accepted with "
                    "an explicit methodological break note."
                ),
                "reviewer": "Reviewer",
                "review_date": "2026-09-06",
            },
        ],
        columns=DECISION_COLUMNS,
    )


def test_validate_accepts_valid_decisions() -> None:
    result = (
        validate_rt_canonical_decisions(
            _decisions(),
            evidence=_evidence(),
        )
    )

    assert list(
        result.columns
    ) == DECISION_COLUMNS

    assert len(
        result
    ) == 2

    assert result[
        "canonical_id"
    ].is_unique

    assert result[
        "documentary_review_complete"
    ].all()


def test_validate_rejects_wrong_schema() -> None:
    decisions = (
        _decisions()
        .drop(
            columns=[
                "reviewer",
            ]
        )
    )

    with pytest.raises(
        RTCanonicalDecisionValidationError,
        match="exactly these columns",
    ):
        validate_rt_canonical_decisions(
            decisions,
            evidence=_evidence(),
        )


def test_validate_rejects_duplicate_canonical_id() -> None:
    decisions = (
        _decisions()
        .copy()
    )

    decisions.loc[
        1,
        "canonical_id",
    ] = "SERIES_A"

    with pytest.raises(
        RTCanonicalDecisionValidationError,
        match="Duplicate",
    ):
        validate_rt_canonical_decisions(
            decisions,
            evidence=_evidence(),
        )


def test_validate_rejects_invalid_decision_vocabulary() -> None:
    decisions = (
        _decisions()
        .copy()
    )

    decisions.loc[
        0,
        "review_decision",
    ] = "accept_exact"

    with pytest.raises(
        RTCanonicalDecisionValidationError,
        match="Unsupported review_decision",
    ):
        validate_rt_canonical_decisions(
            decisions,
            evidence=_evidence(),
        )


def test_validate_rejects_concept_set_mismatch() -> None:
    decisions = (
        _decisions()
        .copy()
    )

    decisions.loc[
        1,
        "canonical_id",
    ] = "SERIES_C"

    with pytest.raises(
        RTCanonicalDecisionValidationError,
        match=(
            "missing evidence concepts"
        ),
    ):
        validate_rt_canonical_decisions(
            decisions,
            evidence=_evidence(),
        )


def test_validate_rejects_documentary_completion_mismatch() -> None:
    decisions = (
        _decisions()
        .copy()
    )

    decisions.loc[
        0,
        "documentary_review_complete",
    ] = False

    with pytest.raises(
        RTCanonicalDecisionValidationError,
        match=(
            "documentary_review_complete differs"
        ),
    ):
        validate_rt_canonical_decisions(
            decisions,
            evidence=_evidence(),
        )


def test_validate_rejects_acceptance_with_incomplete_documentary_review(
) -> None:
    decisions = (
        _decisions()
        .copy()
    )

    evidence = (
        _evidence()
        .copy()
    )

    decisions.loc[
        0,
        "documentary_review_complete",
    ] = False

    evidence.loc[
        0,
        "documentary_review_complete",
    ] = False

    with pytest.raises(
        RTCanonicalDecisionValidationError,
        match=(
            "Accepted RT_CANONICAL transitions"
        ),
    ):
        validate_rt_canonical_decisions(
            decisions,
            evidence=evidence,
        )


def test_validate_allows_needs_further_review_when_documentary_incomplete(
) -> None:
    decisions = (
        _decisions()
        .copy()
    )

    evidence = (
        _evidence()
        .copy()
    )

    decisions.loc[
        0,
        "documentary_review_complete",
    ] = False

    decisions.loc[
        0,
        "review_decision",
    ] = "needs_further_review"

    evidence.loc[
        0,
        "documentary_review_complete",
    ] = False

    result = (
        validate_rt_canonical_decisions(
            decisions,
            evidence=evidence,
        )
    )

    row = (
        result.loc[
            result[
                "canonical_id"
            ].eq(
                "SERIES_A"
            )
        ]
        .iloc[0]
    )

    assert not bool(
        row[
            "documentary_review_complete"
        ]
    )

    assert (
        row[
            "review_decision"
        ]
        == "needs_further_review"
    )


def test_validate_rejects_invalid_review_date() -> None:
    decisions = (
        _decisions()
        .copy()
    )

    decisions.loc[
        0,
        "review_date",
    ] = "2026-02-30"

    with pytest.raises(
        RTCanonicalDecisionValidationError,
        match="Invalid calendar date",
    ):
        validate_rt_canonical_decisions(
            decisions,
            evidence=_evidence(),
        )


def test_validate_rejects_blank_rationale() -> None:
    decisions = (
        _decisions()
        .copy()
    )

    decisions.loc[
        0,
        "rationale",
    ] = "   "

    with pytest.raises(
        RTCanonicalDecisionValidationError,
        match="Blank values",
    ):
        validate_rt_canonical_decisions(
            decisions,
            evidence=_evidence(),
        )


def test_freeze_gate_accepts_resolved_candidate() -> None:
    result = (
        assert_rt_canonical_freezable(
            _decisions(),
            evidence=_evidence(),
        )
    )

    assert len(
        result
    ) == 2

    assert set(
        result[
            "review_decision"
        ]
    ) == {
        "accept_non_exact",
        "accept_with_break_note",
    }


def test_freeze_gate_rejects_needs_further_review() -> None:
    decisions = (
        _decisions()
        .copy()
    )

    decisions.loc[
        0,
        "review_decision",
    ] = "needs_further_review"

    with pytest.raises(
        RTCanonicalNotFreezableError,
        match="needs further review",
    ):
        assert_rt_canonical_freezable(
            decisions,
            evidence=_evidence(),
        )


def test_freeze_gate_rejects_excluded_transition() -> None:
    decisions = (
        _decisions()
        .copy()
    )

    decisions.loc[
        0,
        "review_decision",
    ] = "exclude_from_rt_canonical"

    with pytest.raises(
        RTCanonicalNotFreezableError,
        match="excluded from current",
    ):
        assert_rt_canonical_freezable(
            decisions,
            evidence=_evidence(),
        )