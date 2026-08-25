from __future__ import annotations

import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_registry_reviews import (
    REVIEW_COLUMNS,
    apply_review_overlay,
    build_review_template,
    validate_reviews,
)


def _seed() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "raw_series": "A",
                "canonical_id": "A",
                "needs_stable_window_transition_review": False,
                "include_rt_stable": False,
                "include_rt_canonical": False,
                "relationship_type": "unreviewed",
                "equivalence_level": "",
                "predecessor": "",
                "successor": "",
                "mapping_action": "unreviewed",
                "documented_change_vintage": "",
                "documented_change_type": "",
                "fred_md_change_log_reference": "",
                "fred_series_url": "",
                "source_agency_reference": "",
                "review_status": "unreviewed",
                "notes": "",
                "n_tcode_changes": 0,
            },
            {
                "raw_series": "B",
                "canonical_id": "B",
                "needs_stable_window_transition_review": True,
                "include_rt_stable": False,
                "include_rt_canonical": False,
                "relationship_type": "unreviewed",
                "equivalence_level": "",
                "predecessor": "",
                "successor": "",
                "mapping_action": "unreviewed",
                "documented_change_vintage": "",
                "documented_change_type": "",
                "fred_md_change_log_reference": "",
                "fred_series_url": "",
                "source_agency_reference": "",
                "review_status": "unreviewed",
                "notes": "",
                "n_tcode_changes": 0,
            },
        ]
    )


def test_review_template_contains_transition_and_extra() -> None:
    reviews = build_review_template(
        _seed(),
        extra_series=["A"],
    )
    assert reviews["raw_series"].tolist() == ["A", "B"]
    assert set(REVIEW_COLUMNS) == set(reviews.columns)


def test_sparse_overlay_changes_only_review_fields() -> None:
    seed = _seed()
    reviews = build_review_template(seed, extra_series=["A"])

    reviews.loc[reviews["raw_series"] == "A", "relationship_type"] = "unchanged"
    reviews.loc[reviews["raw_series"] == "A", "mapping_action"] = "direct"
    reviews.loc[reviews["raw_series"] == "A", "include_rt_stable"] = "true"
    reviews.loc[reviews["raw_series"] == "A", "review_status"] = "reviewed"

    out = apply_review_overlay(seed, reviews)
    a = out.loc[out["raw_series"] == "A"].iloc[0]
    b = out.loc[out["raw_series"] == "B"].iloc[0]

    assert a["relationship_type"] == "unchanged"
    assert a["mapping_action"] == "direct"
    assert bool(a["include_rt_stable"]) is True
    assert a["review_status"] == "reviewed"
    assert int(a["n_tcode_changes"]) == 0
    assert b["relationship_type"] == "unreviewed"


def test_reviewed_row_requires_relationship_and_action() -> None:
    seed = _seed()
    reviews = build_review_template(seed)
    reviews.loc[0, "review_status"] = "reviewed"

    with pytest.raises(ValueError):
        validate_reviews(seed, reviews)
