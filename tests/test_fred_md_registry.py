from __future__ import annotations

import pandas as pd

from dfm_pipeline.ingestion.fred_md_registry import (
    build_coverage_summary,
    build_registry_seed,
    build_registry_summary,
)


def _history() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series": "A",
                "first_vintage": "2020-01",
                "last_vintage": "2020-03",
                "n_vintages_present": 3,
                "tcodes_seen": "1",
                "n_distinct_tcodes": 1,
                "n_tcode_changes": 0,
            },
            {
                "series": "B",
                "first_vintage": "2020-02",
                "last_vintage": "2020-03",
                "n_vintages_present": 2,
                "tcodes_seen": "5",
                "n_distinct_tcodes": 1,
                "n_tcode_changes": 0,
            },
        ]
    )


def _presence() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "2020-01": [1, 0],
            "2020-02": [1, 1],
            "2020-03": [1, 1],
        },
        index=pd.Index(["A", "B"], name="series"),
    )


def _changes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "previous_vintage": "2020-01",
                "vintage": "2020-02",
                "change": "added",
                "series": "B",
            }
        ]
    )


def _series_audit() -> pd.DataFrame:
    rows = []
    for vintage, a_valid, a_internal in [
        ("2020-01", 10, 0),
        ("2020-02", 11, 1),
        ("2020-03", 12, 0),
    ]:
        rows.append(
            {
                "vintage": vintage,
                "series": "A",
                "n_rows": 12,
                "n_valid": a_valid,
                "pct_missing": 100 * (12 - a_valid) / 12,
                "n_leading_missing": 1,
                "n_trailing_missing": 0,
                "n_internal_missing": a_internal,
                "all_missing": False,
                "first_valid_date": "2019-02-01",
                "last_valid_date": "2020-01-01",
            }
        )

    for vintage in ["2020-02", "2020-03"]:
        rows.append(
            {
                "vintage": vintage,
                "series": "B",
                "n_rows": 12,
                "n_valid": 6,
                "pct_missing": 50.0,
                "n_leading_missing": 6,
                "n_trailing_missing": 0,
                "n_internal_missing": 0,
                "all_missing": False,
                "first_valid_date": "2019-07-01",
                "last_valid_date": "2020-01-01",
            }
        )

    return pd.DataFrame(rows)


def test_build_coverage_summary() -> None:
    coverage = build_coverage_summary(
        _series_audit(),
        vintage_start="2020-01",
        vintage_end="2020-03",
    )

    a = coverage.loc[coverage["raw_series"] == "A"].iloc[0]
    assert int(a["coverage_vintages_present"]) == 3
    assert int(a["coverage_min_n_valid"]) == 10
    assert int(a["coverage_max_internal_missing"]) == 1
    assert int(a["coverage_vintages_with_internal_missing"]) == 1

    b = coverage.loc[coverage["raw_series"] == "B"].iloc[0]
    assert int(b["coverage_vintages_present"]) == 2
    assert int(b["coverage_max_leading_missing"]) == 6


def test_build_registry_seed_marks_only_mechanical_stability() -> None:
    registry = build_registry_seed(
        series_tcode_history=_history(),
        series_presence=_presence(),
        series_changes=_changes(),
        series_audit=_series_audit(),
        stable_start="2020-01",
        stable_end="2020-03",
    )

    a = registry.loc[registry["raw_series"] == "A"].iloc[0]
    b = registry.loc[registry["raw_series"] == "B"].iloc[0]

    assert bool(a["candidate_rt_stable"]) is True
    assert bool(a["needs_stable_window_transition_review"]) is False

    assert bool(b["candidate_rt_stable"]) is False
    assert bool(b["needs_stable_window_transition_review"]) is True
    assert int(b["n_added_events"]) == 1

    # Semantic judgments must not be inferred by the generator.
    assert a["relationship_type"] == "unreviewed"
    assert b["relationship_type"] == "unreviewed"
    assert a["mapping_action"] == "unreviewed"
    assert b["mapping_action"] == "unreviewed"
    assert bool(a["include_rt_stable"]) is False
    assert bool(a["include_rt_canonical"]) is False


def test_registry_summary() -> None:
    registry = build_registry_seed(
        series_tcode_history=_history(),
        series_presence=_presence(),
        series_changes=_changes(),
        series_audit=_series_audit(),
        stable_start="2020-01",
        stable_end="2020-03",
    )

    summary = build_registry_summary(registry).iloc[0]

    assert int(summary["n_raw_series"]) == 2
    assert int(summary["n_rt_stable_candidates"]) == 1
    assert int(summary["n_transition_review"]) == 1
    assert int(summary["n_series_with_tcode_changes"]) == 0
