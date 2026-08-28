from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_rt_canonical import (
    audit_rt_canonical_transitions,
    build_rt_canonical_specification,
    build_rt_canonical_summary,
    canonicalize_transformed_vintage,
    resolve_active_source,
    transform_then_canonicalize_vintage,
)


def _registry() -> pd.DataFrame:
    """
    Small synthetic registry used to test canonical specification logic.

    - A is inherited directly from RT_STABLE.
    - OLD -> NEW is a reviewed switch-by-vintage mapping for canonical X.
    - EXCLUDED_OLD belongs to a canonical mapping that is deliberately not
      approved for RT_CANONICAL and therefore must not enter the specification.
    """
    return pd.DataFrame(
        [
            {
                "raw_series": "A",
                "canonical_id": "A",
                "mapping_action": "unreviewed",
                "include_rt_canonical": "False",
                "review_status": "unreviewed",
                "relationship_type": "unreviewed",
                "equivalence_level": "",
                "active_from_vintage": "1999-08",
                "active_to_vintage": "2026-06",
                "tcodes_seen": "1",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
                "documented_change_vintage": "",
                "notes": "",
            },
            {
                "raw_series": "OLD",
                "canonical_id": "X",
                "mapping_action": "switch_by_vintage",
                "include_rt_canonical": "True",
                "review_status": "reviewed",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "active_from_vintage": "1999-08",
                "active_to_vintage": "2010-02",
                "tcodes_seen": "5",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
                "documented_change_vintage": "2010-03",
                "notes": "old source",
            },
            {
                "raw_series": "NEW",
                "canonical_id": "X",
                "mapping_action": "switch_by_vintage",
                "include_rt_canonical": "True",
                "review_status": "reviewed",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "active_from_vintage": "2010-03",
                "active_to_vintage": "2026-06",
                "tcodes_seen": "5",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
                "documented_change_vintage": "2010-03",
                "notes": "new source",
            },
            {
                "raw_series": "EXCLUDED_OLD",
                "canonical_id": "Y",
                "mapping_action": "switch_by_vintage",
                "include_rt_canonical": "False",
                "review_status": "reviewed",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "active_from_vintage": "1999-08",
                "active_to_vintage": "2010-02",
                "tcodes_seen": "1",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
                "documented_change_vintage": "",
                "notes": "",
            },
        ]
    )


def _audit_registry() -> pd.DataFrame:
    """
    Synthetic registry with a switch exactly between 2010-01 and 2010-02.

    This makes it possible to test the empirical archive audit with only two
    temporary FRED-MD-style vintage files.
    """
    return pd.DataFrame(
        [
            {
                "raw_series": "A",
                "canonical_id": "A",
                "mapping_action": "unreviewed",
                "include_rt_canonical": "False",
                "review_status": "unreviewed",
                "relationship_type": "unreviewed",
                "equivalence_level": "",
                "active_from_vintage": "1999-08",
                "active_to_vintage": "2026-06",
                "tcodes_seen": "1",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
                "documented_change_vintage": "",
                "notes": "",
            },
            {
                "raw_series": "OLD",
                "canonical_id": "X",
                "mapping_action": "switch_by_vintage",
                "include_rt_canonical": "True",
                "review_status": "reviewed",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "active_from_vintage": "1999-08",
                "active_to_vintage": "2010-01",
                "tcodes_seen": "5",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
                "documented_change_vintage": "2010-02",
                "notes": "old source",
            },
            {
                "raw_series": "NEW",
                "canonical_id": "X",
                "mapping_action": "switch_by_vintage",
                "include_rt_canonical": "True",
                "review_status": "reviewed",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "active_from_vintage": "2010-02",
                "active_to_vintage": "2026-06",
                "tcodes_seen": "5",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
                "documented_change_vintage": "2010-02",
                "notes": "new source",
            },
        ]
    )


def _stable() -> pd.DataFrame:
    """
    Minimal frozen RT_STABLE specification used by the synthetic tests.
    """
    return pd.DataFrame(
        [
            {
                "panel_position": "1",
                "raw_series": "A",
                "canonical_id": "A",
                "tcode": "1",
                "stable_window_start": "2010-01",
                "stable_window_end": "2026-06",
                "final_include": "True",
            }
        ]
    )


def _write_vintage(
    path: Path,
    *,
    old: bool,
    new: bool,
) -> None:
    """
    Write a tiny FRED-MD-style CSV with an embedded t-code row.

    IMPORTANT:
    OLD and NEW deliberately use non-constant positive growth rates.
    Because they have t-code 5 (first log difference), a perfect geometric
    progression such as 100, 110, 121, 133.1 would transform to a constant
    series and would correctly trigger the production zero-variance hard gate.
    These values instead produce strictly positive transformed variance, so
    this fixture represents a valid transition rather than a degenerate one.
    """
    columns = ["sasdate", "A"]
    tcode_row = ["transform", "1"]

    data_rows = [
        ["2009-01-01", "1"],
        ["2009-02-01", "2"],
        ["2009-03-01", "3"],
        ["2009-04-01", "4"],
    ]

    if old:
        columns.append("OLD")
        tcode_row.append("5")
        old_values = ["100", "108", "119", "131"]
        for row, value in zip(data_rows, old_values):
            row.append(value)

    if new:
        columns.append("NEW")
        tcode_row.append("5")
        new_values = ["1000", "1070", "1160", "1300"]
        for row, value in zip(data_rows, new_values):
            row.append(value)

    frame = pd.DataFrame(
        [tcode_row] + data_rows,
        columns=columns,
    )
    frame.to_csv(path, index=False)


def test_builds_direct_plus_reviewed_switch() -> None:
    panel, source_map = build_rt_canonical_specification(
        _registry(),
        _stable(),
    )

    assert panel["canonical_id"].tolist() == ["A", "X"]
    assert panel["mapping_type"].tolist() == [
        "direct",
        "switch_by_vintage",
    ]
    assert panel["panel_position"].tolist() == [1, 2]

    x = source_map.loc[
        source_map["canonical_id"].eq("X")
    ].sort_values("source_order")

    assert x["source_series"].tolist() == ["OLD", "NEW"]
    assert x["active_from_vintage"].tolist() == [
        "2010-01",
        "2010-03",
    ]
    assert x["active_to_vintage"].tolist() == [
        "2010-02",
        "2026-06",
    ]


def test_unapproved_switch_is_excluded() -> None:
    panel, _ = build_rt_canonical_specification(
        _registry(),
        _stable(),
    )

    assert "Y" not in set(panel["canonical_id"])


def test_source_resolution_is_exact_at_boundary() -> None:
    _, source_map = build_rt_canonical_specification(
        _registry(),
        _stable(),
    )

    old = resolve_active_source(
        source_map,
        canonical_id="X",
        vintage="2010-02",
    )
    new = resolve_active_source(
        source_map,
        canonical_id="X",
        vintage="2010-03",
    )

    assert old["source_series"] == "OLD"
    assert new["source_series"] == "NEW"


def test_gap_is_rejected() -> None:
    registry = _registry()

    registry.loc[
        registry["raw_series"].eq("NEW"),
        "active_from_vintage",
    ] = "2010-04"

    with pytest.raises(ValueError, match="gap"):
        build_rt_canonical_specification(
            registry,
            _stable(),
        )


def test_overlap_is_rejected() -> None:
    registry = _registry()

    registry.loc[
        registry["raw_series"].eq("NEW"),
        "active_from_vintage",
    ] = "2010-02"

    with pytest.raises(ValueError, match="overlap"):
        build_rt_canonical_specification(
            registry,
            _stable(),
        )


def test_tcode_change_is_rejected() -> None:
    registry = _registry()

    registry.loc[
        registry["raw_series"].eq("OLD"),
        "n_tcode_changes",
    ] = "1"

    with pytest.raises(ValueError, match="t-code changes"):
        build_rt_canonical_specification(
            registry,
            _stable(),
        )


def test_deterministic_order() -> None:
    registry = (
        _registry()
        .sample(frac=1.0, random_state=42)
        .reset_index(drop=True)
    )

    panel, source_map = build_rt_canonical_specification(
        registry,
        _stable(),
    )

    assert panel["canonical_id"].tolist() == ["A", "X"]

    x = source_map.loc[
        source_map["canonical_id"].eq("X")
    ].sort_values("source_order")

    assert x["source_series"].tolist() == ["OLD", "NEW"]


def test_canonicalize_transformed_vintage_preserves_nan() -> None:
    panel, source_map = build_rt_canonical_specification(
        _registry(),
        _stable(),
    )

    index = pd.date_range(
        "2009-01-01",
        periods=3,
        freq="MS",
    )

    transformed = pd.DataFrame(
        {
            "A": [1.0, np.nan, 3.0],
            "OLD": [0.1, 0.2, 0.3],
            "NEW": [0.4, np.nan, 0.6],
        },
        index=index,
    )

    out = canonicalize_transformed_vintage(
        transformed,
        panel,
        source_map,
        vintage="2010-03",
    )

    assert np.isnan(out.loc[index[1], "A"])
    assert np.isnan(out.loc[index[1], "X"])


def test_transform_then_canonicalize_does_not_raw_splice() -> None:
    panel, source_map = build_rt_canonical_specification(
        _registry(),
        _stable(),
    )

    index = pd.date_range(
        "2009-01-01",
        periods=4,
        freq="MS",
    )

    raw = pd.DataFrame(
        {
            "A": [1.0, 2.0, 3.0, 4.0],
            "OLD": [100.0, 108.0, 119.0, 131.0],
            "NEW": [1000.0, 1070.0, 1160.0, 1300.0],
        },
        index=index,
    )

    out = transform_then_canonicalize_vintage(
        raw,
        {
            "A": 1,
            "OLD": 5,
            "NEW": 5,
        },
        panel,
        source_map,
        vintage="2010-03",
    )

    expected_new = np.log(raw["NEW"]).diff()

    pd.testing.assert_series_equal(
        out["X"],
        expected_new,
        check_names=False,
    )


def test_empirical_transition_audit(
    tmp_path: Path,
) -> None:
    panel, source_map = build_rt_canonical_specification(
        _audit_registry(),
        _stable(),
    )

    _write_vintage(
        tmp_path / "fred_md_2010-01.csv",
        old=True,
        new=False,
    )
    _write_vintage(
        tmp_path / "fred_md_2010-02.csv",
        old=False,
        new=True,
    )

    by_vintage, by_concept, boundaries, summary = (
        audit_rt_canonical_transitions(
            tmp_path,
            panel,
            source_map,
            start="2010-01",
            end="2010-02",
        )
    )

    assert by_vintage["active_source"].tolist() == [
        "OLD",
        "NEW",
    ]

    assert not by_vintage[
        "canonical_audit_hard_fail"
    ].any()

    assert (
        by_concept.loc[
            by_concept["canonical_id"].eq("X"),
            "canonical_audit_status",
        ].iloc[0]
        == "pass"
    )

    assert len(boundaries) == 1
    assert boundaries.iloc[0]["canonical_id"] == "X"
    assert boundaries.iloc[0]["old_source"] == "OLD"
    assert boundaries.iloc[0]["new_source"] == "NEW"
    assert boundaries.iloc[0]["transition_vintage"] == "2010-02"

    assert int(
        summary.loc[
            0,
            "n_vintage_concept_hard_fail",
        ]
    ) == 0

    assert int(
        summary.loc[
            0,
            "n_concepts_hard_fail",
        ]
    ) == 0


def test_summary_counts_concepts_and_segments() -> None:
    panel, source_map = build_rt_canonical_specification(
        _registry(),
        _stable(),
    )

    summary = build_rt_canonical_summary(
        panel,
        source_map,
    ).iloc[0]

    assert int(summary["n_canonical_concepts"]) == 2
    assert int(summary["n_direct_concepts"]) == 1
    assert int(summary["n_switch_by_vintage_concepts"]) == 1
    assert int(summary["n_source_segments"]) == 3
