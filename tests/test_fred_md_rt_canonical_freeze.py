from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_rt_canonical_decisions import (
    DECISION_COLUMNS,
    RTCanonicalNotFreezableError,
)
from dfm_pipeline.ingestion.fred_md_rt_canonical_freeze import (
    FROZEN_COLUMNS,
    RTCanonicalFreezeError,
    build_frozen_rt_canonical_specification,
    freeze_rt_canonical_from_paths,
    sha256_file,
)


def _panel() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "panel_position": 1,
                "canonical_id": "DIRECT_A",
                "mapping_type": "direct",
                "n_source_segments": 1,
                "stable_window_start": "2010-01",
                "stable_window_end": "2010-03",
                "final_include": True,
                "inclusion_reason": (
                    "inherited from frozen RT_STABLE "
                    "specification"
                ),
            },
            {
                "panel_position": 2,
                "canonical_id": "SWITCH_B",
                "mapping_type": "switch_by_vintage",
                "n_source_segments": 2,
                "stable_window_start": "2010-01",
                "stable_window_end": "2010-03",
                "final_include": True,
                "inclusion_reason": (
                    "reviewed canonical source switch"
                ),
            },
        ]
    )


def _source_map() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "DIRECT_A",
                "source_order": 1,
                "source_series": "DIRECT_A",
                "expected_tcode": 1,
                "active_from_vintage": "2010-01",
                "active_to_vintage": "2010-03",
                "mapping_action": "direct",
                "relationship_type": "unreviewed",
                "equivalence_level": "",
                "documented_change_vintage": "",
                "source_origin": "rt_stable",
                "notes": "",
            },
            {
                "canonical_id": "SWITCH_B",
                "source_order": 1,
                "source_series": "B_OLD",
                "expected_tcode": 6,
                "active_from_vintage": "2010-01",
                "active_to_vintage": "2010-02",
                "mapping_action": "switch_by_vintage",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "documented_change_vintage": "2010-03",
                "source_origin": "registry_review",
                "notes": "Historical predecessor.",
            },
            {
                "canonical_id": "SWITCH_B",
                "source_order": 2,
                "source_series": "B_NEW",
                "expected_tcode": 6,
                "active_from_vintage": "2010-03",
                "active_to_vintage": "2010-03",
                "mapping_action": "switch_by_vintage",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "documented_change_vintage": "2010-03",
                "source_origin": "registry_review",
                "notes": "Reviewed successor.",
            },
        ]
    )


def _evidence() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "SWITCH_B",
                "old_source": "B_OLD",
                "new_source": "B_NEW",
                "transition_vintage": "2010-03",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "documentary_review_complete": True,
            }
        ]
    )


def _decisions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "SWITCH_B",
                "documentary_review_complete": True,
                "review_decision": (
                    "accept_with_break_note"
                ),
                "rationale": (
                    "Official non-exact successor "
                    "accepted with an explicit break note."
                ),
                "reviewer": "Reviewer",
                "review_date": "2026-09-06",
            }
        ],
        columns=DECISION_COLUMNS,
    )


def _write_inputs(
    tmp_path: Path,
) -> dict[str, Path]:
    paths = {
        "panel": (
            tmp_path
            / "candidate.csv"
        ),
        "source_map": (
            tmp_path
            / "source_map.csv"
        ),
        "evidence": (
            tmp_path
            / "evidence.csv"
        ),
        "decisions": (
            tmp_path
            / "decisions.csv"
        ),
    }

    _panel().to_csv(
        paths[
            "panel"
        ],
        index=False,
    )

    _source_map().to_csv(
        paths[
            "source_map"
        ],
        index=False,
    )

    _evidence().to_csv(
        paths[
            "evidence"
        ],
        index=False,
    )

    _decisions().to_csv(
        paths[
            "decisions"
        ],
        index=False,
    )

    return paths


def test_build_frozen_specification_preserves_all_segments(
) -> None:
    frozen = (
        build_frozen_rt_canonical_specification(
            _panel(),
            _source_map(),
            _decisions(),
            _evidence(),
            expected_start="2010-01",
            expected_end="2010-03",
        )
    )

    assert list(
        frozen.columns
    ) == FROZEN_COLUMNS

    assert len(
        frozen
    ) == 3

    assert frozen[
        "canonical_id"
    ].nunique() == 2

    direct = (
        frozen.loc[
            frozen[
                "canonical_id"
            ].eq(
                "DIRECT_A"
            )
        ]
        .iloc[0]
    )

    assert (
        direct[
            "transition_review_required"
        ]
        is False
        or not bool(
            direct[
                "transition_review_required"
            ]
        )
    )

    assert (
        direct[
            "review_decision"
        ]
        == ""
    )

    switched = frozen.loc[
        frozen[
            "canonical_id"
        ].eq(
            "SWITCH_B"
        )
    ]

    assert len(
        switched
    ) == 2

    assert switched[
        "transition_review_required"
    ].all()

    assert set(
        switched[
            "review_decision"
        ]
    ) == {
        "accept_with_break_note",
    }

    assert switched[
        "documentary_review_complete"
    ].map(
        bool
    ).all()


def test_rejects_final_include_false(
) -> None:
    panel = (
        _panel()
        .copy()
    )

    panel.loc[
        panel[
            "canonical_id"
        ].eq(
            "DIRECT_A"
        ),
        "final_include",
    ] = False

    with pytest.raises(
        RTCanonicalFreezeError,
        match="final_include=True",
    ):
        build_frozen_rt_canonical_specification(
            panel,
            _source_map(),
            _decisions(),
            _evidence(),
            expected_start="2010-01",
            expected_end="2010-03",
        )


def test_rejects_expected_window_mismatch(
) -> None:
    with pytest.raises(
        RTCanonicalFreezeError,
        match="does not match expected freeze window",
    ):
        build_frozen_rt_canonical_specification(
            _panel(),
            _source_map(),
            _decisions(),
            _evidence(),
            expected_start="2010-01",
            expected_end="2010-04",
        )


def test_rejects_switch_decision_set_mismatch(
) -> None:
    evidence = (
        _evidence()
        .copy()
    )

    decisions = (
        _decisions()
        .copy()
    )

    evidence.loc[
        0,
        "canonical_id",
    ] = "OTHER_SWITCH"

    decisions.loc[
        0,
        "canonical_id",
    ] = "OTHER_SWITCH"

    with pytest.raises(
        RTCanonicalFreezeError,
        match="Switch concept set does not match",
    ):
        build_frozen_rt_canonical_specification(
            _panel(),
            _source_map(),
            decisions,
            evidence,
            expected_start="2010-01",
            expected_end="2010-03",
        )


def test_rejects_stale_transition_source(
) -> None:
    evidence = (
        _evidence()
        .copy()
    )

    evidence.loc[
        0,
        "new_source",
    ] = "WRONG_SUCCESSOR"

    with pytest.raises(
        RTCanonicalFreezeError,
        match="new_source does not match candidate",
    ):
        build_frozen_rt_canonical_specification(
            _panel(),
            _source_map(),
            _decisions(),
            evidence,
            expected_start="2010-01",
            expected_end="2010-03",
        )


def test_rejects_stale_transition_vintage(
) -> None:
    evidence = (
        _evidence()
        .copy()
    )

    evidence.loc[
        0,
        "transition_vintage",
    ] = "2010-02"

    with pytest.raises(
        RTCanonicalFreezeError,
        match=(
            "transition_vintage does not match candidate"
        ),
    ):
        build_frozen_rt_canonical_specification(
            _panel(),
            _source_map(),
            _decisions(),
            evidence,
            expected_start="2010-01",
            expected_end="2010-03",
        )


def test_rejects_unresolved_transition_decision(
) -> None:
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
        build_frozen_rt_canonical_specification(
            _panel(),
            _source_map(),
            decisions,
            _evidence(),
            expected_start="2010-01",
            expected_end="2010-03",
        )


def test_rejects_source_map_gap(
) -> None:
    source_map = (
        _source_map()
        .copy()
    )

    source_map.loc[
        (
            source_map[
                "canonical_id"
            ].eq(
                "SWITCH_B"
            )
            & source_map[
                "source_order"
            ].eq(
                1
            )
        ),
        "active_to_vintage",
    ] = "2010-01"

    with pytest.raises(
        RTCanonicalFreezeError,
        match="gap",
    ):
        build_frozen_rt_canonical_specification(
            _panel(),
            source_map,
            _decisions(),
            _evidence(),
            expected_start="2010-01",
            expected_end="2010-03",
        )


def test_sha256_file(
    tmp_path: Path,
) -> None:
    path = (
        tmp_path
        / "test.bin"
    )

    payload = b"abc"

    path.write_bytes(
        payload
    )

    expected = (
        hashlib.sha256(
            payload
        )
        .hexdigest()
    )

    assert (
        sha256_file(
            path
        )
        == expected
    )


def test_freeze_from_paths_writes_csv_and_manifest(
    tmp_path: Path,
) -> None:
    paths = (
        _write_inputs(
            tmp_path
        )
    )

    output = (
        tmp_path
        / "frozen.csv"
    )

    manifest_path = (
        tmp_path
        / "manifest.json"
    )

    (
        returned_output,
        returned_manifest,
        manifest,
    ) = freeze_rt_canonical_from_paths(
        candidate_panel_path=(
            paths[
                "panel"
            ]
        ),
        candidate_source_map_path=(
            paths[
                "source_map"
            ]
        ),
        evidence_path=(
            paths[
                "evidence"
            ]
        ),
        decisions_path=(
            paths[
                "decisions"
            ]
        ),
        output_csv_path=(
            output
        ),
        manifest_path=(
            manifest_path
        ),
        expected_start="2010-01",
        expected_end="2010-03",
        freeze_date="2026-09-06",
    )

    assert (
        returned_output
        == output
    )

    assert (
        returned_manifest
        == manifest_path
    )

    assert output.exists()

    assert manifest_path.exists()

    frozen = pd.read_csv(
        output,
        dtype=str,
        keep_default_na=False,
    )

    assert len(
        frozen
    ) == 3

    assert frozen[
        "canonical_id"
    ].nunique() == 2

    saved_manifest = json.loads(
        manifest_path.read_text(
            encoding="utf-8"
        )
    )

    assert (
        saved_manifest
        == manifest
    )

    assert (
        manifest[
            "schema_version"
        ]
        == "1.0"
    )

    assert (
        manifest[
            "freeze_date"
        ]
        == "2026-09-06"
    )

    assert (
        manifest[
            "stable_window"
        ]
        == {
            "start": "2010-01",
            "end": "2010-03",
        }
    )

    assert (
        manifest[
            "counts"
        ][
            "canonical_concepts"
        ]
        == 2
    )

    assert (
        manifest[
            "counts"
        ][
            "direct_concepts"
        ]
        == 1
    )

    assert (
        manifest[
            "counts"
        ][
            "switch_by_vintage_concepts"
        ]
        == 1
    )

    assert (
        manifest[
            "counts"
        ][
            "source_segments"
        ]
        == 3
    )

    assert (
        manifest[
            "counts"
        ][
            "transition_boundaries"
        ]
        == 1
    )

    assert (
        manifest[
            "decision_distribution"
        ]
        == {
            "accept_with_break_note": 1,
        }
    )

    assert (
        manifest[
            "output"
        ][
            "frozen_csv"
        ][
            "sha256"
        ]
        == sha256_file(
            output
        )
    )

    for input_record in (
        manifest[
            "inputs"
        ].values()
    ):
        assert len(
            input_record[
                "sha256"
            ]
        ) == 64


def test_freeze_refuses_overwrite(
    tmp_path: Path,
) -> None:
    paths = (
        _write_inputs(
            tmp_path
        )
    )

    output = (
        tmp_path
        / "frozen.csv"
    )

    manifest = (
        tmp_path
        / "manifest.json"
    )

    kwargs = {
        "candidate_panel_path": (
            paths[
                "panel"
            ]
        ),
        "candidate_source_map_path": (
            paths[
                "source_map"
            ]
        ),
        "evidence_path": (
            paths[
                "evidence"
            ]
        ),
        "decisions_path": (
            paths[
                "decisions"
            ]
        ),
        "output_csv_path": (
            output
        ),
        "manifest_path": (
            manifest
        ),
        "expected_start": (
            "2010-01"
        ),
        "expected_end": (
            "2010-03"
        ),
        "freeze_date": (
            "2026-09-06"
        ),
    }

    freeze_rt_canonical_from_paths(
        **kwargs
    )

    with pytest.raises(
        RTCanonicalFreezeError,
        match="Refusing to overwrite",
    ):
        freeze_rt_canonical_from_paths(
            **kwargs
        )


def test_freeze_overwrite_allows_explicit_rebuild(
    tmp_path: Path,
) -> None:
    paths = (
        _write_inputs(
            tmp_path
        )
    )

    output = (
        tmp_path
        / "frozen.csv"
    )

    manifest = (
        tmp_path
        / "manifest.json"
    )

    kwargs = {
        "candidate_panel_path": (
            paths[
                "panel"
            ]
        ),
        "candidate_source_map_path": (
            paths[
                "source_map"
            ]
        ),
        "evidence_path": (
            paths[
                "evidence"
            ]
        ),
        "decisions_path": (
            paths[
                "decisions"
            ]
        ),
        "output_csv_path": (
            output
        ),
        "manifest_path": (
            manifest
        ),
        "expected_start": (
            "2010-01"
        ),
        "expected_end": (
            "2010-03"
        ),
        "freeze_date": (
            "2026-09-06"
        ),
    }

    freeze_rt_canonical_from_paths(
        **kwargs
    )

    (
        returned_output,
        returned_manifest,
        _,
    ) = freeze_rt_canonical_from_paths(
        **kwargs,
        overwrite=True,
    )

    assert returned_output.exists()

    assert returned_manifest.exists()

    assert len(
        pd.read_csv(
            returned_output
        )
    ) == 3


def test_rejects_invalid_freeze_date(
    tmp_path: Path,
) -> None:
    paths = (
        _write_inputs(
            tmp_path
        )
    )

    with pytest.raises(
        RTCanonicalFreezeError,
        match="Invalid calendar freeze_date",
    ):
        freeze_rt_canonical_from_paths(
            candidate_panel_path=(
                paths[
                    "panel"
                ]
            ),
            candidate_source_map_path=(
                paths[
                    "source_map"
                ]
            ),
            evidence_path=(
                paths[
                    "evidence"
                ]
            ),
            decisions_path=(
                paths[
                    "decisions"
                ]
            ),
            output_csv_path=(
                tmp_path
                / "frozen.csv"
            ),
            manifest_path=(
                tmp_path
                / "manifest.json"
            ),
            expected_start="2010-01",
            expected_end="2010-03",
            freeze_date="2026-02-30",
        )