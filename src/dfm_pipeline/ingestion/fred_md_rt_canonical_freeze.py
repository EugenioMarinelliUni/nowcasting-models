from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical import (
    build_transition_boundaries,
    validate_rt_canonical_specification,
)
from dfm_pipeline.ingestion.fred_md_rt_canonical_decisions import (
    ACCEPTED_REVIEW_DECISIONS,
    assert_rt_canonical_freezable,
)


FROZEN_SCHEMA_VERSION = "1.0"

PANEL_COLUMNS = [
    "panel_position",
    "canonical_id",
    "mapping_type",
    "n_source_segments",
    "stable_window_start",
    "stable_window_end",
    "final_include",
    "inclusion_reason",
]

SOURCE_MAP_COLUMNS = [
    "canonical_id",
    "source_order",
    "source_series",
    "expected_tcode",
    "active_from_vintage",
    "active_to_vintage",
    "mapping_action",
    "relationship_type",
    "equivalence_level",
    "documented_change_vintage",
    "source_origin",
    "notes",
]

EVIDENCE_TRANSITION_COLUMNS = [
    "canonical_id",
    "old_source",
    "new_source",
    "transition_vintage",
    "relationship_type",
    "equivalence_level",
    "documentary_review_complete",
]

FROZEN_COLUMNS = [
    "panel_position",
    "canonical_id",
    "mapping_type",
    "n_source_segments",
    "stable_window_start",
    "stable_window_end",
    "final_include",
    "inclusion_reason",
    "source_order",
    "source_series",
    "expected_tcode",
    "active_from_vintage",
    "active_to_vintage",
    "mapping_action",
    "relationship_type",
    "equivalence_level",
    "documented_change_vintage",
    "source_origin",
    "source_notes",
    "transition_review_required",
    "documentary_review_complete",
    "review_decision",
    "decision_rationale",
    "decision_reviewer",
    "decision_review_date",
]

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


class RTCanonicalFreezeError(
    ValueError
):
    """The RT_CANONICAL candidate cannot be frozen safely."""


def _clean_text(
    value: object,
) -> str:
    if value is None or pd.isna(value):
        return ""

    return str(value).strip()


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
        raise RTCanonicalFreezeError(
            f"{name} is missing required columns: "
            + ", ".join(missing)
        )


def _parse_bool(
    value: object,
    *,
    field_name: str,
) -> bool:
    if isinstance(value, bool):
        return value

    text = _clean_text(
        value
    ).lower()

    if text in _TRUE_VALUES:
        return True

    if text in _FALSE_VALUES:
        return False

    raise RTCanonicalFreezeError(
        f"Invalid boolean value for "
        f"{field_name}: {value!r}"
    )


def _integer_series(
    series: pd.Series,
    *,
    field_name: str,
) -> pd.Series:
    values = pd.to_numeric(
        series,
        errors="coerce",
    )

    if values.isna().any():
        raise RTCanonicalFreezeError(
            f"{field_name} contains non-numeric values"
        )

    non_integer = (
        values
        .mod(1)
        .ne(0)
    )

    if non_integer.any():
        raise RTCanonicalFreezeError(
            f"{field_name} contains non-integer values"
        )

    return values.astype(
        int
    )


def _validate_month(
    value: object,
    *,
    field_name: str,
) -> str:
    text = _clean_text(
        value
    )

    if not text:
        raise RTCanonicalFreezeError(
            f"{field_name} is blank"
        )

    try:
        period = pd.Period(
            text,
            freq="M",
        )
    except Exception as exc:
        raise RTCanonicalFreezeError(
            f"Invalid {field_name}: {value!r}"
        ) from exc

    normalized = str(
        period
    )

    if normalized != text:
        raise RTCanonicalFreezeError(
            f"{field_name} must use YYYY-MM format: "
            f"{value!r}"
        )

    return normalized


def _validate_freeze_date(
    value: object,
) -> str:
    text = _clean_text(
        value
    )

    try:
        parsed = datetime.strptime(
            text,
            "%Y-%m-%d",
        )
    except ValueError as exc:
        raise RTCanonicalFreezeError(
            "Invalid calendar freeze_date "
            f"{value!r}; expected YYYY-MM-DD"
        ) from exc

    return parsed.date().isoformat()


def sha256_file(
    path: str | Path,
) -> str:
    path = Path(
        path
    )

    digest = hashlib.sha256()

    with path.open(
        "rb"
    ) as handle:
        for chunk in iter(
            lambda: handle.read(
                1024 * 1024
            ),
            b"",
        ):
            digest.update(
                chunk
            )

    return digest.hexdigest()


def _normalize_panel(
    panel: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(
        panel,
        PANEL_COLUMNS,
        name="RT_CANONICAL candidate panel",
    )

    out = panel.loc[
        :,
        PANEL_COLUMNS,
    ].copy()

    out[
        "canonical_id"
    ] = out[
        "canonical_id"
    ].map(
        _clean_text
    )

    out[
        "mapping_type"
    ] = out[
        "mapping_type"
    ].map(
        _clean_text
    )

    out[
        "stable_window_start"
    ] = out[
        "stable_window_start"
    ].map(
        lambda value: _validate_month(
            value,
            field_name="stable_window_start",
        )
    )

    out[
        "stable_window_end"
    ] = out[
        "stable_window_end"
    ].map(
        lambda value: _validate_month(
            value,
            field_name="stable_window_end",
        )
    )

    out[
        "inclusion_reason"
    ] = out[
        "inclusion_reason"
    ].map(
        _clean_text
    )

    out[
        "panel_position"
    ] = _integer_series(
        out[
            "panel_position"
        ],
        field_name="panel_position",
    )

    out[
        "n_source_segments"
    ] = _integer_series(
        out[
            "n_source_segments"
        ],
        field_name="n_source_segments",
    )

    out[
        "final_include"
    ] = out[
        "final_include"
    ].map(
        lambda value: _parse_bool(
            value,
            field_name="final_include",
        )
    )

    if not out[
        "final_include"
    ].all():
        excluded = out.loc[
            ~out[
                "final_include"
            ],
            "canonical_id",
        ].tolist()

        raise RTCanonicalFreezeError(
            "RT_CANONICAL freeze requires every "
            "candidate concept to have final_include=True. "
            f"Non-included concepts: {sorted(excluded)}"
        )

    return (
        out
        .sort_values(
            "panel_position"
        )
        .reset_index(
            drop=True
        )
    )


def _normalize_source_map(
    source_map: pd.DataFrame,
) -> pd.DataFrame:
    _require_columns(
        source_map,
        SOURCE_MAP_COLUMNS,
        name="RT_CANONICAL candidate source map",
    )

    out = source_map.loc[
        :,
        SOURCE_MAP_COLUMNS,
    ].copy()

    text_columns = [
        "canonical_id",
        "source_series",
        "active_from_vintage",
        "active_to_vintage",
        "mapping_action",
        "relationship_type",
        "equivalence_level",
        "documented_change_vintage",
        "source_origin",
        "notes",
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
        "source_order"
    ] = _integer_series(
        out[
            "source_order"
        ],
        field_name="source_order",
    )

    out[
        "expected_tcode"
    ] = _integer_series(
        out[
            "expected_tcode"
        ],
        field_name="expected_tcode",
    )

    return out.reset_index(
        drop=True
    )


def _validate_common_window(
    panel: pd.DataFrame,
    *,
    expected_start: str,
    expected_end: str,
) -> tuple[str, str]:
    expected_start = _validate_month(
        expected_start,
        field_name="expected_start",
    )

    expected_end = _validate_month(
        expected_end,
        field_name="expected_end",
    )

    starts = set(
        panel[
            "stable_window_start"
        ]
    )

    ends = set(
        panel[
            "stable_window_end"
        ]
    )

    if len(
        starts
    ) != 1 or len(
        ends
    ) != 1:
        raise RTCanonicalFreezeError(
            "RT_CANONICAL candidate does not have "
            "one common stable window"
        )

    actual_start = next(
        iter(
            starts
        )
    )

    actual_end = next(
        iter(
            ends
        )
    )

    if (
        actual_start
        != expected_start
        or actual_end
        != expected_end
    ):
        raise RTCanonicalFreezeError(
            "Candidate stable window "
            f"{actual_start}..{actual_end} does not match "
            "expected freeze window "
            f"{expected_start}..{expected_end}"
        )

    return (
        actual_start,
        actual_end,
    )


def _validate_transition_identity(
    *,
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    evidence: pd.DataFrame,
    validated_decisions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Ensure that the transition reviewed in C3/C4 is exactly the
    transition contained in the candidate being frozen.

    This prevents a stale evidence/decision artifact from being used
    after somebody has changed a predecessor, successor, transition
    vintage, relationship type, or equivalence level in the candidate.
    """
    _require_columns(
        evidence,
        EVIDENCE_TRANSITION_COLUMNS,
        name="RT_CANONICAL evidence",
    )

    switch_ids = set(
        panel.loc[
            panel[
                "mapping_type"
            ].eq(
                "switch_by_vintage"
            ),
            "canonical_id",
        ]
    )

    decision_ids = set(
        validated_decisions[
            "canonical_id"
        ]
    )

    if (
        switch_ids
        != decision_ids
    ):
        raise RTCanonicalFreezeError(
            "Switch concept set does not match "
            "decision/evidence review set. "
            f"Candidate-only: "
            f"{sorted(switch_ids - decision_ids)}; "
            f"review-only: "
            f"{sorted(decision_ids - switch_ids)}"
        )

    boundaries = (
        build_transition_boundaries(
            panel,
            source_map,
        )
    )

    if len(
        boundaries
    ) != len(
        switch_ids
    ):
        raise RTCanonicalFreezeError(
            "Current freeze schema requires exactly one "
            "reviewed transition boundary per "
            "switch_by_vintage concept. "
            f"Found {len(boundaries)} boundaries for "
            f"{len(switch_ids)} switch concepts."
        )

    boundary_ids = set(
        boundaries[
            "canonical_id"
        ].astype(
            str
        )
    )

    if (
        boundary_ids
        != switch_ids
    ):
        raise RTCanonicalFreezeError(
            "Transition boundary concepts differ from "
            "switch_by_vintage concepts"
        )

    evidence_part = evidence.loc[
        :,
        EVIDENCE_TRANSITION_COLUMNS,
    ].copy()

    for column in [
        "canonical_id",
        "old_source",
        "new_source",
        "transition_vintage",
        "relationship_type",
        "equivalence_level",
    ]:
        evidence_part[
            column
        ] = evidence_part[
            column
        ].map(
            _clean_text
        )

    if evidence_part[
        "canonical_id"
    ].duplicated().any():
        raise RTCanonicalFreezeError(
            "Evidence contains duplicate canonical_id rows"
        )

    evidence_by_id = (
        evidence_part
        .set_index(
            "canonical_id"
        )
    )

    boundary_by_id = (
        boundaries
        .set_index(
            "canonical_id"
        )
    )

    fields = [
        "old_source",
        "new_source",
        "transition_vintage",
        "relationship_type",
        "equivalence_level",
    ]

    for canonical_id in sorted(
        switch_ids
    ):
        if (
            canonical_id
            not in evidence_by_id.index
        ):
            raise RTCanonicalFreezeError(
                f"{canonical_id}: missing transition "
                "evidence row"
            )

        boundary = boundary_by_id.loc[
            canonical_id
        ]

        evidence_row = evidence_by_id.loc[
            canonical_id
        ]

        for field in fields:
            candidate_value = _clean_text(
                boundary[
                    field
                ]
            )

            evidence_value = _clean_text(
                evidence_row[
                    field
                ]
            )

            if (
                candidate_value
                != evidence_value
            ):
                raise RTCanonicalFreezeError(
                    f"{canonical_id}: reviewed transition "
                    f"{field} does not match candidate: "
                    f"{evidence_value!r} != "
                    f"{candidate_value!r}"
                )

    return (
        boundaries
        .sort_values(
            [
                "canonical_id",
                "transition_vintage",
            ]
        )
        .reset_index(
            drop=True
        )
    )


def validate_rt_canonical_freeze_inputs(
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    decisions: pd.DataFrame,
    evidence: pd.DataFrame,
    *,
    expected_start: str = "2010-01",
    expected_end: str = "2026-06",
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """
    Apply all structural and decision-level checks required before
    materializing the frozen RT_CANONICAL specification.
    """
    try:
        validate_rt_canonical_specification(
            panel,
            source_map,
        )
    except Exception as exc:
        raise RTCanonicalFreezeError(
            "RT_CANONICAL candidate specification "
            f"validation failed: {exc}"
        ) from exc

    normalized_panel = (
        _normalize_panel(
            panel
        )
    )

    normalized_source_map = (
        _normalize_source_map(
            source_map
        )
    )

    _validate_common_window(
        normalized_panel,
        expected_start=expected_start,
        expected_end=expected_end,
    )

    validated_decisions = (
        assert_rt_canonical_freezable(
            decisions,
            evidence=evidence,
        )
    )

    nonaccepted = (
        validated_decisions.loc[
            ~validated_decisions[
                "review_decision"
            ].isin(
                ACCEPTED_REVIEW_DECISIONS
            ),
            [
                "canonical_id",
                "review_decision",
            ],
        ]
    )

    if not nonaccepted.empty:
        raise RTCanonicalFreezeError(
            "Freeze gate returned decision values "
            "that are not accepted transition outcomes: "
            + nonaccepted.to_dict(
                orient="records"
            ).__repr__()
        )

    boundaries = (
        _validate_transition_identity(
            panel=normalized_panel,
            source_map=normalized_source_map,
            evidence=evidence,
            validated_decisions=validated_decisions,
        )
    )

    return (
        normalized_panel,
        normalized_source_map,
        validated_decisions,
        boundaries,
    )


def _build_frozen_from_validated(
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    validated_decisions: pd.DataFrame,
) -> pd.DataFrame:
    decisions_by_id = (
        validated_decisions
        .set_index(
            "canonical_id"
        )
    )

    rows: list[
        dict[str, Any]
    ] = []

    panel_sorted = (
        panel
        .sort_values(
            "panel_position"
        )
    )

    for _, panel_row in (
        panel_sorted.iterrows()
    ):
        canonical_id = str(
            panel_row[
                "canonical_id"
            ]
        )

        segments = (
            source_map.loc[
                source_map[
                    "canonical_id"
                ].eq(
                    canonical_id
                )
            ]
            .sort_values(
                "source_order"
            )
        )

        transition_review_required = (
            panel_row[
                "mapping_type"
            ]
            == "switch_by_vintage"
        )

        if transition_review_required:
            decision = decisions_by_id.loc[
                canonical_id
            ]

            documentary_complete: object = bool(
                decision[
                    "documentary_review_complete"
                ]
            )

            review_decision = str(
                decision[
                    "review_decision"
                ]
            )

            rationale = str(
                decision[
                    "rationale"
                ]
            )

            reviewer = str(
                decision[
                    "reviewer"
                ]
            )

            review_date = str(
                decision[
                    "review_date"
                ]
            )

        else:
            documentary_complete = ""
            review_decision = ""
            rationale = ""
            reviewer = ""
            review_date = ""

        for _, source_row in (
            segments.iterrows()
        ):
            rows.append(
                {
                    "panel_position": int(
                        panel_row[
                            "panel_position"
                        ]
                    ),
                    "canonical_id": (
                        canonical_id
                    ),
                    "mapping_type": (
                        panel_row[
                            "mapping_type"
                        ]
                    ),
                    "n_source_segments": int(
                        panel_row[
                            "n_source_segments"
                        ]
                    ),
                    "stable_window_start": (
                        panel_row[
                            "stable_window_start"
                        ]
                    ),
                    "stable_window_end": (
                        panel_row[
                            "stable_window_end"
                        ]
                    ),
                    "final_include": bool(
                        panel_row[
                            "final_include"
                        ]
                    ),
                    "inclusion_reason": (
                        panel_row[
                            "inclusion_reason"
                        ]
                    ),
                    "source_order": int(
                        source_row[
                            "source_order"
                        ]
                    ),
                    "source_series": (
                        source_row[
                            "source_series"
                        ]
                    ),
                    "expected_tcode": int(
                        source_row[
                            "expected_tcode"
                        ]
                    ),
                    "active_from_vintage": (
                        source_row[
                            "active_from_vintage"
                        ]
                    ),
                    "active_to_vintage": (
                        source_row[
                            "active_to_vintage"
                        ]
                    ),
                    "mapping_action": (
                        source_row[
                            "mapping_action"
                        ]
                    ),
                    "relationship_type": (
                        source_row[
                            "relationship_type"
                        ]
                    ),
                    "equivalence_level": (
                        source_row[
                            "equivalence_level"
                        ]
                    ),
                    "documented_change_vintage": (
                        source_row[
                            "documented_change_vintage"
                        ]
                    ),
                    "source_origin": (
                        source_row[
                            "source_origin"
                        ]
                    ),
                    "source_notes": (
                        source_row[
                            "notes"
                        ]
                    ),
                    "transition_review_required": bool(
                        transition_review_required
                    ),
                    "documentary_review_complete": (
                        documentary_complete
                    ),
                    "review_decision": (
                        review_decision
                    ),
                    "decision_rationale": (
                        rationale
                    ),
                    "decision_reviewer": (
                        reviewer
                    ),
                    "decision_review_date": (
                        review_date
                    ),
                }
            )

    frozen = pd.DataFrame(
        rows,
        columns=FROZEN_COLUMNS,
    )

    if len(
        frozen
    ) != len(
        source_map
    ):
        raise RTCanonicalFreezeError(
            "Frozen artifact row count differs from "
            "candidate source-map row count"
        )

    if set(
        frozen[
            "canonical_id"
        ]
    ) != set(
        panel[
            "canonical_id"
        ]
    ):
        raise RTCanonicalFreezeError(
            "Frozen artifact concept set differs from "
            "candidate concept set"
        )

    return frozen.reset_index(
        drop=True
    )


def build_frozen_rt_canonical_specification(
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    decisions: pd.DataFrame,
    evidence: pd.DataFrame,
    *,
    expected_start: str = "2010-01",
    expected_end: str = "2026-06",
) -> pd.DataFrame:
    """
    Build the immutable source-segment representation of the reviewed
    RT_CANONICAL candidate.

    No source selection, thresholding, or statistical decision rule is
    executed here.
    """
    (
        normalized_panel,
        normalized_source_map,
        validated_decisions,
        _,
    ) = validate_rt_canonical_freeze_inputs(
        panel,
        source_map,
        decisions,
        evidence,
        expected_start=expected_start,
        expected_end=expected_end,
    )

    return _build_frozen_from_validated(
        normalized_panel,
        normalized_source_map,
        validated_decisions,
    )


def _manifest_path_text(
    path: Path,
) -> str:
    return path.as_posix()


def _build_manifest(
    *,
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    validated_decisions: pd.DataFrame,
    boundaries: pd.DataFrame,
    frozen: pd.DataFrame,
    freeze_date: str,
    candidate_panel_path: Path,
    candidate_source_map_path: Path,
    evidence_path: Path,
    decisions_path: Path,
    output_csv_path: Path,
    output_csv_sha256: str,
) -> dict[str, Any]:
    freeze_date = (
        _validate_freeze_date(
            freeze_date
        )
    )

    stable_start = str(
        panel[
            "stable_window_start"
        ].iloc[0]
    )

    stable_end = str(
        panel[
            "stable_window_end"
        ].iloc[0]
    )

    n_direct = int(
        panel[
            "mapping_type"
        ].eq(
            "direct"
        ).sum()
    )

    n_switch = int(
        panel[
            "mapping_type"
        ].eq(
            "switch_by_vintage"
        ).sum()
    )

    decision_counts = (
        validated_decisions[
            "review_decision"
        ]
        .value_counts()
        .sort_index()
    )

    decision_distribution = {
        str(
            decision
        ): int(
            count
        )
        for decision, count
        in decision_counts.items()
    }

    input_paths = {
        "candidate_panel": (
            candidate_panel_path
        ),
        "candidate_source_map": (
            candidate_source_map_path
        ),
        "transition_evidence": (
            evidence_path
        ),
        "transition_decisions": (
            decisions_path
        ),
    }

    inputs: dict[
        str,
        dict[str, Any],
    ] = {}

    for name, path in (
        input_paths.items()
    ):
        inputs[
            name
        ] = {
            "path": (
                _manifest_path_text(
                    path
                )
            ),
            "sha256": (
                sha256_file(
                    path
                )
            ),
        }

    return {
        "schema_version": (
            FROZEN_SCHEMA_VERSION
        ),
        "artifact_type": (
            "fred_md_rt_canonical_frozen_specification"
        ),
        "freeze_date": (
            freeze_date
        ),
        "generator": (
            "dfm_pipeline.ingestion."
            "fred_md_rt_canonical_freeze"
        ),
        "stable_window": {
            "start": (
                stable_start
            ),
            "end": (
                stable_end
            ),
        },
        "counts": {
            "canonical_concepts": int(
                len(
                    panel
                )
            ),
            "direct_concepts": (
                n_direct
            ),
            "switch_by_vintage_concepts": (
                n_switch
            ),
            "source_segments": int(
                len(
                    source_map
                )
            ),
            "transition_boundaries": int(
                len(
                    boundaries
                )
            ),
            "reviewed_switch_concepts": int(
                len(
                    validated_decisions
                )
            ),
        },
        "decision_distribution": (
            decision_distribution
        ),
        "freeze_gate": {
            "passed": True,
            "candidate_structure_validated": True,
            "candidate_transition_identity_verified": True,
            "decision_level_gate_passed": True,
            "unresolved_concepts": [],
            "excluded_concepts": [],
        },
        "inputs": (
            inputs
        ),
        "output": {
            "frozen_csv": {
                "path": (
                    _manifest_path_text(
                        output_csv_path
                    )
                ),
                "sha256": (
                    output_csv_sha256
                ),
                "rows": int(
                    len(
                        frozen
                    )
                ),
                "columns": int(
                    len(
                        frozen.columns
                    )
                ),
                "column_names": (
                    list(
                        frozen.columns
                    )
                ),
            },
        },
        "notes": [
            (
                "The frozen specification preserves "
                "the reviewed candidate source segments "
                "without re-estimating or applying "
                "statistical acceptance thresholds."
            ),
            (
                "Transition decisions are attached only "
                "to switch_by_vintage concepts; direct "
                "concepts inherit the frozen RT_STABLE "
                "specification."
            ),
            (
                "External C4 reference series remain "
                "review evidence and are not substituted "
                "for FRED-MD historical-vintage model "
                "inputs."
            ),
        ],
    }


def freeze_rt_canonical_from_paths(
    *,
    candidate_panel_path: str | Path,
    candidate_source_map_path: str | Path,
    evidence_path: str | Path,
    decisions_path: str | Path,
    output_csv_path: str | Path,
    manifest_path: str | Path,
    expected_start: str = "2010-01",
    expected_end: str = "2026-06",
    freeze_date: str,
    overwrite: bool = False,
) -> tuple[
    Path,
    Path,
    dict[str, Any],
]:
    """
    Validate and materialize the frozen RT_CANONICAL specification and
    provenance manifest.

    Both output files are refused by default if either already exists.
    """
    freeze_date = (
        _validate_freeze_date(
            freeze_date
        )
    )

    candidate_panel_path = Path(
        candidate_panel_path
    )

    candidate_source_map_path = Path(
        candidate_source_map_path
    )

    evidence_path = Path(
        evidence_path
    )

    decisions_path = Path(
        decisions_path
    )

    output_csv_path = Path(
        output_csv_path
    )

    manifest_path = Path(
        manifest_path
    )

    existing = [
        path
        for path in [
            output_csv_path,
            manifest_path,
        ]
        if path.exists()
    ]

    if (
        existing
        and not overwrite
    ):
        raise RTCanonicalFreezeError(
            "Refusing to overwrite existing freeze "
            "artifact(s): "
            + ", ".join(
                str(
                    path
                )
                for path in existing
            )
        )

    panel = pd.read_csv(
        candidate_panel_path,
        dtype=str,
        keep_default_na=False,
    )

    source_map = pd.read_csv(
        candidate_source_map_path,
        dtype=str,
        keep_default_na=False,
    )

    evidence = pd.read_csv(
        evidence_path,
        dtype=str,
        keep_default_na=False,
    )

    decisions = pd.read_csv(
        decisions_path,
        dtype=str,
        keep_default_na=False,
    )

    (
        normalized_panel,
        normalized_source_map,
        validated_decisions,
        boundaries,
    ) = validate_rt_canonical_freeze_inputs(
        panel,
        source_map,
        decisions,
        evidence,
        expected_start=expected_start,
        expected_end=expected_end,
    )

    frozen = (
        _build_frozen_from_validated(
            normalized_panel,
            normalized_source_map,
            validated_decisions,
        )
    )

    output_csv_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    manifest_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_csv = (
        output_csv_path.with_name(
            f".{output_csv_path.name}.tmp"
        )
    )

    temporary_manifest = (
        manifest_path.with_name(
            f".{manifest_path.name}.tmp"
        )
    )

    for temporary in [
        temporary_csv,
        temporary_manifest,
    ]:
        if temporary.exists():
            temporary.unlink()

    try:
        with temporary_csv.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            frozen.to_csv(
                handle,
                index=False,
                lineterminator="\n",
            )

        frozen_sha256 = (
            sha256_file(
                temporary_csv
            )
        )

        manifest = (
            _build_manifest(
                panel=normalized_panel,
                source_map=normalized_source_map,
                validated_decisions=(
                    validated_decisions
                ),
                boundaries=boundaries,
                frozen=frozen,
                freeze_date=freeze_date,
                candidate_panel_path=(
                    candidate_panel_path
                ),
                candidate_source_map_path=(
                    candidate_source_map_path
                ),
                evidence_path=evidence_path,
                decisions_path=decisions_path,
                output_csv_path=output_csv_path,
                output_csv_sha256=(
                    frozen_sha256
                ),
            )
        )

        with temporary_manifest.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            json.dump(
                manifest,
                handle,
                indent=2,
                sort_keys=True,
            )

            handle.write(
                "\n"
            )

        temporary_csv.replace(
            output_csv_path
        )

        temporary_manifest.replace(
            manifest_path
        )

    finally:
        for temporary in [
            temporary_csv,
            temporary_manifest,
        ]:
            if temporary.exists():
                temporary.unlink()

    return (
        output_csv_path,
        manifest_path,
        manifest,
    )


__all__ = [
    "FROZEN_SCHEMA_VERSION",
    "FROZEN_COLUMNS",
    "RTCanonicalFreezeError",
    "sha256_file",
    "validate_rt_canonical_freeze_inputs",
    "build_frozen_rt_canonical_specification",
    "freeze_rt_canonical_from_paths",
]