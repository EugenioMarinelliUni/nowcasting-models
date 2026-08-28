from __future__ import annotations

import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_panel_selection import (
    parse_registry_bool,
    select_panel_candidates,
    single_stable_tcode,
)


def _registry_row(
    raw_series: str,
    *,
    candidate: bool = True,
    review_status: str = "unreviewed",
    include_rt_stable: bool = False,
    include_rt_canonical: bool = False,
    present_all: bool = True,
    tcodes_seen: str = "5",
    n_distinct_tcodes: int = 1,
    n_tcode_changes: int = 0,
) -> dict[str, object]:
    return {
        "raw_series": raw_series,
        "candidate_rt_stable": candidate,
        "review_status": review_status,
        "include_rt_stable": include_rt_stable,
        "include_rt_canonical": include_rt_canonical,
        "present_all_stable_window": present_all,
        "tcodes_seen": tcodes_seen,
        "n_distinct_tcodes": n_distinct_tcodes,
        "n_tcode_changes": n_tcode_changes,
    }


def test_rt_stable_candidates_add_reviewed_keeps_but_not_reviewed_drops() -> None:
    registry = pd.DataFrame(
        [
            _registry_row("ORDINARY"),
            _registry_row(
                "SPECIAL_KEEP",
                review_status="reviewed",
                include_rt_stable=True,
            ),
            _registry_row(
                "SPECIAL_DROP",
                review_status="reviewed",
                include_rt_stable=False,
            ),
            _registry_row("NONSTABLE", candidate=False, present_all=False),
        ]
    )

    selected = select_panel_candidates(
        registry,
        scope="rt-stable-candidates",
    )

    assert selected["raw_series"].tolist() == [
        "ORDINARY",
        "SPECIAL_KEEP",
    ]
    classes = selected.set_index("raw_series")["selection_class"]
    assert classes["ORDINARY"] == "ordinary_stable"
    assert classes["SPECIAL_KEEP"] == "reviewed_stable_keep"


def test_ordinary_and_all_stable_scopes_remain_distinct() -> None:
    registry = pd.DataFrame(
        [
            _registry_row("A"),
            _registry_row("B", review_status="reviewed", include_rt_stable=True),
            _registry_row("C", review_status="reviewed", include_rt_stable=False),
        ]
    )

    ordinary = select_panel_candidates(registry, scope="ordinary")
    all_stable = select_panel_candidates(registry, scope="all-stable")

    assert ordinary["raw_series"].tolist() == ["A"]
    assert set(all_stable["raw_series"]) == {"A", "B", "C"}


def test_single_stable_tcode_requires_exactly_one_valid_code() -> None:
    assert single_stable_tcode("5", 1) == 5
    assert single_stable_tcode("2|5", 2) is None
    assert single_stable_tcode("99", 1) is None
    assert single_stable_tcode("", 1) is None


def test_unknown_registry_boolean_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot parse include_rt_stable"):
        parse_registry_bool(
            "maybe",
            field="include_rt_stable",
            raw_series="A",
        )
