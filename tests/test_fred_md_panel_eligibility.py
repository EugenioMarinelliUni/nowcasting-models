from __future__ import annotations

import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_panel_eligibility import (
    build_eligibility_report,
    build_summary,
)


def _row(
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
    expected_vintages: int = 198,
    vintages_present: int = 198,
    coverage_vintages_present: int = 198,
    min_n_valid: int = 600,
    median_n_valid: float = 612.0,
    min_observed_fraction: float = 0.99,
    max_pct_missing: float = 1.0,
    max_leading_missing: int = 0,
    max_trailing_missing: int = 1,
    max_internal_missing: int = 0,
    vintages_with_internal_missing: int = 0,
    all_missing_vintages: int = 0,
) -> dict[str, object]:
    return {
        "raw_series": raw_series,
        "candidate_rt_stable": candidate,
        "review_status": review_status,
        "include_rt_stable": include_rt_stable,
        "include_rt_canonical": include_rt_canonical,
        "stable_window_start": "2010-01",
        "stable_window_end": "2026-06",
        "stable_window_expected_vintages": expected_vintages,
        "stable_window_vintages_present": vintages_present,
        "present_all_stable_window": present_all,
        "tcodes_seen": tcodes_seen,
        "n_distinct_tcodes": n_distinct_tcodes,
        "n_tcode_changes": n_tcode_changes,
        "coverage_expected_vintages": expected_vintages,
        "coverage_vintages_present": coverage_vintages_present,
        "coverage_min_n_valid": min_n_valid,
        "coverage_median_n_valid": median_n_valid,
        "coverage_min_observed_fraction": min_observed_fraction,
        "coverage_max_pct_missing": max_pct_missing,
        "coverage_max_leading_missing": max_leading_missing,
        "coverage_max_trailing_missing": max_trailing_missing,
        "coverage_max_internal_missing": max_internal_missing,
        "coverage_vintages_with_internal_missing":
            vintages_with_internal_missing,
        "coverage_all_missing_vintages": all_missing_vintages,
        "coverage_earliest_first_valid_date": "1960-01-01",
        "coverage_latest_first_valid_date": "1960-01-01",
        "coverage_earliest_last_valid_date": "2025-12-01",
        "coverage_latest_last_valid_date": "2026-05-01",
    }


def test_ordinary_scope_selects_only_unreviewed_stable_candidates() -> None:
    registry = pd.DataFrame(
        [
            _row("ORDINARY"),
            _row(
                "REVIEWED",
                review_status="reviewed",
                include_rt_stable=True,
            ),
            _row(
                "NONSTABLE",
                candidate=False,
                present_all=False,
            ),
        ]
    )

    report = build_eligibility_report(registry, scope="ordinary")
    assert report["raw_series"].tolist() == ["ORDINARY"]


def test_all_stable_scope_includes_reviewed_and_unreviewed_candidates() -> None:
    registry = pd.DataFrame(
        [
            _row("ORDINARY"),
            _row(
                "REVIEWED",
                review_status="reviewed",
                include_rt_stable=True,
            ),
            _row(
                "NONSTABLE",
                candidate=False,
                present_all=False,
            ),
        ]
    )

    report = build_eligibility_report(registry, scope="all-stable")
    assert set(report["raw_series"]) == {"ORDINARY", "REVIEWED"}


def test_rt_stable_scope_excludes_reviewed_manual_drop() -> None:
    registry = pd.DataFrame(
        [
            _row("ORDINARY"),
            _row(
                "KEEP",
                review_status="reviewed",
                include_rt_stable=True,
            ),
            _row(
                "DROP",
                review_status="reviewed",
                include_rt_stable=False,
            ),
        ]
    )

    report = build_eligibility_report(
        registry,
        scope="rt-stable-candidates",
    )
    assert report["raw_series"].tolist() == ["KEEP", "ORDINARY"]


def test_missingness_is_flagged_but_not_hard_failed() -> None:
    registry = pd.DataFrame(
        [
            _row(
                "RAGGED",
                min_observed_fraction=0.95,
                max_pct_missing=5.0,
                max_leading_missing=12,
                max_trailing_missing=2,
                max_internal_missing=1,
                vintages_with_internal_missing=25,
            )
        ]
    )

    row = build_eligibility_report(registry, scope="ordinary").iloc[0]

    assert bool(row["has_leading_missing"]) is True
    assert bool(row["has_trailing_missing"]) is True
    assert bool(row["has_internal_missing"]) is True
    assert bool(row["has_persistent_internal_missing"]) is True
    assert bool(row["qc_hard_fail"]) is False
    assert row["qc_status"] == "structurally_clean_with_missingness_flags"

    flags = set(str(row["qc_flags"]).split("|"))
    assert {
        "leading_missing",
        "trailing_missing",
        "internal_missing",
        "persistent_internal_missing",
        "incomplete_history",
    }.issubset(flags)


def test_all_missing_vintage_is_structural_hard_failure() -> None:
    registry = pd.DataFrame(
        [
            _row(
                "ALL_MISSING",
                min_n_valid=0,
                min_observed_fraction=0.0,
                max_pct_missing=100.0,
                all_missing_vintages=1,
            )
        ]
    )

    row = build_eligibility_report(registry, scope="ordinary").iloc[0]
    assert bool(row["qc_hard_fail"]) is True
    assert row["qc_status"] == "hard_fail"

    reasons = set(str(row["hard_failure_reasons"]).split("|"))
    assert "all_missing_vintage" in reasons
    assert "no_valid_observations_in_worst_vintage" in reasons


def test_tcode_change_is_structural_hard_failure() -> None:
    registry = pd.DataFrame(
        [
            _row(
                "TCODE_CHANGE",
                tcodes_seen="5|2",
                n_distinct_tcodes=2,
                n_tcode_changes=1,
            )
        ]
    )

    row = build_eligibility_report(registry, scope="ordinary").iloc[0]
    assert bool(row["qc_hard_fail"]) is True

    reasons = set(str(row["hard_failure_reasons"]).split("|"))
    assert "missing_or_invalid_unique_tcode" in reasons
    assert "multiple_or_missing_tcodes" in reasons
    assert "tcode_changes" in reasons


def test_invalid_unique_tcode_is_structural_hard_failure() -> None:
    registry = pd.DataFrame(
        [
            _row(
                "BAD_TCODE",
                tcodes_seen="99",
                n_distinct_tcodes=1,
                n_tcode_changes=0,
            )
        ]
    )

    row = build_eligibility_report(registry, scope="ordinary").iloc[0]
    assert bool(row["qc_hard_fail"]) is True
    assert "missing_or_invalid_unique_tcode" in str(
        row["hard_failure_reasons"]
    )


def test_presence_count_mismatch_is_structural_hard_failure() -> None:
    registry = pd.DataFrame(
        [
            _row(
                "PRESENCE_MISMATCH",
                expected_vintages=198,
                vintages_present=197,
                coverage_vintages_present=197,
            )
        ]
    )

    row = build_eligibility_report(registry, scope="ordinary").iloc[0]
    assert bool(row["qc_hard_fail"]) is True

    reasons = set(str(row["hard_failure_reasons"]).split("|"))
    assert "stable_window_presence_count_mismatch" in reasons
    assert "coverage_vintage_count_mismatch" in reasons


def test_reviewed_manual_decision_is_preserved_only_for_reviewed_rows() -> None:
    registry = pd.DataFrame(
        [
            _row(
                "ORDINARY",
                review_status="unreviewed",
                include_rt_stable=False,
            ),
            _row(
                "KEEP",
                review_status="reviewed",
                include_rt_stable=True,
                include_rt_canonical=True,
            ),
            _row(
                "DROP",
                review_status="reviewed",
                include_rt_stable=False,
                include_rt_canonical=False,
            ),
        ]
    )

    report = build_eligibility_report(
        registry,
        scope="all-stable",
    ).set_index("raw_series")

    assert report.loc["ORDINARY", "manual_include_rt_stable"] == ""
    assert bool(report.loc["KEEP", "manual_include_rt_stable"]) is True
    assert bool(report.loc["KEEP", "manual_include_rt_canonical"]) is True
    assert bool(report.loc["DROP", "manual_include_rt_stable"]) is False


def test_summary_counts_candidate_review_states_and_qc_results() -> None:
    registry = pd.DataFrame(
        [
            _row("A"),
            _row(
                "B",
                review_status="reviewed",
                include_rt_stable=True,
            ),
            _row(
                "C",
                candidate=False,
                present_all=False,
            ),
        ]
    )

    report = build_eligibility_report(registry, scope="all-stable")
    summary = build_summary(
        registry,
        report,
        scope="all-stable",
    ).iloc[0]

    assert int(summary["n_registry_series"]) == 3
    assert int(summary["n_candidate_rt_stable"]) == 2
    assert int(summary["n_candidate_reviewed"]) == 1
    assert int(summary["n_candidate_unreviewed"]) == 1
    assert int(summary["n_rows_in_qc_report"]) == 2
    assert int(summary["n_structural_hard_fail"]) == 0
    assert int(summary["n_structurally_clean"]) == 2


def test_unknown_boolean_value_is_rejected() -> None:
    bad_row = _row("A")
    bad_row["candidate_rt_stable"] = "maybe"
    registry = pd.DataFrame([bad_row])

    with pytest.raises(
        ValueError,
        match="cannot parse candidate_rt_stable",
    ):
        build_eligibility_report(registry, scope="ordinary")
