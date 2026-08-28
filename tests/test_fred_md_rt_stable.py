from __future__ import annotations

import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_rt_stable import (
    build_rt_stable_specification,
    build_rt_stable_summary,
)


def _registry() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "raw_series": "B_SERIES",
                "canonical_id": "B_SERIES",
                "candidate_rt_stable": "True",
                "review_status": "reviewed",
                "include_rt_stable": "True",
                "include_rt_canonical": "True",
                "present_all_stable_window": "True",
                "tcodes_seen": "2",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
            },
            {
                "raw_series": "A_SERIES",
                "canonical_id": "A_SERIES",
                "candidate_rt_stable": "True",
                "review_status": "unreviewed",
                "include_rt_stable": "False",
                "include_rt_canonical": "False",
                "present_all_stable_window": "True",
                "tcodes_seen": "5",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
            },
            {
                "raw_series": "C_EXCLUDED",
                "canonical_id": "C_EXCLUDED",
                "candidate_rt_stable": "True",
                "review_status": "reviewed",
                "include_rt_stable": "False",
                "include_rt_canonical": "False",
                "present_all_stable_window": "True",
                "tcodes_seen": "1",
                "n_distinct_tcodes": "1",
                "n_tcode_changes": "0",
            },
        ]
    )


def _stage1() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "raw_series": "B_SERIES",
                "selection_class":
                    "reviewed_stable_keep",
                "tcode": "2",
                "stable_window_start": "2010-01",
                "stable_window_end": "2026-06",
                "stable_window_expected_vintages": "198",
                "coverage_min_n_valid": "392",
                "coverage_min_observed_fraction": "0.50",
                "qc_hard_fail": "False",
                "qc_status":
                    "structurally_clean_with_missingness_flags",
                "qc_flags":
                    "leading_missing|incomplete_history",
            },
            {
                "raw_series": "A_SERIES",
                "selection_class":
                    "ordinary_stable",
                "tcode": "5",
                "stable_window_start": "2010-01",
                "stable_window_end": "2026-06",
                "stable_window_expected_vintages": "198",
                "coverage_min_n_valid": "610",
                "coverage_min_observed_fraction": "0.99",
                "qc_hard_fail": "False",
                "qc_status": "structurally_clean",
                "qc_flags": "",
            },
        ]
    )


def _stage2() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "raw_series": "A_SERIES",
                "selection_class": "ordinary_stable",
                "registry_tcode": "5",
                "embedded_tcodes_seen": "5",
                "n_vintages_checked": "198",
                "n_vintages_hard_fail": "0",
                "min_transformed_n_valid": "609",
                "min_transformed_observed_fraction":
                    "0.989",
                "min_transformed_std": "0.002",
                "stage2_hard_fail": "False",
                "stage2_status": "pass",
                "stage2_flags": "",
            },
            {
                "raw_series": "B_SERIES",
                "selection_class":
                    "reviewed_stable_keep",
                "registry_tcode": "2",
                "embedded_tcodes_seen": "2",
                "n_vintages_checked": "198",
                "n_vintages_hard_fail": "0",
                "min_transformed_n_valid": "391",
                "min_transformed_observed_fraction":
                    "0.495",
                "min_transformed_std": "0.035",
                "stage2_hard_fail": "False",
                "stage2_status": "pass",
                "stage2_flags": "",
            },
        ]
    )


def test_freezes_only_final_candidates() -> None:
    out = build_rt_stable_specification(
        _registry(),
        _stage1(),
        _stage2(),
    )

    assert out["raw_series"].tolist() == [
        "A_SERIES",
        "B_SERIES",
    ]

    assert out["panel_position"].tolist() == [
        1,
        2,
    ]

    assert out["selection_class"].tolist() == [
        "ordinary_stable",
        "reviewed_stable_keep",
    ]

    assert out["final_include"].all()


def test_reviewed_exclusion_does_not_enter() -> None:
    out = build_rt_stable_specification(
        _registry(),
        _stage1(),
        _stage2(),
    )

    assert (
        "C_EXCLUDED"
        not in set(out["raw_series"])
    )


def test_stage1_hard_failure_blocks_freeze() -> None:
    stage1 = _stage1()

    stage1.loc[
        stage1["raw_series"].eq("A_SERIES"),
        "qc_hard_fail",
    ] = "True"

    with pytest.raises(
        ValueError,
        match="Stage-1 hard failures",
    ):
        build_rt_stable_specification(
            _registry(),
            stage1,
            _stage2(),
        )


def test_stage2_hard_failure_blocks_freeze() -> None:
    stage2 = _stage2()

    stage2.loc[
        stage2["raw_series"].eq("A_SERIES"),
        "stage2_hard_fail",
    ] = "True"

    with pytest.raises(
        ValueError,
        match="Stage-2 hard failures",
    ):
        build_rt_stable_specification(
            _registry(),
            _stage1(),
            stage2,
        )


def test_pass_with_flags_is_allowed() -> None:
    stage2 = _stage2()

    mask = stage2[
        "raw_series"
    ].eq("A_SERIES")

    stage2.loc[
        mask,
        "stage2_status",
    ] = "pass_with_flags"

    stage2.loc[
        mask,
        "stage2_flags",
    ] = "near_zero_transformed_variance"

    out = build_rt_stable_specification(
        _registry(),
        _stage1(),
        stage2,
    )

    row = out.loc[
        out["raw_series"].eq("A_SERIES")
    ].iloc[0]

    assert (
        row["stage2_status"]
        == "pass_with_flags"
    )

    assert row["final_include"]


def test_missing_stage1_candidate_is_rejected() -> None:
    stage1 = _stage1()

    stage1 = stage1.loc[
        ~stage1[
            "raw_series"
        ].eq("B_SERIES")
    ]

    with pytest.raises(
        ValueError,
        match="does not match",
    ):
        build_rt_stable_specification(
            _registry(),
            stage1,
            _stage2(),
        )


def test_extra_stage2_series_is_rejected() -> None:
    stage2 = _stage2()

    extra = stage2.iloc[[0]].copy()
    extra["raw_series"] = "EXTRA_SERIES"

    stage2 = pd.concat(
        [stage2, extra],
        ignore_index=True,
    )

    with pytest.raises(
        ValueError,
        match="does not match",
    ):
        build_rt_stable_specification(
            _registry(),
            _stage1(),
            stage2,
        )


def test_tcode_mismatch_is_rejected() -> None:
    stage2 = _stage2()

    stage2.loc[
        stage2[
            "raw_series"
        ].eq("A_SERIES"),
        "embedded_tcodes_seen",
    ] = "2"

    with pytest.raises(
        ValueError,
        match="embedded t-codes",
    ):
        build_rt_stable_specification(
            _registry(),
            _stage1(),
            stage2,
        )


def test_wrong_vintage_count_is_rejected() -> None:
    stage2 = _stage2()

    stage2.loc[
        stage2[
            "raw_series"
        ].eq("A_SERIES"),
        "n_vintages_checked",
    ] = "197"

    with pytest.raises(
        ValueError,
        match="vintage count",
    ):
        build_rt_stable_specification(
            _registry(),
            _stage1(),
            stage2,
        )


def test_order_is_deterministic() -> None:
    registry = (
        _registry()
        .sample(
            frac=1,
            random_state=123,
        )
        .reset_index(drop=True)
    )

    out = build_rt_stable_specification(
        registry,
        _stage1().iloc[::-1],
        _stage2().iloc[::-1],
    )

    assert out["raw_series"].tolist() == [
        "A_SERIES",
        "B_SERIES",
    ]


def test_summary() -> None:
    out = build_rt_stable_specification(
        _registry(),
        _stage1(),
        _stage2(),
    )

    summary = build_rt_stable_summary(
        out
    ).iloc[0]

    assert summary["n_series"] == 2
    assert summary["n_ordinary_stable"] == 1
    assert summary["n_reviewed_stable_keep"] == 1
    assert summary["n_stage1_hard_fail"] == 0
    assert summary["n_stage2_hard_fail"] == 0