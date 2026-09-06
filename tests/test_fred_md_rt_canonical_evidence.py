from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_rt_canonical_evidence import (
    DOCUMENTARY_COLUMNS,
    build_rt_canonical_evidence,
    select_near_transition_diagnostics,
    validate_documentary_review,
    write_evidence_csv,
)
from dfm_pipeline.ingestion.fred_md_registry_reviews import (
    REVIEW_COLUMNS,
)


def _c3() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "TEST_CONCEPT",
                "transition_vintage": "2020-01",
                "old_source": "OLD",
                "new_source": "NEW",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "n_common_valid": 100,
                "first_common_date": "2010-01-01",
                "last_common_date": "2019-11-01",
                "pearson_corr": 0.95,
                "spearman_corr": 0.94,
                "std_ratio_new_old": 0.98,
                "nrmse_old_std": 0.20,
                "sign_agreement": 0.90,
                "ols_beta": 0.97,
                "ols_r2": 0.90,
                "diagnostic_status": "computed",
                "diagnostic_flags": "",
            }
        ]
    )


def _spec(
    *,
    triangulation: bool = False,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "TEST_CONCEPT",
                "old_reference_series": "OLD",
                "new_reference_series": "NEW",
                "aux_reference_series": (
                    "AUX"
                    if triangulation
                    else ""
                ),
                "canonical_tcode": 6,
                "review_mode": (
                    "triangulation"
                    if triangulation
                    else "pairwise"
                ),
            }
        ]
    )


def _series_reviews() -> pd.DataFrame:
    rows = []

    for raw_series in [
        "OLD",
        "NEW",
    ]:
        row = {
            column: ""
            for column in REVIEW_COLUMNS
        }

        row.update(
            {
                "raw_series": raw_series,
                "canonical_id": "TEST_CONCEPT",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "mapping_action": "switch_by_vintage",
                "include_rt_stable": "false",
                "include_rt_canonical": "true",
                "documented_change_vintage": "2020-01",
                "documented_change_type": "replacement",
                "fred_md_change_log_reference": "change log",
                "fred_series_url": (
                    f"FRED:{raw_series}"
                ),
                "source_agency_reference": "agency",
                "review_status": "reviewed",
                "notes": "reviewed test row",
            }
        )

        if raw_series == "OLD":
            row["successor"] = "NEW"
        else:
            row["predecessor"] = "OLD"

        rows.append(
            row
        )

    return pd.DataFrame(
        rows,
        columns=REVIEW_COLUMNS,
    )


def _documentary(
    *,
    triangulation: bool = False,
) -> pd.DataFrame:
    row = {
        column: ""
        for column in DOCUMENTARY_COLUMNS
    }

    row.update(
        {
            "canonical_id": "TEST_CONCEPT",
            "transition_vintage": "2020-01",
            "old_source": "OLD",
            "new_source": "NEW",
            "aux_reference_series": (
                "AUX"
                if triangulation
                else ""
            ),
            "relationship_type": "official_successor",
            "equivalence_level": "non_exact",
            "old_title": "Old test series",
            "new_title": "New test series",
            "old_source_agency": "Agency",
            "new_source_agency": "Agency",
            "old_frequency": "Monthly",
            "new_frequency": "Monthly",
            "old_units": "Index",
            "new_units": "Index",
            "old_seasonal_adjustment": "SA",
            "new_seasonal_adjustment": "SA",
            "official_fred_md_replacement": "true",
            "source_agency_change": "false",
            "methodology_change": "true",
            "official_successor_relation": "official replacement",
            "construction_or_splice_note": "non-exact",
            "documentary_assessment": (
                "official_non_exact_successor"
            ),
            "methodology_reference": "official methodology",
            "review_status": "reviewed",
            "notes": "documentary test",
        }
    )

    return pd.DataFrame(
        [row],
        columns=DOCUMENTARY_COLUMNS,
    )


def _c4_summary(
    *,
    triangulation: bool = False,
) -> pd.DataFrame:
    roles = [
        "old_vs_new",
    ]

    if triangulation:
        roles = [
            "old_vs_aux",
            "aux_vs_new",
            "old_vs_new",
        ]

    rows = []

    for role in roles:
        rows.append(
            {
                "canonical_id": "TEST_CONCEPT",
                "pair_role": role,
                "n_common_valid": 100,
                "first_common_date": "2010-01-01",
                "last_common_date": "2019-11-01",
                "pearson_corr": (
                    0.99
                    if role == "aux_vs_new"
                    else 0.96
                ),
                "spearman_corr": 0.95,
                "std_ratio_new_old": 0.98,
                "nrmse_old_std": (
                    0.14
                    if role == "aux_vs_new"
                    else 0.28
                ),
                "sign_agreement": 0.90,
                "ols_beta": 0.97,
                "ols_r2": 0.92,
                "diagnostic_status": "computed",
            }
        )

    return pd.DataFrame(
        rows
    )


def _c4_rolling(
    *,
    triangulation: bool = False,
) -> pd.DataFrame:
    roles = [
        "old_vs_new",
    ]

    if triangulation:
        roles = [
            "old_vs_aux",
            "aux_vs_new",
            "old_vs_new",
        ]

    rows = []

    for role in roles:
        for end_date, pearson in [
            ("2019-09-01", 0.94),
            ("2019-10-01", 0.95),
            ("2019-11-01", 0.96),
            # Must not be selected because it is
            # after the 2020-01 transition.
            ("2020-02-01", 0.50),
        ]:
            end = pd.Timestamp(
                end_date
            )
            start = (
                end
                - pd.DateOffset(
                    months=59
                )
            )

            rows.append(
                {
                    "canonical_id": "TEST_CONCEPT",
                    "pair_role": role,
                    "window_start_date": (
                        start.date().isoformat()
                    ),
                    "window_end_date": end_date,
                    "n_common_valid": 60,
                    "pearson_corr": (
                        0.99
                        if role == "aux_vs_new"
                        and end_date
                        == "2019-11-01"
                        else pearson
                    ),
                    "std_ratio_new_old": 0.98,
                    "nrmse_old_std": (
                        0.14
                        if role == "aux_vs_new"
                        else 0.28
                    ),
                    "diagnostic_status": "computed",
                }
            )

    return pd.DataFrame(
        rows
    )


def test_documentary_validation_accepts_consistent_review() -> None:
    result = validate_documentary_review(
        _documentary(),
        c3=_c3(),
        review_spec=_spec(),
        series_reviews=_series_reviews(),
    )

    assert len(result) == 1

    assert bool(
        result.loc[
            0,
            "official_fred_md_replacement",
        ]
    )

    assert (
        result.loc[
            0,
            "canonical_id",
        ]
        == "TEST_CONCEPT"
    )


def test_documentary_validation_rejects_changed_predecessor() -> None:
    documentary = (
        _documentary()
    )

    documentary.loc[
        0,
        "old_source",
    ] = "WRONG"

    with pytest.raises(
        ValueError,
        match="does not match C3",
    ):
        validate_documentary_review(
            documentary,
            c3=_c3(),
            review_spec=_spec(),
            series_reviews=_series_reviews(),
        )


def test_documentary_validation_rejects_wrong_transition_vintage() -> None:
    documentary = (
        _documentary()
    )

    documentary.loc[
        0,
        "transition_vintage",
    ] = "2020-02"

    with pytest.raises(
        ValueError,
        match="does not match C3",
    ):
        validate_documentary_review(
            documentary,
            c3=_c3(),
            review_spec=_spec(),
            series_reviews=_series_reviews(),
        )


def test_near_transition_selects_last_pre_transition_window() -> None:
    result = (
        select_near_transition_diagnostics(
            _c4_rolling(),
            c3=_c3(),
            pair_role="old_vs_new",
            prefix="c4_near",
        )
    )

    assert len(result) == 1

    assert (
        result.loc[
            0,
            "c4_near_window_end_date",
        ]
        == "2019-11-01"
    )

    assert (
        result.loc[
            0,
            "c4_near_months_to_transition",
        ]
        == 2
    )

    assert (
        result.loc[
            0,
            "c4_near_pearson_corr",
        ]
        == pytest.approx(
            0.96
        )
    )


def test_evidence_builder_combines_c3_c4_and_documentary() -> None:
    evidence = (
        build_rt_canonical_evidence(
            documentary=_documentary(),
            c3=_c3(),
            c4_summary=_c4_summary(),
            c4_rolling=_c4_rolling(),
            review_spec=_spec(),
            series_reviews=_series_reviews(),
        )
    )

    assert len(evidence) == 1

    row = evidence.iloc[
        0
    ]

    assert row[
        "c3_pearson_corr"
    ] == pytest.approx(
        0.95
    )

    assert row[
        "c4_full_pearson_corr"
    ] == pytest.approx(
        0.96
    )

    assert row[
        "c4_near_pearson_corr"
    ] == pytest.approx(
        0.96
    )

    assert (
        row[
            "c4_near_months_to_transition"
        ]
        == 2
    )

    assert bool(
        row[
            "documentary_review_complete"
        ]
    )


def test_evidence_builder_preserves_triangulation() -> None:
    evidence = (
        build_rt_canonical_evidence(
            documentary=_documentary(
                triangulation=True
            ),
            c3=_c3(),
            c4_summary=_c4_summary(
                triangulation=True
            ),
            c4_rolling=_c4_rolling(
                triangulation=True
            ),
            review_spec=_spec(
                triangulation=True
            ),
            series_reviews=_series_reviews(),
        )
    )

    row = evidence.iloc[
        0
    ]

    assert row[
        "c4_aux_vs_new_full_pearson_corr"
    ] == pytest.approx(
        0.99
    )

    assert row[
        "c4_aux_vs_new_near_pearson_corr"
    ] == pytest.approx(
        0.99
    )


def test_write_evidence_refuses_overwrite(
    tmp_path: Path,
) -> None:
    evidence = (
        build_rt_canonical_evidence(
            documentary=_documentary(),
            c3=_c3(),
            c4_summary=_c4_summary(),
            c4_rolling=_c4_rolling(),
            review_spec=_spec(),
            series_reviews=_series_reviews(),
        )
    )

    path = (
        tmp_path
        / "evidence.csv"
    )

    write_evidence_csv(
        evidence,
        path,
    )

    assert path.exists()

    with pytest.raises(
        FileExistsError
    ):
        write_evidence_csv(
            evidence,
            path,
        )