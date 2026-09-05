from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical_comparability import (
    audit_rt_canonical_comparability,
    build_comparability_run_summary,
)


def _panel() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "panel_position": 1,
                "canonical_id": "X",
                "mapping_type": "switch_by_vintage",
                "n_source_segments": 2,
                "stable_window_start": "2010-01",
                "stable_window_end": "2010-02",
                "final_include": True,
                "inclusion_reason": "synthetic test",
            }
        ]
    )


def _source_map(
    *,
    old_tcode: int = 5,
    new_tcode: int = 5,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "X",
                "source_order": 1,
                "source_series": "OLD",
                "expected_tcode": old_tcode,
                "active_from_vintage": "2010-01",
                "active_to_vintage": "2010-01",
                "mapping_action": "switch_by_vintage",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "documented_change_vintage": "2010-02",
                "source_origin": "registry_review",
                "notes": "",
            },
            {
                "canonical_id": "X",
                "source_order": 2,
                "source_series": "NEW",
                "expected_tcode": new_tcode,
                "active_from_vintage": "2010-02",
                "active_to_vintage": "2010-02",
                "mapping_action": "switch_by_vintage",
                "relationship_type": "official_successor",
                "equivalence_level": "non_exact",
                "documented_change_vintage": "2010-02",
                "source_origin": "registry_review",
                "notes": "",
            },
        ]
    )


def _write_vintage(
    path: Path,
    *,
    old_values: list[object] | None,
    new_values: list[object] | None,
    old_tcode: int = 5,
    new_tcode: int = 5,
) -> None:
    dates = pd.date_range("2009-01-01", periods=6, freq="MS")
    columns = ["sasdate"]
    tcodes: list[object] = ["transform"]

    data = pd.DataFrame({"sasdate": dates.strftime("%Y-%m-%d")})

    if old_values is not None:
        columns.append("OLD")
        tcodes.append(str(old_tcode))
        data["OLD"] = old_values

    if new_values is not None:
        columns.append("NEW")
        tcodes.append(str(new_tcode))
        data["NEW"] = new_values

    trow = pd.DataFrame([tcodes], columns=columns)
    out = pd.concat([trow, data[columns]], ignore_index=True)
    out.to_csv(path, index=False)


OLD = [100.0, 108.0, 119.0, 131.0, 145.0, 160.0]
NEW = [1000.0, 1080.0, 1190.0, 1310.0, 1450.0, 1600.0]


def test_prefers_transition_vintage_same_vintage(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2010-01.csv",
        old_values=OLD,
        new_values=None,
    )
    _write_vintage(
        tmp_path / "2010-02.csv",
        old_values=OLD,
        new_values=NEW,
    )

    comparison, aligned, run_summary = audit_rt_canonical_comparability(
        tmp_path,
        _panel(),
        _source_map(),
    )

    row = comparison.iloc[0]
    assert row["comparison_mode"] == "same_vintage_transition"
    assert row["old_data_vintage"] == "2010-02"
    assert row["new_data_vintage"] == "2010-02"
    assert row["diagnostic_status"] == "computed"
    assert len(aligned) == 5
    assert int(run_summary.loc[0, "n_same_vintage_transition"]) == 1


def test_falls_back_to_predecessor_same_vintage(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2010-01.csv",
        old_values=OLD,
        new_values=NEW,
    )
    _write_vintage(
        tmp_path / "2010-02.csv",
        old_values=None,
        new_values=NEW,
    )

    comparison, _, _ = audit_rt_canonical_comparability(
        tmp_path,
        _panel(),
        _source_map(),
    )

    row = comparison.iloc[0]
    assert row["comparison_mode"] == "same_vintage_predecessor"
    assert row["old_data_vintage"] == "2010-01"
    assert row["new_data_vintage"] == "2010-01"


def test_bridge_when_sources_do_not_coexist(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2010-01.csv",
        old_values=OLD,
        new_values=None,
    )
    _write_vintage(
        tmp_path / "2010-02.csv",
        old_values=None,
        new_values=NEW,
    )

    comparison, _, run_summary = audit_rt_canonical_comparability(
        tmp_path,
        _panel(),
        _source_map(),
    )

    row = comparison.iloc[0]
    assert row["comparison_mode"] == "adjacent_vintage_bridge"
    assert row["old_data_vintage"] == "2010-01"
    assert row["new_data_vintage"] == "2010-02"
    assert "cross_vintage_revisions_may_contribute" in row["diagnostic_flags"]
    assert int(run_summary.loc[0, "n_adjacent_vintage_bridge"]) == 1


def test_tcode5_sources_are_transformed_independently(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2010-01.csv",
        old_values=OLD,
        new_values=None,
    )
    _write_vintage(
        tmp_path / "2010-02.csv",
        old_values=OLD,
        new_values=NEW,
    )

    comparison, aligned, _ = audit_rt_canonical_comparability(
        tmp_path,
        _panel(),
        _source_map(),
    )

    row = comparison.iloc[0]
    assert np.isclose(row["pearson_corr"], 1.0)
    assert np.isclose(row["std_ratio_new_old"], 1.0)
    assert np.isclose(row["rmse_difference"], 0.0, atol=1e-12)
    assert np.allclose(
        aligned["old_transformed"],
        aligned["new_transformed"],
        atol=1e-12,
    )


def test_tcode_mismatch_is_mechanical_failure(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2010-01.csv",
        old_values=OLD,
        new_values=None,
        old_tcode=1,
    )
    _write_vintage(
        tmp_path / "2010-02.csv",
        old_values=None,
        new_values=NEW,
        new_tcode=5,
    )

    comparison, aligned, run_summary = audit_rt_canonical_comparability(
        tmp_path,
        _panel(),
        _source_map(old_tcode=5, new_tcode=5),
    )

    row = comparison.iloc[0]
    assert row["diagnostic_status"] == "mechanical_failure"
    assert "embedded_tcode_mismatch" in row["mechanical_failure_reasons"]
    assert aligned.empty
    assert int(run_summary.loc[0, "n_mechanical_failure"]) == 1


def test_single_common_point_is_insufficient_overlap(tmp_path: Path) -> None:
    old = [100.0, np.nan, np.nan, np.nan, np.nan, np.nan]
    new = [1000.0, np.nan, np.nan, np.nan, np.nan, np.nan]

    _write_vintage(
        tmp_path / "2010-01.csv",
        old_values=old,
        new_values=None,
        old_tcode=1,
    )
    _write_vintage(
        tmp_path / "2010-02.csv",
        old_values=old,
        new_values=new,
        old_tcode=1,
        new_tcode=1,
    )

    comparison, aligned, run_summary = audit_rt_canonical_comparability(
        tmp_path,
        _panel(),
        _source_map(old_tcode=1, new_tcode=1),
    )

    row = comparison.iloc[0]
    assert len(aligned) == 1
    assert row["n_common_valid"] == 1
    assert row["diagnostic_status"] == "insufficient_overlap"
    assert int(run_summary.loc[0, "n_insufficient_overlap"]) == 1


def test_zero_variance_is_diagnostic_not_mechanical(tmp_path: Path) -> None:
    old = [5.0] * 6
    new = [10.0, 11.0, 9.0, 12.0, 8.0, 13.0]

    _write_vintage(
        tmp_path / "2010-01.csv",
        old_values=old,
        new_values=None,
        old_tcode=1,
    )
    _write_vintage(
        tmp_path / "2010-02.csv",
        old_values=old,
        new_values=new,
        old_tcode=1,
        new_tcode=1,
    )

    comparison, _, run_summary = audit_rt_canonical_comparability(
        tmp_path,
        _panel(),
        _source_map(old_tcode=1, new_tcode=1),
    )

    row = comparison.iloc[0]
    assert row["diagnostic_status"] == "zero_variance"
    assert row["mechanical_failure_reasons"] == ""
    assert np.isnan(row["pearson_corr"])
    assert np.isnan(row["std_ratio_new_old"])
    assert int(run_summary.loc[0, "n_zero_variance"]) == 1


def test_aligned_output_contains_only_common_finite_dates(tmp_path: Path) -> None:
    old = [1.0, 2.0, np.nan, 4.0, 5.0, 6.0]
    new = [10.0, np.nan, 30.0, 40.0, 50.0, 60.0]

    _write_vintage(
        tmp_path / "2010-01.csv",
        old_values=old,
        new_values=None,
        old_tcode=1,
    )
    _write_vintage(
        tmp_path / "2010-02.csv",
        old_values=old,
        new_values=new,
        old_tcode=1,
        new_tcode=1,
    )

    comparison, aligned, _ = audit_rt_canonical_comparability(
        tmp_path,
        _panel(),
        _source_map(old_tcode=1, new_tcode=1),
    )

    assert comparison.iloc[0]["n_common_valid"] == 4
    assert aligned["reference_date"].tolist() == [
        "2009-01-01",
        "2009-04-01",
        "2009-05-01",
        "2009-06-01",
    ]


def test_run_summary_counts_status_and_mode() -> None:
    comparison = pd.DataFrame(
        {
            "diagnostic_status": [
                "computed",
                "mechanical_failure",
                "zero_variance",
                "insufficient_overlap",
            ],
            "comparison_mode": [
                "same_vintage_transition",
                "same_vintage_predecessor",
                "adjacent_vintage_bridge",
                "adjacent_vintage_bridge",
            ],
        }
    )
    aligned = pd.DataFrame({"x": [1, 2, 3]})

    out = build_comparability_run_summary(comparison, aligned).iloc[0]

    assert int(out["n_transition_boundaries"]) == 4
    assert int(out["n_computed"]) == 1
    assert int(out["n_mechanical_failure"]) == 1
    assert int(out["n_zero_variance"]) == 1
    assert int(out["n_insufficient_overlap"]) == 1
    assert int(out["n_same_vintage_transition"]) == 1
    assert int(out["n_same_vintage_predecessor"]) == 1
    assert int(out["n_adjacent_vintage_bridge"]) == 2
    assert int(out["total_aligned_rows"]) == 3
