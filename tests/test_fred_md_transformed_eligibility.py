from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_transformed_qc import (
    audit_transformed_collection,
    audit_transformed_series,
    audit_transformed_vintage,
    expected_transform_valid_mask,
    validate_stage1_gate,
)
from dfm_pipeline.preprocessing.tcode import apply_tcode_transformations


def _series(values: list[float | None]) -> pd.Series:
    index = pd.date_range("2010-01-01", periods=len(values), freq="MS")
    return pd.Series(values, index=index, dtype=float)


def _registry_row(
    raw_series: str,
    *,
    tcode: int = 2,
    review_status: str = "unreviewed",
    include_rt_stable: bool = False,
) -> dict[str, object]:
    return {
        "raw_series": raw_series,
        "candidate_rt_stable": True,
        "review_status": review_status,
        "include_rt_stable": include_rt_stable,
        "include_rt_canonical": include_rt_stable,
        "present_all_stable_window": True,
        "tcodes_seen": str(tcode),
        "n_distinct_tcodes": 1,
        "n_tcode_changes": 0,
    }


def _selected(raw_series: str, tcode: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "raw_series": raw_series,
                "selection_class": "ordinary_stable",
                "tcode": tcode,
            }
        ]
    )


def _write_vintage(
    path: Path,
    *,
    tcode: int = 2,
    values: tuple[float, ...] = (1.0, 2.0, 4.0, 7.0),
) -> None:
    dates = pd.date_range("2009-10-01", periods=len(values), freq="MS")
    lines = ["sasdate,A", f"Transform:,{tcode}"]
    lines.extend(
        f"{date.date().isoformat()},{value}"
        for date, value in zip(dates, values)
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_expected_valid_mask_respects_operator_and_raw_gaps() -> None:
    raw = _series([1.0, 2.0, np.nan, 4.0, 5.0, 6.0])

    assert expected_transform_valid_mask(raw, 1).tolist() == [
        True, True, False, True, True, True
    ]
    assert expected_transform_valid_mask(raw, 2).tolist() == [
        False, True, False, False, True, True
    ]
    assert expected_transform_valid_mask(raw, 3).tolist() == [
        False, False, False, False, False, True
    ]


def test_expected_valid_mask_enforces_log_domain() -> None:
    raw = _series([10.0, 11.0, 0.0, -1.0, 12.0, 13.0])

    assert expected_transform_valid_mask(raw, 4).tolist() == [
        True, True, False, False, True, True
    ]
    assert expected_transform_valid_mask(raw, 5).tolist() == [
        False, True, False, False, False, True
    ]


def test_tcode7_preserves_missingness_without_forward_fill() -> None:
    raw = _series([10.0, 11.0, np.nan, 13.0, 14.0, 15.0, 16.0])
    transformed = apply_tcode_transformations(
        raw.to_frame("A"),
        {"A": 7},
    )["A"]

    metrics = audit_transformed_series(raw, transformed, tcode=7)

    assert metrics["n_unexpected_transform_missing"] == 0
    assert metrics["n_unexpected_transform_value"] == 0
    assert bool(metrics["stage2_hard_fail"]) is False
    assert pd.isna(transformed.iloc[3])
    assert pd.isna(transformed.iloc[4])


def test_nonpositive_log_inputs_are_flagged_not_automatically_rejected() -> None:
    raw = _series([10.0, 11.0, 0.0, 12.0, 13.0, 14.0])
    transformed = apply_tcode_transformations(
        raw.to_frame("A"),
        {"A": 5},
    )["A"]

    metrics = audit_transformed_series(raw, transformed, tcode=5)

    assert metrics["n_nonpositive_raw_for_log"] == 1
    assert "nonpositive_log_domain_input" in metrics["stage2_flags"]
    assert metrics["n_unexpected_transform_missing"] == 0
    assert bool(metrics["stage2_hard_fail"]) is False


def test_zero_transformed_variance_is_hard_failure() -> None:
    raw = _series([5.0, 5.0, 5.0, 5.0])
    transformed = apply_tcode_transformations(
        raw.to_frame("A"),
        {"A": 1},
    )["A"]

    metrics = audit_transformed_series(raw, transformed, tcode=1)

    assert bool(metrics["zero_variance"]) is True
    assert bool(metrics["stage2_hard_fail"]) is True
    assert "zero_transformed_variance" in metrics["hard_failure_reasons"]


def test_unexpected_transformation_missingness_is_detected() -> None:
    raw = _series([1.0, 2.0, 3.0, 4.0])
    transformed = apply_tcode_transformations(
        raw.to_frame("A"),
        {"A": 2},
    )["A"].copy()
    transformed.iloc[-1] = np.nan

    metrics = audit_transformed_series(raw, transformed, tcode=2)

    assert metrics["n_unexpected_transform_missing"] == 1
    assert bool(metrics["stage2_hard_fail"]) is True
    assert "unexpected_missing_after_transform" in metrics[
        "hard_failure_reasons"
    ]


def test_stage1_gate_rejects_selected_hard_failure() -> None:
    selected = pd.DataFrame(
        [
            {"raw_series": "A"},
            {"raw_series": "B"},
        ]
    )
    stage1 = pd.DataFrame(
        [
            {"raw_series": "A", "qc_hard_fail": "false"},
            {"raw_series": "B", "qc_hard_fail": "true"},
        ]
    )

    with pytest.raises(ValueError, match="Stage-1 hard failures remain: B"):
        validate_stage1_gate(selected, stage1)


def test_single_vintage_audit_uses_embedded_tcode(tmp_path: Path) -> None:
    path = tmp_path / "2010-01-MD.csv"
    _write_vintage(path, tcode=2)

    report = audit_transformed_vintage(
        path,
        _selected("A", 2),
        vintage="2010-01",
    )
    row = report.iloc[0]

    assert int(row["registry_tcode"]) == 2
    assert int(row["tcode"]) == 2
    assert bool(row["stage2_hard_fail"]) is False
    assert int(row["n_unexpected_transform_missing"]) == 0
    assert int(row["n_unexpected_transform_value"]) == 0


def test_embedded_tcode_mismatch_is_hard_failure(tmp_path: Path) -> None:
    path = tmp_path / "2010-01-MD.csv"
    _write_vintage(path, tcode=2)

    report = audit_transformed_vintage(
        path,
        _selected("A", 5),
        vintage="2010-01",
    )
    row = report.iloc[0]

    assert bool(row["stage2_hard_fail"]) is True
    assert "embedded_tcode_mismatch_registry" in row[
        "hard_failure_reasons"
    ]


def test_collection_builds_series_and_summary_reports(tmp_path: Path) -> None:
    _write_vintage(tmp_path / "2010-01-MD.csv", tcode=2)
    _write_vintage(tmp_path / "2010-02-MD.csv", tcode=2)

    registry = pd.DataFrame([_registry_row("A", tcode=2)])
    stage1 = pd.DataFrame(
        [{"raw_series": "A", "qc_hard_fail": "false"}]
    )

    outputs = audit_transformed_collection(
        tmp_path,
        registry,
        start="2010-01",
        end="2010-02",
        stage1_report=stage1,
    )

    by_vintage = outputs["by_vintage"]
    by_series = outputs["by_series"]
    summary = outputs["summary"].iloc[0]

    assert len(by_vintage) == 2
    assert len(by_series) == 1
    assert by_series.iloc[0]["stage2_status"] == "pass"
    assert int(summary["n_vintages_checked"]) == 2
    assert int(summary["n_series_selected"]) == 1
    assert int(summary["n_series_hard_fail"]) == 0
