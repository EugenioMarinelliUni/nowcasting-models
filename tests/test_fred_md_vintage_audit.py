from __future__ import annotations

from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_vintages import (
    audit_single_vintage,
    audit_vintage_collection,
    classify_missingness,
    parse_vintage_filename,
)


def _write_vintage(
    path: Path,
    *,
    series: dict[str, tuple[int, list[str]]],
    dates: list[str],
) -> None:
    names = list(series)
    lines = [
        ",".join(["sasdate", *names]),
        ",".join(["Transform:", *[str(series[name][0]) for name in names]]),
    ]
    for row_idx, date in enumerate(dates):
        values = [series[name][1][row_idx] for name in names]
        lines.append(",".join([date, *values]))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_parse_vintage_filename() -> None:
    assert parse_vintage_filename("1999-08.csv") == "1999-08"
    assert parse_vintage_filename("FRED-MD_2024m03.csv") == "2024-03"
    assert parse_vintage_filename("FRED-MD_2024m09.csv") == "2024-09"
    assert parse_vintage_filename("FRED-MD_2024m10.csv") == "2024-10"
    assert parse_vintage_filename("FRED-MD_2024m11.csv") == "2024-11"
    assert parse_vintage_filename("FRED-MD_2024m12.csv") == "2024-12"
    assert parse_vintage_filename("2026-06-MD.csv") == "2026-06"
    assert parse_vintage_filename("2026-06-md.csv") == "2026-06"
    assert parse_vintage_filename("current.csv") is None


def test_classify_missingness() -> None:
    idx = pd.date_range("2020-01-01", periods=5, freq="MS")
    series = pd.Series(
        [float("nan"), 1.0, float("nan"), 2.0, float("nan")],
        index=idx,
    )
    info = classify_missingness(series)
    assert info["n_missing"] == 3
    assert info["n_leading_missing"] == 1
    assert info["n_internal_missing"] == 1
    assert info["n_trailing_missing"] == 1
    assert info["all_missing"] is False


def test_valid_vintage_and_internal_missing_detection(tmp_path: Path) -> None:
    path = tmp_path / "2020-03.csv"
    _write_vintage(
        path,
        series={
            "INDPRO": (5, ["100", "", "102"]),
            "UNRATE": (2, ["3.5", "3.6", "3.7"]),
        },
        dates=["2020-01-01", "2020-02-01", "2020-03-01"],
    )

    vintage, series_rows, anomalies = audit_single_vintage(path)
    assert vintage["schema_valid"] is True
    assert vintage["sasdate_ok"] is True
    assert vintage["transform_marker_ok"] is True
    assert vintage["tcode_valid"] is True
    assert vintage["monthly_grid_gaps"] == 0

    indpro = next(row for row in series_rows if row["series"] == "INDPRO")
    assert indpro["n_internal_missing"] == 1

    internal = [
        row
        for row in anomalies
        if row["series"] == "INDPRO"
        and row["anomaly_type"] == "internal_missing_run"
    ]
    assert len(internal) == 1
    assert internal[0]["start"] == "2020-02-01"
    assert internal[0]["end"] == "2020-02-01"
    assert internal[0]["length"] == 1


def test_invalid_tcode_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "2020-03.csv"
    _write_vintage(
        path,
        series={"INDPRO": (9, ["100", "101"])},
        dates=["2020-01-01", "2020-02-01"],
    )
    vintage, _, _ = audit_single_vintage(path)
    assert vintage["schema_valid"] is False
    assert "invalid_or_incomplete_tcodes" in vintage["problems"]


def test_monthly_grid_gap_is_flagged(tmp_path: Path) -> None:
    path = tmp_path / "2020-03.csv"
    _write_vintage(
        path,
        series={"INDPRO": (5, ["100", "102"])},
        dates=["2020-01-01", "2020-03-01"],
    )
    vintage, _, _ = audit_single_vintage(path)
    assert vintage["schema_valid"] is False
    assert vintage["monthly_grid_gaps"] == 1
    assert "monthly_grid_gaps" in vintage["problems"]


def test_collection_detects_additions_and_removals(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2020-01.csv",
        series={"A": (1, ["1", "2"]), "B": (2, ["3", "4"])},
        dates=["2019-12-01", "2020-01-01"],
    )
    _write_vintage(
        tmp_path / "2020-02.csv",
        series={"B": (2, ["3", "4"]), "C": (5, ["5", "6"])},
        dates=["2020-01-01", "2020-02-01"],
    )

    outputs = audit_vintage_collection(tmp_path)
    changes = outputs["series_changes"]
    added = changes[
        (changes["vintage"] == "2020-02") & (changes["change"] == "added")
    ]["series"].tolist()
    removed = changes[
        (changes["vintage"] == "2020-02") & (changes["change"] == "removed")
    ]["series"].tolist()
    assert added == ["C"]
    assert removed == ["A"]


def test_collection_detects_duplicate_vintage(tmp_path: Path) -> None:
    for name in ["2020-01.csv", "FRED-MD_2020m01.csv"]:
        _write_vintage(
            tmp_path / name,
            series={"A": (1, ["1", "2"])},
            dates=["2019-12-01", "2020-01-01"],
        )

    outputs = audit_vintage_collection(tmp_path)
    vintage = outputs["vintage_audit"]
    assert int(vintage["duplicate_vintage"].sum()) == 2
    assert not vintage["schema_valid"].any()
    assert all("duplicate_vintage" in value for value in vintage["problems"].tolist())
    collection = outputs["collection_summary"].iloc[0]
    assert bool(collection["collection_valid"]) is False
    assert int(collection["n_duplicate_vintage_months"]) == 1


def test_tcode_history_flags_change(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2020-01.csv",
        series={"A": (1, ["1", "2"])},
        dates=["2019-12-01", "2020-01-01"],
    )
    _write_vintage(
        tmp_path / "2020-02.csv",
        series={"A": (2, ["1", "2"])},
        dates=["2020-01-01", "2020-02-01"],
    )

    outputs = audit_vintage_collection(tmp_path)
    history = outputs["series_tcode_history"]
    row = history.loc[history["series"] == "A"].iloc[0]
    assert row["tcodes_seen"] == "1|2"
    assert int(row["n_distinct_tcodes"]) == 2
    assert int(row["n_tcode_changes"]) == 1


def test_collection_summary_detects_missing_vintage_month(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2020-01.csv",
        series={"A": (1, ["1", "2"])},
        dates=["2019-12-01", "2020-01-01"],
    )
    _write_vintage(
        tmp_path / "2020-03.csv",
        series={"A": (1, ["1", "2"])},
        dates=["2020-02-01", "2020-03-01"],
    )

    outputs = audit_vintage_collection(tmp_path)
    summary = outputs["collection_summary"].iloc[0]

    assert bool(summary["collection_valid"]) is False
    assert int(summary["n_missing_vintage_months"]) == 1
    assert summary["missing_vintage_months"] == "2020-02"


def test_expected_range_detects_missing_endpoint(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2020-02.csv",
        series={"A": (1, ["1", "2"])},
        dates=["2020-01-01", "2020-02-01"],
    )
    _write_vintage(
        tmp_path / "2020-03.csv",
        series={"A": (1, ["1", "2"])},
        dates=["2020-02-01", "2020-03-01"],
    )

    outputs = audit_vintage_collection(
        tmp_path,
        expected_start="2020-01",
        expected_end="2020-03",
    )
    summary = outputs["collection_summary"].iloc[0]

    assert bool(summary["collection_valid"]) is False
    assert summary["missing_vintage_months"] == "2020-01"


def test_anomaly_summary_collapses_recurring_internal_gap(tmp_path: Path) -> None:
    for vintage in ["2020-03", "2020-04"]:
        _write_vintage(
            tmp_path / f"{vintage}.csv",
            series={"A": (1, ["1", "", "3"])},
            dates=["2020-01-01", "2020-02-01", "2020-03-01"],
        )

    outputs = audit_vintage_collection(tmp_path)
    summary = outputs["anomaly_summary"]

    row = summary[
        (summary["series"] == "A")
        & (summary["anomaly_type"] == "internal_missing_run")
        & (summary["start"] == "2020-02-01")
    ].iloc[0]

    assert int(row["n_vintages"]) == 2
    assert row["first_vintage"] == "2020-03"
    assert row["last_vintage"] == "2020-04"


def test_vintage_manifest_is_machine_independent(tmp_path: Path) -> None:
    _write_vintage(
        tmp_path / "2020-01.csv",
        series={"A": (1, ["1", "2"])},
        dates=["2019-12-01", "2020-01-01"],
    )

    outputs = audit_vintage_collection(tmp_path)
    manifest = outputs["vintage_manifest"]

    assert "source_path" not in manifest.columns
    assert {
        "vintage",
        "filename",
        "sha256",
        "n_series",
        "n_rows",
        "first_reference_date",
        "last_reference_date",
        "schema_valid",
    }.issubset(manifest.columns)

