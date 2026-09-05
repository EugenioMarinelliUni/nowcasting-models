from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from dfm_pipeline.ingestion.fred_md_rt_canonical_review import (
    ALIGNED_REVIEW_COLUMNS,
    REVIEW_SUMMARY_COLUMNS,
    ROLLING_REVIEW_COLUMNS,
)
from dfm_pipeline.ingestion.fred_md_rt_canonical_review_execution import (
    REFERENCE_MANIFEST_COLUMNS,
    load_reference_manifest,
    load_verified_reference_snapshots,
    run_rt_canonical_transition_review,
)
from dfm_pipeline.ingestion.fred_reference_data import (
    FRED_OBSERVATIONS_URL,
    collect_reference_requirements,
    sha256_file,
)


def _make_spec() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "canonical_id": "TEST_CONCEPT",
                "old_reference_series": "OLD",
                "new_reference_series": "NEW",
                "aux_reference_series": "",
                "canonical_tcode": 1,
                "review_mode": "pairwise",
            }
        ]
    )


def _write_reference_csv(
    path: Path,
    *,
    series_id: str,
    values: list[float],
) -> None:
    dates = pd.date_range(
        "2000-01-01",
        periods=len(values),
        freq="MS",
    )

    frame = pd.DataFrame(
        {
            "observation_date": dates,
            series_id: values,
        }
    )

    frame.to_csv(
        path,
        index=False,
        date_format="%Y-%m-%d",
    )


def _make_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    spec = _make_spec()

    spec_path = (
        tmp_path
        / "review_spec.csv"
    )
    spec.to_csv(
        spec_path,
        index=False,
    )

    reference_dir = (
        tmp_path
        / "references"
    )
    reference_dir.mkdir()

    _write_reference_csv(
        reference_dir / "OLD.csv",
        series_id="OLD",
        values=[
            1.0,
            2.0,
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0,
        ],
    )

    _write_reference_csv(
        reference_dir / "NEW.csv",
        series_id="NEW",
        values=[
            1.1,
            2.1,
            3.1,
            4.1,
            5.1,
            6.1,
            7.1,
            8.1,
        ],
    )

    requirements = (
        collect_reference_requirements(
            spec
        )
    )

    manifest_rows = []

    for _, requirement in requirements.iterrows():
        series_id = str(
            requirement["series_id"]
        )
        path = (
            reference_dir
            / f"{series_id}.csv"
        )

        loaded = pd.read_csv(path)

        manifest_rows.append(
            {
                "series_id": series_id,
                "canonical_ids": requirement[
                    "canonical_ids"
                ],
                "reference_roles": requirement[
                    "reference_roles"
                ],
                "source": "FRED",
                "api_endpoint": (
                    FRED_OBSERVATIONS_URL
                ),
                "retrieval_timestamp_utc": (
                    "2026-09-05T12:00:00+00:00"
                ),
                "fred_realtime_start": (
                    "2026-09-05"
                ),
                "fred_realtime_end": (
                    "2026-09-05"
                ),
                "local_filename": (
                    f"{series_id}.csv"
                ),
                "n_observations": int(
                    len(loaded)
                ),
                "n_finite": int(
                    loaded[series_id]
                    .notna()
                    .sum()
                ),
                "first_date": (
                    "2000-01-01"
                ),
                "last_date": (
                    "2000-08-01"
                ),
                "sha256": sha256_file(
                    path
                ),
            }
        )

    manifest = pd.DataFrame(
        manifest_rows,
        columns=REFERENCE_MANIFEST_COLUMNS,
    )

    manifest_path = (
        tmp_path
        / "reference_manifest.csv"
    )
    manifest.to_csv(
        manifest_path,
        index=False,
    )

    output_dir = (
        tmp_path
        / "outputs"
    )

    return {
        "spec": spec_path,
        "manifest": manifest_path,
        "reference_dir": reference_dir,
        "output_dir": output_dir,
    }


def test_load_reference_manifest_matches_spec(
    tmp_path: Path,
) -> None:
    fixture = _make_fixture(
        tmp_path
    )

    spec = pd.read_csv(
        fixture["spec"]
    )

    manifest = load_reference_manifest(
        fixture["manifest"],
        spec=spec,
    )

    assert manifest[
        "series_id"
    ].tolist() == [
        "OLD",
        "NEW",
    ]


def test_reference_manifest_rejects_missing_required_series(
    tmp_path: Path,
) -> None:
    fixture = _make_fixture(
        tmp_path
    )

    manifest = pd.read_csv(
        fixture["manifest"]
    )

    manifest = manifest.loc[
        manifest["series_id"].ne(
            "NEW"
        )
    ]

    manifest.to_csv(
        fixture["manifest"],
        index=False,
    )

    spec = pd.read_csv(
        fixture["spec"]
    )

    with pytest.raises(
        ValueError,
        match="missing required series",
    ):
        load_reference_manifest(
            fixture["manifest"],
            spec=spec,
        )


def test_verified_reference_loader_rejects_hash_mismatch(
    tmp_path: Path,
) -> None:
    fixture = _make_fixture(
        tmp_path
    )

    old_path = (
        fixture["reference_dir"]
        / "OLD.csv"
    )

    with old_path.open(
        "a",
        encoding="utf-8",
    ) as handle:
        handle.write(
            "\n2000-09-01,9.0\n"
        )

    spec = pd.read_csv(
        fixture["spec"]
    )
    manifest = pd.read_csv(
        fixture["manifest"]
    )

    with pytest.raises(
        ValueError,
        match="SHA-256 mismatch",
    ):
        load_verified_reference_snapshots(
            spec=spec,
            manifest=manifest,
            reference_dir=(
                fixture["reference_dir"]
            ),
        )


def test_verified_reference_loader_rejects_missing_csv(
    tmp_path: Path,
) -> None:
    fixture = _make_fixture(
        tmp_path
    )

    (
        fixture["reference_dir"]
        / "NEW.csv"
    ).unlink()

    spec = pd.read_csv(
        fixture["spec"]
    )
    manifest = pd.read_csv(
        fixture["manifest"]
    )

    with pytest.raises(
        FileNotFoundError,
        match="Missing local FRED reference snapshot",
    ):
        load_verified_reference_snapshots(
            spec=spec,
            manifest=manifest,
            reference_dir=(
                fixture["reference_dir"]
            ),
        )


def test_verified_reference_loader_checks_manifest_metadata(
    tmp_path: Path,
) -> None:
    fixture = _make_fixture(
        tmp_path
    )

    manifest = pd.read_csv(
        fixture["manifest"]
    )

    manifest.loc[
        manifest["series_id"].eq(
            "OLD"
        ),
        "n_observations",
    ] = 999

    spec = pd.read_csv(
        fixture["spec"]
    )

    with pytest.raises(
        ValueError,
        match="n_observations mismatch",
    ):
        load_verified_reference_snapshots(
            spec=spec,
            manifest=manifest,
            reference_dir=(
                fixture["reference_dir"]
            ),
        )


def test_run_transition_review_writes_expected_reports(
    tmp_path: Path,
) -> None:
    fixture = _make_fixture(
        tmp_path
    )

    result = (
        run_rt_canonical_transition_review(
            spec_path=fixture["spec"],
            manifest_path=fixture[
                "manifest"
            ],
            reference_dir=fixture[
                "reference_dir"
            ],
            output_dir=fixture[
                "output_dir"
            ],
            rolling_window_months=3,
        )
    )

    assert (
        result["n_reference_series"]
        == 2
    )
    assert result["n_summary_rows"] == 1
    assert result["n_aligned_rows"] == 8
    assert result["n_rolling_rows"] == 6

    summary = pd.read_csv(
        result["summary_path"]
    )
    aligned = pd.read_csv(
        result["aligned_path"]
    )
    rolling = pd.read_csv(
        result["rolling_path"]
    )

    assert (
        summary.columns.tolist()
        == REVIEW_SUMMARY_COLUMNS
    )
    assert (
        aligned.columns.tolist()
        == ALIGNED_REVIEW_COLUMNS
    )
    assert (
        rolling.columns.tolist()
        == ROLLING_REVIEW_COLUMNS
    )

    forbidden_decision_columns = {
        "decision",
        "review_decision",
        "accepted",
        "rejected",
        "accept",
        "reject",
    }

    for frame in (
        summary,
        aligned,
        rolling,
    ):
        assert not (
            forbidden_decision_columns
            & set(frame.columns)
        )


def test_cli_runs_offline_with_verified_snapshots(
    tmp_path: Path,
) -> None:
    fixture = _make_fixture(
        tmp_path
    )

    repo_root = (
        Path(__file__)
        .resolve()
        .parents[1]
    )

    script = (
        repo_root
        / "scripts"
        / "ingestion"
        / "review_fred_md_rt_canonical_transitions.py"
    )

    cli_output_dir = (
        tmp_path
        / "cli_outputs"
    )

    env = os.environ.copy()

    src_path = str(
        repo_root / "src"
    )

    existing_pythonpath = env.get(
        "PYTHONPATH",
        "",
    )

    env["PYTHONPATH"] = (
        src_path
        if not existing_pythonpath
        else (
            src_path
            + os.pathsep
            + existing_pythonpath
        )
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--spec",
            str(fixture["spec"]),
            "--manifest",
            str(fixture["manifest"]),
            "--reference-dir",
            str(
                fixture[
                    "reference_dir"
                ]
            ),
            "--output-dir",
            str(cli_output_dir),
            "--rolling-window-months",
            "3",
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, (
        completed.stderr
    )

    assert (
        "Verified reference series: 2"
        in completed.stdout
    )
    assert (
        "Summary rows: 1"
        in completed.stdout
    )
    assert (
        "No automatic RT_CANONICAL acceptance"
        in completed.stdout
    )

    assert (
        cli_output_dir
        / "fred_md_rt_canonical_transition_review__summary.csv"
    ).exists()

    assert (
        cli_output_dir
        / "fred_md_rt_canonical_transition_review__aligned.csv"
    ).exists()

    assert (
        cli_output_dir
        / "fred_md_rt_canonical_transition_review__rolling.csv"
    ).exists()