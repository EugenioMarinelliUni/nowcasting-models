from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import dfm_pipeline.ingestion.fred_reference_data as module
from dfm_pipeline.ingestion.fred_reference_data import (
    collect_reference_requirements,
    download_fred_reference_series,
    fetch_fred_series_observations,
    fred_observations_to_frame,
    get_fred_api_key,
    sha256_file,
    validate_fred_reference_csv,
    write_fred_reference_csv,
)


VALID_KEY = "a" * 32


def _payload(
    *,
    values: list[str] | None = None,
) -> dict[str, object]:
    if values is None:
        values = [
            "100.0",
            "101.5",
            ".",
            "103.0",
        ]

    observations = [
        {
            "realtime_start": "2026-09-05",
            "realtime_end": "2026-09-05",
            "date": date,
            "value": value,
        }
        for date, value in zip(
            [
                "2000-01-01",
                "2000-02-01",
                "2000-03-01",
                "2000-04-01",
            ],
            values,
            strict=True,
        )
    ]

    return {
        "realtime_start": "2026-09-05",
        "realtime_end": "2026-09-05",
        "observations": observations,
    }


def test_get_fred_api_key_reads_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "FRED_API_KEY",
        VALID_KEY,
    )

    assert get_fred_api_key() == VALID_KEY


def test_get_fred_api_key_rejects_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(
        "FRED_API_KEY",
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match="environment variable is not set",
    ):
        get_fred_api_key()


def test_get_fred_api_key_rejects_bad_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "FRED_API_KEY",
        "not-a-valid-key",
    )

    with pytest.raises(
        RuntimeError,
        match="expected FRED API-key format",
    ):
        get_fred_api_key()


def test_fred_observations_to_frame_converts_missing_token() -> None:
    frame = fred_observations_to_frame(
        _payload(),
        series_id="TEST",
    )

    assert frame.columns.tolist() == [
        "observation_date",
        "TEST",
    ]

    assert frame["observation_date"].tolist() == [
        pd.Timestamp("2000-01-01"),
        pd.Timestamp("2000-02-01"),
        pd.Timestamp("2000-03-01"),
        pd.Timestamp("2000-04-01"),
    ]

    assert frame.loc[0, "TEST"] == 100.0
    assert frame.loc[1, "TEST"] == 101.5
    assert np.isnan(frame.loc[2, "TEST"])
    assert frame.loc[3, "TEST"] == 103.0


def test_fred_observations_to_frame_rejects_bad_numeric() -> None:
    payload = _payload(
        values=[
            "100.0",
            "not-numeric",
            "102.0",
            "103.0",
        ]
    )

    with pytest.raises(
        ValueError,
        match="non-numeric observations",
    ):
        fred_observations_to_frame(
            payload,
            series_id="TEST",
        )


def test_fred_observations_to_frame_rejects_duplicate_dates() -> None:
    payload = _payload()

    observations = payload["observations"]
    assert isinstance(observations, list)

    observations[1]["date"] = "2000-01-01"

    with pytest.raises(
        ValueError,
        match="duplicate dates",
    ):
        fred_observations_to_frame(
            payload,
            series_id="TEST",
        )


class _FakeResponse:
    status_code = 200

    def json(self) -> dict[str, object]:
        return _payload()


def test_fetch_fred_series_observations_uses_current_reference_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_get(
        url: str,
        *,
        params: dict[str, object],
        timeout: int,
    ) -> _FakeResponse:
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(
        module.requests,
        "get",
        fake_get,
    )

    payload = fetch_fred_series_observations(
        "TEST",
        api_key=VALID_KEY,
        timeout=17,
    )

    assert "observations" in payload
    assert captured["url"] == module.FRED_OBSERVATIONS_URL
    assert captured["timeout"] == 17

    params = captured["params"]
    assert isinstance(params, dict)

    assert params["series_id"] == "TEST"
    assert params["api_key"] == VALID_KEY
    assert params["file_type"] == "json"

    assert "realtime_start" not in params
    assert "realtime_end" not in params
    assert "vintage_dates" not in params


def test_write_and_validate_reference_csv(
    tmp_path: Path,
) -> None:
    frame = fred_observations_to_frame(
        _payload(),
        series_id="TEST",
    )

    path = tmp_path / "TEST.csv"

    write_fred_reference_csv(
        frame,
        series_id="TEST",
        path=path,
    )

    series = validate_fred_reference_csv(
        path,
        series_id="TEST",
    )

    assert path.exists()
    assert len(series) == 4
    assert int(series.notna().sum()) == 3
    assert series.index.min() == pd.Timestamp(
        "2000-01-01"
    )
    assert series.index.max() == pd.Timestamp(
        "2000-04-01"
    )


def test_write_reference_csv_refuses_unintentional_overwrite(
    tmp_path: Path,
) -> None:
    frame = fred_observations_to_frame(
        _payload(),
        series_id="TEST",
    )

    path = tmp_path / "TEST.csv"

    write_fred_reference_csv(
        frame,
        series_id="TEST",
        path=path,
    )

    with pytest.raises(
        FileExistsError,
        match="already exists",
    ):
        write_fred_reference_csv(
            frame,
            series_id="TEST",
            path=path,
        )


def test_sha256_file_is_deterministic(
    tmp_path: Path,
) -> None:
    path = tmp_path / "example.txt"
    path.write_bytes(b"abc")

    expected = hashlib.sha256(
        b"abc"
    ).hexdigest()

    assert sha256_file(path) == expected


def test_collect_reference_requirements_deduplicates_series() -> None:
    spec = pd.DataFrame(
        [
            {
                "canonical_id": "X",
                "old_reference_series": "OLD",
                "new_reference_series": "SHARED",
                "aux_reference_series": "",
            },
            {
                "canonical_id": "Y",
                "old_reference_series": "SHARED",
                "new_reference_series": "NEW",
                "aux_reference_series": "AUX",
            },
        ]
    )

    out = collect_reference_requirements(
        spec
    )

    assert out["series_id"].tolist() == [
        "OLD",
        "SHARED",
        "NEW",
        "AUX",
    ]

    shared = out.loc[
        out["series_id"].eq("SHARED")
    ].iloc[0]

    assert shared["canonical_ids"] == "X|Y"
    assert (
        shared["reference_roles"]
        == "new_reference|old_reference"
    )


def test_download_reference_series_end_to_end_without_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_fetch(
        series_id: str,
        *,
        api_key: str,
        timeout: int = 30,
        api_url: str = module.FRED_OBSERVATIONS_URL,
    ) -> dict[str, object]:
        assert series_id == "TEST"
        assert api_key == VALID_KEY
        assert timeout == 11
        return _payload()

    monkeypatch.setattr(
        module,
        "fetch_fred_series_observations",
        fake_fetch,
    )

    metadata = download_fred_reference_series(
        "TEST",
        output_dir=tmp_path,
        api_key=VALID_KEY,
        timeout=11,
    )

    path = tmp_path / "TEST.csv"

    assert path.exists()

    assert metadata["series_id"] == "TEST"
    assert metadata["source"] == "FRED"
    assert metadata["local_filename"] == "TEST.csv"
    assert metadata["n_observations"] == 4
    assert metadata["n_finite"] == 3
    assert metadata["first_date"] == "2000-01-01"
    assert metadata["last_date"] == "2000-04-01"
    assert metadata["fred_realtime_start"] == "2026-09-05"
    assert metadata["fred_realtime_end"] == "2026-09-05"

    assert metadata["sha256"] == sha256_file(
        path
    )

    assert VALID_KEY not in str(metadata)
    assert "api_key" not in metadata