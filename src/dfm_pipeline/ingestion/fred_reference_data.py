from __future__ import annotations

import hashlib
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from dfm_pipeline.ingestion.fred_md_rt_canonical_review import (
    load_reference_series,
)


FRED_OBSERVATIONS_URL = (
    "https://api.stlouisfed.org/fred/series/observations"
)

REFERENCE_REQUIREMENT_COLUMNS = [
    "series_id",
    "canonical_ids",
    "reference_roles",
]

DOWNLOAD_METADATA_COLUMNS = [
    "series_id",
    "source",
    "api_endpoint",
    "retrieval_timestamp_utc",
    "fred_realtime_start",
    "fred_realtime_end",
    "local_filename",
    "n_observations",
    "n_finite",
    "first_date",
    "last_date",
    "sha256",
]

_SERIES_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


def _validate_series_id(series_id: str) -> str:
    value = str(series_id).strip()

    if not value:
        raise ValueError("FRED series_id must be non-empty")

    if not _SERIES_ID_PATTERN.fullmatch(value):
        raise ValueError(
            f"Invalid FRED series_id: {series_id!r}"
        )

    return value


def get_fred_api_key(
    *,
    env_var: str = "FRED_API_KEY",
) -> str:
    """
    Read and validate the FRED API key from the environment.

    The key is deliberately not accepted as a command-line argument so it
    cannot accidentally enter shell history or ordinary CLI diagnostics.
    """
    raw = os.environ.get(env_var)
    if raw is None:
        raise RuntimeError(
            f"{env_var} environment variable is not set"
        )

    key = raw.strip()

    if not re.fullmatch(r"[a-z0-9]{32}", key):
        raise RuntimeError(
            f"{env_var} does not have the expected FRED API-key format"
        )

    return key


def fetch_fred_series_observations(
    series_id: str,
    *,
    api_key: str,
    timeout: int = 30,
    api_url: str = FRED_OBSERVATIONS_URL,
) -> dict[str, Any]:
    """
    Fetch the current FRED observation payload for one series.

    No ALFRED vintage parameters are supplied. The result is therefore used as
    current external/reference evidence, not as a pseudo-real-time model input.

    Errors are deliberately sanitized so the API key is never included in
    exception messages.
    """
    series_id = _validate_series_id(series_id)

    if timeout <= 0:
        raise ValueError("timeout must be positive")

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "asc",
        "limit": 100000,
    }

    try:
        response = requests.get(
            api_url,
            params=params,
            timeout=timeout,
        )
    except requests.RequestException:
        raise RuntimeError(
            f"FRED request failed for series {series_id!r}"
        ) from None

    if not 200 <= int(response.status_code) < 300:
        raise RuntimeError(
            "FRED request failed for "
            f"series {series_id!r}: HTTP {response.status_code}"
        )

    try:
        payload = response.json()
    except ValueError:
        raise RuntimeError(
            f"FRED returned invalid JSON for series {series_id!r}"
        ) from None

    if not isinstance(payload, dict):
        raise RuntimeError(
            f"FRED returned an invalid payload for series {series_id!r}"
        )

    observations = payload.get("observations")
    if not isinstance(observations, list) or not observations:
        raise RuntimeError(
            f"FRED returned no observations for series {series_id!r}"
        )

    return payload


def fred_observations_to_frame(
    payload: dict[str, Any],
    *,
    series_id: str,
) -> pd.DataFrame:
    """
    Convert a FRED observations JSON payload to the C4 reference CSV schema.

    The returned columns are:
        observation_date, <series_id>

    FRED's "." missing-value token becomes NaN. Unexpected non-numeric tokens,
    bad dates and duplicate reference dates are rejected.
    """
    series_id = _validate_series_id(series_id)

    observations = payload.get("observations")
    if not isinstance(observations, list) or not observations:
        raise ValueError(
            f"No observations available for series {series_id!r}"
        )

    dates: list[object] = []
    raw_values: list[object] = []

    for position, observation in enumerate(observations):
        if not isinstance(observation, dict):
            raise ValueError(
                "FRED observation must be an object at "
                f"position {position}"
            )

        if "date" not in observation:
            raise ValueError(
                f"FRED observation missing date at position {position}"
            )

        if "value" not in observation:
            raise ValueError(
                f"FRED observation missing value at position {position}"
            )

        dates.append(observation["date"])
        raw_values.append(observation["value"])

    parsed_dates = pd.to_datetime(
        pd.Series(dates),
        errors="coerce",
    )

    if parsed_dates.isna().any():
        raise ValueError(
            f"FRED payload contains unparseable dates for {series_id!r}"
        )

    if parsed_dates.duplicated().any():
        raise ValueError(
            f"FRED payload contains duplicate dates for {series_id!r}"
        )

    raw = pd.Series(raw_values, dtype="object")
    text = raw.astype("string").str.strip()

    missing_token = text.eq(".") | text.eq("")
    numeric = pd.to_numeric(
        raw.where(~missing_token),
        errors="coerce",
    )

    bad_numeric = (
        numeric.isna()
        & ~missing_token
        & raw.notna()
    )

    if bad_numeric.any():
        raise ValueError(
            f"FRED payload contains non-numeric observations "
            f"for {series_id!r}"
        )

    frame = pd.DataFrame(
        {
            "observation_date": parsed_dates,
            series_id: numeric.astype(float),
        }
    )

    return frame.sort_values(
        "observation_date"
    ).reset_index(drop=True)


def write_fred_reference_csv(
    frame: pd.DataFrame,
    *,
    series_id: str,
    path: str | Path,
    overwrite: bool = False,
) -> Path:
    """
    Write a normalized FRED reference CSV atomically.
    """
    series_id = _validate_series_id(series_id)
    path = Path(path)

    expected_columns = [
        "observation_date",
        series_id,
    ]
    if frame.columns.tolist() != expected_columns:
        raise ValueError(
            "Reference frame must have columns "
            f"{expected_columns}, got {frame.columns.tolist()}"
        )

    if path.exists() and not overwrite:
        raise FileExistsError(
            f"Reference CSV already exists: {path}"
        )

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        f".{path.name}.tmp"
    )

    try:
        frame.to_csv(
            temporary,
            index=False,
            date_format="%Y-%m-%d",
            float_format="%.15g",
        )
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()

    return path


def validate_fred_reference_csv(
    path: str | Path,
    *,
    series_id: str,
) -> pd.Series:
    """
    Validate a downloaded CSV using the same loader used by C4.
    """
    return load_reference_series(
        path,
        series_id=series_id,
    )


def sha256_file(
    path: str | Path,
) -> str:
    """
    Compute a lowercase SHA-256 digest for one file.
    """
    digest = hashlib.sha256()

    with Path(path).open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def collect_reference_requirements(
    spec: pd.DataFrame,
) -> pd.DataFrame:
    """
    Derive the unique external FRED series required by a C4 review spec.

    Roles and canonical IDs are aggregated deterministically so the downloader
    does not maintain a second hard-coded list of reference series.
    """
    required_columns = [
        "canonical_id",
        "old_reference_series",
        "new_reference_series",
        "aux_reference_series",
    ]

    missing = [
        column
        for column in required_columns
        if column not in spec.columns
    ]
    if missing:
        raise ValueError(
            "Reference specification missing columns: "
            + ", ".join(missing)
        )

    collected: dict[str, dict[str, list[str]]] = {}

    role_columns = [
        ("old_reference_series", "old_reference"),
        ("new_reference_series", "new_reference"),
        ("aux_reference_series", "aux_reference"),
    ]

    for _, row in spec.iterrows():
        canonical_id = str(
            row["canonical_id"]
        ).strip()

        for column, role in role_columns:
            raw = row[column]

            if pd.isna(raw):
                continue

            series_id = str(raw).strip()
            if not series_id:
                continue

            series_id = _validate_series_id(
                series_id
            )

            entry = collected.setdefault(
                series_id,
                {
                    "canonical_ids": [],
                    "reference_roles": [],
                },
            )

            if canonical_id not in entry["canonical_ids"]:
                entry["canonical_ids"].append(
                    canonical_id
                )

            if role not in entry["reference_roles"]:
                entry["reference_roles"].append(
                    role
                )

    rows = [
        {
            "series_id": series_id,
            "canonical_ids": "|".join(
                values["canonical_ids"]
            ),
            "reference_roles": "|".join(
                values["reference_roles"]
            ),
        }
        for series_id, values in collected.items()
    ]

    if not rows:
        raise ValueError(
            "Reference specification contains no usable series"
        )

    return pd.DataFrame(
        rows,
        columns=REFERENCE_REQUIREMENT_COLUMNS,
    )


def download_fred_reference_series(
    series_id: str,
    *,
    output_dir: str | Path,
    api_key: str | None = None,
    timeout: int = 30,
    overwrite: bool = False,
) -> dict[str, Any]:
    """
    Fetch, normalize, write, validate and fingerprint one FRED series.

    The intermediate JSON payload remains in memory. Only the normalized CSV
    is written to disk.

    Returns provenance metadata suitable for a committed manifest. The API key
    is never included in the returned metadata.
    """
    series_id = _validate_series_id(series_id)
    output_dir = Path(output_dir)

    if api_key is None:
        api_key = get_fred_api_key()

    payload = fetch_fred_series_observations(
        series_id,
        api_key=api_key,
        timeout=timeout,
    )

    frame = fred_observations_to_frame(
        payload,
        series_id=series_id,
    )

    path = (
        output_dir
        / f"{series_id}.csv"
    )

    write_fred_reference_csv(
        frame,
        series_id=series_id,
        path=path,
        overwrite=overwrite,
    )

    try:
        loaded = validate_fred_reference_csv(
            path,
            series_id=series_id,
        )
    except Exception:
        path.unlink(missing_ok=True)
        raise

    finite = np.isfinite(
        loaded.to_numpy(dtype=float)
    )

    retrieval_timestamp = datetime.now(
        UTC
    ).isoformat(timespec="seconds")

    first_date = (
        pd.Timestamp(loaded.index.min())
        .date()
        .isoformat()
    )
    last_date = (
        pd.Timestamp(loaded.index.max())
        .date()
        .isoformat()
    )

    return {
        "series_id": series_id,
        "source": "FRED",
        "api_endpoint": FRED_OBSERVATIONS_URL,
        "retrieval_timestamp_utc": retrieval_timestamp,
        "fred_realtime_start": str(
            payload.get("realtime_start", "")
        ),
        "fred_realtime_end": str(
            payload.get("realtime_end", "")
        ),
        "local_filename": path.name,
        "n_observations": int(len(loaded)),
        "n_finite": int(finite.sum()),
        "first_date": first_date,
        "last_date": last_date,
        "sha256": sha256_file(path),
    }


__all__ = [
    "FRED_OBSERVATIONS_URL",
    "REFERENCE_REQUIREMENT_COLUMNS",
    "DOWNLOAD_METADATA_COLUMNS",
    "get_fred_api_key",
    "fetch_fred_series_observations",
    "fred_observations_to_frame",
    "write_fred_reference_csv",
    "validate_fred_reference_csv",
    "sha256_file",
    "collect_reference_requirements",
    "download_fred_reference_series",
]