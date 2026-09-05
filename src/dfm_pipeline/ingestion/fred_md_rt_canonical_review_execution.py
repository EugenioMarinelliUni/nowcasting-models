from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical_review import (
    ALIGNED_REVIEW_COLUMNS,
    REVIEW_SUMMARY_COLUMNS,
    ROLLING_REVIEW_COLUMNS,
    load_reference_series,
    load_transition_review_spec,
    review_transition_references,
    validate_transition_review_spec,
)
from dfm_pipeline.ingestion.fred_reference_data import (
    DOWNLOAD_METADATA_COLUMNS,
    collect_reference_requirements,
    sha256_file,
)


REFERENCE_MANIFEST_COLUMNS = [
    "series_id",
    "canonical_ids",
    "reference_roles",
    *[
        column
        for column in DOWNLOAD_METADATA_COLUMNS
        if column != "series_id"
    ],
]

SUMMARY_FILENAME = (
    "fred_md_rt_canonical_transition_review__summary.csv"
)
ALIGNED_FILENAME = (
    "fred_md_rt_canonical_transition_review__aligned.csv"
)
ROLLING_FILENAME = (
    "fred_md_rt_canonical_transition_review__rolling.csv"
)

_SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _coerce_nonnegative_int(
    value: object,
    *,
    field: str,
    series_id: str,
) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid {field} for {series_id!r}: {value!r}"
        ) from exc

    if (
        not np.isfinite(numeric)
        or not numeric.is_integer()
        or numeric < 0
    ):
        raise ValueError(
            f"Invalid {field} for {series_id!r}: {value!r}"
        )

    return int(numeric)


def validate_reference_manifest(
    manifest: pd.DataFrame,
    spec: pd.DataFrame,
) -> pd.DataFrame:
    """
    Validate the C4 external-reference provenance manifest against the
    transition-review specification.

    This validation is structural and provenance-oriented. It does not make
    any statistical or canonicalization decision.
    """
    validated_spec = validate_transition_review_spec(spec)
    requirements = collect_reference_requirements(
        validated_spec
    )

    missing_columns = [
        column
        for column in REFERENCE_MANIFEST_COLUMNS
        if column not in manifest.columns
    ]
    if missing_columns:
        raise ValueError(
            "Reference manifest missing columns: "
            + ", ".join(missing_columns)
        )

    if manifest.empty:
        raise ValueError(
            "Reference manifest must contain at least one row"
        )

    out = manifest.loc[
        :,
        REFERENCE_MANIFEST_COLUMNS,
    ].copy()

    text_columns = [
        "series_id",
        "canonical_ids",
        "reference_roles",
        "source",
        "api_endpoint",
        "retrieval_timestamp_utc",
        "fred_realtime_start",
        "fred_realtime_end",
        "local_filename",
        "first_date",
        "last_date",
        "sha256",
    ]

    for column in text_columns:
        out[column] = out[column].map(_clean_text)

    required_text_columns = [
        "series_id",
        "canonical_ids",
        "reference_roles",
        "source",
        "api_endpoint",
        "retrieval_timestamp_utc",
        "local_filename",
        "first_date",
        "last_date",
        "sha256",
    ]

    for column in required_text_columns:
        if out[column].eq("").any():
            raise ValueError(
                f"Reference manifest contains blank {column}"
            )

    duplicated = out["series_id"].duplicated(
        keep=False
    )
    if duplicated.any():
        values = sorted(
            out.loc[
                duplicated,
                "series_id",
            ].unique().tolist()
        )
        raise ValueError(
            "Duplicate series_id values in reference manifest: "
            f"{values}"
        )

    normalized_n_observations: list[int] = []
    normalized_n_finite: list[int] = []

    for _, row in out.iterrows():
        series_id = row["series_id"]

        n_observations = _coerce_nonnegative_int(
            row["n_observations"],
            field="n_observations",
            series_id=series_id,
        )
        n_finite = _coerce_nonnegative_int(
            row["n_finite"],
            field="n_finite",
            series_id=series_id,
        )

        if n_finite > n_observations:
            raise ValueError(
                f"n_finite exceeds n_observations for "
                f"{series_id!r}"
            )

        normalized_n_observations.append(
            n_observations
        )
        normalized_n_finite.append(
            n_finite
        )

    out["n_observations"] = (
        normalized_n_observations
    )
    out["n_finite"] = normalized_n_finite

    bad_sha = ~out["sha256"].map(
        lambda value: bool(
            _SHA256_PATTERN.fullmatch(value)
        )
    )
    if bad_sha.any():
        bad = out.loc[
            bad_sha,
            "series_id",
        ].tolist()
        raise ValueError(
            "Invalid SHA-256 value in reference manifest "
            f"for: {bad}"
        )

    out["sha256"] = out["sha256"].str.lower()

    first_dates = pd.to_datetime(
        out["first_date"],
        errors="coerce",
    )
    last_dates = pd.to_datetime(
        out["last_date"],
        errors="coerce",
    )

    bad_dates = (
        first_dates.isna()
        | last_dates.isna()
    )
    if bad_dates.any():
        bad = out.loc[
            bad_dates,
            "series_id",
        ].tolist()
        raise ValueError(
            "Reference manifest contains invalid date "
            f"metadata for: {bad}"
        )

    reversed_dates = first_dates > last_dates
    if reversed_dates.any():
        bad = out.loc[
            reversed_dates,
            "series_id",
        ].tolist()
        raise ValueError(
            "Reference manifest first_date exceeds "
            f"last_date for: {bad}"
        )

    out["first_date"] = first_dates.dt.strftime(
        "%Y-%m-%d"
    )
    out["last_date"] = last_dates.dt.strftime(
        "%Y-%m-%d"
    )

    for _, row in out.iterrows():
        series_id = row["series_id"]
        filename = row["local_filename"]

        if Path(filename).name != filename:
            raise ValueError(
                "Reference manifest local_filename must "
                f"be a basename for {series_id!r}: "
                f"{filename!r}"
            )

        expected_filename = f"{series_id}.csv"
        if filename != expected_filename:
            raise ValueError(
                "Unexpected local_filename for "
                f"{series_id!r}: expected "
                f"{expected_filename!r}, got "
                f"{filename!r}"
            )

    required_ids = requirements[
        "series_id"
    ].tolist()
    manifest_ids = out[
        "series_id"
    ].tolist()

    missing_series = sorted(
        set(required_ids)
        - set(manifest_ids)
    )
    extra_series = sorted(
        set(manifest_ids)
        - set(required_ids)
    )

    if missing_series:
        raise ValueError(
            "Reference manifest is missing required "
            f"series: {missing_series}"
        )

    if extra_series:
        raise ValueError(
            "Reference manifest contains unexpected "
            f"series: {extra_series}"
        )

    manifest_by_id = out.set_index(
        "series_id"
    )
    requirements_by_id = requirements.set_index(
        "series_id"
    )

    for series_id in required_ids:
        manifest_row = manifest_by_id.loc[
            series_id
        ]
        requirement_row = requirements_by_id.loc[
            series_id
        ]

        expected_canonical_ids = str(
            requirement_row["canonical_ids"]
        )
        expected_roles = str(
            requirement_row["reference_roles"]
        )

        if (
            manifest_row["canonical_ids"]
            != expected_canonical_ids
        ):
            raise ValueError(
                "Reference manifest canonical_ids "
                f"mismatch for {series_id!r}: "
                f"expected {expected_canonical_ids!r}, "
                f"got "
                f"{manifest_row['canonical_ids']!r}"
            )

        if (
            manifest_row["reference_roles"]
            != expected_roles
        ):
            raise ValueError(
                "Reference manifest reference_roles "
                f"mismatch for {series_id!r}: "
                f"expected {expected_roles!r}, got "
                f"{manifest_row['reference_roles']!r}"
            )

    # Return the manifest in the exact deterministic
    # order implied by the transition-review specification.
    ordered = (
        manifest_by_id.loc[required_ids]
        .reset_index()
    )

    return ordered.loc[
        :,
        REFERENCE_MANIFEST_COLUMNS,
    ]


def load_reference_manifest(
    path: str | Path,
    *,
    spec: pd.DataFrame,
) -> pd.DataFrame:
    """
    Load and validate the committed provenance manifest.
    """
    path = Path(path)

    manifest = pd.read_csv(path)

    return validate_reference_manifest(
        manifest,
        spec,
    )


def load_verified_reference_snapshots(
    *,
    spec: pd.DataFrame,
    manifest: pd.DataFrame,
    reference_dir: str | Path,
) -> dict[str, pd.Series]:
    """
    Verify and load every local reference snapshot required by C4.

    Verification includes:
      - specification/manifest consistency;
      - local file existence;
      - SHA-256 equality;
      - successful load through the actual C4 loader;
      - observation count;
      - finite-observation count;
      - first/last calendar dates.

    The returned mapping is suitable for
    review_transition_references(...).
    """
    reference_dir = Path(reference_dir)

    validated_manifest = (
        validate_reference_manifest(
            manifest,
            spec,
        )
    )

    references: dict[str, pd.Series] = {}

    for _, row in validated_manifest.iterrows():
        series_id = str(
            row["series_id"]
        )
        filename = str(
            row["local_filename"]
        )

        path = reference_dir / filename

        if not path.exists():
            raise FileNotFoundError(
                "Missing local FRED reference snapshot "
                f"for {series_id!r}: {path}"
            )

        if not path.is_file():
            raise ValueError(
                "Reference snapshot path is not a file "
                f"for {series_id!r}: {path}"
            )

        actual_sha256 = sha256_file(path)

        expected_sha256 = str(
            row["sha256"]
        ).lower()

        if actual_sha256 != expected_sha256:
            raise ValueError(
                "SHA-256 mismatch for reference "
                f"series {series_id!r}: expected "
                f"{expected_sha256}, got "
                f"{actual_sha256}"
            )

        series = load_reference_series(
            path,
            series_id=series_id,
        )

        actual_n_observations = int(
            len(series)
        )
        actual_n_finite = int(
            np.isfinite(
                series.to_numpy(dtype=float)
            ).sum()
        )

        if (
            actual_n_observations
            != int(row["n_observations"])
        ):
            raise ValueError(
                "n_observations mismatch for "
                f"{series_id!r}: expected "
                f"{int(row['n_observations'])}, got "
                f"{actual_n_observations}"
            )

        if (
            actual_n_finite
            != int(row["n_finite"])
        ):
            raise ValueError(
                "n_finite mismatch for "
                f"{series_id!r}: expected "
                f"{int(row['n_finite'])}, got "
                f"{actual_n_finite}"
            )

        actual_first_date = (
            pd.Timestamp(
                series.index.min()
            )
            .date()
            .isoformat()
        )
        actual_last_date = (
            pd.Timestamp(
                series.index.max()
            )
            .date()
            .isoformat()
        )

        if (
            actual_first_date
            != row["first_date"]
        ):
            raise ValueError(
                "first_date mismatch for "
                f"{series_id!r}: expected "
                f"{row['first_date']!r}, got "
                f"{actual_first_date!r}"
            )

        if (
            actual_last_date
            != row["last_date"]
        ):
            raise ValueError(
                "last_date mismatch for "
                f"{series_id!r}: expected "
                f"{row['last_date']!r}, got "
                f"{actual_last_date!r}"
            )

        references[series_id] = series

    return references


def review_output_paths(
    output_dir: str | Path,
) -> dict[str, Path]:
    """
    Return the canonical output paths for a C4 review run.
    """
    output_dir = Path(output_dir)

    return {
        "summary": (
            output_dir
            / SUMMARY_FILENAME
        ),
        "aligned": (
            output_dir
            / ALIGNED_FILENAME
        ),
        "rolling": (
            output_dir
            / ROLLING_FILENAME
        ),
    }


def _write_csv_atomic(
    frame: pd.DataFrame,
    path: Path,
) -> None:
    """
    Write one CSV through a temporary sibling file.
    """
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
            float_format="%.15g",
        )
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def run_rt_canonical_transition_review(
    *,
    spec_path: str | Path,
    manifest_path: str | Path,
    reference_dir: str | Path,
    output_dir: str | Path,
    rolling_window_months: int = 60,
    overwrite: bool = False,
) -> dict[str, Any]:
    """
    Execute the full threshold-free C4 external-reference review.

    This orchestration function:
      1. loads the validated transition-review specification;
      2. verifies the provenance manifest;
      3. verifies and loads the local FRED snapshots;
      4. calls the existing C4 statistical review core;
      5. writes summary, aligned and rolling reports.

    It deliberately does not create a canonicalization decision.
    """
    if rolling_window_months < 2:
        raise ValueError(
            "rolling_window_months must be at least 2"
        )

    spec = load_transition_review_spec(
        spec_path
    )

    manifest = load_reference_manifest(
        manifest_path,
        spec=spec,
    )

    output_paths = review_output_paths(
        output_dir
    )

    if not overwrite:
        existing = [
            path
            for path in output_paths.values()
            if path.exists()
        ]

        if existing:
            formatted = "\n".join(
                f"  {path}"
                for path in existing
            )
            raise FileExistsError(
                "Refusing to overwrite existing C4 "
                "review outputs:\n"
                f"{formatted}\n"
                "Re-run with overwrite=True if "
                "replacement is intentional."
            )

    references = load_verified_reference_snapshots(
        spec=spec,
        manifest=manifest,
        reference_dir=reference_dir,
    )

    summary, aligned, rolling = (
        review_transition_references(
            spec,
            references,
            rolling_window_months=(
                rolling_window_months
            ),
        )
    )

    expected_schemas = {
        "summary": REVIEW_SUMMARY_COLUMNS,
        "aligned": ALIGNED_REVIEW_COLUMNS,
        "rolling": ROLLING_REVIEW_COLUMNS,
    }

    frames = {
        "summary": summary,
        "aligned": aligned,
        "rolling": rolling,
    }

    for name, frame in frames.items():
        expected = expected_schemas[name]

        if frame.columns.tolist() != expected:
            raise RuntimeError(
                f"Unexpected {name} output schema. "
                f"Expected {expected}, got "
                f"{frame.columns.tolist()}"
            )

    for name, frame in frames.items():
        _write_csv_atomic(
            frame,
            output_paths[name],
        )

    return {
        "summary_path": output_paths[
            "summary"
        ],
        "aligned_path": output_paths[
            "aligned"
        ],
        "rolling_path": output_paths[
            "rolling"
        ],
        "n_reference_series": int(
            len(references)
        ),
        "n_summary_rows": int(
            len(summary)
        ),
        "n_aligned_rows": int(
            len(aligned)
        ),
        "n_rolling_rows": int(
            len(rolling)
        ),
        "rolling_window_months": int(
            rolling_window_months
        ),
    }


__all__ = [
    "REFERENCE_MANIFEST_COLUMNS",
    "SUMMARY_FILENAME",
    "ALIGNED_FILENAME",
    "ROLLING_FILENAME",
    "validate_reference_manifest",
    "load_reference_manifest",
    "load_verified_reference_snapshots",
    "review_output_paths",
    "run_rt_canonical_transition_review",
]