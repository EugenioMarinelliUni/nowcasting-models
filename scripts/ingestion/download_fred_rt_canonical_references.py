#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical_review import (
    load_transition_review_spec,
)
from dfm_pipeline.ingestion.fred_reference_data import (
    DOWNLOAD_METADATA_COLUMNS,
    collect_reference_requirements,
    download_fred_reference_series,
    get_fred_api_key,
)


DEFAULT_SPEC = Path(
    "data/metadata/series_maps/"
    "fred_md_rt_canonical_transition_review_spec.csv"
)

DEFAULT_OUTPUT_DIR = Path(
    "data/external/fred_transition_review"
)

DEFAULT_MANIFEST = Path(
    "data/metadata/series_maps/"
    "fred_md_rt_canonical_transition_review__reference_manifest.csv"
)

MANIFEST_COLUMNS = [
    "series_id",
    "canonical_ids",
    "reference_roles",
    *[
        column
        for column in DOWNLOAD_METADATA_COLUMNS
        if column != "series_id"
    ],
]


def _write_manifest(
    manifest: pd.DataFrame,
    path: Path,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        f".{path.name}.tmp"
    )

    try:
        manifest.to_csv(
            temporary,
            index=False,
        )
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download and validate the external FRED reference series "
            "required for the RT_CANONICAL C4 transition review."
        )
    )

    parser.add_argument(
        "--spec",
        type=Path,
        default=DEFAULT_SPEC,
        help=(
            "Validated RT_CANONICAL transition-review specification "
            f"(default: {DEFAULT_SPEC})"
        ),
    )

    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=(
            "Local Git-ignored directory for reference CSV snapshots "
            f"(default: {DEFAULT_OUTPUT_DIR})"
        ),
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help=(
            "Committed provenance-manifest path "
            f"(default: {DEFAULT_MANIFEST})"
        ),
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Per-request HTTP timeout in seconds (default: 30)",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Replace existing reference CSVs. Without this flag, the "
            "command refuses to start if any target CSV already exists."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.timeout <= 0:
        raise SystemExit(
            "--timeout must be positive"
        )

    # Validate the secret before touching output files.
    api_key = get_fred_api_key()

    spec = load_transition_review_spec(
        args.spec
    )

    requirements = collect_reference_requirements(
        spec
    )

    if not args.overwrite:
        existing = [
            args.out_dir / f"{series_id}.csv"
            for series_id in requirements["series_id"]
            if (
                args.out_dir
                / f"{series_id}.csv"
            ).exists()
        ]

        if existing:
            formatted = "\n".join(
                f"  {path}"
                for path in existing
            )
            raise SystemExit(
                "Refusing to overwrite existing reference CSVs:\n"
                f"{formatted}\n"
                "Re-run with --overwrite if replacement is intentional."
            )

    args.out_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    metadata_rows: list[dict[str, object]] = []

    n_series = len(requirements)

    for position, requirement in requirements.iterrows():
        series_id = str(
            requirement["series_id"]
        )

        print(
            f"[{position + 1}/{n_series}] "
            f"Downloading {series_id}..."
        )

        metadata = download_fred_reference_series(
            series_id,
            output_dir=args.out_dir,
            api_key=api_key,
            timeout=args.timeout,
            overwrite=args.overwrite,
        )

        metadata_rows.append(
            {
                "series_id": series_id,
                "canonical_ids": requirement[
                    "canonical_ids"
                ],
                "reference_roles": requirement[
                    "reference_roles"
                ],
                **{
                    key: value
                    for key, value in metadata.items()
                    if key != "series_id"
                },
            }
        )

        print(
            "    "
            f"rows={metadata['n_observations']} "
            f"finite={metadata['n_finite']} "
            f"range={metadata['first_date']}.."
            f"{metadata['last_date']}"
        )

    manifest = pd.DataFrame(
        metadata_rows,
        columns=MANIFEST_COLUMNS,
    )

    _write_manifest(
        manifest,
        args.manifest,
    )

    print()
    print(
        f"Downloaded {len(manifest)} reference series."
    )
    print(
        f"Reference directory: {args.out_dir}"
    )
    print(
        f"Provenance manifest: {args.manifest}"
    )


if __name__ == "__main__":
    main()