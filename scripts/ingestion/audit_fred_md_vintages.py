#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from dfm_pipeline.ingestion.fred_md_vintages import write_audit_outputs


ENV_RAW_DIR = "FRED_MD_VINTAGE_RAW_DIR"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only audit of historical FRED-MD CSV vintages. "
            "The raw source files are never modified."
        )
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help=(
            "Directory containing raw FRED-MD vintage CSVs. "
            f"If omitted, read {ENV_RAW_DIR}."
        ),
    )
    parser.add_argument(
        "--audit-dir",
        type=Path,
        default=None,
        help=(
            "Output directory for audit CSVs. "
            "Default: sibling 'audit' directory next to the raw directory."
        ),
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search recursively for CSV files under --raw-dir.",
    )
    parser.add_argument(
        "--expected-start",
        default=None,
        help="Expected first vintage in YYYY-MM form, e.g. 1999-08.",
    )
    parser.add_argument(
        "--expected-end",
        default=None,
        help="Expected final vintage in YYYY-MM form, e.g. 2026-06.",
    )
    parser.add_argument(
        "--fail-on-invalid",
        action="store_true",
        help=(
            "Exit with status 1 after writing reports if any file fails "
            "schema validation or the collection continuity check fails."
        ),
    )
    return parser.parse_args()


def resolve_raw_dir(args: argparse.Namespace) -> Path:
    if args.raw_dir is not None:
        return args.raw_dir

    env_value = os.environ.get(ENV_RAW_DIR)
    if env_value:
        return Path(env_value)

    raise SystemExit(
        "No raw directory supplied. Use --raw-dir PATH or set "
        f"{ENV_RAW_DIR}."
    )


def main() -> int:
    args = parse_args()
    raw_dir = resolve_raw_dir(args).expanduser().resolve()
    audit_dir = (
        args.audit_dir.expanduser().resolve()
        if args.audit_dir is not None
        else raw_dir.parent / "audit"
    )

    outputs, paths = write_audit_outputs(
        raw_dir,
        audit_dir,
        recursive=args.recursive,
        expected_start=args.expected_start,
        expected_end=args.expected_end,
    )

    vintage = outputs["vintage_audit"]
    collection = outputs["collection_summary"].iloc[0]

    n_files = int(len(vintage))
    n_valid = int(vintage["schema_valid"].sum())
    n_invalid = int(n_files - n_valid)
    n_duplicate_vintages = int(vintage["duplicate_vintage"].sum())
    collection_valid = bool(collection["collection_valid"])

    print()
    print("FRED-MD vintage audit complete")
    print(f"raw_dir             : {raw_dir}")
    print(f"audit_dir           : {audit_dir}")
    print(f"csv_files           : {n_files}")
    print(f"schema_valid        : {n_valid}")
    print(f"schema_invalid      : {n_invalid}")
    print(f"duplicate_vintages  : {n_duplicate_vintages}")
    print(f"collection_valid    : {collection_valid}")
    print(
        "vintage_range       : "
        f"{collection['observed_first_vintage']} -> "
        f"{collection['observed_last_vintage']}"
    )
    print(f"missing_vintages    : {int(collection['n_missing_vintage_months'])}")
    print()

    print("Artifacts:")
    for name, path in paths.items():
        print(f"  {name:22s} {path}")

    if n_invalid:
        print()
        print("Files requiring attention:")
        cols = ["vintage", "filename", "problems"]
        print(vintage.loc[~vintage["schema_valid"], cols].to_string(index=False))

    if not collection_valid:
        print()
        print("Collection continuity issues:")
        print(outputs["collection_summary"].to_string(index=False))

    if args.fail_on_invalid and (n_invalid or not collection_valid):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
