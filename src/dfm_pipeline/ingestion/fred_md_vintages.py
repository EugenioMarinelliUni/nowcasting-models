from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md import (
    detect_tcode_row,
    read_embedded_tcode_map,
)
from dfm_pipeline.utils.hashing import sha256_file
from dfm_pipeline.validation.panel_missing_diagnostics import missing_runs_by_series
from dfm_pipeline.validation.tcode_map import validate_tcode_map_against_columns


MISSING_TOKENS = {
    "",
    ".",
    "NA",
    "N/A",
    "NaN",
    "nan",
    "NULL",
    "null",
}

_FILENAME_PATTERNS = (
    re.compile(
        r"(?P<year>19\d{2}|20\d{2})[-_](?P<month>0[1-9]|1[0-2])(?!\d)",
        flags=re.IGNORECASE,
    ),
    re.compile(
        r"(?P<year>19\d{2}|20\d{2})[mM](?P<month>1[0-2]|0?[1-9])(?!\d)",
        flags=re.IGNORECASE,
    ),
)


def parse_vintage_filename(path_or_name: str | Path) -> str | None:
    """Infer canonical ``YYYY-MM`` vintage from a historical FRED-MD filename."""
    stem = Path(path_or_name).stem
    for pattern in _FILENAME_PATTERNS:
        match = pattern.search(stem)
        if match is not None:
            year = int(match.group("year"))
            month = int(match.group("month"))
            return f"{year:04d}-{month:02d}"
    return None


def read_literal_header(csv_path: str | Path) -> list[str]:
    """Read the literal CSV header without pandas duplicate-name mangling."""
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            return next(reader)
        except StopIteration:
            return []


def read_literal_row(csv_path: str | Path, row_idx: int) -> list[str] | None:
    """Return one literal CSV row by 0-based file-row index, including header."""
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for idx, row in enumerate(csv.reader(handle)):
            if idx == row_idx:
                return row
    return None


def classify_missingness(series: pd.Series) -> dict[str, Any]:
    """Classify missing observations as leading, trailing, internal, or all-missing."""
    mask = series.isna().to_numpy(dtype=bool)
    n_rows = int(mask.size)
    n_missing = int(mask.sum())

    if n_rows == 0:
        return {
            "n_rows": 0,
            "n_missing": 0,
            "n_leading_missing": 0,
            "n_trailing_missing": 0,
            "n_internal_missing": 0,
            "all_missing": False,
            "first_valid_date": None,
            "last_valid_date": None,
        }

    valid_pos = np.flatnonzero(~mask)
    if valid_pos.size == 0:
        return {
            "n_rows": n_rows,
            "n_missing": n_missing,
            "n_leading_missing": n_rows,
            "n_trailing_missing": 0,
            "n_internal_missing": 0,
            "all_missing": True,
            "first_valid_date": None,
            "last_valid_date": None,
        }

    first_pos = int(valid_pos[0])
    last_pos = int(valid_pos[-1])
    first_valid_date = series.index[first_pos]
    last_valid_date = series.index[last_pos]

    return {
        "n_rows": n_rows,
        "n_missing": n_missing,
        "n_leading_missing": first_pos,
        "n_trailing_missing": n_rows - last_pos - 1,
        "n_internal_missing": int(mask[first_pos : last_pos + 1].sum()),
        "all_missing": False,
        "first_valid_date": pd.Timestamp(first_valid_date),
        "last_valid_date": pd.Timestamp(last_valid_date),
    }


def _problem(
    problems: list[str],
    anomaly_rows: list[dict[str, Any]],
    *,
    vintage: str | None,
    filename: str,
    code: str,
    detail: str | None = None,
) -> None:
    problems.append(code)
    anomaly_rows.append(
        {
            "vintage": vintage,
            "filename": filename,
            "series": None,
            "anomaly_type": code,
            "start": None,
            "end": None,
            "length": None,
            "detail": detail,
        }
    )


def _load_numeric_panel(
    csv_path: Path,
    *,
    tcode_row: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load the data part of a raw FRED-MD file in memory without modifying source."""
    skiprows = list(range(1, tcode_row + 1))
    raw = pd.read_csv(
        csv_path,
        skiprows=skiprows,
        dtype=str,
        keep_default_na=False,
    )

    diagnostics: dict[str, Any] = {
        "date_parse_failures": 0,
        "duplicate_reference_dates": 0,
        "dates_monotonic_in_input": True,
        "non_month_start_dates": 0,
        "monthly_grid_gaps": 0,
        "bad_numeric_cells": 0,
    }

    if "sasdate" not in raw.columns:
        raise ValueError("'sasdate' not present after loading the data rows.")

    date_strings = raw["sasdate"].astype("string").str.strip()
    dates = pd.to_datetime(
        date_strings.mask(date_strings.isin(MISSING_TOKENS)),
        errors="coerce",
    )

    diagnostics["date_parse_failures"] = int(dates.isna().sum())
    valid_dates_input_order = pd.DatetimeIndex(dates.dropna())
    diagnostics["duplicate_reference_dates"] = int(
        valid_dates_input_order.duplicated().sum()
    )
    diagnostics["dates_monotonic_in_input"] = bool(
        valid_dates_input_order.is_monotonic_increasing
    )
    diagnostics["non_month_start_dates"] = int(
        (valid_dates_input_order.day != 1).sum()
    )

    predictor_cols = [c for c in raw.columns if c != "sasdate"]
    numeric = pd.DataFrame(index=raw.index, columns=predictor_cols, dtype=float)

    n_bad_numeric = 0
    for col in predictor_cols:
        values = raw[col].astype("string").str.strip()
        missing_mask = values.isin(MISSING_TOKENS) | values.isna()
        converted = pd.to_numeric(values.mask(missing_mask), errors="coerce")
        invalid_nonmissing = (~missing_mask) & converted.isna()
        n_bad_numeric += int(invalid_nonmissing.sum())
        numeric[col] = converted.astype(float)

    diagnostics["bad_numeric_cells"] = n_bad_numeric

    valid_date_mask = dates.notna()
    panel = numeric.loc[valid_date_mask].copy()
    panel.index = pd.DatetimeIndex(dates.loc[valid_date_mask])
    panel.index.name = "sasdate"
    panel = panel.sort_index()

    if len(panel.index):
        unique_index = pd.DatetimeIndex(panel.index.unique()).sort_values()
        expected = pd.date_range(unique_index.min(), unique_index.max(), freq="MS")
        diagnostics["monthly_grid_gaps"] = int(
            len(expected.difference(unique_index))
        )

    return panel, diagnostics


def audit_single_vintage(
    csv_path: str | Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Read-only audit of one raw FRED-MD vintage."""
    path = Path(csv_path)
    vintage = parse_vintage_filename(path.name)
    problems: list[str] = []
    series_rows: list[dict[str, Any]] = []
    anomaly_rows: list[dict[str, Any]] = []

    if vintage is None:
        _problem(
            problems,
            anomaly_rows,
            vintage=vintage,
            filename=path.name,
            code="unrecognized_vintage_filename",
            detail="Filename does not contain a recognizable YYYY-MM vintage.",
        )

    try:
        header = read_literal_header(path)
    except Exception as exc:
        header = []
        _problem(
            problems,
            anomaly_rows,
            vintage=vintage,
            filename=path.name,
            code="header_read_error",
            detail=f"{type(exc).__name__}: {exc}",
        )

    if not header:
        _problem(
            problems,
            anomaly_rows,
            vintage=vintage,
            filename=path.name,
            code="empty_header",
        )
        return (
            {
                "vintage": vintage,
                "filename": path.name,
                "source_path": str(path),
                "sha256": sha256_file(path),
                "n_series": 0,
                "first_column": None,
                "sasdate_ok": False,
                "transform_marker_ok": False,
                "tcode_row": None,
                "tcode_valid": False,
                "n_missing_tcodes": None,
                "n_extra_tcodes": None,
                "n_duplicate_columns": None,
                "n_rows": None,
                "first_reference_date": None,
                "last_reference_date": None,
                "date_parse_failures": None,
                "duplicate_reference_dates": None,
                "dates_monotonic_in_input": None,
                "non_month_start_dates": None,
                "monthly_grid_gaps": None,
                "bad_numeric_cells": None,
                "n_missing_values": None,
                "pct_missing_values": None,
                "n_series_with_leading_missing": None,
                "n_series_with_trailing_missing": None,
                "n_series_with_internal_missing": None,
                "n_all_missing_series": None,
                "schema_valid": False,
                "problems": ";".join(dict.fromkeys(problems)),
            },
            series_rows,
            anomaly_rows,
        )

    first_column = header[0].strip()
    sasdate_ok = first_column == "sasdate"
    if not sasdate_ok:
        _problem(
            problems,
            anomaly_rows,
            vintage=vintage,
            filename=path.name,
            code="first_column_not_sasdate",
            detail=f"Found first column {first_column!r}.",
        )

    counts: dict[str, int] = {}
    for name in header:
        counts[name] = counts.get(name, 0) + 1
    duplicate_columns = sorted(name for name, count in counts.items() if count > 1)
    if duplicate_columns:
        _problem(
            problems,
            anomaly_rows,
            vintage=vintage,
            filename=path.name,
            code="duplicate_columns",
            detail=",".join(duplicate_columns[:20]),
        )

    tcode_row: int | None = None
    tcode_map: dict[str, int] = {}
    transform_marker_ok = False
    tcode_valid = False
    missing_tcodes: list[str] = []
    extra_tcodes: list[str] = []

    try:
        tcode_row = detect_tcode_row(path, date_col="sasdate", max_scan_rows=5)
    except Exception as exc:
        _problem(
            problems,
            anomaly_rows,
            vintage=vintage,
            filename=path.name,
            code="tcode_detection_error",
            detail=f"{type(exc).__name__}: {exc}",
        )

    # If numeric-code autodetection fails (for example because a malformed code
    # is present), still locate an explicit Transform: marker so the audit can
    # report the bad/incomplete t-code map rather than merely saying the row was
    # not found.
    if tcode_row is None:
        for candidate in range(1, 6):
            literal = read_literal_row(path, candidate)
            if literal and literal[0].strip() == "Transform:":
                tcode_row = candidate
                break

    if tcode_row is None:
        _problem(
            problems,
            anomaly_rows,
            vintage=vintage,
            filename=path.name,
            code="tcode_row_not_found",
        )
    else:
        literal_tcode_row = read_literal_row(path, tcode_row)
        transform_marker = (
            literal_tcode_row[0].strip()
            if literal_tcode_row and len(literal_tcode_row) > 0
            else None
        )
        transform_marker_ok = transform_marker == "Transform:"
        if not transform_marker_ok:
            _problem(
                problems,
                anomaly_rows,
                vintage=vintage,
                filename=path.name,
                code="transform_marker_invalid",
                detail=f"Found {transform_marker!r} on t-code row.",
            )

        try:
            tcode_map = read_embedded_tcode_map(
                path,
                date_col="sasdate",
                tcode_row=tcode_row,
            )
            _, tcode_check = validate_tcode_map_against_columns(
                df_columns=header,
                tcode_map=tcode_map,
                date_col="sasdate",
                require_all=False,
            )
            missing_tcodes = list(tcode_check["missing_in_tcodes"])
            extra_tcodes = list(tcode_check["extra_in_tcodes"])
            tcode_valid = (
                len(missing_tcodes) == 0
                and len(extra_tcodes) == 0
                and len(tcode_map) == len(header) - 1
            )
            if not tcode_valid:
                _problem(
                    problems,
                    anomaly_rows,
                    vintage=vintage,
                    filename=path.name,
                    code="invalid_or_incomplete_tcodes",
                    detail=f"missing={missing_tcodes[:20]}, extra={extra_tcodes[:20]}",
                )
        except Exception as exc:
            _problem(
                problems,
                anomaly_rows,
                vintage=vintage,
                filename=path.name,
                code="tcode_validation_error",
                detail=f"{type(exc).__name__}: {exc}",
            )

    panel: pd.DataFrame | None = None
    panel_diag: dict[str, Any] = {
        "date_parse_failures": None,
        "duplicate_reference_dates": None,
        "dates_monotonic_in_input": None,
        "non_month_start_dates": None,
        "monthly_grid_gaps": None,
        "bad_numeric_cells": None,
    }

    if tcode_row is not None:
        try:
            panel, panel_diag = _load_numeric_panel(path, tcode_row=tcode_row)
        except Exception as exc:
            _problem(
                problems,
                anomaly_rows,
                vintage=vintage,
                filename=path.name,
                code="panel_read_error",
                detail=f"{type(exc).__name__}: {exc}",
            )

    if panel is not None:
        if panel_diag["date_parse_failures"]:
            _problem(problems, anomaly_rows, vintage=vintage, filename=path.name,
                     code="date_parse_failure", detail=str(panel_diag["date_parse_failures"]))
        if panel_diag["duplicate_reference_dates"]:
            _problem(problems, anomaly_rows, vintage=vintage, filename=path.name,
                     code="duplicate_reference_dates", detail=str(panel_diag["duplicate_reference_dates"]))
        if not panel_diag["dates_monotonic_in_input"]:
            _problem(problems, anomaly_rows, vintage=vintage, filename=path.name,
                     code="reference_dates_not_monotonic")
        if panel_diag["non_month_start_dates"]:
            _problem(problems, anomaly_rows, vintage=vintage, filename=path.name,
                     code="non_month_start_dates", detail=str(panel_diag["non_month_start_dates"]))
        if panel_diag["monthly_grid_gaps"]:
            _problem(problems, anomaly_rows, vintage=vintage, filename=path.name,
                     code="monthly_grid_gaps", detail=str(panel_diag["monthly_grid_gaps"]))
        if panel_diag["bad_numeric_cells"]:
            _problem(problems, anomaly_rows, vintage=vintage, filename=path.name,
                     code="non_numeric_data_cells", detail=str(panel_diag["bad_numeric_cells"]))

    total_missing: int | None = None
    pct_missing: float | None = None
    n_series_leading: int | None = None
    n_series_trailing: int | None = None
    n_series_internal: int | None = None
    n_all_missing: int | None = None

    if panel is not None:
        total_missing = int(panel.isna().sum().sum())
        total_cells = int(panel.shape[0] * panel.shape[1])
        pct_missing = 100.0 * total_missing / total_cells if total_cells > 0 else np.nan
        runs = missing_runs_by_series(panel)

        n_series_leading = 0
        n_series_trailing = 0
        n_series_internal = 0
        n_all_missing = 0

        for series_name in panel.columns:
            s = panel[series_name]
            missing = classify_missingness(s)

            if missing["n_leading_missing"] > 0:
                n_series_leading += 1
            if missing["n_trailing_missing"] > 0:
                n_series_trailing += 1
            if missing["n_internal_missing"] > 0:
                n_series_internal += 1
            if missing["all_missing"]:
                n_all_missing += 1

            series_runs = runs[runs["series"] == series_name]
            n_runs = int(len(series_runs))
            longest_run = int(series_runs["length"].max()) if not series_runs.empty else 0
            first_valid = missing["first_valid_date"]
            last_valid = missing["last_valid_date"]

            series_rows.append(
                {
                    "vintage": vintage,
                    "filename": path.name,
                    "series": series_name,
                    "tcode": tcode_map.get(series_name),
                    "n_rows": int(len(s)),
                    "n_valid": int(s.notna().sum()),
                    "n_missing": int(missing["n_missing"]),
                    "pct_missing": 100.0 * missing["n_missing"] / len(s) if len(s) > 0 else np.nan,
                    "n_leading_missing": int(missing["n_leading_missing"]),
                    "n_trailing_missing": int(missing["n_trailing_missing"]),
                    "n_internal_missing": int(missing["n_internal_missing"]),
                    "all_missing": bool(missing["all_missing"]),
                    "first_valid_date": (
                        pd.Timestamp(first_valid).date().isoformat() if first_valid is not None else None
                    ),
                    "last_valid_date": (
                        pd.Timestamp(last_valid).date().isoformat() if last_valid is not None else None
                    ),
                    "n_missing_runs": n_runs,
                    "longest_missing_run": longest_run,
                }
            )

            if missing["all_missing"]:
                anomaly_rows.append(
                    {
                        "vintage": vintage,
                        "filename": path.name,
                        "series": series_name,
                        "anomaly_type": "all_missing_series",
                        "start": None,
                        "end": None,
                        "length": int(len(s)),
                        "detail": None,
                    }
                )

            if first_valid is not None and last_valid is not None:
                for _, run in series_runs.iterrows():
                    start = pd.Timestamp(run["start"])
                    end = pd.Timestamp(run["end"])
                    if start > first_valid and end < last_valid:
                        anomaly_rows.append(
                            {
                                "vintage": vintage,
                                "filename": path.name,
                                "series": series_name,
                                "anomaly_type": "internal_missing_run",
                                "start": start.date().isoformat(),
                                "end": end.date().isoformat(),
                                "length": int(run["length"]),
                                "detail": None,
                            }
                        )

    unique_problems = list(dict.fromkeys(problems))
    vintage_row = {
        "vintage": vintage,
        "filename": path.name,
        "source_path": str(path),
        "sha256": sha256_file(path),
        "n_series": max(len(header) - 1, 0),
        "first_column": first_column,
        "sasdate_ok": sasdate_ok,
        "transform_marker_ok": transform_marker_ok,
        "tcode_row": tcode_row,
        "tcode_valid": tcode_valid,
        "n_missing_tcodes": len(missing_tcodes),
        "n_extra_tcodes": len(extra_tcodes),
        "n_duplicate_columns": len(duplicate_columns),
        "n_rows": int(len(panel)) if panel is not None else None,
        "first_reference_date": (
            panel.index.min().date().isoformat() if panel is not None and len(panel) > 0 else None
        ),
        "last_reference_date": (
            panel.index.max().date().isoformat() if panel is not None and len(panel) > 0 else None
        ),
        "date_parse_failures": panel_diag["date_parse_failures"],
        "duplicate_reference_dates": panel_diag["duplicate_reference_dates"],
        "dates_monotonic_in_input": panel_diag["dates_monotonic_in_input"],
        "non_month_start_dates": panel_diag["non_month_start_dates"],
        "monthly_grid_gaps": panel_diag["monthly_grid_gaps"],
        "bad_numeric_cells": panel_diag["bad_numeric_cells"],
        "n_missing_values": total_missing,
        "pct_missing_values": pct_missing,
        "n_series_with_leading_missing": n_series_leading,
        "n_series_with_trailing_missing": n_series_trailing,
        "n_series_with_internal_missing": n_series_internal,
        "n_all_missing_series": n_all_missing,
        "schema_valid": len(unique_problems) == 0,
        "problems": ";".join(unique_problems),
    }

    return vintage_row, series_rows, anomaly_rows


def _build_presence(series_df: pd.DataFrame) -> pd.DataFrame:
    if series_df.empty:
        return pd.DataFrame()
    usable = series_df.dropna(subset=["vintage", "series"]).copy()
    if usable.empty:
        return pd.DataFrame()
    presence = (
        usable.assign(present=1)
        .pivot_table(index="series", columns="vintage", values="present", aggfunc="max", fill_value=0)
        .astype(int)
        .sort_index()
    )
    return presence.reindex(sorted(presence.columns), axis=1)


def _build_series_changes(presence_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["previous_vintage", "vintage", "change", "series"]
    if presence_df.empty or len(presence_df.columns) < 2:
        return pd.DataFrame(columns=columns)

    vintages = list(presence_df.columns)
    rows: list[dict[str, Any]] = []
    for previous, current in zip(vintages[:-1], vintages[1:]):
        prev = presence_df[previous].astype(int)
        cur = presence_df[current].astype(int)
        added = presence_df.index[(prev == 0) & (cur == 1)]
        removed = presence_df.index[(prev == 1) & (cur == 0)]
        for series_name in added:
            rows.append({"previous_vintage": previous, "vintage": current, "change": "added", "series": series_name})
        for series_name in removed:
            rows.append({"previous_vintage": previous, "vintage": current, "change": "removed", "series": series_name})

    return pd.DataFrame(rows, columns=columns).sort_values(["vintage", "change", "series"])


def _build_tcode_history(series_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "series",
        "first_vintage",
        "last_vintage",
        "n_vintages_present",
        "tcodes_seen",
        "n_distinct_tcodes",
        "n_tcode_changes",
    ]
    if series_df.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    for series_name, group in series_df.groupby("series", sort=True):
        g = group.dropna(subset=["vintage"]).sort_values("vintage")
        if g.empty:
            continue
        tcodes = [int(x) for x in g["tcode"].dropna().tolist()]
        distinct = list(dict.fromkeys(tcodes))
        n_changes = int(sum(a != b for a, b in zip(tcodes[:-1], tcodes[1:])))
        rows.append(
            {
                "series": series_name,
                "first_vintage": g["vintage"].iloc[0],
                "last_vintage": g["vintage"].iloc[-1],
                "n_vintages_present": int(g["vintage"].nunique()),
                "tcodes_seen": "|".join(str(x) for x in distinct),
                "n_distinct_tcodes": len(set(tcodes)),
                "n_tcode_changes": n_changes,
            }
        )
    return pd.DataFrame(rows, columns=columns).sort_values("series")



def _build_collection_summary(
    vintage_df: pd.DataFrame,
    *,
    expected_start: str | None = None,
    expected_end: str | None = None,
) -> pd.DataFrame:
    """Summarize continuity and integrity of the vintage-file collection."""
    vintage_strings = (
        vintage_df["vintage"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .sort_values()
    )

    n_unrecognized = int(vintage_df["vintage"].isna().sum())
    duplicate_rows = vintage_df.loc[
        vintage_df["vintage"].notna()
        & vintage_df["vintage"].duplicated(keep=False),
        "vintage",
    ]
    duplicate_months = sorted(set(duplicate_rows.astype(str)))
    schema_invalid = int((~vintage_df["schema_valid"]).sum())

    if vintage_strings.empty:
        return pd.DataFrame([{
            "observed_first_vintage": None,
            "observed_last_vintage": None,
            "expected_start_vintage": expected_start,
            "expected_end_vintage": expected_end,
            "n_unique_vintages": 0,
            "expected_n_vintages": 0,
            "n_missing_vintage_months": 0,
            "missing_vintage_months": "",
            "n_unexpected_vintage_months": 0,
            "unexpected_vintage_months": "",
            "n_duplicate_vintage_files": int(len(duplicate_rows)),
            "n_duplicate_vintage_months": int(len(duplicate_months)),
            "n_unrecognized_vintage_files": n_unrecognized,
            "schema_valid_files": int(vintage_df["schema_valid"].sum()),
            "schema_invalid_files": schema_invalid,
            "collection_valid": False,
        }])

    observed = pd.PeriodIndex(vintage_strings, freq="M").unique().sort_values()
    start = pd.Period(expected_start, freq="M") if expected_start else observed.min()
    end = pd.Period(expected_end, freq="M") if expected_end else observed.max()
    if end < start:
        raise ValueError(f"expected_end ({end}) is earlier than expected_start ({start}).")

    expected = pd.period_range(start, end, freq="M")
    missing = expected.difference(observed)
    unexpected = observed.difference(expected)

    collection_valid = (
        len(missing) == 0
        and len(unexpected) == 0
        and len(duplicate_months) == 0
        and n_unrecognized == 0
        and schema_invalid == 0
    )

    return pd.DataFrame([{
        "observed_first_vintage": str(observed.min()),
        "observed_last_vintage": str(observed.max()),
        "expected_start_vintage": str(start),
        "expected_end_vintage": str(end),
        "n_unique_vintages": int(len(observed)),
        "expected_n_vintages": int(len(expected)),
        "n_missing_vintage_months": int(len(missing)),
        "missing_vintage_months": "|".join(str(x) for x in missing),
        "n_unexpected_vintage_months": int(len(unexpected)),
        "unexpected_vintage_months": "|".join(str(x) for x in unexpected),
        "n_duplicate_vintage_files": int(len(duplicate_rows)),
        "n_duplicate_vintage_months": int(len(duplicate_months)),
        "n_unrecognized_vintage_files": n_unrecognized,
        "schema_valid_files": int(vintage_df["schema_valid"].sum()),
        "schema_invalid_files": schema_invalid,
        "collection_valid": bool(collection_valid),
    }])


def _build_anomaly_summary(anomaly_df: pd.DataFrame) -> pd.DataFrame:
    """Collapse recurring identical anomaly patterns across vintages."""
    columns = [
        "series",
        "anomaly_type",
        "start",
        "end",
        "length",
        "first_vintage",
        "last_vintage",
        "n_vintages",
        "n_records",
    ]
    if anomaly_df.empty:
        return pd.DataFrame(columns=columns)

    out = (
        anomaly_df.groupby(
            ["series", "anomaly_type", "start", "end", "length"],
            dropna=False,
        )
        .agg(
            first_vintage=("vintage", "min"),
            last_vintage=("vintage", "max"),
            n_vintages=("vintage", "nunique"),
            n_records=("vintage", "size"),
        )
        .reset_index()
    )
    return out[columns].sort_values(
        ["series", "anomaly_type", "start"],
        na_position="last",
    )


def _build_vintage_manifest(vintage_df: pd.DataFrame) -> pd.DataFrame:
    """Small machine-independent provenance manifest suitable for version control."""
    columns = [
        "vintage",
        "filename",
        "sha256",
        "n_series",
        "n_rows",
        "first_reference_date",
        "last_reference_date",
        "schema_valid",
    ]
    return (
        vintage_df[columns]
        .copy()
        .sort_values(["vintage", "filename"], na_position="last")
        .reset_index(drop=True)
    )

def audit_vintage_collection(
    raw_dir: str | Path,
    *,
    recursive: bool = False,
    expected_start: str | None = None,
    expected_end: str | None = None,
) -> dict[str, pd.DataFrame]:
    """Audit every CSV in a raw FRED-MD vintage directory; source files are untouched."""
    raw_path = Path(raw_dir)
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw vintage directory not found: {raw_path}")
    if not raw_path.is_dir():
        raise NotADirectoryError(f"Expected a directory: {raw_path}")

    files = sorted(raw_path.rglob("*.csv") if recursive else raw_path.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {raw_path}")

    vintage_rows: list[dict[str, Any]] = []
    series_rows: list[dict[str, Any]] = []
    anomaly_rows: list[dict[str, Any]] = []

    for path in files:
        vintage_row, one_series, one_anomalies = audit_single_vintage(path)
        vintage_rows.append(vintage_row)
        series_rows.extend(one_series)
        anomaly_rows.extend(one_anomalies)

    vintage_df = pd.DataFrame(vintage_rows)
    series_df = pd.DataFrame(series_rows)
    anomaly_df = pd.DataFrame(
        anomaly_rows,
        columns=["vintage", "filename", "series", "anomaly_type", "start", "end", "length", "detail"],
    )

    duplicate_mask = vintage_df["vintage"].notna() & vintage_df["vintage"].duplicated(keep=False)
    vintage_df["duplicate_vintage"] = duplicate_mask

    if duplicate_mask.any():
        for idx in vintage_df.index[duplicate_mask]:
            current = str(vintage_df.at[idx, "problems"] or "")
            vintage_df.at[idx, "problems"] = f"{current};duplicate_vintage" if current else "duplicate_vintage"
            vintage_df.at[idx, "schema_valid"] = False
            anomaly_df.loc[len(anomaly_df)] = {
                "vintage": vintage_df.at[idx, "vintage"],
                "filename": vintage_df.at[idx, "filename"],
                "series": None,
                "anomaly_type": "duplicate_vintage",
                "start": None,
                "end": None,
                "length": None,
                "detail": "More than one CSV resolves to this vintage month.",
            }

    vintage_df = vintage_df.sort_values(["vintage", "filename"], na_position="last").reset_index(drop=True)
    if not series_df.empty:
        series_df = series_df.sort_values(["vintage", "series"], na_position="last").reset_index(drop=True)
    if not anomaly_df.empty:
        anomaly_df = anomaly_df.sort_values(
            ["vintage", "filename", "series", "anomaly_type", "start"], na_position="last"
        ).reset_index(drop=True)

    presence_df = _build_presence(series_df)
    changes_df = _build_series_changes(presence_df)
    tcode_history_df = _build_tcode_history(series_df)
    collection_summary_df = _build_collection_summary(
        vintage_df,
        expected_start=expected_start,
        expected_end=expected_end,
    )
    anomaly_summary_df = _build_anomaly_summary(anomaly_df)
    vintage_manifest_df = _build_vintage_manifest(vintage_df)

    return {
        "vintage_audit": vintage_df,
        "series_audit": series_df,
        "series_presence": presence_df,
        "series_changes": changes_df,
        "series_tcode_history": tcode_history_df,
        "anomalies": anomaly_df,
        "collection_summary": collection_summary_df,
        "anomaly_summary": anomaly_summary_df,
        "vintage_manifest": vintage_manifest_df,
    }


def write_audit_outputs(
    raw_dir: str | Path,
    audit_dir: str | Path,
    *,
    recursive: bool = False,
    expected_start: str | None = None,
    expected_end: str | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, Path]]:
    """Run the audit and write deterministic CSV artifacts."""
    outputs = audit_vintage_collection(
        raw_dir,
        recursive=recursive,
        expected_start=expected_start,
        expected_end=expected_end,
    )
    out_dir = Path(audit_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "vintage_audit": out_dir / "vintage_audit.csv",
        "series_audit": out_dir / "series_audit.csv",
        "series_presence": out_dir / "series_presence.csv",
        "series_changes": out_dir / "series_changes.csv",
        "series_tcode_history": out_dir / "series_tcode_history.csv",
        "anomalies": out_dir / "anomalies.csv",
        "collection_summary": out_dir / "collection_summary.csv",
        "anomaly_summary": out_dir / "anomaly_summary.csv",
        "vintage_manifest": out_dir / "vintage_manifest.csv",
    }

    outputs["vintage_audit"].to_csv(paths["vintage_audit"], index=False, float_format="%.10g")
    outputs["series_audit"].to_csv(paths["series_audit"], index=False, float_format="%.10g")
    outputs["series_presence"].to_csv(paths["series_presence"], index=True)
    outputs["series_changes"].to_csv(paths["series_changes"], index=False)
    outputs["series_tcode_history"].to_csv(paths["series_tcode_history"], index=False)
    outputs["anomalies"].to_csv(paths["anomalies"], index=False)
    outputs["collection_summary"].to_csv(paths["collection_summary"], index=False)
    outputs["anomaly_summary"].to_csv(paths["anomaly_summary"], index=False)
    outputs["vintage_manifest"].to_csv(paths["vintage_manifest"], index=False)

    return outputs, paths


__all__ = [
    "MISSING_TOKENS",
    "parse_vintage_filename",
    "read_literal_header",
    "read_literal_row",
    "classify_missingness",
    "audit_single_vintage",
    "audit_vintage_collection",
    "write_audit_outputs",
]
