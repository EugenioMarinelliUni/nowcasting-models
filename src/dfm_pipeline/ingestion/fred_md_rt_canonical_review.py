from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md_rt_canonical_comparability import (
    align_finite_common,
    compute_pairwise_diagnostics,
)
from dfm_pipeline.preprocessing.tcode import (
    ALLOWED_TCODES,
    apply_tcode_transformations,
)


REVIEW_SPEC_COLUMNS = [
    "canonical_id",
    "old_reference_series",
    "new_reference_series",
    "aux_reference_series",
    "canonical_tcode",
    "review_mode",
]

REVIEW_MODES = {
    "pairwise",
    "triangulation",
    "fred_md_splice",
}

PAIRWISE_METRIC_COLUMNS = [
    "n_common_valid",
    "first_common_date",
    "last_common_date",
    "pearson_corr",
    "spearman_corr",
    "old_mean",
    "new_mean",
    "old_std",
    "new_std",
    "std_ratio_new_old",
    "mean_difference",
    "mae_difference",
    "rmse_difference",
    "nrmse_old_std",
    "sign_agreement",
    "ols_alpha",
    "ols_beta",
    "ols_r2",
    "diagnostic_status",
]

REVIEW_SUMMARY_COLUMNS = [
    "canonical_id",
    "review_mode",
    "pair_role",
    "old_reference_series",
    "new_reference_series",
    "canonical_tcode",
    *PAIRWISE_METRIC_COLUMNS,
]

ALIGNED_REVIEW_COLUMNS = [
    "canonical_id",
    "review_mode",
    "pair_role",
    "old_reference_series",
    "new_reference_series",
    "canonical_tcode",
    "reference_date",
    "old_transformed",
    "new_transformed",
    "difference_new_minus_old",
]

ROLLING_REVIEW_COLUMNS = [
    "canonical_id",
    "review_mode",
    "pair_role",
    "old_reference_series",
    "new_reference_series",
    "canonical_tcode",
    "window_months",
    "window_start_date",
    "window_end_date",
    "n_common_valid",
    "pearson_corr",
    "std_ratio_new_old",
    "nrmse_old_std",
    "diagnostic_status",
]

DATE_COLUMN_CANDIDATES = (
    "observation_date",
    "DATE",
    "date",
)


def _clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _coerce_tcode(value: object) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid canonical t-code: {value!r}") from exc

    if not np.isfinite(numeric) or not numeric.is_integer():
        raise ValueError(f"Invalid canonical t-code: {value!r}")

    code = int(numeric)
    if code not in ALLOWED_TCODES:
        raise ValueError(
            f"Invalid canonical t-code: {code}. "
            f"Allowed: {sorted(ALLOWED_TCODES)}"
        )
    return code


def validate_transition_review_spec(
    spec: pd.DataFrame,
) -> pd.DataFrame:
    """
    Validate and normalize the C4 RT_CANONICAL transition-review specification.

    This function performs structural validation only. It does not apply any
    statistical acceptance threshold and does not make canonicalization
    decisions.
    """
    missing_columns = [
        column
        for column in REVIEW_SPEC_COLUMNS
        if column not in spec.columns
    ]
    if missing_columns:
        raise ValueError(
            "Transition review specification missing columns: "
            + ", ".join(missing_columns)
        )

    if spec.empty:
        raise ValueError(
            "Transition review specification must contain at least one row"
        )

    out = spec.loc[:, REVIEW_SPEC_COLUMNS].copy()

    text_columns = [
        "canonical_id",
        "old_reference_series",
        "new_reference_series",
        "aux_reference_series",
        "review_mode",
    ]
    for column in text_columns:
        out[column] = out[column].map(_clean_text)

    required_text = [
        "canonical_id",
        "old_reference_series",
        "new_reference_series",
        "review_mode",
    ]
    for column in required_text:
        if out[column].eq("").any():
            raise ValueError(
                f"Transition review specification contains blank {column}"
            )

    duplicated = out["canonical_id"].duplicated(keep=False)
    if duplicated.any():
        values = sorted(
            out.loc[duplicated, "canonical_id"].unique().tolist()
        )
        raise ValueError(
            "Duplicate canonical_id values in transition review "
            f"specification: {values}"
        )

    out["canonical_tcode"] = out["canonical_tcode"].map(_coerce_tcode)

    invalid_modes = sorted(
        set(out["review_mode"]) - REVIEW_MODES
    )
    if invalid_modes:
        raise ValueError(
            "Unsupported transition review modes: "
            f"{invalid_modes}. Allowed: {sorted(REVIEW_MODES)}"
        )

    same_pair = (
        out["old_reference_series"]
        == out["new_reference_series"]
    )
    if same_pair.any():
        bad = out.loc[same_pair, "canonical_id"].tolist()
        raise ValueError(
            "Old and new reference series must be distinct for: "
            f"{bad}"
        )

    triangulation = out["review_mode"].eq("triangulation")
    missing_aux = triangulation & out["aux_reference_series"].eq("")
    if missing_aux.any():
        bad = out.loc[missing_aux, "canonical_id"].tolist()
        raise ValueError(
            "Triangulation review requires aux_reference_series for: "
            f"{bad}"
        )

    for _, row in out.loc[triangulation].iterrows():
        members = {
            row["old_reference_series"],
            row["aux_reference_series"],
            row["new_reference_series"],
        }
        if len(members) != 3:
            raise ValueError(
                "Triangulation reference series must be distinct for "
                f"{row['canonical_id']}"
            )

    return out.reset_index(drop=True)


def load_transition_review_spec(
    path: str | Path,
) -> pd.DataFrame:
    path = Path(path)
    spec = pd.read_csv(path)
    return validate_transition_review_spec(spec)


def load_reference_series(
    path: str | Path,
    *,
    series_id: str | None = None,
) -> pd.Series:
    """
    Load one external/reference series from a simple CSV export.

    Supported date-column names are ``observation_date``, ``DATE`` and
    ``date``. If ``series_id`` is provided, that exact value column must be
    present. Otherwise the file must contain exactly one non-date column.

    FRED-style "." observations are treated as missing. Other non-numeric,
    non-missing tokens are rejected.
    """
    path = Path(path)
    frame = pd.read_csv(path)

    date_columns = [
        column
        for column in DATE_COLUMN_CANDIDATES
        if column in frame.columns
    ]
    if not date_columns:
        raise ValueError(
            f"No supported date column found in {path}. "
            f"Expected one of {DATE_COLUMN_CANDIDATES}"
        )

    date_col = date_columns[0]

    value_columns = [
        column
        for column in frame.columns
        if column != date_col
    ]

    if series_id is not None:
        if series_id not in frame.columns:
            raise ValueError(
                f"Reference CSV {path} does not contain "
                f"series column {series_id!r}"
            )
        value_col = series_id
    else:
        if len(value_columns) != 1:
            raise ValueError(
                f"Reference CSV {path} must contain exactly one "
                "non-date column when series_id is not supplied"
            )
        value_col = value_columns[0]

    dates = pd.to_datetime(
        frame[date_col],
        errors="coerce",
    )
    if dates.isna().any():
        raise ValueError(
            f"Reference CSV {path} contains unparseable dates"
        )

    if dates.duplicated().any():
        raise ValueError(
            f"Reference CSV {path} contains duplicate reference dates"
        )

    raw_values = frame[value_col]
    values = pd.to_numeric(
        raw_values,
        errors="coerce",
    )

    text_values = raw_values.astype("string").str.strip()
    accepted_missing_tokens = {
        "",
        ".",
        "NA",
        "N/A",
        "NaN",
        "nan",
    }
    bad_numeric = (
        values.isna()
        & raw_values.notna()
        & ~text_values.isin(accepted_missing_tokens)
    )
    if bad_numeric.any():
        raise ValueError(
            f"Reference CSV {path} contains non-numeric observations "
            f"in series {value_col!r}"
        )

    series = pd.Series(
        values.to_numpy(dtype=float),
        index=pd.DatetimeIndex(dates),
        name=value_col,
        dtype=float,
    ).sort_index()

    if int(np.isfinite(series.to_numpy(dtype=float)).sum()) == 0:
        raise ValueError(
            f"Reference CSV {path} contains no finite observations "
            f"for series {value_col!r}"
        )

    return series


def transform_reference_series(
    series: pd.Series,
    *,
    tcode: int,
) -> pd.Series:
    """
    Apply the canonical FRED-MD transformation to one reference series.
    """
    code = _coerce_tcode(tcode)
    name = str(series.name) if series.name is not None else "reference_series"

    frame = pd.DataFrame(
        {
            name: pd.to_numeric(
                series,
                errors="coerce",
            )
        },
        index=series.index,
    )

    transformed = apply_tcode_transformations(
        frame,
        {name: code},
    )
    return transformed[name]


def build_review_pairs(
    spec_row: Mapping[str, Any] | pd.Series,
) -> list[tuple[str, str, str]]:
    """
    Expand one C4 specification row into the pairwise comparisons required.

    Returns tuples of:
        (pair_role, old/reference-left series, new/reference-right series)
    """
    mode = _clean_text(spec_row["review_mode"])
    old = _clean_text(spec_row["old_reference_series"])
    new = _clean_text(spec_row["new_reference_series"])
    aux = _clean_text(spec_row.get("aux_reference_series", ""))

    if mode not in REVIEW_MODES:
        raise ValueError(f"Unsupported transition review mode: {mode!r}")

    if mode == "triangulation":
        if not aux:
            raise ValueError(
                "Triangulation review requires aux_reference_series"
            )
        return [
            ("old_vs_aux", old, aux),
            ("aux_vs_new", aux, new),
            ("old_vs_new", old, new),
        ]

    return [
        ("old_vs_new", old, new),
    ]


def compute_rolling_pairwise_diagnostics(
    aligned: pd.DataFrame,
    *,
    old_col: str = "old_transformed",
    new_col: str = "new_transformed",
    window_months: int = 60,
) -> pd.DataFrame:
    """
    Compute threshold-free diagnostics in trailing calendar-month windows.

    ``window_months`` describes calendar time rather than a fixed number of
    observations. Missing months therefore do not get silently replaced by
    older observations.
    """
    if window_months < 2:
        raise ValueError("window_months must be at least 2")

    missing = [
        column
        for column in (old_col, new_col)
        if column not in aligned.columns
    ]
    if missing:
        raise ValueError(
            "Aligned rolling comparison data missing columns: "
            + ", ".join(missing)
        )

    if aligned.empty:
        return pd.DataFrame(
            columns=[
                "window_months",
                "window_start_date",
                "window_end_date",
                "n_common_valid",
                "pearson_corr",
                "std_ratio_new_old",
                "nrmse_old_std",
                "diagnostic_status",
            ]
        )

    work = aligned.copy()
    parsed_index = pd.to_datetime(
        work.index,
        errors="coerce",
    )
    if parsed_index.isna().any():
        raise ValueError(
            "Rolling comparison index contains unparseable dates"
        )

    work.index = pd.DatetimeIndex(parsed_index)

    if work.index.duplicated().any():
        raise ValueError(
            "Rolling comparison index contains duplicate dates"
        )

    work = work.sort_index()

    first_date = pd.Timestamp(work.index.min())
    first_eligible_end = first_date + pd.DateOffset(
        months=window_months - 1
    )

    rows: list[dict[str, Any]] = []

    for end_date in work.index:
        end_date = pd.Timestamp(end_date)
        if end_date < first_eligible_end:
            continue

        start_date = end_date - pd.DateOffset(
            months=window_months - 1
        )

        subset = work.loc[
            (work.index >= start_date)
            & (work.index <= end_date)
        ]

        metrics = compute_pairwise_diagnostics(
            subset,
            old_col=old_col,
            new_col=new_col,
        )

        rows.append(
            {
                "window_months": int(window_months),
                "window_start_date": start_date.date().isoformat(),
                "window_end_date": end_date.date().isoformat(),
                "n_common_valid": metrics["n_common_valid"],
                "pearson_corr": metrics["pearson_corr"],
                "std_ratio_new_old": metrics["std_ratio_new_old"],
                "nrmse_old_std": metrics["nrmse_old_std"],
                "diagnostic_status": metrics["diagnostic_status"],
            }
        )

    return pd.DataFrame(rows)


def review_reference_mapping(
    spec_row: Mapping[str, Any] | pd.Series,
    references: Mapping[str, pd.Series],
    *,
    rolling_window_months: int = 60,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Review one RT_CANONICAL transition using external/reference series.

    The returned objects are:
        1. full-history transformed pairwise diagnostics;
        2. aligned transformed observations;
        3. trailing calendar-window diagnostics.

    The function deliberately does not make an accept/reject decision.
    """
    normalized = validate_transition_review_spec(
        pd.DataFrame([dict(spec_row)])
    ).iloc[0]

    canonical_id = str(normalized["canonical_id"])
    review_mode = str(normalized["review_mode"])
    tcode = int(normalized["canonical_tcode"])

    pairs = build_review_pairs(normalized)

    required_series = {
        series_id
        for _, old_id, new_id in pairs
        for series_id in (old_id, new_id)
    }
    missing_references = sorted(
        series_id
        for series_id in required_series
        if series_id not in references
    )
    if missing_references:
        raise KeyError(
            "Missing reference series for "
            f"{canonical_id}: {missing_references}"
        )

    transformed_cache: dict[str, pd.Series] = {}

    def transformed(series_id: str) -> pd.Series:
        if series_id not in transformed_cache:
            transformed_cache[series_id] = transform_reference_series(
                references[series_id],
                tcode=tcode,
            )
        return transformed_cache[series_id]

    summary_rows: list[dict[str, Any]] = []
    aligned_rows: list[dict[str, Any]] = []
    rolling_rows: list[dict[str, Any]] = []

    for pair_role, old_id, new_id in pairs:
        old_transformed = transformed(old_id)
        new_transformed = transformed(new_id)

        aligned = align_finite_common(
            old_transformed,
            new_transformed,
        )

        metrics = compute_pairwise_diagnostics(aligned)

        summary_rows.append(
            {
                "canonical_id": canonical_id,
                "review_mode": review_mode,
                "pair_role": pair_role,
                "old_reference_series": old_id,
                "new_reference_series": new_id,
                "canonical_tcode": tcode,
                **metrics,
            }
        )

        for reference_date, obs in aligned.iterrows():
            aligned_rows.append(
                {
                    "canonical_id": canonical_id,
                    "review_mode": review_mode,
                    "pair_role": pair_role,
                    "old_reference_series": old_id,
                    "new_reference_series": new_id,
                    "canonical_tcode": tcode,
                    "reference_date": (
                        pd.Timestamp(reference_date).date().isoformat()
                    ),
                    "old_transformed": float(obs["old_transformed"]),
                    "new_transformed": float(obs["new_transformed"]),
                    "difference_new_minus_old": float(
                        obs["new_transformed"]
                        - obs["old_transformed"]
                    ),
                }
            )

        rolling = compute_rolling_pairwise_diagnostics(
            aligned,
            window_months=rolling_window_months,
        )

        for _, rolling_row in rolling.iterrows():
            rolling_rows.append(
                {
                    "canonical_id": canonical_id,
                    "review_mode": review_mode,
                    "pair_role": pair_role,
                    "old_reference_series": old_id,
                    "new_reference_series": new_id,
                    "canonical_tcode": tcode,
                    "window_months": int(
                        rolling_row["window_months"]
                    ),
                    "window_start_date": rolling_row[
                        "window_start_date"
                    ],
                    "window_end_date": rolling_row[
                        "window_end_date"
                    ],
                    "n_common_valid": int(
                        rolling_row["n_common_valid"]
                    ),
                    "pearson_corr": rolling_row["pearson_corr"],
                    "std_ratio_new_old": rolling_row[
                        "std_ratio_new_old"
                    ],
                    "nrmse_old_std": rolling_row[
                        "nrmse_old_std"
                    ],
                    "diagnostic_status": rolling_row[
                        "diagnostic_status"
                    ],
                }
            )

    summary = pd.DataFrame(
        summary_rows,
        columns=REVIEW_SUMMARY_COLUMNS,
    )

    aligned_out = pd.DataFrame(
        aligned_rows,
        columns=ALIGNED_REVIEW_COLUMNS,
    )

    rolling_out = pd.DataFrame(
        rolling_rows,
        columns=ROLLING_REVIEW_COLUMNS,
    )

    return summary, aligned_out, rolling_out


def review_transition_references(
    spec: pd.DataFrame,
    references: Mapping[str, pd.Series],
    *,
    rolling_window_months: int = 60,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run C4 reference diagnostics for every row in a validated review spec.
    """
    validated = validate_transition_review_spec(spec)

    summaries: list[pd.DataFrame] = []
    aligned_parts: list[pd.DataFrame] = []
    rolling_parts: list[pd.DataFrame] = []

    for _, row in validated.iterrows():
        summary, aligned, rolling = review_reference_mapping(
            row,
            references,
            rolling_window_months=rolling_window_months,
        )
        summaries.append(summary)
        aligned_parts.append(aligned)
        rolling_parts.append(rolling)

    summary_out = pd.concat(
        summaries,
        ignore_index=True,
    )
    aligned_out = pd.concat(
        aligned_parts,
        ignore_index=True,
    )
    rolling_out = pd.concat(
        rolling_parts,
        ignore_index=True,
    )

    return summary_out, aligned_out, rolling_out


__all__ = [
    "REVIEW_SPEC_COLUMNS",
    "REVIEW_MODES",
    "PAIRWISE_METRIC_COLUMNS",
    "REVIEW_SUMMARY_COLUMNS",
    "ALIGNED_REVIEW_COLUMNS",
    "ROLLING_REVIEW_COLUMNS",
    "validate_transition_review_spec",
    "load_transition_review_spec",
    "load_reference_series",
    "transform_reference_series",
    "build_review_pairs",
    "compute_rolling_pairwise_diagnostics",
    "review_reference_mapping",
    "review_transition_references",
]