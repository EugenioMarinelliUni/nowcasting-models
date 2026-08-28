from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md_panel_selection import (
    SelectionScope,
    registry_bool_series,
    select_panel_candidates,
)

Stage1Scope = Literal["ordinary", "all-stable", "rt-stable-candidates"]

REQUIRED_COLUMNS = [
    "raw_series",
    "candidate_rt_stable",
    "review_status",
    "include_rt_stable",
    "include_rt_canonical",
    "stable_window_start",
    "stable_window_end",
    "stable_window_expected_vintages",
    "stable_window_vintages_present",
    "present_all_stable_window",
    "tcodes_seen",
    "n_distinct_tcodes",
    "n_tcode_changes",
    "coverage_expected_vintages",
    "coverage_vintages_present",
    "coverage_min_n_valid",
    "coverage_median_n_valid",
    "coverage_min_observed_fraction",
    "coverage_max_pct_missing",
    "coverage_max_leading_missing",
    "coverage_max_trailing_missing",
    "coverage_max_internal_missing",
    "coverage_vintages_with_internal_missing",
    "coverage_all_missing_vintages",
    "coverage_earliest_first_valid_date",
    "coverage_latest_first_valid_date",
    "coverage_earliest_last_valid_date",
    "coverage_latest_last_valid_date",
]

NUMERIC_COLUMNS = [
    "stable_window_expected_vintages",
    "stable_window_vintages_present",
    "n_distinct_tcodes",
    "n_tcode_changes",
    "coverage_expected_vintages",
    "coverage_vintages_present",
    "coverage_min_n_valid",
    "coverage_median_n_valid",
    "coverage_min_observed_fraction",
    "coverage_max_pct_missing",
    "coverage_max_leading_missing",
    "coverage_max_trailing_missing",
    "coverage_max_internal_missing",
    "coverage_vintages_with_internal_missing",
    "coverage_all_missing_vintages",
]


def require_registry_columns(df: pd.DataFrame) -> None:
    """Raise if the curated registry lacks Stage-1 QC fields."""
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            "Registry is missing required eligibility-QC columns: "
            + ", ".join(missing)
        )


def _join_flags(flags: list[str]) -> str:
    return "|".join(dict.fromkeys(flags))


def _rank_ascending(values: pd.Series) -> pd.Series:
    """Rank low values as more extreme; rank 1 is the smallest value."""
    return (
        values.rank(
            method="min",
            ascending=True,
            na_option="bottom",
        )
        .round()
        .astype("Int64")
    )


def _rank_descending(values: pd.Series) -> pd.Series:
    """Rank high values as more extreme; rank 1 is the largest value."""
    return (
        values.rank(
            method="min",
            ascending=False,
            na_option="bottom",
        )
        .round()
        .astype("Int64")
    )


def build_eligibility_report(
    registry: pd.DataFrame,
    *,
    scope: Stage1Scope,
) -> pd.DataFrame:
    """Build the Stage-1 raw structural eligibility report."""
    require_registry_columns(registry)
    x = select_panel_candidates(registry, scope=scope).copy()

    for col in NUMERIC_COLUMNS:
        x[col] = pd.to_numeric(x[col], errors="coerce")

    x["has_leading_missing"] = (
        x["coverage_max_leading_missing"].fillna(0) > 0
    )
    x["has_trailing_missing"] = (
        x["coverage_max_trailing_missing"].fillna(0) > 0
    )
    x["has_internal_missing"] = (
        x["coverage_max_internal_missing"].fillna(0) > 0
    )
    x["has_persistent_internal_missing"] = (
        x["coverage_vintages_with_internal_missing"].fillna(0) > 1
    )
    x["has_all_missing_vintage"] = (
        x["coverage_all_missing_vintages"].fillna(0) > 0
    )
    x["has_incomplete_history"] = (
        x["coverage_min_observed_fraction"].fillna(0) < 1.0
    )

    hard_failure_column: list[str] = []
    qc_flag_column: list[str] = []

    required_numeric = [
        "stable_window_expected_vintages",
        "stable_window_vintages_present",
        "n_distinct_tcodes",
        "n_tcode_changes",
        "coverage_expected_vintages",
        "coverage_vintages_present",
        "coverage_min_n_valid",
        "coverage_min_observed_fraction",
        "coverage_all_missing_vintages",
    ]

    for _, row in x.iterrows():
        hard_failure_reasons: list[str] = []
        qc_flags: list[str] = []

        if any(pd.isna(row[col]) for col in required_numeric):
            hard_failure_reasons.append("missing_required_qc_metric")

        if not bool(row["present_all_stable_window"]):
            hard_failure_reasons.append("not_present_all_stable_window")

        expected = row["stable_window_expected_vintages"]
        observed = row["stable_window_vintages_present"]
        if (
            pd.notna(expected)
            and pd.notna(observed)
            and int(expected) != int(observed)
        ):
            hard_failure_reasons.append(
                "stable_window_presence_count_mismatch"
            )

        coverage_expected = row["coverage_expected_vintages"]
        coverage_present = row["coverage_vintages_present"]
        if (
            pd.notna(coverage_expected)
            and pd.notna(coverage_present)
            and int(coverage_expected) != int(coverage_present)
        ):
            hard_failure_reasons.append(
                "coverage_vintage_count_mismatch"
            )

        if row["tcode"] is None or pd.isna(row["tcode"]):
            hard_failure_reasons.append(
                "missing_or_invalid_unique_tcode"
            )

        if (
            pd.notna(row["n_distinct_tcodes"])
            and int(row["n_distinct_tcodes"]) != 1
        ):
            hard_failure_reasons.append(
                "multiple_or_missing_tcodes"
            )

        if (
            pd.notna(row["n_tcode_changes"])
            and int(row["n_tcode_changes"]) > 0
        ):
            hard_failure_reasons.append("tcode_changes")

        if bool(row["has_all_missing_vintage"]):
            hard_failure_reasons.append("all_missing_vintage")

        if (
            pd.notna(row["coverage_min_n_valid"])
            and float(row["coverage_min_n_valid"]) <= 0
        ):
            hard_failure_reasons.append(
                "no_valid_observations_in_worst_vintage"
            )

        if bool(row["has_leading_missing"]):
            qc_flags.append("leading_missing")
        if bool(row["has_trailing_missing"]):
            qc_flags.append("trailing_missing")
        if bool(row["has_internal_missing"]):
            qc_flags.append("internal_missing")
        if bool(row["has_persistent_internal_missing"]):
            qc_flags.append("persistent_internal_missing")
        if bool(row["has_incomplete_history"]):
            qc_flags.append("incomplete_history")

        hard_failure_column.append(_join_flags(hard_failure_reasons))
        qc_flag_column.append(_join_flags(qc_flags))

    x["hard_failure_reasons"] = hard_failure_column
    x["qc_flags"] = qc_flag_column
    x["qc_hard_fail"] = (
        x["hard_failure_reasons"].astype(str).str.len() > 0
    )
    x["qc_has_missingness_flags"] = (
        x["qc_flags"].astype(str).str.len() > 0
    )
    x["qc_status"] = np.select(
        [x["qc_hard_fail"], x["qc_has_missingness_flags"]],
        ["hard_fail", "structurally_clean_with_missingness_flags"],
        default="structurally_clean",
    )

    x["rank_low_observed_fraction"] = _rank_ascending(
        x["coverage_min_observed_fraction"]
    )
    x["rank_low_min_n_valid"] = _rank_ascending(
        x["coverage_min_n_valid"]
    )
    x["rank_large_leading_missing"] = _rank_descending(
        x["coverage_max_leading_missing"]
    )
    x["rank_large_internal_missing"] = _rank_descending(
        x["coverage_max_internal_missing"]
    )
    x["rank_persistent_internal_missing"] = _rank_descending(
        x["coverage_vintages_with_internal_missing"]
    )

    # Unreviewed include_rt_* values are seed defaults, not human decisions.
    x["manual_include_rt_stable"] = np.where(
        x["review_status"].eq("reviewed"),
        x["include_rt_stable"].astype(object),
        "",
    )
    x["manual_include_rt_canonical"] = np.where(
        x["review_status"].eq("reviewed"),
        x["include_rt_canonical"].astype(object),
        "",
    )

    output_columns = [
        "raw_series",
        "review_status",
        "candidate_rt_stable",
        "selection_class",
        "manual_include_rt_stable",
        "manual_include_rt_canonical",
        "stable_window_start",
        "stable_window_end",
        "stable_window_expected_vintages",
        "stable_window_vintages_present",
        "tcodes_seen",
        "tcode",
        "n_distinct_tcodes",
        "n_tcode_changes",
        "coverage_expected_vintages",
        "coverage_vintages_present",
        "coverage_min_n_valid",
        "coverage_median_n_valid",
        "coverage_min_observed_fraction",
        "coverage_max_pct_missing",
        "coverage_max_leading_missing",
        "coverage_max_trailing_missing",
        "coverage_max_internal_missing",
        "coverage_vintages_with_internal_missing",
        "coverage_all_missing_vintages",
        "coverage_earliest_first_valid_date",
        "coverage_latest_first_valid_date",
        "coverage_earliest_last_valid_date",
        "coverage_latest_last_valid_date",
        "has_leading_missing",
        "has_trailing_missing",
        "has_internal_missing",
        "has_persistent_internal_missing",
        "has_all_missing_vintage",
        "has_incomplete_history",
        "rank_low_observed_fraction",
        "rank_low_min_n_valid",
        "rank_large_leading_missing",
        "rank_large_internal_missing",
        "rank_persistent_internal_missing",
        "qc_hard_fail",
        "qc_has_missingness_flags",
        "qc_status",
        "hard_failure_reasons",
        "qc_flags",
    ]

    return (
        x[output_columns]
        .sort_values("raw_series")
        .reset_index(drop=True)
    )


def build_summary(
    registry: pd.DataFrame,
    report: pd.DataFrame,
    *,
    scope: Stage1Scope,
) -> pd.DataFrame:
    """Summarize Stage-1 QC without changing inclusion decisions."""
    require_registry_columns(registry)

    candidate_mask = registry_bool_series(
        registry,
        "candidate_rt_stable",
    )
    candidate_rows = registry.loc[candidate_mask].copy()
    candidate_review_status = (
        candidate_rows["review_status"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    observed = pd.to_numeric(
        report["coverage_min_observed_fraction"], errors="coerce"
    )
    min_valid = pd.to_numeric(
        report["coverage_min_n_valid"], errors="coerce"
    )

    return pd.DataFrame(
        [
            {
                "scope": scope,
                "n_registry_series": int(len(registry)),
                "n_candidate_rt_stable": int(candidate_mask.sum()),
                "n_candidate_reviewed": int(
                    candidate_review_status.eq("reviewed").sum()
                ),
                "n_candidate_unreviewed": int(
                    candidate_review_status.eq("unreviewed").sum()
                ),
                "n_rows_in_qc_report": int(len(report)),
                "n_structural_hard_fail": int(
                    report["qc_hard_fail"].sum()
                ),
                "n_structurally_clean": int(
                    (~report["qc_hard_fail"]).sum()
                ),
                "n_with_leading_missing": int(
                    report["has_leading_missing"].sum()
                ),
                "n_with_trailing_missing": int(
                    report["has_trailing_missing"].sum()
                ),
                "n_with_internal_missing": int(
                    report["has_internal_missing"].sum()
                ),
                "n_with_persistent_internal_missing": int(
                    report["has_persistent_internal_missing"].sum()
                ),
                "n_with_all_missing_vintage": int(
                    report["has_all_missing_vintage"].sum()
                ),
                "min_of_min_observed_fraction": (
                    float(observed.min())
                    if observed.notna().any()
                    else np.nan
                ),
                "p10_min_observed_fraction": (
                    float(observed.quantile(0.10))
                    if observed.notna().any()
                    else np.nan
                ),
                "median_min_observed_fraction": (
                    float(observed.median())
                    if observed.notna().any()
                    else np.nan
                ),
                "min_of_min_n_valid": (
                    int(min_valid.min())
                    if min_valid.notna().any()
                    else np.nan
                ),
                "median_min_n_valid": (
                    float(min_valid.median())
                    if min_valid.notna().any()
                    else np.nan
                ),
            }
        ]
    )


__all__ = [
    "Stage1Scope",
    "REQUIRED_COLUMNS",
    "NUMERIC_COLUMNS",
    "require_registry_columns",
    "build_eligibility_report",
    "build_summary",
]
