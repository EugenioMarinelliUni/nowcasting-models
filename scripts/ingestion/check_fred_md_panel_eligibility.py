#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ALLOWED_TCODES = {1, 2, 3, 4, 5, 6, 7}

TRUE_VALUES = {"true", "1", "yes", "y"}
FALSE_VALUES = {"false", "0", "no", "n", ""}

DEFAULT_REGISTRY = Path(
    "data/metadata/series_maps/fred_md_series_registry.csv"
)
DEFAULT_OUT = Path(
    "data/metadata/series_maps/"
    "fred_md_panel_eligibility_2010_2026.csv"
)


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run structural eligibility QC on mechanically stable "
            "FRED-MD predictor candidates. This script is diagnostic: "
            "it does not modify the curated registry or make automatic "
            "panel-inclusion decisions."
        )
    )

    parser.add_argument(
        "--registry",
        type=Path,
        default=DEFAULT_REGISTRY,
        help=f"Curated FRED-MD registry (default: {DEFAULT_REGISTRY}).",
    )

    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Detailed QC output CSV (default: {DEFAULT_OUT}).",
    )

    parser.add_argument(
        "--summary-out",
        type=Path,
        default=None,
        help=(
            "Optional summary CSV path. Default: "
            "<out stem>__summary.csv."
        ),
    )

    parser.add_argument(
        "--scope",
        choices=("ordinary", "all-stable"),
        default="ordinary",
        help=(
            "'ordinary' checks mechanically stable candidates whose "
            "review_status is still unreviewed (expected: 109). "
            "'all-stable' checks all mechanically stable candidates, "
            "including the six manually reviewed special cases."
        ),
    )

    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of extreme cases printed for each diagnostic.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow replacement of existing QC output artifacts.",
    )

    return parser.parse_args()


def _require_columns(df: pd.DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(
            "Registry is missing required eligibility-QC columns: "
            + ", ".join(missing)
        )


def _parse_bool(value: object, *, field: str, raw_series: str) -> bool:
    text = str(value).strip().lower()

    if text in TRUE_VALUES:
        return True

    if text in FALSE_VALUES:
        return False

    raise ValueError(
        f"{raw_series}: cannot parse {field}={value!r} as Boolean."
    )


def _bool_series(
    df: pd.DataFrame,
    field: str,
) -> pd.Series:
    values: list[bool] = []

    for _, row in df.iterrows():
        values.append(
            _parse_bool(
                row[field],
                field=field,
                raw_series=str(row["raw_series"]),
            )
        )

    return pd.Series(values, index=df.index, dtype=bool)


def _single_tcode(
    tcodes_seen: object,
    n_distinct_tcodes: object,
) -> int | None:
    """
    Return the unique valid t-code if exactly one is recorded.

    series_tcode_history stores distinct codes as strings such as:
        "5"
        "2|5"
    """
    try:
        n_distinct = int(float(n_distinct_tcodes))
    except (TypeError, ValueError):
        return None

    if n_distinct != 1:
        return None

    text = str(tcodes_seen).strip()

    if not text:
        return None

    parts = [part.strip() for part in text.split("|") if part.strip()]

    if len(parts) != 1:
        return None

    try:
        code = int(parts[0])
    except ValueError:
        return None

    if code not in ALLOWED_TCODES:
        return None

    return code


def _join_flags(flags: list[str]) -> str:
    return "|".join(dict.fromkeys(flags))


def _rank_ascending(
    values: pd.Series,
) -> pd.Series:
    """
    Rank low values as more extreme.

    Rank 1 therefore means the smallest value.
    """
    return (
        values.rank(
            method="min",
            ascending=True,
            na_option="bottom",
        )
        .round()
        .astype("Int64")
    )


def _rank_descending(
    values: pd.Series,
) -> pd.Series:
    """
    Rank high values as more extreme.

    Rank 1 therefore means the largest value.
    """
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
    scope: str,
) -> pd.DataFrame:
    _require_columns(registry)

    x = registry.copy()

    # Explicit conversion prevents pandas from silently interpreting
    # empty/manual-review fields as numeric columns.
    x["raw_series"] = x["raw_series"].astype(str)
    x["review_status"] = (
        x["review_status"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    candidate = _bool_series(x, "candidate_rt_stable")
    present_all = _bool_series(x, "present_all_stable_window")
    manual_stable = _bool_series(x, "include_rt_stable")
    manual_canonical = _bool_series(x, "include_rt_canonical")

    x["_candidate_rt_stable"] = candidate
    x["_present_all_stable_window"] = present_all
    x["_manual_include_rt_stable"] = manual_stable
    x["_manual_include_rt_canonical"] = manual_canonical

    if scope == "ordinary":
        mask = (
            x["_candidate_rt_stable"]
            & x["review_status"].eq("unreviewed")
        )
    elif scope == "all-stable":
        mask = x["_candidate_rt_stable"]
    else:
        raise ValueError(f"Unknown scope: {scope!r}")

    x = x.loc[mask].copy()

    if x.empty:
        raise ValueError(
            f"No rows selected for scope={scope!r}."
        )

    for col in NUMERIC_COLUMNS:
        x[col] = pd.to_numeric(
            x[col],
            errors="coerce",
        )

    # ----------------------------------------------------------
    # Derive the unique t-code for structurally stable series.
    # ----------------------------------------------------------

    x["tcode"] = [
        _single_tcode(
            row["tcodes_seen"],
            row["n_distinct_tcodes"],
        )
        for _, row in x.iterrows()
    ]

    # ----------------------------------------------------------
    # Mechanical missingness indicators.
    #
    # These describe the series. They are NOT automatic exclusion
    # decisions.
    # ----------------------------------------------------------

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

    # ----------------------------------------------------------
    # Structural hard-failure checks.
    #
    # Notice that ordinary leading/trailing/internal missingness
    # is NOT a hard failure. A state-space DFM may legitimately
    # operate on an unbalanced panel.
    # ----------------------------------------------------------

    hard_failure_reasons: list[str] = []
    qc_flags: list[str] = []

    hard_failure_column: list[str] = []
    qc_flag_column: list[str] = []

    for _, row in x.iterrows():
        hard_failure_reasons = []
        qc_flags = []

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

        if any(pd.isna(row[col]) for col in required_numeric):
            hard_failure_reasons.append("missing_required_qc_metric")

        if not bool(row["_present_all_stable_window"]):
            hard_failure_reasons.append(
                "not_present_all_stable_window"
            )

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

        # Diagnostic flags only.
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

        hard_failure_column.append(
            _join_flags(hard_failure_reasons)
        )
        qc_flag_column.append(
            _join_flags(qc_flags)
        )

    x["hard_failure_reasons"] = hard_failure_column
    x["qc_flags"] = qc_flag_column

    x["qc_hard_fail"] = (
        x["hard_failure_reasons"].astype(str).str.len() > 0
    )

    x["qc_has_missingness_flags"] = (
        x["qc_flags"].astype(str).str.len() > 0
    )

    x["qc_status"] = np.select(
        [
            x["qc_hard_fail"],
            x["qc_has_missingness_flags"],
        ],
        [
            "hard_fail",
            "structurally_clean_with_missingness_flags",
        ],
        default="structurally_clean",
    )

    # ----------------------------------------------------------
    # Rankings identify outliers without inventing arbitrary
    # missingness thresholds.
    # ----------------------------------------------------------

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

    # For unreviewed rows, include_rt_stable=False is merely the
    # seed default and must NOT be interpreted as a manual exclusion.
    x["manual_include_rt_stable"] = np.where(
        x["review_status"].eq("reviewed"),
        x["_manual_include_rt_stable"].astype(object),
        "",
    )

    x["manual_include_rt_canonical"] = np.where(
        x["review_status"].eq("reviewed"),
        x["_manual_include_rt_canonical"].astype(object),
        "",
    )

    output_columns = [
        "raw_series",
        "review_status",
        "candidate_rt_stable",
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
    scope: str,
) -> pd.DataFrame:
    registry_candidate = _bool_series(
        registry,
        "candidate_rt_stable",
    )

    candidate_rows = registry.loc[registry_candidate].copy()

    candidate_review_status = (
        candidate_rows["review_status"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    observed = pd.to_numeric(
        report["coverage_min_observed_fraction"],
        errors="coerce",
    )

    min_valid = pd.to_numeric(
        report["coverage_min_n_valid"],
        errors="coerce",
    )

    return pd.DataFrame(
        [
            {
                "scope": scope,
                "n_registry_series": int(len(registry)),
                "n_candidate_rt_stable": int(
                    registry_candidate.sum()
                ),
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
                    report[
                        "has_persistent_internal_missing"
                    ].sum()
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


def _print_extremes(
    report: pd.DataFrame,
    *,
    top_n: int,
) -> None:
    n = min(top_n, len(report))

    print()
    print("Lowest minimum observed fractions")
    print("---------------------------------")
    print(
        report[
            [
                "raw_series",
                "coverage_min_n_valid",
                "coverage_min_observed_fraction",
                "coverage_max_pct_missing",
                "coverage_max_leading_missing",
                "qc_status",
            ]
        ]
        .sort_values(
            [
                "coverage_min_observed_fraction",
                "raw_series",
            ],
            ascending=[True, True],
        )
        .head(n)
        .to_string(index=False)
    )

    print()
    print("Largest leading-history gaps")
    print("----------------------------")
    print(
        report[
            [
                "raw_series",
                "coverage_max_leading_missing",
                "coverage_min_n_valid",
                "coverage_min_observed_fraction",
                "qc_status",
            ]
        ]
        .sort_values(
            [
                "coverage_max_leading_missing",
                "raw_series",
            ],
            ascending=[False, True],
        )
        .head(n)
        .to_string(index=False)
    )

    print()
    print("Largest internal gaps")
    print("---------------------")
    print(
        report[
            [
                "raw_series",
                "coverage_max_internal_missing",
                "coverage_vintages_with_internal_missing",
                "coverage_min_observed_fraction",
                "qc_status",
            ]
        ]
        .sort_values(
            [
                "coverage_max_internal_missing",
                "coverage_vintages_with_internal_missing",
                "raw_series",
            ],
            ascending=[False, False, True],
        )
        .head(n)
        .to_string(index=False)
    )

    print()
    print("Most persistent internal missingness")
    print("------------------------------------")
    print(
        report[
            [
                "raw_series",
                "coverage_vintages_with_internal_missing",
                "coverage_max_internal_missing",
                "qc_status",
            ]
        ]
        .sort_values(
            [
                "coverage_vintages_with_internal_missing",
                "coverage_max_internal_missing",
                "raw_series",
            ],
            ascending=[False, False, True],
        )
        .head(n)
        .to_string(index=False)
    )

    hard = report.loc[report["qc_hard_fail"]].copy()

    print()
    print("Structural hard failures")
    print("------------------------")

    if hard.empty:
        print("None")
    else:
        print(
            hard[
                [
                    "raw_series",
                    "hard_failure_reasons",
                ]
            ].to_string(index=False)
        )


def main() -> int:
    args = parse_args()

    if args.top_n <= 0:
        raise SystemExit("--top-n must be greater than zero.")

    if not args.registry.exists():
        raise SystemExit(
            f"Registry not found: {args.registry}"
        )

    summary_out = args.summary_out or args.out.with_name(
        f"{args.out.stem}__summary.csv"
    )

    protected = [
        path
        for path in (args.out, summary_out)
        if path.exists()
    ]

    if protected and not args.overwrite:
        raise SystemExit(
            "Refusing to overwrite existing QC artifacts: "
            + ", ".join(str(path) for path in protected)
            + ". Use --overwrite to regenerate them."
        )

    # Read as strings so all-empty review fields cannot be silently
    # inferred as float64. Numeric conversion is explicit below.
    registry = pd.read_csv(
        args.registry,
        dtype=str,
        keep_default_na=False,
    )

    _require_columns(registry)

    report = build_eligibility_report(
        registry,
        scope=args.scope,
    )

    summary = build_summary(
        registry,
        report,
        scope=args.scope,
    )

    args.out.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    report.to_csv(
        args.out,
        index=False,
    )

    summary.to_csv(
        summary_out,
        index=False,
    )

    print("FRED-MD panel eligibility QC completed")
    print(f"registry              : {args.registry}")
    print(f"scope                 : {args.scope}")
    print(f"report                : {args.out}")
    print(f"summary               : {summary_out}")
    print()

    print(summary.to_string(index=False))

    _print_extremes(
        report,
        top_n=args.top_n,
    )

    print()
    print(
        "NOTE: qc_status is diagnostic and is not a final "
        "include/exclude decision. Leading, trailing, and internal "
        "missingness are reported rather than automatically rejected."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())