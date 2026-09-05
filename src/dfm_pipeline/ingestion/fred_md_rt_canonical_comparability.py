from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md import (
    detect_tcode_row,
    read_embedded_tcode_map,
)
from dfm_pipeline.ingestion.fred_md_rt_canonical import (
    build_transition_boundaries,
    validate_rt_canonical_specification,
)
from dfm_pipeline.ingestion.fred_md_transformed_qc import (
    audit_transformed_series,
    discover_vintage_files,
)
from dfm_pipeline.ingestion.fred_md_vintages import (
    load_numeric_fred_md_vintage,
)
from dfm_pipeline.preprocessing.tcode import (
    ALLOWED_TCODES,
    apply_tcode_transformations,
)


COMPARABILITY_COLUMNS = [
    "boundary_position",
    "transition_id",
    "canonical_id",
    "transition_vintage",
    "old_source",
    "new_source",
    "relationship_type",
    "equivalence_level",
    "old_active_to_vintage",
    "new_active_from_vintage",
    "old_expected_tcode",
    "new_expected_tcode",
    "transformation_codes_equal",
    "comparison_mode",
    "selection_note",
    "old_data_vintage",
    "new_data_vintage",
    "old_filename",
    "new_filename",
    "old_embedded_tcode",
    "new_embedded_tcode",
    "old_transformed_n_valid",
    "new_transformed_n_valid",
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
    "diagnostic_flags",
    "mechanical_failure_reasons",
]

ALIGNED_COLUMNS = [
    "boundary_position",
    "transition_id",
    "canonical_id",
    "transition_vintage",
    "old_source",
    "new_source",
    "comparison_mode",
    "old_data_vintage",
    "new_data_vintage",
    "reference_date",
    "old_transformed",
    "new_transformed",
    "difference_new_minus_old",
]

RUN_SUMMARY_COLUMNS = [
    "n_transition_boundaries",
    "n_computed",
    "n_insufficient_overlap",
    "n_zero_variance",
    "n_mechanical_failure",
    "n_same_vintage_transition",
    "n_same_vintage_predecessor",
    "n_adjacent_vintage_bridge",
    "total_aligned_rows",
]


def _join_flags(parts: list[str]) -> str:
    return "|".join(dict.fromkeys(x for x in parts if x))


def _loader_failures(diag: dict[str, Any]) -> list[str]:
    checks = {
        "date_parse_failures": "loader_date_parse_failure",
        "duplicate_reference_dates": "loader_duplicate_reference_dates",
        "non_month_start_dates": "loader_non_month_start_dates",
        "monthly_grid_gaps": "loader_monthly_grid_gaps",
        "bad_numeric_cells": "loader_bad_numeric_cells",
    }
    return [
        reason
        for field, reason in checks.items()
        if int(diag.get(field, 0) or 0) > 0
    ]


def _empty_comparability_row(boundary: pd.Series, position: int) -> dict[str, Any]:
    cid = str(boundary["canonical_id"])
    old = str(boundary["old_source"])
    new = str(boundary["new_source"])
    transition = str(boundary["transition_vintage"])
    transition_id = f"{cid}__{transition}__{old}__to__{new}"

    row = {column: np.nan for column in COMPARABILITY_COLUMNS}
    row.update(
        {
            "boundary_position": int(position),
            "transition_id": transition_id,
            "canonical_id": cid,
            "transition_vintage": transition,
            "old_source": old,
            "new_source": new,
            "relationship_type": str(boundary["relationship_type"]),
            "equivalence_level": str(boundary["equivalence_level"]),
            "old_active_to_vintage": str(boundary["old_active_to_vintage"]),
            "new_active_from_vintage": str(boundary["new_active_from_vintage"]),
            "old_expected_tcode": int(boundary["old_expected_tcode"]),
            "new_expected_tcode": int(boundary["new_expected_tcode"]),
            "transformation_codes_equal": (
                int(boundary["old_expected_tcode"])
                == int(boundary["new_expected_tcode"])
            ),
            "selection_note": "",
            "diagnostic_status": "mechanical_failure",
            "diagnostic_flags": "",
            "mechanical_failure_reasons": "",
        }
    )
    return row


def _load_vintage(
    vintage: str,
    file_map: dict[str, Path],
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if vintage in cache:
        return cache[vintage]

    path = file_map.get(vintage)
    if path is None:
        result = {
            "vintage": vintage,
            "path": None,
            "raw": None,
            "tcodes": {},
            "diagnostics": {},
            "failures": ["vintage_file_missing"],
        }
        cache[vintage] = result
        return result

    try:
        trow = detect_tcode_row(
            path,
            date_col="sasdate",
            max_scan_rows=5,
        )
    except Exception as exc:
        result = {
            "vintage": vintage,
            "path": path,
            "raw": None,
            "tcodes": {},
            "diagnostics": {},
            "failures": [f"tcode_row_detection_error:{type(exc).__name__}"],
        }
        cache[vintage] = result
        return result

    if trow is None:
        result = {
            "vintage": vintage,
            "path": path,
            "raw": None,
            "tcodes": {},
            "diagnostics": {},
            "failures": ["tcode_row_not_found"],
        }
        cache[vintage] = result
        return result

    try:
        raw, diagnostics = load_numeric_fred_md_vintage(
            path,
            tcode_row=trow,
        )
        tcodes = read_embedded_tcode_map(
            path,
            date_col="sasdate",
            tcode_row=trow,
        )
    except Exception as exc:
        result = {
            "vintage": vintage,
            "path": path,
            "raw": None,
            "tcodes": {},
            "diagnostics": {},
            "failures": [f"vintage_load_error:{type(exc).__name__}"],
        }
        cache[vintage] = result
        return result

    result = {
        "vintage": vintage,
        "path": path,
        "raw": raw,
        "tcodes": tcodes,
        "diagnostics": diagnostics,
        "failures": _loader_failures(diagnostics),
    }
    cache[vintage] = result
    return result


def _source_ready(
    bundle: dict[str, Any],
    source: str,
    expected_tcode: int,
) -> tuple[bool, list[str]]:
    failures = list(bundle["failures"])
    raw = bundle["raw"]
    tcodes = bundle["tcodes"]

    if raw is None:
        return False, failures

    if source not in raw.columns:
        failures.append(f"{source}:source_missing")

    embedded = tcodes.get(source)
    if embedded is None:
        failures.append(f"{source}:embedded_tcode_missing_or_invalid")
    elif int(embedded) != int(expected_tcode):
        failures.append(
            f"{source}:embedded_tcode_mismatch:"
            f"expected={expected_tcode}:embedded={embedded}"
        )

    return not failures, failures


def _pair_ready(
    bundle: dict[str, Any],
    *,
    old_source: str,
    new_source: str,
    old_expected_tcode: int,
    new_expected_tcode: int,
) -> bool:
    old_ok, _ = _source_ready(bundle, old_source, old_expected_tcode)
    new_ok, _ = _source_ready(bundle, new_source, new_expected_tcode)
    return old_ok and new_ok


def _transform_source(
    bundle: dict[str, Any],
    source: str,
    expected_tcode: int,
) -> tuple[pd.Series | None, dict[str, Any] | None, list[str]]:
    ready, failures = _source_ready(bundle, source, expected_tcode)
    if not ready:
        return None, None, failures

    raw = bundle["raw"]
    embedded = int(bundle["tcodes"][source])
    if embedded not in ALLOWED_TCODES:
        return None, None, [f"{source}:embedded_tcode_not_allowed:{embedded}"]

    try:
        transformed = apply_tcode_transformations(
            raw[[source]],
            {source: embedded},
        )[source]
    except Exception as exc:
        return None, None, [
            f"{source}:transformation_error:{type(exc).__name__}"
        ]

    metrics = audit_transformed_series(
        raw[source],
        transformed,
        tcode=embedded,
    )

    # C3 is diagnostic. Zero variance or too few transformed observations are
    # represented by diagnostic_status below rather than promoted to a C3
    # mechanical failure. Structural transformation failures remain mechanical.
    nonmechanical_stage2 = {
        "zero_transformed_variance",
        "all_missing_after_transform",
        "insufficient_transformed_observations_for_variance",
    }
    stage2_hard = [
        part
        for part in str(metrics["hard_failure_reasons"]).split("|")
        if part and part not in nonmechanical_stage2
    ]
    failures.extend(f"{source}:{part}" for part in stage2_hard)

    return transformed, metrics, failures


def _select_comparison_mode(
    boundary: pd.Series,
    file_map: dict[str, Path],
    cache: dict[str, dict[str, Any]],
) -> tuple[str, str, str, dict[str, Any], dict[str, Any], str]:
    old_source = str(boundary["old_source"])
    new_source = str(boundary["new_source"])
    old_code = int(boundary["old_expected_tcode"])
    new_code = int(boundary["new_expected_tcode"])
    old_vintage = str(boundary["old_active_to_vintage"])
    new_vintage = str(boundary["new_active_from_vintage"])

    new_bundle = _load_vintage(new_vintage, file_map, cache)
    if _pair_ready(
        new_bundle,
        old_source=old_source,
        new_source=new_source,
        old_expected_tcode=old_code,
        new_expected_tcode=new_code,
    ):
        return (
            "same_vintage_transition",
            new_vintage,
            new_vintage,
            new_bundle,
            new_bundle,
            "",
        )

    old_bundle = _load_vintage(old_vintage, file_map, cache)
    if _pair_ready(
        old_bundle,
        old_source=old_source,
        new_source=new_source,
        old_expected_tcode=old_code,
        new_expected_tcode=new_code,
    ):
        return (
            "same_vintage_predecessor",
            old_vintage,
            old_vintage,
            old_bundle,
            old_bundle,
            "transition_vintage_no_valid_coexistence",
        )

    return (
        "adjacent_vintage_bridge",
        old_vintage,
        new_vintage,
        old_bundle,
        new_bundle,
        (
            "transition_vintage_no_valid_coexistence|"
            "predecessor_vintage_no_valid_coexistence"
        ),
    )


def align_finite_common(
    old: pd.Series,
    new: pd.Series,
    *,
    old_name: str = "old_transformed",
    new_name: str = "new_transformed",
) -> pd.DataFrame:
    """
    Align two series by index and retain only observations finite in both.

    The default output column names preserve the C3 comparability schema.
    Custom names allow reuse for reference-series and level comparisons.
    """
    if not old_name or not new_name:
        raise ValueError("Aligned column names must be non-empty")
    if old_name == new_name:
        raise ValueError("Aligned column names must be distinct")

    aligned = pd.concat(
        [
            pd.to_numeric(old, errors="coerce").rename(old_name),
            pd.to_numeric(new, errors="coerce").rename(new_name),
        ],
        axis=1,
        join="inner",
    )
    finite = (
        np.isfinite(aligned[old_name].to_numpy(dtype=float))
        & np.isfinite(aligned[new_name].to_numpy(dtype=float))
    )
    return aligned.loc[finite].copy()


# Backward-compatible private alias.
_finite_common = align_finite_common


def _pearson(x: pd.Series, y: pd.Series) -> float:
    if len(x) < 2:
        return np.nan
    if float(x.std(ddof=0)) == 0.0 or float(y.std(ddof=0)) == 0.0:
        return np.nan
    return float(x.corr(y))


def _spearman(x: pd.Series, y: pd.Series) -> float:
    if len(x) < 2:
        return np.nan
    xr = x.rank(method="average")
    yr = y.rank(method="average")
    if float(xr.std(ddof=0)) == 0.0 or float(yr.std(ddof=0)) == 0.0:
        return np.nan
    return float(xr.corr(yr))


def _ols_new_on_old(
    old: pd.Series,
    new: pd.Series,
) -> tuple[float, float, float]:
    if len(old) < 2 or float(old.std(ddof=0)) == 0.0:
        return np.nan, np.nan, np.nan

    x = old.to_numpy(dtype=float)
    y = new.to_numpy(dtype=float)
    X = np.column_stack([np.ones(len(x)), x])
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    alpha = float(coef[0])
    beta = float(coef[1])

    fitted = X @ coef
    resid = y - fitted
    sse = float(np.dot(resid, resid))
    centered = y - float(y.mean())
    sst = float(np.dot(centered, centered))
    r2 = float(1.0 - sse / sst) if sst > 0.0 else np.nan
    return alpha, beta, r2


def compute_pairwise_diagnostics(
    aligned: pd.DataFrame,
    *,
    old_col: str = "old_transformed",
    new_col: str = "new_transformed",
) -> dict[str, Any]:
    """
    Compute threshold-free pairwise comparability diagnostics.

    The input must already contain the observations that should enter the
    comparison. No statistical acceptance threshold is imposed here.
    """
    missing = [
        column
        for column in (old_col, new_col)
        if column not in aligned.columns
    ]
    if missing:
        raise ValueError(
            "Aligned comparison data missing columns: "
            + ", ".join(missing)
        )

    n = int(len(aligned))
    if n == 0:
        return {
            "n_common_valid": 0,
            "first_common_date": None,
            "last_common_date": None,
            "pearson_corr": np.nan,
            "spearman_corr": np.nan,
            "old_mean": np.nan,
            "new_mean": np.nan,
            "old_std": np.nan,
            "new_std": np.nan,
            "std_ratio_new_old": np.nan,
            "mean_difference": np.nan,
            "mae_difference": np.nan,
            "rmse_difference": np.nan,
            "nrmse_old_std": np.nan,
            "sign_agreement": np.nan,
            "ols_alpha": np.nan,
            "ols_beta": np.nan,
            "ols_r2": np.nan,
            "diagnostic_status": "insufficient_overlap",
        }

    old = aligned[old_col].astype(float)
    new = aligned[new_col].astype(float)
    diff = new - old

    old_std = float(old.std(ddof=0))
    new_std = float(new.std(ddof=0))
    alpha, beta, r2 = _ols_new_on_old(old, new)
    rmse = float(np.sqrt(np.mean(np.square(diff.to_numpy(dtype=float)))))
    first = pd.Timestamp(aligned.index.min()).date().isoformat()
    last = pd.Timestamp(aligned.index.max()).date().isoformat()

    if n < 2:
        status = "insufficient_overlap"
    elif old_std == 0.0 or new_std == 0.0:
        status = "zero_variance"
    else:
        status = "computed"

    return {
        "n_common_valid": n,
        "first_common_date": first,
        "last_common_date": last,
        "pearson_corr": _pearson(old, new),
        "spearman_corr": _spearman(old, new),
        "old_mean": float(old.mean()),
        "new_mean": float(new.mean()),
        "old_std": old_std,
        "new_std": new_std,
        "std_ratio_new_old": (
            float(new_std / old_std) if old_std > 0.0 else np.nan
        ),
        "mean_difference": float(diff.mean()),
        "mae_difference": float(np.mean(np.abs(diff.to_numpy(dtype=float)))),
        "rmse_difference": rmse,
        "nrmse_old_std": (
            float(rmse / old_std) if old_std > 0.0 else np.nan
        ),
        "sign_agreement": float(
            np.mean(
                np.sign(old.to_numpy(dtype=float))
                == np.sign(new.to_numpy(dtype=float))
            )
        ),
        "ols_alpha": alpha,
        "ols_beta": beta,
        "ols_r2": r2,
        "diagnostic_status": status,
    }


# Backward-compatible private alias.
_metrics_from_aligned = compute_pairwise_diagnostics


def build_comparability_run_summary(
    comparison: pd.DataFrame,
    aligned: pd.DataFrame,
) -> pd.DataFrame:
    status = comparison["diagnostic_status"].astype(str)
    mode = comparison["comparison_mode"].astype(str)
    row = {
        "n_transition_boundaries": int(len(comparison)),
        "n_computed": int(status.eq("computed").sum()),
        "n_insufficient_overlap": int(status.eq("insufficient_overlap").sum()),
        "n_zero_variance": int(status.eq("zero_variance").sum()),
        "n_mechanical_failure": int(status.eq("mechanical_failure").sum()),
        "n_same_vintage_transition": int(
            mode.eq("same_vintage_transition").sum()
        ),
        "n_same_vintage_predecessor": int(
            mode.eq("same_vintage_predecessor").sum()
        ),
        "n_adjacent_vintage_bridge": int(
            mode.eq("adjacent_vintage_bridge").sum()
        ),
        "total_aligned_rows": int(len(aligned)),
    }
    return pd.DataFrame([row], columns=RUN_SUMMARY_COLUMNS)


def audit_rt_canonical_comparability(
    raw_dir: str | Path,
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    *,
    recursive: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Diagnose transformed predecessor/successor comparability at each canonical
    source-transition boundary.

    Policy
    ------
    1. Use the candidate panel/source map only; no forecast outcomes enter.
    2. Prefer a same-vintage comparison in the first successor vintage if both
       source columns are present and have the expected embedded t-codes.
    3. Otherwise prefer a same-vintage comparison in the last predecessor
       vintage if both sources coexist there with expected t-codes.
    4. Otherwise compare predecessor history from its last active vintage with
       successor history from its first active vintage. This is explicitly
       labelled ``adjacent_vintage_bridge`` because ordinary cross-vintage
       revisions may contribute to measured differences.
    5. Transform each raw source independently using its own embedded t-code
       before aligning by reference date.
    6. Compute diagnostics on the full finite common transformed history.
       There are no arbitrary correlation/variance acceptance thresholds.
    """
    validate_rt_canonical_specification(panel, source_map)
    boundaries = build_transition_boundaries(panel, source_map)
    if boundaries.empty:
        raise ValueError(
            "RT_CANONICAL specification contains no transition boundaries"
        )

    stable_start = str(panel["stable_window_start"].iloc[0])
    stable_end = str(panel["stable_window_end"].iloc[0])
    discovered = discover_vintage_files(
        raw_dir,
        start=stable_start,
        end=stable_end,
        recursive=recursive,
    )
    file_map = dict(discovered)
    cache: dict[str, dict[str, Any]] = {}

    summary_rows: list[dict[str, Any]] = []
    aligned_rows: list[dict[str, Any]] = []

    for position, (_, boundary) in enumerate(
        boundaries.iterrows(),
        start=1,
    ):
        row = _empty_comparability_row(boundary, position)
        flags: list[str] = []
        failures: list[str] = []

        (
            mode,
            old_data_vintage,
            new_data_vintage,
            old_bundle,
            new_bundle,
            selection_note,
        ) = _select_comparison_mode(boundary, file_map, cache)

        row["comparison_mode"] = mode
        row["selection_note"] = selection_note
        row["old_data_vintage"] = old_data_vintage
        row["new_data_vintage"] = new_data_vintage
        row["old_filename"] = (
            old_bundle["path"].name
            if old_bundle["path"] is not None
            else np.nan
        )
        row["new_filename"] = (
            new_bundle["path"].name
            if new_bundle["path"] is not None
            else np.nan
        )

        old_source = str(boundary["old_source"])
        new_source = str(boundary["new_source"])
        old_expected = int(boundary["old_expected_tcode"])
        new_expected = int(boundary["new_expected_tcode"])

        row["old_embedded_tcode"] = old_bundle["tcodes"].get(
            old_source,
            np.nan,
        )
        row["new_embedded_tcode"] = new_bundle["tcodes"].get(
            new_source,
            np.nan,
        )

        if mode == "adjacent_vintage_bridge":
            flags.append("cross_vintage_revisions_may_contribute")
        if old_expected != new_expected:
            flags.append("different_expected_tcodes")

        old_transformed, old_metrics, old_fail = _transform_source(
            old_bundle,
            old_source,
            old_expected,
        )
        new_transformed, new_metrics, new_fail = _transform_source(
            new_bundle,
            new_source,
            new_expected,
        )
        failures.extend(old_fail)
        failures.extend(new_fail)

        if old_metrics is not None:
            row["old_transformed_n_valid"] = int(
                old_metrics["transformed_n_valid"]
            )
        if new_metrics is not None:
            row["new_transformed_n_valid"] = int(
                new_metrics["transformed_n_valid"]
            )

        if failures or old_transformed is None or new_transformed is None:
            row["diagnostic_status"] = "mechanical_failure"
            row["diagnostic_flags"] = _join_flags(flags)
            row["mechanical_failure_reasons"] = _join_flags(failures)
            summary_rows.append(row)
            continue

        aligned = align_finite_common(old_transformed, new_transformed)
        row.update(compute_pairwise_diagnostics(aligned))
        row["diagnostic_flags"] = _join_flags(flags)
        row["mechanical_failure_reasons"] = ""

        for ref_date, obs in aligned.iterrows():
            aligned_rows.append(
                {
                    "boundary_position": int(position),
                    "transition_id": row["transition_id"],
                    "canonical_id": row["canonical_id"],
                    "transition_vintage": row["transition_vintage"],
                    "old_source": old_source,
                    "new_source": new_source,
                    "comparison_mode": mode,
                    "old_data_vintage": old_data_vintage,
                    "new_data_vintage": new_data_vintage,
                    "reference_date": pd.Timestamp(ref_date).date().isoformat(),
                    "old_transformed": float(obs["old_transformed"]),
                    "new_transformed": float(obs["new_transformed"]),
                    "difference_new_minus_old": float(
                        obs["new_transformed"] - obs["old_transformed"]
                    ),
                }
            )

        summary_rows.append(row)

    comparison = pd.DataFrame(
        summary_rows,
        columns=COMPARABILITY_COLUMNS,
    ).sort_values("boundary_position").reset_index(drop=True)

    aligned = pd.DataFrame(aligned_rows, columns=ALIGNED_COLUMNS)
    if not aligned.empty:
        aligned = aligned.sort_values(
            ["boundary_position", "reference_date"]
        ).reset_index(drop=True)

    run_summary = build_comparability_run_summary(comparison, aligned)
    return comparison, aligned, run_summary


__all__ = [
    "COMPARABILITY_COLUMNS",
    "ALIGNED_COLUMNS",
    "RUN_SUMMARY_COLUMNS",
    "align_finite_common",
    "compute_pairwise_diagnostics",
    "audit_rt_canonical_comparability",
    "build_comparability_run_summary",
]