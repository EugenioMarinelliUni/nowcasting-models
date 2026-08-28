from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md import (
    detect_tcode_row,
    read_embedded_tcode_map,
)
from dfm_pipeline.ingestion.fred_md_panel_selection import (
    parse_registry_bool,
    select_panel_candidates,
)
from dfm_pipeline.ingestion.fred_md_vintages import (
    load_numeric_fred_md_vintage,
    parse_vintage_filename,
)
from dfm_pipeline.preprocessing.tcode import (
    ALLOWED_TCODES,
    LEADS_LOST,
    apply_tcode_transformations,
)


def _join_flags(flags: Iterable[str]) -> str:
    return "|".join(dict.fromkeys(flag for flag in flags if flag))


def _union_pipe(values: pd.Series) -> str:
    flags: list[str] = []
    for value in values.astype(str):
        flags.extend(part for part in value.split("|") if part)
    return _join_flags(flags)


def expected_transform_valid_mask(
    raw: pd.Series,
    tcode: int,
) -> pd.Series:
    """
    Mark dates where the canonical t-code transformation is mathematically
    expected to produce a finite value from the available raw observations.

    This mask is intentionally independent of the transformed output.  It lets
    Stage 2 detect both unexpected NaNs and accidental values produced across
    genuine raw-data gaps.
    """
    if int(tcode) not in ALLOWED_TCODES:
        raise ValueError(
            f"Unknown tcode: {tcode}. Allowed: {sorted(ALLOWED_TCODES)}"
        )

    s = pd.to_numeric(raw, errors="coerce").astype(float)
    observed = s.notna()
    code = int(tcode)

    if code == 1:
        mask = observed
    elif code == 2:
        mask = observed & observed.shift(1, fill_value=False)
    elif code == 3:
        mask = (
            observed
            & observed.shift(1, fill_value=False)
            & observed.shift(2, fill_value=False)
        )
    elif code == 4:
        mask = observed & s.gt(0)
    elif code == 5:
        positive = observed & s.gt(0)
        mask = positive & positive.shift(1, fill_value=False)
    elif code == 6:
        positive = observed & s.gt(0)
        mask = (
            positive
            & positive.shift(1, fill_value=False)
            & positive.shift(2, fill_value=False)
        )
    else:  # code == 7
        mask = (
            observed
            & observed.shift(1, fill_value=False)
            & observed.shift(2, fill_value=False)
            & s.shift(1).ne(0).fillna(False)
            & s.shift(2).ne(0).fillna(False)
        )

    return mask.astype(bool)


def audit_transformed_series(
    raw: pd.Series,
    transformed: pd.Series,
    *,
    tcode: int,
    near_zero_std: float | None = None,
    history_cutoff: str | pd.Timestamp | None = None,
    min_history: int | None = None,
) -> dict[str, Any]:
    """Audit one raw/transformed series pair for a single vintage."""
    if not raw.index.equals(transformed.index):
        raise ValueError("raw and transformed series must have identical indices")
    if near_zero_std is not None and near_zero_std < 0:
        raise ValueError("near_zero_std must be non-negative")
    if min_history is not None and min_history < 0:
        raise ValueError("min_history must be non-negative")
    if min_history is not None and history_cutoff is None:
        raise ValueError("min_history requires history_cutoff")

    code = int(tcode)
    if code not in ALLOWED_TCODES:
        raise ValueError(
            f"Unknown tcode: {code}. Allowed: {sorted(ALLOWED_TCODES)}"
        )

    raw_num = pd.to_numeric(raw, errors="coerce").astype(float)
    y = pd.to_numeric(transformed, errors="coerce").astype(float)
    expected_valid = expected_transform_valid_mask(raw_num, code)

    finite = pd.Series(np.isfinite(y.to_numpy()), index=y.index)
    actual_valid = y.notna() & finite
    unexpected_missing = expected_valid & ~actual_valid
    unexpected_value = ~expected_valid & actual_valid

    valid_values = y.loc[actual_valid]
    n_valid = int(actual_valid.sum())
    n_rows = int(len(y))
    std = (
        float(valid_values.std(ddof=0))
        if n_valid > 0
        else np.nan
    )

    zero_variance = bool(n_valid >= 2 and std == 0.0)
    near_zero_variance = bool(
        near_zero_std is not None
        and n_valid >= 2
        and np.isfinite(std)
        and std > 0.0
        and std <= float(near_zero_std)
    )

    n_nonpositive_for_log = (
        int((raw_num.notna() & raw_num.le(0)).sum())
        if code in {4, 5, 6}
        else 0
    )
    n_zero_raw_for_ratio = (
        int((raw_num.notna() & raw_num.eq(0)).sum())
        if code == 7
        else 0
    )

    first_valid = valid_values.index.min() if n_valid else None
    last_valid = valid_values.index.max() if n_valid else None

    n_valid_before_cutoff: int | None = None
    short_history = False
    if history_cutoff is not None:
        cutoff = pd.Timestamp(history_cutoff)
        n_valid_before_cutoff = int(
            actual_valid.loc[actual_valid.index <= cutoff].sum()
        )
        if min_history is not None:
            short_history = n_valid_before_cutoff < int(min_history)

    hard_failures: list[str] = []
    flags: list[str] = []

    if n_valid == 0:
        hard_failures.append("all_missing_after_transform")
    elif n_valid < 2:
        hard_failures.append(
            "insufficient_transformed_observations_for_variance"
        )

    n_inf = int(np.isinf(y.to_numpy()).sum())
    if n_inf:
        hard_failures.append("infinite_value_after_transform")
    if int(unexpected_missing.sum()):
        hard_failures.append("unexpected_missing_after_transform")
    if int(unexpected_value.sum()):
        hard_failures.append("unexpected_value_after_transform")
    if zero_variance:
        hard_failures.append("zero_transformed_variance")

    if n_nonpositive_for_log:
        flags.append("nonpositive_log_domain_input")
    if n_zero_raw_for_ratio:
        flags.append("zero_ratio_denominator_input")
    if near_zero_variance:
        flags.append("near_zero_transformed_variance")
    if short_history:
        flags.append("short_pre_evaluation_history")

    return {
        "n_rows": n_rows,
        "raw_n_valid": int(raw_num.notna().sum()),
        "expected_leads_lost": int(LEADS_LOST[code]),
        "expected_transformed_n_valid": int(expected_valid.sum()),
        "transformed_n_valid": n_valid,
        "transformed_observed_fraction": (
            float(n_valid / n_rows) if n_rows else np.nan
        ),
        "n_unexpected_transform_missing": int(unexpected_missing.sum()),
        "n_unexpected_transform_value": int(unexpected_value.sum()),
        "n_inf_after_transform": n_inf,
        "n_nonpositive_raw_for_log": n_nonpositive_for_log,
        "n_zero_raw_for_ratio": n_zero_raw_for_ratio,
        "transformed_mean": (
            float(valid_values.mean()) if n_valid else np.nan
        ),
        "transformed_std": std,
        "transformed_min": (
            float(valid_values.min()) if n_valid else np.nan
        ),
        "transformed_max": (
            float(valid_values.max()) if n_valid else np.nan
        ),
        "transformed_n_unique": int(valid_values.nunique(dropna=True)),
        "zero_variance": zero_variance,
        "near_zero_variance": near_zero_variance,
        "first_transformed_valid_date": (
            pd.Timestamp(first_valid).date().isoformat()
            if first_valid is not None
            else None
        ),
        "last_transformed_valid_date": (
            pd.Timestamp(last_valid).date().isoformat()
            if last_valid is not None
            else None
        ),
        "history_cutoff": (
            pd.Timestamp(history_cutoff).date().isoformat()
            if history_cutoff is not None
            else None
        ),
        "n_valid_before_history_cutoff": n_valid_before_cutoff,
        "min_history_requested": min_history,
        "short_pre_evaluation_history": short_history,
        "stage2_hard_fail": bool(hard_failures),
        "hard_failure_reasons": _join_flags(hard_failures),
        "stage2_flags": _join_flags(flags),
    }


def validate_stage1_gate(
    selected_registry: pd.DataFrame,
    stage1_report: pd.DataFrame,
) -> None:
    """Require every Stage-2 candidate to be present and clean in Stage 1."""
    required = {"raw_series", "qc_hard_fail"}
    missing_cols = sorted(required - set(stage1_report.columns))
    if missing_cols:
        raise ValueError(
            "Stage-1 report is missing required columns: "
            + ", ".join(missing_cols)
        )

    stage1 = stage1_report.copy()
    stage1["raw_series"] = stage1["raw_series"].astype(str)
    if stage1["raw_series"].duplicated().any():
        dup = sorted(
            stage1.loc[
                stage1["raw_series"].duplicated(keep=False),
                "raw_series",
            ].unique()
        )
        raise ValueError(
            "Stage-1 report contains duplicate series: " + ", ".join(dup)
        )

    wanted = selected_registry["raw_series"].astype(str).tolist()
    indexed = stage1.set_index("raw_series")
    missing_series = [series for series in wanted if series not in indexed.index]
    if missing_series:
        raise ValueError(
            "Stage-1 report does not contain all Stage-2 candidates: "
            + ", ".join(missing_series)
        )

    hard_fail: list[str] = []
    for series in wanted:
        value = indexed.at[series, "qc_hard_fail"]
        if parse_registry_bool(
            value,
            field="qc_hard_fail",
            raw_series=series,
        ):
            hard_fail.append(series)

    if hard_fail:
        raise ValueError(
            "Stage-2 cannot proceed because Stage-1 hard failures remain: "
            + ", ".join(hard_fail)
        )


def discover_vintage_files(
    raw_dir: str | Path,
    *,
    start: str,
    end: str,
    recursive: bool = False,
) -> list[tuple[str, Path]]:
    """Return exactly one raw CSV for every monthly vintage in [start, end]."""
    root = Path(raw_dir)
    if not root.exists():
        raise FileNotFoundError(f"Raw vintage directory not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Expected a directory: {root}")

    start_p = pd.Period(start, freq="M")
    end_p = pd.Period(end, freq="M")
    if end_p < start_p:
        raise ValueError("end vintage is earlier than start vintage")

    paths = sorted(root.rglob("*.csv") if recursive else root.glob("*.csv"))
    by_vintage: dict[str, list[Path]] = {}
    for path in paths:
        vintage = parse_vintage_filename(path.name)
        if vintage is None:
            continue
        period = pd.Period(vintage, freq="M")
        if start_p <= period <= end_p:
            by_vintage.setdefault(vintage, []).append(path)

    duplicates = {
        vintage: files
        for vintage, files in by_vintage.items()
        if len(files) != 1
    }
    if duplicates:
        detail = "; ".join(
            f"{vintage}: {[p.name for p in files]}"
            for vintage, files in sorted(duplicates.items())
        )
        raise ValueError(f"Duplicate vintage files in Stage-2 window: {detail}")

    expected = pd.period_range(start_p, end_p, freq="M")
    missing = [
        str(period)
        for period in expected
        if str(period) not in by_vintage
    ]
    if missing:
        raise ValueError(
            "Missing raw vintage files in Stage-2 window: "
            + ", ".join(missing)
        )

    return [
        (str(period), by_vintage[str(period)][0])
        for period in expected
    ]


def _failed_vintage_series_row(
    *,
    vintage: str,
    filename: str,
    raw_series: str,
    selection_class: str,
    registry_tcode: int | None,
    embedded_tcode: int | None,
    reasons: Iterable[str],
    flags: Iterable[str] = (),
    loader_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    loader_diagnostics = loader_diagnostics or {}
    return {
        "vintage": vintage,
        "filename": filename,
        "raw_series": raw_series,
        "selection_class": selection_class,
        "registry_tcode": registry_tcode,
        "tcode": embedded_tcode,
        "n_rows": np.nan,
        "raw_n_valid": np.nan,
        "expected_leads_lost": (
            LEADS_LOST.get(int(embedded_tcode))
            if embedded_tcode is not None
            else np.nan
        ),
        "expected_transformed_n_valid": np.nan,
        "transformed_n_valid": 0,
        "transformed_observed_fraction": np.nan,
        "n_unexpected_transform_missing": np.nan,
        "n_unexpected_transform_value": np.nan,
        "n_inf_after_transform": np.nan,
        "n_nonpositive_raw_for_log": np.nan,
        "n_zero_raw_for_ratio": np.nan,
        "transformed_mean": np.nan,
        "transformed_std": np.nan,
        "transformed_min": np.nan,
        "transformed_max": np.nan,
        "transformed_n_unique": 0,
        "zero_variance": False,
        "near_zero_variance": False,
        "first_transformed_valid_date": None,
        "last_transformed_valid_date": None,
        "history_cutoff": None,
        "n_valid_before_history_cutoff": np.nan,
        "min_history_requested": np.nan,
        "short_pre_evaluation_history": False,
        "loader_date_parse_failures": loader_diagnostics.get(
            "date_parse_failures", np.nan
        ),
        "loader_duplicate_reference_dates": loader_diagnostics.get(
            "duplicate_reference_dates", np.nan
        ),
        "loader_non_month_start_dates": loader_diagnostics.get(
            "non_month_start_dates", np.nan
        ),
        "loader_monthly_grid_gaps": loader_diagnostics.get(
            "monthly_grid_gaps", np.nan
        ),
        "loader_bad_numeric_cells": loader_diagnostics.get(
            "bad_numeric_cells", np.nan
        ),
        "stage2_hard_fail": True,
        "hard_failure_reasons": _join_flags(reasons),
        "stage2_flags": _join_flags(flags),
    }


def audit_transformed_vintage(
    csv_path: str | Path,
    selected_registry: pd.DataFrame,
    *,
    vintage: str | None = None,
    near_zero_std: float | None = None,
    history_cutoff: str | pd.Timestamp | None = None,
    min_history: int | None = None,
) -> pd.DataFrame:
    """Run Stage-2 transformation QC for every selected series in one vintage."""
    path = Path(csv_path)
    vintage_id = vintage or parse_vintage_filename(path.name)
    if vintage_id is None:
        raise ValueError(f"Cannot infer vintage from filename: {path.name}")

    required = {"raw_series", "selection_class", "tcode"}
    missing_cols = sorted(required - set(selected_registry.columns))
    if missing_cols:
        raise ValueError(
            "Selected registry is missing columns: " + ", ".join(missing_cols)
        )

    tcode_row = detect_tcode_row(path, date_col="sasdate", max_scan_rows=5)
    if tcode_row is None:
        return pd.DataFrame(
            [
                _failed_vintage_series_row(
                    vintage=vintage_id,
                    filename=path.name,
                    raw_series=str(row["raw_series"]),
                    selection_class=str(row["selection_class"]),
                    registry_tcode=(
                        int(row["tcode"])
                        if pd.notna(row["tcode"])
                        else None
                    ),
                    embedded_tcode=None,
                    reasons=["tcode_row_not_found"],
                )
                for _, row in selected_registry.iterrows()
            ]
        )

    panel, loader_diag = load_numeric_fred_md_vintage(
        path,
        tcode_row=tcode_row,
    )
    embedded = read_embedded_tcode_map(
        path,
        date_col="sasdate",
        tcode_row=tcode_row,
    )

    loader_failures: list[str] = []
    if int(loader_diag.get("date_parse_failures", 0) or 0):
        loader_failures.append("loader_date_parse_failure")
    if int(loader_diag.get("duplicate_reference_dates", 0) or 0):
        loader_failures.append("loader_duplicate_reference_dates")
    if int(loader_diag.get("non_month_start_dates", 0) or 0):
        loader_failures.append("loader_non_month_start_dates")
    if int(loader_diag.get("monthly_grid_gaps", 0) or 0):
        loader_failures.append("loader_monthly_grid_gaps")
    if int(loader_diag.get("bad_numeric_cells", 0) or 0):
        loader_failures.append("loader_bad_numeric_cells")

    usable: list[str] = []
    tcode_map: dict[str, int] = {}
    pre_failures: dict[str, list[str]] = {}

    for _, row in selected_registry.iterrows():
        series = str(row["raw_series"])
        failures: list[str] = list(loader_failures)
        registry_tcode = (
            int(row["tcode"])
            if pd.notna(row["tcode"])
            else None
        )

        if series not in panel.columns:
            failures.append("raw_series_missing_from_vintage")
        embedded_tcode = embedded.get(series)
        if embedded_tcode is None:
            failures.append("embedded_tcode_missing_or_invalid")
        if registry_tcode is None:
            failures.append("registry_tcode_missing_or_invalid")
        elif (
            embedded_tcode is not None
            and int(embedded_tcode) != int(registry_tcode)
        ):
            failures.append("embedded_tcode_mismatch_registry")

        pre_failures[series] = failures
        if series in panel.columns and embedded_tcode in ALLOWED_TCODES:
            usable.append(series)
            tcode_map[series] = int(embedded_tcode)

    transformed = pd.DataFrame(index=panel.index)
    if usable:
        transformed = apply_tcode_transformations(
            panel[usable],
            tcode_map,
        )

    rows: list[dict[str, Any]] = []
    for _, registry_row in selected_registry.iterrows():
        series = str(registry_row["raw_series"])
        selection_class = str(registry_row["selection_class"])
        registry_tcode = (
            int(registry_row["tcode"])
            if pd.notna(registry_row["tcode"])
            else None
        )
        embedded_tcode = embedded.get(series)
        failures = list(pre_failures[series])

        if series not in transformed.columns or embedded_tcode is None:
            rows.append(
                _failed_vintage_series_row(
                    vintage=vintage_id,
                    filename=path.name,
                    raw_series=series,
                    selection_class=selection_class,
                    registry_tcode=registry_tcode,
                    embedded_tcode=embedded_tcode,
                    reasons=failures or ["transformation_not_available"],
                    loader_diagnostics=loader_diag,
                )
            )
            continue

        metrics = audit_transformed_series(
            panel[series],
            transformed[series],
            tcode=int(embedded_tcode),
            near_zero_std=near_zero_std,
            history_cutoff=history_cutoff,
            min_history=min_history,
        )

        metric_failures = [
            part
            for part in str(metrics["hard_failure_reasons"]).split("|")
            if part
        ]
        combined_failures = failures + metric_failures
        metrics["stage2_hard_fail"] = bool(combined_failures)
        metrics["hard_failure_reasons"] = _join_flags(combined_failures)

        rows.append(
            {
                "vintage": vintage_id,
                "filename": path.name,
                "raw_series": series,
                "selection_class": selection_class,
                "registry_tcode": registry_tcode,
                "tcode": int(embedded_tcode),
                **metrics,
                "loader_date_parse_failures": int(
                    loader_diag.get("date_parse_failures", 0) or 0
                ),
                "loader_duplicate_reference_dates": int(
                    loader_diag.get("duplicate_reference_dates", 0) or 0
                ),
                "loader_non_month_start_dates": int(
                    loader_diag.get("non_month_start_dates", 0) or 0
                ),
                "loader_monthly_grid_gaps": int(
                    loader_diag.get("monthly_grid_gaps", 0) or 0
                ),
                "loader_bad_numeric_cells": int(
                    loader_diag.get("bad_numeric_cells", 0) or 0
                ),
            }
        )

    return pd.DataFrame(rows).sort_values("raw_series").reset_index(drop=True)


def aggregate_transformed_qc(
    by_vintage: pd.DataFrame,
) -> pd.DataFrame:
    """Collapse vintage-by-series Stage-2 diagnostics to one row per series."""
    if by_vintage.empty:
        raise ValueError("Cannot aggregate an empty Stage-2 report")

    rows: list[dict[str, Any]] = []
    for series, group in by_vintage.groupby("raw_series", sort=True):
        g = group.sort_values("vintage")
        std = pd.to_numeric(g["transformed_std"], errors="coerce")
        n_valid = pd.to_numeric(g["transformed_n_valid"], errors="coerce")
        observed = pd.to_numeric(
            g["transformed_observed_fraction"], errors="coerce"
        )
        n_unique = pd.to_numeric(
            g["transformed_n_unique"], errors="coerce"
        )

        hard = bool(g["stage2_hard_fail"].astype(bool).any())
        hard_reasons = _union_pipe(g["hard_failure_reasons"])
        flags = _union_pipe(g["stage2_flags"])
        if hard:
            status = "hard_fail"
        elif flags:
            status = "pass_with_flags"
        else:
            status = "pass"

        tcodes = sorted(
            {
                int(value)
                for value in pd.to_numeric(g["tcode"], errors="coerce").dropna()
            }
        )

        rows.append(
            {
                "raw_series": series,
                "selection_class": str(g["selection_class"].iloc[0]),
                "registry_tcode": g["registry_tcode"].iloc[0],
                "embedded_tcodes_seen": "|".join(str(x) for x in tcodes),
                "n_vintages_checked": int(len(g)),
                "n_vintages_hard_fail": int(g["stage2_hard_fail"].sum()),
                "n_vintages_with_flags": int(
                    g["stage2_flags"].astype(str).str.len().gt(0).sum()
                ),
                "min_transformed_n_valid": (
                    int(n_valid.min()) if n_valid.notna().any() else np.nan
                ),
                "median_transformed_n_valid": (
                    float(n_valid.median()) if n_valid.notna().any() else np.nan
                ),
                "min_transformed_observed_fraction": (
                    float(observed.min()) if observed.notna().any() else np.nan
                ),
                "median_transformed_observed_fraction": (
                    float(observed.median()) if observed.notna().any() else np.nan
                ),
                "min_transformed_std": (
                    float(std.min()) if std.notna().any() else np.nan
                ),
                "median_transformed_std": (
                    float(std.median()) if std.notna().any() else np.nan
                ),
                "min_transformed_n_unique": (
                    int(n_unique.min()) if n_unique.notna().any() else np.nan
                ),
                "n_zero_variance_vintages": int(g["zero_variance"].sum()),
                "n_near_zero_variance_vintages": int(
                    g["near_zero_variance"].sum()
                ),
                "n_vintages_with_nonpositive_log_input": int(
                    pd.to_numeric(
                        g["n_nonpositive_raw_for_log"], errors="coerce"
                    ).fillna(0).gt(0).sum()
                ),
                "n_vintages_with_zero_ratio_input": int(
                    pd.to_numeric(
                        g["n_zero_raw_for_ratio"], errors="coerce"
                    ).fillna(0).gt(0).sum()
                ),
                "total_unexpected_transform_missing": int(
                    pd.to_numeric(
                        g["n_unexpected_transform_missing"], errors="coerce"
                    ).fillna(0).sum()
                ),
                "total_unexpected_transform_value": int(
                    pd.to_numeric(
                        g["n_unexpected_transform_value"], errors="coerce"
                    ).fillna(0).sum()
                ),
                "total_inf_after_transform": int(
                    pd.to_numeric(
                        g["n_inf_after_transform"], errors="coerce"
                    ).fillna(0).sum()
                ),
                "n_vintages_short_pre_evaluation_history": int(
                    g["short_pre_evaluation_history"].sum()
                ),
                "stage2_hard_fail": hard,
                "stage2_status": status,
                "hard_failure_reasons": hard_reasons,
                "stage2_flags": flags,
            }
        )

    out = pd.DataFrame(rows).sort_values("raw_series").reset_index(drop=True)
    out["rank_low_min_transformed_n_valid"] = (
        pd.to_numeric(out["min_transformed_n_valid"], errors="coerce")
        .rank(method="min", ascending=True, na_option="bottom")
        .round()
        .astype("Int64")
    )
    out["rank_low_min_transformed_observed_fraction"] = (
        pd.to_numeric(
            out["min_transformed_observed_fraction"], errors="coerce"
        )
        .rank(method="min", ascending=True, na_option="bottom")
        .round()
        .astype("Int64")
    )
    out["rank_low_min_transformed_std"] = (
        pd.to_numeric(out["min_transformed_std"], errors="coerce")
        .rank(method="min", ascending=True, na_option="bottom")
        .round()
        .astype("Int64")
    )
    return out


def build_transformed_summary(
    by_vintage: pd.DataFrame,
    by_series: pd.DataFrame,
    *,
    start: str,
    end: str,
) -> pd.DataFrame:
    """Build a compact Stage-2 collection summary."""
    min_n_valid = pd.to_numeric(
        by_series["min_transformed_n_valid"], errors="coerce"
    )
    min_fraction = pd.to_numeric(
        by_series["min_transformed_observed_fraction"], errors="coerce"
    )
    min_std = pd.to_numeric(
        by_series["min_transformed_std"], errors="coerce"
    )

    return pd.DataFrame(
        [
            {
                "selection_scope": "rt-stable-candidates",
                "start_vintage": start,
                "end_vintage": end,
                "n_vintages_expected": int(
                    len(pd.period_range(start, end, freq="M"))
                ),
                "n_vintages_checked": int(by_vintage["vintage"].nunique()),
                "n_series_selected": int(len(by_series)),
                "n_series_hard_fail": int(by_series["stage2_hard_fail"].sum()),
                "n_series_pass": int(
                    by_series["stage2_status"].eq("pass").sum()
                ),
                "n_series_pass_with_flags": int(
                    by_series["stage2_status"].eq("pass_with_flags").sum()
                ),
                "n_vintage_series_rows": int(len(by_vintage)),
                "n_vintage_series_hard_fail": int(
                    by_vintage["stage2_hard_fail"].sum()
                ),
                "n_zero_variance_vintage_series": int(
                    by_vintage["zero_variance"].sum()
                ),
                "n_near_zero_variance_vintage_series": int(
                    by_vintage["near_zero_variance"].sum()
                ),
                "total_unexpected_transform_missing": int(
                    pd.to_numeric(
                        by_vintage["n_unexpected_transform_missing"],
                        errors="coerce",
                    ).fillna(0).sum()
                ),
                "total_unexpected_transform_value": int(
                    pd.to_numeric(
                        by_vintage["n_unexpected_transform_value"],
                        errors="coerce",
                    ).fillna(0).sum()
                ),
                "total_inf_after_transform": int(
                    pd.to_numeric(
                        by_vintage["n_inf_after_transform"],
                        errors="coerce",
                    ).fillna(0).sum()
                ),
                "min_of_min_transformed_n_valid": (
                    int(min_n_valid.min())
                    if min_n_valid.notna().any()
                    else np.nan
                ),
                "min_of_min_transformed_observed_fraction": (
                    float(min_fraction.min())
                    if min_fraction.notna().any()
                    else np.nan
                ),
                "min_of_min_transformed_std": (
                    float(min_std.min())
                    if min_std.notna().any()
                    else np.nan
                ),
            }
        ]
    )


def audit_transformed_collection(
    raw_dir: str | Path,
    registry: pd.DataFrame,
    *,
    start: str,
    end: str,
    stage1_report: pd.DataFrame | None = None,
    recursive: bool = False,
    near_zero_std: float | None = None,
    history_cutoff: str | pd.Timestamp | None = None,
    min_history: int | None = None,
) -> dict[str, pd.DataFrame]:
    """Run Stage-2 transformed-series QC over the complete stable window."""
    selected = select_panel_candidates(
        registry,
        scope="rt-stable-candidates",
    )

    bad_tcodes = selected.loc[selected["tcode"].isna(), "raw_series"].tolist()
    if bad_tcodes:
        raise ValueError(
            "Selected Stage-2 candidates lack a unique valid registry t-code: "
            + ", ".join(bad_tcodes)
        )

    if stage1_report is not None:
        validate_stage1_gate(selected, stage1_report)

    files = discover_vintage_files(
        raw_dir,
        start=start,
        end=end,
        recursive=recursive,
    )

    reports = [
        audit_transformed_vintage(
            path,
            selected,
            vintage=vintage,
            near_zero_std=near_zero_std,
            history_cutoff=history_cutoff,
            min_history=min_history,
        )
        for vintage, path in files
    ]
    by_vintage = pd.concat(reports, ignore_index=True)
    by_series = aggregate_transformed_qc(by_vintage)
    summary = build_transformed_summary(
        by_vintage,
        by_series,
        start=start,
        end=end,
    )

    return {
        "selected_registry": selected,
        "by_vintage": by_vintage,
        "by_series": by_series,
        "summary": summary,
    }


__all__ = [
    "expected_transform_valid_mask",
    "audit_transformed_series",
    "validate_stage1_gate",
    "discover_vintage_files",
    "audit_transformed_vintage",
    "aggregate_transformed_qc",
    "build_transformed_summary",
    "audit_transformed_collection",
]
