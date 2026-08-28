from __future__ import annotations

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md_panel_selection import (
    parse_registry_bool,
    select_panel_candidates,
)
from dfm_pipeline.preprocessing.tcode import ALLOWED_TCODES


REQUIRED_REGISTRY_COLUMNS = {
    "raw_series",
    "canonical_id",
}

REQUIRED_STAGE1_COLUMNS = {
    "raw_series",
    "selection_class",
    "tcode",
    "stable_window_start",
    "stable_window_end",
    "stable_window_expected_vintages",
    "coverage_min_n_valid",
    "coverage_min_observed_fraction",
    "qc_hard_fail",
    "qc_status",
    "qc_flags",
}

REQUIRED_STAGE2_COLUMNS = {
    "raw_series",
    "selection_class",
    "registry_tcode",
    "embedded_tcodes_seen",
    "n_vintages_checked",
    "n_vintages_hard_fail",
    "min_transformed_n_valid",
    "min_transformed_observed_fraction",
    "min_transformed_std",
    "stage2_hard_fail",
    "stage2_status",
    "stage2_flags",
}

ALLOWED_STAGE2_PASS_STATUSES = {
    "pass",
    "pass_with_flags",
}


def _require_columns(
    df: pd.DataFrame,
    required: set[str],
    *,
    label: str,
) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(
            f"{label} is missing required columns: "
            + ", ".join(missing)
        )


def _normalize_series_key(
    df: pd.DataFrame,
    *,
    label: str,
) -> pd.DataFrame:
    out = df.copy()
    out["raw_series"] = out["raw_series"].astype(str).str.strip()

    if out["raw_series"].eq("").any():
        raise ValueError(
            f"{label} contains an empty raw_series"
        )

    duplicated = out["raw_series"].duplicated(keep=False)

    if duplicated.any():
        names = sorted(
            out.loc[duplicated, "raw_series"].unique()
        )
        raise ValueError(
            f"{label} contains duplicate series: "
            + ", ".join(names)
        )

    return out


def _parse_bool_column(
    df: pd.DataFrame,
    field: str,
) -> pd.Series:
    values = []

    for _, row in df.iterrows():
        values.append(
            parse_registry_bool(
                row[field],
                field=field,
                raw_series=str(row["raw_series"]),
            )
        )

    return pd.Series(
        values,
        index=df.index,
        dtype=bool,
    )


def _as_int(
    value: object,
    *,
    field: str,
    raw_series: str,
) -> int:
    value_num = pd.to_numeric(
        pd.Series([value]),
        errors="coerce",
    ).iloc[0]

    if pd.isna(value_num):
        raise ValueError(
            f"{raw_series}: invalid {field}={value!r}"
        )

    value_float = float(value_num)

    if not np.isfinite(value_float):
        raise ValueError(
            f"{raw_series}: non-finite {field}"
        )

    value_int = int(value_float)

    if value_float != value_int:
        raise ValueError(
            f"{raw_series}: non-integral {field}={value!r}"
        )

    return value_int


def _embedded_tcode_set(
    value: object,
    *,
    raw_series: str,
) -> set[int]:
    text = str(value).strip()

    if not text:
        raise ValueError(
            f"{raw_series}: embedded_tcodes_seen is empty"
        )

    values = {
        _as_int(
            part.strip(),
            field="embedded_tcodes_seen",
            raw_series=raw_series,
        )
        for part in text.split("|")
        if part.strip()
    }

    if not values:
        raise ValueError(
            f"{raw_series}: no embedded t-code recorded"
        )

    return values


def _require_exact_series_set(
    expected: set[str],
    observed: set[str],
    *,
    label: str,
) -> None:
    missing = sorted(expected - observed)
    extra = sorted(observed - expected)

    if not missing and not extra:
        return

    details = []

    if missing:
        details.append(
            "missing: " + ", ".join(missing)
        )

    if extra:
        details.append(
            "extra: " + ", ".join(extra)
        )

    raise ValueError(
        f"{label} does not match the RT_STABLE "
        f"candidate universe ({'; '.join(details)})"
    )


def build_rt_stable_specification(
    registry: pd.DataFrame,
    stage1_report: pd.DataFrame,
    stage2_report: pd.DataFrame,
) -> pd.DataFrame:
    """
    Freeze the final fixed raw-series RT_STABLE panel.

    This function performs no new variable selection and uses no
    forecasting results. Membership comes from the curated registry,
    subject only to the validated Stage-1 and Stage-2 hard-failure gates.

    Missingness flags and Stage-2 diagnostic flags do not automatically
    exclude a series.
    """
    _require_columns(
        registry,
        REQUIRED_REGISTRY_COLUMNS,
        label="Registry",
    )

    _require_columns(
        stage1_report,
        REQUIRED_STAGE1_COLUMNS,
        label="Stage-1 report",
    )

    _require_columns(
        stage2_report,
        REQUIRED_STAGE2_COLUMNS,
        label="Stage-2 report",
    )

    selected = select_panel_candidates(
        registry,
        scope="rt-stable-candidates",
    ).copy()

    selected = _normalize_series_key(
        selected,
        label="Selected registry",
    )

    stage1 = _normalize_series_key(
        stage1_report,
        label="Stage-1 report",
    )

    stage2 = _normalize_series_key(
        stage2_report,
        label="Stage-2 report",
    )

    expected = set(selected["raw_series"])

    _require_exact_series_set(
        expected,
        set(stage1["raw_series"]),
        label="Stage-1 report",
    )

    _require_exact_series_set(
        expected,
        set(stage2["raw_series"]),
        label="Stage-2 report",
    )

    # select_panel_candidates() already gives deterministic ordering.
    selected = selected.set_index(
        "raw_series",
        drop=False,
    )

    stage1 = (
        stage1
        .set_index("raw_series", drop=False)
        .loc[selected.index]
    )

    stage2 = (
        stage2
        .set_index("raw_series", drop=False)
        .loc[selected.index]
    )

    # --------------------------------------------------------------
    # Selection-class consistency
    # --------------------------------------------------------------

    for label, report in (
        ("Stage-1", stage1),
        ("Stage-2", stage2),
    ):
        mismatch = (
            report["selection_class"].astype(str)
            != selected["selection_class"].astype(str)
        )

        if mismatch.any():
            names = selected.index[mismatch].tolist()

            raise ValueError(
                f"{label} selection_class mismatch for: "
                + ", ".join(names)
            )

    allowed_classes = {
        "ordinary_stable",
        "reviewed_stable_keep",
    }

    bad_class = ~selected[
        "selection_class"
    ].isin(allowed_classes)

    if bad_class.any():
        names = selected.index[bad_class].tolist()

        raise ValueError(
            "Unexpected RT_STABLE selection class for: "
            + ", ".join(names)
        )

    # --------------------------------------------------------------
    # Stage-1 hard gate
    # --------------------------------------------------------------

    stage1_hard = _parse_bool_column(
        stage1,
        "qc_hard_fail",
    )

    if stage1_hard.any():
        names = stage1.index[
            stage1_hard
        ].tolist()

        raise ValueError(
            "Cannot freeze RT_STABLE with Stage-1 "
            "hard failures: "
            + ", ".join(names)
        )

    # --------------------------------------------------------------
    # Stage-2 hard gate
    # --------------------------------------------------------------

    stage2_hard = _parse_bool_column(
        stage2,
        "stage2_hard_fail",
    )

    if stage2_hard.any():
        names = stage2.index[
            stage2_hard
        ].tolist()

        raise ValueError(
            "Cannot freeze RT_STABLE with Stage-2 "
            "hard failures: "
            + ", ".join(names)
        )

    stage2_status = (
        stage2["stage2_status"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    bad_status = ~stage2_status.isin(
        ALLOWED_STAGE2_PASS_STATUSES
    )

    if bad_status.any():
        names = stage2.index[
            bad_status
        ].tolist()

        raise ValueError(
            "Non-passing Stage-2 status for: "
            + ", ".join(names)
        )

    n_vintage_hard = pd.to_numeric(
        stage2["n_vintages_hard_fail"],
        errors="coerce",
    )

    invalid_vintage_hard = (
        n_vintage_hard.isna()
        | n_vintage_hard.ne(0)
    )

    if invalid_vintage_hard.any():
        names = stage2.index[
            invalid_vintage_hard
        ].tolist()

        raise ValueError(
            "Stage-2 vintage hard failures remain for: "
            + ", ".join(names)
        )

    # --------------------------------------------------------------
    # T-code consistency
    # --------------------------------------------------------------

    tcodes = []

    for series in selected.index:
        registry_tcode = _as_int(
            selected.at[series, "tcode"],
            field="registry tcode",
            raw_series=series,
        )

        stage1_tcode = _as_int(
            stage1.at[series, "tcode"],
            field="Stage-1 tcode",
            raw_series=series,
        )

        stage2_tcode = _as_int(
            stage2.at[series, "registry_tcode"],
            field="Stage-2 registry_tcode",
            raw_series=series,
        )

        embedded = _embedded_tcode_set(
            stage2.at[
                series,
                "embedded_tcodes_seen",
            ],
            raw_series=series,
        )

        if registry_tcode not in ALLOWED_TCODES:
            raise ValueError(
                f"{series}: invalid registry "
                f"tcode={registry_tcode}"
            )

        if stage1_tcode != registry_tcode:
            raise ValueError(
                f"{series}: Stage-1 tcode "
                f"{stage1_tcode} != registry "
                f"tcode {registry_tcode}"
            )

        if stage2_tcode != registry_tcode:
            raise ValueError(
                f"{series}: Stage-2 registry tcode "
                f"{stage2_tcode} != registry "
                f"tcode {registry_tcode}"
            )

        if embedded != {registry_tcode}:
            raise ValueError(
                f"{series}: embedded t-codes "
                f"{sorted(embedded)} != registry "
                f"tcode {registry_tcode}"
            )

        tcodes.append(registry_tcode)

    # --------------------------------------------------------------
    # Stable-window consistency
    # --------------------------------------------------------------

    starts = set(
        stage1[
            "stable_window_start"
        ].astype(str).str.strip()
    )

    ends = set(
        stage1[
            "stable_window_end"
        ].astype(str).str.strip()
    )

    if len(starts) != 1 or len(ends) != 1:
        raise ValueError(
            "Stage-1 report does not describe one "
            "common stable window"
        )

    start = next(iter(starts))
    end = next(iter(ends))

    expected_vintages = pd.to_numeric(
        stage1[
            "stable_window_expected_vintages"
        ],
        errors="coerce",
    )

    if (
        expected_vintages.isna().any()
        or expected_vintages.nunique() != 1
    ):
        raise ValueError(
            "Stage-1 expected vintage count is inconsistent"
        )

    expected_n = int(
        expected_vintages.iloc[0]
    )

    checked = pd.to_numeric(
        stage2["n_vintages_checked"],
        errors="coerce",
    )

    bad_checked = (
        checked.isna()
        | checked.ne(expected_n)
    )

    if bad_checked.any():
        names = stage2.index[
            bad_checked
        ].tolist()

        raise ValueError(
            "Stage-2 vintage count does not match "
            "Stage-1 stable window for: "
            + ", ".join(names)
        )

    # --------------------------------------------------------------
    # Passing reports must contain sensible provenance metrics.
    # These are consistency checks, not new exclusion thresholds.
    # --------------------------------------------------------------

    stage1_n = pd.to_numeric(
        stage1["coverage_min_n_valid"],
        errors="coerce",
    )

    stage1_fraction = pd.to_numeric(
        stage1[
            "coverage_min_observed_fraction"
        ],
        errors="coerce",
    )

    stage2_n = pd.to_numeric(
        stage2["min_transformed_n_valid"],
        errors="coerce",
    )

    stage2_fraction = pd.to_numeric(
        stage2[
            "min_transformed_observed_fraction"
        ],
        errors="coerce",
    )

    stage2_std = pd.to_numeric(
        stage2["min_transformed_std"],
        errors="coerce",
    )

    if (
        stage1_n.isna().any()
        or stage1_n.le(0).any()
    ):
        raise ValueError(
            "Invalid Stage-1 minimum observation count"
        )

    if (
        stage1_fraction.isna().any()
        or stage1_fraction.le(0).any()
        or stage1_fraction.gt(1).any()
    ):
        raise ValueError(
            "Invalid Stage-1 observed fraction"
        )

    if (
        stage2_n.isna().any()
        or stage2_n.le(0).any()
    ):
        raise ValueError(
            "Invalid Stage-2 transformed observation count"
        )

    if (
        stage2_fraction.isna().any()
        or stage2_fraction.le(0).any()
        or stage2_fraction.gt(1).any()
    ):
        raise ValueError(
            "Invalid Stage-2 transformed observed fraction"
        )

    if (
        stage2_std.isna().any()
        or ~np.isfinite(
            stage2_std.to_numpy(dtype=float)
        ).all()
        or stage2_std.le(0).any()
    ):
        raise ValueError(
            "Invalid Stage-2 transformed standard deviation"
        )

    # --------------------------------------------------------------
    # Authoritative specification
    # --------------------------------------------------------------

    out = pd.DataFrame(
        {
            "panel_position": range(
                1,
                len(selected) + 1,
            ),
            "raw_series":
                selected["raw_series"].to_numpy(),
            "canonical_id":
                selected["canonical_id"]
                .astype(str)
                .str.strip()
                .to_numpy(),
            "tcode": tcodes,
            "selection_class":
                selected["selection_class"]
                .astype(str)
                .to_numpy(),
            "review_status":
                selected["review_status"]
                .astype(str)
                .to_numpy(),
            "stable_window_start": start,
            "stable_window_end": end,
            "stage1_hard_fail":
                stage1_hard.to_numpy(),
            "stage1_status":
                stage1["qc_status"]
                .astype(str)
                .to_numpy(),
            "stage1_qc_flags":
                stage1["qc_flags"]
                .astype(str)
                .to_numpy(),
            "stage1_min_n_valid":
                stage1_n.to_numpy(),
            "stage1_min_observed_fraction":
                stage1_fraction.to_numpy(),
            "stage2_hard_fail":
                stage2_hard.to_numpy(),
            "stage2_status":
                stage2_status.to_numpy(),
            "stage2_flags":
                stage2["stage2_flags"]
                .astype(str)
                .to_numpy(),
            "stage2_n_vintages_checked":
                checked.astype(int).to_numpy(),
            "stage2_n_vintages_hard_fail":
                n_vintage_hard
                .astype(int)
                .to_numpy(),
            "stage2_min_transformed_n_valid":
                stage2_n.to_numpy(),
            "stage2_min_transformed_observed_fraction":
                stage2_fraction.to_numpy(),
            "stage2_min_transformed_std":
                stage2_std.to_numpy(),
            "final_include": True,
        }
    )

    if out["canonical_id"].eq("").any():
        names = out.loc[
            out["canonical_id"].eq(""),
            "raw_series",
        ].tolist()

        raise ValueError(
            "Missing canonical_id for: "
            + ", ".join(names)
        )

    out["inclusion_reason"] = np.where(
        out["selection_class"].eq(
            "reviewed_stable_keep"
        ),
        (
            "reviewed RT_STABLE keep; passed Stage-1 "
            "structural QC; passed Stage-2 transformed QC"
        ),
        (
            "ordinary mechanically stable candidate; "
            "passed Stage-1 structural QC; "
            "passed Stage-2 transformed QC"
        ),
    )

    return out.reset_index(drop=True)


def build_rt_stable_summary(
    specification: pd.DataFrame,
) -> pd.DataFrame:
    """Build a compact summary of the frozen panel."""

    return pd.DataFrame(
        [
            {
                "n_series": int(
                    len(specification)
                ),
                "n_ordinary_stable": int(
                    specification[
                        "selection_class"
                    ]
                    .eq("ordinary_stable")
                    .sum()
                ),
                "n_reviewed_stable_keep": int(
                    specification[
                        "selection_class"
                    ]
                    .eq("reviewed_stable_keep")
                    .sum()
                ),
                "stable_window_start":
                    specification[
                        "stable_window_start"
                    ].iloc[0],
                "stable_window_end":
                    specification[
                        "stable_window_end"
                    ].iloc[0],
                "n_stage1_hard_fail": int(
                    specification[
                        "stage1_hard_fail"
                    ].sum()
                ),
                "n_stage2_hard_fail": int(
                    specification[
                        "stage2_hard_fail"
                    ].sum()
                ),
                "n_stage2_pass": int(
                    specification[
                        "stage2_status"
                    ].eq("pass").sum()
                ),
                "n_stage2_pass_with_flags": int(
                    specification[
                        "stage2_status"
                    ]
                    .eq("pass_with_flags")
                    .sum()
                ),
                "min_stage1_n_valid": int(
                    specification[
                        "stage1_min_n_valid"
                    ].min()
                ),
                "min_stage2_transformed_n_valid": int(
                    specification[
                        "stage2_min_transformed_n_valid"
                    ].min()
                ),
                "min_stage2_transformed_std": float(
                    specification[
                        "stage2_min_transformed_std"
                    ].min()
                ),
            }
        ]
    )


__all__ = [
    "build_rt_stable_specification",
    "build_rt_stable_summary",
]