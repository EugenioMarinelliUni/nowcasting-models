from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


RELATIONSHIP_TYPES = (
    "unreviewed",
    "unchanged",
    "exact_rename",
    "official_successor",
    "related_non_equivalent",
    "new_unmatched",
    "retired_unmatched",
    "excluded",
)

MAPPING_ACTIONS = (
    "unreviewed",
    "direct",
    "switch_by_vintage",
    "keep_separate",
    "exclude",
)


def _require_columns(
    df: pd.DataFrame,
    required: Iterable[str],
    *,
    name: str,
) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(
            f"{name} is missing required columns: {', '.join(missing)}"
        )


def _window_columns(
    presence: pd.DataFrame,
    *,
    start: str,
    end: str,
) -> list[str]:
    columns = [str(c) for c in presence.columns]
    selected = [c for c in columns if start <= c <= end]

    if not selected:
        raise ValueError(
            f"No vintage columns found between {start} and {end}."
        )

    if selected[0] != start or selected[-1] != end:
        raise ValueError(
            "Requested stable-window endpoints are not both present in "
            f"series_presence.csv. Found {selected[0]} through {selected[-1]}."
        )

    expected = pd.period_range(start, end, freq="M").astype(str).tolist()
    if selected != expected:
        missing = sorted(set(expected) - set(selected))
        unexpected = sorted(set(selected) - set(expected))
        raise ValueError(
            "The selected vintage window is not a complete monthly sequence. "
            f"Missing={missing}; unexpected={unexpected}"
        )

    return selected


def build_coverage_summary(
    series_audit: pd.DataFrame,
    *,
    vintage_start: str,
    vintage_end: str,
) -> pd.DataFrame:
    """
    Aggregate historical-coverage diagnostics for each raw series.

    The filter is on *vintage month*, not reference date.  The resulting
    diagnostics therefore answer questions such as:

      - Was the series available in every chosen vintage?
      - In the worst vintage, how much usable history did it have?
      - How severe were leading, trailing, and internal gaps?

    No model-inclusion threshold is imposed here.
    """
    required = [
        "vintage",
        "series",
        "n_rows",
        "n_valid",
        "pct_missing",
        "n_leading_missing",
        "n_trailing_missing",
        "n_internal_missing",
        "all_missing",
        "first_valid_date",
        "last_valid_date",
    ]
    _require_columns(series_audit, required, name="series_audit")

    x = series_audit.copy()
    x["vintage"] = x["vintage"].astype(str)
    x = x[
        (x["vintage"] >= vintage_start)
        & (x["vintage"] <= vintage_end)
    ].copy()

    if x.empty:
        raise ValueError(
            f"series_audit has no rows between {vintage_start} and {vintage_end}."
        )

    x["first_valid_date"] = pd.to_datetime(
        x["first_valid_date"],
        errors="coerce",
    )
    x["last_valid_date"] = pd.to_datetime(
        x["last_valid_date"],
        errors="coerce",
    )

    for col in [
        "n_rows",
        "n_valid",
        "pct_missing",
        "n_leading_missing",
        "n_trailing_missing",
        "n_internal_missing",
    ]:
        x[col] = pd.to_numeric(x[col], errors="coerce")

    x["observed_fraction"] = np.where(
        x["n_rows"] > 0,
        x["n_valid"] / x["n_rows"],
        np.nan,
    )

    expected_n_vintages = len(
        pd.period_range(vintage_start, vintage_end, freq="M")
    )

    rows: list[dict[str, object]] = []
    for series, g in x.groupby("series", sort=True):
        rows.append(
            {
                "raw_series": series,
                "coverage_window_start": vintage_start,
                "coverage_window_end": vintage_end,
                "coverage_expected_vintages": expected_n_vintages,
                "coverage_vintages_present": int(g["vintage"].nunique()),
                "coverage_min_n_valid": int(g["n_valid"].min()),
                "coverage_median_n_valid": float(g["n_valid"].median()),
                "coverage_min_observed_fraction": float(
                    g["observed_fraction"].min()
                ),
                "coverage_max_pct_missing": float(g["pct_missing"].max()),
                "coverage_max_leading_missing": int(
                    g["n_leading_missing"].max()
                ),
                "coverage_max_trailing_missing": int(
                    g["n_trailing_missing"].max()
                ),
                "coverage_max_internal_missing": int(
                    g["n_internal_missing"].max()
                ),
                "coverage_vintages_with_internal_missing": int(
                    (g["n_internal_missing"] > 0).sum()
                ),
                "coverage_all_missing_vintages": int(
                    g["all_missing"].fillna(False).astype(bool).sum()
                ),
                "coverage_earliest_first_valid_date": (
                    g["first_valid_date"].min().date().isoformat()
                    if g["first_valid_date"].notna().any()
                    else ""
                ),
                "coverage_latest_first_valid_date": (
                    g["first_valid_date"].max().date().isoformat()
                    if g["first_valid_date"].notna().any()
                    else ""
                ),
                "coverage_earliest_last_valid_date": (
                    g["last_valid_date"].min().date().isoformat()
                    if g["last_valid_date"].notna().any()
                    else ""
                ),
                "coverage_latest_last_valid_date": (
                    g["last_valid_date"].max().date().isoformat()
                    if g["last_valid_date"].notna().any()
                    else ""
                ),
            }
        )

    return pd.DataFrame(rows).sort_values("raw_series").reset_index(drop=True)


def build_registry_seed(
    *,
    series_tcode_history: pd.DataFrame,
    series_presence: pd.DataFrame,
    series_changes: pd.DataFrame,
    series_audit: pd.DataFrame,
    stable_start: str,
    stable_end: str,
) -> pd.DataFrame:
    """
    Build the initial reviewable FRED-MD series registry.

    The seed intentionally does not infer semantic equivalence between
    predecessor/successor mnemonics.  It records empirical facts from the
    audit and creates explicit fields for later documentary review.
    """
    _require_columns(
        series_tcode_history,
        [
            "series",
            "first_vintage",
            "last_vintage",
            "n_vintages_present",
            "tcodes_seen",
            "n_distinct_tcodes",
            "n_tcode_changes",
        ],
        name="series_tcode_history",
    )
    _require_columns(
        series_changes,
        ["previous_vintage", "vintage", "change", "series"],
        name="series_changes",
    )

    presence = series_presence.copy()
    presence.index = presence.index.astype(str)
    presence.columns = presence.columns.astype(str)

    window_cols = _window_columns(
        presence,
        start=stable_start,
        end=stable_end,
    )
    window_presence = presence[window_cols].astype(int)

    n_window_present = window_presence.sum(axis=1)
    present_all = window_presence.eq(1).all(axis=1)

    changes = series_changes.copy()
    changes["series"] = changes["series"].astype(str)
    changes["vintage"] = changes["vintage"].astype(str)

    change_counts = (
        changes.groupby(["series", "change"])
        .size()
        .unstack(fill_value=0)
        if not changes.empty
        else pd.DataFrame()
    )

    change_bounds = (
        changes.groupby("series")
        .agg(
            first_observed_change_vintage=("vintage", "min"),
            last_observed_change_vintage=("vintage", "max"),
        )
        if not changes.empty
        else pd.DataFrame()
    )

    coverage = build_coverage_summary(
        series_audit,
        vintage_start=stable_start,
        vintage_end=stable_end,
    ).set_index("raw_series")

    reg = series_tcode_history.rename(
        columns={"series": "raw_series"}
    ).copy()

    reg["canonical_id"] = reg["raw_series"]
    reg["stable_window_start"] = stable_start
    reg["stable_window_end"] = stable_end
    reg["stable_window_expected_vintages"] = len(window_cols)
    reg["stable_window_vintages_present"] = (
        reg["raw_series"].map(n_window_present).fillna(0).astype(int)
    )
    reg["present_all_stable_window"] = (
        reg["raw_series"].map(present_all).fillna(False).astype(bool)
    )

    if not change_counts.empty:
        added = change_counts["added"] if "added" in change_counts else pd.Series(dtype=int)
        removed = change_counts["removed"] if "removed" in change_counts else pd.Series(dtype=int)
        reg["n_added_events"] = (
            reg["raw_series"].map(added).fillna(0).astype(int)
        )
        reg["n_removed_events"] = (
            reg["raw_series"].map(removed).fillna(0).astype(int)
        )
    else:
        reg["n_added_events"] = 0
        reg["n_removed_events"] = 0

    if not change_bounds.empty:
        reg["first_observed_change_vintage"] = (
            reg["raw_series"]
            .map(change_bounds["first_observed_change_vintage"])
            .fillna("")
        )
        reg["last_observed_change_vintage"] = (
            reg["raw_series"]
            .map(change_bounds["last_observed_change_vintage"])
            .fillna("")
        )
    else:
        reg["first_observed_change_vintage"] = ""
        reg["last_observed_change_vintage"] = ""

    reg = reg.merge(
        coverage.reset_index(),
        on="raw_series",
        how="left",
        validate="one_to_one",
    )

    # Mechanical flags only.
    reg["candidate_rt_stable"] = reg["present_all_stable_window"]
    reg["needs_stable_window_transition_review"] = (
        ~reg["present_all_stable_window"]
    )

    # Human/research-decision fields: deliberately not inferred.
    reg["include_rt_stable"] = False
    reg["relationship_type"] = "unreviewed"
    reg["equivalence_level"] = ""
    reg["predecessor"] = ""
    reg["successor"] = ""
    reg["mapping_action"] = "unreviewed"
    reg["active_from_vintage"] = reg["first_vintage"]
    reg["active_to_vintage"] = reg["last_vintage"]
    reg["include_rt_canonical"] = False

    # Documentary-review fields.
    reg["documented_change_vintage"] = ""
    reg["documented_change_type"] = ""
    reg["fred_md_change_log_reference"] = ""
    reg["fred_series_url"] = ""
    reg["source_agency_reference"] = ""
    reg["review_status"] = "unreviewed"
    reg["notes"] = ""

    columns = [
        "raw_series",
        "canonical_id",
        "first_vintage",
        "last_vintage",
        "n_vintages_present",
        "tcodes_seen",
        "n_distinct_tcodes",
        "n_tcode_changes",
        "stable_window_start",
        "stable_window_end",
        "stable_window_expected_vintages",
        "stable_window_vintages_present",
        "present_all_stable_window",
        "candidate_rt_stable",
        "include_rt_stable",
        "needs_stable_window_transition_review",
        "n_added_events",
        "n_removed_events",
        "first_observed_change_vintage",
        "last_observed_change_vintage",
        "coverage_window_start",
        "coverage_window_end",
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
        "relationship_type",
        "equivalence_level",
        "predecessor",
        "successor",
        "mapping_action",
        "active_from_vintage",
        "active_to_vintage",
        "include_rt_canonical",
        "documented_change_vintage",
        "documented_change_type",
        "fred_md_change_log_reference",
        "fred_series_url",
        "source_agency_reference",
        "review_status",
        "notes",
    ]

    return reg[columns].sort_values("raw_series").reset_index(drop=True)


def build_registry_seed_from_audit_dir(
    audit_dir: str | Path,
    *,
    stable_start: str,
    stable_end: str,
) -> pd.DataFrame:
    audit_dir = Path(audit_dir)

    history = pd.read_csv(audit_dir / "series_tcode_history.csv")
    presence = pd.read_csv(
        audit_dir / "series_presence.csv",
        index_col=0,
    )
    changes = pd.read_csv(audit_dir / "series_changes.csv")
    audit = pd.read_csv(audit_dir / "series_audit.csv")

    return build_registry_seed(
        series_tcode_history=history,
        series_presence=presence,
        series_changes=changes,
        series_audit=audit,
        stable_start=stable_start,
        stable_end=stable_end,
    )


def build_registry_summary(registry: pd.DataFrame) -> pd.DataFrame:
    """One-row summary suitable for a small generated QC artifact."""
    _require_columns(
        registry,
        [
            "raw_series",
            "candidate_rt_stable",
            "needs_stable_window_transition_review",
            "n_tcode_changes",
            "coverage_vintages_with_internal_missing",
            "coverage_all_missing_vintages",
        ],
        name="registry",
    )

    return pd.DataFrame(
        [
            {
                "n_raw_series": int(len(registry)),
                "n_rt_stable_candidates": int(
                    registry["candidate_rt_stable"].sum()
                ),
                "n_transition_review": int(
                    registry["needs_stable_window_transition_review"].sum()
                ),
                "n_series_with_tcode_changes": int(
                    (registry["n_tcode_changes"] > 0).sum()
                ),
                "n_series_with_internal_missing_in_window": int(
                    (
                        registry[
                            "coverage_vintages_with_internal_missing"
                        ] > 0
                    ).sum()
                ),
                "n_series_with_all_missing_vintage_in_window": int(
                    (registry["coverage_all_missing_vintages"] > 0).sum()
                ),
            }
        ]
    )


__all__ = [
    "RELATIONSHIP_TYPES",
    "MAPPING_ACTIONS",
    "build_coverage_summary",
    "build_registry_seed",
    "build_registry_seed_from_audit_dir",
    "build_registry_summary",
]
