from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dfm_pipeline.ingestion.fred_md import detect_tcode_row, read_embedded_tcode_map
from dfm_pipeline.ingestion.fred_md_panel_selection import (
    parse_registry_bool,
    single_stable_tcode,
)
from dfm_pipeline.ingestion.fred_md_transformed_qc import (
    audit_transformed_series,
    discover_vintage_files,
)
from dfm_pipeline.ingestion.fred_md_vintages import load_numeric_fred_md_vintage
from dfm_pipeline.preprocessing.tcode import ALLOWED_TCODES, apply_tcode_transformations

REGISTRY_REQUIRED = {
    "raw_series", "canonical_id", "mapping_action", "include_rt_canonical",
    "review_status", "relationship_type", "equivalence_level",
    "active_from_vintage", "active_to_vintage", "tcodes_seen",
    "n_distinct_tcodes", "n_tcode_changes", "documented_change_vintage", "notes",
}
RT_STABLE_REQUIRED = {
    "panel_position", "raw_series", "canonical_id", "tcode",
    "stable_window_start", "stable_window_end", "final_include",
}


def _require(df: pd.DataFrame, cols: set[str], label: str) -> None:
    missing = sorted(cols - set(df.columns))
    if missing:
        raise ValueError(f"{label} missing columns: {', '.join(missing)}")


def _p(value: object, field: str, label: str) -> pd.Period:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{label}: missing {field}")
    try:
        return pd.Period(text, freq="M")
    except Exception as exc:
        raise ValueError(f"{label}: invalid {field}={value!r}") from exc


def _bool(value: object, field: str, series: str) -> bool:
    return parse_registry_bool(value, field=field, raw_series=series)


def _stable_window(rt_stable: pd.DataFrame) -> tuple[pd.Period, pd.Period]:
    starts = {str(x).strip() for x in rt_stable["stable_window_start"] if str(x).strip()}
    ends = {str(x).strip() for x in rt_stable["stable_window_end"] if str(x).strip()}
    if len(starts) != 1 or len(ends) != 1:
        raise ValueError("RT_STABLE must contain one common stable window")
    start = _p(next(iter(starts)), "stable_window_start", "RT_STABLE")
    end = _p(next(iter(ends)), "stable_window_end", "RT_STABLE")
    if end < start:
        raise ValueError("RT_STABLE stable window is reversed")
    return start, end


def _validate_rt_stable(rt_stable: pd.DataFrame) -> pd.DataFrame:
    _require(rt_stable, RT_STABLE_REQUIRED, "RT_STABLE")
    x = rt_stable.copy()
    x["raw_series"] = x["raw_series"].astype(str).str.strip()
    x["canonical_id"] = x["canonical_id"].astype(str).str.strip()
    if x["raw_series"].eq("").any() or x["canonical_id"].eq("").any():
        raise ValueError("RT_STABLE contains empty raw_series/canonical_id")
    if x["raw_series"].duplicated().any() or x["canonical_id"].duplicated().any():
        raise ValueError("RT_STABLE raw_series and canonical_id must both be unique")
    for _, row in x.iterrows():
        if not _bool(row["final_include"], "final_include", str(row["raw_series"])):
            raise ValueError(f"{row['raw_series']}: RT_STABLE final_include must be True")
    pos = pd.to_numeric(x["panel_position"], errors="coerce")
    if pos.isna().any() or pos.astype(int).tolist() != list(range(1, len(x) + 1)):
        raise ValueError("RT_STABLE panel_position must be exactly 1..N")
    _stable_window(x)
    return x.sort_values("panel_position").reset_index(drop=True)


def validate_rt_canonical_specification(panel: pd.DataFrame, source_map: pd.DataFrame) -> None:
    panel_cols = {
        "panel_position", "canonical_id", "mapping_type", "n_source_segments",
        "stable_window_start", "stable_window_end", "final_include", "inclusion_reason",
    }
    source_cols = {
        "canonical_id", "source_order", "source_series", "expected_tcode",
        "active_from_vintage", "active_to_vintage", "mapping_action",
        "relationship_type", "equivalence_level", "documented_change_vintage",
        "source_origin", "notes",
    }
    _require(panel, panel_cols, "RT_CANONICAL panel")
    _require(source_map, source_cols, "RT_CANONICAL source map")

    p = panel.copy()
    p["canonical_id"] = p["canonical_id"].astype(str).str.strip()
    if p["canonical_id"].eq("").any() or p["canonical_id"].duplicated().any():
        raise ValueError("RT_CANONICAL canonical_id must be non-empty and unique")
    pos = pd.to_numeric(p["panel_position"], errors="coerce")
    if pos.isna().any() or pos.astype(int).tolist() != list(range(1, len(p) + 1)):
        raise ValueError("RT_CANONICAL panel_position must be exactly 1..N")
    if not p["mapping_type"].isin({"direct", "switch_by_vintage"}).all():
        raise ValueError("Invalid RT_CANONICAL mapping_type")

    sm = source_map.copy()
    sm["canonical_id"] = sm["canonical_id"].astype(str).str.strip()
    if set(sm["canonical_id"]) != set(p["canonical_id"]):
        raise ValueError("RT_CANONICAL panel/source-map concept sets differ")

    for _, prow in p.iterrows():
        cid = str(prow["canonical_id"])
        g = sm.loc[sm["canonical_id"].eq(cid)].copy().sort_values("source_order")
        nseg = int(prow["n_source_segments"])
        if len(g) != nseg:
            raise ValueError(f"{cid}: n_source_segments mismatch")
        orders = pd.to_numeric(g["source_order"], errors="coerce")
        if orders.isna().any() or orders.astype(int).tolist() != list(range(1, len(g) + 1)):
            raise ValueError(f"{cid}: source_order must be exactly 1..K")
        if prow["mapping_type"] == "direct" and len(g) != 1:
            raise ValueError(f"{cid}: direct mapping requires exactly one source")
        if prow["mapping_type"] == "switch_by_vintage" and len(g) < 2:
            raise ValueError(f"{cid}: switch_by_vintage requires >=2 sources")

        w0 = _p(prow["stable_window_start"], "stable_window_start", cid)
        w1 = _p(prow["stable_window_end"], "stable_window_end", cid)
        segs: list[tuple[pd.Period, pd.Period]] = []
        for _, srow in g.iterrows():
            code = int(srow["expected_tcode"])
            if code not in ALLOWED_TCODES:
                raise ValueError(f"{cid}/{srow['source_series']}: invalid expected_tcode={code}")
            a = _p(srow["active_from_vintage"], "active_from_vintage", cid)
            b = _p(srow["active_to_vintage"], "active_to_vintage", cid)
            if b < a:
                raise ValueError(f"{cid}: reversed source interval")
            segs.append((a, b))
        if segs[0][0] != w0 or segs[-1][1] != w1:
            raise ValueError(f"{cid}: source intervals do not cover the full RT window")
        for (_, prev_end), (next_start, _) in zip(segs, segs[1:]):
            if next_start != prev_end + 1:
                kind = "overlap" if next_start <= prev_end else "gap"
                raise ValueError(f"{cid}: source intervals have {kind}: {prev_end} -> {next_start}")


def build_rt_canonical_specification(
    registry: pd.DataFrame,
    rt_stable: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build candidate RT_CANONICAL concept and source-map specifications."""
    _require(registry, REGISTRY_REQUIRED, "Registry")
    stable = _validate_rt_stable(rt_stable)
    reg = registry.copy()
    reg["raw_series"] = reg["raw_series"].astype(str).str.strip()
    reg["canonical_id"] = reg["canonical_id"].astype(str).str.strip()
    if reg["raw_series"].duplicated().any():
        raise ValueError("Registry raw_series must be unique")
    reg_idx = reg.set_index("raw_series", drop=False)
    missing = sorted(set(stable["raw_series"]) - set(reg_idx.index))
    if missing:
        raise ValueError("RT_STABLE series absent from registry: " + ", ".join(missing))

    w0, w1 = _stable_window(stable)
    panel_rows: list[dict[str, Any]] = []
    source_rows: list[dict[str, Any]] = []

    # Direct concepts inherit RT_STABLE positions exactly.
    for _, srow in stable.iterrows():
        raw = str(srow["raw_series"])
        cid = str(srow["canonical_id"])
        code = int(srow["tcode"])
        r = reg_idx.loc[raw]
        panel_rows.append({
            "panel_position": int(srow["panel_position"]),
            "canonical_id": cid,
            "mapping_type": "direct",
            "n_source_segments": 1,
            "stable_window_start": str(w0),
            "stable_window_end": str(w1),
            "final_include": True,
            "inclusion_reason": "inherited from frozen RT_STABLE specification",
        })
        source_rows.append({
            "canonical_id": cid,
            "source_order": 1,
            "source_series": raw,
            "expected_tcode": code,
            "active_from_vintage": str(w0),
            "active_to_vintage": str(w1),
            "mapping_action": "direct",
            "relationship_type": str(r["relationship_type"]).strip(),
            "equivalence_level": str(r["equivalence_level"]).strip(),
            "documented_change_vintage": str(r["documented_change_vintage"]).strip(),
            "source_origin": "rt_stable",
            "notes": str(r["notes"]).strip(),
        })

    direct_ids = {x["canonical_id"] for x in panel_rows}
    reg["mapping_action_norm"] = reg["mapping_action"].astype(str).str.strip().str.lower()
    reg["review_status_norm"] = reg["review_status"].astype(str).str.strip().str.lower()
    include = pd.Series([
        _bool(row["include_rt_canonical"], "include_rt_canonical", str(row["raw_series"]))
        for _, row in reg.iterrows()
    ], index=reg.index, dtype=bool)
    switches = reg.loc[
        reg["mapping_action_norm"].eq("switch_by_vintage")
        & reg["review_status_norm"].eq("reviewed")
        & include
    ].copy()

    next_pos = len(panel_rows) + 1
    for cid, group in switches.groupby("canonical_id", sort=True):
        cid = str(cid).strip()
        if not cid:
            raise ValueError("Approved switch row has empty canonical_id")
        if cid in direct_ids:
            raise ValueError(f"{cid}: appears as both direct and switch concept")

        segments: list[dict[str, Any]] = []
        for _, row in group.iterrows():
            raw = str(row["raw_series"])
            n_changes = pd.to_numeric(pd.Series([row["n_tcode_changes"]]), errors="coerce").iloc[0]
            if pd.isna(n_changes) or int(n_changes) != 0:
                raise ValueError(f"{cid}/{raw}: t-code changes are not supported")
            code = single_stable_tcode(row["tcodes_seen"], row["n_distinct_tcodes"])
            if code is None:
                raise ValueError(f"{cid}/{raw}: no unique valid t-code")
            a0 = _p(row["active_from_vintage"], "active_from_vintage", f"{cid}/{raw}")
            b0 = _p(row["active_to_vintage"], "active_to_vintage", f"{cid}/{raw}")
            a, b = max(a0, w0), min(b0, w1)
            if b < a:
                continue
            segments.append({
                "canonical_id": cid,
                "source_series": raw,
                "expected_tcode": int(code),
                "active_from_vintage": str(a),
                "active_to_vintage": str(b),
                "mapping_action": "switch_by_vintage",
                "relationship_type": str(row["relationship_type"]).strip(),
                "equivalence_level": str(row["equivalence_level"]).strip(),
                "documented_change_vintage": str(row["documented_change_vintage"]).strip(),
                "source_origin": "registry_review",
                "notes": str(row["notes"]).strip(),
            })
        if len(segments) < 2:
            raise ValueError(f"{cid}: fewer than two active source segments in RT window")
        segments.sort(key=lambda x: (pd.Period(x["active_from_vintage"], freq="M"), x["source_series"]))
        for order, seg in enumerate(segments, start=1):
            source_rows.append({**seg, "source_order": order})
        panel_rows.append({
            "panel_position": next_pos,
            "canonical_id": cid,
            "mapping_type": "switch_by_vintage",
            "n_source_segments": len(segments),
            "stable_window_start": str(w0),
            "stable_window_end": str(w1),
            "final_include": True,
            "inclusion_reason": "reviewed canonical source switch; subject to empirical transition audit",
        })
        next_pos += 1

    panel = pd.DataFrame(panel_rows).sort_values("panel_position").reset_index(drop=True)
    source_map = pd.DataFrame(source_rows).sort_values(["canonical_id", "source_order"]).reset_index(drop=True)
    validate_rt_canonical_specification(panel, source_map)
    return panel, source_map


def build_rt_canonical_summary(panel: pd.DataFrame, source_map: pd.DataFrame) -> pd.DataFrame:
    validate_rt_canonical_specification(panel, source_map)
    return pd.DataFrame([{
        "n_canonical_concepts": int(len(panel)),
        "n_direct_concepts": int(panel["mapping_type"].eq("direct").sum()),
        "n_switch_by_vintage_concepts": int(panel["mapping_type"].eq("switch_by_vintage").sum()),
        "n_source_segments": int(len(source_map)),
        "stable_window_start": str(panel["stable_window_start"].iloc[0]),
        "stable_window_end": str(panel["stable_window_end"].iloc[0]),
    }])


def resolve_active_source(source_map: pd.DataFrame, *, canonical_id: str, vintage: str) -> pd.Series:
    period = pd.Period(vintage, freq="M")
    g = source_map.loc[source_map["canonical_id"].astype(str).eq(str(canonical_id))].copy()
    if g.empty:
        raise KeyError(f"No source-map rows for canonical_id={canonical_id!r}")
    starts = pd.PeriodIndex(g["active_from_vintage"].astype(str), freq="M")
    ends = pd.PeriodIndex(g["active_to_vintage"].astype(str), freq="M")
    active = g.loc[(starts <= period) & (period <= ends)]
    if len(active) != 1:
        raise ValueError(f"{canonical_id} at {vintage}: expected one active source, found {len(active)}")
    return active.iloc[0]


def canonicalize_transformed_vintage(
    transformed_sources: pd.DataFrame,
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    *,
    vintage: str,
) -> pd.DataFrame:
    """Select already-transformed sources; never splice raw levels."""
    validate_rt_canonical_specification(panel, source_map)
    out: dict[str, pd.Series] = {}
    for _, row in panel.sort_values("panel_position").iterrows():
        cid = str(row["canonical_id"])
        src = str(resolve_active_source(source_map, canonical_id=cid, vintage=vintage)["source_series"])
        if src not in transformed_sources.columns:
            raise KeyError(f"{cid} at {vintage}: transformed source {src!r} absent")
        out[cid] = transformed_sources[src]
    return pd.DataFrame(out, index=transformed_sources.index)


def transform_then_canonicalize_vintage(
    raw_panel: pd.DataFrame,
    embedded_tcodes: dict[str, int],
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    *,
    vintage: str,
) -> pd.DataFrame:
    """Transform each active raw source with its vintage t-code, then canonicalize."""
    active: list[str] = []
    for cid in panel.sort_values("panel_position")["canonical_id"]:
        src = str(resolve_active_source(source_map, canonical_id=str(cid), vintage=vintage)["source_series"])
        if src not in active:
            active.append(src)
    missing = [s for s in active if s not in raw_panel.columns]
    if missing:
        raise KeyError(f"Raw panel at {vintage} missing active sources: {', '.join(missing)}")
    tmap: dict[str, int] = {}
    for src in active:
        if src not in embedded_tcodes:
            raise KeyError(f"Raw panel at {vintage}: no embedded t-code for {src}")
        code = int(embedded_tcodes[src])
        if code not in ALLOWED_TCODES:
            raise ValueError(f"Raw panel at {vintage}: invalid t-code {code} for {src}")
        tmap[src] = code
    transformed = apply_tcode_transformations(raw_panel[active], tmap)
    return canonicalize_transformed_vintage(transformed, panel, source_map, vintage=vintage)


def build_transition_boundaries(panel: pd.DataFrame, source_map: pd.DataFrame) -> pd.DataFrame:
    validate_rt_canonical_specification(panel, source_map)
    rows: list[dict[str, Any]] = []
    switch_ids = panel.loc[panel["mapping_type"].eq("switch_by_vintage"), "canonical_id"].astype(str)
    for cid in switch_ids:
        g = source_map.loc[source_map["canonical_id"].astype(str).eq(cid)].sort_values("source_order").reset_index(drop=True)
        for i in range(len(g) - 1):
            old, new = g.iloc[i], g.iloc[i + 1]
            rows.append({
                "canonical_id": cid,
                "old_source": old["source_series"],
                "new_source": new["source_series"],
                "old_active_to_vintage": old["active_to_vintage"],
                "new_active_from_vintage": new["active_from_vintage"],
                "transition_vintage": new["active_from_vintage"],
                "old_expected_tcode": int(old["expected_tcode"]),
                "new_expected_tcode": int(new["expected_tcode"]),
                "relationship_type": new["relationship_type"],
                "equivalence_level": new["equivalence_level"],
            })
    return pd.DataFrame(rows)


def _loader_failures(diag: dict[str, Any]) -> list[str]:
    checks = {
        "date_parse_failures": "loader_date_parse_failure",
        "duplicate_reference_dates": "loader_duplicate_reference_dates",
        "non_month_start_dates": "loader_non_month_start_dates",
        "monthly_grid_gaps": "loader_monthly_grid_gaps",
        "bad_numeric_cells": "loader_bad_numeric_cells",
    }
    return [reason for field, reason in checks.items() if int(diag.get(field, 0) or 0) > 0]


def audit_rt_canonical_transitions(
    raw_dir: str | Path,
    panel: pd.DataFrame,
    source_map: pd.DataFrame,
    *,
    start: str,
    end: str,
    recursive: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Audit switched concepts across the real historical-vintage archive without writing model panels."""
    validate_rt_canonical_specification(panel, source_map)
    switch_ids = panel.loc[panel["mapping_type"].eq("switch_by_vintage"), "canonical_id"].astype(str).tolist()
    if not switch_ids:
        raise ValueError("No switch_by_vintage concepts to audit")
    files = discover_vintage_files(raw_dir, start=start, end=end, recursive=recursive)
    rows: list[dict[str, Any]] = []

    for vintage, path in files:
        trow = detect_tcode_row(path, date_col="sasdate", max_scan_rows=5)
        if trow is None:
            for cid in switch_ids:
                srow = resolve_active_source(source_map, canonical_id=cid, vintage=vintage)
                rows.append({
                    "vintage": vintage, "filename": path.name, "canonical_id": cid,
                    "active_source": srow["source_series"], "expected_tcode": srow["expected_tcode"],
                    "embedded_tcode": np.nan, "transformed_n_valid": 0,
                    "transformed_observed_fraction": np.nan, "transformed_std": np.nan,
                    "stage2_flags": "", "canonical_audit_hard_fail": True,
                    "hard_failure_reasons": "tcode_row_not_found",
                })
            continue

        raw, ldiag = load_numeric_fred_md_vintage(path, tcode_row=trow)
        embedded = read_embedded_tcode_map(path, date_col="sasdate", tcode_row=trow)
        base_failures = _loader_failures(ldiag)

        for cid in switch_ids:
            srow = resolve_active_source(source_map, canonical_id=cid, vintage=vintage)
            src = str(srow["source_series"])
            expected = int(srow["expected_tcode"])
            failures = list(base_failures)
            code = embedded.get(src)
            if src not in raw.columns:
                failures.append("active_source_missing_from_vintage")
            if code is None:
                failures.append("embedded_tcode_missing_or_invalid")
            elif int(code) != expected:
                failures.append("embedded_tcode_mismatch_source_map")

            n_valid = 0
            frac = np.nan
            std = np.nan
            flags = ""
            if src in raw.columns and code in ALLOWED_TCODES:
                transformed = apply_tcode_transformations(raw[[src]], {src: int(code)})
                metrics = audit_transformed_series(raw[src], transformed[src], tcode=int(code))
                n_valid = metrics["transformed_n_valid"]
                frac = metrics["transformed_observed_fraction"]
                std = metrics["transformed_std"]
                flags = str(metrics["stage2_flags"])
                failures.extend(
                    x for x in str(metrics["hard_failure_reasons"]).split("|") if x
                )

            hard_text = "|".join(dict.fromkeys(failures))
            rows.append({
                "vintage": vintage, "filename": path.name, "canonical_id": cid,
                "active_source": src, "expected_tcode": expected, "embedded_tcode": code,
                "transformed_n_valid": n_valid, "transformed_observed_fraction": frac,
                "transformed_std": std, "stage2_flags": flags,
                "canonical_audit_hard_fail": bool(hard_text), "hard_failure_reasons": hard_text,
            })

    by_vintage = pd.DataFrame(rows).sort_values(["canonical_id", "vintage"]).reset_index(drop=True)
    agg: list[dict[str, Any]] = []
    for cid, g in by_vintage.groupby("canonical_id", sort=True):
        hard = g["canonical_audit_hard_fail"].astype(bool)
        n = pd.to_numeric(g["transformed_n_valid"], errors="coerce")
        frac = pd.to_numeric(g["transformed_observed_fraction"], errors="coerce")
        std = pd.to_numeric(g["transformed_std"], errors="coerce")
        agg.append({
            "canonical_id": cid,
            "n_vintages_checked": int(len(g)),
            "active_sources_seen": "|".join(sorted(set(g["active_source"].astype(str)))),
            "n_vintages_hard_fail": int(hard.sum()),
            "min_transformed_n_valid": int(n.min()) if n.notna().any() else np.nan,
            "min_transformed_observed_fraction": float(frac.min()) if frac.notna().any() else np.nan,
            "min_transformed_std": float(std.min()) if std.notna().any() else np.nan,
            "canonical_audit_status": "hard_fail" if hard.any() else "pass",
            "hard_failure_reasons": "|".join(dict.fromkeys(
                part for text in g["hard_failure_reasons"].astype(str)
                for part in text.split("|") if part
            )),
        })
    by_concept = pd.DataFrame(agg)
    boundaries = build_transition_boundaries(panel, source_map)
    summary = pd.DataFrame([{
        "start_vintage": start,
        "end_vintage": end,
        "n_vintages_expected": int(len(pd.period_range(start, end, freq="M"))),
        "n_vintages_checked": int(by_vintage["vintage"].nunique()),
        "n_switch_concepts": int(len(by_concept)),
        "n_transition_boundaries": int(len(boundaries)),
        "n_vintage_concept_rows": int(len(by_vintage)),
        "n_vintage_concept_hard_fail": int(by_vintage["canonical_audit_hard_fail"].sum()),
        "n_concepts_hard_fail": int(by_concept["canonical_audit_status"].eq("hard_fail").sum()),
    }])
    return by_vintage, by_concept, boundaries, summary


__all__ = [
    "build_rt_canonical_specification", "build_rt_canonical_summary",
    "validate_rt_canonical_specification", "resolve_active_source",
    "canonicalize_transformed_vintage", "transform_then_canonicalize_vintage",
    "build_transition_boundaries", "audit_rt_canonical_transitions",
]
