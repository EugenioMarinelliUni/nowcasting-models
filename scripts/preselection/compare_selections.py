# FILE: scripts/preselection/compare_selections.py
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import itertools
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Iterable
import pandas as pd


OUT_DIR = Path("output")  # base for all reports we write


@dataclass
class RunMeta:
    method: str                # "sis" | "tstat" | "lars" (as stored in JSON)
    label: Optional[str]       # optional label parsed from filename suffix
    panel: str
    tag: str
    json_path: Path            # full path to metadata JSON
    selected: List[str]        # selected variable names


def _load_meta(json_path: Path) -> RunMeta:
    """
    Expect JSON shaped like:
    {
      "method": "tstat",
      "panel_id": "1960_noVIX",
      "train_tag": "train1990_2019",
      "params": { ... },
      "selected": ["VAR1", "VAR2", ...]
    }
    Filename convention (recommended):
      data/metadata/variants/{panel}__{tag}__preselect_{method}[__{label}].json
    """
    data = json.loads(json_path.read_text(encoding="utf-8"))
    try:
        method = str(data["method"])
        panel = str(data["panel_id"])
        tag = str(data["train_tag"])
        sel = data.get("selected", [])
        if not isinstance(sel, list):
            raise ValueError("'selected' must be a list")
        selected = [str(x) for x in sel]
    except KeyError as e:
        raise ValueError(f"Missing expected key {e} in {json_path}") from e

    # Best-effort label from filename suffix after '__preselect_{method}__'
    name = json_path.name
    label: Optional[str] = None
    try:
        # {panel}__{tag}__preselect_{method}__{label}.json
        toks = name.split("__")
        if len(toks) >= 4:
            lbl = toks[3]
            if lbl.endswith(".json"):
                lbl = lbl[:-5]
            label = lbl
    except Exception:
        label = None

    return RunMeta(
        method=method,
        label=label,
        panel=panel,
        tag=tag,
        json_path=json_path,
        selected=selected,
    )


def _pairwise_overlap(a: List[str], b: List[str]) -> Dict[str, float]:
    A, B = set(a), set(b)
    inter = A & B
    union = A | B
    only_a = A - B
    only_b = B - A
    return {
        "n_A": float(len(A)),
        "n_B": float(len(B)),
        "n_intersection": float(len(inter)),
        "n_union": float(len(union)),
        "jaccard": (len(inter) / len(union)) if union else 0.0,
        "precision_A_on_B": (len(inter) / len(A)) if A else 0.0,  # share of A covered by B
        "precision_B_on_A": (len(inter) / len(B)) if B else 0.0,  # share of B covered by A
        "only_A": float(len(only_a)),
        "only_B": float(len(only_b)),
    }


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _safe_token(s: str) -> str:
    """File/system-safe token."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", s)


def _run_key(r: RunMeta) -> str:
    """Key used for columns and filenames: method__label (or method if no label)."""
    base = r.method
    if r.label:
        base += f"__{r.label}"
    return _safe_token(base)


def compare_runs(runs: List[RunMeta]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      counts_df  : per-run counts table
      overlap_df : pairwise overlaps (one row per unordered pair)
    """
    counts_rows = [{
        "method": r.method,
        "label": r.label or "",
        "n_selected": len(r.selected),
        "json_path": str(r.json_path),
    } for r in runs]
    counts_df = pd.DataFrame(counts_rows, columns=["method", "label", "n_selected", "json_path"])

    overlap_rows = []
    for ri, rj in itertools.combinations(runs, 2):
        stats = _pairwise_overlap(ri.selected, rj.selected)
        overlap_rows.append({
            "method_A": ri.method, "label_A": ri.label or "",
            "method_B": rj.method, "label_B": rj.label or "",
            **stats
        })
    overlap_df = pd.DataFrame(overlap_rows, columns=[
        "method_A","label_A","method_B","label_B",
        "n_A","n_B","n_intersection","n_union",
        "jaccard","precision_A_on_B","precision_B_on_A",
        "only_A","only_B"
    ])
    return counts_df, overlap_df


def _df_to_markdown(df: pd.DataFrame) -> str:
    """Minimal dependency-free Markdown table (GitHub-flavored)."""
    if df.empty:
        return "_(no rows)_"
    cols = list(df.columns)
    rows = [[str(v) for v in row] for row in df.itertuples(index=False, name=None)]
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    body = "\n".join("| " + " | ".join(r) + " |" for r in rows)
    return "\n".join([header, sep, body])


def _write_list(path: Path, series: Iterable[str]) -> None:
    """Write a one-column CSV with header 'series'."""
    _ensure_parent(path)
    pd.DataFrame({"series": sorted(series)}).to_csv(path, index=False)


def write_outputs(out_prefix: Path, runs: List[RunMeta], counts: pd.DataFrame, overlap: pd.DataFrame) -> None:
    _ensure_parent(out_prefix)
    counts_path = out_prefix.with_suffix(".counts.csv")
    overlap_path = out_prefix.with_suffix(".overlap.csv")
    md_path = out_prefix.with_suffix(".md")

    counts.to_csv(counts_path, index=False)
    overlap.to_csv(overlap_path, index=False)

    # ---------- NEW: global presence matrix + unions/intersections/uniques ----------
    run_keys = [_run_key(r) for r in runs]
    series_all = sorted(set().union(*[set(r.selected) for r in runs]))

    # Presence matrix (0/1 per run)
    presence = pd.DataFrame(index=series_all, columns=run_keys, dtype=int)
    for r, key in zip(runs, run_keys):
        S = set(r.selected)
        presence[key] = [1 if s in S else 0 for s in presence.index]
    presence_path = out_prefix.with_suffix(".presence_matrix.csv")
    presence.to_csv(presence_path, index_label="series")

    # Global union & intersection
    union_all = set(series_all)
    inter_all = set(series_all)
    for r in runs:
        Sr = set(r.selected)
        union_all |= Sr
        inter_all &= Sr
    # Note: inter_all should be computed as intersection over all runs; start from first set
    if runs:
        inter_all = set(runs[0].selected)
        for r in runs[1:]:
            inter_all &= set(r.selected)

    _write_list(out_prefix.with_suffix(".union_all.csv"), union_all)
    _write_list(out_prefix.with_suffix(".intersection_all.csv"), inter_all)

    # Per-run uniques (relative to all others)
    for i, ri in enumerate(runs):
        others = set().union(*[set(r.selected) for j, r in enumerate(runs) if j != i])
        only_i = set(ri.selected) - others
        _write_list(out_prefix.with_suffix(f".only__{_run_key(ri)}.csv"), only_i)

    # Pairwise lists
    for ri, rj in itertools.combinations(runs, 2):
        keyA = _run_key(ri)
        keyB = _run_key(rj)
        A = set(ri.selected); B = set(rj.selected)
        inter = A & B
        onlyA = A - B
        onlyB = B - A
        union = A | B
        _write_list(out_prefix.with_suffix(f".pair__{keyA}__vs__{keyB}__intersection.csv"), inter)
        _write_list(out_prefix.with_suffix(f".pair__{keyA}__vs__{keyB}__onlyA.csv"), onlyA)
        _write_list(out_prefix.with_suffix(f".pair__{keyA}__vs__{keyB}__onlyB.csv"), onlyB)
        _write_list(out_prefix.with_suffix(f".pair__{keyA}__vs__{keyB}__union.csv"), union)

    # Markdown summary (no external deps)
    lines: List[str] = []
    if runs:
        panel = runs[0].panel
        tag = runs[0].tag
    else:
        panel = tag = "(unknown)"
    lines.append("# Variable preselection comparison\n")
    lines.append(f"- Panel: **{panel}**")
    lines.append(f"- Tag: **{tag}**\n")

    lines.append("## Runs")
    for r in runs:
        lines.append(f"- `{r.method}`  label=`{r.label or ''}`  → n={len(r.selected)}  ({r.json_path})")

    lines.append("\n## Counts")
    lines.append(_df_to_markdown(counts))

    lines.append("\n## Pairwise overlap")
    if not overlap.empty:
        lines.append(_df_to_markdown(overlap))
    else:
        lines.append("_(At least two inputs are required — this should never be empty.)_")

    lines.append("\n## Artifacts")
    lines.append(f"- Counts: `{counts_path}`")
    lines.append(f"- Overlap: `{overlap_path}`")
    lines.append(f"- Presence matrix: `{presence_path}`")
    lines.append(f"- Global union list: `{out_prefix.with_suffix('.union_all.csv')}`")
    lines.append(f"- Global intersection list: `{out_prefix.with_suffix('.intersection_all.csv')}`")
    lines.append(f"- Per-run uniques: `{out_prefix}.only__<run_key>.csv`")
    lines.append(f"- Pairwise lists: `{out_prefix}.pair__<A>__vs__<B>__{{intersection|onlyA|onlyB|union}}.csv`")

    md_path.write_text("\n".join(lines), encoding="utf-8")

    print("Wrote:")
    print(f"  {counts_path}")
    print(f"  {overlap_path}")
    print(f"  {presence_path}")
    print(f"  {out_prefix.with_suffix('.union_all.csv')}")
    print(f"  {out_prefix.with_suffix('.intersection_all.csv')}")
    # Per-run and pairwise files are numerous; rely on the .md for the full index.


def main():
    ap = argparse.ArgumentParser(
        description="Compare variable preselection selections across TWO OR MORE metadata JSONs "
                    "(all from the SAME {panel, tag}). "
                    "Writes counts, pairwise stats, presence matrix, and series lists for intersections/uniques/unions."
    )
    ap.add_argument(
        "--json", nargs="+", required=True,
        help="Two or more metadata JSONs produced by scripts/preselection/run_preselection.py "
             "(e.g., data/metadata/variants/{panel}__{tag}__preselect_{method}__[label].json)"
    )
    ap.add_argument(
        "--out-prefix", default=None,
        help="Output prefix for artifacts. If omitted, defaults to "
             "output/compare__{panel}__{tag}__{k}runs (k = number of inputs)."
    )
    args = ap.parse_args()

    json_paths = [Path(p) for p in args.json]
    if len(json_paths) < 2:
        raise ValueError("Please provide at least TWO JSONs via --json.")

    # Load and validate
    runs = [_load_meta(p) for p in json_paths]

    # Enforce same (panel, tag)
    p0, t0 = runs[0].panel, runs[0].tag
    for r in runs[1:]:
        if r.panel != p0 or r.tag != t0:
            raise ValueError(
                f"All inputs must come from the SAME (panel, tag). "
                f"Got ({p0}, {t0}) and ({r.panel}, {r.tag})."
            )

    # Prepare outputs
    if args.out_prefix:
        out_prefix = Path(args.out_prefix)
    else:
        out_prefix = OUT_DIR / f"compare__{p0}__{t0}__{len(runs)}runs"

    counts_df, overlap_df = compare_runs(runs)
    write_outputs(out_prefix, runs, counts_df, overlap_df)


if __name__ == "__main__":
    main()
