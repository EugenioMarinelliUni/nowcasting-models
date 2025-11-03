#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union

import pandas as pd


JsonLike = Union[str, Path]
NameToSet = Mapping[str, Set[str]]


@dataclass
class SelectionComparison:
    """Container for comparison outputs."""
    sizes: pd.Series                     # |S_m| per method
    overlaps: pd.DataFrame               # |S_i ∩ S_j|
    jaccard: pd.DataFrame                # J(S_i, S_j)
    union_size: int                      # |⋃ S_m|
    intersection_size: int               # |⋂ S_m|
    membership: pd.DataFrame             # rows: variables; cols: indicator per method + 'support'


# --------------------------
# Loading helpers
# --------------------------

def _load_json_selected(path: JsonLike) -> List[str]:
    """
    Load a 'selected' list from your meta JSON.
    Tries keys: 'selected', then 'selected_features'.
    """
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    if "selected" in data and isinstance(data["selected"], list):
        return list(map(str, data["selected"]))
    if "selected_features" in data and isinstance(data["selected_features"], list):
        return list(map(str, data["selected_features"]))
    raise ValueError(f"No 'selected' or 'selected_features' list found in {p}")


def selections_from_json_files(named_paths: Mapping[str, JsonLike]) -> Dict[str, Set[str]]:
    """
    Load multiple selections from JSON artifacts.
    named_paths: dict like {'sis': '...preselect_sis.json', 'tstat': '...preselect_tstat.json', ...}
    Returns: dict of {name: set_of_variables}
    """
    out: Dict[str, Set[str]] = {}
    for name, jpath in named_paths.items():
        out[name] = set(_load_json_selected(jpath))
    return out


def selections_from_csv_headers(named_paths: Mapping[str, JsonLike]) -> Dict[str, Set[str]]:
    """
    Alternative loader: read selected variables as the columns of a preselection CSV.
    """
    out: Dict[str, Set[str]] = {}
    for name, cpath in named_paths.items():
        cols = pd.read_csv(cpath, nrows=0).columns.tolist()
        out[name] = set(map(str, cols))
    return out


# --------------------------
# Core comparison
# --------------------------

def compare_selected_sets(selections: NameToSet) -> SelectionComparison:
    """
    Compare any number (>=2) of named selections.

    Returns:
      - sizes: |S_m| per method
      - overlaps: pairwise |S_i ∩ S_j|
      - jaccard: pairwise Jaccard indices
      - union_size: |⋃ S_m|
      - intersection_size: |⋂ S_m|
      - membership: long table of variables with 0/1 indicators per method and a 'support' count
    """
    if len(selections) < 2:
        raise ValueError("Need at least two selections to compare.")

    names = list(selections.keys())
    sets = [set(selections[n]) for n in names]

    # Sizes
    sizes = pd.Series({n: len(selections[n]) for n in names}, dtype="int64")

    # Pairwise overlaps and Jaccard
    overlaps = pd.DataFrame(0, index=names, columns=names, dtype="int64")
    jaccard = pd.DataFrame(0.0, index=names, columns=names, dtype="float64")
    for i, ni in enumerate(names):
        Si = selections[ni]
        for j, nj in enumerate(names):
            Sj = selections[nj]
            inter = len(Si & Sj)
            union = len(Si | Sj) if (Si or Sj) else 1
            overlaps.loc[ni, nj] = inter if i <= j else overlaps.loc[nj, ni]
            jaccard.loc[ni, nj] = inter / union if union > 0 else 1.0

    # Multi-way union & intersection
    union_set = set().union(*sets)
    inter_set = set.intersection(*sets) if sets else set()

    # Membership table
    rows = []
    for v in sorted(union_set):
        row = {"variable": v}
        support = 0
        for n in names:
            flag = 1 if v in selections[n] else 0
            row[n] = flag
            support += flag
        row["support"] = support
        rows.append(row)
    membership = pd.DataFrame(rows).sort_values(["support", "variable"], ascending=[False, True])

    return SelectionComparison(
        sizes=sizes,
        overlaps=overlaps,
        jaccard=jaccard,
        union_size=len(union_set),
        intersection_size=len(inter_set),
        membership=membership,
    )


# --------------------------
# Save helpers
# --------------------------

def save_comparison_outputs(
    comp: SelectionComparison,
    out_prefix: Union[str, Path],
    write_membership: bool = True,
    write_pairwise: bool = True,
) -> Dict[str, str]:
    """
    Save tables to CSV. Returns dict of written paths.
      - {out_prefix}__membership.csv        (diff table)
      - {out_prefix}__overlaps.csv          (|S_i ∩ S_j|)
      - {out_prefix}__jaccard.csv           (Jaccard matrix)
      - {out_prefix}__sizes.csv             (sizes)
    """
    out_prefix = Path(out_prefix)
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    written = {}

    comp.sizes.to_csv(f"{out_prefix}__sizes.csv", header=["size"])
    written["sizes"] = f"{out_prefix}__sizes.csv"

    if write_pairwise:
        comp.overlaps.to_csv(f"{out_prefix}__overlaps.csv")
        comp.jaccard.to_csv(f"{out_prefix}__jaccard.csv")
        written["overlaps"] = f"{out_prefix}__overlaps.csv"
        written["jaccard"] = f"{out_prefix}__jaccard.csv"

    if write_membership:
        comp.membership.to_csv(f"{out_prefix}__membership.csv", index=False)
        written["membership"] = f"{out_prefix}__membership.csv"

    return written
