from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

from dfm_pipeline.preprocessing.target_standardize import (
    read_quarterly_target,
    quarterly_to_monthly,
    build_monthly_index_from_panel,
    standardize_target_on_window,
)

_TRAIN_TAG_RE = re.compile(r"^train(\d{4})_(\d{4})$")


@dataclass(frozen=True)
class TrainingSetRef:
    panel: str
    tag: str
    x_path: Path
    start: pd.Timestamp
    end: pd.Timestamp

    @property
    def out_dir(self) -> Path:
        return Path("dataset") / self.panel / "baseline"

    @property
    def out_path(self) -> Path:
        return self.out_dir / f"y_target_z__{self.panel}__{self.tag}.csv"

    @property
    def stats_path(self) -> Path:
        p = self.out_path.with_suffix("")
        return p.with_name(p.name + "__train_stats.csv")


def _infer_train_window_from_tag(tag: str) -> Tuple[str, str] | None:
    """
    If tag looks like trainYYYY_YYYY, return canonical monthly dates
    (YYYY-02-01 .. YYYY-12-01) to match your training panels.
    """
    m = _TRAIN_TAG_RE.match(tag)
    if not m:
        return None
    y0, y1 = m.group(1), m.group(2)
    return f"{y0}-02-01", f"{y1}-12-01"


def _discover_training_sets(root: Path = Path("dataset")) -> List[TrainingSetRef]:
    """
    Find all standardized training X panels at:
      dataset/{panel}/training_sets/{tag}/standardized_train__{panel}__{tag}.csv

    Robust to nearby artifacts: skips any '*__train_stats.csv'.
    If the tag doesn't look like 'trainYYYY_YYYY', we fall back to the X index
    to infer [start, end].
    """
    refs: List[TrainingSetRef] = []
    for x_path in root.glob("*/training_sets/*/standardized_train__*__*.csv"):
        name = x_path.name
        # Skip stats artifacts (these have no Date column)
        if name.endswith("__train_stats.csv"):
            continue

        # path parts: ('dataset', '{panel}', 'training_sets', '{tag}', file)
        try:
            panel = x_path.parts[1]
            tag = x_path.parts[3]
        except Exception:
            # Unexpected layout; ignore
            continue

        se = _infer_train_window_from_tag(tag)
        if se is not None:
            start_s, end_s = se
        else:
            # Fallback: read the file and infer start/end from the Date index
            X = pd.read_csv(x_path, parse_dates=["Date"]).set_index("Date").sort_index()
            start_s, end_s = str(X.index.min().date()), str(X.index.max().date())

        refs.append(
            TrainingSetRef(
                panel=panel,
                tag=tag,
                x_path=x_path,
                start=pd.to_datetime(start_s),
                end=pd.to_datetime(end_s),
            )
        )
    return refs


def build_targets_for_all(
    raw_quarterly_csv: Path,
    *,
    monthly_freq: str = "MS",
    place: str = "start",                # "start" (FRED-style) or "end"
    panels: Iterable[str] | None = None,
    save_stats: bool = True,
) -> List[TrainingSetRef]:
    """
    For every discovered training set (optionally filtered by panel),
    create standardized target y aligned to the X monthly index and z-scored on the
    training window, then write to dataset/{panel}/baseline/y_target_z__{panel}__{tag}.csv.
    """
    # 1) Load raw quarterly target once — force known columns
    yq = read_quarterly_target(
        raw_quarterly_csv,
        date_col="sasdate",
        value_col="gdp_qoq_saar",
    )
    # Quarter->monthly mapping (default: quarter-start dating)
    ym_proto = quarterly_to_monthly(yq, monthly_freq=monthly_freq, place=place)

    # 2) Discover training sets
    refs = _discover_training_sets()
    if panels:
        pset = set(panels)
        refs = [r for r in refs if r.panel in pset]

    processed: List[TrainingSetRef] = []
    for r in refs:
        # Align monthly index to the X panel to ensure exact matching timestamps
        try:
            idx = build_monthly_index_from_panel(r.x_path, date_col="Date", monthly_freq=monthly_freq)
        except Exception as e:
            print(f"[SKIP] {r.panel} {r.tag}: cannot build index from {r.x_path.name} ({e})")
            continue

        ym = ym_proto.reindex(idx)

        # Guard: if the training window has no quarter observations (non-NaN), skip
        if ym.loc[r.start:r.end].dropna().empty:
            print(f"[SKIP] {r.panel} {r.tag}: no non-NaN target values in {r.start.date()}..{r.end.date()}")
            continue

        # Standardize on [start, end] using non-NaN months
        try:
            yz, stats = standardize_target_on_window(ym, start=r.start, end=r.end)
        except ValueError as e:
            print(f"[SKIP] {r.panel} {r.tag}: {e}")
            continue

        # Write outputs
        r.out_dir.mkdir(parents=True, exist_ok=True)
        yz.to_frame("y").to_csv(r.out_path, index_label="Date")
        if save_stats:
            pd.DataFrame(
                {"mean": [stats.mean], "std": [stats.std], "nobs": [stats.nobs]},
                index=[f"{r.start.date()}..{r.end.date()}"],
            ).to_csv(r.stats_path, index_label="train_window")

        processed.append(r)

    return processed

