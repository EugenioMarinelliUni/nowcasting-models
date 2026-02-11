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
    m = _TRAIN_TAG_RE.match(tag)
    if not m:
        return None
    y0, y1 = m.group(1), m.group(2)
    return f"{y0}-02-01", f"{y1}-12-01"


def _discover_training_sets(root: Path = Path("dataset")) -> List[TrainingSetRef]:
    refs: List[TrainingSetRef] = []
    for x_path in root.glob("*/training_sets/*/standardized_train__*__*.csv"):
        name = x_path.name
        if name.endswith("__train_stats.csv"):
            continue

        try:
            panel = x_path.parts[1]
            tag = x_path.parts[3]
        except Exception:
            continue

        se = _infer_train_window_from_tag(tag)
        if se is not None:
            start_s, end_s = se
        else:
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
    place: str = "end",
    panels: Iterable[str] | None = None,
    save_stats: bool = True,
) -> List[TrainingSetRef]:
    yq = read_quarterly_target(
        raw_quarterly_csv,
        date_col="sasdate",
        value_col="gdp_qoq_saar",
    )
    # Quarter->monthly mapping (default: quarter-end dating)
    ym_proto = quarterly_to_monthly(yq, monthly_freq=monthly_freq, place=place)

    refs = _discover_training_sets()
    if panels:
        pset = set(panels)
        refs = [r for r in refs if r.panel in pset]

    processed: List[TrainingSetRef] = []
    for r in refs:
        try:
            idx = build_monthly_index_from_panel(r.x_path, date_col="Date", monthly_freq=monthly_freq)
        except Exception as e:
            print(f"[SKIP] {r.panel} {r.tag}: cannot build index from {r.x_path.name} ({e})")
            continue

        ym = ym_proto.reindex(idx)

        if ym.loc[r.start:r.end].dropna().empty:
            print(f"[SKIP] {r.panel} {r.tag}: no non-NaN target values in {r.start.date()}..{r.end.date()}")
            continue

        try:
            yz, stats = standardize_target_on_window(ym, start=r.start, end=r.end)
        except ValueError as e:
            print(f"[SKIP] {r.panel} {r.tag}: {e}")
            continue

        r.out_dir.mkdir(parents=True, exist_ok=True)
        yz.to_frame("y").to_csv(r.out_path, index_label="Date")
        if save_stats:
            pd.DataFrame(
                {"mean": [stats.mean], "std": [stats.std], "nobs": [stats.nobs]},
                index=[f"{r.start.date()}..{r.end.date()}"],
            ).to_csv(r.stats_path, index_label="train_window")

        processed.append(r)

    return processed
