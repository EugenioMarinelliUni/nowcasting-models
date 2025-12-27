# src/dfm_pipeline/covid/spec.py
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional, Tuple

import pandas as pd


def _month_boundary(x: str, monthly_freq: str = "MS") -> pd.Timestamp:
    ym = x[:7]
    p = pd.Period(ym, "M")
    how = "start" if str(monthly_freq).upper() == "MS" else "end"
    return p.to_timestamp(how=how)


def parse_window(s: Optional[str]) -> Optional[Tuple[str, str]]:
    if s is None:
        return None
    ss = str(s).strip()
    if not ss:
        return None
    parts = [p.strip() for p in ss.split(",")]
    if len(parts) != 2:
        raise ValueError("window must be 'YYYY-MM-DD,YYYY-MM-DD'")
    return parts[0], parts[1]


@dataclass(frozen=True)
class CovidSpec:
    covid_start: str
    covid_end: str
    monthly_freq: str = "MS"
    train_start: Optional[str] = None
    train_end: Optional[str] = None
    fit_window: Optional[Tuple[str, str]] = None
    apply_window: Optional[Tuple[str, str]] = None
    allow_leakage: bool = False

    def covid_bounds(self) -> Tuple[pd.Timestamp, pd.Timestamp]:
        s = _month_boundary(self.covid_start, self.monthly_freq)
        e = _month_boundary(self.covid_end, self.monthly_freq)
        if s > e:
            raise ValueError(f"covid_start {s.date()} > covid_end {e.date()}")
        return s, e

    def covid_suffix(self) -> str:
        s = self.covid_start[:7].replace("-", "M")
        e = self.covid_end[:7].replace("-", "M")
        return f"covid_{s}_{e}"

    def to_dict(self) -> dict:
        d = asdict(self)
        if d.get("fit_window") is not None:
            d["fit_window"] = list(d["fit_window"])
        if d.get("apply_window") is not None:
            d["apply_window"] = list(d["apply_window"])
        return d
