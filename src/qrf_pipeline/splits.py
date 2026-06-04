from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json

import pandas as pd


@dataclass(frozen=True)
class QRFValidationProtocol:
    """
    Explicit split protocol for QRF model selection.

    preselect_* is used to create fixed predictor lists.
    tune_* is used to choose QRF hyperparameters.
    test_* is kept untouched until the final evaluation.
    """

    preselect_start: str
    preselect_end: str
    tune_start: str
    tune_end: str
    test_start: str
    test_end: str

    def as_dict(self) -> dict[str, str]:
        return {
            "preselect_start": self.preselect_start,
            "preselect_end": self.preselect_end,
            "tune_start": self.tune_start,
            "tune_end": self.tune_end,
            "test_start": self.test_start,
            "test_end": self.test_end,
        }


def month_start(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError("Date cannot be NaT")
    return ts.to_period("M").to_timestamp(how="start")


def validate_protocol(protocol: QRFValidationProtocol) -> None:
    ps = month_start(protocol.preselect_start)
    pe = month_start(protocol.preselect_end)
    ts = month_start(protocol.tune_start)
    te = month_start(protocol.tune_end)
    xs = month_start(protocol.test_start)
    xe = month_start(protocol.test_end)

    if not (ps <= pe < ts <= te < xs <= xe):
        raise ValueError(
            "Invalid protocol. Expected "
            "preselect_start <= preselect_end < tune_start <= tune_end < test_start <= test_end."
        )


def load_protocol(path: str | Path) -> QRFValidationProtocol:
    with open(path, "r", encoding="utf-8") as f:
        obj: dict[str, Any] = json.load(f)
    protocol = QRFValidationProtocol(**obj)
    validate_protocol(protocol)
    return protocol


def save_protocol(protocol: QRFValidationProtocol, path: str | Path) -> Path:
    validate_protocol(protocol)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(protocol.as_dict(), f, indent=2)
    return path


def restrict_monthly(
    obj: pd.DataFrame | pd.Series,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
) -> pd.DataFrame | pd.Series:
    out = obj.copy()
    idx = pd.DatetimeIndex(pd.to_datetime(out.index))
    out.index = idx.to_period("M").to_timestamp(how="start")
    out = out.sort_index()
    return out.loc[(out.index >= month_start(start)) & (out.index <= month_start(end))]
