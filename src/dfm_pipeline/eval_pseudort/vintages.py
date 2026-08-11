from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import pandas as pd


@dataclass(frozen=True)
class VintageSnapshot:
    as_of: pd.Timestamp
    X: pd.DataFrame
    y: pd.Series
    metadata: dict[str, Any] = field(default_factory=dict)


class VintageProvider(Protocol):
    def get_vintage(self, as_of: pd.Timestamp) -> VintageSnapshot: ...


class LongFormatVintageProvider:
    """Construct true historical snapshots from a long-format vintage table.

    Required columns by default:
      ``series``, ``reference_date``, ``vintage_date``, ``value``.

    ``target_series`` identifies the quarterly target.  At each evaluation date,
    the provider chooses the latest vintage not later than that date for every
    (series, reference_date) pair.  Predictor columns are returned in
    ``predictor_order`` when supplied.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        *,
        target_series: str,
        predictor_order: list[str] | None = None,
        series_col: str = "series",
        reference_col: str = "reference_date",
        vintage_col: str = "vintage_date",
        value_col: str = "value",
    ) -> None:
        self.series_col = series_col
        self.reference_col = reference_col
        self.vintage_col = vintage_col
        self.value_col = value_col
        self.target_series = str(target_series)
        self.predictor_order = predictor_order

        required = {series_col, reference_col, vintage_col, value_col}
        missing = required.difference(data.columns)
        if missing:
            raise ValueError(f"Vintage table is missing columns: {sorted(missing)}")

        df = data[list(required)].copy()
        df[reference_col] = pd.to_datetime(df[reference_col]).dt.to_period("M").dt.to_timestamp()
        df[vintage_col] = pd.to_datetime(df[vintage_col])
        df = df.dropna(subset=[series_col, reference_col, vintage_col]).copy()

        key_cols = [series_col, reference_col, vintage_col]
        duplicate_mask = df.duplicated(key_cols, keep=False)
        if duplicate_mask.any():
            examples = (
                df.loc[duplicate_mask, key_cols]
                .drop_duplicates()
                .head(5)
                .to_dict(orient="records")
            )
            raise ValueError(
                "Vintage table contains duplicate "
                "(series, reference_date, vintage_date) keys after monthly "
                f"reference-date normalization. Examples: {examples}"
            )

        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
        self.data = df.sort_values(vintage_col)

    @classmethod
    def from_csv(cls, path: str | Path, **kwargs) -> "LongFormatVintageProvider":
        return cls(pd.read_csv(path), **kwargs)

    def get_vintage(self, as_of: pd.Timestamp) -> VintageSnapshot:
        as_of = pd.Timestamp(as_of)
        available = self.data[self.data[self.vintage_col] <= as_of].copy()
        if available.empty:
            raise ValueError(f"No vintage observations are available by {as_of.date()}.")

        latest = available.drop_duplicates(
            [self.series_col, self.reference_col], keep="last"
        )
        wide = latest.pivot(
            index=self.reference_col,
            columns=self.series_col,
            values=self.value_col,
        ).sort_index()
        wide.index = pd.DatetimeIndex(wide.index).to_period("M").to_timestamp()

        if self.target_series not in wide.columns:
            y = pd.Series(index=wide.index, dtype=float, name=self.target_series)
        else:
            y = wide[self.target_series].astype(float)

        X = wide.drop(columns=[self.target_series], errors="ignore").astype(float)
        if self.predictor_order is not None:
            X = X.reindex(columns=self.predictor_order)

        return VintageSnapshot(
            as_of=as_of,
            X=X,
            y=y,
            metadata={"mode": "historical_long_format", "rows_available": int(len(available))},
        )


__all__ = ["VintageSnapshot", "VintageProvider", "LongFormatVintageProvider"]
