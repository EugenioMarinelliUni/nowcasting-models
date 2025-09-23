# src/dfm_pipeline/ingestion/fred_client.py
from __future__ import annotations

from typing import Optional, Dict, Any
import pandas as pd
import requests

__all__ = [
    "API_URL_DEFAULT",
    "get_series",
    "to_named_column",
]

# Official observations endpoint (JSON)
API_URL_DEFAULT = "https://api.stlouisfed.org/fred/series/observations"


def get_series(
    series_id: str,
    api_key: str,
    *,
    vintage: Optional[str] = None,         # 'YYYY-MM-DD' for a specific ALFRED vintage
    api_url: str = API_URL_DEFAULT,
    timeout: int = 30,
    extra_params: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Fetch a single FRED/ALFRED series' observations as a tidy DataFrame.

    Parameters
    ----------
    series_id : str
        FRED series id (e.g., "A191RL1Q225SBEA" for real GDP q/q SAAR).
    api_key : str
        Your FRED API key (keep it out of source control; pass via env/.env).
    vintage : str | None
        If provided (YYYY-MM-DD), uses ALFRED real-time vintage on that date.
        If omitted, returns latest revised values (plain FRED).
    api_url : str
        Observations endpoint (defaults to API_URL_DEFAULT).
    timeout : int
        HTTP timeout (seconds).
    extra_params : dict | None
        Optional extra query params (e.g., {"observation_start": "1990-01-01"}).

    Returns
    -------
    DataFrame with columns: ['sasdate', '<series_id>'] and dtype float for values.
    Missing values appear as NaN.

    Raises
    ------
    requests.HTTPError
        If the HTTP response is not 2xx.
    RuntimeError
        If the payload has no observations.
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
    }
    if vintage:
        params["realtime_start"] = vintage
        params["realtime_end"] = vintage
    if extra_params:
        params.update(extra_params)

    r = requests.get(api_url, params=params, timeout=timeout)
    r.raise_for_status()
    payload = r.json()

    obs = payload.get("observations", [])
    if not obs:
        raise RuntimeError(
            f"No observations returned for series_id='{series_id}' (vintage={vintage})."
        )

    df = pd.DataFrame(obs)[["date", "value"]]
    # Convert "." to NaN, then to float; parse date
    df["value"] = pd.to_numeric(df["value"].replace(".", None), errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    df = df.rename(columns={"date": "sasdate", "value": series_id}).sort_values("sasdate")
    return df


def to_named_column(df: pd.DataFrame, series_id: str, out_name: str) -> pd.DataFrame:
    """
    Rename the value column '<series_id>' to a friendlier name (e.g., 'gdp_qoq_saar').

    Parameters
    ----------
    df : DataFrame
        Output of get_series(...).
    series_id : str
        The original series id column to rename.
    out_name : str
        New column name.

    Returns
    -------
    DataFrame with columns ['sasdate', out_name].
    """
    if series_id not in df.columns:
        raise KeyError(f"{series_id!r} not found in DataFrame columns: {df.columns.tolist()}")
    out = df.rename(columns={series_id: out_name})
    return out[["sasdate", out_name]]
