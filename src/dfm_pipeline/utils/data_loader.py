# src/dfm_pipeline/utils/data_loader.py

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from dfm_pipeline.utils.config_loader import load_config


def _resolve_path(p: str | Path, base_dir: Path) -> Path:
    pp = Path(p)
    return pp if pp.is_absolute() else (base_dir / pp).resolve()


def _first_existing(paths: Iterable[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None


def _detect_tcode_row(csv_path: Path, date_col: str) -> bool:
    """
    Detect whether the first data row (after header) is a FRED-MD style
    transformation-code row.

    Heuristic:
    - date_col exists
    - row 0 date_col is not parseable as a date
    - most non-null cells in row 0 (excluding date_col) are small ints (1..7)
    - row 1 date_col *is* parseable as a date (i.e., first real observation)
    """
    peek = pd.read_csv(csv_path, nrows=2)
    if date_col not in peek.columns or len(peek) < 2:
        return False

    r0 = peek.iloc[0]
    r1 = peek.iloc[1]

    r0_date = pd.to_datetime(r0.get(date_col), errors="coerce")
    r1_date = pd.to_datetime(r1.get(date_col), errors="coerce")

    if pd.notna(r0_date):
        return False
    if pd.isna(r1_date):
        return False

    rest = r0.drop(labels=[date_col], errors="ignore")
    nums = pd.to_numeric(rest, errors="coerce").dropna()
    if nums.empty or len(nums) < 5:
        return False

    share_small_ints = (nums.between(1, 7)).mean()
    return float(share_small_ints) >= 0.8


def _read_csv_with_optional_tcodes(
    csv_path: Path,
    *,
    date_col: str = "sasdate",
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Read a CSV into (df, transform_codes).

    If the file is in FRED-MD format (row 2 = tcodes), we extract tcodes
    and skip that row when reading data.

    If not, return an empty transform_codes Series.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"File not found at: {csv_path}")

    # Allow common alternative date column
    # (many panel CSVs use "Date" instead of "sasdate")
    cols = pd.read_csv(csv_path, nrows=0).columns
    if date_col not in cols and "Date" in cols:
        date_col = "Date"

    has_tcodes = _detect_tcode_row(csv_path, date_col=date_col)

    if has_tcodes:
        # Read first data row after header (tcodes row)
        tcode_row = pd.read_csv(csv_path, nrows=1)
        # Convert to numeric where possible; drop date_col
        tc = pd.to_numeric(tcode_row.iloc[0].drop(labels=[date_col], errors="ignore"), errors="coerce")
        tc.name = "tcode"

        df = pd.read_csv(
            csv_path,
            skiprows=[1],  # skip tcodes row (line 1; header is line 0)
            parse_dates=[date_col],
            index_col=date_col,
        )
        return df, tc

    # No tcodes row; just read the file as a standard time-indexed panel
    df = pd.read_csv(
        csv_path,
        parse_dates=[date_col] if date_col in cols or date_col == "Date" else None,
    )

    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.set_index(date_col)

    df = df.sort_index()
    empty_tc = pd.Series(dtype="float64", name="tcode")
    return df, empty_tc


def load_data(
    stage: str = "raw",
    *,
    config_path: str | Path = "config.yaml",
    path: str | Path | None = None,
    panel_key: str | None = None,
    date_col: str = "sasdate",
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Load dataset and (optional) transformation codes.

    Parameters
    ----------
    stage:
        'raw' or 'processed'.
    config_path:
        Path to YAML config (default: config.yaml in CWD).
    path:
        Explicit CSV path override. If provided, stage/panel_key are ignored.
    panel_key:
        For stage='processed', optionally select a specific config key under
        config['paths'] (e.g., 'panel_1960', 'panel_1965', 'panel_z_1960_baseline', ...).
        If not provided, the loader tries sensible fallbacks.
    date_col:
        Preferred date column name (default 'sasdate'). If missing and 'Date' exists,
        it will use 'Date'.

    Returns
    -------
    df:
        Time-indexed DataFrame.
    transform_codes:
        Series of tcodes if file is in FRED-MD format; otherwise empty Series.

    Notes
    -----
    Option A fix: stage='processed' no longer requires config['paths']['processed_filename'].
    """
    config_path = Path(config_path)
    base_dir = config_path.resolve().parent if config_path.exists() else Path.cwd()

    config = load_config(str(config_path))

    # Explicit override
    if path is not None:
        csv_path = _resolve_path(path, base_dir)
        return _read_csv_with_optional_tcodes(csv_path, date_col=date_col)

    paths_cfg = (config or {}).get("paths", {})

    if stage == "raw":
        raw_filename = paths_cfg.get("raw_filename")
        candidates: list[Path] = []

        if isinstance(raw_filename, str) and raw_filename.strip():
            # raw_filename may be a full relative path or just a filename
            candidates.extend(
                [
                    _resolve_path(raw_filename, base_dir),
                    _resolve_path(Path("data") / "raw_data" / raw_filename, base_dir),
                    _resolve_path(Path("data") / raw_filename, base_dir),
                ]
            )

        # If user already stored the raw file somewhere else, try common repo locations
        candidates.extend(
            [
                _resolve_path(Path("data") / "raw_data" / "fred_md_current.csv", base_dir),
                _resolve_path(Path("data") / "fred_md_current.csv", base_dir),
            ]
        )

        csv_path = _first_existing(candidates)
        if csv_path is None:
            remote = paths_cfg.get("remote_url")
            hint = f"stage='raw' could not find a CSV. Checked: {[str(p) for p in candidates]}"
            if remote:
                hint += f". Config provides paths.remote_url; download the file to one of these locations or pass path=..."
            raise FileNotFoundError(hint)

        return _read_csv_with_optional_tcodes(csv_path, date_col=date_col)

    if stage == "processed":
        # Preferred: explicit processed_filename if present
        processed_filename = paths_cfg.get("processed_filename")
        if isinstance(processed_filename, str) and processed_filename.strip():
            candidates = [
                _resolve_path(processed_filename, base_dir),
                _resolve_path(Path("data") / "processed_data" / processed_filename, base_dir),
            ]
            csv_path = _first_existing(candidates)
            if csv_path is not None:
                return _read_csv_with_optional_tcodes(csv_path, date_col=date_col)

        # Next: user selects a specific config key (panel_key)
        if panel_key is not None:
            if panel_key not in paths_cfg:
                raise KeyError(
                    f"panel_key='{panel_key}' not found in config['paths']. "
                    f"Available keys: {sorted(paths_cfg.keys())[:20]} ..."
                )
            csv_path = _resolve_path(paths_cfg[panel_key], base_dir)
            return _read_csv_with_optional_tcodes(csv_path, date_col=date_col)

        # Sensible fallbacks for this repo: try common panel keys
        for k in ("panel_1960", "panel_1965", "panel_1960_fixhousing_delta", "panel_1965_fixhousing_delta"):
            v = paths_cfg.get(k)
            if isinstance(v, str) and v.strip():
                p = _resolve_path(v, base_dir)
                if p.exists():
                    return _read_csv_with_optional_tcodes(p, date_col=date_col)

        # Next: if processed_dir exists and has exactly one CSV, take it
        processed_dir = paths_cfg.get("processed_dir")
        if isinstance(processed_dir, str) and processed_dir.strip():
            pdir = _resolve_path(processed_dir, base_dir)
            if pdir.exists() and pdir.is_dir():
                csvs = sorted([p for p in pdir.glob("*.csv") if p.is_file()])
                if len(csvs) == 1:
                    return _read_csv_with_optional_tcodes(csvs[0], date_col=date_col)

        # Otherwise: fail with an actionable message (but no KeyError)
        raise ValueError(
            "stage='processed' requires either:\n"
            "  - config['paths']['processed_filename'], or\n"
            "  - panel_key pointing to an existing CSV path in config['paths'], or\n"
            "  - one of the common panel keys (panel_1960/panel_1965/...) to exist, or\n"
            "  - processed_dir containing exactly one CSV.\n"
            f"Config keys present under 'paths': {sorted(paths_cfg.keys())[:30]} ..."
        )

    raise ValueError("stage must be 'raw' or 'processed'")
