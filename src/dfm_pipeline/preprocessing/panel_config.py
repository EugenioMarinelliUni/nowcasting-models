# src/dfm_pipeline/preprocessing/panel_config.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import yaml


@dataclass(frozen=True)
class TrainOOSWindow:
    """
    Generic time window.

    - start: required (YYYY-MM-DD string)
    - end:   optional; if None, downstream code can infer it
            (e.g. from the last date in the full panel).
    """
    start: str
    end: str | None = None


@dataclass(frozen=True)
class CovidPaths:
    """
    All file paths related to Covid artifacts for a given panel.
    """
    covid_meta_dir: str

    delete_weights_csv: str
    dummies_full_csv: str
    winsor_full_csv: str

    train_delete_csv: str
    train_win_csv: str
    train_dummy_csv: str

    oos_delete_csv: str
    oos_win_csv: str
    oos_dummy_csv: str


@dataclass(frozen=True)
class PanelConfig:
    """
    Minimal config needed for:
      - concatenating train + OOS standardized panels into a full panel
      - building Covid delete/dummy/winsorized variants and their
        train/OOS splits.
    """
    panel_name: str

    # Standardized X panels
    train_panel_csv: str
    oos_panel_csv: str
    full_panel_csv: str

    # Logical train/OOS windows
    train_window: TrainOOSWindow
    oos_window: TrainOOSWindow

    # Covid-related paths
    covid: CovidPaths


def load_panel_config(path: str | Path) -> PanelConfig:
    """
    Load a panel YAML config.

    Requirements:
      - train_window.start (string)
      - train_window.end   (string)
      - oos_window.start   (string)
      - oos_window.end     (string or omitted/null; if missing, we store None)
    """
    p = Path(path)
    with p.open("r") as f:
        cfg: Dict[str, Any] = yaml.safe_load(f)

    # train_window: end required
    tw_raw = cfg["train_window"]
    train_w = TrainOOSWindow(
        start=tw_raw["start"],
        end=tw_raw.get("end"),  # you can still allow None here if you want
    )

    # oos_window: end optional
    ow_raw = cfg["oos_window"]
    oos_w = TrainOOSWindow(
        start=ow_raw["start"],
        end=ow_raw.get("end"),  # may be None
    )

    covid_paths = CovidPaths(**cfg["covid"])

    return PanelConfig(
        panel_name=cfg["panel_name"],
        train_panel_csv=cfg["train_panel_csv"],
        oos_panel_csv=cfg["oos_panel_csv"],
        full_panel_csv=cfg["full_panel_csv"],
        train_window=train_w,
        oos_window=oos_w,
        covid=covid_paths,
    )
