#!/usr/bin/env python3
from __future__ import annotations
from pathlib import Path
import json, yaml

# -------------------------
# Config + backtest helpers
# -------------------------

def load_yaml(path: str | Path = "config.yaml") -> dict:
    """Load project YAML config."""
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))

def load_backtest_config(cfg_path: str | Path) -> dict:
    """Load a single backtest JSON config."""
    return json.loads(Path(cfg_path).read_text(encoding="utf-8"))

def list_backtest_configs_from_yaml(yml: dict) -> list[Path]:
    """Return all backtest config paths listed in config.yaml -> backtest.configs."""
    cfg_map = yml.get("backtest", {}).get("configs", {})
    return [Path(p) for p in cfg_map.values()]

def monthly_is_ms(yml: dict) -> bool:
    """True if dates.monthly_freq == 'MS' (month-start)."""
    return str(yml["dates"]["monthly_freq"]).upper() == "MS"

# -------------------------
# Path constructors (tagged)
# -------------------------

def train_tag_from_cfg(cfg: dict) -> str:
    """Build train tag 'trainYYYY_YYYY' from cfg['time_spans']['train']."""
    tr = cfg["time_spans"]["train"]
    return f"train{tr['start'][:4]}_{tr['end'][:4]}"

def baseline_panel_path_from_cfg(cfg: dict) -> Path:
    """dataset/<panel>/baseline/X_panel_z__<panel>__<train_tag>.csv"""
    panel = cfg["panel_id"]
    tag = train_tag_from_cfg(cfg)
    return Path(f"dataset/{panel}/baseline/X_panel_z__{panel}__{tag}.csv")

def variant_panel_path_from_cfg(cfg: dict, variant: str) -> Path:
    """
    dataset/<panel>/<variant>/X_panel_z__<panel>__<train_tag>__<variant>.csv
    variant in {'covid_dummies','covid_winsor'}
    """
    panel = cfg["panel_id"]
    tag = train_tag_from_cfg(cfg)
    if variant not in {"covid_dummies", "covid_winsor"}:
        raise ValueError("variant must be 'covid_dummies' or 'covid_winsor'")
    return Path(f"dataset/{panel}/{variant}/X_panel_z__{panel}__{tag}__{variant}.csv")

def delete_weights_path_from_cfg(cfg: dict) -> Path:
    """dataset/<panel>/covid_delete/W_estimation_weights__<panel>__<train_tag>.csv"""
    panel = cfg["panel_id"]
    tag = train_tag_from_cfg(cfg)
    return Path(f"dataset/{panel}/covid_delete/W_estimation_weights__{panel}__{tag}.csv")

# -------------------------
# Backward-compat (legacy)
# -------------------------

def panel_z_path_from_cfg(cfg: dict, variant: str = "baseline") -> Path:
    """
    Legacy name kept for older scripts.
    For variant='baseline' -> baseline tagged path.
    For 'covid_dummies'/'covid_winsor' -> uses variant tagged path.
    """
    if variant == "baseline":
        return baseline_panel_path_from_cfg(cfg)
    return variant_panel_path_from_cfg(cfg, variant)
