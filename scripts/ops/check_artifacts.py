#!/usr/bin/env python3
from pathlib import Path
import json, yaml

def load_yaml(p="config.yaml"): return yaml.safe_load(Path(p).read_text(encoding="utf-8"))
def load_cfg(p: Path): return json.loads(p.read_text(encoding="utf-8"))

def tag(cfg):
    tr = cfg["time_spans"]["train"]; return f"train{tr['start'][:4]}_{tr['end'][:4]}"

def expected_for(cfg):
    panel = cfg["panel_id"]; t = tag(cfg)
    base = [
        Path(f"dataset/{panel}/baseline/X_panel_z__{panel}__{t}.csv"),
        Path(f"dataset/{panel}/baseline/y_target_z__{panel}__{t}.csv"),
    ]
    covid = [
        Path(f"dataset/{panel}/covid_dummies/X_panel_z__{panel}__{t}__covid_dummies.csv"),
        Path(f"dataset/{panel}/covid_winsor/X_panel_z__{panel}__{t}__covid_winsor.csv"),
        Path(f"dataset/{panel}/covid_delete/W_estimation_weights__{panel}__{t}.csv"),
    ]
    masks = [
        Path(f"output/{panel}/masks_pred"),     # directory existence
        Path(f"output/{panel}/masks_target"),   # directory existence
    ]
    return base, covid, masks

def main():
    yml = load_yaml()
    cfg_map = yml.get("backtest", {}).get("configs", {})
    if not cfg_map:
        print("No backtest configs found in config.yaml"); return
    missing = False
    for name, p in cfg_map.items():
        cfg = load_cfg(Path(p))
        base, covid, masks = expected_for(cfg)
        print(f"[{name}]")
        for fp in base:
            print(("OK   " if fp.exists() else "MISS ") + str(fp))
            missing |= not fp.exists()
        # Optional COVID checks: only warn if folder is expected to exist but empty
        for fp in covid:
            print(("OK   " if fp.exists() else "WARN ") + str(fp))
        for d in masks:
            if d.exists() and any(d.iterdir()):
                print("OK   " + str(d) + " (has files)")
            else:
                print("MISS " + str(d))
                missing = True
        print()
    if missing:
        print("Some required artifacts are missing. Build them via preprocessing/covid/masking scripts.")
    else:
        print("All required artifacts present.")
if __name__ == "__main__":
    main()
