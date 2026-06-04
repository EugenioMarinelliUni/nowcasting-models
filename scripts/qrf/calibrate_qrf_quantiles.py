from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qrf_pipeline.calibration import (
    apply_additive_quantile_calibration,
    fit_additive_quantile_calibration,
    save_calibration,
)
from qrf_pipeline.diagnostics import compute_qrf_density_diagnostics


def _parse_quantiles(value: str) -> tuple[float, ...]:
    return tuple(float(x.strip()) for x in value.split(",") if x.strip())


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fit QRF quantile calibration on validation predictions and apply to test predictions.")
    p.add_argument("--validation-predictions", required=True)
    p.add_argument("--test-predictions", required=True)
    p.add_argument("--outdir", required=True)
    p.add_argument("--quantiles", default="0.10,0.25,0.50,0.75,0.90")
    return p


def main() -> None:
    args = build_parser().parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    quantiles = _parse_quantiles(args.quantiles)

    val = pd.read_csv(args.validation_predictions, parse_dates=["eval_date", "target_date"])
    test = pd.read_csv(args.test_predictions, parse_dates=["eval_date", "target_date"])

    calibration = fit_additive_quantile_calibration(val, quantiles=quantiles)
    calibrated = apply_additive_quantile_calibration(test, calibration)

    cal_path = save_calibration(calibration, outdir / "quantile_calibration.json")
    pred_path = outdir / "predictions_calibrated.csv"
    calibrated.to_csv(pred_path, index=False)

    metrics = compute_qrf_density_diagnostics(calibrated, quantiles=quantiles)
    metrics_path = outdir / "scores_calibrated_quantile.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    print("Saved calibration to:", cal_path)
    print("Saved calibrated predictions to:", pred_path)
    print("Saved calibrated density scores to:", metrics_path)
    print(metrics)


if __name__ == "__main__":
    main()
