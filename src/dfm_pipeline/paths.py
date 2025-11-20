# src/dfm_pipeline/paths.py
from __future__ import annotations
from pathlib import Path


# ---------- Generic helpers ----------

VARIANTS_ROOT = Path("data/metadata/variants")


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


# ---------- Preselection artifacts ----------

def preselection_json(panel: str, tag: str, method: str, label: str | None) -> Path:
    """
    Matches your current layout:
      data/metadata/variants/{panel}__{tag}__preselect_{method}__{label}.json
    If label is None, the suffix is omitted.
    """
    suffix = f"__{label}" if label else ""
    return VARIANTS_ROOT / f"{panel}__{tag}__preselect_{method}{suffix}.json"


def preselected_X_csv(panel: str, tag: str, method: str, label: str | None) -> Path:
    """
    Default dataset CSV produced by your preselection step (adjust if needed):
      dataset/{panel}/preselect/{method}/X_panel_z__{panel}__{tag}__preselect-{method}__{label}.csv
    """
    suffix = f"__{label}" if label else ""
    return Path("dataset") / panel / "preselect" / method / f"X_panel_z__{panel}__{tag}__preselect-{method}{suffix}.csv"


# ---------- Bai–Ng artifacts ----------

def baing_out_dir(panel: str, tag: str, method: str, label: str) -> Path:
    """
    New nested layout:
      data/metadata/variants/{panel}/{tag}/factors/baing/{method}/{label}/
    """
    return ensure_dir(VARIANTS_ROOT / panel / tag / "factors" / "baing" / method / label)


def baing_base(panel: str, tag: str, method: str, label: str) -> str:
    return f"{panel}__{tag}__baing__{method}__{label}"


def baing_artifacts(panel: str, tag: str, method: str, label: str) -> dict[str, Path]:
    out_dir = baing_out_dir(panel, tag, method, label)
    base = baing_base(panel, tag, method, label)
    return {
        "dir": out_dir,
        "grid_csv": out_dir / f"{base}.grid.csv",
        "summary_json": out_dir / f"{base}.json",
        "factors_csv": out_dir / f"{base}.factors.csv",
        "loadings_csv": out_dir / f"{base}.loadings.csv",
        "eigen_csv": out_dir / f"{base}.eigen.csv",
    }
