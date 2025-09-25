# src/dfm_pipeline/validation/stationarity.py
from __future__ import annotations

from typing import Optional, Dict, Any, Literal
import warnings

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller, kpss

# Optional tests via 'arch' (Phillips–Perron, DF-GLS, Zivot–Andrews)
try:
    from arch.unitroot import PhillipsPerron, DFGLS, ZivotAndrews  # type: ignore[import-not-found]
    _HAVE_ARCH = True
except Exception:
    _HAVE_ARCH = False


__all__ = [
    "run_stationarity_tests_on_series",
    "run_stationarity_tests_on_panel",
]


# -------------------------
# Helpers
# -------------------------

def _clean_numeric_series(s: pd.Series) -> pd.Series:
    """Coerce to numeric and drop NaNs."""
    return pd.to_numeric(s, errors="coerce").dropna()


def _final_decision(adf_p: float, kpss_p: float, alpha: float) -> str:
    """
    Combine ADF (H0: unit root) and KPSS (H0: stationarity) into a single decision.

    Rules:
      - 'stationary'      if ADF rejects (p<alpha) AND KPSS does NOT reject (p>=alpha)
      - 'non-stationary'  if ADF does NOT reject AND KPSS rejects
      - 'inconclusive'    otherwise (or if one of the tests failed)
    """
    if np.isnan(adf_p) or np.isnan(kpss_p):
        return "inconclusive"
    adf_reject = adf_p < alpha
    kpss_reject = kpss_p < alpha
    if adf_reject and not kpss_reject:
        return "stationary"
    if (not adf_reject) and kpss_reject:
        return "non-stationary"
    return "inconclusive"


# -------------------------
# Single-series interface
# -------------------------

def run_stationarity_tests_on_series(
    s: pd.Series,
    *,
    adf_autolag: Optional[str] = "AIC",
    kpss_reg: Literal["c", "ct"] = "c",           # "c" (level) or "ct" (trend)
    kpss_nlags: str | int = "auto",
    run_pp: bool = False,
    run_dfgls: bool = False,
    run_za: bool = False,
    alpha: float = 0.05,
) -> pd.Series:
    """
    Run ADF + KPSS (and optional PP, DF-GLS, ZA) on a single time series and
    return a one-row summary (as a pandas Series).

    Returns keys:
      adf_pvalue, kpss_pvalue, pp_pvalue, dfgls_pvalue, za_pvalue, za_stat,
      za_break_index, kpss_reg, n_non_na, decision
    """
    x = _clean_numeric_series(s)
    out: Dict[str, Any] = {
        "adf_pvalue": np.nan,
        "kpss_pvalue": np.nan,
        "pp_pvalue": np.nan,
        "dfgls_pvalue": np.nan,
        "za_pvalue": np.nan,
        "za_stat": np.nan,
        "za_break_index": np.nan,
        "kpss_reg": kpss_reg,
        "n_non_na": int(x.shape[0]),
    }

    if x.empty:
        out["decision"] = "inconclusive"
        return pd.Series(out)

    # ADF (H0: unit root)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _stat, adf_p, *_ = adfuller(x, autolag=adf_autolag)
        out["adf_pvalue"] = float(adf_p)
    except Exception:
        pass

    # KPSS (H0: stationarity)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _stat, kpss_p, _lags, *_ = kpss(x, regression=kpss_reg, nlags=kpss_nlags)
        out["kpss_pvalue"] = float(kpss_p)
    except Exception:
        pass

    # Optional: Phillips–Perron
    if run_pp and _HAVE_ARCH:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                out["pp_pvalue"] = float(PhillipsPerron(x).pvalue)
        except Exception:
            pass

    # Optional: DF-GLS
    if run_dfgls and _HAVE_ARCH:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                out["dfgls_pvalue"] = float(DFGLS(x).pvalue)
        except Exception:
            pass

    # Optional: Zivot–Andrews (endogenous single break)
    if run_za and _HAVE_ARCH:
        pvals: list[float] = []
        stats: list[float] = []
        breaks: list[Any] = []
        for trend in ("c", "t", "ct"):  # type: Literal["c","t","ct"]
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    za = ZivotAndrews(x, trend=trend)  # type: ignore[name-defined]
                pvals.append(float(za.pvalue))  # type: ignore[attr-defined]
                stats.append(float(za.stat))    # type: ignore[attr-defined]
                # Some type checkers don't know 'breakpoint'; use getattr to be safe
                bi = int(getattr(za, "breakpoint", -1))
                if 0 <= bi < len(x.index):
                    bts = x.index[bi]
                else:
                    bts = np.nan
                breaks.append(bts)
            except Exception:
                continue
        if pvals:
            # report the most conservative (max p-value)
            i = int(np.argmax(pvals))
            out["za_pvalue"] = pvals[i]
            out["za_stat"] = stats[i]
            out["za_break_index"] = breaks[i] if isinstance(breaks[i], pd.Timestamp) else np.nan

    # Final decision from ADF + KPSS
    out["decision"] = _final_decision(out["adf_pvalue"], out["kpss_pvalue"], alpha)
    return pd.Series(out)


# -------------------------
# Panel (DataFrame) interface
# -------------------------

def run_stationarity_tests_on_panel(
    df: pd.DataFrame,
    *,
    adf_autolag: Optional[str] = "AIC",
    kpss_reg: Literal["c", "ct"] = "ct",         # default used in your panel scripts
    kpss_nlags: str | int = "auto",
    run_pp: bool = False,
    run_dfgls: bool = False,
    run_za: bool = False,
    alpha: float = 0.05,
) -> pd.DataFrame:
    """
    Apply `run_stationarity_tests_on_series` to each column of a panel.

    Returns a DataFrame indexed by series names with the same columns as the
    single-series output plus the final 'decision'.
    """
    results: Dict[str, pd.Series] = {}

    for col in df.columns:
        s = df[col]
        res = run_stationarity_tests_on_series(
            s,
            adf_autolag=adf_autolag,
            kpss_reg=kpss_reg,
            kpss_nlags=kpss_nlags,
            run_pp=run_pp,
            run_dfgls=run_dfgls,
            run_za=run_za,
            alpha=alpha,
        )
        results[col] = res

    out = pd.DataFrame(results).T
    out.index.name = "series"
    # Order columns nicely if present
    preferred = [
        "adf_pvalue",
        "kpss_pvalue",
        "pp_pvalue",
        "dfgls_pvalue",
        "za_pvalue",
        "za_stat",
        "za_break_index",
        "kpss_reg",
        "n_non_na",
        "decision",
    ]
    return out[[c for c in preferred if c in out.columns] + [c for c in out.columns if c not in preferred]]


