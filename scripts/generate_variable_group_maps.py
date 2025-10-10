#!/usr/bin/env python3
"""
Generate McCracken–Ng style variable group maps.

Outputs:
  - data/metadata/variable_group_map_1960_noVIX.json
  - data/metadata/variable_group_map_1965_withVIX.json
"""

from pathlib import Path
import json

# --- Resolve project paths robustly ---
# scripts/generate_variable_group_maps.py -> project_root = parent of 'scripts'
PROJECT_ROOT = Path(__file__).resolve().parents[1]
META_DIR = PROJECT_ROOT / "data" / "metadata"
META_DIR.mkdir(parents=True, exist_ok=True)

def make_map(include_vix: bool):
    return {
        # Output & Income
        **dict.fromkeys([
            "INDPRO","IPFINAL","IPMANSICS","IPCONGD","IPDCONGD","IPNCONGD",
            "IPMAT","IPDMAT","IPNMAT","IPBUSEQ","IPB51222S","IPFPNSS","IPFUELS",
            "CUMFNS","RPI"
        ], "Output & Income"),
        # Labor Market
        **dict.fromkeys([
            "HWI","HWIURATIO","CLF16OV","CE16OV","UNRATE","UEMPMEAN","UEMPLT5",
            "UEMP5TO14","UEMP15OV","UEMP15T26","UEMP27OV","CLAIMSx","PAYEMS",
            "USGOOD","USCONS","USFIRE","USGOVT","USTPU","USTRADE","USWTRADE",
            "MANEMP","DMANEMP","NDMANEMP","SRVPRD","CES1021000001",
            "CES0600000007","CES0600000008","CES2000000008","CES3000000008",
            "AWHMAN","AWOTMAN"
        ], "Labor Market"),
        # Housing
        **dict.fromkeys([
            "HOUST","HOUSTNE","HOUSTMW","HOUSTS","HOUSTW",
            "PERMIT","PERMITNE","PERMITMW","PERMITS","PERMITW","CONSPI"
        ], "Housing"),
        # Consumption
        **dict.fromkeys([
            "W875RX1","RETAILx","DPCERA3M086SBEA","DDURRG3M086SBEA",
            "DNDGRG3M086SBEA","DSERRG3M086SBEA"
        ], "Consumption"),
        # Orders & Inventories
        **dict.fromkeys([
            "AMDMNOx","AMDMUOx","BUSINVx","ISRATIOx","CMRMTSPLx","INVEST"
        ], "Orders & Inventories"),
        # Money & Credit
        **dict.fromkeys([
            "M1SL","M2SL","M2REAL","BOGMBASE","TOTRESNS","NONBORRES",
            "BUSLOANS","REALLN","NONREVSL","DTCOLNVHFNM","DTCTHFNM"
        ], "Money & Credit"),
        # Interest & Exchange Rates (incl. FX)
        **dict.fromkeys([
            "FEDFUNDS","CP3Mx","TB3MS","TB6MS","GS1","GS5","GS10",
            "AAA","BAA","COMPAPFFx","TB3SMFFM","TB6SMFFM","T1YFFM","T5YFFM","T10YFFM",
            "AAAFFM","BAAFFM","EXCAUSx","EXJPUSx","EXSZUSx","EXUSUKx"
        ], "Interest & Exchange Rates"),
        # Prices
        **dict.fromkeys([
            "CPIAUCSL","CPIAPPSL","CPIMEDSL","CPITRNSL","CPIULFSL",
            "CUSR0000SAC","CUSR0000SAD","CUSR0000SAS","CUSR0000SA0L2","CUSR0000SA0L5",
            "PCEPI","PPICMM","WPSFD49207","WPSFD49502","WPSID61","WPSID62","OILPRICEx"
        ], "Prices"),
        # Stock Market
        **dict.fromkeys(
            ["S&P 500","S&P div yield","S&P PE ratio"] + (["VIXCLSx"] if include_vix else []),
            "Stock Market"
        ),
    }

def main():
    map_1960 = make_map(include_vix=False)   # starts 1960
    map_1965 = make_map(include_vix=True)    # starts 1965 (includes VIX)

    out_a = META_DIR / "variable_group_map_1960_noVIX.json"
    out_b = META_DIR / "variable_group_map_1965_withVIX.json"

    with out_a.open("w", encoding="utf-8") as f:
        json.dump(map_1960, f, indent=4, ensure_ascii=False)
    with out_b.open("w", encoding="utf-8") as f:
        json.dump(map_1965, f, indent=4, ensure_ascii=False)

    print(f" Wrote:\n  - {out_a}\n  - {out_b}")

if __name__ == "__main__":
    main()
