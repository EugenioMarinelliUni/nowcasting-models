import json
import os

variable_group_map = {
    # --- Output & Income ---
    **dict.fromkeys([
        'INDPRO', 'IPFINAL', 'IPMANSICS',
        'IPCONGD', 'IPDCONGD', 'IPNCONGD',
        'IPMAT', 'IPDMAT', 'IPNMAT', 'IPBUSEQ',
        'IPB51222S', 'IPFPNSS', 'IPFUELS',
        'CUMFNS'
    ], "Output & Income"),

    # --- Labor Market ---
    **dict.fromkeys([
        'HWI', 'HWIURATIO',
        'CLF16OV', 'CE16OV',
        'UNRATE', 'UEMPMEAN', 'UEMPLT5', 'UEMP5TO14', 'UEMP15OV', 'UEMP15T26', 'UEMP27OV',
        'CLAIMSx',
        'PAYEMS', 'USGOOD', 'USCONS', 'USFIRE', 'USGOVT', 'USTPU', 'USTRADE', 'USWTRADE',
        'MANEMP', 'DMANEMP', 'NDMANEMP',
        'SRVPRD',
        'CES1021000001',          # Manufacturing employment (subset)
        'CES0600000007',          # Avg hours (nonsupervisory) – labor
        'CES0600000008', 'CES2000000008', 'CES3000000008',  # avg weekly hours/earnings variants
        'AWHMAN', 'AWOTMAN'
    ], "Labor Market"),

    # --- Housing ---
    **dict.fromkeys([
        'HOUST', 'HOUSTNE', 'HOUSTMW', 'HOUSTS', 'HOUSTW',
        'PERMIT', 'PERMITNE', 'PERMITMW', 'PERMITS', 'PERMITW',
        'CONSPI'                   # Construction spending, private → housing/construction
    ], "Housing"),

    # --- Consumption ---
    **dict.fromkeys([
        'RPI', 'W875RX1',          # Real personal income (ex transfers), real retail control
        'RETAILx',                 # Real retail sales
        'DPCERA3M086SBEA',         # Real PCE (alt BEA monthly)
        'DDURRG3M086SBEA',         # Real PCE Durables
        'DNDGRG3M086SBEA',         # Real PCE Nondurables
        'DSERRG3M086SBEA',         # Real PCE Services
        'CMRMTSPLx'                # Mfg & trade sales (often grouped with consumption activity)
    ], "Consumption"),

    # --- Orders & Inventories ---
    **dict.fromkeys([
        'AMDMNOx',                 # Mfg new orders: durables
        'AMDMUOx',                 # Unfilled orders
        'BUSINVx',                 # Business inventories
        'ISRATIOx',                # Inventories-to-sales ratio
        'INVEST'                   # Investment proxy (kept here; adjust if you use a different convention)
    ], "Orders & Inventories"),

    # --- Money & Credit ---
    **dict.fromkeys([
        'M1SL', 'M2SL', 'M2REAL',
        'BOGMBASE', 'TOTRESNS', 'NONBORRES',
        'BUSLOANS', 'REALLN', 'NONREVSL',
        'DTCOLNVHFNM', 'DTCTHFNM'  # consumer credit measures
    ], "Money & Credit"),

    # --- Interest & Exchange Rates ---
    **dict.fromkeys([
        # Policy/short rates & money market
        'FEDFUNDS', 'CP3Mx', 'TB3MS', 'TB6MS',
        # Treasury yields
        'GS1', 'GS5', 'GS10',
        # Corporate yields
        'AAA', 'BAA',
        # Spreads vs fed funds / term spreads
        'COMPAPFFx', 'TB3SMFFM', 'TB6SMFFM', 'T1YFFM', 'T5YFFM', 'T10YFFM',
        'AAAFFM', 'BAAFFM',
        # Exchange rates (merged per FRED-MD convention)
        'EXCAUSx', 'EXJPUSx', 'EXSZUSx', 'EXUSUKx'
    ], "Interest & Exchange Rates"),

    # --- Prices ---
    **dict.fromkeys([
        'CPIAUCSL', 'CPIAPPSL', 'CPIMEDSL', 'CPITRNSL', 'CPIULFSL',
        'CUSR0000SAC', 'CUSR0000SAD', 'CUSR0000SAS',
        'CUSR0000SA0L2', 'CUSR0000SA0L5',
        'PCEPI',
        'PPICMM', 'WPSFD49207', 'WPSFD49502', 'WPSID61', 'WPSID62',
        'OILPRICEx'               # commodity price (oil) sits with prices in FRED-MD releases
    ], "Prices"),

    # --- Stock Market ---
    **dict.fromkeys([
        'S&P 500', 'S&P div yield', 'S&P PE ratio'
    ], "Stock Market"),
}

# Save to ../data/metadata/variable_group_map.json
output_dir = "../data/metadata"
os.makedirs(output_dir, exist_ok=True)

output_path = os.path.join(output_dir, "variable_group_map.json")
with open(output_path, "w") as f:
    json.dump(variable_group_map, f, indent=4)

print(f" variable_group_map saved to: {output_path}")


