import json
import os

variable_group_map = {
    **dict.fromkeys([
        'RPI', 'W875RX1', 'DPCERA3M086SBEA', 'CMRMTSPLx', 'RETAILx', 'INDPRO',
        'IPFPNSS', 'IPFINAL', 'IPCONGD', 'IPDCONGD', 'IPNCONGD', 'IPBUSEQ',
        'IPMAT', 'IPDMAT', 'IPNMAT', 'IPMANSICS', 'IPB51222S', 'IPFUELS',
        'CUMFNS'
    ], "Output & Income"),

    **dict.fromkeys([
        'HWI', 'HWIURATIO', 'CLF16OV', 'CE16OV', 'UNRATE', 'UEMPMEAN',
        'UEMPLT5', 'UEMP5TO14', 'UEMP15OV', 'UEMP15T26', 'UEMP27OV',
        'CLAIMSx', 'PAYEMS', 'USGOOD', 'CES1021000001', 'USCONS', 'MANEMP',
        'DMANEMP', 'NDMANEMP', 'SRVPRD', 'USTPU', 'USWTRADE', 'USTRADE',
        'USFIRE', 'USGOVT', 'CES0600000007', 'AWOTMAN', 'AWHMAN'
    ], "Labor Market"),

    **dict.fromkeys([
        'HOUST', 'HOUSTNE', 'HOUSTMW', 'HOUSTS', 'HOUSTW',
        'PERMIT', 'PERMITNE', 'PERMITMW', 'PERMITS', 'PERMITW'
    ], "Housing"),

    **dict.fromkeys([
        'ACOGNO', 'AMDMNOx', 'ANDENOx', 'AMDMUOx', 'BUSINVx', 'ISRATIOx'
    ], "Orders & Inventories"),

    **dict.fromkeys([
        'M1SL', 'M2SL', 'M2REAL', 'BOGMBASE', 'TOTRESNS', 'NONBORRES',
        'BUSLOANS', 'REALLN', 'NONREVSL', 'CONSPI'
    ], "Money & Credit"),

    **dict.fromkeys([
        'S&P 500', 'S&P div yield', 'S&P PE ratio'
    ], "Stock Market"),

    **dict.fromkeys([
        'FEDFUNDS', 'CP3Mx', 'TB3MS', 'TB6MS', 'GS1', 'GS5', 'GS10',
        'AAA', 'BAA', 'COMPAPFFx', 'TB3SMFFM', 'TB6SMFFM', 'T1YFFM',
        'T5YFFM', 'T10YFFM', 'AAAFFM', 'BAAFFM'
    ], "Interest & Exchange Rates"),

    **dict.fromkeys([
        'TWEXAFEGSMTHx', 'EXSZUSx', 'EXJPUSx', 'EXUSUKx', 'EXCAUSx'
    ], "Exchange Rates"),

    **dict.fromkeys([
        'WPSFD49207', 'WPSFD49502', 'WPSID61', 'WPSID62', 'OILPRICEx',
        'PPICMM', 'CPIAUCSL', 'CPIAPPSL', 'CPITRNSL', 'CPIMEDSL',
        'CUSR0000SAC', 'CUSR0000SAD', 'CUSR0000SAS', 'CPIULFSL',
        'CUSR0000SA0L2', 'CUSR0000SA0L5', 'PCEPI'
    ], "Prices"),

    **dict.fromkeys([
        'DDURRG3M086SBEA', 'DNDGRG3M086SBEA', 'DSERRG3M086SBEA',
        'CES0600000008', 'CES2000000008', 'CES3000000008'
    ], "Consumption & Investment"),

    **dict.fromkeys([
        'UMCSENTx', 'DTCOLNVHFNM', 'DTCTHFNM', 'INVEST', 'VIXCLSx'
    ], "Expectations")
}

# Save to ../data/metadata/variable_group_map.json
output_dir = "../data/metadata"
os.makedirs(output_dir, exist_ok=True)

output_path = os.path.join(output_dir, "variable_group_map.json")
with open(output_path, "w") as f:
    json.dump(variable_group_map, f, indent=4)

print(f"✅ variable_group_map saved to: {output_path}")


