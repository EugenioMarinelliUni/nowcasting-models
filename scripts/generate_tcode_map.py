import json
import os

tcode_map = {
    **dict.fromkeys([
        'CES0600000007', 'COMPAPFFx', 'TB3SMFFM', 'TB6SMFFM', 'T1YFFM',
        'T5YFFM', 'T10YFFM', 'AAAFFM', 'BAAFFM', 'VIXCLSx', 'AWHMAN'
    ], 1),

    **dict.fromkeys([
        'CUMFNS', 'HWI', 'HWIURATIO', 'UNRATE', 'UEMPMEAN', 'CONSPI',
        'S&P div yield', 'FEDFUNDS', 'CP3Mx', 'TB3MS', 'TB6MS', 'GS1',
        'GS5', 'GS10', 'AAA', 'BAA', 'UMCSENTx', 'AWOTMAN', 'ISRATIOx'
    ], 2),

    **dict.fromkeys([], 3),  # Explicit empty group

    **dict.fromkeys([
        'HOUST', 'HOUSTNE', 'HOUSTMW', 'HOUSTS', 'HOUSTW',
        'PERMIT', 'PERMITNE', 'PERMITMW', 'PERMITS', 'PERMITW'
    ], 4),

    **dict.fromkeys([
        'RPI', 'W875RX1', 'DPCERA3M086SBEA', 'CMRMTSPLx', 'RETAILx',
        'INDPRO', 'IPFPNSS', 'IPFINAL', 'IPCONGD', 'IPDCONGD', 'IPNCONGD',
        'IPBUSEQ', 'IPMAT', 'IPDMAT', 'IPNMAT', 'IPMANSICS', 'IPB51222S',
        'IPFUELS', 'CLF16OV', 'CE16OV', 'UEMPLT5', 'UEMP5TO14', 'UEMP15OV',
        'UEMP15T26', 'UEMP27OV', 'CLAIMSx', 'PAYEMS', 'USGOOD',
        'CES1021000001', 'USCONS', 'MANEMP', 'DMANEMP', 'NDMANEMP', 'SRVPRD',
        'USTPU', 'USWTRADE', 'USTRADE', 'USFIRE', 'USGOVT', 'ACOGNO',
        'AMDMNOx', 'ANDENOx', 'AMDMUOx', 'BUSINVx', 'M2REAL', 'S&P 500',
        'S&P PE ratio', 'TWEXAFEGSMTHx', 'EXSZUSx', 'EXJPUSx', 'EXUSUKx',
        'EXCAUSx'
    ], 5),

    **dict.fromkeys([
        'M1SL', 'M2SL', 'BOGMBASE', 'TOTRESNS', 'BUSLOANS', 'REALLN',
        'NONREVSL', 'WPSFD49207', 'WPSFD49502', 'WPSID61', 'WPSID62',
        'OILPRICEx', 'PPICMM', 'CPIAUCSL', 'CPIAPPSL', 'CPITRNSL',
        'CPIMEDSL', 'CUSR0000SAC', 'CUSR0000SAD', 'CUSR0000SAS',
        'CPIULFSL', 'CUSR0000SA0L2', 'CUSR0000SA0L5', 'PCEPI',
        'DDURRG3M086SBEA', 'DNDGRG3M086SBEA', 'DSERRG3M086SBEA',
        'CES0600000008', 'CES2000000008', 'CES3000000008', 'DTCOLNVHFNM',
        'DTCTHFNM', 'INVEST'
    ], 6),

    **dict.fromkeys(['NONBORRES'], 7)
}

# Ensure the folder exists
output_dir = "../data/metadata"
os.makedirs(output_dir, exist_ok=True)

# Save the JSON
output_path = os.path.join(output_dir, "tcode_map.json")
with open(output_path, "w") as f:
    json.dump(tcode_map, f, indent=4)
