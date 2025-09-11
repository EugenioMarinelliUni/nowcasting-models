# grouping_series.py
"""
Utility module for variable group mapping and related helper functions
based on the FRED-MD economic categories from McCracken & Ng (2016).
"""

VARIABLE_GROUP_MAP = {
    # Group 1: Output and Income
    "RPI": "Output and Income",
    "W875RX1": "Output and Income",
    "INDPRO": "Output and Income",
    "IPFPNSS": "Output and Income",
    "IPFINAL": "Output and Income",
    "IPCONGD": "Output and Income",
    "IPDCONGD": "Output and Income",
    "IPNCONGD": "Output and Income",
    "IPBUSEQ": "Output and Income",
    "IPMAT": "Output and Income",
    "IPDMAT": "Output and Income",
    "IPNMAT": "Output and Income",
    "IPMANSICS": "Output and Income",
    "IPB51222S": "Output and Income",
    "IPFUELS": "Output and Income",
    "NAPMPI": "Output and Income",
    "CUMFNS": "Output and Income",

    # Group 2: Labor Market
    "HWI": "Labor Market",
    "HWIURATIO": "Labor Market",
    "CLF16OV": "Labor Market",
    "CE16OV": "Labor Market",
    "UNRATE": "Labor Market",
    "UEMPMEAN": "Labor Market",
    "UEMPLT5": "Labor Market",
    "UEMP5TO14": "Labor Market",
    "UEMP15OV": "Labor Market",
    "UEMP15T26": "Labor Market",
    "UEMP27OV": "Labor Market",
    "CLAIMSx": "Labor Market",
    "PAYEMS": "Labor Market",
    "USGOOD": "Labor Market",
    "CES1021000001": "Labor Market",
    "USCONS": "Labor Market",
    "MANEMP": "Labor Market",
    "DMANEMP": "Labor Market",
    "NDMANEMP": "Labor Market",
    "SRVPRD": "Labor Market",
    "USTPU": "Labor Market",
    "USWTRADE": "Labor Market",
    "USTRADE": "Labor Market",
    "USFIRE": "Labor Market",
    "USGOVT": "Labor Market",
    "CES0600000007": "Labor Market",
    "AWOTMAN": "Labor Market",
    "AWHMAN": "Labor Market",
    "NAPMEI": "Labor Market",
    "CES0600000008": "Labor Market",
    "CES2000000008": "Labor Market",
    "CES3000000008": "Labor Market",

    # Group 3: Consumption and Orders
    "HOUST": "Consumption and Orders",
    "HOUSTNE": "Consumption and Orders",
    "HOUSTMW": "Consumption and Orders",
    "HOUSTS": "Consumption and Orders",
    "HOUSTW": "Consumption and Orders",
    "PERMIT": "Consumption and Orders",
    "PERMITNE": "Consumption and Orders",
    "PERMITMW": "Consumption and Orders",
    "PERMITS": "Consumption and Orders",
    "PERMITW": "Consumption and Orders",

    # Group 4: Orders and Inventories
    "DPCERA3M086SBEA": "Orders and Inventories",
    "CMRMTSPLx": "Orders and Inventories",
    "RETAILx": "Orders and Inventories",
    "NAPM": "Orders and Inventories",
    "NAPMNOI": "Orders and Inventories",
    "NAPMSDI": "Orders and Inventories",
    "NAPMII": "Orders and Inventories",
    "ACOGNO": "Orders and Inventories",
    "AMDMNOx": "Orders and Inventories",
    "ANDENOx": "Orders and Inventories",
    "AMDMUOx": "Orders and Inventories",
    "BUSINVx": "Orders and Inventories",
    "ISRATIOx": "Orders and Inventories",
    "UMCSENTx": "Orders and Inventories",

    # Group 5: Money and Credit
    "M1SL": "Money and Credit",
    "M2SL": "Money and Credit",
    "M2REAL": "Money and Credit",
    "AMBSL": "Money and Credit",
    "TOTRESNS": "Money and Credit",
    "NONBORRES": "Money and Credit",
    "BUSLOANS": "Money and Credit",
    "REALLN": "Money and Credit",
    "NONREVSL": "Money and Credit",
    "CONSPI": "Money and Credit",
    "MZMSL": "Money and Credit",
    "DTCOLNVHFNM": "Money and Credit",
    "DTCTHFNM": "Money and Credit",
    "INVEST": "Money and Credit",
    "BOGMBASE": "Money and Credit",

    # Group 6: Interest Rate and Exchange Rates
    "FEDFUNDS": "Interest Rate and Exchange Rates",
    "CP3Mx": "Interest Rate and Exchange Rates",
    "TB3MS": "Interest Rate and Exchange Rates",
    "TB6MS": "Interest Rate and Exchange Rates",
    "GS1": "Interest Rate and Exchange Rates",
    "GS5": "Interest Rate and Exchange Rates",
    "GS10": "Interest Rate and Exchange Rates",
    "AAA": "Interest Rate and Exchange Rates",
    "BAA": "Interest Rate and Exchange Rates",
    "COMPAPFFx": "Interest Rate and Exchange Rates",
    "TB3SMFFM": "Interest Rate and Exchange Rates",
    "TB6SMFFM": "Interest Rate and Exchange Rates",
    "T1YFFM": "Interest Rate and Exchange Rates",
    "T5YFFM": "Interest Rate and Exchange Rates",
    "T10YFFM": "Interest Rate and Exchange Rates",
    "AAAFFM": "Interest Rate and Exchange Rates",
    "BAAFFM": "Interest Rate and Exchange Rates",
    "TWEXMMTH": "Interest Rate and Exchange Rates",
    "EXSZUSx": "Interest Rate and Exchange Rates",
    "EXJPUSx": "Interest Rate and Exchange Rates",
    "EXUSUKx": "Interest Rate and Exchange Rates",
    "EXCAUSx": "Interest Rate and Exchange Rates",
    "TWEXAFEGSMTHx": "Interest Rate and Exchange Rates",

    # Group 7: Prices
    "PPIIFGS": "Prices",
    "PPIFCG": "Prices",
    "PPIITM": "Prices",
    "PPICRM": "Prices",
    "OILPRICEx": "Prices",
    "PPICMM": "Prices",
    "NAPMPRI": "Prices",
    "CPIAUCSL": "Prices",
    "CPIAPPSL": "Prices",
    "CPITRNSL": "Prices",
    "CPIMEDSL": "Prices",
    "CUSR0000SAC": "Prices",
    "CUSR0000SAD": "Prices",
    "CUSR0000SAS": "Prices",
    "CPIULFSL": "Prices",
    "CUUR0000SA0L2": "Prices",
    "CUSR0000SA0L5": "Prices",
    "PCEPI": "Prices",
    "DDURRG3M086SBEA": "Prices",
    "DNDGRG3M086SBEA": "Prices",
    "DSERRG3M086SBEA": "Prices",
    "CUSR0000SA0L2": "Prices",
    "WPSID61": "Prices",
    "WPSID62": "Prices",
    "WPSFD49502": "Prices",
    "WPSFD49207": "Prices",

    # Group 8: Stock Market
    "S&P 500": "Stock Market",
    "S&P: indust": "Stock Market",
    "S&P div yield": "Stock Market",
    "S&P PE ratio": "Stock Market",
    "VIXCLSx": "Stock Market",
}

import pandas as pd

def assign_variable_groups(df: pd.DataFrame) -> pd.Series:
    """
    Assign group labels to each column in the DataFrame using VARIABLE_GROUP_MAP.

    Args:
        df (pd.DataFrame): DataFrame containing macroeconomic time series.

    Returns:
        pd.Series: Series where the index is variable names and the value is the group.
    """
    return df.columns.to_series().map(VARIABLE_GROUP_MAP)


def filter_by_group(df: pd.DataFrame, group_name: str) -> pd.DataFrame:
    """
    Filter the DataFrame to include only columns in a specific economic group.

    Args:
        df (pd.DataFrame): The full dataset.
        group_name (str): One of the categories in VARIABLE_GROUP_MAP.

    Returns:
        pd.DataFrame: A subset of the original DataFrame with only columns in that group.
    """
    group_series = assign_variable_groups(df)
    selected_vars = group_series[group_series == group_name].index
    return df[selected_vars]


def create_metadata_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a metadata DataFrame showing each variable and its assigned group.

    Args:
        df (pd.DataFrame): The dataset with variables as columns.

    Returns:
        pd.DataFrame: Metadata DataFrame with variable name and group.
    """
    return pd.DataFrame({
        "variable": df.columns,
        "group": df.columns.map(VARIABLE_GROUP_MAP)
    })
