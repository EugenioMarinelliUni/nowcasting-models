from statsmodels.tsa.seasonal import STL


def deseasonalize_stl(df, label):
    """
    Deseasonalize a monthly time series using STL decomposition.

    Args:
        df (pd.DataFrame): DataFrame with a datetime index and macroeconomic series.
        label (str): Column name of the series to deseasonalize.

    Returns:
        pd.Series: Deseasonalized series.
    """
    series = df[label].dropna()
    stl = STL(series, period=12, robust=True)
    result = stl.fit()
    return series - result.seasonal


from statsmodels.tsa.x13 import x13_arima_analysis
import shutil
import pandas as pd


def deseasonalize_x13(df: pd.DataFrame, label: str) -> pd.Series:
    """
    Deseasonalize a monthly time series using X-13ARIMA-SEATS.

    Args:
        df (pd.DataFrame): DataFrame with a datetime index and macroeconomic series.
        label (str): Column name of the series to deseasonalize.

    Returns:
        pd.Series: Deseasonalized series.

    Raises:
        EnvironmentError: If X-13ARIMA-SEATS binary is not found.
        ValueError: If seasonal adjustment fails.
    """
    if not shutil.which("x13as"):
        raise EnvironmentError("❌ X-13ARIMA-SEATS binary ('x13as') not found in PATH.")

    series = df[label].dropna()

    result = x13_arima_analysis(series)

    if hasattr(result, "seasadj"):
        return result.seasadj
    else:
        raise ValueError("❌ Seasonal adjustment failed. 'seasadj' not found in X-13 output.")

