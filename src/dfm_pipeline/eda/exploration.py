import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

from dfm_pipeline.utils.data_loader import load_data


def summarize_data(df: pd.DataFrame):
    """Print basic summary statistics and structure."""
    print("Data shape:", df.shape)
    print("\nMissing values:\n", df.isnull().sum())
    print("\nData types:\n", df.dtypes)
    print("\nDescriptive stats:\n", df.describe())


def plot_correlation_heatmap(df: pd.DataFrame):
    """Plot a heatmap of variable correlations."""
    plt.figure(figsize=(12, 10))
    sns.heatmap(df.corr(), cmap="coolwarm", center=0)
    plt.title("Correlation Heatmap")
    plt.tight_layout()
    plt.show()


def plot_timeseries_sample(df: pd.DataFrame, num_vars: int = 6):
    """Plot a few time series for visual inspection."""
    df.iloc[:, :num_vars].plot(subplots=True, figsize=(10, 8), linewidth=1)
    plt.suptitle(f"First {num_vars} Variables Over Time")
    plt.tight_layout()
    plt.show()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    df, _ = load_data(stage="raw")  # Unpack the tuple but ignore transform_codes for now

    logging.info("Summarizing dataset...")
    summarize_data(df)

    logging.info("Plotting correlation heatmap...")
    plot_correlation_heatmap(df)

    logging.info("Plotting sample time series...")
    plot_timeseries_sample(df)


