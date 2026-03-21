"""Features component: engineer features from clean data for ML/analytics."""

import logging
import time
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd

from kgs_pipeline.config import FEATURES_DIR, PROCESSED_DATA_DIR

logger = logging.getLogger(__name__)


def load_processed_data(processed_dir: Path = PROCESSED_DATA_DIR) -> dd.DataFrame:
    """
    Load processed Parquet data as Dask DataFrame.

    Parameters
    ----------
    processed_dir : Path
        Directory containing processed Parquet files.

    Returns
    -------
    dask.dataframe.DataFrame
        Lazy Dask DataFrame.

    Raises
    ------
    FileNotFoundError
        If directory does not exist or contains no Parquet files.
    """
    if not processed_dir.exists():
        raise FileNotFoundError(f"Processed directory not found: {processed_dir}")

    try:
        ddf = dd.read_parquet(str(processed_dir), engine="pyarrow")
    except (ValueError, FileNotFoundError) as e:
        raise FileNotFoundError(
            f"No Parquet files found in {processed_dir}"
        ) from e

    logger.info(f"Loaded processed data from {processed_dir}")
    return ddf


def filter_2020_2025(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Filter to records from 2020-01-01 to 2025-12-31.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Filtered Dask DataFrame.

    Raises
    ------
    KeyError
        If production_date column not found.
    """
    if "production_date" not in ddf.columns:
        raise KeyError("production_date column not found")

    def filter_dates(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
        return df[
            (df["production_date"] >= "2020-01-01")
            & (df["production_date"] <= "2025-12-31")
        ]

    ddf = ddf.map_partitions(filter_dates)
    logger.info("Filtered to 2020-2025")
    return ddf


def aggregate_by_well_month(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Aggregate production by well, month, and product.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame (one row per well-date-product).

    Returns
    -------
    dask.dataframe.DataFrame
        Aggregated Dask DataFrame with _month suffix.

    Raises
    ------
    KeyError
        If required columns not found.
    """
    required = ["well_id", "production_date", "product", "production"]
    missing = [col for col in required if col not in ddf.columns]
    if missing:
        raise KeyError(f"Missing columns: {missing}")

    def agg_partition(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
        # Create year_month for grouping
        df["year_month"] = df["production_date"].dt.to_period("M")

        agg_dict = {
            "production": ["sum", "mean", "std", "count"],
            "latitude": "first",
            "longitude": "first",
            "operator": "first",
            "field_name": "first",
            "lease_name": "first",
        }

        # Build grouped aggregation
        grouped = df.groupby(["well_id", "year_month", "product"]).agg(agg_dict)

        # Flatten column names
        grouped.columns = [
            f"{col[0]}_{col[1]}_month" if col[1] else col[0]
            for col in grouped.columns
        ]

        return grouped.reset_index()

    ddf = ddf.map_partitions(agg_partition)
    logger.info("Aggregated by well-month")
    return ddf


def rolling_averages(ddf: dd.DataFrame, window: int = 12) -> dd.DataFrame:
    """
    Compute rolling averages (per well, per product).

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame (after well-month aggregation).
    window : int
        Rolling window size (months).

    Returns
    -------
    dask.dataframe.DataFrame
        Dask DataFrame with rolling_avg_{N}mo columns.

    Raises
    ------
    KeyError
        If production_sum_month column not found.
    """
    if "production_sum_month" not in ddf.columns:
        raise KeyError("production_sum_month column not found")

    def rolling_partition(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
        # Must sort by well and date first
        df = df.sort_values(by=["well_id", "year_month", "product"])

        # Group by well and product
        def compute_rolling(group: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
            group[f"rolling_avg_{window}mo"] = (
                group["production_sum_month"].rolling(window=window, min_periods=1).mean()
            )
            return group

        return df.groupby(["well_id", "product"], group_keys=False).apply(
            compute_rolling
        )

    ddf = ddf.map_partitions(rolling_partition)
    logger.info(f"Computed {window}-month rolling average")
    return ddf


def cumulative_production(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute cumulative production per well per product.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame (after well-month aggregation).

    Returns
    -------
    dask.dataframe.DataFrame
        Dask DataFrame with cumulative_production column.

    Raises
    ------
    KeyError
        If production_sum_month column not found.
    """
    if "production_sum_month" not in ddf.columns:
        raise KeyError("production_sum_month column not found")

    def cum_prod_partition(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
        df = df.sort_values(by=["well_id", "year_month", "product"])

        def compute_cumsum(group: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
            group["cumulative_production"] = group["production_sum_month"].cumsum()
            return group

        return df.groupby(["well_id", "product"], group_keys=False).apply(
            compute_cumsum
        )

    ddf = ddf.map_partitions(cum_prod_partition)
    logger.info("Computed cumulative production")
    return ddf


def production_trend(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Add production trend indicator (increasing/decreasing/stable).

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Dask DataFrame with production_trend column.

    Raises
    ------
    KeyError
        If rolling_avg_12mo column not found.
    """
    if "rolling_avg_12mo" not in ddf.columns:
        raise KeyError("rolling_avg_12mo column not found")

    def trend_partition(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
        def compute_trend(group: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
            # Compare last vs first non-null rolling avg
            valid_idx = group["rolling_avg_12mo"].notna()
            if valid_idx.sum() < 2:
                group["production_trend"] = "insufficient_data"
            else:
                first_valid = group.loc[valid_idx, "rolling_avg_12mo"].iloc[0]
                last_valid = group.loc[valid_idx, "rolling_avg_12mo"].iloc[-1]

                pct_change = (last_valid - first_valid) / first_valid * 100

                if pct_change > 5:
                    trend = "increasing"
                elif pct_change < -5:
                    trend = "decreasing"
                else:
                    trend = "stable"

                group["production_trend"] = trend
            return group

        return df.groupby(["well_id", "product"], group_keys=False).apply(
            compute_trend
        )

    ddf = ddf.map_partitions(trend_partition)
    logger.info("Computed production trend")
    return ddf


def compute_well_lifetime(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute months of production per well per product.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Dask DataFrame with well_lifetime_months column.

    Raises
    ------
    KeyError
        If year_month column not found.
    """
    if "year_month" not in ddf.columns:
        raise KeyError("year_month column not found")

    def lifetime_partition(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
        def compute_lifetime(group: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
            if len(group) > 0:
                group["well_lifetime_months"] = len(group)
            else:
                group["well_lifetime_months"] = 0
            return group

        return df.groupby(["well_id", "product"], group_keys=False).apply(
            compute_lifetime
        )

    ddf = ddf.map_partitions(lifetime_partition)
    logger.info("Computed well lifetime")
    return ddf


def standardize_numerics(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Standardize numeric columns (z-score normalization).

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Dask DataFrame with standardized columns.
    """
    numeric_cols = [
        "production_sum_month",
        "rolling_avg_12mo",
        "cumulative_production",
        "well_lifetime_months",
    ]

    def standardize_partition(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[type-arg]
        for col in numeric_cols:
            if col in df.columns and df[col].dtype in ["float64", "int64"]:
                mean = df[col].mean()
                std = df[col].std()
                if std > 0:
                    df[col + "_zscore"] = (df[col] - mean) / std
                else:
                    df[col + "_zscore"] = 0
        return df

    ddf = ddf.map_partitions(standardize_partition)
    logger.info("Standardized numeric features")
    return ddf


def write_features_parquet(
    ddf: dd.DataFrame, features_dir: Path = FEATURES_DIR
) -> None:
    """
    Write feature engineering results as Parquet.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Feature DataFrame.
    features_dir : Path
        Output directory.

    Raises
    ------
    TypeError
        If ddf is not a Dask DataFrame.
    OSError
        If write fails.
    """
    if not isinstance(ddf, dd.DataFrame):
        raise TypeError("Expected a dask DataFrame")

    features_dir.mkdir(parents=True, exist_ok=True)

    try:
        ddf.to_parquet(
            str(features_dir),
            write_index=False,
            overwrite=True,
            engine="pyarrow",
        )
        logger.info(f"Wrote feature Parquet to {features_dir}")
    except OSError as e:
        raise OSError(f"Failed to write feature Parquet: {e}") from e


def run_features_pipeline(
    processed_dir: Path = PROCESSED_DATA_DIR, features_dir: Path = FEATURES_DIR
) -> None:
    """
    Orchestrate full feature engineering pipeline.

    Parameters
    ----------
    processed_dir : Path
        Input directory with processed Parquet.
    features_dir : Path
        Output directory for engineered features.
    """
    logger.info("Starting features pipeline")
    start = time.perf_counter()

    # Load
    logger.info("Loading processed data")
    ddf = load_processed_data(processed_dir)

    # Filter 2020-2025
    logger.info("Filtering to 2020-2025 period")
    ddf = filter_2020_2025(ddf)

    # Aggregate by well-month
    logger.info("Aggregating by well-month")
    ddf = aggregate_by_well_month(ddf)

    # Rolling averages
    logger.info("Computing rolling averages")
    ddf = rolling_averages(ddf, window=12)

    # Cumulative production
    logger.info("Computing cumulative production")
    ddf = cumulative_production(ddf)

    # Production trend
    logger.info("Computing production trend")
    ddf = production_trend(ddf)

    # Well lifetime
    logger.info("Computing well lifetime")
    ddf = compute_well_lifetime(ddf)

    # Standardize
    logger.info("Standardizing numeric features")
    ddf = standardize_numerics(ddf)

    # Write (triggers computation)
    logger.info(f"Writing features to {features_dir}")
    write_features_parquet(ddf, features_dir)

    elapsed = time.perf_counter() - start
    logger.info(f"Features pipeline complete in {elapsed:.1f}s")
