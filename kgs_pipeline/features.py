import logging
import time
import json
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd
import numpy as np
import dask.dataframe as dd
from kgs_pipeline.config import PROCESSED_DATA_DIR, FEATURES_DATA_DIR

logger = logging.getLogger(__name__)


def read_processed_parquet(processed_dir: Path) -> dd.DataFrame:
    """
    Read the processed well-partitioned Parquet dataset.

    Args:
        processed_dir: Path to the processed data directory.

    Returns:
        Dask DataFrame from the processed wells Parquet files.

    Raises:
        FileNotFoundError: If the wells subdirectory does not exist.
        ValueError: If no Parquet files are found.
    """
    wells_dir = processed_dir / "wells"

    if not wells_dir.exists():
        raise FileNotFoundError(f"Processed wells directory not found: {wells_dir}")

    try:
        ddf = dd.read_parquet(str(wells_dir), engine="pyarrow")
        if ddf.npartitions == 0:
            raise ValueError("No Parquet files found in processed wells directory")
        logger.info(f"Read processed Parquet with {ddf.npartitions} partitions from {wells_dir}")
        return ddf
    except Exception as e:
        if isinstance(e, ValueError) and "No Parquet files" in str(e):
            raise
        raise ValueError(f"No Parquet files found in processed wells directory") from e


def compute_time_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute time-based features: months_since_first_prod, producing_months_count, and production_phase.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with time-based features added.

    Raises:
        KeyError: If production_date or well_id columns are missing.
    """
    required_cols = {"production_date", "well_id", "oil_bbl", "gas_mcf"}
    missing = required_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing columns: {missing}")

    def _compute_time_features_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Compute time features in a single partition."""
        df = df.copy()
        df = df.sort_values(["well_id", "production_date"])

        df["months_since_first_prod"] = 0.0
        df["producing_months_count"] = 0.0
        df["production_phase"] = ""

        for well_id in df["well_id"].unique():
            mask = df["well_id"] == well_id
            well_df = df.loc[mask, :].copy()

            if len(well_df) == 0:
                continue

            # Months since first production
            first_date = well_df["production_date"].min()
            months_elapsed = (well_df["production_date"] - first_date) / pd.Timedelta(days=365.25 / 12)
            df.loc[mask, "months_since_first_prod"] = months_elapsed.values

            # Producing months count (cumulative)
            is_producing = (
                ((well_df["oil_bbl"] > 0.0) | (well_df["gas_mcf"] > 0.0))
                .astype(int)
                .cumsum()
            )
            df.loc[mask, "producing_months_count"] = is_producing.values

            # Production phase
            phase = pd.cut(
                months_elapsed,
                bins=[0, 12, 60, np.inf],
                labels=["early", "mid", "late"],
                right=True,
                include_lowest=True,
            )
            df.loc[mask, "production_phase"] = phase.astype(str).values

        return df

    result = ddf.map_partitions(_compute_time_features_partition)
    logger.info("Computed time-based features")
    return result


def compute_rolling_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute rolling mean production statistics per well.

    Args:
        ddf: Input Dask DataFrame (should be partitioned by well_id).

    Returns:
        Dask DataFrame with rolling features added.

    Raises:
        KeyError: If required columns are missing.
    """
    required_cols = {"oil_bbl", "gas_mcf", "well_id", "production_date"}
    missing = required_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing columns: {missing}")

    # Repartition by well_id to ensure all rows for a well are in the same partition
    ddf = ddf.set_index("well_id").reset_index()

    def _compute_rolling_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Compute rolling features in a single partition."""
        df = df.copy()
        df = df.sort_values(["well_id", "production_date"])

        # Initialize rolling columns
        for col in ["oil_bbl_roll3", "oil_bbl_roll6", "oil_bbl_roll12",
                    "gas_mcf_roll3", "gas_mcf_roll6", "gas_mcf_roll12"]:
            df[col] = np.nan

        for well_id in df["well_id"].unique():
            mask = df["well_id"] == well_id
            well_df = df.loc[mask, :].copy()

            if len(well_df) == 0:
                continue

            # Sort by production_date
            well_df = well_df.sort_values("production_date")

            # Compute rolling means for oil
            df.loc[mask, "oil_bbl_roll3"] = well_df["oil_bbl"].rolling(
                3, min_periods=1
            ).mean().values
            df.loc[mask, "oil_bbl_roll6"] = well_df["oil_bbl"].rolling(
                6, min_periods=1
            ).mean().values
            df.loc[mask, "oil_bbl_roll12"] = well_df["oil_bbl"].rolling(
                12, min_periods=1
            ).mean().values

            # Compute rolling means for gas
            df.loc[mask, "gas_mcf_roll3"] = well_df["gas_mcf"].rolling(
                3, min_periods=1
            ).mean().values
            df.loc[mask, "gas_mcf_roll6"] = well_df["gas_mcf"].rolling(
                6, min_periods=1
            ).mean().values
            df.loc[mask, "gas_mcf_roll12"] = well_df["gas_mcf"].rolling(
                12, min_periods=1
            ).mean().values

        return df

    result = ddf.map_partitions(_compute_rolling_partition)
    logger.info("Computed rolling features")
    return result


def compute_decline_and_gor(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute month-on-month decline rates and gas-oil ratio.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with decline and GOR features added.

    Raises:
        KeyError: If required columns are missing.
    """
    required_cols = {"well_id", "production_date", "oil_bbl", "gas_mcf"}
    missing = required_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing columns: {missing}")

    def _compute_decline_and_gor_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Compute decline and GOR features in a single partition."""
        df = df.copy()
        df = df.sort_values(["well_id", "production_date"])

        df["oil_decline_rate_mom"] = np.nan
        df["gas_decline_rate_mom"] = np.nan
        df["gor"] = np.nan

        for well_id in df["well_id"].unique():
            mask = df["well_id"] == well_id
            well_df = df.loc[mask, :].copy().reset_index(drop=True)

            if len(well_df) == 0:
                continue

            # Oil decline rate
            oil_prev = well_df["oil_bbl"].shift(1)
            oil_decline = (well_df["oil_bbl"] - oil_prev) / oil_prev
            df.loc[mask, "oil_decline_rate_mom"] = oil_decline.values

            # Gas decline rate
            gas_prev = well_df["gas_mcf"].shift(1)
            gas_decline = (well_df["gas_mcf"] - gas_prev) / gas_prev
            df.loc[mask, "gas_decline_rate_mom"] = gas_decline.values

            # GOR (avoid division by zero)
            gor = well_df["gas_mcf"] / well_df["oil_bbl"].replace(0, np.nan)
            df.loc[mask, "gor"] = gor.values

        return df

    result = ddf.map_partitions(_compute_decline_and_gor_partition)
    logger.info("Computed decline and GOR features")
    return result


def encode_categorical_features(
    ddf: dd.DataFrame,
    encoding_map: Optional[dict] = None,
) -> Tuple[dd.DataFrame, dict]:
    """
    Label-encode categorical columns.

    Args:
        ddf: Input Dask DataFrame.
        encoding_map: Optional pre-computed encoding map (for inference).

    Returns:
        Tuple of (encoded_ddf, encoding_map).

    Raises:
        KeyError: If any categorical columns are missing.
    """
    categorical_cols = {"county", "field", "producing_zone", "operator"}
    missing = categorical_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing categorical columns: {missing}")

    # If no encoding_map provided, compute it from the data
    if encoding_map is None:
        # Compute unique values for each column (permitted .compute() call)
        encoding_map = {}
        for col in categorical_cols:
            unique_vals = (
                ddf[col]
                .unique()
                .compute()  # Permitted for metadata aggregation
                .dropna()
                .sort_values()
            )
            encoding_map[col] = {
                val: idx for idx, val in enumerate(unique_vals)
            }
        logger.info(f"Computed encoding map for {len(categorical_cols)} categorical columns")

    def _encode_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Encode categorical columns in a single partition."""
        df = df.copy()

        for col in categorical_cols:
            col_encoded = col + "_encoded"
            df[col_encoded] = df[col].map(encoding_map[col]).fillna(-1).astype("int32")

        return df

    result = ddf.map_partitions(_encode_partition)
    logger.info("Encoded categorical features")
    return result, encoding_map


def save_encoding_map(encoding_map: dict, output_dir: Path) -> Path:
    """
    Save the encoding map to JSON.

    Args:
        encoding_map: Dictionary mapping column names to encoding dicts.
        output_dir: Output directory.

    Returns:
        Path to the saved encoding_map.json file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "encoding_map.json"

    with open(output_file, "w") as f:
        json.dump(encoding_map, f, indent=2)

    logger.info(f"Saved encoding map to {output_file}")
    return output_file


def load_encoding_map(output_dir: Path) -> dict:
    """
    Load the encoding map from JSON.

    Args:
        output_dir: Output directory.

    Returns:
        Encoding map dictionary.

    Raises:
        FileNotFoundError: If encoding_map.json does not exist.
    """
    encoding_file = output_dir / "encoding_map.json"

    if not encoding_file.exists():
        raise FileNotFoundError(f"Encoding map not found at {encoding_file}")

    with open(encoding_file, "r") as f:
        encoding_map = json.load(f)

    logger.info(f"Loaded encoding map from {encoding_file}")
    return encoding_map


def write_feature_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write the feature DataFrame to Parquet, partitioned by well_id.

    Args:
        ddf: Input Dask DataFrame.
        output_dir: Output directory.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        ddf.to_parquet(
            str(output_dir),
            engine="pyarrow",
            compression="snappy",
            write_index=False,
            overwrite=True,
            partition_on=["well_id"],
        )
        logger.info(f"Wrote feature Parquet to {output_dir}")
    except Exception as e:
        logger.error(f"Failed to write feature Parquet: {type(e).__name__}: {e}")
        raise

    return output_dir


def run_features_pipeline(processed_dir: Path, output_dir: Path) -> Path:
    """
    Top-level entry point for the features component.

    Args:
        processed_dir: Path to the processed data directory.
        output_dir: Path to the output features directory.

    Returns:
        Path to the output features directory.
    """
    start_time = time.time()

    try:
        logger.info("Starting features pipeline...")

        # Step 1: Read processed parquet
        step_start = time.time()
        ddf = read_processed_parquet(processed_dir)
        logger.info(f"Step 1 (read_processed_parquet) took {time.time() - step_start:.2f}s")

        # Step 2: Compute time features
        step_start = time.time()
        ddf = compute_time_features(ddf)
        logger.info(f"Step 2 (compute_time_features) took {time.time() - step_start:.2f}s")

        # Step 3: Compute rolling features
        step_start = time.time()
        ddf = compute_rolling_features(ddf)
        logger.info(f"Step 3 (compute_rolling_features) took {time.time() - step_start:.2f}s")

        # Step 4: Compute decline and GOR
        step_start = time.time()
        ddf = compute_decline_and_gor(ddf)
        logger.info(f"Step 4 (compute_decline_and_gor) took {time.time() - step_start:.2f}s")

        # Step 5: Encode categorical features
        step_start = time.time()
        ddf, encoding_map = encode_categorical_features(ddf)
        logger.info(f"Step 5 (encode_categorical_features) took {time.time() - step_start:.2f}s")

        # Step 6: Save encoding map
        step_start = time.time()
        save_encoding_map(encoding_map, output_dir)
        logger.info(f"Step 6 (save_encoding_map) took {time.time() - step_start:.2f}s")

        # Step 7: Write feature parquet
        step_start = time.time()
        output_path = write_feature_parquet(ddf, output_dir)
        logger.info(f"Step 7 (write_feature_parquet) took {time.time() - step_start:.2f}s")

        total_time = time.time() - start_time
        logger.info(f"Features pipeline completed in {total_time:.2f}s")

        return output_path

    except Exception as e:
        logger.error(f"Features pipeline failed: {type(e).__name__}: {e}")
        raise
