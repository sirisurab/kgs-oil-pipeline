"""
Features component: Engineer temporal, categorical, and production features for modeling.
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
import dask.dataframe as dd  # type: ignore

from kgs_pipeline.config import PROCESSED_DATA_DIR, FEATURES_DATA_DIR

logger = logging.getLogger(__name__)


def read_processed_parquet(processed_dir: Path) -> dd.DataFrame:
    """
    Read the processed well-partitioned Parquet dataset.

    Args:
        processed_dir: Directory containing the processed wells Parquet.

    Returns:
        Dask DataFrame from the processed wells Parquet.

    Raises:
        FileNotFoundError: If the path does not exist.
        ValueError: If no Parquet files are found.
    """
    wells_dir = processed_dir / "wells"

    if not wells_dir.exists():
        raise FileNotFoundError(f"Processed wells directory not found: {wells_dir}")

    # Check if any parquet files exist
    parquet_files = list(wells_dir.glob("**/*.parquet"))
    if not parquet_files:
        raise ValueError("No Parquet files found in processed wells directory")

    ddf = dd.read_parquet(str(wells_dir))
    logger.info(f"Read processed Parquet with {ddf.npartitions} partitions")

    return ddf


def compute_time_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute time-based features: months_since_first_prod, producing_months_count, production_phase.

    Args:
        ddf: Dask DataFrame with production_date and well_id columns.

    Returns:
        Dask DataFrame with time feature columns added.

    Raises:
        KeyError: If required columns are missing.
    """
    if "production_date" not in ddf.columns or "well_id" not in ddf.columns:
        raise KeyError("Missing 'production_date' or 'well_id' column")

    def _compute_time_features_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Compute time features for a single partition."""
        if len(df) == 0:
            df = df.copy()
            df["months_since_first_prod"] = pd.Series([], dtype="float64")
            df["producing_months_count"] = pd.Series([], dtype="float64")
            df["production_phase"] = pd.Series([], dtype="object")
            return df

        df = df.copy()
        df = df.sort_values(["well_id", "production_date"])

        # months_since_first_prod
        first_prod_dates = df.groupby("well_id")["production_date"].transform("min")
        df["months_since_first_prod"] = (
            (df["production_date"].dt.year - first_prod_dates.dt.year) * 12 +
            (df["production_date"].dt.month - first_prod_dates.dt.month)
        ).astype("float64")

        # producing_months_count (cumulative count of months with production)
        is_producing = ((df["oil_bbl"] > 0.0) | (df["gas_mcf"] > 0.0)).astype(int)
        df["producing_months_count"] = df.groupby("well_id")["is_producing"].transform(
            lambda x: x.cumsum()
        ).astype("float64")

        # production_phase
        def phase_label(months: float) -> str:
            if months <= 12:
                return "early"
            elif months <= 60:
                return "mid"
            else:
                return "late"

        df["production_phase"] = df["months_since_first_prod"].apply(phase_label)

        # Clean up temporary column
        df = df.drop(columns=["is_producing"])

        return df

    meta = ddf._meta.copy()
    meta["months_since_first_prod"] = pd.Series([], dtype="float64")
    meta["producing_months_count"] = pd.Series([], dtype="float64")
    meta["production_phase"] = pd.Series([], dtype="object")

    result = ddf.map_partitions(_compute_time_features_partition, meta=meta)
    logger.info("Computed time-based features")
    return result


def compute_rolling_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute rolling mean production statistics per well.

    Args:
        ddf: Dask DataFrame with well_id, production_date, oil_bbl, gas_mcf columns.

    Returns:
        Dask DataFrame with rolling feature columns added.

    Raises:
        KeyError: If required columns are missing.
    """
    required = ["oil_bbl", "gas_mcf", "well_id", "production_date"]
    missing = [col for col in required if col not in ddf.columns]
    if missing:
        raise KeyError(f"Missing required columns for rolling: {missing}")

    # Re-partition by well_id to keep all months for a well in one partition
    ddf = ddf.set_index("well_id").reset_index()

    def _compute_rolling_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Compute rolling means for a single partition."""
        if len(df) == 0:
            df = df.copy()
            for col_suffix in ["roll3", "roll6", "roll12"]:
                df[f"oil_bbl_{col_suffix}"] = pd.Series([], dtype="float64")
                df[f"gas_mcf_{col_suffix}"] = pd.Series([], dtype="float64")
            return df

        df = df.copy()
        df = df.sort_values(["well_id", "production_date"])

        # Rolling windows per well
        for window_size in [3, 6, 12]:
            df[f"oil_bbl_roll{window_size}"] = df.groupby("well_id")["oil_bbl"].transform(
                lambda x: x.rolling(window_size, min_periods=1).mean()
            )
            df[f"gas_mcf_roll{window_size}"] = df.groupby("well_id")["gas_mcf"].transform(
                lambda x: x.rolling(window_size, min_periods=1).mean()
            )

        return df

    meta = ddf._meta.copy()
    for col_suffix in ["roll3", "roll6", "roll12"]:
        meta[f"oil_bbl_{col_suffix}"] = pd.Series([], dtype="float64")
        meta[f"gas_mcf_{col_suffix}"] = pd.Series([], dtype="float64")

    result = ddf.map_partitions(_compute_rolling_partition, meta=meta)
    logger.info("Computed rolling production statistics")
    return result


def compute_decline_and_gor(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute month-on-month decline rates and gas-oil ratio (GOR).

    Args:
        ddf: Dask DataFrame with well_id, production_date, oil_bbl, gas_mcf columns.

    Returns:
        Dask DataFrame with decline and GOR columns added.

    Raises:
        KeyError: If required columns are missing.
    """
    required = ["oil_bbl", "gas_mcf", "well_id", "production_date"]
    missing = [col for col in required if col not in ddf.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")

    def _compute_decline_gor_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Compute decline rates and GOR for a single partition."""
        if len(df) == 0:
            df = df.copy()
            df["oil_decline_rate_mom"] = pd.Series([], dtype="float64")
            df["gas_decline_rate_mom"] = pd.Series([], dtype="float64")
            df["gor"] = pd.Series([], dtype="float64")
            return df

        df = df.copy()
        df = df.sort_values(["well_id", "production_date"])

        # Decline rates (month-on-month)
        df["oil_decline_rate_mom"] = df.groupby("well_id")["oil_bbl"].transform(
            lambda x: x.pct_change()
        ).astype("float64")

        df["gas_decline_rate_mom"] = df.groupby("well_id")["gas_mcf"].transform(
            lambda x: x.pct_change()
        ).astype("float64")

        # GOR (avoid division by zero)
        df["gor"] = np.where(
            (df["oil_bbl"] > 0.0) & (df["oil_bbl"].notna()),
            df["gas_mcf"] / df["oil_bbl"],
            np.nan
        ).astype("float64")

        return df

    meta = ddf._meta.copy()
    meta["oil_decline_rate_mom"] = pd.Series([], dtype="float64")
    meta["gas_decline_rate_mom"] = pd.Series([], dtype="float64")
    meta["gor"] = pd.Series([], dtype="float64")

    result = ddf.map_partitions(_compute_decline_gor_partition, meta=meta)
    logger.info("Computed decline rates and GOR")
    return result


def encode_categorical_features(
    ddf: dd.DataFrame,
    encoding_map: Optional[dict] = None
) -> tuple:
    """
    Label-encode categorical columns and return the encoded DataFrame and encoding map.

    Args:
        ddf: Dask DataFrame with categorical columns.
        encoding_map: If provided, use this existing map (inference mode).
                     If None, compute from the data (training mode).

    Returns:
        Tuple of (encoded_ddf, encoding_map).

    Raises:
        KeyError: If any categorical columns are missing.
    """
    categorical_cols = ["county", "field", "producing_zone", "operator"]
    missing = [col for col in categorical_cols if col not in ddf.columns]
    if missing:
        raise KeyError(f"Missing categorical columns: {missing}")

    if encoding_map is None:
        # Training mode: compute unique values and create encoding map
        logger.info("Computing encoding map from data...")
        encoding_map = {}

        for col in categorical_cols:
            # Compute unique values (permitted one-time metadata computation)
            unique_vals = ddf[col].dropna().unique().compute()
            unique_vals_sorted = sorted(unique_vals)
            encoding_map[col] = {val: idx for idx, val in enumerate(unique_vals_sorted)}
            logger.info(f"Encoded {col}: {len(unique_vals_sorted)} unique values")

    def _encode_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Encode categorical columns in a single partition."""
        df = df.copy()

        for col in categorical_cols:
            col_encoded = f"{col}_encoded"
            col_map = encoding_map.get(col, {})

            # Apply encoding with -1 for unseen categories
            df[col_encoded] = df[col].map(col_map).fillna(-1).astype("int32")

        return df

    meta = ddf._meta.copy()
    for col in categorical_cols:
        meta[f"{col}_encoded"] = pd.Series([], dtype="int32")

    result = ddf.map_partitions(_encode_partition, meta=meta)
    logger.info("Encoded categorical features")
    return result, encoding_map


def save_encoding_map(encoding_map: dict, output_dir: Path) -> Path:
    """
    Persist the encoding map to a JSON sidecar file.

    Args:
        encoding_map: Dict of {column_name: {category_string: integer_code}}.
        output_dir: Output directory.

    Returns:
        Path to the written JSON file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "encoding_map.json"

    with open(output_file, "w") as f:
        json.dump(encoding_map, f, indent=2)

    logger.info(f"Saved encoding map to {output_file}")
    return output_file


def load_encoding_map(output_dir: Path) -> dict:
    """
    Load the encoding map from a JSON sidecar file.

    Args:
        output_dir: Directory containing the encoding_map.json file.

    Returns:
        Deserialized encoding map dict.

    Raises:
        FileNotFoundError: If the encoding_map.json file does not exist.
    """
    encoding_file = output_dir / "encoding_map.json"

    if not encoding_file.exists():
        raise FileNotFoundError(f"Encoding map file not found: {encoding_file}")

    with open(encoding_file, "r") as f:
        encoding_map = json.load(f)

    logger.info(f"Loaded encoding map from {encoding_file}")
    return encoding_map


def write_feature_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write the feature-engineered DataFrame to partitioned Parquet.

    Args:
        ddf: Dask DataFrame to write.
        output_dir: Output directory.

    Returns:
        Path to the output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        ddf.to_parquet(
            str(output_dir),
            partition_on=["well_id"],
            write_index=False,
            overwrite=True,
            compression="snappy",
        )
        logger.info(f"Wrote feature Parquet to {output_dir}")
        return output_dir
    except Exception as e:
        logger.error(f"Error writing feature Parquet: {e}")
        raise


def run_features_pipeline(
    processed_dir: Path = PROCESSED_DATA_DIR,
    output_dir: Path = FEATURES_DATA_DIR,
) -> Path:
    """
    Orchestrate the full features engineering pipeline.

    Args:
        processed_dir: Input processed Parquet directory.
        output_dir: Output features Parquet directory.

    Returns:
        Path to the output features directory.
    """
    logger.info("Starting features pipeline")
    start_time = time.time()

    steps = []

    # Step 1: Read processed Parquet
    logger.info("Step 1: Reading processed Parquet...")
    step_start = time.time()
    ddf = read_processed_parquet(processed_dir)
    steps.append(("read_processed_parquet", time.time() - step_start))

    # Step 2: Compute time features
    logger.info("Step 2: Computing time-based features...")
    step_start = time.time()
    ddf = compute_time_features(ddf)
    steps.append(("compute_time_features", time.time() - step_start))

    # Step 3: Compute rolling features
    logger.info("Step 3: Computing rolling production statistics...")
    step_start = time.time()
    ddf = compute_rolling_features(ddf)
    steps.append(("compute_rolling_features", time.time() - step_start))

    # Step 4: Compute decline and GOR
    logger.info("Step 4: Computing decline rates and GOR...")
    step_start = time.time()
    ddf = compute_decline_and_gor(ddf)
    steps.append(("compute_decline_and_gor", time.time() - step_start))

    # Step 5: Encode categorical features
    logger.info("Step 5: Encoding categorical features...")
    step_start = time.time()
    ddf, encoding_map = encode_categorical_features(ddf)
    steps.append(("encode_categorical_features", time.time() - step_start))

    # Step 6: Save encoding map
    logger.info("Step 6: Saving encoding map...")
    step_start = time.time()
    save_encoding_map(encoding_map, output_dir)
    steps.append(("save_encoding_map", time.time() - step_start))

    # Step 7: Write feature Parquet
    logger.info("Step 7: Writing feature Parquet...")
    step_start = time.time()
    output_path = write_feature_parquet(ddf, output_dir)
    steps.append(("write_feature_parquet", time.time() - step_start))

    total_time = time.time() - start_time
    logger.info(f"Features pipeline complete in {total_time:.2f}s")

    for step_name, step_time in steps:
        logger.info(f"  {step_name}: {step_time:.2f}s")

    return output_path
