"""Transform component: clean, standardize, and prepare data for feature engineering."""

import logging
import time
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyarrow as pa

from kgs_pipeline.config import (
    GAS_UNIT,
    INTERIM_DATA_DIR,
    MAX_REALISTIC_OIL_BBL_PER_MONTH,
    OIL_UNIT,
    PROCESSED_DATA_DIR,
)

logger = logging.getLogger(__name__)


def load_interim_data(interim_dir: Path = INTERIM_DATA_DIR) -> dd.DataFrame:
    """
    Load interim Parquet data as Dask DataFrame.

    Parameters
    ----------
    interim_dir : Path
        Directory containing interim Parquet files.

    Returns
    -------
    dask.dataframe.DataFrame
        Lazy Dask DataFrame.

    Raises
    ------
    FileNotFoundError
        If directory does not exist or contains no Parquet files.
    """
    if not interim_dir.exists():
        raise FileNotFoundError(f"Interim directory not found: {interim_dir}")

    try:
        ddf = dd.read_parquet(str(interim_dir), engine="pyarrow")
    except (ValueError, FileNotFoundError) as e:
        raise FileNotFoundError(
            f"No Parquet files found in {interim_dir}"
        ) from e

    logger.info(f"Loaded interim data from {interim_dir}")
    return ddf


def parse_dates(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Parse MONTH_YEAR (M-YYYY format) to datetime production_date.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Updated Dask DataFrame with production_date column.

    Raises
    ------
    KeyError
        If MONTH_YEAR column not found.
    """
    if "MONTH_YEAR" not in ddf.columns and "MONTH-YEAR" not in ddf.columns:
        raise KeyError("MONTH_YEAR column not found")

    # Rename if needed
    if "MONTH-YEAR" in ddf.columns:
        ddf = ddf.rename(columns={"MONTH-YEAR": "MONTH_YEAR"})

    def parse_month_year(df):
        # Convert "M-YYYY" to "1-M-YYYY" (day-month-year format) for parsing
        date_str = "1-" + df["MONTH_YEAR"]
        df["production_date"] = pd.to_datetime(
            date_str, format="%d-%m-%Y", errors="coerce"
        )
        return df.drop(columns=["MONTH_YEAR"])

    ddf = ddf.map_partitions(parse_month_year)
    logger.info("Parsed dates to production_date")
    return ddf


def cast_and_rename_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Rename all columns to snake_case and cast to correct types.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Updated Dask DataFrame with renamed and recast columns.

    Raises
    ------
    KeyError
        If mandatory columns are missing.
    """
    rename_map = {
        "LEASE_KID": "lease_kid",
        "LEASE": "lease_name",
        "DOR_CODE": "dor_code",
        "API_NUMBER": "api_number",
        "FIELD": "field_name",
        "PRODUCING_ZONE": "producing_zone",
        "OPERATOR": "operator",
        "COUNTY": "county",
        "TOWNSHIP": "township",
        "TWN_DIR": "twn_dir",
        "RANGE": "range_val",
        "RANGE_DIR": "range_dir",
        "SECTION": "section",
        "SPOT": "spot",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "PRODUCT": "product",
        "WELLS": "well_count",
        "PRODUCTION": "production",
    }

    # Check mandatory columns
    mandatory = ["LEASE_KID", "API_NUMBER", "PRODUCT", "PRODUCTION"]
    missing = [col for col in mandatory if col not in ddf.columns]
    if missing:
        raise KeyError(f"Mandatory columns missing: {missing}")

    # Rename
    available_rename = {
        k: v for k, v in rename_map.items() if k in ddf.columns
    }
    ddf = ddf.rename(columns=available_rename)

    # Cast and clean
    def cast_columns(df):
        # String columns: strip whitespace
        str_cols = [
            "lease_kid",
            "lease_name",
            "dor_code",
            "api_number",
            "field_name",
            "producing_zone",
            "operator",
            "county",
            "township",
            "twn_dir",
            "range_val",
            "range_dir",
            "section",
            "spot",
            "source_file",
            "product",
        ]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].str.strip()

        # Uppercase product
        if "product" in df.columns:
            df["product"] = df["product"].str.upper()

        # Numeric columns
        if "latitude" in df.columns:
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        if "longitude" in df.columns:
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        if "production" in df.columns:
            df["production"] = pd.to_numeric(df["production"], errors="coerce")
        if "well_count" in df.columns:
            df["well_count"] = pd.to_numeric(df["well_count"], errors="coerce")

        return df

    ddf = ddf.map_partitions(cast_columns)
    logger.info("Renamed and cast columns")
    return ddf


def explode_api_numbers(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Explode comma-separated API numbers into individual well records.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame with api_number column.

    Returns
    -------
    dask.dataframe.DataFrame
        Dask DataFrame with one row per API number (well).

    Raises
    ------
    KeyError
        If api_number column not found.
    """
    if "api_number" not in ddf.columns:
        raise KeyError("api_number column not found")

    def explode_partition(df):
        # Split and explode
        df["api_number"] = df["api_number"].str.split(",")
        df = df.explode("api_number")

        # Strip whitespace and remove null/empty entries
        df["api_number"] = df["api_number"].str.strip()
        df = df[df["api_number"].notna()]
        df = df[df["api_number"] != ""]
        df = df[df["api_number"] != "nan"]

        # Rename to well_id
        df = df.rename(columns={"api_number": "well_id"})
        df = df.reset_index(drop=True)

        return df

    ddf = ddf.map_partitions(explode_partition)
    logger.info("Exploded API numbers to well_id")
    return ddf


def validate_physical_bounds(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Enforce physical domain constraints on production data.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Validated Dask DataFrame with outlier_flag and bounds enforcement.

    Raises
    ------
    KeyError
        If production or product columns not found.
    """
    if "production" not in ddf.columns:
        raise KeyError("production column not found")
    if "product" not in ddf.columns:
        raise KeyError("product column not found")

    def validate_partition(df):
        # Negative production → NaN
        neg_mask = df["production"] < 0
        if neg_mask.any():
            logger.warning(f"Found {neg_mask.sum()} negative production values")
            df.loc[neg_mask, "production"] = np.nan

        # Oil outlier flag
        df["outlier_flag"] = (
            (df["product"] == "O")
            & (df["production"] > MAX_REALISTIC_OIL_BBL_PER_MONTH)
        ).astype(bool)

        # Geographic bounds (Kansas)
        df.loc[(df["latitude"] < 36.9) | (df["latitude"] > 40.1), "latitude"] = np.nan
        df.loc[
            (df["longitude"] < -102.1) | (df["longitude"] > -94.5), "longitude"
        ] = np.nan

        # Valid products only
        df = df[df["product"].isin(["O", "G"])]

        return df

    ddf = ddf.map_partitions(validate_partition)
    logger.info("Validated physical bounds")
    return ddf


def deduplicate_records(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Remove duplicate well-month-product records.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Deduplicated Dask DataFrame.

    Raises
    ------
    KeyError
        If well_id or production_date columns missing.
    """
    if "well_id" not in ddf.columns:
        raise KeyError("well_id column not found")
    if "production_date" not in ddf.columns:
        raise KeyError("production_date column not found")

    def dedup_partition(df):
        dedup_cols = ["well_id", "production_date", "product"]
        df = df.sort_values(by=dedup_cols)
        df = df.drop_duplicates(subset=dedup_cols, keep="first")
        return df

    ddf = ddf.map_partitions(dedup_partition)
    logger.info("Deduplicated records")
    return ddf


def add_unit_column(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Add unit column based on product type.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Dask DataFrame with unit column.

    Raises
    ------
    KeyError
        If product column not found.
    """
    if "product" not in ddf.columns:
        raise KeyError("product column not found")

    def add_units(df):
        df["unit"] = np.where(
            df["product"] == "O",
            OIL_UNIT,
            np.where(df["product"] == "G", GAS_UNIT, "UNKNOWN"),
        )
        return df

    ddf = ddf.map_partitions(add_units)
    logger.info("Added unit column")
    return ddf


def sort_by_well_and_date(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Repartition by well_id and sort chronologically by production_date.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Repartitioned and sorted Dask DataFrame.

    Raises
    ------
    KeyError
        If well_id or production_date columns missing.
    """
    if "well_id" not in ddf.columns:
        raise KeyError("well_id column not found")
    if "production_date" not in ddf.columns:
        raise KeyError("production_date column not found")

    # Set index to well_id to partition by well
    ddf = ddf.set_index("well_id", sorted=False, drop=False)

    # Sort within each partition
    def sort_partition(df):
        return df.sort_values(by=["well_id", "production_date", "product"])

    ddf = ddf.map_partitions(sort_partition)
    logger.info("Repartitioned by well_id and sorted chronologically")
    return ddf


def write_processed_parquet(
    ddf: dd.DataFrame, processed_dir: Path = PROCESSED_DATA_DIR
) -> None:
    """
    Write processed Parquet partitioned by well_id.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Data to persist.
    processed_dir : Path
        Output directory.

    Raises
    ------
    TypeError
        If ddf is not a Dask DataFrame.
    KeyError
        If well_id column not found.
    OSError
        If write fails.
    """
    if not isinstance(ddf, dd.DataFrame):
        raise TypeError("Expected a dask DataFrame")
    if "well_id" not in ddf.columns:
        raise KeyError("well_id column required for partitioning")

    processed_dir.mkdir(parents=True, exist_ok=True)

    # Define schema to ensure consistency across partitions
    schema = pa.schema(
        [
            ("lease_kid", pa.string()),
            ("lease_name", pa.string()),
            ("dor_code", pa.string()),
            ("well_id", pa.string()),
            ("field_name", pa.string()),
            ("producing_zone", pa.string()),
            ("operator", pa.string()),
            ("county", pa.string()),
            ("township", pa.string()),
            ("twn_dir", pa.string()),
            ("range_val", pa.string()),
            ("range_dir", pa.string()),
            ("section", pa.string()),
            ("spot", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("product", pa.string()),
            ("well_count", pa.float64()),
            ("production", pa.float64()),
            ("source_file", pa.string()),
            ("production_date", pa.timestamp("ns")),
            ("unit", pa.string()),
            ("outlier_flag", pa.bool_()),
        ]
    )

    try:
        ddf.to_parquet(
            str(processed_dir),
            partition_on=["well_id"],
            write_index=False,
            overwrite=True,
            engine="pyarrow",
            schema=schema,
        )
        logger.info(f"Wrote processed Parquet to {processed_dir}")
    except OSError as e:
        raise OSError(f"Failed to write processed Parquet to {processed_dir}: {e}") from e


def run_transform_pipeline(
    interim_dir: Path = INTERIM_DATA_DIR, processed_dir: Path = PROCESSED_DATA_DIR
) -> None:
    """
    Orchestrate full transform pipeline.

    Parameters
    ----------
    interim_dir : Path
        Input directory with interim Parquet.
    processed_dir : Path
        Output directory for processed Parquet.
    """
    logger.info("Starting transform pipeline")
    start = time.perf_counter()

    # Load
    logger.info("Loading interim data")
    ddf = load_interim_data(interim_dir)

    # Parse dates
    logger.info("Parsing dates")
    ddf = parse_dates(ddf)

    # Cast and rename
    logger.info("Casting and renaming columns")
    ddf = cast_and_rename_columns(ddf)

    # Explode API numbers
    logger.info("Exploding API numbers to well level")
    ddf = explode_api_numbers(ddf)

    # Validate bounds
    logger.info("Validating physical bounds")
    ddf = validate_physical_bounds(ddf)

    # Deduplicate
    logger.info("Deduplicating records")
    ddf = deduplicate_records(ddf)

    # Add units
    logger.info("Adding unit column")
    ddf = add_unit_column(ddf)

    # Sort by well and date
    logger.info("Sorting by well and date")
    ddf = sort_by_well_and_date(ddf)

    # Write (triggers computation)
    logger.info(f"Writing processed Parquet to {processed_dir}")
    write_processed_parquet(ddf, processed_dir)

    elapsed = time.perf_counter() - start
    logger.info(f"Transform pipeline complete in {elapsed:.1f}s")
