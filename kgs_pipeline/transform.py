"""
Transform component: Clean, structure, and preprocess interim Parquet into processed Parquet.
"""

import logging
import time
from pathlib import Path
from typing import Optional
import pandas as pd  # type: ignore
import dask.dataframe as dd  # type: ignore

from kgs_pipeline.config import INTERIM_DATA_DIR, PROCESSED_DATA_DIR

logger = logging.getLogger(__name__)


def read_interim_parquet(interim_dir: Path) -> dd.DataFrame:
    """
    Read the interim Parquet dataset into a Dask DataFrame.

    Args:
        interim_dir: Directory containing interim Parquet files.

    Returns:
        Dask DataFrame from the interim Parquet.

    Raises:
        FileNotFoundError: If the interim Parquet does not exist.
    """
    interim_parquet = interim_dir / "kgs_monthly_raw.parquet"

    if not interim_parquet.exists():
        raise FileNotFoundError(f"Interim Parquet not found: {interim_parquet}")

    ddf = dd.read_parquet(str(interim_parquet))
    logger.info(f"Read interim Parquet with {ddf.npartitions} partitions")

    return ddf


def cast_column_types(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Cast all columns to their correct types per the output schema.

    Args:
        ddf: Dask DataFrame from ingest with all columns as str.

    Returns:
        Dask DataFrame with properly cast columns.
    """

    def _cast_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns in a single partition."""
        df = df.copy()

        # Numeric casts
        for col in ["latitude", "longitude", "production", "wells"]:
            if col in df.columns:
                if col == "production":
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                elif col == "wells":
                    df["wells_count"] = pd.to_numeric(df[col], errors="coerce")
                    df = df.drop(columns=[col])
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

        # String casts (strip whitespace)
        string_cols = [
            "well_id", "lease_kid", "lease", "dor_code", "field",
            "producing_zone", "operator", "county", "twn_dir",
            "range_dir", "spot", "product", "source_file"
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].str.strip()

        # PLSS numeric string columns (keep as str but strip)
        for col in ["township", "range", "section"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # production_date should already be datetime64; validate
        if "production_date" in df.columns:
            if df["production_date"].dtype != "datetime64[ns]":
                df["production_date"] = pd.to_datetime(df["production_date"], errors="coerce")

        return df

    result = ddf.map_partitions(_cast_partition, meta=ddf._meta)
    logger.info("Cast column types")
    return result


def deduplicate(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Remove duplicate rows based on (well_id, production_date, product).

    Args:
        ddf: Dask DataFrame.

    Returns:
        Deduplicated Dask DataFrame.

    Raises:
        KeyError: If required subset columns are missing.
    """
    required = ["well_id", "production_date", "product"]
    missing = [col for col in required if col not in ddf.columns]

    if missing:
        raise KeyError(f"Missing required columns for deduplication: {missing}")

    result = ddf.drop_duplicates(subset=required, keep="first")
    logger.info("Deduplicated rows")
    return result


def validate_physical_bounds(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Enforce domain-specific physical constraints on production values.

    Args:
        ddf: Dask DataFrame.

    Returns:
        Validated Dask DataFrame with is_suspect_rate column added.

    Raises:
        KeyError: If production or product column is missing.
    """
    if "production" not in ddf.columns or "product" not in ddf.columns:
        raise KeyError("Missing 'production' or 'product' column")

    def _validate_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Validate physical bounds in a single partition."""
        df = df.copy()

        # Initialize is_suspect_rate column
        df["is_suspect_rate"] = False

        # Non-negative production
        neg_mask = df["production"] < 0
        if neg_mask.any():
            logger.warning(f"Found {neg_mask.sum()} negative production values; setting to NaN")
            df.loc[neg_mask, "production"] = pd.NA

        # Oil rate ceiling (>50000 BBL/month is suspect)
        oil_mask = (df["product"] == "O") & (df["production"] > 50000.0)
        df.loc[oil_mask, "is_suspect_rate"] = True

        # Validate coordinates (Kansas bounds)
        if "latitude" in df.columns:
            lat_bad = (df["latitude"] < 35.0) | (df["latitude"] > 40.5)
            if lat_bad.any():
                logger.warning(f"Found {lat_bad.sum()} latitude values outside Kansas bounds; setting to NaN")
                df.loc[lat_bad, "latitude"] = pd.NA

        if "longitude" in df.columns:
            lon_bad = (df["longitude"] < -102.5) | (df["longitude"] > -94.5)
            if lon_bad.any():
                logger.warning(f"Found {lon_bad.sum()} longitude values outside Kansas bounds; setting to NaN")
                df.loc[lon_bad, "longitude"] = pd.NA

        return df

    # Create meta with is_suspect_rate column
    meta = ddf._meta.copy()
    meta["is_suspect_rate"] = False

    result = ddf.map_partitions(_validate_partition, meta=meta)
    logger.info("Validated physical bounds")
    return result


def pivot_product_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Pivot long-format (product column) into wide format (oil_bbl and gas_mcf columns).

    Args:
        ddf: Dask DataFrame in long format.

    Returns:
        Wide-format Dask DataFrame.

    Raises:
        ValueError: If neither O nor G product is found.
        KeyError: If required columns are missing.
    """
    required = ["well_id", "production_date", "product", "production"]
    missing = [col for col in required if col not in ddf.columns]
    if missing:
        raise KeyError(f"Missing required columns for pivot: {missing}")

    def _pivot_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Pivot a single partition."""
        if len(df) == 0:
            # Empty partition; return with schema
            result = df.copy()
            result["oil_bbl"] = pd.Series([], dtype="float64")
            result["gas_mcf"] = pd.Series([], dtype="float64")
            for col in ["product", "production"]:
                if col in result.columns:
                    result = result.drop(columns=[col])
            return result

        # Check products present
        products = df["product"].unique()
        if not any(p in ("O", "G") for p in products):
            raise ValueError("No recognized product types (O or G) found in partition")

        # Group-by columns (all except product and production)
        group_cols = [
            "well_id", "production_date", "lease_kid", "lease", "dor_code",
            "field", "producing_zone", "operator", "county", "township",
            "twn_dir", "range", "range_dir", "section", "spot",
            "latitude", "longitude", "wells_count", "is_suspect_rate", "source_file"
        ]
        # Filter to columns that exist
        group_cols = [col for col in group_cols if col in df.columns]

        # Pivot
        pivoted = df.pivot_table(
            index=group_cols,
            columns="product",
            values="production",
            aggfunc="first"
        )

        # Rename columns
        if "O" in pivoted.columns:
            pivoted = pivoted.rename(columns={"O": "oil_bbl"})
        else:
            pivoted["oil_bbl"] = pd.NA

        if "G" in pivoted.columns:
            pivoted = pivoted.rename(columns={"G": "gas_mcf"})
        else:
            pivoted["gas_mcf"] = pd.NA

        # Reset index
        result = pivoted.reset_index()

        # Ensure dtypes
        result["oil_bbl"] = result["oil_bbl"].astype("float64")
        result["gas_mcf"] = result["gas_mcf"].astype("float64")
        result["is_suspect_rate"] = result["is_suspect_rate"].astype(bool)

        return result

    # Create proper meta as an empty DataFrame
    meta = ddf._meta.copy()
    # Remove product and production columns
    if "product" in meta.columns:
        meta = meta.drop(columns=["product"])
    if "production" in meta.columns:
        meta = meta.drop(columns=["production"])
    # Add new columns
    meta["oil_bbl"] = pd.Series([], dtype="float64")
    meta["gas_mcf"] = pd.Series([], dtype="float64")

    result = ddf.map_partitions(_pivot_partition, meta=meta)
    logger.info("Pivoted product columns")
    return result


def fill_date_gaps(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Fill monthly date gaps per well with NaN for production columns.

    Args:
        ddf: Wide-format Dask DataFrame.

    Returns:
        Gap-filled Dask DataFrame.

    Raises:
        KeyError: If well_id or production_date columns are missing.
    """
    if "well_id" not in ddf.columns or "production_date" not in ddf.columns:
        raise KeyError("Missing 'well_id' or 'production_date' column")

    def _fill_gaps_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Fill gaps per well in a partition."""
        if len(df) == 0:
            return df.copy()

        results = []

        for well_id, well_data in df.groupby("well_id"):
            well_data = well_data.sort_values("production_date").copy()

            if len(well_data) <= 1:
                results.append(well_data)
                continue

            # Create full monthly range
            first_date = well_data["production_date"].min()
            last_date = well_data["production_date"].max()
            full_range = pd.date_range(start=first_date, end=last_date, freq="MS")

            # Reindex to full range
            well_data = well_data.set_index("production_date")
            well_data = well_data.reindex(full_range)

            # Forward-fill non-production metadata
            metadata_cols = [
                "well_id", "lease_kid", "lease", "dor_code", "field",
                "producing_zone", "operator", "county", "township",
                "twn_dir", "range", "range_dir", "section", "spot",
                "latitude", "longitude", "wells_count", "is_suspect_rate", "source_file"
            ]
            metadata_cols = [col for col in metadata_cols if col in well_data.columns]
            well_data[metadata_cols] = well_data[metadata_cols].ffill()

            # Reset index
            well_data = well_data.reset_index()
            well_data = well_data.rename(columns={"index": "production_date"})

            results.append(well_data)

        result = pd.concat(results, ignore_index=True)
        return result

    # Set index to well_id for grouping, then apply
    ddf_indexed = ddf.set_index("well_id")

    meta = ddf._meta.copy()
    result = ddf_indexed.map_partitions(_fill_gaps_partition, meta=meta)

    logger.info("Filled date gaps")
    return result


def compute_cumulative_production(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute cumulative production columns per well.

    Args:
        ddf: Gap-filled Dask DataFrame.

    Returns:
        Dask DataFrame with cumulative_oil_bbl and cumulative_gas_mcf columns.

    Raises:
        KeyError: If required columns are missing.
    """
    required = ["oil_bbl", "gas_mcf", "production_date"]
    missing = [col for col in required if col not in ddf.columns]
    if missing:
        raise KeyError(f"Missing required columns for cumulative: {missing}")

    def _compute_cumsum_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Compute cumulative sums per well in a partition."""
        if len(df) == 0:
            df = df.copy()
            df["cumulative_oil_bbl"] = pd.Series([], dtype="float64")
            df["cumulative_gas_mcf"] = pd.Series([], dtype="float64")
            return df

        df = df.copy()

        # Sort by well_id and production_date
        if "well_id" in df.columns:
            df = df.sort_values(["well_id", "production_date"])

            # Compute cumsum per well (treating NaN as 0 in the cumsum)
            df["cumulative_oil_bbl"] = df.groupby("well_id")["oil_bbl"].transform(
                lambda x: x.fillna(0).cumsum()
            )
            df["cumulative_gas_mcf"] = df.groupby("well_id")["gas_mcf"].transform(
                lambda x: x.fillna(0).cumsum()
            )
        else:
            # No well_id grouping; just cumsum across all rows
            df["cumulative_oil_bbl"] = df["oil_bbl"].fillna(0).cumsum()
            df["cumulative_gas_mcf"] = df["gas_mcf"].fillna(0).cumsum()

        return df

    meta = ddf._meta.copy()
    meta["cumulative_oil_bbl"] = pd.Series([], dtype="float64")
    meta["cumulative_gas_mcf"] = pd.Series([], dtype="float64")

    result = ddf.map_partitions(_compute_cumsum_partition, meta=meta)
    logger.info("Computed cumulative production")
    return result


def write_processed_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write the transformed DataFrame to partitioned Parquet by well_id.

    Args:
        ddf: Transformed Dask DataFrame.
        output_dir: Output directory.

    Returns:
        Path to the wells subdirectory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    wells_dir = output_dir / "wells"

    try:
        ddf.to_parquet(
            str(wells_dir),
            partition_on=["well_id"],
            write_index=False,
            overwrite=True,
            compression="snappy",
        )
        logger.info(f"Wrote processed Parquet to {wells_dir}")
        return wells_dir
    except Exception as e:
        logger.error(f"Error writing processed Parquet: {e}")
        raise


def run_transform_pipeline(
    interim_dir: Path = INTERIM_DATA_DIR,
    output_dir: Path = PROCESSED_DATA_DIR,
) -> Path:
    """
    Orchestrate the full transform pipeline.

    Args:
        interim_dir: Input interim Parquet directory.
        output_dir: Output processed Parquet directory.

    Returns:
        Path to the processed wells Parquet directory.
    """
    logger.info("Starting transform pipeline")
    start_time = time.time()

    steps = []

    # Step 1: Read
    logger.info("Step 1: Reading interim Parquet...")
    step_start = time.time()
    ddf = read_interim_parquet(interim_dir)
    steps.append(("read_interim_parquet", time.time() - step_start))

    # Step 2: Cast types
    logger.info("Step 2: Casting column types...")
    step_start = time.time()
    ddf = cast_column_types(ddf)
    steps.append(("cast_column_types", time.time() - step_start))

    # Step 3: Deduplicate
    logger.info("Step 3: Deduplicating...")
    step_start = time.time()
    ddf = deduplicate(ddf)
    steps.append(("deduplicate", time.time() - step_start))

    # Step 4: Validate bounds
    logger.info("Step 4: Validating physical bounds...")
    step_start = time.time()
    ddf = validate_physical_bounds(ddf)
    steps.append(("validate_physical_bounds", time.time() - step_start))

    # Step 5: Pivot product
    logger.info("Step 5: Pivoting product columns...")
    step_start = time.time()
    ddf = pivot_product_columns(ddf)
    steps.append(("pivot_product_columns", time.time() - step_start))

    # Step 6: Fill date gaps
    logger.info("Step 6: Filling date gaps...")
    step_start = time.time()
    ddf = fill_date_gaps(ddf)
    steps.append(("fill_date_gaps", time.time() - step_start))

    # Step 7: Compute cumulative
    logger.info("Step 7: Computing cumulative production...")
    step_start = time.time()
    ddf = compute_cumulative_production(ddf)
    steps.append(("compute_cumulative_production", time.time() - step_start))

    # Step 8: Write output (triggers computation)
    logger.info("Step 8: Writing processed Parquet...")
    step_start = time.time()
    output_path = write_processed_parquet(ddf, output_dir)
    steps.append(("write_processed_parquet", time.time() - step_start))

    total_time = time.time() - start_time
    logger.info(f"Transform pipeline complete in {total_time:.2f}s")

    for step_name, step_time in steps:
        logger.info(f"  {step_name}: {step_time:.2f}s")

    return output_path
