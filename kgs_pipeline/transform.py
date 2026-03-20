import logging
import time
from pathlib import Path
from typing import Optional
import pandas as pd  # type: ignore[import-untyped]
import numpy as np
import dask.dataframe as dd
from kgs_pipeline.config import INTERIM_DATA_DIR, PROCESSED_DATA_DIR

logger = logging.getLogger(__name__)


def read_interim_parquet(interim_dir: Path) -> dd.DataFrame:
    """
    Read the interim Parquet dataset produced by the ingest step.

    Args:
        interim_dir: Path to the interim data directory.

    Returns:
        Dask DataFrame from the interim Parquet file.

    Raises:
        FileNotFoundError: If the interim Parquet path does not exist.
    """
    parquet_path = interim_dir / "kgs_monthly_raw.parquet"

    if not parquet_path.exists():
        raise FileNotFoundError(f"Interim Parquet not found at {parquet_path}")

    try:
        ddf = dd.read_parquet(str(parquet_path), engine="pyarrow")
        logger.info(
            f"Read interim Parquet with {ddf.npartitions} partitions from {parquet_path}"
        )
        return ddf
    except Exception as e:
        logger.error(f"Failed to read interim Parquet: {type(e).__name__}: {e}")
        raise


def cast_column_types(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Cast all columns to their correct types per the output schema.

    Args:
        ddf: Input Dask DataFrame with all columns as str.

    Returns:
        Dask DataFrame with correctly typed columns.
    """

    def _cast_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Cast column types in a single partition."""
        df = df.copy()

        # Numeric columns with coercion
        for col in ["latitude", "longitude", "production", "wells"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Strip whitespace from string columns
        string_cols = [
            "well_id",
            "lease_kid",
            "lease",
            "dor_code",
            "field",
            "producing_zone",
            "operator",
            "county",
            "township",
            "twn_dir",
            "range",
            "range_dir",
            "spot",
            "product",
            "source_file",
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        # Validate production_date is datetime64[ns]
        if "production_date" in df.columns:
            df["production_date"] = pd.to_datetime(df["production_date"], errors="coerce")

        # Rename 'wells' to 'wells_count'
        if "wells" in df.columns:
            df = df.rename(columns={"wells": "wells_count"})

        return df

    result = ddf.map_partitions(_cast_partition)
    logger.info("Cast column types")
    return result


def deduplicate(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Remove duplicate rows based on (well_id, production_date, product).

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Deduplicated Dask DataFrame.

    Raises:
        KeyError: If required columns are missing.
    """
    required_cols = {"well_id", "production_date", "product"}
    missing = required_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing columns for deduplication: {missing}")

    result = ddf.drop_duplicates(subset=["well_id", "production_date", "product"])
    logger.info("Deduplicated data")
    return result


def validate_physical_bounds(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Enforce domain-specific physical constraints on production and coordinates.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with bounds-validated columns.

    Raises:
        KeyError: If required columns are missing.
    """
    required_cols = {"production", "product"}
    missing = required_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing columns for bounds validation: {missing}")

    def _validate_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Validate physical bounds in a single partition."""
        df = df.copy()

        # Non-negative production
        if "production" in df.columns:
            mask = df["production"] < 0
            if mask.any():
                logger.warning(f"Found {mask.sum()} negative production values, setting to NaN")
                df.loc[mask, "production"] = None

        # Oil rate ceiling
        if "product" in df.columns and "production" in df.columns:
            df["is_suspect_rate"] = False
            mask = (df["product"] == "O") & (df["production"] > 50000.0)
            df.loc[mask, "is_suspect_rate"] = True

        # Coordinate bounds
        if "latitude" in df.columns:
            mask = (df["latitude"] < 35.0) | (df["latitude"] > 40.5)
            if mask.any():
                df.loc[mask, "latitude"] = None

        if "longitude" in df.columns:
            mask = (df["longitude"] < -102.5) | (df["longitude"] > -94.5)
            if mask.any():
                df.loc[mask, "longitude"] = None

        return df

    result = ddf.map_partitions(_validate_partition)
    logger.info("Validated physical bounds")
    return result


def pivot_product_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Transform long-format data (one row per well × month × product) into wide format
    (one row per well × month with separate oil_bbl and gas_mcf columns).

    Args:
        ddf: Input Dask DataFrame with product column.

    Returns:
        Dask DataFrame with pivoted product columns.

    Raises:
        ValueError: If neither "O" nor "G" appears in product column.
        KeyError: If required columns are missing.
    """
    required_cols = {
        "well_id",
        "production_date",
        "product",
        "production",
        "lease_kid",
    }
    missing = required_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing columns for pivot: {missing}")

    def _pivot_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Pivot product column in a single partition."""
        if df.empty:
            df["oil_bbl"] = pd.Series(dtype="float64")
            df["gas_mcf"] = pd.Series(dtype="float64")
            return df.drop(columns=["product", "production"], errors="ignore")

        # Index columns for groupby (all except product and production)
        index_cols = [c for c in df.columns if c not in ["product", "production"]]

        # Pivot using groupby and unstack
        try:
            pivoted = df.groupby(index_cols + ["product"])["production"].first().unstack(
                fill_value=None
            )
        except Exception:
            pivoted = df.groupby(index_cols)["production"].first().unstack(fill_value=None)

        # Reset index to get index columns back as regular columns
        pivoted = pivoted.reset_index()

        # Rename product columns
        if "O" in pivoted.columns:
            pivoted = pivoted.rename(columns={"O": "oil_bbl"})
        else:
            pivoted["oil_bbl"] = None

        if "G" in pivoted.columns:
            pivoted = pivoted.rename(columns={"G": "gas_mcf"})
        else:
            pivoted["gas_mcf"] = None

        # Ensure both columns exist
        if "oil_bbl" not in pivoted.columns:
            pivoted["oil_bbl"] = None
        if "gas_mcf" not in pivoted.columns:
            pivoted["gas_mcf"] = None

        # Convert to float64
        pivoted["oil_bbl"] = pivoted["oil_bbl"].astype("float64")
        pivoted["gas_mcf"] = pivoted["gas_mcf"].astype("float64")

        # Preserve is_suspect_rate if it exists
        if "is_suspect_rate" in df.columns and "is_suspect_rate" not in pivoted.columns:
            pivoted["is_suspect_rate"] = False

        return pivoted

    result = ddf.map_partitions(_pivot_partition)
    logger.info("Pivoted product columns")
    return result


def fill_date_gaps(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Fill missing monthly dates for each well with NaN production values.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with date gaps filled.

    Raises:
        KeyError: If required columns are missing.
    """
    required_cols = {"well_id", "production_date"}
    missing = required_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing columns for date gap filling: {missing}")

    # Repartition by well_id to ensure all rows for a well are in the same partition
    ddf_indexed = ddf.set_index("well_id").reset_index()

    def _fill_gaps_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Fill date gaps for wells in a single partition."""
        if df.empty:
            return df

        all_wells: list[pd.DataFrame] = []

        for well_id in df["well_id"].unique():
            well_df = df[df["well_id"] == well_id].copy()
            well_df = well_df.sort_values("production_date")

            if len(well_df) <= 1:
                all_wells.append(well_df)
                continue

            # Create full monthly date range
            first_date = well_df["production_date"].min()
            last_date = well_df["production_date"].max()
            full_date_range = pd.date_range(
                start=first_date, end=last_date, freq="MS"
            )

            # Reindex to full date range
            well_df = well_df.set_index("production_date")
            well_df = well_df.reindex(full_date_range)
            well_df = well_df.reset_index().rename(columns={"index": "production_date"})

            # Forward-fill non-production metadata
            non_prod_cols = [c for c in well_df.columns if c not in [
                "oil_bbl", "gas_mcf", "production_date"
            ]]
            for col in non_prod_cols:
                if col in well_df.columns:
                    well_df[col] = well_df[col].fillna(method="ffill")

            all_wells.append(well_df)

        result = pd.concat(all_wells, ignore_index=True) if all_wells else df
        return result

    result = ddf_indexed.map_partitions(_fill_gaps_partition)
    logger.info("Filled date gaps")
    return result


def compute_cumulative_production(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Compute cumulative production per well, sorted by production_date.

    Args:
        ddf: Input Dask DataFrame.

    Returns:
        Dask DataFrame with cumulative production columns added.

    Raises:
        KeyError: If required columns are missing.
    """
    required_cols = {"well_id", "production_date", "oil_bbl", "gas_mcf"}
    missing = required_cols - set(ddf.columns)
    if missing:
        raise KeyError(f"Missing columns for cumulative production: {missing}")

    # Sort by well_id and production_date
    ddf = ddf.sort_values(["well_id", "production_date"])

    def _cumsum_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Compute cumulative sums in a single partition."""
        df = df.copy()

        if df.empty:
            df["cumulative_oil_bbl"] = pd.Series(dtype="float64")
            df["cumulative_gas_mcf"] = pd.Series(dtype="float64")
            return df

        # Compute cumulative sums per well, treating NaN as 0 for cumsum
        df["cumulative_oil_bbl"] = df.groupby("well_id")["oil_bbl"].apply(
            lambda x: x.fillna(0).cumsum(), include_groups=False
        )
        df["cumulative_gas_mcf"] = df.groupby("well_id")["gas_mcf"].apply(
            lambda x: x.fillna(0).cumsum(), include_groups=False
        )

        return df

    result = ddf.map_partitions(_cumsum_partition)
    logger.info("Computed cumulative production")
    return result


def write_processed_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write the transformed Dask DataFrame to a well-partitioned Parquet dataset.

    Args:
        ddf: Input Dask DataFrame.
        output_dir: Output directory.

    Returns:
        Path to the wells output directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    wells_dir = output_dir / "wells"

    try:
        ddf.to_parquet(
            str(wells_dir),
            engine="pyarrow",
            compression="snappy",
            write_index=False,
            overwrite=True,
            partition_on=["well_id"],
        )
        logger.info(f"Wrote processed Parquet to {wells_dir}")
    except Exception as e:
        logger.error(f"Failed to write processed Parquet: {type(e).__name__}: {e}")
        raise

    return wells_dir


def run_transform_pipeline(interim_dir: Path, output_dir: Path) -> Path:
    """
    Top-level entry point for the transform component.

    Chains all transformation steps: read → cast → deduplicate → validate bounds →
    pivot → fill gaps → compute cumulative → write.

    Args:
        interim_dir: Path to interim data directory.
        output_dir: Path to processed output directory.

    Returns:
        Path to the processed Parquet output directory.
    """
    start_time = time.time()

    try:
        logger.info("Starting transform pipeline...")

        # Step 1: Read interim parquet
        step_start = time.time()
        ddf = read_interim_parquet(interim_dir)
        logger.info(f"Step 1 (read_interim_parquet) took {time.time() - step_start:.2f}s")

        # Step 2: Cast column types
        step_start = time.time()
        ddf = cast_column_types(ddf)
        logger.info(f"Step 2 (cast_column_types) took {time.time() - step_start:.2f}s")

        # Step 3: Deduplicate
        step_start = time.time()
        ddf = deduplicate(ddf)
        logger.info(f"Step 3 (deduplicate) took {time.time() - step_start:.2f}s")

        # Step 4: Validate physical bounds
        step_start = time.time()
        ddf = validate_physical_bounds(ddf)
        logger.info(f"Step 4 (validate_physical_bounds) took {time.time() - step_start:.2f}s")

        # Step 5: Pivot product columns
        step_start = time.time()
        ddf = pivot_product_columns(ddf)
        logger.info(f"Step 5 (pivot_product_columns) took {time.time() - step_start:.2f}s")

        # Step 6: Fill date gaps
        step_start = time.time()
        ddf = fill_date_gaps(ddf)
        logger.info(f"Step 6 (fill_date_gaps) took {time.time() - step_start:.2f}s")

        # Step 7: Compute cumulative production
        step_start = time.time()
        ddf = compute_cumulative_production(ddf)
        logger.info(f"Step 7 (compute_cumulative_production) took {time.time() - step_start:.2f}s")

        # Step 8: Write processed parquet
        step_start = time.time()
        output_path = write_processed_parquet(ddf, output_dir)
        logger.info(f"Step 8 (write_processed_parquet) took {time.time() - step_start:.2f}s")

        total_time = time.time() - start_time
        logger.info(f"Transform pipeline completed in {total_time:.2f}s")

        return output_path

    except Exception as e:
        logger.error(f"Transform pipeline failed: {type(e).__name__}: {e}")
        raise
