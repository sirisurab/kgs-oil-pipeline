"""
Ingest component: Read, parse, and combine raw .txt files into interim partitioned Parquet.
"""

import logging
from pathlib import Path
from typing import Optional
import pandas as pd  # type: ignore
import dask.dataframe as dd  # type: ignore

from kgs_pipeline.config import RAW_DATA_DIR, INTERIM_DATA_DIR

logger = logging.getLogger(__name__)


def discover_raw_files(raw_dir: Path) -> list[Path]:
    """
    Scan the raw_dir directory and return a sorted list of all .txt files.

    Args:
        raw_dir: Directory to scan for .txt files.

    Returns:
        Sorted list of Path objects for .txt files.

    Raises:
        FileNotFoundError: If raw_dir does not exist.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")

    txt_files = sorted(raw_dir.glob("*.txt"))

    if not txt_files:
        logger.warning(f"No .txt files found in {raw_dir}")
        return []

    logger.info(f"Discovered {len(txt_files)} raw files in {raw_dir}")
    return txt_files


def read_raw_files(file_paths: list[Path]) -> dd.DataFrame:
    """
    Read all per-lease .txt files into a single unified Dask DataFrame.

    Args:
        file_paths: List of Path objects to read.

    Returns:
        Dask DataFrame with all rows from input files.

    Raises:
        ValueError: If file_paths is empty.
    """
    if not file_paths:
        raise ValueError("No raw files provided for ingestion")

    dfs = []
    for fpath in file_paths:
        try:
            # Read file with all columns as strings
            df = dd.read_csv(
                str(fpath),
                dtype=str,
                blocksize=None,  # Don't split single files
            )
            # Add source_file column
            df["source_file"] = fpath.stem
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Error reading file {fpath}: {e}")

    if not dfs:
        raise ValueError("No files could be successfully read")

    # Concatenate all dataframes
    result = dd.concat(dfs, axis=0, interleave_partitions=True)

    # Normalize column names: lowercase, replace hyphens and spaces with underscores
    result.columns = [
        col.strip().lower().replace("-", "_").replace(" ", "_")
        for col in result.columns
    ]

    logger.info(f"Read {len(file_paths)} files into Dask DataFrame")
    return result


def parse_month_year(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Parse month_year column and filter out non-monthly records.

    Args:
        ddf: Dask DataFrame with month_year column.

    Returns:
        Dask DataFrame with production_date column and monthly records only.
    """

    def _parse_month_year_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Parse month_year for a single partition."""
        # Filter out yearly (0-YYYY) and cumulative (-1-YYYY) records
        df = df[~df["month_year"].str.startswith("0-")]
        df = df[~df["month_year"].str.startswith("-1-")]

        # Parse month_year to datetime
        df["production_date"] = pd.to_datetime(
            df["month_year"], format="%m-%Y", errors="coerce"
        )

        # Drop the old month_year column
        df = df.drop(columns=["month_year"])

        return df

    # Create meta for the result
    meta = ddf._meta.copy()
    meta = meta.drop(columns=["month_year"])
    meta["production_date"] = pd.Series([], dtype="datetime64[ns]")

    result = ddf.map_partitions(_parse_month_year_partition, meta=meta)

    logger.info("Parsed month_year and filtered non-monthly records")
    return result


def explode_api_numbers(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Explode comma-separated API numbers into separate well rows.

    Args:
        ddf: Dask DataFrame with api_number column.

    Returns:
        Dask DataFrame with well_id column (exploded from api_number).

    Raises:
        KeyError: If api_number column is missing.
    """
    if "api_number" not in ddf.columns:
        raise KeyError("api_number column not found in DataFrame")

    def _explode_api_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Explode API numbers for a single partition."""
        # Split and explode the api_number column
        df = df.copy()
        df["api_number"] = df["api_number"].fillna("")
        df["api_number"] = df["api_number"].str.split(",")

        # Explode
        df = df.explode("api_number", ignore_index=False)

        # Strip whitespace and rename
        df["well_id"] = df["api_number"].str.strip()
        df = df.drop(columns=["api_number"])

        # Reset index
        df = df.reset_index(drop=True)

        return df

    # Get meta (schema) for output
    meta = ddf._meta.copy()
    meta = meta.drop(columns=["api_number"])
    meta["well_id"] = ""

    result = ddf.map_partitions(_explode_api_partition, meta=meta)
    logger.info("Exploded API numbers to well_id")
    return result


def write_interim_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write the Dask DataFrame to a partitioned Parquet dataset.

    Args:
        ddf: Dask DataFrame to write.
        output_dir: Directory to write Parquet files to.

    Returns:
        Path to the output Parquet directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_parquet = output_dir / "kgs_monthly_raw.parquet"

    try:
        ddf.to_parquet(
            str(output_parquet),
            write_index=False,
            compression="snappy",
            overwrite=True,
        )
        logger.info(f"Wrote interim Parquet to {output_parquet}")
        return output_parquet
    except Exception as e:
        logger.error(f"Error writing Parquet: {e}")
        raise


def run_ingest_pipeline(
    raw_dir: Path = RAW_DATA_DIR,
    output_dir: Path = INTERIM_DATA_DIR,
) -> Optional[Path]:
    """
    Orchestrate the full ingest pipeline.

    Args:
        raw_dir: Directory containing raw .txt files.
        output_dir: Directory to write interim Parquet.

    Returns:
        Path to the interim Parquet directory, or None if no files to ingest.
    """
    logger.info("Starting ingest pipeline")

    # Step 1: Discover files
    file_paths = discover_raw_files(raw_dir)
    if not file_paths:
        logger.warning("No raw files discovered; aborting ingest pipeline")
        return None

    # Step 2: Read files
    logger.info("Reading raw files...")
    ddf = read_raw_files(file_paths)

    # Step 3: Parse month_year
    logger.info("Parsing production dates...")
    ddf = parse_month_year(ddf)

    # Step 4: Explode API numbers
    logger.info("Exploding API numbers to well level...")
    ddf = explode_api_numbers(ddf)

    # Step 5: Write to interim Parquet
    logger.info("Writing interim Parquet...")
    output_path = write_interim_parquet(ddf, output_dir)

    logger.info(f"Ingest pipeline complete. Output: {output_path}")
    return output_path
