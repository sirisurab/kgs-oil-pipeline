import logging
import time
from pathlib import Path
from typing import Optional
import pandas as pd
import dask.dataframe as dd
from kgs_pipeline.config import RAW_DATA_DIR, INTERIM_DATA_DIR

logger = logging.getLogger(__name__)


def discover_raw_files(raw_dir: Path) -> list[Path]:
    """
    Scan raw_dir and return a sorted list of all .txt files available for ingestion.

    Args:
        raw_dir: Path to the raw data directory.

    Returns:
        Sorted list of Path objects for all .txt files in raw_dir.

    Raises:
        FileNotFoundError: If raw_dir does not exist.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    txt_files = sorted(raw_dir.glob("*.txt"))

    if not txt_files:
        logger.warning(f"No .txt files found in {raw_dir}")
        return []

    logger.info(f"Discovered {len(txt_files)} .txt files in {raw_dir}")
    return txt_files


def read_raw_files(file_paths: list[Path]) -> dd.DataFrame:
    """
    Read all per-lease .txt files into a single unified Dask DataFrame.

    Args:
        file_paths: List of Path objects pointing to .txt files.

    Returns:
        Dask DataFrame with all files concatenated and source_file column added.

    Raises:
        ValueError: If file_paths is empty.
    """
    if not file_paths:
        raise ValueError("No raw files provided for ingestion")

    dfs = []

    for file_path in file_paths:
        try:
            df = dd.read_csv(
                str(file_path),
                dtype=str,
                blocksize=None,
            )
            # Add source_file column with the stem of the filename
            df["source_file"] = file_path.stem
            dfs.append(df)
        except Exception as e:
            logger.warning(
                f"Failed to read {file_path.name}: {type(e).__name__}: {e}"
            )

    if not dfs:
        raise ValueError(
            "No files could be successfully read from the provided file list"
        )

    # Concatenate all DataFrames
    combined_df = dd.concat(dfs, axis=0, ignore_index=True)

    # Normalize column names: lowercase, replace - and spaces with _
    combined_df.columns = [
        col.strip().lower().replace("-", "_").replace(" ", "_")
        for col in combined_df.columns
    ]

    logger.info(f"Successfully read {len(file_paths)} files into Dask DataFrame")
    return combined_df


def parse_month_year(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Parse and filter the month_year column, converting to datetime and removing
    non-monthly records (yearly aggregates with Month=0 or cumulative Month=-1).

    Args:
        ddf: Input Dask DataFrame with month_year column.

    Returns:
        Dask DataFrame with month_year parsed to production_date (datetime64[ns]).
    """

    def _filter_and_parse_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Filter out non-monthly records and parse dates in a single partition."""
        # Filter: keep only rows where month_year doesn't start with "0-" or "-1-"
        mask = ~(df["month_year"].astype(str).str.startswith(("0-", "-1-")))
        df = df[mask].copy()

        if df.empty:
            # Return with production_date column as datetime, but empty
            df["production_date"] = pd.to_datetime([], dtype="datetime64[ns]")
            return df

        # Parse month_year to datetime
        try:
            df["production_date"] = pd.to_datetime(
                df["month_year"], format="%m-%Y", errors="coerce"
            )
        except Exception as e:
            logger.warning(f"Error parsing month_year: {e}")
            df["production_date"] = pd.NaT

        # Drop the original month_year column
        df = df.drop(columns=["month_year"])

        return df

    filtered_ddf = ddf.map_partitions(_filter_and_parse_partition)

    logger.info("Parsed month_year to production_date and filtered non-monthly records")
    return filtered_ddf


def explode_api_numbers(ddf: dd.DataFrame) -> dd.DataFrame:
    """
    Transform lease-level rows into well-level rows by exploding comma-separated
    api_number column.

    Args:
        ddf: Input Dask DataFrame with api_number column.

    Returns:
        Dask DataFrame with exploded api_number renamed to well_id.

    Raises:
        KeyError: If api_number column is missing.
    """
    if "api_number" not in ddf.columns:
        raise KeyError("api_number column not found in input DataFrame")

    def _explode_partition(df: pd.DataFrame) -> pd.DataFrame:
        """Explode api_number column in a single partition."""
        # Split on comma, strip whitespace, and explode
        df = df.copy()
        df["api_number"] = df["api_number"].fillna("").astype(str)
        df["api_number"] = df["api_number"].apply(
            lambda x: [s.strip() for s in x.split(",") if s.strip()]
            if x.strip()
            else [None]
        )
        df = df.explode("api_number", ignore_index=True)

        # Rename to well_id
        df = df.rename(columns={"api_number": "well_id"})

        return df

    exploded_ddf = ddf.map_partitions(_explode_partition)

    logger.info("Exploded api_number to well_id (well-level rows)")
    return exploded_ddf


def write_interim_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """
    Write the processed Dask DataFrame to a partitioned Parquet dataset.

    Args:
        ddf: Input Dask DataFrame to write.
        output_dir: Directory to write the Parquet dataset to.

    Returns:
        Path to the output Parquet directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "kgs_monthly_raw.parquet"

    try:
        ddf.to_parquet(
            str(output_path),
            engine="pyarrow",
            compression="snappy",
            write_index=False,
            overwrite=True,
        )
        logger.info(
            f"Successfully wrote {ddf.npartitions} partitions to {output_path}"
        )
    except Exception as e:
        logger.error(f"Failed to write Parquet to {output_path}: {e}")
        raise

    return output_path


def run_ingest_pipeline(raw_dir: Path, output_dir: Path) -> Optional[Path]:
    """
    Top-level entry point for the ingest component.

    Chains: discover → read → parse_month_year → explode_api_numbers → write

    Args:
        raw_dir: Path to the raw data directory.
        output_dir: Path to the interim output directory.

    Returns:
        Path to the output Parquet file, or None if no files were discovered.
    """
    start_time = time.time()

    try:
        # Step 1: Discover raw files
        logger.info("Starting ingest pipeline...")
        step_start = time.time()
        file_list = discover_raw_files(raw_dir)
        logger.info(f"Step 1 (discover_raw_files) took {time.time() - step_start:.2f}s")

        if not file_list:
            logger.warning("No raw files discovered; aborting ingest pipeline")
            return None

        # Step 2: Read raw files
        step_start = time.time()
        ddf = read_raw_files(file_list)
        logger.info(f"Step 2 (read_raw_files) took {time.time() - step_start:.2f}s")

        # Step 3: Parse month_year
        step_start = time.time()
        ddf = parse_month_year(ddf)
        logger.info(f"Step 3 (parse_month_year) took {time.time() - step_start:.2f}s")

        # Step 4: Explode api_numbers
        step_start = time.time()
        ddf = explode_api_numbers(ddf)
        logger.info(f"Step 4 (explode_api_numbers) took {time.time() - step_start:.2f}s")

        # Step 5: Write interim parquet
        step_start = time.time()
        output_path = write_interim_parquet(ddf, output_dir)
        logger.info(f"Step 5 (write_interim_parquet) took {time.time() - step_start:.2f}s")

        total_time = time.time() - start_time
        logger.info(f"Ingest pipeline completed in {total_time:.2f}s")

        return output_path

    except Exception as e:
        logger.error(f"Ingest pipeline failed: {type(e).__name__}: {e}")
        raise
