"""Ingest component: read and combine raw KGS data into unified interim format."""

import logging
from pathlib import Path

import dask.dataframe as dd  # type: ignore
import pandas as pd

from kgs_pipeline.config import (
    EXTERNAL_DATA_DIR,
    INTERIM_DATA_DIR,
    LEASE_INDEX_FILE,
    RAW_DATA_DIR,
)

logger = logging.getLogger(__name__)


def read_raw_lease_files(raw_dir: Path = RAW_DATA_DIR) -> dd.DataFrame:  # type: ignore
    """
    Read all per-lease .txt files from raw directory as a Dask DataFrame.

    Parameters
    ----------
    raw_dir : Path
        Directory containing lp*.txt files.

    Returns
    -------
    dask.dataframe.DataFrame
        Lazy Dask DataFrame with all raw lease data.

    Raises
    ------
    FileNotFoundError
        If raw_dir does not exist or contains no lp*.txt files.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")

    glob_pattern = str(raw_dir / "lp*.txt")

    # Try to read files; catch if none match
    try:
        ddf = dd.read_csv(glob_pattern, dtype=str, assume_missing=True)  # type: ignore
    except (ValueError, FileNotFoundError) as e:
        raise FileNotFoundError(
            f"No raw lease files found matching {glob_pattern}"
        ) from e

    # Better approach: read files individually and concatenate with source
    files = sorted(raw_dir.glob("lp*.txt"))
    if not files:
        raise FileNotFoundError(f"No raw lease files found in {raw_dir}")

    dfs = []
    for file in files:
        df = pd.read_csv(file, dtype=str)
        df["source_file"] = file.name
        dfs.append(df)

    raw_df = pd.concat(dfs, ignore_index=True)  # type: ignore
    ddf = dd.from_pandas(raw_df, npartitions=max(1, len(files) // 4 or 1))

    logger.info(
        f"Read {len(files)} raw lease files from {raw_dir} with {len(ddf)} rows"
    )
    return ddf


def read_lease_index(lease_index_path: Path = LEASE_INDEX_FILE) -> dd.DataFrame:  # type: ignore
    """
    Read lease index and return metadata for enrichment.

    Parameters
    ----------
    lease_index_path : Path
        Path to lease index file.

    Returns
    -------
    dask.dataframe.DataFrame
        Lazy Dask DataFrame with lease metadata (deduplicated by LEASE_KID).

    Raises
    ------
    FileNotFoundError
        If lease_index_path does not exist.
    KeyError
        If required columns are missing.
    """
    if not lease_index_path.exists():
        raise FileNotFoundError(f"Lease index file not found: {lease_index_path}")

    ddf = dd.read_csv(lease_index_path, dtype=str, assume_missing=True)  # type: ignore

    # Check required columns
    required = [
        "LEASE_KID",
        "LEASE",
        "DOR_CODE",
        "FIELD",
        "PRODUCING_ZONE",
        "OPERATOR",
        "COUNTY",
        "TOWNSHIP",
        "TWN_DIR",
        "RANGE",
        "RANGE_DIR",
        "SECTION",
        "SPOT",
        "LATITUDE",
        "LONGITUDE",
    ]
    missing = [col for col in required if col not in ddf.columns]
    if missing:
        raise KeyError(f"Missing columns in lease index: {missing}")

    # Rename MONTH-YEAR to MONTH_YEAR
    if "MONTH-YEAR" in ddf.columns:
        ddf = ddf.rename(columns={"MONTH-YEAR": "MONTH_YEAR"})

    # Filter out yearly (month=0) and starting cumulative (month=-1)
    def filter_non_monthly(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore
        if "MONTH_YEAR" in df.columns:
            # Parse month from "M-YYYY" format
            month = df["MONTH_YEAR"].str.split("-").str[0]
            df = df[~month.isin(["0", "-1"])]
        return df

    ddf = ddf.map_partitions(filter_non_monthly)

    # Select metadata columns only
    metadata_cols = required
    ddf = ddf[metadata_cols]

    # Deduplicate on LEASE_KID (keep first)
    ddf = ddf.drop_duplicates(subset=["LEASE_KID"], keep="first")

    logger.info(f"Read lease index from {lease_index_path}")
    return ddf


def filter_monthly_records(ddf: dd.DataFrame) -> dd.DataFrame:  # type: ignore
    """
    Filter out yearly and starting cumulative records.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Input Dask DataFrame.

    Returns
    -------
    dask.dataframe.DataFrame
        Filtered lazy Dask DataFrame with only monthly records.

    Raises
    ------
    KeyError
        If MONTH_YEAR or MONTH-YEAR column not found.
    """
    # Handle both column name variants
    month_col = None
    if "MONTH_YEAR" in ddf.columns:
        month_col = "MONTH_YEAR"
    elif "MONTH-YEAR" in ddf.columns:
        month_col = "MONTH-YEAR"
        ddf = ddf.rename(columns={"MONTH-YEAR": "MONTH_YEAR"})
        month_col = "MONTH_YEAR"
    else:
        raise KeyError("Expected column 'MONTH_YEAR' or 'MONTH-YEAR' not found")

    def filter_records(df: pd.DataFrame) -> pd.DataFrame:  # type: ignore
        # Parse month from "M-YYYY" format
        month = df[month_col].str.split("-", expand=True)[0]
        # Keep only valid months (not "0" or "-1")
        return df[~month.isin(["0", "-1"]) & df[month_col].notna()]

    return ddf.map_partitions(filter_records)


def merge_with_metadata(
    raw_ddf: dd.DataFrame, metadata_ddf: dd.DataFrame  # type: ignore
) -> dd.DataFrame:  # type: ignore
    """
    Left-join raw production data with lease metadata.

    Parameters
    ----------
    raw_ddf : dask.dataframe.DataFrame
        Raw per-lease production data.
    metadata_ddf : dask.dataframe.DataFrame
        Lease metadata (deduplicated by LEASE_KID).

    Returns
    -------
    dask.dataframe.DataFrame
        Merged lazy Dask DataFrame.

    Raises
    ------
    KeyError
        If LEASE_KID is missing from either DataFrame.
    """
    if "LEASE_KID" not in raw_ddf.columns:
        raise KeyError("LEASE_KID column missing from raw DataFrame")
    if "LEASE_KID" not in metadata_ddf.columns:
        raise KeyError("LEASE_KID column missing from metadata DataFrame")

    # Merge on LEASE_KID (left join)
    merged = raw_ddf.merge(
        metadata_ddf, on="LEASE_KID", how="left", suffixes=("", "_meta")
    )

    # Drop _meta-suffixed columns (duplicates)
    meta_cols = [col for col in merged.columns if col.endswith("_meta")]
    merged = merged.drop(columns=meta_cols)

    logger.info("Merged raw data with metadata")
    return merged


def write_interim_parquet(
    ddf: dd.DataFrame, interim_dir: Path = INTERIM_DATA_DIR  # type: ignore
) -> None:
    """
    Write interim Parquet store partitioned by LEASE_KID.

    Parameters
    ----------
    ddf : dask.dataframe.DataFrame
        Data to persist.
    interim_dir : Path
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

    interim_dir.mkdir(parents=True, exist_ok=True)

    try:
        ddf.to_parquet(
            str(interim_dir),
            partition_on=["LEASE_KID"],
            write_index=False,
            overwrite=True,
            engine="pyarrow",
        )
        logger.info(f"Wrote interim Parquet to {interim_dir}")
    except OSError as e:
        raise OSError(f"Failed to write interim Parquet to {interim_dir}: {e}") from e


def run_ingest_pipeline(
    raw_dir: Path = RAW_DATA_DIR,
    lease_index_path: Path = LEASE_INDEX_FILE,
    interim_dir: Path = INTERIM_DATA_DIR,
) -> None:
    """
    Orchestrate full ingest pipeline.

    Parameters
    ----------
    raw_dir : Path
        Directory with raw .txt files.
    lease_index_path : Path
        Path to lease index file.
    interim_dir : Path
        Output directory for interim Parquet.
    """
    logger.info("Starting ingest pipeline")

    # Read raw files
    logger.info(f"Reading raw lease files from {raw_dir}")
    raw_ddf = read_raw_lease_files(raw_dir)

    # Filter monthly records
    logger.info("Filtering monthly records")
    filtered_ddf = filter_monthly_records(raw_ddf)

    # Read metadata
    logger.info(f"Reading lease metadata from {lease_index_path}")
    metadata_ddf = read_lease_index(lease_index_path)

    # Merge
    logger.info("Merging with metadata")
    merged_ddf = merge_with_metadata(filtered_ddf, metadata_ddf)

    # Write
    logger.info(f"Writing interim Parquet to {interim_dir}")
    write_interim_parquet(merged_ddf, interim_dir)

    logger.info("Ingest pipeline complete")
