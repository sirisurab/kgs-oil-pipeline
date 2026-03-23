"""Ingest component — reads raw KGS lease files into a unified Dask DataFrame."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

import kgs_pipeline.config as config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task 06: Raw file discovery
# ---------------------------------------------------------------------------


def discover_raw_files(raw_dir: Path) -> list[Path]:
    """Return a sorted list of all .txt files in raw_dir.

    Args:
        raw_dir: Directory to scan.

    Returns:
        Sorted list of .txt Path objects.

    Raises:
        FileNotFoundError: If raw_dir does not exist.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")

    files = sorted(raw_dir.glob("*.txt"))

    if not files:
        logger.warning("No .txt files found in %s", raw_dir)
        return []

    logger.info("Discovered %d raw .txt files in %s", len(files), raw_dir)
    return files


# ---------------------------------------------------------------------------
# Task 07: Single raw file reader
# ---------------------------------------------------------------------------


def _add_source_file_col(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """Partition-level helper: adds the source_file column."""
    df = df.copy()
    df["source_file"] = source_name
    return df


def read_raw_file(file_path: Path) -> dd.DataFrame:
    """Read a single KGS monthly production .txt file into a lazy Dask DataFrame.

    Args:
        file_path: Path to the .txt file.

    Returns:
        Lazy Dask DataFrame with LEASE_KID and source_file columns.

    Raises:
        FileNotFoundError: If file_path does not exist.
        ValueError: If the file cannot be parsed after both encoding attempts.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Raw file not found: {file_path}")

    source_name = file_path.name

    # Attempt UTF-8 first, fall back to latin-1
    ddf: dd.DataFrame | None = None
    for encoding in ("utf-8", "latin-1"):
        try:
            candidate = dd.read_csv(
                str(file_path),
                encoding=encoding,
                blocksize=None,
                dtype=str,
                on_bad_lines="warn",
            )
            # Trigger schema inference to catch encoding issues early
            _ = candidate.dtypes
            ddf = candidate
            break
        except (UnicodeDecodeError, Exception) as exc:
            if encoding == "utf-8":
                logger.debug("UTF-8 read failed for %s, retrying with latin-1: %s", file_path, exc)
            else:
                logger.error("Failed to parse %s with latin-1: %s", file_path, exc)
                raise ValueError(f"Cannot parse file: {file_path}") from exc

    if ddf is None:
        raise ValueError(f"Cannot parse file: {file_path}")

    # Rename "LEASE KID" -> "LEASE_KID" if present
    cols = list(ddf.columns)
    if "LEASE KID" in cols:
        ddf = ddf.rename(columns={"LEASE KID": "LEASE_KID"})
    elif "LEASE_KID" not in ddf.columns:
        logger.warning("Neither 'LEASE KID' nor 'LEASE_KID' column found in %s", file_path)

    # Add source_file column via map_partitions
    meta = ddf._meta.copy()
    meta["source_file"] = pd.Series(dtype="object")

    ddf = ddf.map_partitions(_add_source_file_col, source_name, meta=meta)
    return ddf


# ---------------------------------------------------------------------------
# Task 08: Multi-file concatenation
# ---------------------------------------------------------------------------


def concatenate_raw_files(file_paths: list[Path]) -> dd.DataFrame:
    """Concatenate multiple KGS raw files into a single lazy Dask DataFrame.

    Args:
        file_paths: List of .txt file Paths returned by discover_raw_files().

    Returns:
        Concatenated lazy Dask DataFrame.

    Raises:
        ValueError: If file_paths is empty or all files fail to parse.
    """
    if not file_paths:
        raise ValueError("file_paths must not be empty")

    valid_ddfs: list[dd.DataFrame] = []
    skipped = 0

    for fp in file_paths:
        try:
            ddf = read_raw_file(fp)
            valid_ddfs.append(ddf)
        except ValueError as exc:
            logger.error("Skipping malformed file %s: %s", fp, exc)
            skipped += 1

    logger.info(
        "Read %d files successfully, skipped %d malformed files",
        len(valid_ddfs),
        skipped,
    )

    if not valid_ddfs:
        raise ValueError("No valid raw files could be read")

    return dd.concat(valid_ddfs)


# ---------------------------------------------------------------------------
# Task 09: Monthly record filter
# ---------------------------------------------------------------------------


def filter_monthly_records(ddf: dd.DataFrame) -> dd.DataFrame:
    """Filter out yearly (Month=0) and starting-cumulative (Month=-1) records.

    Args:
        ddf: Lazy Dask DataFrame with a MONTH-YEAR column.

    Returns:
        Filtered lazy Dask DataFrame retaining only Month 1–12 records.

    Raises:
        KeyError: If the MONTH-YEAR column is absent.
    """
    if "MONTH-YEAR" not in ddf.columns:
        raise KeyError("Required column 'MONTH-YEAR' is absent from the DataFrame")

    logger.info("Filtering non-monthly records (Month=0 and Month=-1)")

    def _filter_partition(df: pd.DataFrame) -> pd.DataFrame:
        month_str = df["MONTH-YEAR"].astype(str).str.split("-", n=1).str[0]
        try:
            month_int = pd.to_numeric(month_str, errors="coerce")
        except Exception:
            month_int = pd.Series([float("nan")] * len(df), index=df.index)
        mask = month_int.between(1, 12, inclusive="both")
        return df[mask]

    meta = ddf._meta
    return ddf.map_partitions(_filter_partition, meta=meta)


# ---------------------------------------------------------------------------
# Task 10: Lease metadata enrichment
# ---------------------------------------------------------------------------


def enrich_with_lease_metadata(
    ddf: dd.DataFrame, lease_index_file: Path
) -> dd.DataFrame:
    """Left-join monthly production data with lease-level metadata.

    Args:
        ddf: Lazy Dask DataFrame with LEASE_KID column.
        lease_index_file: Path to the KGS lease archive index file.

    Returns:
        Enriched lazy Dask DataFrame.

    Raises:
        FileNotFoundError: If lease_index_file does not exist.
        KeyError: If LEASE_KID is absent from either DataFrame.
    """
    if not lease_index_file.exists():
        raise FileNotFoundError(f"Lease index file not found: {lease_index_file}")

    if "LEASE_KID" not in ddf.columns:
        raise KeyError("LEASE_KID column absent from production DataFrame")

    # Read metadata into pandas
    meta_df = pd.read_csv(lease_index_file, sep=",", dtype=str, low_memory=False)
    if meta_df.shape[1] == 1:
        meta_df = pd.read_csv(lease_index_file, sep="\t", dtype=str, low_memory=False)
    meta_df.columns = [c.strip() for c in meta_df.columns]

    if "LEASE KID" in meta_df.columns:
        meta_df = meta_df.rename(columns={"LEASE KID": "LEASE_KID"})

    if "LEASE_KID" not in meta_df.columns:
        raise KeyError("LEASE_KID column absent from lease metadata DataFrame")

    # Determine columns to add (exclude columns already in ddf)
    existing_cols = set(ddf.columns)
    meta_cols = ["LEASE_KID"] + [
        c for c in meta_df.columns if c not in existing_cols and c != "LEASE_KID"
    ]
    meta_df = meta_df[meta_cols].drop_duplicates(subset=["LEASE_KID"])

    meta_ddf = dd.from_pandas(meta_df, npartitions=1)

    enriched = ddf.merge(meta_ddf, on="LEASE_KID", how="left", suffixes=("", "_meta"))

    # Drop any _meta suffixed duplicate columns
    drop_cols = [c for c in enriched.columns if c.endswith("_meta")]
    if drop_cols:
        enriched = enriched.drop(columns=drop_cols)

    logger.info("Enriched production data with metadata from %s", lease_index_file)
    return enriched


# ---------------------------------------------------------------------------
# Task 11: Interim Parquet writer and pipeline orchestrator
# ---------------------------------------------------------------------------


def write_interim_parquet(ddf: dd.DataFrame, output_dir: Path) -> Path:
    """Write enriched Dask DataFrame to Parquet, partitioned by LEASE_KID.

    Args:
        ddf: Lazy Dask DataFrame with LEASE_KID column.
        output_dir: Directory where Parquet files are written.

    Returns:
        output_dir as a Path.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    ddf = ddf.set_index("LEASE_KID", drop=False, sorted=False)

    ddf.to_parquet(
        str(output_dir),
        engine="pyarrow",
        write_index=True,
        overwrite=True,
    )

    parquet_files = list(output_dir.glob("*.parquet"))
    logger.info(
        "Wrote interim Parquet to %s (%d files)", output_dir, len(parquet_files)
    )
    return output_dir


def run_ingest_pipeline() -> Path:
    """Orchestrate the full ingest workflow end-to-end.

    Returns:
        Path to the interim Parquet directory.

    Raises:
        RuntimeError: If no raw files are found to ingest.
    """
    start = time.time()
    logger.info("Ingest pipeline starting")

    file_paths = discover_raw_files(config.RAW_DATA_DIR)

    if not file_paths:
        logger.error("No raw files found in %s", config.RAW_DATA_DIR)
        raise RuntimeError("No raw files to ingest")

    ddf = concatenate_raw_files(file_paths)
    ddf = filter_monthly_records(ddf)
    ddf = enrich_with_lease_metadata(ddf, config.LEASE_INDEX_FILE)
    output = write_interim_parquet(ddf, config.INTERIM_DATA_DIR)

    elapsed = time.time() - start
    logger.info("Ingest pipeline complete in %.1fs — output: %s", elapsed, output)
    return output
