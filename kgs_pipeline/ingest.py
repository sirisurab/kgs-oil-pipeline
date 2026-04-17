"""Ingest stage: load raw KGS lease production files and write consolidated Parquet."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dask.bag as db
import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Canonical non-nullable columns (absent → raise ValueError)
_NON_NULLABLE = {"LEASE_KID", "MONTH-YEAR", "PRODUCT"}

# Column to drop from source files (not part of canonical monthly schema)
_DROP_COLS = {"URL"}


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class IngestConfig:
    """Configuration for the ingest stage."""

    input_dir: Path
    output_dir: Path
    dict_path: Path


@dataclass
class IngestSummary:
    """Summary of an ingest stage run."""

    n_files_ingested: int
    output_dir: Path
    partition_count: int


# ---------------------------------------------------------------------------
# Task 01: Schema loader
# ---------------------------------------------------------------------------


def load_schema(dict_path: str | Path) -> dict[str, dict]:
    """Read the data dictionary CSV and return column schema metadata.

    Args:
        dict_path: Path to the data dictionary CSV file.

    Returns:
        Mapping from column name to {dtype, nullable, categories}.

    Raises:
        FileNotFoundError: If dict_path does not exist.
        ValueError: If required columns are absent from the CSV.
    """
    path = Path(dict_path)
    if not path.exists():
        raise FileNotFoundError(f"Data dictionary not found: {path}")

    df = pd.read_csv(path)
    required = {"column", "dtype", "nullable"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Data dictionary missing required columns: {sorted(missing)}")

    schema: dict[str, dict] = {}
    for _, row in df.iterrows():
        col_name: str = str(row["column"])
        dtype_str: str = str(row["dtype"])
        nullable: bool = str(row["nullable"]).strip().lower() == "yes"
        cats_raw = str(row.get("categories", "")).strip()
        categories: list[str] | None = cats_raw.split("|") if cats_raw else None
        schema[col_name] = {
            "dtype": dtype_str,
            "nullable": nullable,
            "categories": categories,
        }
    return schema


# ---------------------------------------------------------------------------
# Task 02: Pandas dtype resolver
# ---------------------------------------------------------------------------


def resolve_pandas_dtype(
    dtype_str: str,
    nullable: bool,
    categories: list[str] | None,
) -> Any:
    """Convert data dictionary dtype string to the correct pandas dtype.

    Args:
        dtype_str: One of "int", "float", "string", "categorical".
        nullable: Whether the column is nullable.
        categories: List of allowed category values for categorical dtype.

    Returns:
        Pandas dtype object.

    Raises:
        ValueError: If dtype_str is not recognized.
    """
    if dtype_str == "int":
        return pd.Int64Dtype()
    elif dtype_str == "float":
        return np.float64
    elif dtype_str == "string":
        return pd.StringDtype()
    elif dtype_str == "categorical":
        if categories:
            return pd.CategoricalDtype(categories=categories, ordered=False)
        return pd.CategoricalDtype()
    else:
        raise ValueError(f"Unrecognized dtype string: '{dtype_str}'")


# ---------------------------------------------------------------------------
# Task 03: Single-file reader
# ---------------------------------------------------------------------------


def read_raw_file(file_path: Path, schema: dict[str, dict]) -> pd.DataFrame:
    """Read one raw KGS .txt file into a pandas DataFrame with canonical schema.

    Args:
        file_path: Path to the raw .txt file.
        schema: Column schema from load_schema().

    Returns:
        DataFrame with canonical schema, source_file column, and year >= 2024 rows.

    Raises:
        FileNotFoundError: If file_path does not exist.
        ValueError: If file cannot be parsed or a non-nullable column is absent.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Raw file not found: {file_path}")

    try:
        df = pd.read_csv(file_path, low_memory=False)
    except pd.errors.ParserError as exc:
        raise ValueError(f"Failed to parse {file_path.name}: {exc}") from exc

    # Drop URL column if present
    for col in _DROP_COLS:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Check non-nullable columns — absent → raise immediately
    non_nullable_absent = [
        col
        for col, meta in schema.items()
        if not meta["nullable"] and col not in df.columns and col != "source_file"
    ]
    if non_nullable_absent:
        raise ValueError(
            f"Non-nullable columns absent from {file_path.name}: {non_nullable_absent}"
        )

    # Add absent nullable columns as all-NA at the resolved dtype
    for col, meta in schema.items():
        if col == "source_file":
            continue
        if col in _DROP_COLS:
            continue
        if col not in df.columns and meta["nullable"]:
            resolved = resolve_pandas_dtype(meta["dtype"], meta["nullable"], meta["categories"])
            if pd.api.types.is_float_dtype(resolved):
                df[col] = np.nan
            elif pd.api.types.is_integer_dtype(resolved):
                df[col] = pd.array([pd.NA] * len(df), dtype=resolved)
            else:
                df[col] = pd.NA

    # Cast each column to its resolved dtype
    for col, meta in schema.items():
        if col == "source_file" or col not in df.columns:
            continue
        resolved = resolve_pandas_dtype(meta["dtype"], meta["nullable"], meta["categories"])
        if meta["dtype"] == "categorical" and meta["categories"]:
            # Replace out-of-set values with NA before casting
            valid = set(meta["categories"])
            df[col] = df[col].where(df[col].astype(str).isin(valid), other=None)  # type: ignore[call-overload]
        try:
            df[col] = df[col].astype(resolved)
        except (ValueError, TypeError):
            logger.warning("Could not cast column %s in %s to %s", col, file_path.name, resolved)

    # Add source_file column
    df["source_file"] = pd.array([file_path.name] * len(df), dtype=pd.StringDtype())

    # Year filter: extract year from MONTH-YEAR
    if "MONTH-YEAR" in df.columns:
        year_parts = df["MONTH-YEAR"].astype(str).str.split("-").str[-1]
        numeric_mask = year_parts.str.isnumeric()
        year_ints = pd.to_numeric(year_parts.where(numeric_mask), errors="coerce")
        keep_mask = numeric_mask & (year_ints >= 2024)
        df = df[keep_mask].reset_index(drop=True)

    return df


# ---------------------------------------------------------------------------
# Task 04: Dask-based parallel file ingestion
# ---------------------------------------------------------------------------


def _build_meta_schema(schema: dict[str, dict]) -> pd.DataFrame:
    """Build an empty pandas DataFrame with the canonical schema plus source_file."""
    cols: dict[str, Any] = {}
    for col, meta in schema.items():
        if col == "URL":
            continue
        resolved = resolve_pandas_dtype(meta["dtype"], meta["nullable"], meta["categories"])
        cols[col] = pd.array([], dtype=resolved)
    cols["source_file"] = pd.array([], dtype=pd.StringDtype())
    return pd.DataFrame(cols)


def ingest_raw_files(
    input_dir: Path,
    schema: dict[str, dict],
    client: Any,  # distributed.Client
) -> dd.DataFrame:
    """Discover .txt files in input_dir and return a lazy Dask DataFrame.

    Args:
        input_dir: Directory containing raw .txt files.
        schema: Column schema from load_schema().
        client: Dask distributed Client for scheduling.

    Returns:
        Lazy Dask DataFrame (do not call .compute()).

    Raises:
        FileNotFoundError: If input_dir does not exist or no .txt files found.
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    files = sorted(input_dir.glob("*.txt"))
    n_files = len(files)
    logger.info("Found %d raw .txt files in %s", n_files, input_dir)

    if n_files == 0:
        raise FileNotFoundError(f"No .txt files found in {input_dir}")

    meta = _build_meta_schema(schema)

    # Use Dask bag + distributed client (CPU-bound, ADR-001)
    bag = db.from_sequence(files, npartitions=max(1, min(n_files, 50)))
    delayed_dfs = bag.map(lambda fp: read_raw_file(fp, schema)).to_delayed()
    ddf = dd.from_delayed(delayed_dfs, meta=meta)
    return ddf


# ---------------------------------------------------------------------------
# Task 05: Parquet writer
# ---------------------------------------------------------------------------


def write_interim_parquet(ddf: dd.DataFrame, output_dir: Path, n_files: int) -> None:
    """Repartition and write the Dask DataFrame as partitioned Parquet.

    Args:
        ddf: Lazy Dask DataFrame to write.
        output_dir: Output directory for Parquet files.
        n_files: Number of source files (used to compute partition count).
    """
    partition_count = max(10, min(n_files, 50))
    output_dir.mkdir(parents=True, exist_ok=True)
    ddf = ddf.repartition(npartitions=partition_count)
    ddf.to_parquet(str(output_dir), write_index=False, overwrite=True)
    logger.info("Wrote %d Parquet partitions to %s", partition_count, output_dir)


# ---------------------------------------------------------------------------
# Task 06: Ingest stage entry point
# ---------------------------------------------------------------------------


def run_ingest(config: IngestConfig, client: Any) -> IngestSummary:
    """Orchestrate the full ingest stage.

    Args:
        config: IngestConfig with all ingest settings.
        client: Dask distributed Client.

    Returns:
        IngestSummary with file count and partition info.
    """
    schema = load_schema(config.dict_path)

    input_dir = Path(config.input_dir)
    files = sorted(input_dir.glob("*.txt"))
    n_files = len(files)

    ddf = ingest_raw_files(input_dir, schema, client)

    output_dir = Path(config.output_dir)
    write_interim_parquet(ddf, output_dir, n_files)

    partition_count = max(10, min(n_files, 50))
    return IngestSummary(
        n_files_ingested=n_files,
        output_dir=output_dir,
        partition_count=partition_count,
    )
