"""Ingest stage: read raw KGS files, enforce schema, write interim Parquet."""

from __future__ import annotations

import glob as glob_module
import logging
import os
from pathlib import Path

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class SchemaError(Exception):
    """Raised when a non-nullable column is missing or has unrecoverable cast failures."""


# ---------------------------------------------------------------------------
# Task I-01: Data dictionary loader
# ---------------------------------------------------------------------------


def load_data_dictionary(dict_path: str) -> dict[str, dict]:
    """Read the KGS data dictionary CSV and return a column→spec mapping.

    Each value contains: dtype (str), nullable (bool), categories (list[str]).
    """
    path = Path(dict_path)
    if not path.exists():
        raise FileNotFoundError(f"Data dictionary not found: {dict_path}")

    df = pd.read_csv(dict_path)

    for col in ("column", "dtype", "nullable"):
        if col not in df.columns:
            raise KeyError(f"Required column '{col}' missing from data dictionary")

    result: dict[str, dict] = {}
    for _, row in df.iterrows():
        col_name = str(row["column"])
        dtype_str = str(row["dtype"])
        nullable = str(row["nullable"]).strip().lower() == "yes"
        raw_cats = row.get("categories", "")
        if pd.isna(raw_cats) or str(raw_cats).strip() == "":
            categories: list[str] = []
        else:
            categories = [c.strip() for c in str(raw_cats).split("|") if c.strip()]
        result[col_name] = {
            "dtype": dtype_str,
            "nullable": nullable,
            "categories": categories,
        }
    return result


# ---------------------------------------------------------------------------
# Task I-02: Dtype resolver
# ---------------------------------------------------------------------------


def resolve_pandas_dtype(
    dtype_str: str,
    nullable: bool,
    categories: list[str],
) -> object:
    """Convert a data dictionary dtype string to a pandas-compatible dtype object."""
    if dtype_str == "int":
        if nullable:
            return pd.Int64Dtype()
        else:
            return np.dtype("int64")
    if dtype_str == "float":
        return np.dtype("float64")
    if dtype_str == "string":
        return pd.StringDtype()
    if dtype_str == "categorical":
        return pd.CategoricalDtype(categories=categories, ordered=False)
    raise ValueError(f"Unknown dtype string: '{dtype_str}'")


# ---------------------------------------------------------------------------
# Task I-03: Single-file reader
# ---------------------------------------------------------------------------


def _filter_year(df: pd.DataFrame, min_year: int = 2024) -> pd.DataFrame:
    """Keep rows where MONTH-YEAR year component is numeric and >= min_year."""
    if df.empty:
        return df

    def _is_valid(val: object) -> bool:
        parts = str(val).split("-")
        year_str = parts[-1]
        return year_str.isdigit() and int(year_str) >= min_year

    mask = df["MONTH-YEAR"].apply(_is_valid)
    return df[mask]


def read_raw_file(file_path: str, data_dict: dict[str, dict]) -> pd.DataFrame:
    """Read one raw KGS lease .txt file, enforce schema, filter to year >= 2024.

    Returns a pandas DataFrame with the canonical schema.
    """
    # Try UTF-8 first, fall back to latin-1
    try:
        df = pd.read_csv(file_path, dtype=str, encoding="utf-8")
    except UnicodeDecodeError:
        logger.warning("UTF-8 decode failed for %s — retrying with latin-1", file_path)
        df = pd.read_csv(file_path, dtype=str, encoding="latin-1")

    df["source_file"] = os.path.basename(file_path)

    # Enforce canonical schema
    for col_name, spec in data_dict.items():
        dtype_str: str = spec["dtype"]
        nullable: bool = spec["nullable"]
        categories: list[str] = spec["categories"]
        target_dtype = resolve_pandas_dtype(dtype_str, nullable, categories)

        if col_name in df.columns:
            try:
                if dtype_str == "int":
                    coerced = pd.to_numeric(df[col_name], errors="coerce")
                    if not nullable:
                        n_bad = coerced.isna().sum()
                        if n_bad > 0:
                            raise SchemaError(
                                f"Non-nullable column '{col_name}' has {n_bad} "
                                f"un-castable values in {file_path}"
                            )
                    df[col_name] = coerced.astype(pd.Int64Dtype())
                elif dtype_str == "float":
                    coerced = pd.to_numeric(df[col_name], errors="coerce")
                    n_bad = coerced.isna().sum() - df[col_name].isna().sum()
                    if n_bad > 0:
                        logger.warning(
                            "Coerced %d values to NA in nullable float column '%s' in %s",
                            n_bad,
                            col_name,
                            file_path,
                        )
                    df[col_name] = coerced.astype(np.dtype("float64"))
                elif dtype_str == "string":
                    df[col_name] = df[col_name].astype(pd.StringDtype())
                elif dtype_str == "categorical":
                    df[col_name] = df[col_name].astype(target_dtype)  # type: ignore[call-overload]
            except SchemaError:
                raise
            except Exception as exc:
                if not nullable:
                    raise SchemaError(
                        f"Cast failure on non-nullable column '{col_name}' in {file_path}: {exc}"
                    ) from exc
                logger.warning(
                    "Cast failure on nullable column '%s' in %s — coercing to NA: %s",
                    col_name,
                    file_path,
                    exc,
                )
                # Add as all-NA at target dtype
                df[col_name] = pd.array([pd.NA] * len(df), dtype=target_dtype)  # type: ignore[call-overload]
        else:
            if not nullable:
                raise SchemaError(f"Required column '{col_name}' absent from {file_path}")
            logger.info(
                "Nullable column '%s' absent from %s — adding as all-NA", col_name, file_path
            )
            df[col_name] = pd.array([pd.NA] * len(df), dtype=target_dtype)  # type: ignore[call-overload]

    # Ensure source_file is StringDtype
    df["source_file"] = df["source_file"].astype(pd.StringDtype())

    # Filter to year >= 2024
    df = _filter_year(df, min_year=2024)

    if df.empty:
        logger.warning("No rows remaining after year filter in %s", file_path)

    return df


# ---------------------------------------------------------------------------
# Task I-04: Dask ingest runner
# ---------------------------------------------------------------------------


def _build_meta(data_dict: dict[str, dict]) -> pd.DataFrame:
    """Build the canonical empty meta DataFrame for Dask from_delayed."""
    schema: dict[str, object] = {}
    for col_name, spec in data_dict.items():
        schema[col_name] = pd.Series(
            dtype=resolve_pandas_dtype(spec["dtype"], spec["nullable"], spec["categories"])  # type: ignore[call-overload]
        )
    schema["source_file"] = pd.Series(dtype=pd.StringDtype())
    return pd.DataFrame(schema)


def _filter_year_partition(partition: pd.DataFrame) -> pd.DataFrame:
    """Partition-level year filter (used in map_partitions for reliability)."""
    return _filter_year(partition, min_year=2024)


def run_ingest(config: dict) -> dd.DataFrame:
    """Orchestrate the full ingest stage.

    Reads raw .txt files, applies schema, writes interim Parquet.
    Returns the lazy Dask DataFrame.
    """
    raw_dir = config["raw_dir"]
    if not Path(raw_dir).exists():
        raise FileNotFoundError(f"raw_dir does not exist: {raw_dir}")

    dict_path = config["data_dictionary"]
    data_dict = load_data_dictionary(dict_path)
    meta = _build_meta(data_dict)

    raw_files = sorted(glob_module.glob(os.path.join(raw_dir, "*.txt")))
    if not raw_files:
        logger.warning("No .txt files found in %s — returning empty DataFrame", raw_dir)
        return dd.from_pandas(meta, npartitions=1)

    delayed_frames = []
    for fpath in raw_files:
        try:
            delayed_df = dask.delayed(read_raw_file)(fpath, data_dict)
            delayed_frames.append(delayed_df)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skipping %s due to error: %s", fpath, exc)

    ddf: dd.DataFrame = dd.from_delayed(delayed_frames, meta=meta)

    n_partitions = max(10, min(len(raw_files), 50))
    ddf = ddf.repartition(npartitions=n_partitions)

    interim_dir = config["interim_dir"]
    Path(interim_dir).mkdir(parents=True, exist_ok=True)

    ddf.to_parquet(interim_dir, engine="pyarrow", write_index=False, overwrite=True)
    logger.info("Ingest complete: %d partitions written to %s", n_partitions, interim_dir)
    return ddf
