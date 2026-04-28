"""Ingest stage: read raw files, enforce schema, write interim Parquet."""

import logging
import os
from typing import Any

import dask
import dask.dataframe as dd
import pandas as pd

logger = logging.getLogger(__name__)

# Known dtype labels in the data dictionary
_KNOWN_DTYPES = {"int", "float", "string", "categorical"}


def load_data_dictionary(path: str) -> pd.DataFrame:
    """Read the KGS monthly data dictionary CSV and return it as a DataFrame."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    expected_cols = {"column", "description", "dtype", "nullable", "categories"}
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"Data dictionary missing columns: {missing}")
    return df[["column", "description", "dtype", "nullable", "categories"]]


def build_dtype_map(data_dict: pd.DataFrame) -> dict[str, Any]:
    """Build a column-name → pandas dtype mapping from the data dictionary."""
    mapping: dict[str, Any] = {}
    for _, row in data_dict.iterrows():
        col: str = row["column"]
        dtype_label: str = row["dtype"]
        nullable: str = row["nullable"]

        if dtype_label == "int":
            mapping[col] = pd.Int64Dtype() if nullable == "yes" else "int64"
        elif dtype_label == "float":
            mapping[col] = "float64"
        elif dtype_label == "string":
            mapping[col] = pd.StringDtype()
        elif dtype_label == "categorical":
            mapping[col] = pd.StringDtype()
        else:
            raise ValueError(f"Unknown dtype label '{dtype_label}' for column '{col}'")

    return mapping


def read_raw_file(
    file_path: str,
    dtype_map: dict[str, Any],
    data_dict: pd.DataFrame,
) -> pd.DataFrame:
    """Read one raw .txt file, enforce schema, filter to year >= 2024."""
    # Read with all columns as string first so we can detect absent columns
    raw_df = pd.read_csv(file_path, dtype=str, keep_default_na=False)

    dd_cols = data_dict["column"].tolist()
    nullable_flags = data_dict.set_index("column")["nullable"].to_dict()

    # Absent-column handling per stage-manifest-ingest H3
    for col in dd_cols:
        if col not in raw_df.columns:
            if nullable_flags.get(col) == "no":
                raise ValueError(f"Required column '{col}' (nullable=no) absent from {file_path}")
            # nullable=yes — add as all-NA column (will be cast to correct dtype below)
            raw_df[col] = pd.NA

    # Reorder to canonical schema columns only
    canonical_cols = [c for c in dd_cols if c in raw_df.columns]
    raw_df = raw_df[canonical_cols]

    # Filter rows: year >= 2024 using vectorized str accessor
    year_series = raw_df["MONTH-YEAR"].str.split("-").str[-1]
    numeric_mask = year_series.str.match(r"^\d+$", na=False)
    raw_df = raw_df[numeric_mask].copy()
    if len(raw_df) > 0:
        year_series = raw_df["MONTH-YEAR"].str.split("-").str[-1]
        raw_df = raw_df[year_series.astype(int) >= 2024].copy()

    # Cast each column to its data-dictionary dtype
    for col in dd_cols:
        target_dtype = dtype_map.get(col)
        if target_dtype is None:
            continue
        if isinstance(target_dtype, str) and target_dtype == "int64":
            # Non-nullable int: replace empty/NA strings with NaN then cast
            raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce").astype("int64")
        elif isinstance(target_dtype, pd.Int64Dtype):
            raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce").astype(pd.Int64Dtype())
        elif isinstance(target_dtype, str) and target_dtype == "float64":
            raw_df[col] = pd.to_numeric(raw_df[col], errors="coerce").astype("float64")
        elif isinstance(target_dtype, pd.StringDtype):
            raw_df[col] = raw_df[col].replace("", pd.NA).astype(pd.StringDtype())

    # Add source_file column
    raw_df["source_file"] = os.path.basename(file_path)
    raw_df["source_file"] = raw_df["source_file"].astype(pd.StringDtype())

    return raw_df


def build_dask_dataframe(
    raw_dir: str,
    dtype_map: dict[str, Any],
    data_dict: pd.DataFrame,
) -> dd.DataFrame:
    """Build a lazy Dask DataFrame from all .txt files in raw_dir.

    Meta is derived by calling read_raw_file on a real file sliced to zero rows
    (ADR-003). Does not call .compute() (ADR-005).
    """
    import glob as glob_mod

    raw_files = sorted(glob_mod.glob(os.path.join(raw_dir, "*.txt")))
    if not raw_files:
        raise FileNotFoundError(f"No .txt files found in {raw_dir}")

    # Derive meta by calling the actual function on a real file, sliced to zero rows
    meta = read_raw_file(raw_files[0], dtype_map, data_dict).iloc[0:0]

    delayed_dfs = [dask.delayed(read_raw_file)(f, dtype_map, data_dict) for f in raw_files]

    return dd.from_delayed(delayed_dfs, meta=meta)


def ingest(config: dict) -> None:
    """Orchestrate the full ingest stage and write interim Parquet output."""
    raw_dir: str = config["ingest"]["raw_dir"]
    interim_dir: str = config["ingest"]["interim_dir"]
    data_dict_path: str = config["ingest"]["data_dict_path"]

    logger.info("Ingest: loading data dictionary from %s", data_dict_path)
    data_dict = load_data_dictionary(data_dict_path)
    dtype_map = build_dtype_map(data_dict)

    import glob as glob_mod

    raw_files = sorted(glob_mod.glob(os.path.join(raw_dir, "*.txt")))
    n = len(raw_files)
    if n == 0:
        raise FileNotFoundError(f"No raw .txt files found in {raw_dir}")

    n_partitions = max(10, min(n, 50))
    logger.info("Ingest: found %d raw files → %d partitions", n, n_partitions)

    ddf = build_dask_dataframe(raw_dir, dtype_map, data_dict)

    # Repartition is the last structural operation before write (ADR-004)
    ddf = ddf.repartition(npartitions=n_partitions)

    os.makedirs(interim_dir, exist_ok=True)
    logger.info("Ingest: writing interim Parquet to %s", interim_dir)
    ddf.to_parquet(interim_dir, overwrite=True)
    logger.info("Ingest: complete")
