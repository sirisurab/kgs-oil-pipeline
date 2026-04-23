"""Ingest stage: build dtype map, parse raw files, filter date range, write interim Parquet."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Task I-01: Build the dtype map from the data dictionary
# ---------------------------------------------------------------------------

_DTYPE_MAP_CACHE: dict[str, Any] | None = None

_DTYPE_REGISTRY: dict[str, Any] = {
    # non-nullable
    "int_nonnull": "int64",
    "string_nonnull": "str",
    # nullable
    "int_null": pd.Int64Dtype(),
    "string_null": pd.StringDtype(),
    "float_null": pd.Float64Dtype(),
    "float_nonnull": "float64",
    "categorical_null": pd.CategoricalDtype(),
    "categorical_nonnull": pd.CategoricalDtype(),
}


def build_dtype_map(dict_path: str | Path) -> dict[str, Any]:
    """Construct a column → pandas dtype mapping from the data dictionary CSV.

    Args:
        dict_path: Path to kgs_monthly_data_dictionary.csv.

    Returns:
        Dict mapping column name to pandas dtype.

    Raises:
        FileNotFoundError: If dict_path does not exist.
        ValueError: If any dtype value is unrecognised.
    """
    global _DTYPE_MAP_CACHE
    path = Path(dict_path)
    if not path.exists():
        raise FileNotFoundError(f"Data dictionary not found: {path}")

    dd_df = pd.read_csv(path, dtype=str, keep_default_na=False)
    dtype_map: dict[str, Any] = {}

    for _, row in dd_df.iterrows():
        col: str = row["column"]
        raw_dtype: str = row["dtype"].strip().lower()
        nullable: bool = row["nullable"].strip().lower() == "yes"
        categories_raw: str = row.get("categories", "")

        dtype: Any
        if raw_dtype == "int":
            dtype = pd.Int64Dtype() if nullable else "int64"
        elif raw_dtype == "string":
            dtype = pd.StringDtype() if nullable else "str"
        elif raw_dtype == "float":
            dtype = pd.Float64Dtype() if nullable else "float64"
        elif raw_dtype == "categorical":
            if categories_raw:
                cats = [c for c in categories_raw.split("|") if c]
                dtype = pd.CategoricalDtype(categories=cats)
            else:
                dtype = pd.CategoricalDtype()
        else:
            raise ValueError(f"Unrecognised dtype '{raw_dtype}' for column '{col}' in {path}")

        dtype_map[col] = dtype

    _DTYPE_MAP_CACHE = dtype_map
    return dtype_map


def _get_dict_path() -> Path:
    """Return the canonical path to the data dictionary."""
    return Path(__file__).parent.parent / "references" / "kgs_monthly_data_dictionary.csv"


def _load_dict_info(dict_path: Path | None = None) -> pd.DataFrame:
    """Load the data dictionary as a DataFrame."""
    p = dict_path or _get_dict_path()
    return pd.read_csv(p, dtype=str, keep_default_na=False)


# ---------------------------------------------------------------------------
# Task I-02: Parse a single raw lease file
# ---------------------------------------------------------------------------


def parse_raw_file(
    file_path: str | Path,
    dtype_map: dict[str, Any],
) -> pd.DataFrame:
    """Read a raw lease .txt file and enforce canonical schema.

    Args:
        file_path: Path to the raw .txt (CSV) file.
        dtype_map: Mapping from canonical column name to pandas dtype.

    Returns:
        DataFrame with canonical columns, correct dtypes, and source_file column.

    Raises:
        ValueError: If a nullable=no column is absent from the file.
    """
    path = Path(file_path)
    dict_df = _load_dict_info()
    nullable_yes_cols = set(dict_df.loc[dict_df["nullable"].str.strip() == "yes", "column"])
    nullable_no_cols = set(dict_df.loc[dict_df["nullable"].str.strip() == "no", "column"])

    # Attempt utf-8, fall back to latin-1
    encoding = "utf-8"
    try:
        raw = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")
    except pd.errors.EmptyDataError:
        return _build_empty_frame(dtype_map)
    except UnicodeDecodeError:
        logger.warning("UTF-8 decode failed for %s — falling back to latin-1", path)
        encoding = "latin-1"
        try:
            raw = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="latin-1")
        except pd.errors.EmptyDataError:
            return _build_empty_frame(dtype_map)

    _ = encoding  # used above

    # Handle empty file
    if raw.empty or len(raw.columns) == 0:
        return _build_empty_frame(dtype_map)

    # Validate required non-nullable columns
    for col in nullable_no_cols:
        if col not in raw.columns:
            raise ValueError(f"Required (nullable=no) column '{col}' is absent from {path}")

    # Build result frame with correct dtypes
    result_cols: dict[str, Any] = {}

    for col, dtype in dtype_map.items():
        if col in raw.columns:
            series = raw[col]
            result_cols[col] = _cast_series(series, col, dtype)
        elif col in nullable_yes_cols:
            # Absent nullable column — add as all-NA at correct dtype
            result_cols[col] = pd.array([pd.NA] * len(raw), dtype=dtype)  # type: ignore[call-overload]
        # nullable_no handled above (raises)

    result = pd.DataFrame(result_cols)
    result["source_file"] = path.stem
    return result


def _build_empty_frame(dtype_map: dict[str, Any]) -> pd.DataFrame:
    """Return a zero-row DataFrame with canonical columns and correct dtypes."""
    result: dict[str, Any] = {}
    for col, dtype in dtype_map.items():
        result[col] = pd.array([], dtype=dtype)  # type: ignore[call-overload]
    df = pd.DataFrame(result)
    df["source_file"] = pd.array([], dtype=pd.StringDtype())
    return df


def _cast_series(series: pd.Series, col: str, dtype: Any) -> pd.Series:  # type: ignore[type-arg]
    """Cast a string series to the target dtype, coercing errors to NA."""
    try:
        if isinstance(dtype, pd.CategoricalDtype):
            return series.astype(dtype)
        if isinstance(dtype, pd.Int64Dtype):
            numeric = pd.to_numeric(series, errors="coerce")
            return numeric.astype(dtype)
        if isinstance(dtype, pd.Float64Dtype):
            numeric = pd.to_numeric(series, errors="coerce")
            return numeric.astype(dtype)
        if isinstance(dtype, pd.StringDtype):
            result = series.where(series != "", other=np.nan)
            return result.astype(dtype)
        if dtype == "int64":
            return pd.to_numeric(series, errors="coerce").astype("int64")
        if dtype == "float64":
            return pd.to_numeric(series, errors="coerce").astype("float64")
        return series.astype(dtype)
    except Exception:
        logger.warning("Could not cast column '%s' to %s — returning as-is", col, dtype)
        return series


# ---------------------------------------------------------------------------
# Task I-03: Filter rows to target date range
# ---------------------------------------------------------------------------


def filter_date_range(df: pd.DataFrame, min_year: int = 2024) -> pd.DataFrame:
    """Filter a pandas partition to rows where year component of MONTH-YEAR >= min_year.

    Rows with malformed year values (non-numeric) are dropped without raising.
    This function operates on a pandas partition (ADR-004).

    Args:
        df: Pandas DataFrame partition containing MONTH-YEAR column.
        min_year: Minimum year (inclusive) to keep.

    Returns:
        Filtered DataFrame with the same schema as input.

    Raises:
        KeyError: If MONTH-YEAR column is absent.
    """
    if "MONTH-YEAR" not in df.columns:
        raise KeyError("'MONTH-YEAR' column is absent from DataFrame")

    def _year(my: str) -> int | None:
        parts = str(my).split("-")
        year_str = parts[-1]
        if not year_str.isdigit():
            return None
        return int(year_str)

    years = df["MONTH-YEAR"].map(_year)
    bad_count = years.isna().sum()
    if bad_count > 0:
        logger.debug("Dropping %d rows with malformed MONTH-YEAR values", bad_count)

    mask = years >= min_year
    return df[mask.fillna(False)].copy()


# ---------------------------------------------------------------------------
# Task I-04: Ingest all raw files in parallel and write interim Parquet
# ---------------------------------------------------------------------------


def ingest(config: dict[str, Any]) -> dd.DataFrame:
    """Orchestrate the full ingest stage.

    Discovers all .txt files in raw_dir, parses each with canonical schema
    enforcement, filters to target date range, concatenates into a Dask
    DataFrame, repartitions, and writes interim Parquet.

    Args:
        config: Pipeline configuration dict.

    Returns:
        Lazy Dask DataFrame (not computed).

    Raises:
        FileNotFoundError: If no .txt files are found in raw_dir.
    """
    ingest_cfg = config["ingest"]
    raw_dir = Path(ingest_cfg["raw_dir"])
    interim_path = Path(ingest_cfg["interim_path"])
    min_year: int = ingest_cfg.get("min_year", 2024)
    dict_path = Path(ingest_cfg.get("dict_path", _get_dict_path()))

    txt_files = sorted(raw_dir.glob("*.txt"))
    if not txt_files:
        raise FileNotFoundError(f"No .txt files found in raw directory: {raw_dir}")

    dtype_map = build_dtype_map(dict_path)

    def _parse_one(fp: str) -> pd.DataFrame:
        try:
            df = parse_raw_file(fp, dtype_map)
            df = filter_date_range(df, min_year=min_year)
            return df
        except Exception as exc:
            logger.warning("Skipping file %s due to error: %s", fp, exc)
            return _build_empty_frame(dtype_map)

    # Build delayed tasks — one per file
    delayed_parts = [dask.delayed(_parse_one)(str(fp)) for fp in txt_files]

    # Construct empty meta from dtype_map + source_file
    meta = _build_empty_frame(dtype_map)

    ddf = dd.from_delayed(delayed_parts, meta=meta)

    n_partitions = max(10, min(len(txt_files), 50))
    ddf = ddf.repartition(npartitions=n_partitions)

    interim_path.mkdir(parents=True, exist_ok=True)
    ddf.to_parquet(str(interim_path), write_index=False, overwrite=True)

    logger.info(
        "Ingest stage complete: %d files → %s (%d partitions)",
        len(txt_files),
        interim_path,
        n_partitions,
    )
    return ddf
