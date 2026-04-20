"""Ingest stage — read raw KGS files, enforce schema, write interim Parquet."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.bag as db
import dask.dataframe as dd
import pandas as pd

logger = logging.getLogger(__name__)

# Canonical expected columns (schema + source_file)
EXPECTED_COLUMNS: list[str] = [
    "LEASE_KID",
    "LEASE",
    "DOR_CODE",
    "API_NUMBER",
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
    "MONTH-YEAR",
    "PRODUCT",
    "WELLS",
    "PRODUCTION",
    "source_file",
]

# ---------------------------------------------------------------------------
# ING-01: Data dictionary schema loader
# ---------------------------------------------------------------------------


def load_schema(dict_path: str) -> dict[str, dict]:
    """Load the KGS monthly data dictionary and return a schema dict.

    Each value has keys: ``dtype``, ``nullable``, ``categories``.

    Args:
        dict_path: Path to ``kgs_monthly_data_dictionary.csv``.

    Returns:
        Schema dict keyed by column name.

    Raises:
        FileNotFoundError: If ``dict_path`` does not exist.
        ValueError: If required columns are missing from the CSV.
    """
    p = Path(dict_path)
    if not p.exists():
        raise FileNotFoundError(f"Data dictionary not found: {dict_path}")

    df = pd.read_csv(dict_path)
    required = {"column", "dtype", "nullable"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Data dictionary missing columns: {missing}")

    _dtype_map = {
        ("int", "no"): "int64",
        ("int", "yes"): "Int64",
        ("float", "no"): "float64",
        ("float", "yes"): "Float64",
        ("string", "no"): "string",
        ("string", "yes"): "string",
        ("categorical", "no"): "category",
        ("categorical", "yes"): "category",
    }

    schema: dict[str, dict] = {}
    for _, row in df.iterrows():
        col = str(row["column"])
        raw_dtype = str(row["dtype"]).strip().lower()
        nullable = str(row["nullable"]).strip().lower()
        cats_raw = row.get("categories", "")
        categories: list[str] | None = None
        if isinstance(cats_raw, str) and cats_raw.strip():
            categories = [c.strip() for c in cats_raw.split("|")]

        pandas_dtype = _dtype_map.get((raw_dtype, nullable), "string")
        schema[col] = {
            "dtype": pandas_dtype,
            "nullable": nullable == "yes",
            "categories": categories,
        }

    return schema


# ---------------------------------------------------------------------------
# ING-02: Raw file reader
# ---------------------------------------------------------------------------


def read_raw_file(file_path: Path) -> pd.DataFrame:
    """Read a single KGS lease production .txt file.

    All columns are read as ``object`` dtype; type enforcement happens later.

    Args:
        file_path: Path to the .txt (comma-separated, quoted) file.

    Returns:
        DataFrame with all columns as object dtype plus a ``source_file`` column.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file cannot be parsed as CSV.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    file_size = file_path.stat().st_size
    if file_size == 0:
        return pd.DataFrame()

    try:
        df = pd.read_csv(file_path, dtype=str, quoting=0, on_bad_lines="warn")
    except Exception as exc:
        logger.warning("Cannot parse %s as CSV: %s", file_path.name, exc)
        raise ValueError(f"Cannot parse {file_path.name}: {exc}") from exc

    if df.empty:
        return df

    df["source_file"] = file_path.name
    return df


# ---------------------------------------------------------------------------
# ING-03: Date filter
# ---------------------------------------------------------------------------


def filter_by_year(df: pd.DataFrame, min_year: int) -> pd.DataFrame:
    """Filter a DataFrame to rows with year >= min_year from MONTH-YEAR column.

    Args:
        df: DataFrame with a ``MONTH-YEAR`` column.
        min_year: Minimum year (inclusive) to retain.

    Returns:
        Filtered DataFrame.
    """
    if df.empty or "MONTH-YEAR" not in df.columns:
        return df

    def _year(val: object) -> int | None:
        try:
            part = str(val).split("-")[-1].strip()
            return int(part)
        except (ValueError, AttributeError):
            return None

    years = df["MONTH-YEAR"].map(_year)
    return df[years.notna() & (years >= min_year)].copy()


# ---------------------------------------------------------------------------
# ING-04: Schema enforcer
# ---------------------------------------------------------------------------


def enforce_schema(df: pd.DataFrame, schema: dict[str, dict]) -> pd.DataFrame:
    """Enforce canonical schema on a DataFrame.

    Args:
        df: Raw DataFrame to cast.
        schema: Schema dict from ``load_schema``.

    Returns:
        DataFrame with exactly schema columns in schema order.

    Raises:
        ValueError: If a ``nullable=no`` column is absent.
    """
    result: dict[str, pd.Series] = {}

    for col, spec in schema.items():
        dtype = spec["dtype"]
        nullable = spec["nullable"]
        categories = spec["categories"]

        if col not in df.columns:
            if not nullable:
                raise ValueError(f"Non-nullable column '{col}' absent from source data")
            # Add as all-NA column at correct dtype
            if dtype == "category" and categories:
                cat_dtype = pd.CategoricalDtype(categories=categories)
                result[col] = pd.Categorical([None] * len(df), dtype=cat_dtype)  # type: ignore[assignment]
            else:
                result[col] = pd.array([pd.NA] * len(df), dtype=dtype)  # type: ignore[call-overload]
            continue

        series = df[col].copy()

        if dtype == "category" and categories:
            # Replace out-of-set values with NA before casting
            valid = set(categories)
            series = series.where(series.isin(valid), other=pd.NA)  # type: ignore[call-overload]
            cat_dtype = pd.CategoricalDtype(categories=categories)
            try:
                result[col] = series.astype(cat_dtype)
            except Exception as exc:
                logger.warning("Cannot cast %s to category: %s", col, exc)
                if not nullable:
                    raise
                result[col] = pd.Categorical([None] * len(df), dtype=cat_dtype)  # type: ignore[assignment]
        else:
            try:
                result[col] = series.astype(dtype)
            except Exception as exc:
                logger.warning("Cannot cast %s to %s: %s", col, dtype, exc)
                if not nullable:
                    raise
                if dtype == "string":
                    result[col] = pd.array([pd.NA] * len(df), dtype="string")  # type: ignore[assignment]
                else:
                    result[col] = pd.array([pd.NA] * len(df), dtype=dtype)

    return pd.DataFrame(result, index=df.index)


# ---------------------------------------------------------------------------
# ING-05: Single-file ingest worker
# ---------------------------------------------------------------------------


def ingest_file(
    file_path: Path,
    schema: dict[str, dict],
    min_year: int,
) -> pd.DataFrame:
    """Process a single raw lease file into a schema-enforced DataFrame.

    Args:
        file_path: Path to raw .txt file.
        schema: Schema dict from ``load_schema``.
        min_year: Minimum year for date filter.

    Returns:
        Cleaned, schema-enforced DataFrame (may be empty).
    """
    try:
        df = read_raw_file(file_path)
        if df.empty:
            return df
        df = filter_by_year(df, min_year)
        if df.empty:
            return df
        df = enforce_schema(df, schema)
        return df
    except (FileNotFoundError, ValueError):
        raise
    except Exception as exc:
        logger.warning("Unexpected error ingesting %s: %s", file_path, exc)
        # Return empty DataFrame with schema columns
        return pd.DataFrame(columns=list(schema.keys()))


# ---------------------------------------------------------------------------
# ING-06: Parallel ingest orchestrator
# ---------------------------------------------------------------------------


def run_ingest(config: dict, client: object) -> dd.DataFrame:
    """Orchestrate parallel ingestion of all raw files using Dask.

    Args:
        config: Pipeline configuration dict.
        client: Dask distributed Client (CPU-bound — ADR-001).

    Returns:
        Dask DataFrame (lazy — not computed).

    Raises:
        FileNotFoundError: If no .txt files are found in raw_dir.
    """
    ing_cfg = config["ingest"]
    raw_dir = Path(ing_cfg["raw_dir"])
    txt_files = sorted(raw_dir.glob("*.txt"))

    if not txt_files:
        logger.warning("No .txt files found in %s", raw_dir)
        raise FileNotFoundError(f"No .txt files found in {raw_dir}")

    schema = load_schema(ing_cfg["dict_path"])
    min_year: int = int(ing_cfg.get("min_year", 2024))

    # Use dask.bag over file paths, map ingest_file over each
    bag = db.from_sequence(txt_files, npartitions=min(len(txt_files), 10))
    delayed_dfs = bag.map(lambda fp: ingest_file(fp, schema, min_year)).to_delayed()

    # Build Dask DataFrame from delayed list
    ddf = dd.from_delayed(delayed_dfs)

    n = len(txt_files)
    n_partitions = max(10, min(n, 50))
    ddf = ddf.repartition(npartitions=n_partitions)

    return ddf


# ---------------------------------------------------------------------------
# ING-07: Parquet writer
# ---------------------------------------------------------------------------


def write_interim(ddf: dd.DataFrame, config: dict) -> None:
    """Write the ingested Dask DataFrame to partitioned Parquet.

    Args:
        ddf: Dask DataFrame to write.
        config: Pipeline configuration dict.
    """
    output_dir = Path(config["ingest"]["interim_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        ddf.to_parquet(str(output_dir), write_index=False, overwrite=True)
        logger.info("Wrote interim Parquet to %s (%d partitions)", output_dir, ddf.npartitions)
    except Exception as exc:
        logger.error("Failed to write interim Parquet: %s", exc)
        raise


# ---------------------------------------------------------------------------
# ING-08: Schema completeness validator
# ---------------------------------------------------------------------------


def validate_interim_schema(interim_dir: Path, expected_columns: list[str]) -> None:
    """Validate that sampled Parquet partitions contain all expected columns.

    Args:
        interim_dir: Directory of interim Parquet files.
        expected_columns: List of column names that must be present.

    Raises:
        ValueError: If any expected column is missing from any sampled partition.
    """
    parquet_files = sorted(interim_dir.glob("*.parquet"))
    sample = parquet_files[:3]

    for pf in sample:
        df_sample = pd.read_parquet(pf)
        for col in expected_columns:
            if col not in df_sample.columns:
                raise ValueError(f"Missing column '{col}' in partition {pf}")


# ---------------------------------------------------------------------------
# ING-09: Stage entry point
# ---------------------------------------------------------------------------


def ingest(config: dict, client: object) -> None:
    """Top-level entry point for the ingest stage.

    Args:
        config: Pipeline configuration dict.
        client: Dask distributed Client.
    """
    ddf = run_ingest(config, client)
    write_interim(ddf, config)

    interim_dir = Path(config["ingest"]["interim_dir"])
    validate_interim_schema(interim_dir, EXPECTED_COLUMNS)

    logger.info(
        "Ingest stage complete: %d partitions written to %s",
        ddf.npartitions,
        interim_dir,
    )
