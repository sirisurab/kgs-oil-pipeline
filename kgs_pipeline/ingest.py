"""
Ingest stage: read raw lease files, enforce canonical schema, write partitioned
interim Parquet to data/interim/.

Design decisions per ADRs and stage manifests:
- Schema source of truth: kgs_monthly_data_dictionary.csv (ADR-003).
- Parallelization: Dask distributed scheduler (CPU-bound stage) — ADR-001.
  Stages reuse the pipeline's client; they do not initialize their own cluster
  (build-env-manifest "Dask scheduler initialization").
- Laziness: no .compute() before final write — ADR-005.
- Partition count: max(10, min(n, 50)) as last operation before write — ADR-004.
- Inter-stage format: Parquet only — ADR-004.
- Absent-column semantics: nullable=yes → add all-NA; nullable=no → raise — H3.
- Empty DataFrames must still carry the full schema — H2.
- Logging: dual-channel — ADR-006.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import dask
import dask.dataframe as dd
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# dtype mapping per ADR-003
# ---------------------------------------------------------------------------

# Maps data-dictionary dtype tokens to pandas dtype strings.
# nullable=yes variants use nullable-aware pandas dtypes so that integer columns
# carrying NaN do not upcast to float (ADR-003 consequence).

_DTYPE_MAP_NULLABLE: dict[str, str] = {
    "int": "Int64",
    "float": "Float64",
    "string": "string",
    "categorical": "category",
}

_DTYPE_MAP_NON_NULLABLE: dict[str, str] = {
    "int": "int64",
    "float": "float64",
    "string": "string",
    "categorical": "category",
}


# ---------------------------------------------------------------------------
# Task I1: Schema object from the data dictionary
# ---------------------------------------------------------------------------


@dataclass
class ColumnSchema:
    """Schema descriptor for one column from the data dictionary."""

    name: str
    dtype_token: str  # raw value from data dictionary: int, float, string, categorical
    nullable: bool
    categories: list[str] = field(default_factory=list)

    @property
    def pandas_dtype(self) -> str:
        """Return the pandas dtype string for this column per ADR-003."""
        if self.nullable:
            return _DTYPE_MAP_NULLABLE[self.dtype_token]
        return _DTYPE_MAP_NON_NULLABLE[self.dtype_token]


@dataclass
class Schema:
    """Complete schema derived from the data dictionary."""

    columns: list[ColumnSchema]

    def __getitem__(self, name: str) -> ColumnSchema:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(name)

    def column_names(self) -> list[str]:
        """Return canonical column names in dictionary order."""
        return [c.name for c in self.columns]

    def dtypes_dict(self) -> dict[str, str]:
        """Return {column_name: pandas_dtype} mapping."""
        return {c.name: c.pandas_dtype for c in self.columns}

    def has_column(self, name: str) -> bool:
        return any(c.name == name for c in self.columns)


def load_schema(data_dictionary_path: str | Path) -> Schema:
    """Load kgs_monthly_data_dictionary.csv and return a Schema object.

    The data dictionary is the single source of truth for all schema decisions
    per ADR-003. Schema is never inferred from data.

    Parameters
    ----------
    data_dictionary_path:
        Path to kgs_monthly_data_dictionary.csv.

    Returns
    -------
    Schema
        Ordered schema with per-column dtype, nullable flag, and category sets.
    """
    df = pd.read_csv(data_dictionary_path, dtype=str)
    columns: list[ColumnSchema] = []
    for _, row in df.iterrows():
        name = str(row["column"]).strip()
        dtype_token = str(row["dtype"]).strip()
        nullable = str(row.get("nullable", "yes")).strip().lower() == "yes"
        cats_raw = str(row.get("categories", "")).strip()
        cats = [c.strip() for c in cats_raw.split("|") if c.strip()] if cats_raw else []
        columns.append(
            ColumnSchema(name=name, dtype_token=dtype_token, nullable=nullable, categories=cats)
        )
    return Schema(columns=columns)


# ---------------------------------------------------------------------------
# Task I2: Read a single raw lease file into a typed DataFrame
# ---------------------------------------------------------------------------


def _make_empty_dataframe(schema: Schema, extra_cols: list[str] | None = None) -> pd.DataFrame:
    """Return a zero-row DataFrame with the full canonical schema.

    extra_cols are appended after the dictionary columns (e.g. source_file).
    Empty DataFrames must still carry the full schema — stage-manifest-ingest H2.
    """
    all_cols = schema.column_names() + (extra_cols or [])
    dtypes = schema.dtypes_dict()
    if extra_cols:
        for c in extra_cols:
            dtypes[c] = "string"

    empty: dict[str, pd.Series] = {}
    for col in all_cols:
        dtype = dtypes[col]
        if dtype == "category":
            # Get the declared categories for this column if available
            if schema.has_column(col):
                cs = schema[col]
                if cs.categories:
                    cat_type = pd.CategoricalDtype(categories=cs.categories, ordered=False)
                    empty[col] = pd.Series([], dtype=cat_type)
                    continue
            empty[col] = pd.Series([], dtype="category")
        else:
            empty[col] = pd.Series([], dtype=dtype)

    return pd.DataFrame(empty)


def read_raw_file(
    path: str | Path,
    schema: Schema,
) -> pd.DataFrame:
    """Read a single raw lease file and return a typed, filtered DataFrame.

    Output contract per task spec and stage-manifest-ingest H2/H3:
    - All canonical columns present in canonical order.
    - Every column carries its data-dictionary dtype.
    - source_file column added (string).
    - Rows with MONTH-YEAR year component < 2024 or non-numeric are dropped
      (per task-writer-kgs.md <data-filtering>).
    - Categorical columns carry only declared values; out-of-set values replaced
      with the appropriate null sentinel before casting (ADR-003).
    - nullable=yes absent column → add all-NA (H3).
    - nullable=no absent column → raise (H3).

    Parameters
    ----------
    path:
        Path to a raw .txt file in data/raw/.
    schema:
        Schema object from load_schema().

    Returns
    -------
    pd.DataFrame
        Schema-conformant, filtered DataFrame.
    """
    path = Path(path)
    canonical_cols = schema.column_names()
    extra_cols = ["source_file"]

    try:
        raw = pd.read_csv(path, dtype=str, encoding="utf-8", on_bad_lines="warn")
    except UnicodeDecodeError:
        raw = pd.read_csv(path, dtype=str, encoding="latin-1", on_bad_lines="warn")
    except Exception as exc:
        logger.warning("Could not read file %s: %s. Returning empty schema.", path, exc)
        return _make_empty_dataframe(schema, extra_cols)

    # Normalize column names
    raw.columns = [c.strip().strip('"') for c in raw.columns]

    # Check non-nullable columns are present
    for col_schema in schema.columns:
        if not col_schema.nullable and col_schema.name not in raw.columns:
            raise ValueError(
                f"Non-nullable column '{col_schema.name}' is absent from {path}. "
                "This indicates a structural problem with the source file."
            )

    # Filter rows: extract year from MONTH-YEAR per task-writer-kgs.md
    if "MONTH-YEAR" in raw.columns:
        year_str = raw["MONTH-YEAR"].str.strip().str.rsplit("-", n=1).str[-1]
        numeric_mask = year_str.str.match(r"^\d+$", na=False)
        raw = raw[numeric_mask].copy()
        year_str = year_str[numeric_mask]
        year_int = year_str.astype(int)
        raw = raw[year_int >= 2024].copy()

    if raw.empty:
        return _make_empty_dataframe(schema, extra_cols)

    result_cols: dict[str, pd.Series] = {}

    for col_schema in schema.columns:
        col = col_schema.name
        dtype = col_schema.pandas_dtype

        if col not in raw.columns:
            # nullable=yes absent column → all-NA at correct dtype (H3)
            # nullable=no would have raised above
            if dtype == "category":
                if col_schema.categories:
                    cat_type = pd.CategoricalDtype(categories=col_schema.categories, ordered=False)
                    result_cols[col] = pd.Series(pd.NA, index=raw.index, dtype=cat_type)
                else:
                    result_cols[col] = pd.Series(pd.NA, index=raw.index, dtype="category")
            else:
                result_cols[col] = pd.Series(pd.NA, index=raw.index, dtype=dtype)
            continue

        series = raw[col].copy()

        if dtype == "category":
            # Replace out-of-set values with null sentinel before casting (ADR-003)
            if col_schema.categories:
                valid = set(col_schema.categories)
                series = series.where(series.isin(valid), other=np.nan)
                cat_type = pd.CategoricalDtype(categories=col_schema.categories, ordered=False)
                result_cols[col] = series.astype(cat_type)
            else:
                result_cols[col] = series.astype("category")
        elif dtype in ("Int64",):
            result_cols[col] = pd.to_numeric(series, errors="coerce").astype("Int64")
        elif dtype in ("int64",):
            result_cols[col] = pd.to_numeric(series, errors="coerce").astype("int64")
        elif dtype in ("Float64",):
            result_cols[col] = pd.to_numeric(series, errors="coerce").astype("Float64")
        elif dtype in ("float64",):
            result_cols[col] = pd.to_numeric(series, errors="coerce").astype("float64")
        elif dtype == "string":
            result_cols[col] = series.astype("string")
        else:
            result_cols[col] = series

    # source_file column
    result_cols["source_file"] = pd.Series(
        [str(path.name)] * len(raw), index=raw.index, dtype="string"
    )

    all_cols = canonical_cols + extra_cols
    df = pd.DataFrame({c: result_cols[c] for c in all_cols})
    return df


# ---------------------------------------------------------------------------
# Task I3: Ingest stage entry point
# ---------------------------------------------------------------------------


def ingest(config: dict[str, Any]) -> None:
    """Stage entry point: read all raw files, combine, write partitioned Parquet.

    Uses Dask distributed scheduler (CPU-bound) — ADR-001. Stages reuse the
    pipeline's client; no cluster is started here.

    Partition count: max(10, min(n, 50)) as the last operation before write
    per ADR-004.

    Laziness: no .compute() before the final write — ADR-005.

    Parameters
    ----------
    config:
        Full pipeline config dict (parsed config.yaml).
    """
    ing = config["ingest"]
    raw_dir = Path(ing["raw_dir"])
    interim_dir = Path(ing["interim_dir"])
    dd_path = ing.get("data_dictionary_path", "references/kgs_monthly_data_dictionary.csv")

    schema = load_schema(dd_path)

    raw_files = sorted(raw_dir.glob("*.txt"))
    if not raw_files:
        logger.warning("No raw files found in %s; ingest stage produces empty output.", raw_dir)
        raw_files = []

    logger.info("Ingesting %d raw files from %s", len(raw_files), raw_dir)

    # Read each file as a pandas DataFrame (failure-tolerant per task spec)
    per_file_dfs: list[pd.DataFrame] = []
    for f in raw_files:
        try:
            df = read_raw_file(f, schema)
            per_file_dfs.append(df)
        except Exception as exc:
            logger.warning("Skipping unreadable file %s: %s", f, exc)

    if not per_file_dfs:
        # Build an empty Dask DataFrame with the correct schema
        empty_pd = _make_empty_dataframe(schema, ["source_file"])
        ddf = dd.from_pandas(empty_pd, npartitions=10)
    else:
        # Combine all into a single Dask DataFrame
        meta_df = _make_empty_dataframe(schema, ["source_file"])
        ddf = dd.from_delayed(
            [delayed_from_pandas(df) for df in per_file_dfs],
            meta=meta_df,
        )

    n_partitions = max(10, min(len(per_file_dfs), 50))
    ddf = ddf.repartition(npartitions=n_partitions)

    interim_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Writing interim Parquet to %s (%d partitions)", interim_dir, n_partitions)

    ddf.to_parquet(str(interim_dir), overwrite=True, write_index=False)
    logger.info("Ingest complete.")


def delayed_from_pandas(df: pd.DataFrame) -> Any:
    """Wrap a pandas DataFrame in a dask.delayed object.

    This allows building a Dask DataFrame from a list of already-computed
    pandas DataFrames while keeping the graph lazy until final write (ADR-005).
    TR-17 requires that intermediate Dask-returning functions return Dask
    DataFrames rather than pandas DataFrames.
    """
    return dask.delayed(lambda: df)()
