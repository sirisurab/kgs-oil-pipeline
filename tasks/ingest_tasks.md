# Ingest Stage — Task Specifications

## Context and Design Decisions

The ingest stage reads raw KGS lease production `.txt` files from `data/raw/`, enforces
the canonical schema defined in `references/kgs_monthly_data_dictionary.csv`, filters
rows to the target date range (year >= 2024), and writes consolidated partitioned
Parquet output to `data/interim/`.

**Key ADRs governing this stage:**
- ADR-001: Use Dask distributed scheduler for CPU-bound ingest
- ADR-002: All operations must be vectorized — no per-row iteration
- ADR-003: Data dictionary is the single source of truth for schema — no pandas inference
- ADR-004: Parquet inter-stage format; partition count = max(10, min(n, 50))
- ADR-005: Task graph stays lazy until the final write; batch independent computations
- ADR-006: Dual-channel logging; log config read from config.yaml
- ADR-007: Python 3.11+; pytest for tests

**Stage manifest guarantees (output state):**
- Canonical column names enforced (matching data dictionary)
- Data-dictionary dtypes enforced on all columns
- Nullable absent columns filled with NA at the correct dtype
- Non-nullable absent columns raise an error immediately
- Partitioned to max(10, min(n, 50))
- Unsorted (sort is the responsibility of the transform stage)

**Boundary contract delivered to transform:**
- Column names conform to canonical schema
- All columns carry data-dictionary dtypes
- All nullable columns are present (filled with NA if absent from source)
- Partition count within max(10, min(n, 50))

**Package name:** `kgs_pipeline`
**Module path:** `kgs_pipeline/ingest.py`

**Input:**
- Raw `.txt` files in `data/raw/` (CSV format, produced by acquire stage)
- `references/kgs_monthly_data_dictionary.csv` — authoritative schema source

**Output:**
- Partitioned Parquet files in `data/interim/` (e.g. `data/interim/part.0.parquet`, ...)
- One logical dataset; partition count = max(10, min(n_files, 50))

---

## Canonical Schema

The canonical schema is defined by `references/kgs_monthly_data_dictionary.csv`.
Every column in that file must be present in the ingest output with the dtype and
nullable rules defined below.

### dtype mapping rules (ADR-003)

| Data dictionary dtype | nullable=no | nullable=yes |
|---|---|---|
| int | `pd.Int64Dtype()` | `pd.Int64Dtype()` |
| float | `float64` | `float64` |
| string | `pd.StringDtype()` | `pd.StringDtype()` |
| categorical | `pd.CategoricalDtype(categories=[...])` | `pd.CategoricalDtype(categories=[...])` |

For nullable=yes columns, null values are represented as `pd.NA` (for Int64/string) or
`NaN` (for float64). For categorical columns the allowed values come from the `categories`
field in the data dictionary (pipe-separated, e.g. `"S|N"`).

### Canonical columns (from kgs_monthly_data_dictionary.csv)

| Column | dtype (pandas) | nullable | Notes |
|---|---|---|---|
| LEASE_KID | Int64 | no | Non-nullable; absent → raise error |
| LEASE | StringDtype | yes | |
| DOR_CODE | Int64 | yes | |
| API_NUMBER | StringDtype | yes | |
| FIELD | StringDtype | yes | |
| PRODUCING_ZONE | StringDtype | yes | |
| OPERATOR | StringDtype | yes | |
| COUNTY | StringDtype | yes | |
| TOWNSHIP | Int64 | yes | |
| TWN_DIR | CategoricalDtype(["S","N"]) | yes | |
| RANGE | Int64 | yes | |
| RANGE_DIR | CategoricalDtype(["E","W"]) | yes | |
| SECTION | Int64 | yes | |
| SPOT | StringDtype | yes | |
| LATITUDE | float64 | yes | |
| LONGITUDE | float64 | yes | |
| MONTH-YEAR | StringDtype | no | Non-nullable; absent → raise error |
| PRODUCT | CategoricalDtype(["O","G"]) | no | Non-nullable; absent → raise error |
| WELLS | Int64 | yes | |
| PRODUCTION | float64 | yes | |
| source_file | StringDtype | no | Added during ingest; basename of source file |

Note: `URL` column from the lease index is present in some raw files. It must be dropped
at ingest — it is not part of the canonical monthly production schema.

---

## Data Filtering

After loading each raw file and before writing Parquet output, filter rows as follows
(from task-writer-kgs.md `<data-filtering>`):

- Extract the year component from MONTH-YEAR: split on "-", take the last element
- Drop rows where the extracted year is not numeric (e.g. "-1-1965", "0-1966")
- Drop rows where integer year < 2024
- This filter is applied per-partition inside a `map_partitions` call — do not use
  Dask string accessors on repartitioned output (ADR-004)

---

## Task 01: Implement schema loader

**Module:** `kgs_pipeline/ingest.py`
**Function:** `load_schema(dict_path: str | Path) -> dict[str, dict]`

**Description:**
Read the data dictionary CSV and return a mapping from column name to its schema
metadata (dtype string, nullable bool, categories list or None).

**Steps:**
- Read `references/kgs_monthly_data_dictionary.csv` into a pandas DataFrame
- For each row produce a dict entry: `{column_name: {"dtype": ..., "nullable": ..., "categories": [...] or None}}`
- Parse the `nullable` field as a boolean: "yes" → True, "no" → False
- Parse the `categories` field: if non-empty string, split on "|" to produce a list;
  otherwise set to `None`
- Return the complete mapping

**Error handling:**
- If `dict_path` does not exist, raise `FileNotFoundError`
- If required columns (`column`, `dtype`, `nullable`) are absent from the CSV,
  raise `ValueError` naming the missing columns

**Test cases:**
- Given the real data dictionary CSV, assert all 21 columns are present in the returned dict
- Assert LEASE_KID entry has `nullable=False` and dtype `"int"`
- Assert TWN_DIR entry has `nullable=True`, dtype `"categorical"`, and categories `["S", "N"]`
- Assert PRODUCT entry has `nullable=False`, dtype `"categorical"`, and categories `["O", "G"]`
- Given a nonexistent path, assert `FileNotFoundError` is raised

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Implement pandas dtype resolver

**Module:** `kgs_pipeline/ingest.py`
**Function:** `resolve_pandas_dtype(dtype_str: str, nullable: bool, categories: list[str] | None) -> Any`

**Description:**
Convert a data dictionary dtype string plus nullable flag into the correct pandas dtype
object to use when casting columns.

**Mapping rules (ADR-003):**
- `"int"` → `pd.Int64Dtype()` (both nullable and non-nullable use nullable int)
- `"float"` → `np.float64` (standard float, uses NaN for nulls)
- `"string"` → `pd.StringDtype()`
- `"categorical"` with `categories` list → `pd.CategoricalDtype(categories=categories, ordered=False)`
- `"categorical"` with `categories=None` → `pd.CategoricalDtype()`

**Error handling:**
- If `dtype_str` is not one of `{"int", "float", "string", "categorical"}`, raise
  `ValueError` naming the unrecognized dtype string

**Test cases:**
- Assert `resolve_pandas_dtype("int", False, None)` returns `pd.Int64Dtype()`
- Assert `resolve_pandas_dtype("int", True, None)` returns `pd.Int64Dtype()`
- Assert `resolve_pandas_dtype("float", True, None)` returns `np.float64`
- Assert `resolve_pandas_dtype("string", True, None)` returns `pd.StringDtype()`
- Assert `resolve_pandas_dtype("categorical", True, ["S", "N"])` returns
  `pd.CategoricalDtype(categories=["S", "N"], ordered=False)`
- Assert `resolve_pandas_dtype("unknown", True, None)` raises `ValueError`

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Implement single-file reader

**Module:** `kgs_pipeline/ingest.py`
**Function:** `read_raw_file(file_path: Path, schema: dict[str, dict]) -> pd.DataFrame`

**Description:**
Read one raw KGS `.txt` file into a pandas DataFrame. Enforce canonical column names,
apply data-dictionary dtypes, handle absent columns per nullable rules, add `source_file`
column, and filter rows to year >= 2024.

**Steps:**
- Read the file using pandas `read_csv` with `low_memory=False`; the file is
  comma-separated with quoted fields
- Drop the `URL` column if present (not part of canonical monthly schema)
- Check for non-nullable columns (`nullable=no`) that are absent: if any are missing,
  raise `ValueError` naming the absent columns
- Check for nullable columns (`nullable=yes`) that are absent: add them as all-NA
  columns at the resolved pandas dtype — do not raise (H3 from stage manifest)
- Cast each column to its resolved pandas dtype using `resolve_pandas_dtype`; for
  categorical columns, replace values outside the declared category set with `pd.NA`
  before casting (ADR-003)
- Add a `source_file` column (StringDtype) containing the basename of `file_path`
- Apply the year filter: extract year from MONTH-YEAR by splitting on "-" and taking
  the last element; drop rows where year is non-numeric or integer year < 2024
- Return the filtered DataFrame with canonical schema

**Error handling:**
- If `file_path` does not exist, raise `FileNotFoundError`
- On pandas parse errors (`pd.errors.ParserError`), raise `ValueError` with the
  filename and the original exception message
- Non-nullable absent columns: raise `ValueError` immediately (before any dtype casting)
- Nullable absent columns: add as all-NA, no exception

**Test cases:**
- Given a minimal valid CSV matching the canonical schema, assert the returned DataFrame
  has all expected canonical columns plus `source_file`
- Given a file where a nullable column (e.g. OPERATOR) is absent, assert it is added
  as an all-NA `StringDtype` column and no exception is raised
- Given a file where a non-nullable column (LEASE_KID) is absent, assert `ValueError`
  is raised
- Given rows with MONTH-YEAR = "1-2023" (year < 2024), assert those rows are dropped
- Given rows with MONTH-YEAR = "0-1966" (non-numeric year part after split), assert
  those rows are dropped
- Given a file with a categorical column (PRODUCT) containing a value not in `["O","G"]`,
  assert that value becomes `pd.NA` in the output and no exception is raised
- Assert the `source_file` column contains only the basename, not the full path

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Implement Dask-based parallel file ingestion

**Module:** `kgs_pipeline/ingest.py`
**Function:** `ingest_raw_files(input_dir: Path, schema: dict[str, dict], client: distributed.Client) -> dd.DataFrame`

**Description:**
Discover all `.txt` files in `input_dir`, read each one in parallel using the Dask
distributed scheduler via a Dask bag, combine into a single Dask DataFrame, and return
the lazy Dask DataFrame without calling `.compute()` (TR-17, ADR-005).

**Steps:**
- Discover all `.txt` files in `input_dir` using `Path.glob("*.txt")`
- Log the count of files found
- If no files are found, raise `FileNotFoundError` with a descriptive message
- Create a Dask bag from the list of file paths
- Map `read_raw_file` over the bag using the distributed client (CPU-bound; use
  Dask distributed scheduler, not threaded — ADR-001)
- Convert the bag of DataFrames to a Dask DataFrame using `dd.from_delayed`
  with the canonical meta schema
- Return the lazy Dask DataFrame — do not call `.compute()`

**Meta schema construction (ADR-003):**
- Build the meta DataFrame by constructing an empty pandas DataFrame with all canonical
  columns at their resolved dtypes — derived from the same `resolve_pandas_dtype` path
  used for actual data casting, not constructed separately

**Error handling:**
- If `input_dir` does not exist, raise `FileNotFoundError`
- Individual file failures inside the Dask graph surface during `.compute()` at the
  write step — they are not caught here

**Test cases (TR-17):**
- Given a directory with 3 valid `.txt` files, assert the return type is
  `dask.dataframe.DataFrame` (not `pd.DataFrame`) — function must not call `.compute()`
- Assert the returned Dask DataFrame has the canonical column set plus `source_file`
- Given an empty directory, assert `FileNotFoundError` is raised
- Assert the Dask distributed scheduler is used (not threaded), verifiable by checking
  the client type passed and that `map_partitions` / delayed operations are associated
  with the distributed client

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Implement Parquet writer

**Module:** `kgs_pipeline/ingest.py`
**Function:** `write_interim_parquet(ddf: dd.DataFrame, output_dir: Path, n_files: int) -> None`

**Description:**
Repartition the Dask DataFrame and write it as partitioned Parquet to `output_dir`.
The partition count must follow the contract formula and the repartition step must be
the last structural operation before writing (ADR-004).

**Steps:**
- Compute partition count: `max(10, min(n_files, 50))` where `n_files` is the number
  of source files ingested
- Repartition the Dask DataFrame to the computed partition count using `ddf.repartition`
- Call `ddf.to_parquet(output_dir, write_index=False, overwrite=True)` to write
  partitioned Parquet — this is the single `.compute()` call for this stage
- Log the partition count and output directory

**Error handling:**
- If the write fails (e.g. disk full, permission denied), allow the exception to
  propagate to the caller with the original error context preserved

**Test cases (TR-18):**
- Given a small Dask DataFrame, assert that calling `write_interim_parquet` produces
  at least one `.parquet` file in `output_dir`
- Assert each output Parquet file is readable by a fresh `pandas.read_parquet` call
  without raising an exception (TR-18)
- Given `n_files=3`, assert partition count is 10 (lower bound of formula)
- Given `n_files=30`, assert partition count is 30
- Given `n_files=100`, assert partition count is 50 (upper bound of formula)

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 06: Implement ingest stage entry point

**Module:** `kgs_pipeline/ingest.py`
**Function:** `run_ingest(config: IngestConfig, client: distributed.Client) -> IngestSummary`

**Description:**
Orchestrate the full ingest stage: load schema, ingest raw files in parallel, write
Parquet output. Return a summary of the run.

**Steps:**
- Load schema using `load_schema` with the data dictionary path from config
- Ingest raw files using `ingest_raw_files`
- Write output using `write_interim_parquet`
- Return `IngestSummary` with: `n_files_ingested`, `output_dir`, `partition_count`

**`IngestConfig` fields (dataclass):**
- `input_dir: Path` — directory of raw `.txt` files
- `output_dir: Path` — directory for interim Parquet output
- `dict_path: Path` — path to data dictionary CSV

**`IngestSummary` fields (dataclass):**
- `n_files_ingested: int`
- `output_dir: Path`
- `partition_count: int`

**Test cases:**
- Given a mocked set of raw files and a real distributed client (or `dask.distributed.Client()`
  in tests), assert `IngestSummary.n_files_ingested` matches the file count
- Assert `IngestSummary.partition_count` equals `max(10, min(n_files, 50))`
- Assert the output Parquet files are present in `output_dir` after the run

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Schema completeness test (TR-22)

**Module:** `tests/test_ingest.py`

**Description:**
Write integration-level tests that verify schema completeness across sampled interim
Parquet partitions after a complete ingest run.

**Test cases (TR-22):**
- After ingest completes (using a small synthetic dataset of at least 3 raw files),
  read at least 3 interim Parquet partition files independently
- For each sampled partition, assert all expected canonical columns are present:
  `LEASE_KID`, `LEASE`, `API_NUMBER`, `MONTH-YEAR`, `PRODUCT`, `PRODUCTION`,
  `WELLS`, `OPERATOR`, `COUNTY`, `source_file`
- Assert no expected column is missing or renamed in any sampled partition
- Assert each partition file is readable via `pandas.read_parquet` without error (TR-18)

**Definition of done:** Test cases implemented and passing, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
