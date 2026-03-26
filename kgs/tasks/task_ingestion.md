# Task Specification: Data Ingestion Component

**Module:** `kgs/src/ingestion.py`
**Component purpose:** Load all downloaded annual KGS CSV files in parallel into a
single unified raw Dask DataFrame. Handle schema variations across years, attach a
provenance column, and deliver a lazy Dask DataFrame that downstream stages can
chain transformations onto without forcing a compute.

---

## Overview

Each annual KGS file (e.g. `oil2020.csv`, `oil2021.csv`) shares a common schema
defined by the KGS data dictionary, but column names and the presence of optional
columns (e.g. `URL`) can vary between years. This component:

1. Discovers which raw CSV files are available on disk.
2. Reads each file in parallel using a `ThreadPoolExecutor`.
3. Normalises column names across individual file DataFrames to a canonical set.
4. Attaches a `source_year` provenance column.
5. Concatenates all per-year DataFrames into a single unified Dask DataFrame.

The canonical column set is derived from the KGS data dictionary:
`LEASE_KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`,
`OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`,
`SPOT`, `LATITUDE`, `LONGITUDE`, `MONTH_YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`,
`URL` (optional, may be absent for some years).

Note: The raw KGS file uses the column name `MONTH-YEAR` (hyphenated). During
ingestion this must be renamed to `MONTH_YEAR` (underscored) to be a valid
Python identifier and to comply with the snake_case downstream standard.

---

## Task 01: Define ingestion configuration dataclass and file discovery

**Module:** `kgs/src/ingestion.py`
**Function/Class:** `IngestionConfig` (dataclass), `discover_raw_files(config: IngestionConfig) -> list[dict]`

**Description:**
Define a frozen dataclass `IngestionConfig` and a pure function `discover_raw_files`
that scans the raw data directory for expected CSV files.

**IngestionConfig fields:**
- `raw_dir: str` — path to the raw data directory, e.g. `"data/raw"`
- `years: list[int]` — years to ingest; if empty, discover all `oil*.csv` files present
- `max_workers: int` — ThreadPoolExecutor worker count for parallel reads, default `4`
- `encoding: str` — file encoding, default `"utf-8"`
- `dtype_backend: str` — Pandas dtype backend to use when reading CSVs, default `"numpy_nullable"`

**discover_raw_files behaviour:**
- If `config.years` is non-empty, look for files matching `oil{year}.csv` for each year.
- If `config.years` is empty, glob for all files matching the pattern `oil*.csv`
  in `config.raw_dir` and infer the year from the filename.
- For each file found, return a dict with keys: `year: int`, `filename: str`,
  `file_path: Path`, `exists: bool`.
- Files that are in `config.years` but not present on disk should be included
  in the result with `exists=False` (so callers can log missing files clearly).

**Error handling:**
- Raise `ValueError` if `raw_dir` does not exist.
- Log a WARNING for each expected year whose file is not found.

**Dependencies:** dataclasses, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` Create two temp CSV files named `oil2020.csv` and `oil2021.csv`
  in a temp directory; assert `discover_raw_files` returns 2 dicts both with `exists=True`.
- `@pytest.mark.unit` Given `years=[2020, 2022]` but only `oil2020.csv` present, assert
  the 2022 entry has `exists=False` and a WARNING was logged.
- `@pytest.mark.unit` Given an empty `raw_dir`, assert `ValueError` is raised.
- `@pytest.mark.unit` Given `years=[]` and 3 `oil*.csv` files present, assert all 3 are
  discovered with correct `year` values parsed from filenames.

**Definition of done:** Dataclass and function are implemented, all test cases pass,
ruff and mypy report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task 02: Implement per-file CSV reader with schema normalisation

**Module:** `kgs/src/ingestion.py`
**Function:** `read_and_normalise_file(file_meta: dict, config: IngestionConfig) -> pandas.DataFrame`

**Description:**
Read a single KGS annual CSV file into a pandas DataFrame and normalise its
column names to the canonical set.

**Canonical column name mapping (applied in order):**
1. Strip leading/trailing whitespace from all column names.
2. Replace the literal hyphen in `MONTH-YEAR` → `MONTH_YEAR`.
3. Upper-case all column names.
4. Any column not in the canonical set is retained with a prefix `extra_` so
   nothing is silently dropped, but it will not be part of the canonical schema.

**Behaviour:**
- Read the CSV using `pandas.read_csv` with `encoding=config.encoding`,
  `dtype=str` (read all columns as strings to defer type coercion to the
  cleaning stage), and `dtype_backend=config.dtype_backend`.
- After column normalisation, add a `source_year` column (integer) set to
  `file_meta["year"]` for every row.
- For any canonical column that is absent from the file, add it with all
  values set to `pandas.NA`.
- Return the resulting DataFrame. Do NOT call `.compute()` — this function
  returns a pandas DataFrame since it is one partition's worth of data.
- Log INFO: filename, row count, and list of any columns that were missing
  and backfilled with NA.

**Error handling:**
- If the file cannot be parsed as CSV (e.g. `pandas.errors.ParserError`),
  log an ERROR and raise `IngestionError` (a custom exception defined in this module).
- If the file is empty (zero data rows after reading), log a WARNING and return
  an empty DataFrame with the canonical columns plus `source_year`.

**Dependencies:** pandas, pathlib, logging

**Test cases:**
- `@pytest.mark.unit` Write a minimal CSV with all canonical columns plus `URL`;
  assert the returned DataFrame has `source_year` column equal to the file's year
  and all canonical columns present.
- `@pytest.mark.unit` Write a CSV that omits the `URL` column; assert the returned
  DataFrame has a `URL` column populated entirely with `pandas.NA`.
- `@pytest.mark.unit` Write a CSV with a `MONTH-YEAR` column header; assert the
  returned DataFrame column is renamed to `MONTH_YEAR`.
- `@pytest.mark.unit` Write an empty CSV (header row only); assert an empty DataFrame
  is returned with the correct columns and a WARNING was logged.
- `@pytest.mark.unit` Write a corrupted/non-CSV file; assert `IngestionError` is raised.
- `@pytest.mark.unit` Assert that `dtype` of all canonical columns (except `source_year`)
  is `object` or `StringDtype` (string), confirming type coercion is deferred.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 03: Implement parallel file reader and Dask DataFrame builder

**Module:** `kgs/src/ingestion.py`
**Function:** `load_raw_files_parallel(file_metas: list[dict], config: IngestionConfig) -> dask.dataframe.DataFrame`

**Description:**
Use a `ThreadPoolExecutor` to read all discovered CSV files in parallel, then
assemble the results into a single Dask DataFrame.

**Behaviour:**
- Filter `file_metas` to only those with `exists=True`; log a WARNING listing
  any skipped files.
- Submit `read_and_normalise_file` calls to a `ThreadPoolExecutor` with
  `max_workers=config.max_workers`.
- Collect results in the order of submission (preserve year ordering for
  deterministic partition numbering).
- If any individual file read raises `IngestionError`, log the error and
  exclude that year from the combined DataFrame (do not abort the whole run).
- After all futures are resolved, convert the list of pandas DataFrames into
  a Dask DataFrame using `dask.dataframe.from_pandas` per partition (one
  partition per year file), then concatenate with `dask.dataframe.concat`.
- Return the resulting lazy Dask DataFrame. Do NOT call `.compute()`.

**Error handling:**
- Raise `IngestionError` if zero files were successfully read (nothing to ingest).
- Log an INFO summary: total files read, total rows (sum of pandas DataFrame
  lengths before Dask conversion), and number of partitions.

**Dependencies:** dask, pandas, concurrent.futures, logging

**Test cases:**
- `@pytest.mark.unit` Provide two minimal in-memory pandas DataFrames converted to
  temp CSV files; assert the returned object is a `dask.dataframe.DataFrame` and
  `.compute()` produces the expected concatenated result.
- `@pytest.mark.unit` Assert the returned Dask DataFrame has NOT been computed
  (verify return type is `dask.dataframe.DataFrame`, not `pandas.DataFrame`).
- `@pytest.mark.unit` When one file raises `IngestionError`, assert the other
  files are still ingested and the combined DataFrame excludes the failed year.
- `@pytest.mark.unit` When all files raise `IngestionError`, assert `IngestionError`
  is raised from `load_raw_files_parallel`.
- `@pytest.mark.unit` Assert partition count equals the number of successfully
  read files.
- `@pytest.mark.integration` Point `raw_dir` at `data/raw` and verify that all
  present `oil*.csv` files are loaded into a Dask DataFrame with a `source_year`
  column containing the correct year values.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 04: Implement top-level ingestion entry point

**Module:** `kgs/src/ingestion.py`
**Function:** `ingest(config: IngestionConfig) -> dask.dataframe.DataFrame`

**Description:**
Implement the public entry point that orchestrates discovery and parallel loading.

**Behaviour:**
- Call `discover_raw_files(config)` and log the discovery summary.
- Call `load_raw_files_parallel` with the discovered file metas and config.
- Return the lazy Dask DataFrame.
- Log total elapsed time for the ingestion step.

**Error handling:**
- Propagate `IngestionError` raised by `load_raw_files_parallel`.
- Do not swallow exceptions; all unexpected exceptions should propagate to
  the caller (the pipeline orchestrator).

**Dependencies:** dask, logging, time

**Test cases:**
- `@pytest.mark.unit` With mocked `discover_raw_files` returning 3 file metas
  and mocked `load_raw_files_parallel` returning a Dask DataFrame, assert
  `ingest` returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` Assert that `ingest` does not call `.compute()` on the
  returned DataFrame (return type must be `dask.dataframe.DataFrame`).
- `@pytest.mark.integration` With actual CSV files in `data/raw`, assert `ingest`
  returns a Dask DataFrame whose `.columns` include all canonical columns plus
  `source_year`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Design decisions and constraints

- All column names in the output Dask DataFrame are UPPER_CASE at this stage.
  snake_case standardisation is deferred to the cleaning component.
- All data columns are read as strings (dtype=str) to avoid silent type coercion
  errors during ingestion. Type conversion happens in the cleaning component.
- The `source_year` column is the sole provenance field added at ingestion time.
  It must always be present and must be typed as `int` (not string).
- Dask partitioning strategy at ingestion: one partition per annual file.
  Repartitioning for processing efficiency happens in the transform component.
- The `IngestionError` custom exception must be defined at module level in
  `ingestion.py` so it can be imported by the pipeline orchestrator.
- The function `load_raw_files_parallel` must never call `.compute()` on the
  Dask DataFrame it builds. Any test that checks the return type and finds a
  `pandas.DataFrame` should be treated as a failing test.
- Thread safety: `read_and_normalise_file` must not share mutable state with
  other threads. Each call operates on its own file handle and returns an
  independent DataFrame.
