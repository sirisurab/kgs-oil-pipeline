# Ingest Component — Task Specifications
**Module:** `kgs_pipeline/ingest.py`  
**Test file:** `tests/test_ingest.py`  
**Input directory:** `kgs/data/raw/`  
**Output directory:** `kgs/data/interim/`

---

## Design Decisions & Constraints

- All reading of raw `.txt` files uses **Dask** (`dask.dataframe.read_csv`) for parallel I/O — never `pandas.read_csv` for the full dataset.
- Each per-lease `.txt` file from `kgs/data/raw/` follows the same schema as the KGS monthly data dictionary (`kgs/references/kgs_monthly_data_dictionary.csv`): the columns are `LEASE_KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`, `MONTH-YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`, `URL`.
- `API_NUMBER` in KGS data holds **comma-separated API numbers** for all wells associated with the lease (e.g. `"15-131-20143, 15-131-20089-0002"`). This column must be exploded so each row represents one well-month observation.
- `MONTH-YEAR` is formatted as `M-YYYY` (e.g. `"1-2020"`, `"10-2023"`). It must be parsed into a `pandas.Period` or `datetime` of period `"MS"` (month-start) for time-series alignment. Rows where `MONTH-YEAR` has `Month=0` (yearly aggregates) or `Month=-1` (starting cumulative) must be **filtered out** — only monthly records are processed.
- All column names are normalized to lowercase with underscores (e.g. `MONTH-YEAR` → `month_year`, `LEASE_KID` → `lease_kid`, `API_NUMBER` → `api_number`).
- The interim output is a single Parquet dataset written to `kgs/data/interim/kgs_monthly_raw.parquet/` as a Dask-partitioned Parquet directory (using `dask.dataframe.to_parquet`).
- All functions must return `dask.dataframe.DataFrame` (not pandas), except small helper/utility functions that operate on single rows or metadata.
- Paths are sourced from `kgs_pipeline/config.py`.

---

## Task 05: Implement raw file discovery

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `discover_raw_files(raw_dir: Path) -> list[Path]`

**Description:**  
Scan the `raw_dir` directory and return a sorted list of all `.txt` files available for ingestion.

- Use `pathlib.Path.glob("*.txt")` to find all text files.
- Return the list sorted alphabetically by filename (ensures deterministic ordering for reproducibility).
- Log the count of discovered files at INFO level.
- If `raw_dir` does not exist, raise `FileNotFoundError` with a descriptive message.
- If `raw_dir` exists but contains zero `.txt` files, log a WARNING and return an empty list (do not raise).

**Dependencies:** `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a temporary directory containing 3 `.txt` files and 1 `.csv` file, assert the function returns exactly 3 `Path` objects all ending in `.txt`.
- `@pytest.mark.unit` — Given an empty temporary directory, assert the function returns `[]` and does not raise.
- `@pytest.mark.unit` — Given a path that does not exist, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Assert the returned list is sorted alphabetically.
- `@pytest.mark.integration` — Given `RAW_DATA_DIR`, assert the function returns a non-empty list if raw files have been downloaded (skip with `pytest.mark.skipif` if the directory is empty).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 06: Implement raw file reader

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `read_raw_files(file_paths: list[Path]) -> dask.dataframe.DataFrame`

**Description:**  
Read all per-lease `.txt` files into a single unified Dask DataFrame.

- Use `dask.dataframe.read_csv` with a glob pattern or an explicit list of file paths.
- All columns must be read as `dtype=str` initially to prevent Dask from mis-inferring types across files with mixed content.
- Add a column `source_file` (dtype `str`) containing the stem of the source filename (e.g. `"lp564"`) so each row can be traced back to its origin file.
- Normalize all column names: strip whitespace, convert to lowercase, replace `-` and spaces with `_` (e.g. `"MONTH-YEAR"` → `"month_year"`, `"LEASE_KID"` → `"lease_kid"`).
- If `file_paths` is an empty list, raise `ValueError("No raw files provided for ingestion")`.
- The function must return a `dask.dataframe.DataFrame` — `.compute()` must NOT be called inside this function.

**Error handling:**
- If any single file cannot be read (corrupt or wrong format), log the filename as a WARNING and continue reading the remaining files. Do not abort the full read.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given 2 small in-memory `.txt` files (written to a temp dir) with valid KGS schema, assert the returned object is a `dask.dataframe.DataFrame` (not pandas).
- `@pytest.mark.unit` — Assert the returned DataFrame has a `source_file` column and all values are non-null strings.
- `@pytest.mark.unit` — Assert all column names in the result are lowercase and contain no hyphens or spaces.
- `@pytest.mark.unit` — Given an empty `file_paths` list, assert `ValueError` is raised.
- `@pytest.mark.unit` — Assert that `.compute()` is not called inside `read_raw_files` by verifying the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.integration` — Given the actual files in `RAW_DATA_DIR`, assert the returned Dask DataFrame has more than 0 rows after `.compute()`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Implement MONTH-YEAR parser

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `parse_month_year(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Parse and filter the `month_year` column, converting it to a proper datetime, and remove non-monthly records.

- Filter out rows where the month component of `month_year` is `"0"` (yearly aggregates) or `"-1"` (starting cumulative), before attempting datetime parsing. These are identified by checking if the string starts with `"0-"` or `"-1-"`.
- Parse the remaining `month_year` strings (format `M-YYYY`, e.g. `"1-2020"`, `"10-2023"`) into `datetime64[ns]` using `pandas.to_datetime` with the format `"%m-%Y"` applied via `map_partitions`. The resulting datetime represents the first day of the reported month.
- Rename the column from `month_year` to `production_date` after parsing.
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called inside this function.

**Error handling:**
- Rows where `month_year` cannot be parsed (malformed strings) must be set to `NaT` (not dropped). These will be handled downstream in the transform step.
- Log the count of rows filtered (yearly + cumulative) at INFO level using a `map_partitions` approach or a `.describe()` after compute in tests only.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small Dask DataFrame with `month_year` values `["1-2020", "0-2020", "-1-2020", "12-2023"]`, assert the returned DataFrame contains exactly 2 rows (the yearly and cumulative rows are dropped).
- `@pytest.mark.unit` — Assert the `production_date` column dtype is `datetime64[ns]`.
- `@pytest.mark.unit` — Assert that `"1-2020"` parses to `2020-01-01` and `"10-2023"` parses to `2023-10-01`.
- `@pytest.mark.unit` — Given a row with an invalid `month_year` value (e.g. `"99-9999"`), assert it produces `NaT` without raising an exception.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`, confirming `.compute()` was not called.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Implement API number exploder

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `explode_api_numbers(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Transform lease-level rows into well-level rows by exploding the comma-separated `api_number` column.

Domain context: Each lease row in the KGS data can represent multiple wells, with their API numbers listed as a comma-separated string in the `api_number` column (e.g. `"15-131-20143, 15-131-20089-0002, 15-131-20239"`). To enable well-level analysis, each API number must become its own row, with all other columns duplicated.

- Split the `api_number` column on `","`, strip whitespace from each element, and explode so each API number gets its own row.
- Rows where `api_number` is null or empty string after stripping should produce a single row with `api_number` set to `None` (preserve the lease-level record, do not drop it).
- Rename the exploded column to `well_id` (this is the canonical well identifier going forward in the pipeline).
- Strip all leading/trailing whitespace from `well_id` values.
- Reset the Dask DataFrame index after the explosion (use `drop=True`).
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called inside this function. Use `map_partitions` to apply the explosion logic partition-by-partition.

**Error handling:**
- If the `api_number` column is missing from the input DataFrame, raise `KeyError` with a descriptive message.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame row with `api_number = "15-131-20143, 15-131-20089"`, assert the function produces 2 rows each with the correct `well_id` stripped of whitespace.
- `@pytest.mark.unit` — Given a row with a single API number (no comma), assert it produces exactly 1 row.
- `@pytest.mark.unit` — Given a row with `api_number = None`, assert the function produces 1 row with `well_id = None` (not dropped).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`, confirming `.compute()` was not called.
- `@pytest.mark.unit` — Assert the output DataFrame contains a `well_id` column and no `api_number` column.
- `@pytest.mark.unit` — Given a DataFrame missing the `api_number` column, assert `KeyError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 09: Implement interim Parquet writer

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `write_interim_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path) -> Path`

**Description:**  
Write the processed Dask DataFrame to a partitioned Parquet dataset in the interim directory.

- Write using `dask.dataframe.to_parquet` to `output_dir / "kgs_monthly_raw.parquet"`.
- Use `write_index=False` and `overwrite=True` to support re-runs.
- Use Snappy compression (`compression="snappy"`).
- Return the `Path` to the output Parquet directory.
- Log the output path and partition count at INFO level.

**Error handling:**
- If `output_dir` does not exist, create it with `mkdir(parents=True, exist_ok=True)` before writing.
- If the write fails for any reason, log the exception and re-raise it.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small 3-row Dask DataFrame, assert the function returns a `Path` that ends with `"kgs_monthly_raw.parquet"`.
- `@pytest.mark.unit` — Assert `output_dir` is created if it does not exist (use a temp directory).
- `@pytest.mark.integration` — After writing, assert the output directory contains at least one `.parquet` file readable by `dask.dataframe.read_parquet`.
- `@pytest.mark.integration` — Assert that reading back the written Parquet reproduces the same row count as the input Dask DataFrame (call `.compute()` in the test only).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 10: Implement ingest pipeline orchestrator

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `run_ingest_pipeline(raw_dir: Path, output_dir: Path) -> Path`

**Description:**  
Top-level entry point for the ingest component. Chains all ingest steps together and returns the path to the interim Parquet output.

Execution sequence:
1. Call `discover_raw_files(raw_dir)` to get the file list.
2. Call `read_raw_files(file_paths)` to produce the raw Dask DataFrame.
3. Call `parse_month_year(ddf)` to parse and filter production dates.
4. Call `explode_api_numbers(ddf)` to expand to well-level rows.
5. Call `write_interim_parquet(ddf, output_dir)` to persist to disk.
6. Return the output Parquet path.

- Log start and end of each step with elapsed time at INFO level.
- The function must **not** call `.compute()` on any intermediate Dask DataFrame — Dask's lazy graph should be built up across steps 2–4 and materialized only during `to_parquet` in step 5.

**Error handling:**
- If `discover_raw_files` returns an empty list, log a WARNING and return early with `None` — do not attempt to read or write.
- Propagate all other exceptions after logging them.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`, `time`

**Test cases:**
- `@pytest.mark.unit` — Mock all sub-functions. Assert they are called in the correct order (1 → 2 → 3 → 4 → 5).
- `@pytest.mark.unit` — Mock `discover_raw_files` to return `[]`. Assert the function returns `None` and does not call `read_raw_files`.
- `@pytest.mark.unit` — Assert that no intermediate Dask DataFrame is `.compute()`-d by verifying return types of mocked sub-functions remain `dask.dataframe.DataFrame`.
- `@pytest.mark.integration` — Given the actual `RAW_DATA_DIR` (if populated), run the full ingest pipeline and assert the interim Parquet directory exists and is non-empty.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
