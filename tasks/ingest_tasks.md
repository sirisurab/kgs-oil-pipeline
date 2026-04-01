# Ingest Component — Task Specifications

## Overview

The ingest component reads all raw KGS lease production data files downloaded by the acquire
stage from `data/raw/`, parses and validates their schema, applies date-range filtering
(year >= 2024), and writes the consolidated result to `data/interim/` as a small number of
Parquet files suitable for downstream transform processing. Dask is used for parallelism and
all Parquet I/O. The stage is idempotent and must not produce one file per lease.

**Module:** `kgs_pipeline/ingest.py`
**Entry-point CLI:** invoked by `pipeline.py --ingest`
**Test file:** `tests/test_ingest.py`

---

## Data schema reference

Raw KGS lease data files (`.txt`) are comma-delimited CSVs with quoted fields.
Columns defined by `references/kgs_monthly_data_dictionary.csv`:

| Column name     | Type       | Notes                                                  |
|-----------------|------------|--------------------------------------------------------|
| LEASE_KID       | str        | Unique lease ID assigned by KGS                        |
| LEASE           | str        | Lease name                                             |
| DOR_CODE        | str        | Kansas Dept. of Revenue code                           |
| API_NUMBER      | str        | Comma-separated API numbers for wells on the lease     |
| FIELD           | str        | Oil/gas field name                                     |
| PRODUCING_ZONE  | str        | Producing formation                                    |
| OPERATOR        | str        | Operator name                                          |
| COUNTY          | str        | County name                                            |
| TOWNSHIP        | str        | PLSS township number                                   |
| TWN_DIR         | str        | Township direction (always "S" in Kansas)              |
| RANGE           | str        | PLSS range number                                      |
| RANGE_DIR       | str        | Range direction (E or W)                               |
| SECTION         | str        | PLSS section (1–36)                                    |
| SPOT            | str        | Legal quarter description                              |
| LATITUDE        | float      | NAD 1927 latitude                                      |
| LONGITUDE       | float      | NAD 1927 longitude                                     |
| MONTH-YEAR      | str        | Format "M-YYYY"; Month=0 is yearly; Month=-1 is cumul. |
| PRODUCT         | str        | "O" = oil, "G" = gas                                   |
| WELLS           | float      | Number of wells contributing to lease                  |
| PRODUCTION      | float      | Oil in BBL, gas in MCF                                 |
| source_file     | str        | Filename of the raw file this row was read from        |

**Required columns for schema validation (minimum set):**
`LEASE_KID`, `LEASE`, `API_NUMBER`, `MONTH-YEAR`, `PRODUCT`, `PRODUCTION`, `WELLS`,
`OPERATOR`, `COUNTY`, `source_file`

---

## Design decisions and constraints

- All Parquet reads and writes use Dask. No `.compute()` calls inside module functions — all
  functions return `dask.dataframe.DataFrame`. `[TR-17]`
- After reading any Parquet dataset, immediately repartition to
  `min(npartitions, 50)` before any downstream operation.
- Target output file count: `max(1, total_rows // 500_000)` partitions. Never one file per lease.
  Always call `ddf.repartition(npartitions=N).to_parquet(...)` before writing.
- Date filtering (year >= 2024) is applied after reading raw files. Raw files contain full
  production history back to the 1960s. Extract year from MONTH-YEAR by splitting on "-" and
  taking the last element. Drop rows where the year component is not numeric and rows where
  year < 2024.
- String filtering using `.str` accessor must be performed inside a `map_partitions` function
  with an explicit `meta=` argument using `pd.StringDtype()` for string columns — not directly
  on a Dask Series.
- All string-typed `meta=` arguments must use `pd.StringDtype()`, never `"object"`.
- The `source_file` column (basename of the raw file) must be added during ingestion.
- Interim Parquet files are written to `data/interim/`.
- `requirements.txt` must be updated with all third-party packages imported in this module.

---

## Task 01: Implement single-file reader and parser

**Module:** `kgs_pipeline/ingest.py`
**Function:** `read_raw_file(file_path: str) -> pd.DataFrame`

**Description:**
Read a single KGS raw `.txt` file (comma-delimited, quoted fields) into a Pandas DataFrame.
Steps:
1. Read using `pd.read_csv` with `dtype=str` (read all columns as strings initially to avoid
   type coercion errors on malformed rows).
2. Strip leading/trailing whitespace from all column names.
3. Add a `source_file` column containing `Path(file_path).name` (basename only).
4. Return the raw DataFrame with string dtype for all columns except `source_file`.

Do not perform type casting or filtering inside this function — those are handled downstream.

**Inputs:** `file_path: str` — absolute or relative path to a raw `.txt` file.

**Outputs:** `pd.DataFrame` with all original columns plus `source_file`.

**Edge cases:**
- File does not exist → raise `FileNotFoundError`.
- File is empty (zero bytes) → raise `ValueError(f"Raw file is empty: {file_path}")`.
- File is valid CSV but has zero data rows (header only) → raise
  `ValueError(f"Raw file has no data rows: {file_path}")`.
- Columns with surrounding whitespace in the header → strip them silently.

**Test cases:**
- `@pytest.mark.unit` — Given a temporary CSV file with 3 data rows matching the KGS schema,
  assert the returned DataFrame has 3 rows and a `source_file` column equal to the basename.
- `@pytest.mark.unit` — Given a file with only a header row (no data), assert `ValueError`
  is raised.
- `@pytest.mark.unit` — Given a path to a non-existent file, assert `FileNotFoundError`.
- `@pytest.mark.unit` — Given column headers with leading/trailing spaces, assert they are
  stripped in the returned DataFrame.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Implement schema validator

**Module:** `kgs_pipeline/ingest.py`
**Function:** `validate_schema(df: pd.DataFrame, file_path: str) -> None`

**Description:**
Validate that a raw DataFrame contains the minimum required columns. The required column set
is: `LEASE_KID`, `LEASE`, `API_NUMBER`, `MONTH-YEAR`, `PRODUCT`, `PRODUCTION`, `WELLS`,
`OPERATOR`, `COUNTY`, `source_file`.

If any required column is missing, raise `SchemaError` (a custom exception defined in this
module) with a message listing the missing columns and the `file_path`. Do not raise on
extra/unexpected columns — they are allowed.

**Custom exception:** Define `class SchemaError(Exception): pass` at module level.

**Inputs:**
- `df: pd.DataFrame` — the raw DataFrame to validate.
- `file_path: str` — included in the error message for diagnostics.

**Outputs:** `None` (raises on failure).

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame containing all required columns plus extra columns,
  assert no exception is raised.
- `@pytest.mark.unit` — Given a DataFrame missing `PRODUCTION` and `COUNTY`, assert
  `SchemaError` is raised and the message includes both missing column names.
- `@pytest.mark.unit` — Given an empty DataFrame with all required columns, assert no
  exception is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Implement type coercion

**Module:** `kgs_pipeline/ingest.py`
**Function:** `coerce_types(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Convert columns from string dtype to their canonical types. Apply coercions in this order:
1. `PRODUCTION` → `float` (use `pd.to_numeric(errors="coerce")`; non-parseable values become NaN).
2. `WELLS` → `float` (same pattern).
3. `LATITUDE` → `float` (same pattern).
4. `LONGITUDE` → `float` (same pattern).
5. All remaining columns (except those already cast) → `pd.StringDtype()` using `.astype`.
6. Return the modified DataFrame.

Do not cast MONTH-YEAR to a datetime at this stage — it remains a string for the date-filter
step.

**Inputs:** `df: pd.DataFrame` — raw DataFrame (all columns initially string dtype).

**Outputs:** `pd.DataFrame` with coerced column types.

**Edge cases:**
- Non-numeric string in PRODUCTION (e.g., "N/A", "--") → coerced to NaN, no exception raised.
- Entire PRODUCTION column is NaN after coercion → allowed; validation of nulls is downstream.

**Test cases:**
- `@pytest.mark.unit` — Given a DataFrame row with `PRODUCTION="161.8"`, assert the coerced
  value is `float` 161.8.
- `@pytest.mark.unit` — Given a DataFrame row with `PRODUCTION="N/A"`, assert the coerced
  value is `NaN` (not an exception).
- `@pytest.mark.unit` — Assert all string columns in the output have `pd.StringDtype()` dtype.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Implement date-range filter

**Module:** `kgs_pipeline/ingest.py`
**Function:** `filter_date_range(df: pd.DataFrame, min_year: int = 2024) -> pd.DataFrame`

**Description:**
Filter the DataFrame to retain only rows where the year component of MONTH-YEAR >= `min_year`.
Steps:
1. Extract the year by splitting MONTH-YEAR on "-" and taking the **last** element (index -1).
   Use pandas string operations inside a helper function applied via `df.apply` or `map` — not
   directly via `.str` accessor on a potentially mixed-type column.
2. Drop rows where the extracted year component is not a valid integer string (e.g., "-1-1965"
   splits to ["", "1", "1965"] → last element "1965" is valid; "0-1966" → "1966" is valid).
   Drop rows where `int(year_str)` would raise (non-numeric).
3. Drop rows where year < `min_year`.
4. Also drop rows where `MONTH-YEAR` month component is 0 (yearly summary rows) or -1
   (starting cumulative rows) — these are administrative aggregates, not monthly production
   records. A monthly row has month component > 0.
5. Return the filtered DataFrame with the temporary year column dropped.

**Inputs:**
- `df: pd.DataFrame` — DataFrame with a `MONTH-YEAR` string column.
- `min_year: int` — minimum year to retain (inclusive). Default 2024.

**Outputs:** `pd.DataFrame` — filtered DataFrame.

**Edge cases:**
- All rows fall outside the date range → return an empty DataFrame (do not raise).
- MONTH-YEAR value "0-2024" (yearly summary) → drop, even though year == 2024.
- MONTH-YEAR value "-1-2024" (starting cumulative) → drop.
- MONTH-YEAR value "1-2024" → retain.

**Test cases:**
- `@pytest.mark.unit` — Given rows with MONTH-YEAR values "1-2024", "6-2023", "12-2025",
  assert only the 2024 and 2025 rows are returned (2 rows).
- `@pytest.mark.unit` — Given a row with MONTH-YEAR "0-2024" (year summary), assert it is
  dropped.
- `@pytest.mark.unit` — Given a row with MONTH-YEAR "-1-2024" (cumulative sentinel), assert
  it is dropped.
- `@pytest.mark.unit` — Given all rows with year < 2024, assert an empty DataFrame is returned.
- `@pytest.mark.unit` — Verify that zero-PRODUCTION rows with valid 2024 dates are retained
  (zeros are valid measurements). `[TR-05]`

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Implement parallel file ingestion pipeline

**Module:** `kgs_pipeline/ingest.py`
**Function:** `run_ingest(raw_dir: str, output_dir: str) -> dask.dataframe.DataFrame`

**Description:**
Main entry point for the ingest stage. Orchestrates reading, validation, coercion, and
date-filtering for all raw files, then writes consolidated interim Parquet output.

Steps:
1. Discover all `.txt` files in `raw_dir` using `pathlib.Path.glob("*.txt")`. If no files are
   found, raise `ValueError(f"No .txt files found in {raw_dir}")`.
2. For each file, apply the pipeline: `read_raw_file` → `validate_schema` → `coerce_types`
   → `filter_date_range`. Run this per-file pipeline using `dask.delayed` (one delayed task
   per file). Execute all delayed tasks via `dask.compute(*tasks, scheduler="threads")`.
   Each task returns a `pd.DataFrame` or raises; failures for individual files are logged as
   WARNING and that file is skipped (return `None` from the delayed wrapper; filter `None` from
   results).
3. Concatenate all resulting Pandas DataFrames using `pd.concat(results, ignore_index=True)`.
4. Convert to a Dask DataFrame using `dask.dataframe.from_pandas`.
5. Repartition to `max(1, total_rows // 500_000)` partitions.
6. Write to `output_dir` using `.to_parquet(output_dir, write_index=False)`.
7. Re-read the written Parquet with `dask.dataframe.read_parquet(output_dir)`.
8. Repartition to `min(npartitions, 50)`.
9. Return the Dask DataFrame.

The function must NOT call `.compute()` on the returned Dask DataFrame — only on the initial
per-file delayed tasks and during the Parquet write.

**Inputs:**
- `raw_dir: str` — directory containing raw `.txt` files.
- `output_dir: str` — directory for interim Parquet output.

**Outputs:** `dask.dataframe.DataFrame`

**Edge cases:**
- No `.txt` files in `raw_dir` → raise `ValueError`.
- All files fail validation → raise `ValueError("All raw files failed ingestion")`.
- `output_dir` does not exist → create with `Path(output_dir).mkdir(parents=True, exist_ok=True)`.
- After concatenation, if total rows == 0 → raise `ValueError("No data rows survived ingestion")`.

**Test cases:**
- `@pytest.mark.unit` — Given a `tmp_path` raw_dir with 3 minimal valid `.txt` fixtures, assert
  `run_ingest` returns a `dask.dataframe.DataFrame` (not a `pd.DataFrame`). `[TR-17]`
- `@pytest.mark.unit` — Assert the returned Dask DataFrame contains all required schema columns
  plus `source_file`. `[TR-22]`
- `@pytest.mark.unit` — Given 3 fixture files each with 4 rows (2 from 2024, 2 from 2023),
  assert the resulting DataFrame has exactly 6 rows (only 2024 rows retained per file).
- `@pytest.mark.unit` — Given a raw_dir with one valid and one invalid (missing column) file,
  assert the function does not raise and returns a DataFrame from the valid file only.
- `@pytest.mark.unit` — Assert that the output Parquet directory contains at least 1 file and
  no more than 200 files (never one file per lease). `[TR-18]`
- `@pytest.mark.unit` — Read back the written Parquet with a fresh `dask.dataframe.read_parquet`
  call; assert it succeeds without error and contains the expected schema. `[TR-18]`
- `@pytest.mark.unit` — Assert that zero-valued PRODUCTION rows are present in the output (not
  nulled). `[TR-05]`
- `@pytest.mark.integration` — Given actual files in `data/raw/` (populated by acquire stage),
  assert the output Parquet is readable and contains rows with year >= 2024 only.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Schema completeness across partitions test

**Module:** `tests/test_ingest.py`
**Test function:** `test_schema_completeness_across_partitions`

**Description:**
After running `run_ingest` on fixture data, read at least 3 distinct Parquet partition files
from the interim output directory. For each partition, assert that all required columns are
present: `LEASE_KID`, `LEASE`, `API_NUMBER`, `MONTH-YEAR`, `PRODUCT`, `PRODUCTION`, `WELLS`,
`OPERATOR`, `COUNTY`, `source_file`. No expected column should be missing or renamed in any
sampled partition. `[TR-22]`

This test is written separately from the `run_ingest` tests to make schema drift across
partitions explicit and easy to locate in CI.

**Test cases:**
- `@pytest.mark.unit` — Create fixture data that produces >= 3 partitions; read each partition
  file with `pd.read_parquet`; assert column sets are identical and all required columns present.
- `@pytest.mark.integration` — Read 3 random partition files from the actual `data/interim/`
  directory (populated by a real ingest run) and assert schema completeness. `[TR-22]`

**Definition of done:** Test function implemented, all assertions pass, ruff and mypy report no
errors.
