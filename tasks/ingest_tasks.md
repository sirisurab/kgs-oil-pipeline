# Ingest Component Tasks

**Module:** `kgs_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`

## Overview

The ingest component reads all raw KGS lease data files (`.txt`, treated as CSV) from
`data/raw/`, parses them into a unified Dask DataFrame, applies date-range filtering
(year >= 2024), adds provenance metadata, and writes the result as consolidated Parquet
files to `data/interim/`.

The raw files follow the schema defined in `references/kgs_monthly_data_dictionary.csv`:
`LEASE KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`,
`COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`,
`LONGITUDE`, `MONTH-YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`.

## Design Decisions

- All functions return `dask.dataframe.DataFrame` — never call `.compute()` internally
  (TR-17). The caller decides when to materialise.
- The interim Parquet output targets `npartitions = max(1, total_rows // 500_000)` to
  avoid tens of thousands of tiny files. Always `repartition(npartitions=N)` before
  writing. Never use `partition_on` with a high-cardinality column like `LEASE KID`.
- Column name normalisation: strip whitespace, replace spaces and hyphens with
  underscores, uppercase all names (e.g. `"LEASE KID"` → `"LEASE_KID"`,
  `"MONTH-YEAR"` → `"MONTH_YEAR"`).
- All string columns in Dask `meta=` arguments must use `pd.StringDtype()`, never `"object"`.
- After reading any Parquet dataset, immediately repartition to `min(npartitions, 50)`.
- Date-range filter: extract year from `MONTH_YEAR` column (format `"M-YYYY"`, split on
  `"-"`, take the last element). Keep only rows where year >= `min_year` (default 2024)
  and the year is numeric. Rows with non-numeric year components (e.g. `"-1-1965"`,
  `"0-1966"`) are dropped.
- A `source_file` column is added to every ingested row recording the basename of the
  originating file.
- Logging: structured JSON via `kgs_pipeline.logging_utils.get_logger`; log file count,
  total raw row count, and filtered row count at INFO level.

---

## Task 01: Raw file discovery

**Module:** `kgs_pipeline/ingest.py`
**Function:** `discover_raw_files(raw_dir: str, pattern: str = "*.txt") -> list[Path]`

**Description:**
Scan `raw_dir` for all files matching `pattern` (default `"*.txt"`). Return a sorted
list of `Path` objects. Skip hidden files (names beginning with `.`). Raise
`FileNotFoundError` if `raw_dir` does not exist. Log the count of discovered files.

**Dependencies:** `pathlib`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a temporary directory with 3 `.txt` files and 1 `.csv`
  file, assert `discover_raw_files` returns exactly 3 paths, all with `.txt` suffix.
- `@pytest.mark.unit` — Given a temporary directory with a hidden file `.hidden.txt`
  and 2 regular `.txt` files, assert the hidden file is excluded and the result has
  2 paths.
- `@pytest.mark.unit` — Given a path that does not exist, assert `FileNotFoundError`
  is raised.
- `@pytest.mark.unit` — Given an empty directory, assert an empty list is returned
  without error.
- `@pytest.mark.integration` — Given `data/raw/` on disk, assert `discover_raw_files`
  returns a non-empty list (requires prior acquire run).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 02: Single-file reader with schema detection

**Module:** `kgs_pipeline/ingest.py`
**Function:** `read_raw_file(file_path: Path) -> pd.DataFrame`

**Description:**
Read a single KGS raw lease file into a pandas DataFrame. The files are `.txt` formatted
as CSV (comma-separated). Handle encoding issues and schema inconsistencies:

1. Attempt to read with `encoding="utf-8"`. On `UnicodeDecodeError`, retry with
   `encoding="latin-1"`.
2. Strip leading/trailing whitespace from all column names.
3. Normalise column names: replace spaces and hyphens with underscores, uppercase all
   names (e.g. `"LEASE KID"` → `"LEASE_KID"`, `"MONTH-YEAR"` → `"MONTH_YEAR"`).
4. Add a `source_file` column containing `file_path.name` (basename only).
5. Ensure all expected columns are present. The expected columns are:
   `LEASE_KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`,
   `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`,
   `SPOT`, `LATITUDE`, `LONGITUDE`, `MONTH_YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`.
   For any expected column absent in the file, add the column with all-null values.
6. If the file is empty (0 bytes) or has no rows beyond the header, log a warning and
   return an empty DataFrame with the expected columns.

**Error handling:**
- Catch and log any `pd.errors.ParserError`; return an empty DataFrame with expected
  columns on parse failure.
- Never raise from this function — a single bad file must not abort the pipeline.

**Dependencies:** `pandas`, `pathlib`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a temporary `.txt` file with all expected columns and 3
  data rows, assert the returned DataFrame has shape `(3, 21)` (20 expected + 1
  source_file column) and column names are normalised.
- `@pytest.mark.unit` — Given a file with missing `COUNTY` column, assert the returned
  DataFrame contains a `COUNTY` column filled with `NaN`.
- `@pytest.mark.unit` — Given a file with Latin-1 encoded content (bytes with character
  code > 127), assert `read_raw_file` returns a non-empty DataFrame without raising.
- `@pytest.mark.unit` — Given a 0-byte file, assert the function returns an empty
  DataFrame with all expected columns present and no exception is raised.
- `@pytest.mark.unit` — Given a file with corrupted CSV content that triggers
  `pd.errors.ParserError`, assert the function returns an empty DataFrame (no exception).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 03: Year-range row filter

**Module:** `kgs_pipeline/ingest.py`
**Function:** `filter_by_year(ddf: dd.DataFrame, min_year: int = 2024) -> dd.DataFrame`

**Description:**
Filter a Dask DataFrame to retain only rows where the year component of `MONTH_YEAR`
is >= `min_year`. Apply the filter inside a `map_partitions` function — never use the
Dask `.str` accessor directly on a Dask Series (it is unreliable after repartition or
astype).

Implementation approach:
```
def _filter_partition(pdf: pd.DataFrame, min_year: int) -> pd.DataFrame:
    # split MONTH_YEAR on "-", take last element, coerce to int,
    # drop rows where coercion fails (non-numeric), keep year >= min_year
    ...

ddf = ddf.map_partitions(_filter_partition, min_year, meta=ddf._meta)
```

- The `_filter_partition` function must handle:
  - Missing/null `MONTH_YEAR` values (drop those rows).
  - Non-numeric year components such as those arising from entries like `"-1-1965"` or
    `"0-1966"` — after splitting on `"-"` and taking the last element, `"1965"` is
    numeric and correctly excluded; entries where the last token is non-numeric are dropped.
- Return a new Dask DataFrame with identical schema (`meta` unchanged).

**Dependencies:** `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a Dask DataFrame with rows having MONTH_YEAR values
  `["1-2023", "12-2024", "6-2025", "0-2022"]`, assert the filtered result has exactly
  2 rows (2024 and 2025).
- `@pytest.mark.unit` — Given a row with MONTH_YEAR = `"1-2024"` (boundary), assert
  the row is retained.
- `@pytest.mark.unit` — Given a row with MONTH_YEAR = `"12-2023"` (boundary), assert
  the row is excluded.
- `@pytest.mark.unit` — Given rows with null MONTH_YEAR, assert they are excluded and
  no exception is raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`, not
  `pd.DataFrame` (TR-17).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 04: Multi-file Dask ingestion pipeline

**Module:** `kgs_pipeline/ingest.py`
**Function:** `build_dask_dataframe(file_paths: list[Path], min_year: int = 2024) -> dd.DataFrame`

**Description:**
Construct a single Dask DataFrame from multiple raw files using Dask delayed:

1. For each `file_path` in `file_paths`, wrap `read_raw_file(file_path)` in a
   `dask.delayed` call.
2. Assemble the delayed objects into a Dask DataFrame using `dd.from_delayed(...)` with
   an explicit `meta` argument. The meta must use `pd.StringDtype()` for all string
   columns and `float64` for `LATITUDE`, `LONGITUDE`, `PRODUCTION`, `WELLS`.
3. Repartition to `min(len(file_paths), 50)` immediately after construction.
4. Apply `filter_by_year(ddf, min_year)`.
5. Return the filtered Dask DataFrame.

The explicit `meta` dictionary for `dd.from_delayed` must cover all 21 columns
(20 data + `source_file`) with correct dtypes. Use `pd.StringDtype()` for all
columns that would otherwise be `object`.

**Dependencies:** `dask`, `dask.dataframe`, `pandas`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given 3 temporary raw files each with 5 rows (years 2023 and
  2024 mixed), assert the result is a `dask.dataframe.DataFrame` (TR-17) with at least
  the expected columns.
- `@pytest.mark.unit` — Given an empty list of file paths, assert the function raises
  `ValueError` with a descriptive message.
- `@pytest.mark.unit` — Assert the `source_file` column is present in the returned
  Dask DataFrame.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame` (TR-17).

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 05: Interim Parquet writer

**Module:** `kgs_pipeline/ingest.py`
**Function:** `write_interim_parquet(ddf: dd.DataFrame, output_dir: str) -> int`

**Description:**
Write the Dask DataFrame to `data/interim/` as consolidated Parquet files.

1. Compute `npartitions = max(1, ddf.npartitions // 10)` as the target partition count
   (consolidate the delayed partitions down before writing). The goal is to write
   between 1 and 50 Parquet files — never one file per source lease.
   More precisely: after obtaining the final filtered Dask DataFrame, repartition to
   `max(1, estimated_total_rows // 500_000)` where `estimated_total_rows` is derived
   from sampling or is set conservatively to `ddf.npartitions * 10_000` if the
   exact count is unknown at write time.
2. Call `ddf.repartition(npartitions=N).to_parquet(output_dir, write_index=False,
   overwrite=True)`.
3. Return the number of Parquet files written (use `len(list(Path(output_dir).glob("*.parquet")))`).
4. Log the number of files written and the output directory.

**Error handling:**
- Raise `ValueError` if `output_dir` resolves to an existing non-directory path.
- Create `output_dir` (including parents) if it does not exist.

**Dependencies:** `dask`, `pathlib`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Given a small Dask DataFrame (3 partitions, 30 rows total),
  assert `write_interim_parquet` returns an integer >= 1 and the output directory
  contains at least one `.parquet` file.
- `@pytest.mark.unit` — Given a non-existent output directory, assert the directory
  is created and the function does not raise.
- `@pytest.mark.unit` — Assert the return value equals the actual number of `.parquet`
  files found in `output_dir` after the call.

**Definition of done:** Function implemented, all tests pass, `ruff` and `mypy` report
no errors. `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 06: Ingest stage orchestrator and CLI

**Module:** `kgs_pipeline/ingest.py`
**Function:** `run_ingest(raw_dir: str, output_dir: str, min_year: int = 2024) -> int`
**Entry point:** `main()` and `if __name__ == "__main__"` block

**Description:**
Chain all ingest functions into a single callable:

1. Call `discover_raw_files(raw_dir)` → list of paths.
2. Call `build_dask_dataframe(paths, min_year)` → Dask DataFrame.
3. Call `write_interim_parquet(ddf, output_dir)` → file count.
4. Return the file count.

`main()` uses `argparse`:
- `--raw-dir`: default from `config.RAW_DATA_DIR`
- `--output-dir`: default from `config.INTERIM_DATA_DIR`
- `--min-year`: integer, default 2024

Register console script in `pyproject.toml`:
`kgs-ingest = "kgs_pipeline.ingest:main"`

**Dependencies:** `argparse`, `kgs_pipeline.config`, `kgs_pipeline.logging_utils`

**Test cases:**

- `@pytest.mark.unit` — Using `unittest.mock.patch` on `discover_raw_files`,
  `build_dask_dataframe`, and `write_interim_parquet`, assert `run_ingest` calls
  each in order and returns the mocked file count.
- `@pytest.mark.unit` — Assert `main()` with `--min-year 2024` calls `run_ingest`
  with `min_year=2024`.
- `@pytest.mark.integration` — Given `data/raw/` populated by acquire and
  `data/interim/` as output, assert `run_ingest` writes at least 1 Parquet file and
  returns a positive integer.

**Definition of done:** `run_ingest` and `main` implemented, all tests pass, `ruff` and
`mypy` report no errors, console script registered. `requirements.txt` updated with all
third-party packages imported in this task.

---

## Task 07: Ingest schema and Parquet readability tests (TR-17, TR-18, TR-22)

**Module:** `tests/test_ingest.py`

**Description:**
Implement the test cases mandated by TR-17 (lazy Dask evaluation), TR-18 (Parquet
readability), and TR-22 (schema completeness across partitions).

**Test cases:**

- `@pytest.mark.unit` TR-17 — Call `build_dask_dataframe` with a list of 2 temporary
  raw files. Assert the return type is `dask.dataframe.DataFrame`, not `pd.DataFrame`.
- `@pytest.mark.integration` TR-18 — After calling `run_ingest` on real data, read
  each `.parquet` file in `data/interim/` using `pd.read_parquet` and assert no
  `pyarrow` exception is raised for any file.
- `@pytest.mark.integration` TR-22 — After `run_ingest` completes, read at least 3
  Parquet partition files from `data/interim/`. Assert all of the following columns are
  present in every partition: `LEASE_KID`, `LEASE`, `API_NUMBER`, `MONTH_YEAR`,
  `PRODUCT`, `PRODUCTION`, `WELLS`, `OPERATOR`, `COUNTY`, `source_file`. Assert no
  expected column is missing or renamed in any sampled partition.

**Definition of done:** All test cases implemented in `tests/test_ingest.py`, all unit
tests pass, `ruff` and `mypy` report no errors. `requirements.txt` updated with all
third-party packages imported in this task.
