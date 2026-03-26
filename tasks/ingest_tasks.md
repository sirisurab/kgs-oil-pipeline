# Ingest Component — Task Specifications

## Overview

The ingest component is responsible for:
1. Discovering and reading all raw per-lease monthly production `.txt` files from `data/raw/` using Dask's lazy CSV reader.
2. Filtering out non-monthly records (yearly summary rows where `MONTH-YEAR` has `Month=0`, and starting cumulative rows where `Month=-1`).
3. Enriching each record with lease-level metadata from the lease archive index (`data/external/oil_leases_2020_present.txt`) via a left join on `LEASE KID`.
4. Adding a `source_file` traceability column.
5. Writing a unified interim Parquet dataset to `data/interim/`, partitioned by `LEASE_KID`.
6. All pipeline functions must return lazy Dask DataFrames. The only `.compute()` call in the entire ingest component occurs inside `write_interim_parquet()`.

**Source module:** `kgs_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`

---

## Design Decisions & Constraints

- Python 3.11+. All functions must carry full type hints. Module must have a module-level docstring.
- Use `dask.dataframe` throughout. Functions must return `dask.dataframe.DataFrame` unless otherwise stated. Never call `.compute()` in helper functions — only in the write step.
- The raw `.txt` files are comma-delimited. The first row is the header. Encoding is `latin-1` (KGS files sometimes contain non-UTF-8 characters in operator names). Fall back to `utf-8` if `latin-1` fails.
- Column names in the raw files match the data dictionary: `LEASE KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`, `MONTH-YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`.
- `MONTH-YEAR` format: `M-YYYY` or `MM-YYYY` (e.g., `1-2020`, `12-2021`). Records with `Month=0` (yearly total) or `Month=-1` (starting cumulative) must be filtered out before writing.
- `PRODUCT` values: `O` = oil, `G` = gas (including coalbed methane).
- Partition the output Parquet by `LEASE_KID` — this collocates all records for a lease in a single Parquet partition file.
- Use `CONFIG` from `kgs_pipeline/config.py` for all path and parameter defaults.
- Use `setup_logging("ingest", CONFIG.logs_dir)` from `kgs_pipeline/utils.py`.
- Enforce a fixed Parquet output schema using `pyarrow.schema()` at write time to prevent schema drift across partitions.
- The `source_file` column must contain only the filename stem (e.g., `lp564`), not the full path.

### Expected interim Parquet schema

| Column | dtype | Notes |
|--------|-------|-------|
| `LEASE_KID` | `int64` | Renamed from `LEASE KID` (space replaced with underscore) |
| `LEASE` | `string` | Lease name |
| `DOR_CODE` | `string` | KS Dept. of Revenue ID |
| `API_NUMBER` | `string` | Comma-separated API numbers (raw, not yet exploded) |
| `FIELD` | `string` | Field name |
| `PRODUCING_ZONE` | `string` | Producing formation |
| `OPERATOR` | `string` | Operator name |
| `COUNTY` | `string` | County name |
| `TOWNSHIP` | `string` | PLSS township |
| `TWN_DIR` | `string` | Township direction |
| `RANGE` | `string` | PLSS range |
| `RANGE_DIR` | `string` | Range direction |
| `SECTION` | `string` | PLSS section |
| `SPOT` | `string` | Legal quarter description |
| `LATITUDE` | `float64` | NAD 1927 latitude |
| `LONGITUDE` | `float64` | NAD 1927 longitude |
| `MONTH_YEAR` | `string` | Raw `M-YYYY` string (renamed, not yet parsed to date) |
| `PRODUCT` | `string` | `O` or `G` |
| `WELLS` | `int64` | Number of contributing wells |
| `PRODUCTION` | `float64` | Production volume (BBL for oil, MCF for gas) |
| `source_file` | `string` | Filename stem of the raw source file |

---

## Task 06: Implement raw file discovery

**Module:** `kgs_pipeline/ingest.py`
**Function:** `discover_raw_files(raw_dir: Path | None = None) -> list[Path]`

**Description:**
Scan `raw_dir` (default: `CONFIG.raw_dir`) for all files matching the glob pattern `lp*.txt`. Return a sorted list of `Path` objects. Only include files for which `is_valid_raw_file()` (from `kgs_pipeline/utils.py`) returns `True`. Log the count of discovered files at INFO level and the count of skipped invalid files at WARNING level.

**Error handling:**
- Raise `FileNotFoundError` if `raw_dir` does not exist.
- Return an empty list (and log a WARNING) if no matching files are found.

**Dependencies:** pathlib, logging, kgs_pipeline.utils, kgs_pipeline.config

**Test cases (`tests/test_ingest.py`):**
- `@pytest.mark.unit` — Create 3 valid `lp*.txt` fixture files in `tmp_path`; call `discover_raw_files(tmp_path)` and assert the returned list has length 3 and is sorted.
- `@pytest.mark.unit` — Include one zero-byte file in `tmp_path`; assert it is excluded from the result.
- `@pytest.mark.unit` — Call with a non-existent directory; assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Call on a directory with no matching files; assert an empty list is returned and no exception is raised.
- `@pytest.mark.integration` — Call with `CONFIG.raw_dir` (populated by acquire); assert return type is `list[Path]` and all returned files have size > 0.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Implement Dask CSV reader for raw files

**Module:** `kgs_pipeline/ingest.py`
**Function:** `read_raw_files(file_paths: list[Path]) -> dask.dataframe.DataFrame`

**Description:**
Read all files in `file_paths` into a single lazy Dask DataFrame using `dask.dataframe.read_csv()`. Each file becomes one or more Dask partitions. Apply the following during reading:

- Set `encoding="latin-1"` (KGS-specific).
- Set `dtype` for all columns to `str` initially to prevent premature type inference errors on mixed-format files.
- Add a `source_file` column to each partition using `dask.dataframe.map_partitions()`. The column value must be the filename stem (e.g., `lp564`) extracted from the partition's source path. Use the `meta` parameter of `map_partitions` to declare the new column's dtype as `str`.
- Rename the column `LEASE KID` (with space) to `LEASE_KID` (with underscore) immediately after reading.
- Return the lazy Dask DataFrame without calling `.compute()`.

**Error handling:**
- If `file_paths` is empty, raise `ValueError("No raw files provided to read_raw_files()")`.
- Wrap `dask.dataframe.read_csv` in try/except for `Exception`; re-raise with a descriptive message that includes the number of files attempted.

**Dependencies:** dask.dataframe, pathlib, logging

**Test cases (`tests/test_ingest.py`):**
- `@pytest.mark.unit` — Create 2 minimal fixture `lp*.txt` CSV files (header + 2 data rows each) in `tmp_path`; call `read_raw_files` and assert the return type is `dask.dataframe.DataFrame` (do NOT call `.compute()` in this test — assert on the type only).
- `@pytest.mark.unit` — Call `read_raw_files([])` and assert `ValueError` is raised.
- `@pytest.mark.unit` — Call with valid fixture files and assert that `source_file` appears as a column in `ddf.columns` (check metadata only, no `.compute()`).
- `@pytest.mark.unit` — Call with valid fixture files and assert that `LEASE_KID` appears in `ddf.columns` (space-based name must be absent).
- `@pytest.mark.unit` — Verify the return type is `dask.dataframe.DataFrame`, not `pandas.DataFrame` (lazy evaluation test — do not call `.compute()`).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Implement monthly record filter

**Module:** `kgs_pipeline/ingest.py`
**Function:** `filter_monthly_records(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Filter the raw Dask DataFrame to retain only true monthly production records. The `MONTH_YEAR` column contains values in `M-YYYY` format. Records where the month component is `0` (yearly total) or `-1` (starting cumulative) must be excluded.

Implementation approach:
- Extract the month component from `MONTH_YEAR` by splitting on `-` and taking the first token.
- Cast the month token to integer.
- Retain only rows where the month integer is between 1 and 12 inclusive.
- Return the filtered lazy Dask DataFrame.

**Error handling:**
- If `MONTH_YEAR` column is absent, raise `KeyError("MONTH_YEAR column not found in DataFrame")`.
- Rows where `MONTH_YEAR` is null or cannot be parsed must be dropped (logged at WARNING level, counted).
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, logging

**Test cases (`tests/test_ingest.py`):**
- `@pytest.mark.unit` — Build a small pandas DataFrame with 3 rows: `MONTH_YEAR` = `["1-2020", "0-2020", "-1-2020"]`; convert to Dask; call `filter_monthly_records`; assert the result has exactly 1 row after `.compute()`.
- `@pytest.mark.unit` — Input with all valid monthly records (months 1–12); assert all rows are retained.
- `@pytest.mark.unit` — Input with a null `MONTH_YEAR` value; assert the null row is dropped and no exception is raised.
- `@pytest.mark.unit` — Call with a DataFrame missing the `MONTH_YEAR` column; assert `KeyError` is raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`, not `pandas.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 09: Implement lease metadata enrichment join

**Module:** `kgs_pipeline/ingest.py`
**Function:** `enrich_with_lease_metadata(ddf: dask.dataframe.DataFrame, lease_index_path: Path | None = None) -> dask.dataframe.DataFrame`

**Description:**
Left-join the production Dask DataFrame with the lease-level metadata from the archive index file (`data/external/oil_leases_2020_present.txt`). The join key is `LEASE_KID`.

Implementation:
1. Read the lease index file into a **pandas** DataFrame (it is small enough for in-memory loading). Select columns: `LEASE KID` → rename to `LEASE_KID`, plus any columns not already present in the production data that add useful context (e.g. `URL`). Drop duplicates on `LEASE_KID`.
2. Use `ddf.merge(lease_meta_pdf, on="LEASE_KID", how="left", suffixes=("", "_meta"))` to produce the enriched lazy Dask DataFrame.
3. Drop any `_meta`-suffixed duplicate columns from the result.
4. Return the enriched lazy Dask DataFrame.

**Error handling:**
- Raise `FileNotFoundError` if `lease_index_path` does not exist.
- If the `LEASE_KID` join key is absent from either DataFrame, raise `KeyError` with a descriptive message.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, pandas, pathlib, logging

**Test cases (`tests/test_ingest.py`):**
- `@pytest.mark.unit` — Build a minimal production Dask DataFrame and a minimal lease metadata pandas DataFrame sharing one `LEASE_KID` value; call `enrich_with_lease_metadata` and assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert no `_meta`-suffixed columns remain in the result's column list.
- `@pytest.mark.unit` — Pass a non-existent `lease_index_path`; assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Production DataFrame missing `LEASE_KID`; assert `KeyError` is raised.
- `@pytest.mark.unit` — After `.compute()` on a fixture dataset, assert all rows from the left (production) side are preserved (left join property).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 10: Implement schema casting and column normalization

**Module:** `kgs_pipeline/ingest.py`
**Function:** `apply_interim_schema(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Cast and normalize columns to match the expected interim Parquet schema defined in the overview. Specifically:

- Cast `LEASE_KID` from `str` → `int64`. Rows where the cast fails become NaN and are dropped (log count at WARNING).
- Cast `LATITUDE` and `LONGITUDE` from `str` → `float64`. Invalid values become `NaN` (not dropped — missing location data is acceptable).
- Cast `PRODUCTION` from `str` → `float64`. Invalid values become `NaN`.
- Cast `WELLS` from `str` → `int64` (coerce errors to `NaN` then cast to `Int64` nullable integer).
- Ensure `PRODUCT` contains only `"O"` or `"G"`. Rows with any other value must be dropped (log count at WARNING).
- Rename `MONTH-YEAR` column (with hyphen) to `MONTH_YEAR` if not already renamed.
- Ensure all remaining string columns (`LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `source_file`) are cast to string dtype.
- Return the typed lazy Dask DataFrame. Must not call `.compute()`.

**Dependencies:** dask.dataframe, pandas, logging

**Test cases (`tests/test_ingest.py`):**
- `@pytest.mark.unit` — Build a fixture DataFrame with `LEASE_KID` as string `"12345"`; assert after `apply_interim_schema` the column dtype is `int64` (check via `ddf.dtypes`).
- `@pytest.mark.unit` — Build a fixture with an unparseable `LEASE_KID` value (e.g., `"ABC"`); assert the corresponding row is dropped after `.compute()`.
- `@pytest.mark.unit` — Build a fixture with `PRODUCT = "X"` in one row; assert that row is dropped.
- `@pytest.mark.unit` — Assert `PRODUCTION` dtype is `float64` in the output schema.
- `@pytest.mark.unit` — Assert `LATITUDE` and `LONGITUDE` with invalid strings become `NaN` (not an exception) after `.compute()`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 11: Implement interim Parquet writer and ingest orchestrator

**Module:** `kgs_pipeline/ingest.py`
**Functions:**
- `write_interim_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path | None = None) -> list[Path]`
- `run_ingest_pipeline(raw_dir: Path | None = None, lease_index_path: Path | None = None, output_dir: Path | None = None) -> list[Path]`

**Description — `write_interim_parquet`:**
Write the Dask DataFrame to Parquet in `output_dir` (default: `CONFIG.interim_dir`), partitioned by `LEASE_KID`. Use `pyarrow` engine. Enforce the expected schema by passing a `pyarrow.schema()` object to `ddf.to_parquet(schema=...)`. This is the **only** `.compute()` call in the ingest component (triggered internally by `to_parquet`). Call `ensure_dir(output_dir)` before writing. Return a sorted list of all `.parquet` file paths written under `output_dir`.

**Description — `run_ingest_pipeline`:**
Orchestrate the full ingest workflow in sequence:
1. `discover_raw_files(raw_dir)` → list of paths.
2. `read_raw_files(file_paths)` → lazy Dask DataFrame.
3. `filter_monthly_records(ddf)` → filtered lazy Dask DataFrame.
4. `enrich_with_lease_metadata(ddf, lease_index_path)` → enriched lazy Dask DataFrame.
5. `apply_interim_schema(ddf)` → typed lazy Dask DataFrame.
6. `write_interim_parquet(ddf, output_dir)` → list of written paths.
Log a summary at INFO level: input file count, total rows written (call `.compute()` on row count only after the write is complete by reading back the metadata), output Parquet file count.

**Error handling:**
- If `discover_raw_files` returns an empty list, raise `RuntimeError("No valid raw files found; run acquire pipeline first.")`.
- If writing fails partway, log the exception at ERROR level and re-raise.

**Dependencies:** dask.dataframe, pyarrow, pathlib, logging, kgs_pipeline.config, kgs_pipeline.utils

**Test cases (`tests/test_ingest.py`):**
- `@pytest.mark.unit` — Build a small typed Dask DataFrame; call `write_interim_parquet` with `tmp_path`; assert at least one `.parquet` file is written.
- `@pytest.mark.unit` — Read back the written Parquet with `pandas.read_parquet`; assert the schema matches expectations (spot-check 5 columns and dtypes).
- `@pytest.mark.unit` — Assert every written Parquet file is readable by a fresh `dask.dataframe.read_parquet` call (Parquet readability test).
- `@pytest.mark.unit` — Patch `discover_raw_files` to return an empty list; assert `run_ingest_pipeline` raises `RuntimeError`.
- `@pytest.mark.unit` — Verify that calling `run_ingest_pipeline` in unit mode (with all dependencies patched to return tiny in-memory fixtures) produces output Parquet files in `tmp_path`.

**Partition correctness test (`tests/test_ingest.py`):**
- `@pytest.mark.integration` — After a real or fixture-driven ingest run, read each Parquet file from `data/interim/` individually and assert each file contains data for exactly one `LEASE_KID` value (no cross-partition contamination).

**Parquet readability test (`tests/test_ingest.py`):**
- `@pytest.mark.integration` — After writing, open each Parquet file in `data/interim/` with a fresh `pandas.read_parquet()` call; assert no exception is raised and the row count is > 0.

**Row count reconciliation test (`tests/test_ingest.py`):**
- `@pytest.mark.unit` — Build a fixture with 10 rows (including 2 yearly `Month=0` rows and 1 duplicate row). Run the full ingest pipeline on the fixture. Assert the written Parquet row count is at most 7 (10 − 2 yearly − 1 duplicate) and not greater than the input row count.

**Schema stability test (`tests/test_ingest.py`):**
- `@pytest.mark.unit` — Write interim Parquet from two fixture DataFrames representing two different leases (one with all-null `LATITUDE`). Read each back and assert both have identical column names and dtypes.

**Definition of done:** Both functions are implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
