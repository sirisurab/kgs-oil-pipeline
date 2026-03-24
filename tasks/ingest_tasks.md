# Ingest Component — Task Specifications

## Overview

The ingest component is responsible for:
1. Discovering all raw per-lease `.txt` files in `data/raw/` produced by the acquire component.
2. Reading every raw file into a unified Dask DataFrame (lazy), applying the schema from `references/kgs_monthly_data_dictionary.csv`.
3. Enriching each record with lease-level metadata from the archive index (`references/oil_leases_2020_present.txt`).
4. Filtering out non-monthly records (yearly summaries where `MONTH-YEAR` encodes Month=0, and starting cumulative records where Month=-1).
5. Adding a `source_file` traceability column to each row.
6. Writing the unified, enriched, filtered dataset to `data/interim/` as Parquet files partitioned by `LEASE_KID`.

**Source module:** `kgs_pipeline/ingest.py`
**Test file:** `tests/test_ingest.py`

---

## Design Decisions & Constraints

- All functions in `ingest.py` must return `dask.dataframe.DataFrame` (lazy). The only place `.compute()` is called is inside `write_interim_parquet()`, at write time.
- Raw files are comma-separated `.txt` files (same schema as CSV). They follow the schema in `references/kgs_monthly_data_dictionary.csv`.
- Key columns from the monthly data file: `LEASE KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`, `MONTH-YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`.
- The `LEASE KID` column name contains a space and must be renamed to `LEASE_KID` immediately on read.
- `MONTH-YEAR` is a string field (format `M-YYYY`, e.g. `3-2021`). Records with Month part equal to `0` are yearly summaries; records with Month part equal to `-1` are starting cumulative values. Both must be filtered out during ingestion.
- The archive index file (`oil_leases_2020_present.txt`) provides additional lease-level metadata columns (`FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`, `URL`) that supplement the per-lease monthly files.
- A `source_file` column (string, basename of the raw `.txt` file) must be added to every ingested row for traceability.
- The Parquet output in `data/interim/` is partitioned by `LEASE_KID`. Each partition file contains all monthly records for one lease.
- Encoding: KGS files may be encoded as `latin-1` (ISO-8859-1). Try `utf-8` first; fall back to `latin-1` on `UnicodeDecodeError`.
- Use `dask.dataframe.read_csv()` with `blocksize=None` (one partition per file) for raw file reading so that `source_file` can be mapped cleanly per file.
- All logging must use the Python `logging` module. No `print()` statements.
- Configuration values (directory paths, partition column name) must be sourced from `kgs_pipeline/config.py`.

---

## Task 06: Implement raw file discovery

**Module:** `kgs_pipeline/ingest.py`
**Function:** `discover_raw_files(raw_dir: Path) -> list[Path]`

**Description:**
Scan `raw_dir` and return a sorted list of all `.txt` files present. The function uses `Path.glob("*.txt")` to find files and returns them sorted by filename. This list drives all downstream ingestion steps.

Log the count of files discovered at `INFO` level.

**Error handling:**
- If `raw_dir` does not exist, raise `FileNotFoundError` with the path in the message.
- If `raw_dir` contains no `.txt` files, log a `WARNING` and return an empty list (do not raise).

**Dependencies:** `pathlib.Path`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Create 3 `.txt` files and 1 `.csv` file in `tmp_path`. Assert the function returns exactly 3 `Path` objects, all ending with `.txt`, in sorted order.
- `@pytest.mark.unit` — Call with a non-existent directory. Assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Call with an empty directory. Assert an empty list is returned (no exception).
- `@pytest.mark.integration` — Call with the real `data/raw/` directory (after acquire has run). Assert the return is a non-empty list of `Path` objects.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Implement single raw file reader

**Module:** `kgs_pipeline/ingest.py`
**Function:** `read_raw_file(file_path: Path) -> dask.dataframe.DataFrame`

**Description:**
Read a single KGS monthly production `.txt` file into a lazy Dask DataFrame. The function must:

1. Attempt to read the file using `dask.dataframe.read_csv()` with `encoding="utf-8"` and `blocksize=None`.
2. If a `UnicodeDecodeError` is raised (caught via Dask's deferred execution on `.dtypes` access), retry with `encoding="latin-1"`.
3. Rename the column `"LEASE KID"` to `"LEASE_KID"` immediately (using `rename(columns=...)`).
4. Add a `source_file` column containing the basename string of `file_path` (e.g. `"lp564.txt"`). Use `dask.dataframe.map_partitions` to assign this constant string column.
5. Return the lazy Dask DataFrame.

The function must not call `.compute()`.

**Error handling:**
- If the file does not exist, raise `FileNotFoundError`.
- If after both encoding attempts the file still cannot be parsed (malformed CSV), log an `ERROR` and raise `ValueError` with the file path in the message.
- If the `"LEASE KID"` column is absent in the file (some files may omit it), log a `WARNING` but continue without raising.

**Dependencies:** `dask.dataframe`, `pathlib.Path`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Write a small valid CSV string (with `LEASE KID` column, 5 rows) to `tmp_path`. Assert the function returns a `dask.dataframe.DataFrame` (not pandas), has a `LEASE_KID` column, and has a `source_file` column.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`, confirming `.compute()` has not been called inside the function.
- `@pytest.mark.unit` — Write a file with `latin-1`-encoded content (containing a non-UTF-8 byte). Assert the function returns a Dask DataFrame without raising.
- `@pytest.mark.unit` — Call with a non-existent path. Assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Call on a real file from `data/raw/`. Assert the resulting DataFrame has a `LEASE_KID` column, a `source_file` column, and the `source_file` value matches the filename.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Implement multi-file concatenation

**Module:** `kgs_pipeline/ingest.py`
**Function:** `concatenate_raw_files(file_paths: list[Path]) -> dask.dataframe.DataFrame`

**Description:**
Given a list of raw `.txt` file paths (from `discover_raw_files()`), call `read_raw_file()` on each file and concatenate all resulting lazy Dask DataFrames into a single lazy Dask DataFrame using `dask.dataframe.concat()`.

Processing steps:
1. For each path in `file_paths`, call `read_raw_file(path)`.
2. If `read_raw_file()` raises `ValueError` for a file (malformed), log an `ERROR` for that file and skip it (do not abort).
3. Collect all valid Dask DataFrames in a list.
4. If the list is empty, raise `ValueError("No valid raw files could be read")`.
5. Return `dask.dataframe.concat(valid_dfs)`.

Log the count of files successfully read vs. skipped at `INFO` level.

The function must not call `.compute()`.

**Error handling:**
- If `file_paths` is an empty list, raise `ValueError` with a descriptive message.
- Per-file `ValueError` from `read_raw_file()` is caught and logged, not re-raised.

**Dependencies:** `dask.dataframe`, `pathlib.Path`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide 3 valid small CSV files in `tmp_path`. Assert the return is a `dask.dataframe.DataFrame` with 3× the rows of a single file.
- `@pytest.mark.unit` — Provide an empty list. Assert `ValueError` is raised.
- `@pytest.mark.unit` — Provide 2 valid files and 1 malformed file. Assert the function returns a DataFrame covering just the 2 valid files and does not raise.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame` (lazy evaluation check).
- `@pytest.mark.integration` — Run against all files in `data/raw/`. Assert the result has more rows than any single file alone.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 09: Implement monthly record filter

**Module:** `kgs_pipeline/ingest.py`
**Function:** `filter_monthly_records(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Filter out non-monthly records from the concatenated raw Dask DataFrame. The `MONTH-YEAR` field (string, format `M-YYYY`) encodes both monthly and non-monthly records:
- Records where the month portion equals `"0"` are yearly production summaries — exclude them.
- Records where the month portion equals `"-1"` are starting cumulative values — exclude them.
- All other records (month 1–12) are valid monthly records — retain them.

Processing steps:
1. Extract the month portion from `MONTH-YEAR` by splitting on `"-"` and taking the first element. Handle the edge case where `MONTH-YEAR` is null.
2. Keep only rows where the extracted month string is a digit in the range `1` to `12` (inclusive). Specifically, cast the month part to integer and check `1 <= month <= 12`.
3. Return the filtered lazy Dask DataFrame.

Log the approximate percentage of records retained vs. filtered (using `len()` on the dask graph metadata, not `.compute()`) at `DEBUG` level if feasible; otherwise log a static message at `INFO`.

The function must not call `.compute()`.

**Error handling:**
- If the `MONTH-YEAR` column is absent, raise `KeyError` with a descriptive message.
- Rows where `MONTH-YEAR` is null are silently dropped (they are not valid monthly records).

**Dependencies:** `dask.dataframe`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Construct a small pandas DataFrame with 5 rows: months `1`, `6`, `12`, `0`, `-1`. Convert to Dask DataFrame. Assert the filtered result (after `.compute()`) has exactly 3 rows.
- `@pytest.mark.unit` — Include a row with a null `MONTH-YEAR`. Assert the row is excluded from the result.
- `@pytest.mark.unit` — Include only rows with month `0` and `-1`. Assert the result is an empty DataFrame (not an error).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert that a DataFrame missing the `MONTH-YEAR` column raises `KeyError`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 10: Implement lease metadata enrichment

**Module:** `kgs_pipeline/ingest.py`
**Function:** `enrich_with_lease_metadata(ddf: dask.dataframe.DataFrame, lease_index_file: Path) -> dask.dataframe.DataFrame`

**Description:**
Enrich the monthly production Dask DataFrame with lease-level metadata sourced from the KGS lease archive index file (`oil_leases_2020_present.txt`). This file contains columns such as `LEASE KID`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`, `URL`.

Processing steps:
1. Read the lease archive index into a pandas DataFrame using `pandas.read_csv()` (it is small enough for in-memory handling). Apply the same comma/tab delimiter auto-detection as in `load_lease_urls()`. Rename `"LEASE KID"` to `"LEASE_KID"`.
2. Select only the metadata columns not already present in the monthly data (to avoid duplication). Always include `LEASE_KID` as the join key. Include `URL` if present.
3. Broadcast the pandas metadata DataFrame into a Dask DataFrame using `dask.dataframe.from_pandas()` with `npartitions=1`.
4. Perform a left join of the monthly production Dask DataFrame on `LEASE_KID`, keeping all production records even if no metadata match exists.
5. Return the enriched lazy Dask DataFrame.

The function must not call `.compute()`.

**Error handling:**
- If `lease_index_file` does not exist, raise `FileNotFoundError`.
- If `LEASE_KID` is absent from either DataFrame, raise `KeyError` with a clear message identifying which DataFrame is missing the column.
- Duplicate metadata columns (columns present in both DataFrames) must be handled by suffixing with `_meta` on the right side and then dropping the suffixed duplicates.

**Dependencies:** `dask.dataframe`, `pandas`, `pathlib.Path`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Construct a small production Dask DataFrame (3 leases) and a metadata pandas DataFrame (2 matching leases + 1 extra). Assert the join result has 3 rows and that metadata columns are present for the 2 matching leases and null for the 1 unmatched.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Call with a non-existent `lease_index_file` path. Assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Call with a DataFrame missing `LEASE_KID`. Assert `KeyError` is raised.
- `@pytest.mark.integration` — Run against the real lease index file and an interim concatenated DataFrame. Assert the `OPERATOR` column is non-null for at least 80% of rows after enrichment.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 11: Implement interim Parquet writer and pipeline orchestrator

**Module:** `kgs_pipeline/ingest.py`
**Functions:**
- `write_interim_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path) -> Path`
- `run_ingest_pipeline() -> Path`

**Description:**

### `write_interim_parquet`
Write the enriched, filtered Dask DataFrame to Parquet format in `output_dir`, partitioned by `LEASE_KID`. Steps:
1. Ensure `output_dir` exists (`mkdir(parents=True, exist_ok=True)`).
2. Repartition the Dask DataFrame by `LEASE_KID` using `ddf.set_index("LEASE_KID", drop=False, sorted=False)` — this groups records by lease for efficient per-lease processing downstream.
3. Call `ddf.to_parquet(str(output_dir), engine="pyarrow", write_index=True, overwrite=True)`. This is the single `.compute()` call for the ingest pipeline (Dask calls `.compute()` internally in `to_parquet()`).
4. Log the output directory and number of partitions written at `INFO` level.
5. Return `output_dir` as a `Path`.

### `run_ingest_pipeline`
Synchronous entry-point that wires together the full ingest workflow:
1. Read config from `kgs_pipeline/config.py`.
2. Call `discover_raw_files(RAW_DATA_DIR)`.
3. Call `concatenate_raw_files(file_paths)`.
4. Call `filter_monthly_records(ddf)`.
5. Call `enrich_with_lease_metadata(ddf, LEASE_INDEX_FILE)`.
6. Call `write_interim_parquet(ddf, INTERIM_DATA_DIR)`.
7. Return `INTERIM_DATA_DIR`.

Log pipeline start and completion with elapsed time at `INFO` level.

**Error handling:**
- If `discover_raw_files()` returns an empty list, log an `ERROR` and raise `RuntimeError("No raw files to ingest")`.
- Any exception in steps 3–5 must propagate with full traceback (do not swallow).

**Dependencies:** `dask.dataframe`, `pyarrow`, `pathlib.Path`, `logging`, `time`

**Test cases:**

- `@pytest.mark.unit` — Provide a small valid Dask DataFrame (5 rows, 2 unique `LEASE_KID` values). Call `write_interim_parquet()` with `tmp_path`. Assert the directory contains Parquet files and each file is readable by `pandas.read_parquet()`.
- `@pytest.mark.unit` — Patch `discover_raw_files` to return an empty list. Assert `run_ingest_pipeline()` raises `RuntimeError`.
- `@pytest.mark.unit` — Patch all pipeline steps. Assert `run_ingest_pipeline()` calls each step exactly once in the correct order.
- `@pytest.mark.unit` — Assert that `write_interim_parquet()` creates `output_dir` if it does not exist.
- `@pytest.mark.integration` — Run `run_ingest_pipeline()` end-to-end against `data/raw/`. Assert that `data/interim/` is populated with readable Parquet files containing a `LEASE_KID` column, a `source_file` column, and a `PRODUCTION` column with no negative values surviving the ingest (pre-cleaning).

**Parquet readability test:**
- `@pytest.mark.integration` — For each Parquet file written to `data/interim/`, attempt to read it with `pandas.read_parquet()`. Assert no file raises an exception on read.

**Definition of done:** Both functions are implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
