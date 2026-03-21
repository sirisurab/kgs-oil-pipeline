# Ingest Component — Task Specifications

## Overview
The ingest component is responsible for:
1. Reading all raw per-lease `.txt` files from `kgs/data/raw/` (downloaded by the acquire component) using Dask.
2. Reading and parsing the lease-level archive index file `kgs/data/external/oil_leases_2020_present.txt` to enrich raw per-lease files with lease metadata.
3. Combining all per-lease data into a single unified Dask DataFrame.
4. Writing the unified result to `kgs/data/interim/` as Parquet files partitioned by `LEASE_KID`.
5. Keeping all operations lazy (no `.compute()` inside module functions) — Dask evaluation is triggered only by the orchestrator.

**Source module:** `kgs_pipeline/ingest.py`  
**Test file:** `tests/test_ingest.py`

---

## Design Decisions & Constraints
- Python 3.11+, Dask, Pandas, PyArrow (for Parquet).
- All file reads from `kgs/data/raw/` use `dask.dataframe.read_csv()` with the glob pattern `kgs/data/raw/lp*.txt`.
- The per-lease `.txt` files share the same schema as the lease archive file (same columns as `kgs_monthly_data_dictionary.csv`): `LEASE_KID`, `LEASE`, `DOR_CODE`, `API_NUMBER`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`, `MONTH-YEAR`, `PRODUCT`, `WELLS`, `PRODUCTION`, `URL`.
- Raw files may not include a `URL` column — treat it as optional (fill with `None` if absent).
- Records where `MONTH-YEAR` has `Month=0` (yearly summary) or `Month=-1` (starting cumulative) must be **filtered out** at ingest time — they are not part of the monthly production time-series.
- Dask DataFrame must **not** call `.compute()` internally; the orchestrator `run_ingest_pipeline()` is the only place `.compute()` is called.
- The interim Parquet output is partitioned by `LEASE_KID` using `dask.dataframe.to_parquet()` with `write_index=False`.
- All configuration paths come from `kgs_pipeline/config.py`.
- A `source_file` column (basename of the originating `.txt` file) must be added to each partition for traceability.

---

## Task 06: Implement `read_raw_lease_files()`

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `def read_raw_lease_files(raw_dir: Path = RAW_DATA_DIR) -> dask.dataframe.DataFrame`

**Description:**  
Read all per-lease `.txt` files downloaded into `kgs/data/raw/` as a single lazy Dask DataFrame.

Steps:
1. Construct the glob pattern `str(raw_dir / "lp*.txt")`.
2. Use `dask.dataframe.read_csv(glob_pattern, dtype=str, assume_missing=True)` to read all matching files into one Dask DataFrame. Using `dtype=str` defers type coercion to the transform stage to avoid early parse errors on messy data. `assume_missing=True` ensures Dask does not error on columns present in some files but absent in others.
3. Add a `source_file` column derived from the Dask `map_partitions` that, for each partition, extracts just the filename (basename) from a hidden `_metadata` path or by an alternative method: inject the filename as a column at read time using `include_path_column=True` if supported by the Dask version, otherwise handle via `map_partitions` with `pandas.Series`.
4. If no files match the glob (the `raw_dir` has no `lp*.txt` files), raise `FileNotFoundError` with a descriptive message.
5. Return the lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `raw_dir` does not exist, raise `FileNotFoundError`.
- If no matching files are found, raise `FileNotFoundError("No raw lease files found in <raw_dir>")`.

**Test cases:**
- `@pytest.mark.unit` — Write two small temporary `.txt` CSV files matching `lp*.txt` pattern to a tmp directory; assert `read_raw_lease_files(tmp_dir)` returns a `dask.dataframe.DataFrame` (not pandas).
- `@pytest.mark.unit` — Assert the returned Dask DataFrame has a `source_file` column.
- `@pytest.mark.unit` — Given a non-existent directory, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given a directory with no `lp*.txt` files, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Given the real `kgs/data/raw/` directory (populated by acquire), assert the Dask DataFrame has the expected columns from the data dictionary and that `.compute()` on a single partition succeeds.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 07: Implement `read_lease_index()`

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `def read_lease_index(lease_index_path: Path = LEASE_INDEX_FILE) -> dask.dataframe.DataFrame`

**Description:**  
Read the lease archive index file (`oil_leases_2020_present.txt`) as a Dask DataFrame to be used for metadata enrichment.

Steps:
1. Use `dask.dataframe.read_csv(str(lease_index_path), dtype=str, assume_missing=True)`.
2. Rename `MONTH-YEAR` column to `MONTH_YEAR` (replace hyphen with underscore) to make it a valid Python identifier and consistent with downstream column naming convention.
3. Filter out records where the `MONTH_YEAR` value starts with `0-` (yearly summary, Month=0) or `-1-` (starting cumulative, Month=-1). Use `dask.dataframe.map_partitions` with a pandas-compatible filter.
4. Select only the metadata columns needed for enrichment: `LEASE_KID`, `LEASE`, `DOR_CODE`, `FIELD`, `PRODUCING_ZONE`, `OPERATOR`, `COUNTY`, `TOWNSHIP`, `TWN_DIR`, `RANGE`, `RANGE_DIR`, `SECTION`, `SPOT`, `LATITUDE`, `LONGITUDE`. Drop `PRODUCTION`, `WELLS`, `PRODUCT`, `MONTH_YEAR`, `URL` from this read — they come from the per-lease files.
5. Deduplicate on `LEASE_KID` (keep first) since metadata columns are the same for all rows of the same lease.
6. Return the lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `lease_index_path` does not exist, raise `FileNotFoundError`.
- If required columns are missing, raise `KeyError` with a descriptive message listing the missing columns.

**Test cases:**
- `@pytest.mark.unit` — Given a small in-memory CSV written to a temp file, assert the function returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert that the returned DataFrame does not contain `PRODUCTION`, `WELLS`, or `PRODUCT` columns.
- `@pytest.mark.unit` — Assert that records with `MONTH_YEAR` of `"0-2020"` and `"-1-2020"` are excluded from the result.
- `@pytest.mark.unit` — Given a non-existent path, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Given the real `kgs/data/external/oil_leases_2020_present.txt`, assert the result is a valid Dask DataFrame and that `.compute()` returns a pandas DataFrame with `LEASE_KID` as a column and no duplicate `LEASE_KID` values.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 08: Implement `filter_monthly_records()`

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `def filter_monthly_records(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Given a raw Dask DataFrame (from `read_raw_lease_files()`), filter out non-monthly records:

1. Rename `MONTH-YEAR` column to `MONTH_YEAR` if it exists (handle both forms for robustness).
2. Drop rows where the month part of `MONTH_YEAR` equals `"0"` (yearly aggregate) or `"-1"` (starting cumulative). The `MONTH_YEAR` format is `"M-YYYY"` (e.g. `"3-2021"`), so parse the month by splitting on `"-"` and taking the first element.
3. Drop rows where `MONTH_YEAR` is null or empty.
4. Return the filtered lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If neither `MONTH-YEAR` nor `MONTH_YEAR` is present in the DataFrame columns, raise `KeyError("Expected column 'MONTH_YEAR' or 'MONTH-YEAR' not found")`.

**Test cases:**
- `@pytest.mark.unit` — Given a small pandas DataFrame with rows for months `1`, `0`, and `-1` converted to Dask, assert the returned Dask DataFrame contains only the month `1` row after `.compute()`.
- `@pytest.mark.unit` — Assert rows where `MONTH_YEAR` is null are dropped.
- `@pytest.mark.unit` — Given a DataFrame missing both `MONTH-YEAR` and `MONTH_YEAR`, assert `KeyError` is raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame` (lazy evaluation preserved).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 09: Implement `merge_with_metadata()`

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `def merge_with_metadata(raw_ddf: dask.dataframe.DataFrame, metadata_ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Left-join the raw per-lease production DataFrame with the lease metadata DataFrame on `LEASE_KID`, enriching each production record with lease-level metadata fields.

Steps:
1. Ensure both DataFrames have `LEASE_KID` as a string column (cast if necessary using `map_partitions`).
2. Perform a Dask merge: `raw_ddf.merge(metadata_ddf, on="LEASE_KID", how="left", suffixes=("", "_meta"))`.
3. Drop any `_meta`-suffixed duplicate columns that result from the merge (metadata columns already present in the raw file).
4. Return the merged lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `LEASE_KID` is not in `raw_ddf.columns`, raise `KeyError("LEASE_KID column missing from raw DataFrame")`.
- If `LEASE_KID` is not in `metadata_ddf.columns`, raise `KeyError("LEASE_KID column missing from metadata DataFrame")`.

**Test cases:**
- `@pytest.mark.unit` — Given two small Dask DataFrames with matching `LEASE_KID`, assert the merged result has columns from both.
- `@pytest.mark.unit` — Assert that `_meta`-suffixed columns are not present in the result.
- `@pytest.mark.unit` — Given a raw row with a `LEASE_KID` that has no match in metadata, assert the row is still present in the result (left join semantics) and metadata fields are `NaN`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a `raw_ddf` missing `LEASE_KID`, assert `KeyError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 10: Implement `write_interim_parquet()`

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `def write_interim_parquet(ddf: dask.dataframe.DataFrame, interim_dir: Path = INTERIM_DATA_DIR) -> None`

**Description:**  
Persist the unified Dask DataFrame to the interim Parquet store, partitioned by `LEASE_KID`.

Steps:
1. Create `interim_dir` if it does not exist (`interim_dir.mkdir(parents=True, exist_ok=True)`).
2. Call `ddf.to_parquet(str(interim_dir), partition_on=["LEASE_KID"], write_index=False, overwrite=True, engine="pyarrow")`.
3. This call triggers computation (`.to_parquet()` is an action in Dask). This is the only point in the ingest module where Dask computation occurs — it is acceptable here because writing to disk is inherently an eager action.
4. Log the number of partitions written and the target directory on completion.

**Error handling:**
- If `ddf` is not a `dask.dataframe.DataFrame`, raise `TypeError("Expected a dask DataFrame")`.
- Wrap the `to_parquet` call in a try/except for `OSError` and re-raise with a descriptive message including the target directory.

**Test cases:**
- `@pytest.mark.unit` — Given a small Dask DataFrame, assert `write_interim_parquet` creates the target directory and at least one `.parquet` file within it.
- `@pytest.mark.unit` — Given a plain pandas DataFrame (not Dask), assert `TypeError` is raised.
- `@pytest.mark.unit` — Assert the written Parquet files are readable by `pandas.read_parquet()`.
- `@pytest.mark.integration` — Given the merged Dask DataFrame produced from real raw data, assert Parquet files are written to `kgs/data/interim/` and each file's `LEASE_KID` column contains only one unique value (partition correctness).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 11: Implement `run_ingest_pipeline()`

**Module:** `kgs_pipeline/ingest.py`  
**Function:** `def run_ingest_pipeline(raw_dir: Path = RAW_DATA_DIR, lease_index_path: Path = LEASE_INDEX_FILE, interim_dir: Path = INTERIM_DATA_DIR) -> None`

**Description:**  
Synchronous orchestrator that chains all ingest steps into one callable pipeline entry point.

Steps:
1. Call `read_raw_lease_files(raw_dir)` → `raw_ddf`.
2. Call `filter_monthly_records(raw_ddf)` → `filtered_ddf`.
3. Call `read_lease_index(lease_index_path)` → `metadata_ddf`.
4. Call `merge_with_metadata(filtered_ddf, metadata_ddf)` → `merged_ddf`.
5. Call `write_interim_parquet(merged_ddf, interim_dir)` — this is the only computation trigger.
6. Log pipeline start, each step completion, and overall pipeline completion.

**Test cases:**
- `@pytest.mark.unit` — Mock all five sub-functions; assert each is called exactly once in the correct order.
- `@pytest.mark.unit` — If `read_raw_lease_files` raises `FileNotFoundError`, assert the exception propagates from `run_ingest_pipeline` (no swallowing).
- `@pytest.mark.integration` — Run `run_ingest_pipeline()` end-to-end using real raw data (requires acquire to have run); assert that `kgs/data/interim/` contains subdirectories named `LEASE_KID=<value>` each holding at least one `.parquet` file.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.
