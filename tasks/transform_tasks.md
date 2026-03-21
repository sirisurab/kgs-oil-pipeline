# Transform Component — Task Specifications

## Overview
The transform component is responsible for:
1. Reading the interim Parquet data from `kgs/data/interim/` (produced by ingest).
2. Parsing and standardising dates, data types, units and column names.
3. Exploding comma-separated `API_NUMBER` fields into individual well-level records.
4. Cleaning the data: removing duplicates, handling nulls vs zeros, capping outliers, validating physical bounds.
5. Sorting each well's records chronologically and filling gaps (preserving zero vs null distinction).
6. Writing the cleaned, well-level data to `kgs/data/processed/` as Parquet files partitioned by `well_id`.

**Source module:** `kgs_pipeline/transform.py`  
**Test file:** `tests/test_transform.py`

---

## Design Decisions & Constraints
- Python 3.11+, Dask, Pandas, PyArrow.
- All intermediate transformations operate on lazy Dask DataFrames — `.compute()` must **not** be called inside any transform function.
- The only eager computation happens in `write_processed_parquet()` via `dask.dataframe.to_parquet()`.
- `MONTH_YEAR` (format `M-YYYY`) is parsed to a `datetime64[ns]` column named `production_date`, set to the **first day of the reported month**.
- `API_NUMBER` contains comma-separated API numbers for all wells on the lease. Each API number becomes an individual row (`well_id`). The `LEASE_KID` column is retained on each exploded row for traceability.
- Oil production (`PRODUCT == "O"`) is in BBL; gas production (`PRODUCT == "G"`) is in MCF. These must be labelled via a `unit` column.
- Zero production values in `PRODUCTION` must remain as `0.0` (not coerced to null) — zeros are valid measurements.
- Null `PRODUCTION` values represent missing data — they remain as `NaN` and are **not** filled with zero.
- Duplicate records (same `well_id`, `production_date`, `PRODUCT`) are deduplicated by keeping the first occurrence after sorting.
- Physical bound violations: negative `PRODUCTION` values are replaced with `NaN` and logged as warnings.
- Oil production values exceeding `MAX_REALISTIC_OIL_BBL_PER_MONTH` (50,000 BBL) are flagged in a boolean `outlier_flag` column (not removed) for downstream ML handling.
- All string columns are stripped of leading/trailing whitespace.
- Final processed schema column names use `snake_case`.
- All configuration paths come from `kgs_pipeline/config.py`.
- Output Parquet is partitioned by `well_id`, one directory per well: `kgs/data/processed/well_id=<value>/`.

---

## Task 12: Implement `load_interim_data()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def load_interim_data(interim_dir: Path = INTERIM_DATA_DIR) -> dask.dataframe.DataFrame`

**Description:**  
Read the interim Parquet store into a lazy Dask DataFrame.

Steps:
1. Use `dask.dataframe.read_parquet(str(interim_dir), engine="pyarrow")`.
2. If `interim_dir` does not exist or contains no `.parquet` files, raise `FileNotFoundError` with a descriptive message.
3. Return the lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `interim_dir` does not exist, raise `FileNotFoundError`.
- If no Parquet files are found within `interim_dir`, raise `FileNotFoundError("No Parquet files found in <interim_dir>")`.

**Test cases:**
- `@pytest.mark.unit` — Write a small Parquet file to a temporary directory; assert `load_interim_data(tmp_dir)` returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a non-existent directory, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Given an existing but empty directory, assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame` (not pandas).
- `@pytest.mark.integration` — Given the real `kgs/data/interim/` directory (requires ingest to have run), assert the loaded Dask DataFrame has the expected columns from the ingest schema.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 13: Implement `parse_dates()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def parse_dates(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Parse the `MONTH_YEAR` column (string, format `M-YYYY`) into a proper `datetime64[ns]` column named `production_date`.

Steps:
1. Using `ddf.map_partitions()`, apply a per-partition function that:
   a. Constructs a `"1-" + MONTH_YEAR` string to produce `"D-M-YYYY"` format (e.g. `"1-3-2021"`).
   b. Calls `pandas.to_datetime(<column>, format="%d-%m-%Y", errors="coerce")` to produce `NaT` on unparseable values instead of raising.
2. Assign the result to a new column `production_date`.
3. Drop the original `MONTH_YEAR` column.
4. Log a warning (using the `logging` module) if the percentage of `NaT` values in `production_date` exceeds 1% of rows (sampled via `map_partitions`).
5. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `MONTH_YEAR` column does not exist, raise `KeyError("MONTH_YEAR column not found")`.
- Unparseable dates produce `NaT` (not exceptions), as handled by `errors="coerce"`.

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with `MONTH_YEAR = "3-2021"`, assert `production_date` equals `pandas.Timestamp("2021-03-01")` after `.compute()`.
- `@pytest.mark.unit` — Given a `MONTH_YEAR = "0-2020"` that slipped through, assert `production_date` is `NaT` (coerced, not raised).
- `@pytest.mark.unit` — Given a DataFrame missing the `MONTH_YEAR` column, assert `KeyError` is raised.
- `@pytest.mark.unit` — Assert `MONTH_YEAR` column is not present in the output DataFrame.
- `@pytest.mark.unit` — Assert `production_date` dtype is `datetime64[ns]` after `.compute()`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 14: Implement `cast_and_rename_columns()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def cast_and_rename_columns(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Cast columns to correct data types and rename all columns to `snake_case` in a single pass.

**Column rename mapping (raw name → snake_case name):**
- `LEASE_KID` → `lease_kid`
- `LEASE` → `lease_name`
- `DOR_CODE` → `dor_code`
- `API_NUMBER` → `api_number`
- `FIELD` → `field_name`
- `PRODUCING_ZONE` → `producing_zone`
- `OPERATOR` → `operator`
- `COUNTY` → `county`
- `TOWNSHIP` → `township`
- `TWN_DIR` → `twn_dir`
- `RANGE` → `range_val`
- `RANGE_DIR` → `range_dir`
- `SECTION` → `section`
- `SPOT` → `spot`
- `LATITUDE` → `latitude`
- `LONGITUDE` → `longitude`
- `PRODUCT` → `product`
- `WELLS` → `well_count`
- `PRODUCTION` → `production`
- `source_file` → `source_file`
- `production_date` → `production_date` (already named, pass through)

**Type casting rules (applied via `map_partitions`):**
- `lease_kid`: `str` (strip whitespace)
- `lease_name`: `str` (strip whitespace)
- `dor_code`: `str` (strip whitespace)
- `api_number`: `str` (strip whitespace)
- `field_name`: `str` (strip whitespace)
- `producing_zone`: `str` (strip whitespace)
- `operator`: `str` (strip whitespace)
- `county`: `str` (strip whitespace)
- `township`: `str`
- `twn_dir`: `str`
- `range_val`: `str`
- `range_dir`: `str`
- `section`: `str`
- `spot`: `str`
- `latitude`: `float64` (use `pandas.to_numeric(..., errors="coerce")`)
- `longitude`: `float64` (use `pandas.to_numeric(..., errors="coerce")`)
- `product`: `str` (strip, uppercase)
- `well_count`: `float64` (use `pandas.to_numeric(..., errors="coerce")` — column has limited quality per data dictionary)
- `production`: `float64` (use `pandas.to_numeric(..., errors="coerce")`)
- `production_date`: `datetime64[ns]` (already parsed by `parse_dates`)
- `source_file`: `str`

Steps:
1. Use `ddf.rename(columns=rename_mapping)` to rename all columns.
2. Apply type casts via `ddf.map_partitions(lambda df: df.assign(...))`.
3. Strip whitespace from all string columns in the same `map_partitions` pass.
4. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If any column in the rename mapping is absent from `ddf.columns`, log a WARNING (do not raise) — the column may be optional (e.g. `URL` is optional per the data dictionary). Only raise `KeyError` for mandatory columns: `LEASE_KID`, `API_NUMBER`, `MONTH_YEAR` (via `production_date`), `PRODUCT`, `PRODUCTION`.

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with all raw column names, assert all output column names match the snake_case mapping.
- `@pytest.mark.unit` — Given a `PRODUCTION` column containing `"161.8"` (string), assert the cast result is `float64` value `161.8` after `.compute()`.
- `@pytest.mark.unit` — Given a `LATITUDE` column containing `"not_a_number"`, assert the result is `NaN` (coerced, not raised).
- `@pytest.mark.unit` — Given a `PRODUCT` column containing `" o "` (with spaces), assert the result is `"O"` (stripped and uppercased).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert that mandatory columns missing from the input raise `KeyError`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 15: Implement `explode_api_numbers()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def explode_api_numbers(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
The `api_number` column in KGS data contains comma-separated API numbers for all wells on a lease (e.g. `"15-131-20143, 15-131-20089-0002, 15-131-20239"`). This function explodes each row into one row per API number, creating a true well-level record.

Steps:
1. Use `ddf.map_partitions()` to apply a per-partition function that:
   a. Splits `api_number` on `","` and calls `pandas.Series.explode()`.
   b. Strips whitespace from each resulting `api_number` value.
   c. Drops rows where `api_number` is null, empty string, or `"nan"` after the split.
2. Rename the exploded `api_number` column to `well_id` to reflect that it now identifies a single well.
3. Retain the original `lease_kid` column on every exploded row.
4. Reset the partition index after the explode to avoid duplicate index values.
5. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `api_number` column does not exist, raise `KeyError("api_number column not found")`.

**Test cases:**
- `@pytest.mark.unit` — Given a row with `api_number = "15-001-00001, 15-001-00002"`, assert the output contains two rows with `well_id` values `"15-001-00001"` and `"15-001-00002"` after `.compute()`.
- `@pytest.mark.unit` — Given a row with a single `api_number`, assert only one row is produced.
- `@pytest.mark.unit` — Given a row with `api_number = "15-001-00001, "` (trailing comma), assert the empty entry is dropped and only one row remains.
- `@pytest.mark.unit` — Assert the output DataFrame has a `well_id` column and does not have an `api_number` column.
- `@pytest.mark.unit` — Assert `lease_kid` is retained on every exploded row.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing `api_number`, assert `KeyError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 16: Implement `validate_physical_bounds()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def validate_physical_bounds(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Enforce physical domain constraints on production data. This function applies domain rules consulted from the oil-and-gas-domain-expert:

**Physical laws being enforced:**
- Production volumes cannot be negative (sensor error or pipeline bug).
- Oil production above `MAX_REALISTIC_OIL_BBL_PER_MONTH` (50,000 BBL for a single well per month) is almost certainly a unit error and must be flagged.
- Latitude must be between 36.9 and 40.1 (Kansas bounds); longitude must be between -102.1 and -94.5 (Kansas bounds). Values outside these ranges are coerced to `NaN`.
- `product` must be one of `{"O", "G"}`. Records with any other value are dropped and logged.

Steps:
1. Using `ddf.map_partitions()`, apply a per-partition function that:
   a. Sets `production` to `NaN` where `production < 0` and logs a WARNING per affected row count.
   b. Adds a boolean column `outlier_flag` set to `True` where `product == "O"` and `production > MAX_REALISTIC_OIL_BBL_PER_MONTH`, `False` otherwise.
   c. Sets `latitude` to `NaN` where latitude is outside Kansas bounds.
   d. Sets `longitude` to `NaN` where longitude is outside Kansas bounds.
   e. Drops rows where `product` is not in `{"O", "G"}`.
2. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `production` column does not exist, raise `KeyError("production column not found")`.
- If `product` column does not exist, raise `KeyError("product column not found")`.

**Test cases:**
- `@pytest.mark.unit` — Given a row with `production = -5.0`, assert `production` is `NaN` in the output after `.compute()`.
- `@pytest.mark.unit` — Given a row with `product = "O"` and `production = 60000.0`, assert `outlier_flag` is `True`.
- `@pytest.mark.unit` — Given a row with `product = "O"` and `production = 1000.0`, assert `outlier_flag` is `False`.
- `@pytest.mark.unit` — Given a row with `latitude = 0.0` (outside Kansas), assert `latitude` is `NaN` in output.
- `@pytest.mark.unit` — Given a row with `product = "W"` (invalid), assert the row is dropped from the output.
- `@pytest.mark.unit` — Assert the `outlier_flag` column is present in the output DataFrame.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing `production`, assert `KeyError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 17: Implement `deduplicate_records()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def deduplicate_records(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Remove duplicate well-month-product records from the Dask DataFrame.

Steps:
1. Define the deduplication key as `["well_id", "production_date", "product"]`.
2. Sort each partition by `["well_id", "production_date", "product"]` using `map_partitions` with `pandas.DataFrame.sort_values()`.
3. Drop duplicates within each partition using `pandas.DataFrame.drop_duplicates(subset=dedup_key, keep="first")` via `map_partitions`.
4. After the partition-level dedup, perform a cross-partition sort and dedup using `ddf.map_partitions()` — note: because Dask does not support a full global sort easily, document the limitation that cross-partition duplicates (the same `well_id`+`production_date`+`product` key appearing in two different partitions) may not be caught by partition-level dedup alone. In the `run_transform_pipeline()` orchestrator, a global sort step must be applied after repartitioning by `well_id` before the final write.
5. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `well_id` or `production_date` columns are not present, raise `KeyError` with the missing column name.

**Test cases:**
- `@pytest.mark.unit` — Given a partition with 3 rows where 2 rows share the same `well_id`, `production_date`, and `product`, assert the output has 2 rows (1 duplicate removed) after `.compute()`.
- `@pytest.mark.unit` — Given a partition with no duplicates, assert the row count is unchanged.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing `well_id`, assert `KeyError` is raised.
- `@pytest.mark.technical` `@pytest.mark.unit` — Run `deduplicate_records()` twice on the same input Dask DataFrame and assert the output is identical (idempotency check via `.compute()` comparison).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 18: Implement `add_unit_column()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def add_unit_column(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Add a `unit` column to make the unit of measurement for each production record explicit and self-documenting for downstream ML/Analytics workflows.

Steps:
1. Using `ddf.map_partitions()`, apply a per-partition function that:
   a. Creates a `unit` column where `product == "O"` → `OIL_UNIT` (`"BBL"`) and `product == "G"` → `GAS_UNIT` (`"MCF"`), using `numpy.where` or `pandas.Series.map`.
   b. Any other `product` value gets `unit = "UNKNOWN"` (this should be rare after `validate_physical_bounds()` filtered invalid products, but is included for defensive coding).
2. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `product` column is not present, raise `KeyError("product column not found")`.

**Test cases:**
- `@pytest.mark.unit` — Given a row with `product = "O"`, assert `unit == "BBL"` after `.compute()`.
- `@pytest.mark.unit` — Given a row with `product = "G"`, assert `unit == "MCF"` after `.compute()`.
- `@pytest.mark.unit` — Given a row with `product = "X"` (unknown), assert `unit == "UNKNOWN"`.
- `@pytest.mark.unit` — Assert the `unit` column is present in the output DataFrame.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing `product`, assert `KeyError` is raised.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 19: Implement `sort_by_well_and_date()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def sort_by_well_and_date(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Repartition the Dask DataFrame by `well_id` and sort each partition chronologically by `production_date`. This is the step that brings all records for a single well into the same partition, enabling correct per-well cumulative calculations in the features component.

Steps:
1. Call `ddf.set_index("well_id", sorted=False, drop=False)` to set `well_id` as the Dask index (enabling partition-by-well semantics). Note: `drop=False` retains `well_id` as a column.
2. Within each partition, sort by `["well_id", "production_date", "product"]` using `map_partitions`.
3. Verify sort stability: after sorting, the last `production_date` in any partition for a given `well_id` must be less than or equal to the first `production_date` in the next partition for that same `well_id`. This is documented as a test requirement (see test cases), and can be verified via a post-`.compute()` check in the integration test.
4. Return the updated lazy Dask DataFrame — do **not** call `.compute()`.

**Error handling:**
- If `well_id` or `production_date` is not in `ddf.columns`, raise `KeyError` with the missing column name.

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with `well_id` records out of chronological order, assert that after `.compute()` each `well_id`'s records are in ascending `production_date` order.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing `well_id`, assert `KeyError` is raised.
- `@pytest.mark.integration` — Given real processed data, assert that for a sample well, the `production_date` series is strictly non-decreasing and that the last date of one partition precedes the first date of the next (sort stability across partition boundary).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 20: Implement `write_processed_parquet()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def write_processed_parquet(ddf: dask.dataframe.DataFrame, processed_dir: Path = PROCESSED_DATA_DIR) -> None`

**Description:**  
Persist the cleaned, well-level Dask DataFrame to the processed Parquet store, partitioned by `well_id`.

Steps:
1. Create `processed_dir` if it does not exist (`processed_dir.mkdir(parents=True, exist_ok=True)`).
2. Define the final output schema explicitly using `pyarrow.schema()` to prevent schema drift across wells. The schema must include all columns produced by the transform pipeline with their correct types (`string`, `float64`, `timestamp[ns]`, `bool`).
3. Call `ddf.to_parquet(str(processed_dir), partition_on=["well_id"], write_index=False, overwrite=True, engine="pyarrow", schema=output_schema)`.
4. Log the target directory and confirm completion.

**Error handling:**
- If `ddf` is not a `dask.dataframe.DataFrame`, raise `TypeError("Expected a dask DataFrame")`.
- If `well_id` is not in `ddf.columns`, raise `KeyError("well_id column required for partitioning")`.
- Wrap the `to_parquet` call in try/except for `OSError`; re-raise with a descriptive message.

**Test cases:**
- `@pytest.mark.unit` — Given a small Dask DataFrame with `well_id`, assert the function creates subdirectories of the form `well_id=<value>/` in the target directory.
- `@pytest.mark.unit` — Given a plain pandas DataFrame (not Dask), assert `TypeError` is raised.
- `@pytest.mark.unit` — Assert written Parquet files are readable by `pandas.read_parquet()` (Parquet readability check).
- `@pytest.mark.unit` — Assert that each Parquet partition file contains rows for exactly one unique `well_id` (partition correctness).
- `@pytest.mark.integration` — Given the real transformed Dask DataFrame, assert Parquet files are written to `kgs/data/processed/` and schema is consistent across all partitions (schema stability).

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.

---

## Task 21: Implement `run_transform_pipeline()`

**Module:** `kgs_pipeline/transform.py`  
**Function:** `def run_transform_pipeline(interim_dir: Path = INTERIM_DATA_DIR, processed_dir: Path = PROCESSED_DATA_DIR) -> None`

**Description:**  
Synchronous orchestrator that chains all transform steps into a single callable pipeline entry point.

Steps (in order):
1. `load_interim_data(interim_dir)` → `ddf`
2. `parse_dates(ddf)` → `ddf`
3. `cast_and_rename_columns(ddf)` → `ddf`
4. `explode_api_numbers(ddf)` → `ddf`
5. `validate_physical_bounds(ddf)` → `ddf`
6. `deduplicate_records(ddf)` → `ddf`
7. `add_unit_column(ddf)` → `ddf`
8. `sort_by_well_and_date(ddf)` → `ddf`
9. `write_processed_parquet(ddf, processed_dir)` ← only computation trigger

Log each step with timing using Python's `time.perf_counter()`.

**Domain-specific test cases:**

- `@pytest.mark.unit` `@pytest.mark.domain` — **Physical bound validation**: Given a DataFrame with a row where `production = -10.0`, assert it is `NaN` in the processed output.
- `@pytest.mark.unit` `@pytest.mark.domain` — **Zero production handling**: Given a row with `production = 0.0` in raw data, assert the value remains `0.0` (not `NaN`) in the processed Parquet output.
- `@pytest.mark.unit` `@pytest.mark.domain` — **Unit consistency**: Given oil records, assert all `unit` values are `"BBL"`; given gas records, assert all are `"MCF"`.
- `@pytest.mark.unit` `@pytest.mark.domain` — **Oil rate upper bound**: Given oil production values, assert no value exceeds `50000.0 BBL/month` without the `outlier_flag` being `True`.
- `@pytest.mark.integration` `@pytest.mark.domain` — **Well completeness**: For a sample of wells in the processed data, compute the expected number of months between first and last `production_date` and assert the actual record count is ≤ expected span (sparse wells are valid; over-count is a bug).
- `@pytest.mark.integration` — **Data integrity spot-check**: For a random sample of 50 well-month pairs, assert the `production` value in the processed Parquet matches the corresponding raw `.txt` file value (within float rounding tolerance of 0.01 BBL).
- `@pytest.mark.integration` — **Row count reconciliation**: Assert processed total row count ≤ raw total row count.
- `@pytest.mark.integration` — **Deduplication idempotency**: Run `deduplicate_records()` twice on the same input and assert both outputs are identical.
- `@pytest.mark.unit` — **Lazy Dask evaluation**: Assert that no transform function (except `write_processed_parquet`) returns a `pandas.DataFrame` — all must return `dask.dataframe.DataFrame`.
- `@pytest.mark.integration` — **Schema stability**: Load two processed Parquet partitions for different `well_id` values and assert their PyArrow schemas are identical.
- `@pytest.mark.integration` — **Partition correctness**: For each processed Parquet partition file, assert `well_id` has exactly one unique value.
- `@pytest.mark.integration` — **Parquet readability**: For every Parquet file in `kgs/data/processed/`, assert `pandas.read_parquet(file)` succeeds without error.

**Orchestrator test cases:**
- `@pytest.mark.unit` — Mock all sub-functions; assert each is called exactly once in the correct sequence.
- `@pytest.mark.unit` — If `load_interim_data` raises `FileNotFoundError`, assert it propagates from `run_transform_pipeline`.

**Definition of done:** Function implemented, all test cases pass, ruff and mypy report no errors, `requirements.txt` updated with all third-party packages imported in this task.
