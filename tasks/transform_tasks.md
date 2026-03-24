# Transform Component — Task Specifications

## Overview

The transform component is responsible for:
1. Loading the interim Parquet store produced by the ingest component from `data/interim/`.
2. Standardising column names to `snake_case` and casting all columns to their correct data types.
3. Parsing the `MONTH-YEAR` string field into a proper `datetime64[ns]` column (`production_date`).
4. Exploding comma-separated `API_NUMBER` values into one row per well (`well_id`), so that each row represents a single well-month observation.
5. Validating physical production bounds: flagging impossible values (negative production, anomalously high oil volumes) and preserving the distinction between zero production and missing data.
6. Deduplicating on the natural key `[well_id, production_date, product]`.
7. Adding a `unit` column indicating the measurement unit (`BBL` for oil, `MCF` for gas).
8. Sorting each well's records chronologically and repartitioning by `well_id`.
9. Writing the cleaned, well-level dataset to `data/processed/` as Parquet files partitioned by `well_id`.

**Source module:** `kgs_pipeline/transform.py`
**Test file:** `tests/test_transform.py`

---

## Design Decisions & Constraints

- All functions in `transform.py` must return `dask.dataframe.DataFrame` (lazy). The only place `.compute()` is called is inside `write_processed_parquet()`, triggered by Dask's internal `to_parquet()` machinery.
- Column name standardisation: all columns renamed to lowercase `snake_case`. Mapping includes (but is not limited to): `LEASE_KID → lease_kid`, `LEASE → lease_name`, `DOR_CODE → dor_code`, `API_NUMBER → api_number`, `FIELD → field_name`, `PRODUCING_ZONE → producing_zone`, `OPERATOR → operator`, `COUNTY → county`, `TOWNSHIP → township`, `TWN_DIR → twn_dir`, `RANGE → range_num`, `RANGE_DIR → range_dir`, `SECTION → section`, `SPOT → spot`, `LATITUDE → latitude`, `LONGITUDE → longitude`, `MONTH-YEAR → month_year`, `PRODUCT → product`, `WELLS → well_count`, `PRODUCTION → production`, `source_file → source_file`.
- `PRODUCT` values: `"O"` = oil (unit `BBL`), `"G"` = gas (unit `MCF`). Any other value should be logged as a warning and the row kept.
- Physical bound constraints (domain rules from oil-and-gas domain expert):
  - `production` must be non-negative. Negative values are physically impossible — set to `NaN` and set `outlier_flag = True`.
  - Oil production (`product == "O"`) above `50000` BBL/month for a single well is likely a unit error — set `outlier_flag = True` but retain the value.
  - `production` of exactly `0.0` is a valid observation (well shut in) — preserve as `0.0`, not `NaN`.
  - `NaN` production means data was not reported — preserve as `NaN`.
- The `outlier_flag` column is a boolean column added to every row, defaulting to `False`.
- `api_number` may contain multiple API numbers separated by commas (e.g. `"15-001-12345, 15-001-67890"`). Each must become its own row (explosion step). Rows where `api_number` is null or empty string get a synthetic `well_id` derived from `lease_kid` (e.g. `"LEASE-<lease_kid>"`).
- After explosion, the column is renamed from `api_number` to `well_id`. Strip leading/trailing whitespace from each `well_id`.
- Deduplication key: `[well_id, production_date, product]`. Keep the last occurrence (presumed most recent/correct). Log the number of duplicates removed at `INFO`.
- All logging must use the Python `logging` module. No `print()` statements.
- The processed output Parquet is partitioned by `well_id` to ensure all records for a well are co-located for correct per-well cumulative and rolling calculations in the features step.
- Use `pyarrow.schema()` to enforce a fixed schema at Parquet write time, preventing schema drift when one well has all-null columns.
- Configuration values (directory paths, outlier threshold) must come from `kgs_pipeline/config.py`. Add `OIL_OUTLIER_THRESHOLD_BBL = 50000` to config.

---

## Task 12: Extend configuration for transform stage

**Module:** `kgs_pipeline/config.py`
**Function:** N/A — add constants

**Description:**
Add the following constants to `kgs_pipeline/config.py` for use by the transform component:

- `OIL_OUTLIER_THRESHOLD_BBL` — integer `50000` (max plausible monthly oil production per well in BBL)
- `PARTITION_COLUMN_PROCESSED` — string `"well_id"` (column used to partition processed Parquet output)
- `INTERIM_PARTITION_COLUMN` — string `"LEASE_KID"` (column used to partition interim Parquet)
- `COLUMN_RENAME_MAP` — a `dict[str, str]` mapping raw uppercase/hyphenated column names to their `snake_case` equivalents, as described in the Design Decisions section above.

**Error handling:** N/A.

**Dependencies:** None beyond what was already in `config.py`.

**Test cases:**

- `@pytest.mark.unit` — Assert `OIL_OUTLIER_THRESHOLD_BBL` is an integer equal to `50000`.
- `@pytest.mark.unit` — Assert `COLUMN_RENAME_MAP` is a dict and maps `"MONTH-YEAR"` to `"month_year"`.
- `@pytest.mark.unit` — Assert `COLUMN_RENAME_MAP` maps `"PRODUCTION"` to `"production"`.
- `@pytest.mark.unit` — Assert `PARTITION_COLUMN_PROCESSED` is the string `"well_id"`.

**Definition of done:** Constants are added, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 13: Implement column renaming and dtype casting

**Module:** `kgs_pipeline/transform.py`
**Function:** `rename_and_cast_columns(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Apply the `COLUMN_RENAME_MAP` from `config.py` to standardise all column names. Then cast each column to its correct dtype:

- `lease_kid` → `str`
- `lease_name` → `str`
- `dor_code` → `str`
- `api_number` → `str` (comma-separated, not yet exploded)
- `field_name` → `str`
- `producing_zone` → `str`
- `operator` → `str`
- `county` → `str`
- `township` → `int32` (coerce errors to `NaN`, nullable integer)
- `twn_dir` → `str`
- `range_num` → `int32` (coerce errors to `NaN`, nullable integer)
- `range_dir` → `str`
- `section` → `int32` (coerce errors to `NaN`, nullable integer)
- `spot` → `str`
- `latitude` → `float64`
- `longitude` → `float64`
- `month_year` → `str` (parsing to datetime happens in the next task)
- `product` → `str`
- `well_count` → `int32` (coerce errors to `NaN`, nullable integer)
- `production` → `float64`
- `source_file` → `str`

For all string columns, fill `NaN` with empty string `""` only if explicitly required; otherwise preserve nulls so the zero-vs-null distinction is maintained.

For numeric columns (`township`, `range_num`, `section`, `well_count`), use `pandas.Int32Dtype()` (nullable integer) so that NaN can coexist with integer values.

The function must not call `.compute()`.

**Error handling:**
- Unknown column names in the DataFrame that are not in `COLUMN_RENAME_MAP` must be preserved as-is (no drop).
- If `production` cannot be cast to `float64` for a row, coerce to `NaN` (use `errors="coerce"`).

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Build a Dask DataFrame with all expected raw column names. Assert that after `rename_and_cast_columns()`, the `"MONTH-YEAR"` column is absent and `"month_year"` is present.
- `@pytest.mark.unit` — Assert `production` column dtype is `float64` after the call.
- `@pytest.mark.unit` — Assert `township` column dtype is pandas nullable integer (`Int32`) after the call.
- `@pytest.mark.unit` — Include a row with `production = "not_a_number"`. Assert the row's `production` becomes `NaN` (not an error).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert that extra columns not in `COLUMN_RENAME_MAP` are retained unchanged.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 14: Implement production date parser

**Module:** `kgs_pipeline/transform.py`
**Function:** `parse_production_date(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Parse the `month_year` string column (format `M-YYYY`, e.g. `"3-2021"`) into a proper `datetime64[ns]` column named `production_date`, representing the first day of the production month. Drop the original `month_year` column after successful parsing.

Processing steps:
1. Using `dask.dataframe.map_partitions`, apply a pandas-level function that:
   - Splits `month_year` on `"-"` into month and year parts.
   - Constructs a date string of the form `"YYYY-MM-01"` (zero-pad the month to 2 digits).
   - Converts to `datetime64[ns]` using `pandas.to_datetime(format="%Y-%m-%d", errors="coerce")`.
2. Assign the result as the `production_date` column.
3. Drop the `month_year` column.
4. Rows where `production_date` is null after parsing (malformed `month_year` values) are kept in the DataFrame but will fail physical validation downstream — this is intentional for traceability.

The function must not call `.compute()`.

**Error handling:**
- If `month_year` column is absent, raise `KeyError` with a descriptive message.
- Malformed date strings (e.g. `"99-2021"` for month 99) are coerced to `NaT` via `errors="coerce"`, not raised.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide a DataFrame with `month_year` values `["1-2021", "12-2023", "6-2022"]`. Assert `production_date` values are `2021-01-01`, `2023-12-01`, `2022-06-01` respectively.
- `@pytest.mark.unit` — Provide a row with `month_year = "99-2021"`. Assert `production_date` is `NaT` (not an exception).
- `@pytest.mark.unit` — Assert the `month_year` column is absent in the output DataFrame.
- `@pytest.mark.unit` — Assert `production_date` dtype is `datetime64[ns]`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Call with a DataFrame missing `month_year`. Assert `KeyError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 15: Implement API number explosion (lease-to-well expansion)

**Module:** `kgs_pipeline/transform.py`
**Function:** `explode_api_numbers(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Transform lease-level production rows into well-level rows by splitting the comma-separated `api_number` column. Each API number in the comma-separated list becomes its own row, with all other columns duplicated.

Processing steps:
1. Using `dask.dataframe.map_partitions`, apply a pandas-level function that:
   - Strips whitespace from `api_number`.
   - Replaces empty-string `api_number` values with `NaN`.
   - Fills `NaN` `api_number` with a synthetic identifier: `"LEASE-" + lease_kid`.
   - Splits `api_number` on `","` to produce a list per row.
   - Uses `pandas.DataFrame.explode("api_number")` to expand list elements into individual rows.
   - Strips whitespace from each resulting `api_number` value after explosion.
2. Rename the `api_number` column to `well_id`.
3. Return the exploded lazy Dask DataFrame.

The function must not call `.compute()`.

**Error handling:**
- If `api_number` column is absent, raise `KeyError`.
- If `lease_kid` column is absent (needed for synthetic IDs), raise `KeyError`.
- Rows with `NaN` `lease_kid` get synthetic `well_id = "LEASE-UNKNOWN"` — log a `WARNING` for the count of such rows.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide a row with `api_number = "15-001-12345, 15-001-67890"`. Assert the result contains 2 rows with `well_id` values `"15-001-12345"` and `"15-001-67890"`.
- `@pytest.mark.unit` — Provide a row with `api_number = NaN` and `lease_kid = "99999"`. Assert the result contains 1 row with `well_id = "LEASE-99999"`.
- `@pytest.mark.unit` — Provide a row with `api_number = ""`. Assert it is treated the same as `NaN` (synthetic ID).
- `@pytest.mark.unit` — Assert leading/trailing spaces in API numbers are stripped: `" 15-001-12345 "` becomes `"15-001-12345"`.
- `@pytest.mark.unit` — Assert the `api_number` column is absent in the output (renamed to `well_id`).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 16: Implement physical bounds validation

**Module:** `kgs_pipeline/transform.py`
**Function:** `validate_physical_bounds(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Apply oil-and-gas domain rules to flag or nullify physically impossible production values. Add an `outlier_flag` boolean column (default `False`) and apply the following rules:

1. **Negative production** (physically impossible — cannot un-produce): Set `production = NaN` and `outlier_flag = True` for all rows where `production < 0`.
2. **Anomalously high oil production**: For rows where `product == "O"` and `production > OIL_OUTLIER_THRESHOLD_BBL` (50,000 BBL/month), set `outlier_flag = True`. Do not nullify the value — preserve it for human review.
3. **Zero production** (valid — well shut in for the month): Leave `production = 0.0` unchanged. Do not set `outlier_flag = True` for zero values.
4. **NaN production** (missing data — not reported): Leave as `NaN`. Do not set `outlier_flag = True` for nulls.

Log the count of negative-production rows nullified and the count of outlier-flagged rows at `INFO` level.

The function must not call `.compute()`.

**Error handling:**
- If `production` column is absent, raise `KeyError`.
- If `product` column is absent, raise `KeyError`.
- `OIL_OUTLIER_THRESHOLD_BBL` must be sourced from `kgs_pipeline/config.py`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases (domain-specific — physical law validation):**

- `@pytest.mark.unit` — Row with `production = -100.0`, `product = "O"`. Assert `production` becomes `NaN` and `outlier_flag` is `True`.
- `@pytest.mark.unit` — Row with `production = 0.0`, `product = "O"`. Assert `production` remains `0.0` and `outlier_flag` is `False`. (Zero ≠ missing.)
- `@pytest.mark.unit` — Row with `production = NaN`, `product = "O"`. Assert `production` remains `NaN` and `outlier_flag` is `False`. (Missing data preserved.)
- `@pytest.mark.unit` — Row with `production = 75000.0`, `product = "O"`. Assert `outlier_flag` is `True` and `production` is still `75000.0` (not nullified).
- `@pytest.mark.unit` — Row with `production = 75000.0`, `product = "G"`. Assert `outlier_flag` is `False` (gas threshold is not applied in this step).
- `@pytest.mark.unit` — Row with `production = 500.0`, `product = "O"`. Assert `outlier_flag` is `False` and `production` is `500.0`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert the `outlier_flag` column exists and has boolean dtype.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 17: Implement unit label assignment

**Module:** `kgs_pipeline/transform.py`
**Function:** `assign_unit_labels(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Add a `unit` string column to indicate the measurement unit of the `production` column. Apply the following mapping:
- `product == "O"` → `unit = "BBL"` (barrels)
- `product == "G"` → `unit = "MCF"` (thousand cubic feet)
- Any other `product` value → `unit = "UNKNOWN"`

Use `dask.dataframe.map_partitions` with a pandas-level `map` or `np.where` operation.

The function must not call `.compute()`.

**Error handling:**
- If `product` column is absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `numpy`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Row with `product = "O"`. Assert `unit = "BBL"`.
- `@pytest.mark.unit` — Row with `product = "G"`. Assert `unit = "MCF"`.
- `@pytest.mark.unit` — Row with `product = "W"` (unexpected). Assert `unit = "UNKNOWN"`.
- `@pytest.mark.unit` — Assert the `unit` column is present in the output and has `str` (object) dtype.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 18: Implement deduplication

**Module:** `kgs_pipeline/transform.py`
**Function:** `deduplicate_records(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Remove duplicate rows from the well-level Dask DataFrame. The natural key for uniqueness is `[well_id, production_date, product]`. Where duplicates exist, retain the last occurrence (presumed to be the most recently corrected entry).

Processing steps:
1. Using `dask.dataframe.map_partitions`, apply a pandas-level `drop_duplicates(subset=["well_id", "production_date", "product"], keep="last")` within each partition.
2. After partition-level deduplication, perform a global shuffle-and-dedup step: `ddf.drop_duplicates(subset=["well_id", "production_date", "product"], keep="last")` at the Dask level.
3. Return the deduplicated lazy Dask DataFrame.

Log the ratio of rows before/after deduplication. Because pre-dedup count requires `.compute()` (expensive), use the partition count as a proxy and log it at `DEBUG` level only.

The function must not call `.compute()`.

**Error handling:**
- If any of `well_id`, `production_date`, or `product` columns are absent, raise `KeyError` naming the missing column(s).

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide 5 rows where 2 are exact duplicates on `[well_id, production_date, product]`. Assert the result has 4 rows after deduplication (last kept).
- `@pytest.mark.unit` — Provide rows with no duplicates. Assert row count is unchanged.
- `@pytest.mark.unit` — Provide rows that are duplicates only on 2 of the 3 key columns. Assert they are NOT deduplicated (all 3 keys must match).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Run deduplication twice on the same input. Assert the result is identical to running it once (idempotency).

**Technical test (row count reconciliation):**
- `@pytest.mark.integration` — After running the full transform pipeline on `data/interim/`, assert that the processed row count is ≤ the interim row count, never greater.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 19: Implement chronological sort and well-level repartitioning

**Module:** `kgs_pipeline/transform.py`
**Function:** `sort_and_repartition(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Sort all records chronologically within each well and repartition the Dask DataFrame by `well_id` so that all records for a given well are co-located in the same partition. This is a prerequisite for correct cumulative and rolling computations in the features step.

Processing steps:
1. Set the Dask index to `well_id` using `ddf.set_index("well_id", sorted=False, drop=False)`. This triggers a Dask shuffle, grouping all records for each well into the same partition.
2. Within each partition, sort by `["well_id", "production_date"]` in ascending order using `map_partitions` with `pandas.DataFrame.sort_values`.
3. Return the sorted, repartitioned lazy Dask DataFrame with `well_id` as the index.

The function must not call `.compute()`.

**Error handling:**
- If `well_id` or `production_date` columns are absent, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide 3 wells with unsorted dates. Assert that after `sort_and_repartition().compute()`, records for each well are in ascending `production_date` order.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert that the Dask index is `well_id` in the returned DataFrame.

**Sort stability test:**
- `@pytest.mark.integration` — After running the transform pipeline, load two Parquet partition files for the same well from `data/processed/`. Assert the last `production_date` in the first file is earlier than the first `production_date` in the second file (sort stability across partition boundary).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 20: Implement processed Parquet writer with schema enforcement

**Module:** `kgs_pipeline/transform.py`
**Function:** `write_processed_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path) -> Path`

**Description:**
Write the fully cleaned, well-level Dask DataFrame to Parquet format, partitioned by `well_id`, with schema enforcement via PyArrow.

Processing steps:
1. Ensure `output_dir` exists.
2. Define a `pyarrow.schema()` object that explicitly types every expected column (use `pa.string()` for string columns, `pa.float64()` for floats, `pa.int32()` for ints, `pa.timestamp("ns")` for `production_date`, `pa.bool_()` for `outlier_flag`). This schema is passed to `ddf.to_parquet()` via `schema=` to prevent schema drift across partitions.
3. Call `ddf.to_parquet(str(output_dir), engine="pyarrow", schema=enforced_schema, write_index=True, overwrite=True)`.
4. Log the output directory and the number of Parquet files written at `INFO` level.
5. Return `output_dir` as a `Path`.

**Error handling:**
- If `output_dir` cannot be created (permission error), let the `OSError` propagate.
- If the Dask DataFrame schema does not match the enforced PyArrow schema, the error raised by `to_parquet()` must propagate with a clear log message at `ERROR` level identifying the schema mismatch.

**Dependencies:** `dask.dataframe`, `pyarrow`, `pathlib.Path`, `logging`

**Test cases:**

- `@pytest.mark.unit` — Provide a minimal valid Dask DataFrame with the expected columns. Call `write_processed_parquet(ddf, tmp_path)`. Assert the directory contains at least one `.parquet` file.
- `@pytest.mark.unit` — Assert every `.parquet` file written to `tmp_path` is readable by `pandas.read_parquet()` without error.
- `@pytest.mark.unit` — Assert the function returns a `Path` object equal to `output_dir`.

**Parquet readability test:**
- `@pytest.mark.integration` — For every Parquet file in `data/processed/`, assert it can be read by `pandas.read_parquet()` without raising.

**Partition correctness test:**
- `@pytest.mark.integration` — Read each Parquet partition file from `data/processed/` individually. Assert that each file contains rows for exactly one unique `well_id` value (no partition contains rows from multiple wells).

**Schema stability test:**
- `@pytest.mark.integration` — Sample the schema from any two Parquet files in `data/processed/` using `pyarrow.parquet.read_schema()`. Assert the two schemas are identical (same column names and dtypes).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 21: Implement transform pipeline orchestrator and domain integrity tests

**Module:** `kgs_pipeline/transform.py`
**Function:** `run_transform_pipeline() -> Path`

**Description:**
Synchronous entry-point that wires together the full transform workflow end-to-end:
1. Read config from `kgs_pipeline/config.py`.
2. Load the interim Parquet store from `INTERIM_DATA_DIR` using `dask.dataframe.read_parquet()`.
3. Call `rename_and_cast_columns(ddf)`.
4. Call `parse_production_date(ddf)`.
5. Call `explode_api_numbers(ddf)`.
6. Call `validate_physical_bounds(ddf)`.
7. Call `assign_unit_labels(ddf)`.
8. Call `deduplicate_records(ddf)`.
9. Call `sort_and_repartition(ddf)`.
10. Call `write_processed_parquet(ddf, PROCESSED_DATA_DIR)`.
11. Return `PROCESSED_DATA_DIR`.

Log pipeline start, each step name, and completion with elapsed time at `INFO` level.

**Error handling:**
- If `INTERIM_DATA_DIR` contains no Parquet files, raise `RuntimeError`.
- Any exception in steps 3–9 must propagate with full traceback.

**Dependencies:** `dask.dataframe`, `pathlib.Path`, `logging`, `time`

**Test cases (domain integrity — run against real or realistic processed data):**

- `@pytest.mark.unit` — Patch all sub-functions. Assert `run_transform_pipeline()` calls each step exactly once in the correct order.
- `@pytest.mark.unit` — Patch `dask.dataframe.read_parquet` to simulate empty interim store. Assert `RuntimeError` is raised.

**Domain-specific integrity tests (against processed output in `data/processed/`):**

- `@pytest.mark.integration` — **Physical bounds**: Load processed Parquet. Assert no `production` value is negative anywhere in the dataset.
- `@pytest.mark.integration` — **Zero vs. null distinction**: Identify rows in the raw interim data where `production == 0.0`. Assert the corresponding rows in processed data also have `production == 0.0` (not `NaN`).
- `@pytest.mark.integration` — **Unit consistency**: Assert all rows with `product == "O"` have `unit == "BBL"` and all rows with `product == "G"` have `unit == "MCF"`.
- `@pytest.mark.integration` — **Well completeness**: For a sample of 20 randomly selected `well_id` values, compute the expected record count as `(last_production_date - first_production_date).months + 1` and assert the actual row count matches (allowing for zero-production months to exist, but no unexpected gaps). Flag any well with gaps > 3 consecutive months as a warning, not a test failure.
- `@pytest.mark.integration` — **Data integrity spot-check**: For 50 randomly selected `(well_id, production_date, product)` triples, look up the corresponding `production` value in the raw interim Parquet. Assert the processed value equals the raw value (or `NaN` if the raw value was negative or missing).
- `@pytest.mark.integration` — **Lazy evaluation**: Load `data/interim/` and call each transform function in sequence. After each function call (before `write_processed_parquet`), assert the return type is `dask.dataframe.DataFrame`, confirming no premature `.compute()` call.

**Definition of done:** Function and all integration tests are implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
