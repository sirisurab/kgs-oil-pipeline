# Transform Component — Task Specifications

## Overview

The transform component is responsible for:
1. Loading the interim Parquet dataset from `data/interim/` via Dask.
2. Parsing `MONTH_YEAR` strings (`M-YYYY` format) into proper `datetime64[ns]` values in a `production_date` column.
3. Renaming all columns to `snake_case` and casting to their correct final dtypes.
4. Exploding the comma-separated `api_number` column into one row per well, creating a `well_id` column.
5. Validating physical production bounds: negative production is coerced to `NaN`; single-well monthly oil volumes above `CONFIG.oil_max_bbl_per_month` are flagged in an `outlier_flag` boolean column (not dropped).
6. Deduplicating records on `[well_id, production_date, product]`.
7. Preserving the zero-vs-null distinction: zeros in raw data remain `0.0`; missing data remains `NaN`.
8. Adding a `unit` column (`BBL` for oil, `MCF` for gas).
9. Sorting each well's records chronologically; repartitioning by `well_id`.
10. Writing cleaned Parquet to `data/processed/`, partitioned by `well_id`, with a fixed `pyarrow.schema()` to prevent drift.
11. Generating a cleaning report (JSON) summarising rows dropped, nulls filled, outliers flagged, and duplicates removed.

**Source module:** `kgs_pipeline/transform.py`
**Test file:** `tests/test_transform.py`

---

## Design Decisions & Constraints

- Python 3.11+. All functions must carry full type hints. Module must have a module-level docstring.
- Use `dask.dataframe` throughout. Functions must return `dask.dataframe.DataFrame` unless otherwise stated. Never call `.compute()` in helper functions — only in `write_processed_parquet()`.
- `PRODUCT` column distinguishes oil (`O`) and gas (`G`). After exploding by `well_id`, each row represents a single product type for a single well for a single month.
- `api_number` (from `API_NUMBER`) is a comma-separated string like `"1500112345, 1500167890"`. Each entry becomes its own row. After explosion, `well_id` holds a single trimmed API number string. Rows where `api_number` is null or empty are assigned `well_id = "UNKNOWN_<LEASE_KID>"` to preserve the record rather than dropping it.
- Physical bounds: a `PRODUCTION` value of `-5.0` is almost certainly a data entry error; set it to `NaN`. A value of `0.0` is a valid shut-in measurement; leave it as `0.0`.
- The `outlier_flag` column is `True` when a single-well monthly oil production exceeds `CONFIG.oil_max_bbl_per_month` (50,000 BBL). It is always written; it does not drop the row.
- Deduplication key: `[well_id, production_date, product]`. On duplicate, retain the row with the larger `PRODUCTION` value (prefer non-null data over null).
- Partitioning by `well_id` ensures all records for a well are co-located, which is necessary for the correct per-well cumulative and rolling calculations in the features stage.
- Use `CONFIG` from `kgs_pipeline/config.py` and `setup_logging("transform", CONFIG.logs_dir)` from `kgs_pipeline/utils.py`.
- Enforce a fixed Parquet output schema using `pyarrow.schema()` at write time.

### Expected processed Parquet schema

| Column | dtype | Notes |
|--------|-------|-------|
| `well_id` | `string` | Single trimmed API number, or `UNKNOWN_<LEASE_KID>` |
| `lease_kid` | `int64` | KGS lease unique ID |
| `lease` | `string` | Lease name |
| `dor_code` | `string` | KS DOR ID |
| `field` | `string` | Field name |
| `producing_zone` | `string` | Producing formation |
| `operator` | `string` | Operator name |
| `county` | `string` | County name |
| `township` | `string` | PLSS township |
| `twn_dir` | `string` | Township direction |
| `range_` | `string` | PLSS range (renamed to avoid Python keyword) |
| `range_dir` | `string` | Range direction |
| `section` | `string` | PLSS section |
| `spot` | `string` | Legal quarter description |
| `latitude` | `float64` | NAD 1927 latitude |
| `longitude` | `float64` | NAD 1927 longitude |
| `production_date` | `datetime64[ns]` | First day of the production month |
| `product` | `string` | `O` (oil) or `G` (gas) |
| `wells` | `Int64` | Number of contributing wells (nullable int) |
| `production` | `float64` | Production volume |
| `unit` | `string` | `BBL` (oil) or `MCF` (gas) |
| `outlier_flag` | `bool` | True if production > `oil_max_bbl_per_month` for oil rows |
| `source_file` | `string` | Source filename stem |

---

## Task 12: Implement interim Parquet reader

**Module:** `kgs_pipeline/transform.py`
**Function:** `load_interim_parquet(interim_dir: Path | None = None) -> dask.dataframe.DataFrame`

**Description:**
Load all Parquet files from `interim_dir` (default: `CONFIG.interim_dir`) using `dask.dataframe.read_parquet()`. Use the `pyarrow` engine. Return the lazy Dask DataFrame without calling `.compute()`. Log the number of Parquet files found and the inferred partition count at INFO level.

**Error handling:**
- Raise `FileNotFoundError` if `interim_dir` does not exist.
- Raise `RuntimeError("No Parquet files found in interim_dir; run ingest pipeline first.")` if the directory is empty of Parquet files.

**Dependencies:** dask.dataframe, pathlib, logging, kgs_pipeline.config

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Write a small fixture Parquet file to `tmp_path`; call `load_interim_parquet(tmp_path)` and assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Call with a non-existent directory; assert `FileNotFoundError` is raised.
- `@pytest.mark.unit` — Call with an empty directory (no Parquet files); assert `RuntimeError` is raised.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`, not `pandas.DataFrame` (lazy evaluation — no `.compute()` in this test).
- `@pytest.mark.integration` — Call with `CONFIG.interim_dir` populated by the ingest stage; assert return type and that `ddf.npartitions >= 1`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 13: Implement production date parser

**Module:** `kgs_pipeline/transform.py`
**Function:** `parse_production_date(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Convert the `MONTH_YEAR` column (format `M-YYYY` or `MM-YYYY`, e.g., `1-2020`, `12-2021`) into a `production_date` column of dtype `datetime64[ns]` representing the first day of that production month (day = 1).

Implementation:
- Use `dask.dataframe.to_datetime` or `map_partitions` with `pandas.to_datetime` on a constructed `"YYYY-MM-01"` string.
- Split `MONTH_YEAR` on `"-"` to extract month and year components.
- Construct the ISO date string `"<year>-<month_zero_padded>-01"`.
- Rows where the parse fails become `NaT` (log count at WARNING). Do not drop them here — nulls are handled in the validation step.
- Drop the original `MONTH_YEAR` column after creating `production_date`.
- Return the lazy Dask DataFrame.

**Error handling:**
- Raise `KeyError("MONTH_YEAR column not found")` if the column is absent.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, pandas, logging

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture with `MONTH_YEAR = "1-2020"`; after `parse_production_date` and `.compute()`, assert `production_date` is `Timestamp("2020-01-01")`.
- `@pytest.mark.unit` — Build a fixture with `MONTH_YEAR = "12-2021"`; assert `production_date` is `Timestamp("2021-12-01")`.
- `@pytest.mark.unit` — Build a fixture with an invalid `MONTH_YEAR` value; assert the corresponding row has `production_date = NaT` (not an exception).
- `@pytest.mark.unit` — Assert `MONTH_YEAR` column is absent from the output (it must be dropped).
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 14: Implement column renaming and snake_case normalization

**Module:** `kgs_pipeline/transform.py`
**Function:** `normalize_column_names(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Rename all DataFrame columns to `snake_case` following the mapping in the processed schema table above. Specifically:
- Replace spaces with underscores.
- Convert to lowercase.
- Rename `RANGE` to `range_` (to avoid collision with the Python built-in).
- Apply the full column renaming map via `ddf.rename(columns=COLUMN_RENAME_MAP)` where `COLUMN_RENAME_MAP` is a module-level constant dict in `transform.py`.
- Return the renamed lazy Dask DataFrame.

**Error handling:**
- Log a WARNING for any expected column that is absent from the DataFrame (do not raise — partial rename is acceptable).
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, logging

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture with columns `["LEASE_KID", "MONTH_YEAR", "PRODUCT", "PRODUCTION", "API_NUMBER"]`; after `normalize_column_names`, assert `ddf.columns` contains `["lease_kid", "month_year", "product", "production", "api_number"]` (no uppercase letters).
- `@pytest.mark.unit` — Assert `range_` is present if `RANGE` was in the input (Python keyword avoidance).
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Call with a DataFrame that has an unexpected extra column; assert no exception is raised and the extra column is preserved with its original name.

**Definition of done:** Function is implemented, `COLUMN_RENAME_MAP` constant is defined at module level, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 15: Implement API number explosion to well_id rows

**Module:** `kgs_pipeline/transform.py`
**Function:** `explode_api_numbers(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
The `api_number` column contains comma-separated API number strings (e.g., `"1500112345, 1500167890"`). Transform the DataFrame so that each row corresponds to exactly one well:

1. Split `api_number` on `","` to produce a list of trimmed strings per row.
2. Explode the list column so each trimmed API number becomes its own row (use `map_partitions` with pandas `.explode()`).
3. Strip whitespace from each resulting `api_number` value.
4. Rename the exploded column to `well_id`.
5. For rows where the original `api_number` was null, empty, or contained only whitespace, assign `well_id = f"UNKNOWN_{lease_kid}"` where `lease_kid` is the value in the row's `lease_kid` column.
6. Return the expanded lazy Dask DataFrame.

**Constraint:** After explosion, every row must have a non-null, non-empty `well_id`. The original `api_number` column must be dropped from the output.

**Dependencies:** dask.dataframe, pandas, logging

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture with one row where `api_number = "1500112345, 1500167890"` and `lease_kid = 999`; after `explode_api_numbers` and `.compute()`, assert the result has 2 rows with `well_id` values `"1500112345"` and `"1500167890"`.
- `@pytest.mark.unit` — Build a fixture with one row where `api_number = "1500112345"` (single value); assert the result has exactly 1 row.
- `@pytest.mark.unit` — Build a fixture with one row where `api_number` is null and `lease_kid = 42`; assert `well_id = "UNKNOWN_42"`.
- `@pytest.mark.unit` — Assert `api_number` column is absent from the output.
- `@pytest.mark.unit` — Assert all `well_id` values in the output are non-null and non-empty strings.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 16: Implement physical bounds validation

**Module:** `kgs_pipeline/transform.py`
**Function:** `validate_physical_bounds(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Apply domain-specific physical constraints to the production data:

1. **Negative production:** Set any `production` value < 0 to `NaN`. These are data entry errors (production cannot be negative — a physical law). Log the count of rows affected at WARNING level.
2. **Zero production:** Leave `production == 0.0` values unchanged. Zero is a valid measurement (shut-in well). Do not coerce to NaN.
3. **Oil outlier flagging:** Add an `outlier_flag` boolean column. Set `outlier_flag = True` for rows where `product == "O"` and `production > CONFIG.oil_max_bbl_per_month`. For all other rows, `outlier_flag = False`. Do NOT drop these rows.
4. **Unit assignment:** Add a `unit` column: `"BBL"` where `product == "O"`, `"MCF"` where `product == "G"`, and `NaN` for any other product value.

**Physical law notes (from domain expert):**
- Production volumes (oil, gas, water) cannot be negative — any negative value is a sensor or data entry error.
- A well reporting zero production for a month is a shut-in or idle well — this is a valid operational state, not missing data.
- Oil rates above 50,000 BBL/month for a single lease/well are physically plausible only for very high-volume wells; for Kansas (a low-to-moderate production state), this threshold flags likely unit conversion errors (e.g., annual production mistakenly entered as monthly).

**Error handling:**
- Raise `KeyError` if `production` or `product` column is absent.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, logging, kgs_pipeline.config

**Test cases (`tests/test_transform.py`):**

*Physical bound validation (domain):*
- `@pytest.mark.unit` — Build a fixture with `production = -5.0`; assert after `validate_physical_bounds` and `.compute()` the value is `NaN`.
- `@pytest.mark.unit` — Build a fixture with `production = 0.0`; assert the value remains `0.0` after `.compute()` (zero-production preservation test).
- `@pytest.mark.unit` — Build a fixture with `product = "O"` and `production = 60000.0`; assert `outlier_flag = True` in the output and the row is **not** dropped.
- `@pytest.mark.unit` — Build a fixture with `product = "O"` and `production = 100.0`; assert `outlier_flag = False`.
- `@pytest.mark.unit` — Build a fixture with `product = "G"` and `production = 999999.0`; assert `outlier_flag = False` (gas has no outlier threshold in this implementation).
- `@pytest.mark.unit` — Assert `unit = "BBL"` for oil rows and `unit = "MCF"` for gas rows.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 17: Implement deduplication

**Module:** `kgs_pipeline/transform.py`
**Function:** `deduplicate_records(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Remove duplicate records based on the key `[well_id, production_date, product]`. Where duplicates exist, retain the row with the higher `production` value (preferring actual data over null). Implementation:

1. Sort each partition by `production` descending (so the highest value comes first).
2. Drop duplicates on `[well_id, production_date, product]`, keeping the first (highest production) row per group.
3. Log the count of removed duplicate rows at INFO level.
4. Return the deduplicated lazy Dask DataFrame.

**Note:** Since Dask operates partition-by-partition, full cross-partition deduplication requires a shuffle. Use `ddf.drop_duplicates(subset=["well_id", "production_date", "product"], split_out=...)` with an appropriate `split_out` argument. Alternatively, use `map_partitions` with intra-partition dedup and document the limitation.

**Error handling:**
- Raise `KeyError` if any of `well_id`, `production_date`, or `product` columns are absent.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, logging

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture with 3 rows: two duplicates on `[well_id, production_date, product]` (one with `production=100.0`, one with `production=50.0`) and one unique row. After `deduplicate_records` and `.compute()`, assert 2 rows remain, and the retained duplicate has `production=100.0`.
- `@pytest.mark.unit` — Build a fixture with no duplicates; assert the row count is unchanged.
- `@pytest.mark.unit` — Build a fixture where a duplicate pair has one null `production`; assert the non-null row is retained.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Row count reconciliation test (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Run `deduplicate_records` twice on the same fixture (idempotency test); assert both runs produce the same row count and identical data.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 18: Implement per-well chronological sort and well_id repartition

**Module:** `kgs_pipeline/transform.py`
**Function:** `sort_and_repartition(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Sort the Dask DataFrame by `[well_id, production_date]` ascending and repartition by `well_id` so that all records for each well land in a single partition (or minimal partition set). Implementation:

1. Use `ddf.sort_values(["well_id", "production_date"])` to produce a sorted lazy DataFrame.
2. Repartition using `ddf.repartition(partition_size="64MB")` or an explicit partition count to produce balanced partitions. The resulting partition structure must group records by `well_id` as closely as possible.
3. Return the sorted, repartitioned lazy Dask DataFrame.

**Sort stability constraint:** The last row of one partition for a given `well_id` must have a `production_date` earlier than the first row of the same `well_id` in the next partition. This is the sort stability requirement.

**Error handling:**
- Raise `KeyError` if `well_id` or `production_date` columns are absent.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, logging

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture with 3 rows for the same `well_id` in random date order; after `sort_and_repartition` and `.compute()`, assert dates are in ascending order.
- `@pytest.mark.unit` — Build a fixture with 2 different `well_id` values; assert each `well_id`'s dates are internally ascending.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Sort stability test (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture that spans multiple Dask partitions; after `.compute()` reconstruct per-well sequences and assert dates are globally monotonically increasing per `well_id` (no cross-partition date inversions).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 19: Implement processed Parquet writer

**Module:** `kgs_pipeline/transform.py`
**Function:** `write_processed_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path | None = None) -> list[Path]`

**Description:**
Write the cleaned, typed, sorted Dask DataFrame to Parquet in `output_dir` (default: `CONFIG.processed_dir`), partitioned by `well_id`. Use `pyarrow` engine. Enforce the expected schema by constructing a `pyarrow.schema()` object and passing it to `ddf.to_parquet(schema=...)`. This is the **only** `.compute()` call in the transform component. Call `ensure_dir(output_dir)` before writing. Return a sorted list of all `.parquet` file paths written.

**Schema enforcement:** Build the `pyarrow.schema()` from the column dtypes in the processed schema table. This prevents schema drift when one well's partition has all-null values in a column (where type inference would guess wrong).

**Dependencies:** dask.dataframe, pyarrow, pathlib, logging, kgs_pipeline.config, kgs_pipeline.utils

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a small typed Dask DataFrame conforming to the processed schema; call `write_processed_parquet(ddf, tmp_path)`; assert at least one `.parquet` file is written and the function returns a non-empty list of `Path` objects.
- `@pytest.mark.unit` — Read back one written Parquet file with `pandas.read_parquet`; spot-check 5 columns and their dtypes match the processed schema.
- `@pytest.mark.unit` — Assert every returned path exists on disk and has size > 0.
- `@pytest.mark.unit` — Assert every written Parquet file is readable by `dask.dataframe.read_parquet` without raising.

**Partition correctness test (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture with rows for 3 different `well_id` values, partitioned by `well_id`; after writing and reading back, read each Parquet file individually and assert each contains data for exactly one `well_id`.

**Schema stability test (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Write two fixture DataFrames representing two wells (one where `latitude` is all-null, one where it is populated); read back each Parquet file and assert both have identical column names and dtypes.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 20: Implement cleaning report generator

**Module:** `kgs_pipeline/transform.py`
**Function:** `generate_cleaning_report(stats: dict, output_dir: Path | None = None) -> Path`

**Description:**
Write a JSON cleaning report file to `output_dir / "cleaning_report.json"` (default: `CONFIG.processed_dir`). The `stats` dict is built incrementally by the transform orchestrator and must include:

| Key | Type | Description |
|-----|------|-------------|
| `input_row_count` | `int` | Rows loaded from interim Parquet |
| `rows_after_filter` | `int` | After monthly filter (already applied in ingest) |
| `negative_production_set_nan` | `int` | Count of negative production values coerced to NaN |
| `outliers_flagged` | `int` | Count of rows with `outlier_flag = True` |
| `duplicates_removed` | `int` | Count of rows removed by deduplication |
| `null_production_date_dropped` | `int` | Count of rows dropped due to NaT `production_date` |
| `unknown_well_id_assigned` | `int` | Count of rows assigned `UNKNOWN_*` well_id |
| `output_row_count` | `int` | Rows written to processed Parquet |
| `output_parquet_files` | `int` | Count of Parquet files written |
| `pipeline_run_timestamp` | `str` | ISO 8601 timestamp of the transform run |

Call `ensure_dir(output_dir)` before writing. Return the `Path` to the written JSON file.

**Dependencies:** json, pathlib, datetime, kgs_pipeline.utils

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Call `generate_cleaning_report` with a complete `stats` dict and `tmp_path`; assert the returned path exists, ends with `cleaning_report.json`, and can be parsed as valid JSON.
- `@pytest.mark.unit` — Read the written JSON and assert all 10 required keys are present.
- `@pytest.mark.unit` — Assert the `pipeline_run_timestamp` value parses as a valid ISO 8601 datetime string.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 21: Implement transform orchestrator

**Module:** `kgs_pipeline/transform.py`
**Function:** `run_transform_pipeline(interim_dir: Path | None = None, output_dir: Path | None = None) -> list[Path]`

**Description:**
Orchestrate the full transform workflow in sequence:
1. `load_interim_parquet(interim_dir)` → lazy Dask DataFrame.
2. `parse_production_date(ddf)` → date-parsed lazy Dask DataFrame.
3. `normalize_column_names(ddf)` → renamed lazy Dask DataFrame.
4. `explode_api_numbers(ddf)` → exploded lazy Dask DataFrame.
5. `validate_physical_bounds(ddf)` → validated lazy Dask DataFrame.
6. `deduplicate_records(ddf)` → deduplicated lazy Dask DataFrame.
7. `sort_and_repartition(ddf)` → sorted lazy Dask DataFrame.
8. `write_processed_parquet(ddf, output_dir)` → written Parquet paths (triggers `.compute()`).
9. `generate_cleaning_report(stats, output_dir)` → cleaning report JSON path.

The orchestrator must accumulate statistics into the `stats` dict throughout the pipeline (e.g., count rows at key checkpoints by reading Parquet metadata after writing — do not call `.compute()` on the Dask DataFrame itself mid-pipeline to count rows). Return the list of written Parquet paths.

**Error handling:**
- If `load_interim_parquet` raises `RuntimeError`, propagate it.
- Log the start and end of each stage at INFO level with elapsed time (use the `timer` decorator from `utils.py`).

**Dependencies:** dask.dataframe, pathlib, logging, kgs_pipeline.config, kgs_pipeline.utils

**Test cases (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Run `run_transform_pipeline` with all stage functions patched to return small fixture DataFrames; assert the function returns a non-empty list and `generate_cleaning_report` is called exactly once.
- `@pytest.mark.unit` — Patch `load_interim_parquet` to raise `RuntimeError`; assert `run_transform_pipeline` propagates the error.
- `@pytest.mark.integration` — Run `run_transform_pipeline` against real `CONFIG.interim_dir` data; assert output Parquet files are written to `CONFIG.processed_dir` and `cleaning_report.json` exists.

**Data integrity spot-check test (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture with 5 known well records having specific `production` values; run the full pipeline on the fixture; read back the processed Parquet and assert the `production` values for at least 3 of the known wells match the input values (end-to-end data integrity check).

**Well completeness test (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture for one well with 12 monthly records (Jan–Dec 2022); run `parse_production_date` + `sort_and_repartition`; after `.compute()` assert the well has exactly 12 records spanning January to December without gaps.

**Zero production preservation test (`tests/test_transform.py`):**
- `@pytest.mark.unit` — Build a fixture where one well has `production = 0.0` for a given month and another well has a null `production` for that month; run the full transform pipeline on the fixture; assert the zero value is preserved as `0.0` and the null value remains `NaN` in the processed output.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
