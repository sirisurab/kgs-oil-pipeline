# Task Specification: Tests Component

**Test files:**
- `kgs/tests/conftest.py` — shared fixtures and pytest configuration
- `kgs/tests/test_acquisition.py` — acquisition component tests
- `kgs/tests/test_ingestion.py` — ingestion component tests
- `kgs/tests/test_cleaning.py` — cleaning component tests
- `kgs/tests/test_processing.py` — processing component tests
- `kgs/tests/test_pipeline.py` — pipeline orchestration tests

**Component purpose:** Provide a complete pytest test suite covering all pipeline
components with unit tests (mocked I/O, no disk or network access) and integration
tests (real data files on disk, optionally real network). Tests enforce domain
physical constraints, data integrity, schema stability, and lazy Dask evaluation
as specified in the project test requirements.

---

## Overview

The test suite uses two pytest markers:
- `@pytest.mark.unit` — no network access, no files at `data/raw`,
  `data/processed`, or `data/interim` required. Always fast.
- `@pytest.mark.integration` — requires actual data files or network access.
  Skipped in CI unless `--run-integration` flag is passed.

All fixtures that create temporary files use `pytest`'s built-in `tmp_path`
fixture. All HTTP calls in unit tests are mocked using `pytest-mock` or
`unittest.mock`.

---

## Task 01: Implement shared test fixtures (conftest.py)

**File:** `kgs/tests/conftest.py`

**Description:**
Define all shared fixtures consumed by two or more test files.

**Fixtures to implement:**

`minimal_raw_csv_rows() -> list[dict]`
- Returns a list of 10 dicts representing raw KGS CSV rows with all canonical
  columns present. Includes: 2 leases, 3 months of oil and gas production each,
  1 zero-production oil row, 1 negative-production row (for validation tests),
  1 row with `MONTH-YEAR = "0/2022"` (yearly summary), 1 row with a null operator.
- Values must be realistic: lease_kid like `"1001135839"`, production in BBL
  range 10–5000, county = `"ELLIS"`, operator = `"SOME OIL CO"`.

`raw_csv_file(tmp_path, minimal_raw_csv_rows) -> Path`
- Writes `minimal_raw_csv_rows` as a CSV file named `oil2022.csv` in `tmp_path`.
- Returns the Path to the written file.

`raw_dask_df(minimal_raw_csv_rows) -> dask.dataframe.DataFrame`
- Creates a pandas DataFrame from `minimal_raw_csv_rows`, renames `MONTH-YEAR`
  to `MONTH_YEAR`, adds `source_year=2022`, then wraps in a Dask DataFrame
  with `npartitions=2`.

`cleaned_dask_df(raw_dask_df) -> dask.dataframe.DataFrame`
- Runs the full `clean(raw_dask_df, CleaningConfig())` pipeline and returns the
  result. This fixture is used by processing tests.

`minimal_pipeline_config_yaml(tmp_path) -> Path`
- Writes a minimal valid `pipeline_config.yaml` to `tmp_path` and returns the path.
- Must contain all required sections with valid values including
  `years: [2022]` and `output_dir` pointing to `tmp_path / "raw"`.

`acquisition_config(tmp_path) -> AcquisitionConfig`
- Returns an `AcquisitionConfig` with `years=[2022]`, `output_dir=str(tmp_path)`,
  `max_concurrency=2`, `max_retries=2`.

`ingestion_config(tmp_path) -> IngestionConfig`
- Returns an `IngestionConfig` with `raw_dir=str(tmp_path)`, `years=[2022]`.

`cleaning_config() -> CleaningConfig`
- Returns a `CleaningConfig` with default values.

`processing_config(tmp_path) -> ProcessingConfig`
- Returns a `ProcessingConfig` with `processed_dir=str(tmp_path / "processed")`,
  `interim_dir=str(tmp_path / "interim")`.

**pytest.ini / conftest markers registration:**
- Register markers `unit` and `integration` in `conftest.py` using
  `pytest.ini_options` or `config.addinivalue_line`.
- Add a `--run-integration` CLI flag; integration tests are skipped unless
  this flag is present.

**Dependencies:** pytest, dask, pandas, pathlib

**Test cases (self-tests of the fixtures):**
- `@pytest.mark.unit` Assert `raw_csv_file` creates a file with the `.csv` extension.
- `@pytest.mark.unit` Assert `raw_dask_df` returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` Assert `cleaned_dask_df` returns a `dask.dataframe.DataFrame`
  with `prod_date` column present.

**Definition of done:** All fixtures are implemented, conftest loads without error,
ruff and mypy report no errors, requirements.txt updated with all third-party packages
imported in this task.

---

## Task 02: Implement acquisition tests (test_acquisition.py)

**File:** `kgs/tests/test_acquisition.py`

**Description:**
Full test coverage for `kgs/src/acquisition.py`.

**Tests to implement:**

`@pytest.mark.unit test_resolve_download_urls_returns_correct_count`
- Assert `resolve_download_urls` returns one dict per year in `config.years`.

`@pytest.mark.unit test_resolve_download_urls_url_format`
- Assert each dict's `url` matches the pattern
  `https://www.kgs.ku.edu/PRS/petro/productn/oil{year}.csv`.

`@pytest.mark.unit test_resolve_download_urls_empty_years_raises`
- Assert `ValueError` when `years=[]`.

`@pytest.mark.unit test_resolve_download_urls_invalid_year_raises`
- Assert `ValueError` when any year is outside 2000–2030.

`@pytest.mark.unit test_download_file_success`
- Mock `aiohttp.ClientSession.get` to return HTTP 200 with 500-byte content.
- Assert result dict has `success=True` and `bytes_downloaded=500`.

`@pytest.mark.unit test_download_file_all_retries_exhausted`
- Mock `aiohttp.ClientSession.get` to raise `aiohttp.ClientConnectionError`
  on every attempt.
- Assert result dict has `success=False` and `error` is a non-empty string.

`@pytest.mark.unit test_download_file_retry_then_succeed`
- Mock to fail on attempt 1, succeed on attempt 2.
- Assert `success=True`.

`@pytest.mark.unit test_download_file_no_partial_file_on_failure`
- Assert no file is left on disk at `local_path` after all retries fail.

`@pytest.mark.unit test_compute_checksum_known_value`
- Write known bytes to a temp file; assert `compute_checksum` returns
  the expected MD5 hex digest (pre-computed for the test data).

`@pytest.mark.unit test_verify_file_integrity_nonexistent`
- Assert `verify_file_integrity` returns `exists=False` for a non-existent path.

`@pytest.mark.unit test_verify_file_integrity_valid_csv`
- Write a small valid CSV; assert `is_valid_text=True` and `size_bytes > 0`.

`@pytest.mark.unit test_verify_file_integrity_binary`
- Write bytes with invalid UTF-8; assert `is_valid_text=False`.

`@pytest.mark.unit test_acquire_returns_all_years`
- Mock HTTP; assert `acquire` returns a list with length == `len(config.years)`.

`@pytest.mark.unit test_acquire_concurrency_limit`
- Use a counter-based mock to track simultaneous active downloads; assert
  the counter never exceeds `config.max_concurrency`.

`@pytest.mark.unit test_acquire_partial_failure_continues`
- One file fails, others succeed; assert successful files have `success=True`.

`@pytest.mark.integration test_acquire_live_single_year`
- Download `oil2020.csv` from the live KGS URL; assert file exists on disk
  and `is_valid_text=True`.

**Definition of done:** All tests implemented, all unit tests pass without network
access, ruff and mypy report no errors, requirements.txt updated with all third-party
packages imported in this task.

---

## Task 03: Implement ingestion tests (test_ingestion.py)

**File:** `kgs/tests/test_ingestion.py`

**Description:**
Full test coverage for `kgs/src/ingestion.py`.

**Tests to implement:**

`@pytest.mark.unit test_discover_raw_files_finds_expected_files`
- Create two CSV files in tmp_path; assert 2 dicts returned with `exists=True`.

`@pytest.mark.unit test_discover_raw_files_missing_year_flagged`
- Only `oil2020.csv` present; request years `[2020, 2022]`; assert 2022 entry
  has `exists=False`.

`@pytest.mark.unit test_discover_raw_files_nonexistent_dir_raises`
- Assert `ValueError` for a non-existent `raw_dir`.

`@pytest.mark.unit test_discover_raw_files_auto_discover`
- With `years=[]` and 3 CSV files, assert all 3 are auto-discovered.

`@pytest.mark.unit test_read_and_normalise_full_schema`
- Write a CSV with all canonical columns; assert all columns present in output,
  `source_year` correct.

`@pytest.mark.unit test_read_and_normalise_missing_url_column`
- CSV without `URL`; assert `URL` column in output is all `pandas.NA`.

`@pytest.mark.unit test_read_and_normalise_month_year_hyphen_renamed`
- CSV with `MONTH-YEAR` header; assert output column is `MONTH_YEAR`.

`@pytest.mark.unit test_read_and_normalise_empty_csv_returns_empty_df`
- Header-only CSV; assert empty DataFrame returned and WARNING logged.

`@pytest.mark.unit test_read_and_normalise_corrupt_file_raises`
- Corrupt file; assert `IngestionError` raised.

`@pytest.mark.unit test_read_and_normalise_dtype_is_string`
- Assert all canonical columns (except `source_year`) have string dtype.

`@pytest.mark.unit test_load_raw_files_parallel_returns_dask_df`
- Two valid CSV temp files; assert result is `dask.dataframe.DataFrame`.

`@pytest.mark.unit test_load_raw_files_parallel_not_computed`
- Assert return type is `dask.dataframe.DataFrame`, not `pandas.DataFrame`.

`@pytest.mark.unit test_load_raw_files_parallel_one_failure_excluded`
- One file raises `IngestionError`; assert the other file is still loaded.

`@pytest.mark.unit test_load_raw_files_parallel_all_fail_raises`
- All files raise `IngestionError`; assert `IngestionError` is raised.

`@pytest.mark.unit test_load_raw_files_partition_count`
- Assert partition count equals number of successfully read files.

`@pytest.mark.unit test_ingest_returns_dask_df`
- Mocked sub-functions; assert `ingest` returns `dask.dataframe.DataFrame`.

`@pytest.mark.unit test_ingest_no_compute`
- Assert `ingest` does not call `.compute()` internally (return type check).

`@pytest.mark.integration test_ingest_real_files`
- With actual files in `data/raw`; assert result has `source_year` column.

**Definition of done:** All tests implemented, all unit tests pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 04: Implement cleaning tests (test_cleaning.py)

**File:** `kgs/tests/test_cleaning.py`

**Description:**
Full test coverage for `kgs/src/cleaning.py`, with emphasis on domain physical
constraints.

**Tests to implement:**

`@pytest.mark.unit test_standardise_column_names_renames_correctly`
- UPPER_CASE input; assert snake_case output column names.

`@pytest.mark.unit test_standardise_column_names_returns_dask_df`
- Assert return type is `dask.dataframe.DataFrame`.

`@pytest.mark.unit test_standardise_column_names_no_canonical_columns_raises`
- Input with no canonical columns; assert `CleaningError` raised.

`@pytest.mark.unit test_standardise_column_names_extra_columns_passed_through`
- Extra columns retained unchanged.

`@pytest.mark.unit test_coerce_dtypes_production_numeric`
- `"100.5"`, `"0"`, `""`, `"N/A"` → `100.5`, `0.0`, `NaN`, `NaN`.

`@pytest.mark.unit test_coerce_dtypes_zero_preserved`
- `"0"` in production → `0.0`, not NaN.

`@pytest.mark.unit test_coerce_dtypes_wells_nullable_int`
- `wells` column is `Int64` dtype.

`@pytest.mark.unit test_coerce_dtypes_returns_dask_df`
- Assert return type is `dask.dataframe.DataFrame`.

`@pytest.mark.unit test_parse_month_year_valid`
- `"3/2022"` → `prod_month=3`, `prod_year=2022`, `prod_date=datetime(2022, 3, 1)`.

`@pytest.mark.unit test_parse_month_year_drops_summary_rows`
- `"0/2022"` with `drop_summary_rows=True` → row removed.

`@pytest.mark.unit test_parse_month_year_retains_summary_rows`
- `"0/2022"` with `drop_summary_rows=False` → row retained, `prod_date=NaT`.

`@pytest.mark.unit test_parse_month_year_malformed_no_raise`
- `"bad/data"` → no exception, `prod_date=NaT`.

`@pytest.mark.unit test_handle_nulls_empty_string_to_na`
- `operator = ""` → `pandas.NA`.

`@pytest.mark.unit test_handle_nulls_na_token_to_na`
- `operator = "N/A"` → `pandas.NA`.

`@pytest.mark.unit test_handle_nulls_zero_production_preserved`
- `production = 0.0` → remains `0.0`.

`@pytest.mark.unit test_validate_physical_negative_production_removed`
- Row with `production = -5.0` is dropped.

`@pytest.mark.unit test_validate_physical_outlier_flagged`
- Oil row with `production = 100_000.0` and `flag_outliers=True` → retained
  with `is_outlier=True`.

`@pytest.mark.unit test_validate_physical_outlier_dropped`
- Oil row with `production = 100_000.0` and `flag_outliers=False` → dropped.

`@pytest.mark.unit test_validate_physical_zero_retained`
- `production = 0.0` → retained.

`@pytest.mark.unit test_validate_physical_invalid_product_removed`
- `product = "X"` → dropped.

`@pytest.mark.unit test_validate_physical_invalid_month_removed`
- `prod_month = 13` → dropped.

`@pytest.mark.unit test_remove_duplicates_removes_dups`
- 5 rows, 2 duplicates on dedup_subset → 3 rows in output.

`@pytest.mark.unit test_remove_duplicates_idempotent`
- Running twice produces same row count as running once.

`@pytest.mark.unit test_remove_duplicates_invalid_subset_raises`
- Non-existent column in subset → `CleaningError`.

`@pytest.mark.unit test_remove_duplicates_count_not_greater`
- Post-dedup row count ≤ input row count.

`@pytest.mark.unit test_clean_returns_dask_df`
- Full `clean()` returns `dask.dataframe.DataFrame`.

`@pytest.mark.unit test_clean_expected_columns_present`
- Assert all expected output columns are in `.columns`.

`@pytest.mark.unit test_clean_no_compute`
- Return type is `dask.dataframe.DataFrame`, not `pandas.DataFrame`.

`@pytest.mark.integration test_clean_no_negative_production`
- Trigger `.compute()` on a partition; assert no negative `production` values.

`@pytest.mark.integration test_clean_no_invalid_product_codes`
- Assert all `product` values are in `{"O", "G"}` after compute.

`@pytest.mark.integration test_clean_no_nat_prod_date_for_monthly_rows`
- Monthly rows only (summary rows dropped); assert `prod_date` is never NaT.

**Definition of done:** All tests implemented, all unit tests pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 05: Implement processing tests (test_processing.py)

**File:** `kgs/tests/test_processing.py`

**Description:**
Full test coverage for `kgs/src/processing.py`, with emphasis on ML-readiness,
cumulative production correctness, and Parquet output integrity.

**Tests to implement:**

`@pytest.mark.unit test_pivot_oil_gas_produces_wide_format`
- 2 oil + 2 gas rows for same lease/months → 2 wide rows.

`@pytest.mark.unit test_pivot_oil_gas_missing_gas_is_nan`
- Lease-month with oil only → `gas_mcf = NaN`.

`@pytest.mark.unit test_pivot_oil_gas_returns_dask_df`
- Assert return type.

`@pytest.mark.unit test_pivot_oil_gas_no_product_column`
- Assert `product` absent from output.

`@pytest.mark.unit test_cumulative_production_correct_values`
- Oil `[100, 200, 150]` → cum `[100, 300, 450]`.

`@pytest.mark.unit test_cumulative_production_nan_treated_as_zero`
- Month with NaN oil → cumulative does not decrease.

`@pytest.mark.unit test_cumulative_production_two_leases`
- Cumulative restarts per lease.

`@pytest.mark.unit test_cumulative_production_monotonic`
- After `.compute()`, assert `cum_oil_bbl` is non-decreasing within each lease.

`@pytest.mark.unit test_engineer_features_mom_change`
- `[100, 150, 120]` → `oil_mom_change = [NaN, 50, -30]`.

`@pytest.mark.unit test_engineer_features_gor_basic`
- `oil_bbl=200, gas_mcf=400` → `gor=2.0`.

`@pytest.mark.unit test_engineer_features_gor_zero_oil`
- `oil_bbl=0.0` → `gor=NaN`.

`@pytest.mark.unit test_engineer_features_gor_nonnegative`
- Assert `gor` is never negative after compute.

`@pytest.mark.unit test_normalise_and_encode_scaling`
- `oil_bbl [0, 50, 100]` → `oil_bbl_scaled [0.0, 0.5, 1.0]`.

`@pytest.mark.unit test_normalise_and_encode_constant_column`
- Constant column → `_scaled` is all `0.0` and WARNING logged.

`@pytest.mark.unit test_normalise_and_encode_categorical_consistency`
- Same operator string → same integer code.

`@pytest.mark.unit test_normalise_and_encode_original_retained`
- `oil_bbl` column still present after normalisation.

`@pytest.mark.unit test_build_aggregations_returns_dict`
- Returns dict with keys `"by_operator"`, `"by_county"`, `"by_field"`.

`@pytest.mark.unit test_build_aggregations_values_are_dask_df`
- All dict values are `dask.dataframe.DataFrame`.

`@pytest.mark.unit test_build_aggregations_disabled`
- `compute_aggregations=False` → returns empty dict.

`@pytest.mark.unit test_write_parquet_files_created`
- Write to tmp_path; assert non-empty list of Paths returned.

`@pytest.mark.unit test_write_parquet_readable`
- Written files are readable by `pandas.read_parquet`.

`@pytest.mark.unit test_write_parquet_single_lease_per_partition`
- Each Parquet file contains data for exactly one `lease_kid`.

`@pytest.mark.unit test_write_parquet_idempotent`
- Running twice → same row count as once (not doubled).

`@pytest.mark.unit test_process_returns_paths`
- Mocked sub-functions; assert `process` returns list of Paths.

`@pytest.mark.unit test_process_interim_checkpoint_created`
- Assert interim dir is non-empty after process.

`@pytest.mark.integration test_process_parquet_readable`
- Real data; assert `data/processed/` Parquet files readable by Dask.

`@pytest.mark.integration test_process_schema_consistency`
- Schema from one well's Parquet file matches schema from another.

`@pytest.mark.integration test_process_cumulative_monotonic`
- After reading processed Parquet, assert `cum_oil_bbl` is non-decreasing per lease.

`@pytest.mark.integration test_process_no_negative_production`
- Assert no negative `oil_bbl` or `gas_mcf` in processed output.

`@pytest.mark.integration test_process_well_completeness`
- For a sampled lease, assert month count matches span from first to last date.

`@pytest.mark.integration test_process_zero_production_preserved`
- Assert zeros in the raw data remain as `0.0` in processed output (not null).

**Definition of done:** All tests implemented, all unit tests pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Task 06: Implement pipeline orchestration tests (test_pipeline.py)

**File:** `kgs/tests/test_pipeline.py`

**Description:**
Full test coverage for `kgs/src/pipeline.py` including config loading, logging,
stage runner, full pipeline runner, and CLI.

**Tests to implement:**

`@pytest.mark.unit test_load_config_valid_yaml`
- Valid YAML file; assert correct field values in returned `PipelineConfig`.

`@pytest.mark.unit test_load_config_defaults_applied`
- Omit optional key; assert default value used.

`@pytest.mark.unit test_load_config_nonexistent_file_raises`
- `ConfigError` for missing YAML path.

`@pytest.mark.unit test_load_config_invalid_stage_name_raises`
- `enabled_stages: [unknown]` → `ConfigError`.

`@pytest.mark.unit test_load_config_invalid_years_type_raises`
- `years: "not_a_list"` → `ConfigError`.

`@pytest.mark.unit test_configure_logging_level`
- Level `"DEBUG"` → logger has `logging.DEBUG`.

`@pytest.mark.unit test_configure_logging_file_handler`
- `log_file` set → `FileHandler` attached.

`@pytest.mark.unit test_configure_logging_no_duplicate_handlers`
- Call twice → no duplicate handlers.

`@pytest.mark.unit test_configure_logging_invalid_level_raises`
- `"VERBOSE"` → `ConfigError`.

`@pytest.mark.unit test_run_stage_success`
- Stage returns `42`; assert `(True, 42)` returned.

`@pytest.mark.unit test_run_stage_exception_isolated`
- Stage raises `RuntimeError`; assert `(False, None)` returned, no propagation.

`@pytest.mark.unit test_run_stage_keyboard_interrupt_propagates`
- Stage raises `KeyboardInterrupt`; assert it propagates out of `run_stage`.

`@pytest.mark.unit test_run_stage_logs_timing`
- Assert INFO log contains elapsed time.

`@pytest.mark.unit test_run_pipeline_all_stages_succeed`
- All four stages mocked to succeed; assert summary `success=True`.

`@pytest.mark.unit test_run_pipeline_failed_stage_reflected`
- Acquire mocked to fail; assert summary `success=False`.

`@pytest.mark.unit test_run_pipeline_dry_run_no_calls`
- `dry_run=True`; assert no stage functions are called.

`@pytest.mark.unit test_run_pipeline_single_stage_only`
- `enabled_stages=["ingest"]`; assert only ingest is called.

`@pytest.mark.unit test_run_pipeline_summary_keys`
- Assert returned dict always has `success`, `stages`, `start_time`,
  `end_time`, `total_elapsed_seconds`.

`@pytest.mark.unit test_main_config_path_passed`
- Simulate `["--config", "path.yaml"]`; assert `load_config` called with path.

`@pytest.mark.unit test_main_dry_run_flag`
- `--dry-run` flag; assert `config.dry_run=True`.

`@pytest.mark.unit test_main_stage_override`
- `--stage ingest`; assert `enabled_stages=["ingest"]`.

`@pytest.mark.unit test_main_bad_config_exits_2`
- Bad config path; assert `SystemExit` with code 2.

**Definition of done:** All tests implemented, all unit tests pass, ruff and mypy
report no errors, requirements.txt updated with all third-party packages imported in
this task.

---

## Cross-cutting test requirements

The following tests enforce the project-wide domain-specific and technical test
requirements. Each test below must be implemented in the appropriate test file
(noted in parentheses).

**Physical bound validation** (`test_cleaning.py`, `test_processing.py`):
- `test_validate_physical_negative_production_removed` — production cannot be negative.
- `test_engineer_features_gor_nonnegative` — GOR must be ≥ 0.
- `test_process_no_negative_production` — end-to-end check.

**Unit consistency** (`test_cleaning.py`, `test_processing.py`):
- `test_pivot_oil_gas_produces_wide_format` — oil in `oil_bbl` (BBL), gas in `gas_mcf` (MCF).
- `test_normalise_and_encode_scaling` — bounds-check scaled values (0–1).

**Decline curve monotonicity** (`test_processing.py`):
- `test_cumulative_production_monotonic` — `cum_oil_bbl` is non-decreasing per lease.
- `test_process_cumulative_monotonic` — end-to-end integration check.

**Well completeness check** (`test_processing.py`):
- `test_process_well_completeness` — month count matches date span per lease.

**Zero production handling** (`test_cleaning.py`, `test_processing.py`):
- `test_coerce_dtypes_zero_preserved` — `"0"` → `0.0`, not NaN.
- `test_handle_nulls_zero_production_preserved` — `0.0` not replaced with NA.
- `test_process_zero_production_preserved` — zeros survive end-to-end.

**Data integrity** (`test_ingestion.py`, `test_cleaning.py`):
- `test_read_and_normalise_full_schema` — spot-check column values match input.
- `test_remove_duplicates_count_not_greater` — dedup row count ≤ input.

**Data cleaning validation** (`test_cleaning.py`):
- `test_coerce_dtypes_wells_nullable_int` — correct dtype for wells.
- `test_handle_nulls_empty_string_to_na` — nulls normalised correctly.
- `test_validate_physical_invalid_product_removed` — bad product codes removed.

**Partition correctness** (`test_processing.py`):
- `test_write_parquet_single_lease_per_partition` — one lease per Parquet file.

**Schema stability across leases** (`test_processing.py`):
- `test_process_schema_consistency` — schema identical across all Parquet files.

**Row count reconciliation** (`test_cleaning.py`):
- `test_remove_duplicates_count_not_greater` — processed ≤ raw row count.
- `test_remove_duplicates_idempotent` — dedup is idempotent.

**Sort stability** (`test_processing.py`):
- `test_cumulative_production_monotonic` — sort by `(lease_kid, prod_date)` verified.

**Lazy Dask evaluation** (`test_ingestion.py`, `test_cleaning.py`, `test_processing.py`):
- `test_load_raw_files_parallel_not_computed` — ingest returns Dask DF.
- `test_clean_no_compute` — clean returns Dask DF.
- `test_pivot_oil_gas_returns_dask_df` — pivot returns Dask DF.
- `test_build_aggregations_values_are_dask_df` — aggregations are lazy.

**Parquet readability** (`test_processing.py`):
- `test_write_parquet_readable` — written Parquet files readable by pandas.
- `test_process_parquet_readable` — integration check with Dask reader.

---

## Design decisions and constraints

- All unit tests must run in under 30 seconds total on a developer machine without
  network access or data files.
- Integration tests are gated behind the `--run-integration` pytest flag and may
  be slow (network download + full pipeline run).
- Fixtures that return Dask DataFrames must use `npartitions=2` minimum to catch
  partition-boundary bugs that would be invisible with a single partition.
- HTTP mocking in acquisition tests must use `unittest.mock.AsyncMock` for async
  coroutines (`aiohttp` session methods are coroutines).
- All test files must import only from `kgs.src.*` — no direct imports from
  internal private functions (test the public API).
- Test data in `conftest.py` must include edge cases: a lease with only one month
  of data, a lease with a zero-production month, and a lease that appears in
  two different source years (to test deduplication).
