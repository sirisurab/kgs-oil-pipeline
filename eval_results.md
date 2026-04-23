
## Eval Run at 2026-04-20 00:30:21

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/features.py:10: error: Skipping analyzing "pyarrow.parquet": module is installed, but missing library stubs or py.typed marker  [import-untyped]
/kgs_pipeline/features.py:10: note: See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
/kgs_pipeline/features.py:10: error: Skipping analyzing "pyarrow": module is installed, but missing library stubs or py.typed marker  [import-untyped]
/kgs_pipeline/acquire.py:206: note: By default the bodies of untyped functions are not checked, consider using --check-untyped-defs  [annotation-unchecked]
/kgs_pipeline/ingest.py:200: error: Incompatible types in assignment (expression has type "Categorical", target has type "Series[Any]")  [assignment]
/kgs_pipeline/ingest.py:210: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/ingest.py:210: note: Possible overload variants:
/kgs_pipeline/ingest.py:210: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/ingest.py:210: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/ingest.py:218: error: Incompatible types in assignment (expression has type "Categorical", target has type "Series[Any]")  [assignment]
/kgs_pipeline/ingest.py:227: error: Incompatible types in assignment (expression has type "BaseStringArray[None]", target has type "Series[Any]")  [assignment]
Found 6 errors in 2 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:

tests/test_acquire.py::test_load_lease_index_filters_year PASSED         [  0%]
tests/test_acquire.py::test_load_lease_index_deduplicates_url PASSED     [  1%]
tests/test_acquire.py::test_load_lease_index_file_not_found PASSED       [  2%]
tests/test_acquire.py::test_load_lease_index_missing_url_column PASSED   [  3%]
tests/test_acquire.py::test_extract_lease_id_basic PASSED                [  4%]
tests/test_acquire.py::test_extract_lease_id_no_param PASSED             [  5%]
tests/test_acquire.py::test_extract_lease_id_extra_params PASSED         [  6%]
tests/test_acquire.py::test_parse_download_link_absolute PASSED          [  7%]
tests/test_acquire.py::test_parse_download_link_no_match PASSED          [  8%]
tests/test_acquire.py::test_parse_download_link_relative PASSED          [  8%]
tests/test_acquire.py::test_download_lease_success PASSED                [  9%]
tests/test_acquire.py::test_download_lease_idempotent PASSED             [ 10%]
tests/test_acquire.py::test_download_lease_no_download_link 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:153 No download link for lease 99: No anon_blobber.download link found in HTML (base_url=https://chasm.kgs.ku.edu)
PASSED                                                                   [ 11%]
tests/test_acquire.py::test_download_lease_data_500 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:168 Data download returned 500 for lease 77
PASSED                                                                   [ 12%]
tests/test_acquire.py::test_download_lease_empty_content 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:173 Empty content for lease 88
PASSED                                                                   [ 13%]
tests/test_acquire.py::test_retry_success_after_one_failure 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 1/3 failed for flaky: fail — retrying in 0.0s
PASSED                                                                   [ 14%]
tests/test_acquire.py::test_retry_always_fails 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 1/3 failed for always_fail: nope — retrying in 0.0s
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 2/3 failed for always_fail: nope — retrying in 0.0s
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 3/3 failed for always_fail: nope — retrying in 0.0s
PASSED                                                                   [ 15%]
tests/test_acquire.py::test_retry_max_attempts_one 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 1/1 failed for boom: immediate — retrying in 0.0s
PASSED                                                                   [ 16%]
tests/test_acquire.py::test_validate_raw_files_valid PASSED              [ 16%]
tests/test_acquire.py::test_validate_raw_files_zero_byte PASSED          [ 17%]
tests/test_acquire.py::test_validate_raw_files_header_only PASSED        [ 18%]
tests/test_acquire.py::test_validate_raw_files_non_utf8 PASSED           [ 19%]
tests/test_acquire.py::test_validate_raw_files_dir_not_found PASSED      [ 20%]
tests/test_features.py::test_cum_production_values PASSED                [ 21%]
tests/test_features.py::test_cum_production_zero_at_start PASSED         [ 22%]
tests/test_features.py::test_cum_production_monotone PASSED              [ 23%]
tests/test_features.py::test_cum_production_missing_col PASSED           [ 24%]
tests/test_features.py::test_ratios_gor_undefined_zero_oil_nonzero_gas PASSED [ 25%]
tests/test_features.py::test_ratios_gor_zero_for_pos_oil_zero_gas PASSED [ 25%]
tests/test_features.py::test_ratios_gor_both_zero PASSED                 [ 26%]
tests/test_features.py::test_ratios_water_cut_zero_water PASSED          [ 27%]
tests/test_features.py::test_decline_rate_values PASSED                  [ 28%]
tests/test_features.py::test_decline_rate_clipped_low PASSED             [ 29%]
tests/test_features.py::test_decline_rate_zero_prev_is_nan PASSED        [ 30%]
tests/test_features.py::test_decline_rate_clipped_high PASSED            [ 31%]
tests/test_features.py::test_decline_rate_within_bounds_unchanged PASSED [ 32%]
tests/test_features.py::test_rolling_3m_values PASSED                    [ 33%]
tests/test_features.py::test_rolling_6m_all_values PASSED                [ 33%]
tests/test_features.py::test_rolling_partial_window PASSED               [ 34%]
tests/test_features.py::test_rolling_columns_present PASSED              [ 35%]
tests/test_features.py::test_lag_1m_value PASSED                         [ 36%]
tests/test_features.py::test_lag_1m_first_is_nan PASSED                  [ 37%]
tests/test_features.py::test_lag_3m_first_three_are_nan PASSED           [ 38%]
tests/test_features.py::test_lag_columns_present PASSED                  [ 39%]
tests/test_features.py::test_encode_product_code_present PASSED          [ 40%]
tests/test_features.py::test_encode_twn_dir_null_gives_minus1 PASSED     [ 41%]
tests/test_features.py::test_encode_originals_preserved PASSED           [ 41%]
tests/test_features.py::test_encode_range_dir_code_present PASSED        [ 42%]
tests/test_features.py::test_apply_feature_transforms_returns_dask PASSED [ 43%]
tests/test_features.py::test_apply_feature_transforms_all_derived_columns PASSED [ 44%]
tests/test_features.py::test_validate_features_schema_passes PASSED      [ 45%]
tests/test_features.py::test_validate_features_schema_missing_gor PASSED [ 46%]
tests/test_features.py::test_validate_features_schema_multiple_missing PASSED [ 47%]
tests/test_features.py::test_check_schema_stability_identical PASSED     [ 48%]
tests/test_features.py::test_check_schema_stability_extra_column PASSED  [ 49%]
tests/test_features.py::test_validate_correct_df_returns_empty PASSED    [ 50%]
tests/test_features.py::test_validate_negative_production PASSED         [ 50%]
tests/test_features.py::test_validate_decreasing_cum_production PASSED   [ 51%]
tests/test_features.py::test_validate_out_of_bounds_decline_rate PASSED  [ 52%]
tests/test_features.py::test_validate_water_cut_out_of_range PASSED      [ 53%]
tests/test_ingest.py::test_load_schema_returns_21_keys PASSED            [ 54%]
tests/test_ingest.py::test_load_schema_lease_kid_non_nullable_int64 PASSED [ 55%]
tests/test_ingest.py::test_load_schema_wells_nullable_int64 PASSED       [ 56%]
tests/test_ingest.py::test_load_schema_product_categories PASSED         [ 57%]
tests/test_ingest.py::test_load_schema_township_nullable_int64 PASSED    [ 58%]
tests/test_ingest.py::test_load_schema_file_not_found PASSED             [ 58%]
tests/test_ingest.py::test_read_raw_file_adds_source_file PASSED         [ 59%]
tests/test_ingest.py::test_read_raw_file_empty_bytes PASSED              [ 60%]
tests/test_ingest.py::test_read_raw_file_header_only PASSED              [ 61%]
tests/test_ingest.py::test_read_raw_file_not_found PASSED                [ 62%]
tests/test_ingest.py::test_filter_by_year_keeps_2024_onwards PASSED      [ 63%]
tests/test_ingest.py::test_filter_by_year_all_before_2024 PASSED         [ 64%]
tests/test_ingest.py::test_filter_by_year_no_non_numeric_tokens PASSED   [ 65%]
tests/test_ingest.py::test_enforce_schema_adds_nullable_column PASSED    [ 66%]
tests/test_ingest.py::test_enforce_schema_raises_for_non_nullable_absent PASSED [ 66%]
tests/test_ingest.py::test_enforce_schema_product_out_of_set_replaced PASSED [ 67%]
tests/test_ingest.py::test_enforce_schema_drops_extra_column FAILED      [ 68%]
tests/test_ingest.py::test_enforce_schema_twn_dir_categorical PASSED     [ 69%]
tests/test_ingest.py::test_ingest_file_year_filter PASSED                [ 70%]
tests/test_ingest.py::test_ingest_file_all_before_min_year PASSED        [ 71%]
tests/test_ingest.py::test_ingest_file_missing_non_nullable_raises PASSED [ 72%]
tests/test_ingest.py::test_write_interim_creates_parquet PASSED          [ 73%]
tests/test_ingest.py::test_validate_interim_schema_passes PASSED         [ 74%]
tests/test_ingest.py::test_validate_interim_schema_missing_column PASSED [ 75%]
tests/test_ingest.py::test_validate_interim_schema_missing_source_file PASSED [ 75%]
tests/test_transform.py::test_parse_production_date_basic PASSED         [ 76%]
tests/test_transform.py::test_parse_production_date_drops_month_zero PASSED [ 77%]
tests/test_transform.py::test_parse_production_date_no_month_year_col PASSED [ 78%]
tests/test_transform.py::test_parse_production_date_malformed_gives_nat FAILED [ 79%]
tests/test_transform.py::test_clean_keeps_zero_production PASSED         [ 80%]
tests/test_transform.py::test_clean_drops_negative_production 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:130 clean_invalid_values: dropping 1 rows with PRODUCTION < 0 (LEASE_KIDs: [1])
PASSED                                                                   [ 81%]
tests/test_transform.py::test_clean_drops_nat_production_date PASSED     [ 82%]
tests/test_transform.py::test_clean_drops_null_lease_kid PASSED          [ 83%]
tests/test_transform.py::test_clean_null_production_imputed_with_active_wells PASSED [ 83%]
tests/test_transform.py::test_validate_flags_high_oil 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:164 validate_production_units: LEASE_KID=1001 has OIL PRODUCTION=60000.0 > 50000 (likely unit error)
PASSED                                                                   [ 84%]
tests/test_transform.py::test_validate_no_warning_for_low_oil PASSED     [ 85%]
tests/test_transform.py::test_validate_no_flag_for_high_gas PASSED       [ 86%]
tests/test_transform.py::test_validate_returns_same_row_count 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:164 validate_production_units: LEASE_KID=1 has OIL PRODUCTION=60000.0 > 50000 (likely unit error)
PASSED                                                                   [ 87%]
tests/test_transform.py::test_deduplicate_removes_duplicates PASSED      [ 88%]
tests/test_transform.py::test_deduplicate_idempotent PASSED              [ 89%]
tests/test_transform.py::test_deduplicate_no_duplicates PASSED           [ 90%]
tests/test_transform.py::test_deduplicate_output_leq_input PASSED        [ 91%]
tests/test_transform.py::test_index_and_sort_returns_dask_df PASSED      [ 91%]
tests/test_transform.py::test_index_and_sort_production_date_sorted PASSED [ 92%]
tests/test_transform.py::test_index_and_sort_missing_lease_kid PASSED    [ 93%]
tests/test_transform.py::test_index_and_sort_missing_production_date PASSED [ 94%]
tests/test_transform.py::test_check_well_completeness_no_gap PASSED      [ 95%]
tests/test_transform.py::test_check_well_completeness_with_gap 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:256 check_well_completeness: gap > 31 days for ('1001', 'O') from 2024-02-01 to 2024-04-01
PASSED                                                                   [ 96%]
tests/test_transform.py::test_apply_partition_transforms_returns_dask_df PASSED [ 97%]
tests/test_transform.py::test_apply_partition_transforms_adds_production_date PASSED [ 98%]
tests/test_transform.py::test_write_transform_creates_parquet PASSED     [ 99%]
tests/test_transform.py::test_write_transform_readable_by_pandas PASSED  [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
____________________ test_enforce_schema_drops_extra_column ____________________
tests/test_ingest.py:177: in test_enforce_schema_drops_extra_column
    assert "URL" not in result.columns
E   AssertionError: assert 'URL' not in Index(['LEASE_KID', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD',\n       'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE',\n       'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR',\n       'PRODUCT', 'WELLS', 'PRODUCTION', 'URL'],\n      dtype='str')
E    +  where Index(['LEASE_KID', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD',\n       'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE',\n       'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR',\n       'PRODUCT', 'WELLS', 'PRODUCTION', 'URL'],\n      dtype='str') =    LEASE_KID LEASE  DOR_CODE  ... WELLS PRODUCTION               URL\n0          1  <NA>      <NA>  ...  <NA>       <NA>  http://extra.com\n\n[1 rows x 21 columns].columns
________________ test_parse_production_date_malformed_gives_nat ________________
tests/test_transform.py:58: in test_parse_production_date_malformed_gives_nat
    assert pd.isna(result["production_date"].iloc[0])
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexing.py:1207: in __getitem__
    return self._getitem_axis(maybe_callable, axis=axis)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexing.py:1773: in _getitem_axis
    self._validate_integer(key, axis)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexing.py:1706: in _validate_integer
    raise IndexError("single positional indexer is out-of-bounds")
E   IndexError: single positional indexer is out-of-bounds
=========================== short test summary info ============================
FAILED tests/test_ingest.py::test_enforce_schema_drops_extra_column - Asserti...
FAILED tests/test_transform.py::test_parse_production_date_malformed_gives_nat
======================== 2 failed, 110 passed in 10.05s ========================

```

---

## Eval Run at 2026-04-20 00:42:35

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:

tests/test_acquire.py::test_load_lease_index_filters_year PASSED         [  0%]
tests/test_acquire.py::test_load_lease_index_deduplicates_url PASSED     [  1%]
tests/test_acquire.py::test_load_lease_index_file_not_found PASSED       [  2%]
tests/test_acquire.py::test_load_lease_index_missing_url_column PASSED   [  3%]
tests/test_acquire.py::test_extract_lease_id_basic PASSED                [  4%]
tests/test_acquire.py::test_extract_lease_id_no_param PASSED             [  5%]
tests/test_acquire.py::test_extract_lease_id_extra_params PASSED         [  6%]
tests/test_acquire.py::test_parse_download_link_absolute PASSED          [  7%]
tests/test_acquire.py::test_parse_download_link_no_match PASSED          [  8%]
tests/test_acquire.py::test_parse_download_link_relative PASSED          [  8%]
tests/test_acquire.py::test_download_lease_success PASSED                [  9%]
tests/test_acquire.py::test_download_lease_idempotent PASSED             [ 10%]
tests/test_acquire.py::test_download_lease_no_download_link 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:153 No download link for lease 99: No anon_blobber.download link found in HTML (base_url=https://chasm.kgs.ku.edu)
PASSED                                                                   [ 11%]
tests/test_acquire.py::test_download_lease_data_500 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:168 Data download returned 500 for lease 77
PASSED                                                                   [ 12%]
tests/test_acquire.py::test_download_lease_empty_content 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:173 Empty content for lease 88
PASSED                                                                   [ 13%]
tests/test_acquire.py::test_retry_success_after_one_failure 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 1/3 failed for flaky: fail — retrying in 0.0s
PASSED                                                                   [ 14%]
tests/test_acquire.py::test_retry_always_fails 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 1/3 failed for always_fail: nope — retrying in 0.0s
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 2/3 failed for always_fail: nope — retrying in 0.0s
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 3/3 failed for always_fail: nope — retrying in 0.0s
PASSED                                                                   [ 15%]
tests/test_acquire.py::test_retry_max_attempts_one 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.acquire:acquire.py:213 Attempt 1/1 failed for boom: immediate — retrying in 0.0s
PASSED                                                                   [ 16%]
tests/test_acquire.py::test_validate_raw_files_valid PASSED              [ 16%]
tests/test_acquire.py::test_validate_raw_files_zero_byte PASSED          [ 17%]
tests/test_acquire.py::test_validate_raw_files_header_only PASSED        [ 18%]
tests/test_acquire.py::test_validate_raw_files_non_utf8 PASSED           [ 19%]
tests/test_acquire.py::test_validate_raw_files_dir_not_found PASSED      [ 20%]
tests/test_features.py::test_cum_production_values PASSED                [ 21%]
tests/test_features.py::test_cum_production_zero_at_start PASSED         [ 22%]
tests/test_features.py::test_cum_production_monotone PASSED              [ 23%]
tests/test_features.py::test_cum_production_missing_col PASSED           [ 24%]
tests/test_features.py::test_ratios_gor_undefined_zero_oil_nonzero_gas PASSED [ 25%]
tests/test_features.py::test_ratios_gor_zero_for_pos_oil_zero_gas PASSED [ 25%]
tests/test_features.py::test_ratios_gor_both_zero PASSED                 [ 26%]
tests/test_features.py::test_ratios_water_cut_zero_water PASSED          [ 27%]
tests/test_features.py::test_decline_rate_values PASSED                  [ 28%]
tests/test_features.py::test_decline_rate_clipped_low PASSED             [ 29%]
tests/test_features.py::test_decline_rate_zero_prev_is_nan PASSED        [ 30%]
tests/test_features.py::test_decline_rate_clipped_high PASSED            [ 31%]
tests/test_features.py::test_decline_rate_within_bounds_unchanged PASSED [ 32%]
tests/test_features.py::test_rolling_3m_values PASSED                    [ 33%]
tests/test_features.py::test_rolling_6m_all_values PASSED                [ 33%]
tests/test_features.py::test_rolling_partial_window PASSED               [ 34%]
tests/test_features.py::test_rolling_columns_present PASSED              [ 35%]
tests/test_features.py::test_lag_1m_value PASSED                         [ 36%]
tests/test_features.py::test_lag_1m_first_is_nan PASSED                  [ 37%]
tests/test_features.py::test_lag_3m_first_three_are_nan PASSED           [ 38%]
tests/test_features.py::test_lag_columns_present PASSED                  [ 39%]
tests/test_features.py::test_encode_product_code_present PASSED          [ 40%]
tests/test_features.py::test_encode_twn_dir_null_gives_minus1 PASSED     [ 41%]
tests/test_features.py::test_encode_originals_preserved PASSED           [ 41%]
tests/test_features.py::test_encode_range_dir_code_present PASSED        [ 42%]
tests/test_features.py::test_apply_feature_transforms_returns_dask PASSED [ 43%]
tests/test_features.py::test_apply_feature_transforms_all_derived_columns PASSED [ 44%]
tests/test_features.py::test_validate_features_schema_passes PASSED      [ 45%]
tests/test_features.py::test_validate_features_schema_missing_gor PASSED [ 46%]
tests/test_features.py::test_validate_features_schema_multiple_missing PASSED [ 47%]
tests/test_features.py::test_check_schema_stability_identical PASSED     [ 48%]
tests/test_features.py::test_check_schema_stability_extra_column PASSED  [ 49%]
tests/test_features.py::test_validate_correct_df_returns_empty PASSED    [ 50%]
tests/test_features.py::test_validate_negative_production PASSED         [ 50%]
tests/test_features.py::test_validate_decreasing_cum_production PASSED   [ 51%]
tests/test_features.py::test_validate_out_of_bounds_decline_rate PASSED  [ 52%]
tests/test_features.py::test_validate_water_cut_out_of_range PASSED      [ 53%]
tests/test_ingest.py::test_load_schema_returns_21_keys FAILED            [ 54%]
tests/test_ingest.py::test_load_schema_lease_kid_non_nullable_int64 PASSED [ 55%]
tests/test_ingest.py::test_load_schema_wells_nullable_int64 PASSED       [ 56%]
tests/test_ingest.py::test_load_schema_product_categories PASSED         [ 57%]
tests/test_ingest.py::test_load_schema_township_nullable_int64 PASSED    [ 58%]
tests/test_ingest.py::test_load_schema_file_not_found PASSED             [ 58%]
tests/test_ingest.py::test_read_raw_file_adds_source_file PASSED         [ 59%]
tests/test_ingest.py::test_read_raw_file_empty_bytes PASSED              [ 60%]
tests/test_ingest.py::test_read_raw_file_header_only PASSED              [ 61%]
tests/test_ingest.py::test_read_raw_file_not_found PASSED                [ 62%]
tests/test_ingest.py::test_filter_by_year_keeps_2024_onwards PASSED      [ 63%]
tests/test_ingest.py::test_filter_by_year_all_before_2024 PASSED         [ 64%]
tests/test_ingest.py::test_filter_by_year_no_non_numeric_tokens PASSED   [ 65%]
tests/test_ingest.py::test_enforce_schema_adds_nullable_column PASSED    [ 66%]
tests/test_ingest.py::test_enforce_schema_raises_for_non_nullable_absent PASSED [ 66%]
tests/test_ingest.py::test_enforce_schema_product_out_of_set_replaced PASSED [ 67%]
tests/test_ingest.py::test_enforce_schema_drops_extra_column PASSED      [ 68%]
tests/test_ingest.py::test_enforce_schema_twn_dir_categorical PASSED     [ 69%]
tests/test_ingest.py::test_ingest_file_year_filter PASSED                [ 70%]
tests/test_ingest.py::test_ingest_file_all_before_min_year PASSED        [ 71%]
tests/test_ingest.py::test_ingest_file_missing_non_nullable_raises PASSED [ 72%]
tests/test_ingest.py::test_write_interim_creates_parquet PASSED          [ 73%]
tests/test_ingest.py::test_validate_interim_schema_passes PASSED         [ 74%]
tests/test_ingest.py::test_validate_interim_schema_missing_column PASSED [ 75%]
tests/test_ingest.py::test_validate_interim_schema_missing_source_file PASSED [ 75%]
tests/test_transform.py::test_parse_production_date_basic PASSED         [ 76%]
tests/test_transform.py::test_parse_production_date_drops_month_zero PASSED [ 77%]
tests/test_transform.py::test_parse_production_date_no_month_year_col PASSED [ 78%]
tests/test_transform.py::test_parse_production_date_malformed_gives_nat 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:60 parse_production_date: 1 rows have unparseable MONTH-YEAR → NaT
PASSED                                                                   [ 79%]
tests/test_transform.py::test_clean_keeps_zero_production PASSED         [ 80%]
tests/test_transform.py::test_clean_drops_negative_production 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:130 clean_invalid_values: dropping 1 rows with PRODUCTION < 0 (LEASE_KIDs: [1])
PASSED                                                                   [ 81%]
tests/test_transform.py::test_clean_drops_nat_production_date PASSED     [ 82%]
tests/test_transform.py::test_clean_drops_null_lease_kid PASSED          [ 83%]
tests/test_transform.py::test_clean_null_production_imputed_with_active_wells PASSED [ 83%]
tests/test_transform.py::test_validate_flags_high_oil 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:164 validate_production_units: LEASE_KID=1001 has OIL PRODUCTION=60000.0 > 50000 (likely unit error)
PASSED                                                                   [ 84%]
tests/test_transform.py::test_validate_no_warning_for_low_oil PASSED     [ 85%]
tests/test_transform.py::test_validate_no_flag_for_high_gas PASSED       [ 86%]
tests/test_transform.py::test_validate_returns_same_row_count 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:164 validate_production_units: LEASE_KID=1 has OIL PRODUCTION=60000.0 > 50000 (likely unit error)
PASSED                                                                   [ 87%]
tests/test_transform.py::test_deduplicate_removes_duplicates PASSED      [ 88%]
tests/test_transform.py::test_deduplicate_idempotent PASSED              [ 89%]
tests/test_transform.py::test_deduplicate_no_duplicates PASSED           [ 90%]
tests/test_transform.py::test_deduplicate_output_leq_input PASSED        [ 91%]
tests/test_transform.py::test_index_and_sort_returns_dask_df PASSED      [ 91%]
tests/test_transform.py::test_index_and_sort_production_date_sorted PASSED [ 92%]
tests/test_transform.py::test_index_and_sort_missing_lease_kid PASSED    [ 93%]
tests/test_transform.py::test_index_and_sort_missing_production_date PASSED [ 94%]
tests/test_transform.py::test_check_well_completeness_no_gap PASSED      [ 95%]
tests/test_transform.py::test_check_well_completeness_with_gap 
-------------------------------- live log call ---------------------------------
WARNING  kgs_pipeline.transform:transform.py:256 check_well_completeness: gap > 31 days for ('1001', 'O') from 2024-02-01 to 2024-04-01
PASSED                                                                   [ 96%]
tests/test_transform.py::test_apply_partition_transforms_returns_dask_df PASSED [ 97%]
tests/test_transform.py::test_apply_partition_transforms_adds_production_date PASSED [ 98%]
tests/test_transform.py::test_write_transform_creates_parquet PASSED     [ 99%]
tests/test_transform.py::test_write_transform_readable_by_pandas PASSED  [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
_______________________ test_load_schema_returns_21_keys _______________________
tests/test_ingest.py:32: in test_load_schema_returns_21_keys
    assert len(schema) == 21
E   AssertionError: assert 20 == 21
E    +  where 20 = len({'API_NUMBER': {'categories': None, 'dtype': 'string', 'nullable': True}, 'COUNTY': {'categories': None, 'dtype': 'string', 'nullable': True}, 'DOR_CODE': {'categories': None, 'dtype': 'Int64', 'nullable': True}, 'FIELD': {'categories': None, 'dtype': 'string', 'nullable': True}, ...})
=========================== short test summary info ============================
FAILED tests/test_ingest.py::test_load_schema_returns_21_keys - AssertionErro...
======================== 1 failed, 111 passed in 9.41s =========================

```

---

## Eval Run at 2026-04-20 00:47:39

**Status:** ✅ PASSED

---
