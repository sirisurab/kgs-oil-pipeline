
## Eval Run at 2026-04-17 00:03:16

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F841 Local variable `df` is assigned to but never used
   --> tests/test_features.py:285:5
    |
283 | def test_rolling_per_lease_independent(gas_oil_df: pd.DataFrame) -> None:
284 |     """Rolling values per lease don't bleed across leases."""
285 |     df = gas_oil_df.copy()
    |     ^^
286 |     # Extend to multi-month for rolling computation
287 |     rows = []
    |
help: Remove assignment to unused variable `df`

F841 Local variable `report_path` is assigned to but never used
   --> tests/test_pipeline.py:186:5
    |
184 | def test_main_writes_pipeline_report(tmp_path: Path) -> None:
185 |     """Pipeline report is written after run."""
186 |     report_path = tmp_path / "processed" / "pipeline_report.json"
    |     ^^^^^^^^^^^
187 |     cfg = {
188 |         "acquire": {"index_path": "data/external/x.txt", "output_dir": str(tmp_path / "raw")},
    |
help: Remove assignment to unused variable `report_path`

E712 Avoid equality comparisons to `False`; use `not result["has_date_gap"]:` for false checks
   --> tests/test_transform.py:332:13
    |
330 |     )
331 |     result = check_well_completeness(df)
332 |     assert (result["has_date_gap"] == False).all()
    |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    |
help: Replace with `not result["has_date_gap"]`

E712 Avoid equality comparisons to `True`; use `result["has_date_gap"]:` for truth checks
   --> tests/test_transform.py:345:13
    |
343 |     result = check_well_completeness(df)
344 |     # Jan and March present, Feb missing → gap
345 |     assert (result["has_date_gap"] == True).all()
    |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    |
help: Replace with `result["has_date_gap"]`

E712 Avoid equality comparisons to `False`; use `not result["has_date_gap"].iloc[0]:` for false checks
   --> tests/test_transform.py:357:12
    |
355 |     )
356 |     result = check_well_completeness(df)
357 |     assert result["has_date_gap"].iloc[0] == False
    |            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    |
help: Replace with `not result["has_date_gap"].iloc[0]`

Found 5 errors.
No fixes available (5 hidden fixes can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/tests/test_features.py:518: error: Need type annotation for "enc"  [var-annotated]
/kgs_pipeline/ingest.py:186: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/ingest.py:186: note: Possible overload variants:
/kgs_pipeline/ingest.py:186: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/ingest.py:186: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/acquire.py:144: error: Invalid index type "str" for "NavigableString"; expected type "SupportsIndex | slice[SupportsIndex | None, SupportsIndex | None, SupportsIndex | None]"  [index]
/kgs_pipeline/acquire.py:144: note: Error code "index" not covered by "type: ignore" comment
Found 3 errors in 3 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
........................................................................ [ 53%]
...........F.FFF.F.....................FF.....................           [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
__________________ test_read_raw_file_absent_nullable_column ___________________
tests/test_ingest.py:147: in test_read_raw_file_absent_nullable_column
    df = read_raw_file(p, sample_schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/ingest.py:176: in read_raw_file
    df[col] = pd.array([pd.NA] * len(df), dtype=resolved)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:400: in array
    return NumpyExtensionArray._from_sequence(data, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/numpy_.py:145: in _from_sequence
    result = np.asarray(scalars, dtype=dtype)  # type: ignore[arg-type]
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
________________________ test_read_raw_file_year_filter ________________________
tests/test_ingest.py:169: in test_read_raw_file_year_filter
    df = read_raw_file(p, sample_schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/ingest.py:176: in read_raw_file
    df[col] = pd.array([pd.NA] * len(df), dtype=resolved)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:400: in array
    return NumpyExtensionArray._from_sequence(data, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/numpy_.py:145: in _from_sequence
    result = np.asarray(scalars, dtype=dtype)  # type: ignore[arg-type]
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
_________________ test_read_raw_file_non_numeric_year_dropped __________________
tests/test_ingest.py:180: in test_read_raw_file_non_numeric_year_dropped
    df = read_raw_file(p, sample_schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/ingest.py:176: in read_raw_file
    df[col] = pd.array([pd.NA] * len(df), dtype=resolved)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:400: in array
    return NumpyExtensionArray._from_sequence(data, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/numpy_.py:145: in _from_sequence
    result = np.asarray(scalars, dtype=dtype)  # type: ignore[arg-type]
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
______________ test_read_raw_file_invalid_categorical_becomes_na _______________
tests/test_ingest.py:190: in test_read_raw_file_invalid_categorical_becomes_na
    df = read_raw_file(p, sample_schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/ingest.py:176: in read_raw_file
    df[col] = pd.array([pd.NA] * len(df), dtype=resolved)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:400: in array
    return NumpyExtensionArray._from_sequence(data, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/numpy_.py:145: in _from_sequence
    result = np.asarray(scalars, dtype=dtype)  # type: ignore[arg-type]
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
____________________ test_read_raw_file_url_column_dropped _____________________
tests/test_ingest.py:209: in test_read_raw_file_url_column_dropped
    df = read_raw_file(p, sample_schema)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/ingest.py:176: in read_raw_file
    df[col] = pd.array([pd.NA] * len(df), dtype=resolved)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/construction.py:400: in array
    return NumpyExtensionArray._from_sequence(data, dtype=dtype, copy=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/arrays/numpy_.py:145: in _from_sequence
    result = np.asarray(scalars, dtype=dtype)  # type: ignore[arg-type]
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: float() argument must be a string or a real number, not 'NAType'
______________ test_validate_physical_bounds_oil_flag_over_50000 _______________
tests/test_transform.py:187: in test_validate_physical_bounds_oil_flag_over_50000
    assert result["production_unit_flag"].iloc[0] is True
E   assert np.True_ is True
__________________ test_validate_physical_bounds_gas_no_flag ___________________
tests/test_transform.py:200: in test_validate_physical_bounds_gas_no_flag
    assert result["production_unit_flag"].iloc[0] is False
E   assert np.False_ is False
=========================== short test summary info ============================
FAILED tests/test_ingest.py::test_read_raw_file_absent_nullable_column - Type...
FAILED tests/test_ingest.py::test_read_raw_file_year_filter - TypeError: floa...
FAILED tests/test_ingest.py::test_read_raw_file_non_numeric_year_dropped - Ty...
FAILED tests/test_ingest.py::test_read_raw_file_invalid_categorical_becomes_na
FAILED tests/test_ingest.py::test_read_raw_file_url_column_dropped - TypeErro...
FAILED tests/test_transform.py::test_validate_physical_bounds_oil_flag_over_50000
FAILED tests/test_transform.py::test_validate_physical_bounds_gas_no_flag - a...
7 failed, 127 passed in 8.41s

```

---

## Eval Run at 2026-04-17 00:22:15

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
E712 Avoid equality comparisons to `False`; use `not result["has_date_gap"]:` for false checks
   --> tests/test_transform.py:332:13
    |
330 |     )
331 |     result = check_well_completeness(df)
332 |     assert (result["has_date_gap"] == False).all()
    |             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    |
help: Replace with `not result["has_date_gap"]`

Found 1 error.
No fixes available (1 hidden fix can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/ingest.py:191: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "None"  [call-overload]
/kgs_pipeline/ingest.py:191: note: Possible overload variants:
/kgs_pipeline/ingest.py:191: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/ingest.py:191: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/acquire.py:144: error: Incompatible types in assignment (expression has type "str | list[str]", variable has type "str")  [assignment]
/kgs_pipeline/acquire.py:144: note: Error code "assignment" not covered by "type: ignore" comment
Found 2 errors in 2 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
........................................................................ [ 53%]
.................F............................................           [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
____________________ test_read_raw_file_url_column_dropped _____________________
tests/test_ingest.py:210: in test_read_raw_file_url_column_dropped
    assert "URL" not in df.columns
E   AssertionError: assert 'URL' not in Index(['LEASE_KID', 'MONTH-YEAR', 'PRODUCT', 'LEASE', 'DOR_CODE', 'API_NUMBER',\n       'FIELD', 'PRODUCING_ZONE', 'OPE...R', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE',\n       'WELLS', 'PRODUCTION', 'URL', 'source_file'],\n      dtype='str')
E    +  where Index(['LEASE_KID', 'MONTH-YEAR', 'PRODUCT', 'LEASE', 'DOR_CODE', 'API_NUMBER',\n       'FIELD', 'PRODUCING_ZONE', 'OPE...R', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE',\n       'WELLS', 'PRODUCTION', 'URL', 'source_file'],\n      dtype='str') =    LEASE_KID MONTH-YEAR PRODUCT LEASE  ...  WELLS PRODUCTION   URL source_file\n0       1001     1-2024       O  <NA>  ...   <NA>        NaN  <NA>   lp007.txt\n\n[1 rows x 22 columns].columns
=========================== short test summary info ============================
FAILED tests/test_ingest.py::test_read_raw_file_url_column_dropped - Assertio...
1 failed, 133 passed in 8.79s

```

---

## Eval Run at 2026-04-17 00:31:23

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/acquire.py:144: error: Invalid index type "str" for "NavigableString"; expected type "SupportsIndex | slice[SupportsIndex | None, SupportsIndex | None, SupportsIndex | None]"  [index]
/kgs_pipeline/acquire.py:144: note: Error code "index" not covered by "type: ignore" comment
Found 1 error in 1 file (checked 12 source files)

```

---

## Eval Run at 2026-04-17 00:37:09

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/acquire.py:146: error: Incompatible types in assignment (expression has type "str | list[str]", variable has type "str")  [assignment]
Found 1 error in 1 file (checked 12 source files)

```

---

## Eval Run at 2026-04-17 00:41:29

**Status:** ✅ PASSED

---
