
## Eval Run at 2026-04-27 19:17:33

**Status:** ❌ FAILED

### Failures:
- **Install:**
```
pip install failed — likely a build backend error in pyproject.toml.
  error: subprocess-exited-with-error
  
  × Getting requirements to build editable did not run successfully.
  │ exit code: 1
  ╰─> [14 lines of output]
      error: Multiple top-level packages discovered in a flat-layout: ['data', 'references', 'kgs_pipeline'].
      
      To avoid accidental inclusion of unwanted files or directories,
      setuptools will not proceed with this build.
      
      If you are trying to create a single distribution with multiple packages
      on purpose, you should not rely on automatic discovery.
      Instead, consider the following options:
      
      1. set up custom discovery (`find` directive with `include` or `exclude`)
      2. use a `src-layout`
      3. explicitly set `py_modules` or `packages` with a list of names
      
      To find more information, look for "package discovery" on setuptools docs.
      [end of output]
  
  note: This error originates from a subprocess, and is likely not a problem with pip.
ERROR: Failed to build 'file:///Users/sirisurab/projects/dapi_poc/kgs' when getting requirements to build editable

```

---

## Eval Run at 2026-04-27 19:21:32

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F841 Local variable `dtype_map` is assigned to but never used
   --> tests/test_ingest.py:401:5
    |
400 |     data_dict = load_data_dictionary(DATA_DICT_PATH)
401 |     dtype_map = build_dtype_map(data_dict)
    |     ^^^^^^^^^
402 |     df = dd.read_parquet(str(interim_dir)).compute()
    |
help: Remove assignment to unused variable `dtype_map`

F841 Local variable `mock_cluster` is assigned to but never used
   --> tests/test_pipeline.py:164:69
    |
162 |     import distributed
163 |
164 |     with patch("kgs_pipeline.pipeline.distributed.LocalCluster") as mock_cluster:
    |                                                                     ^^^^^^^^^^^^
165 |         with patch("kgs_pipeline.pipeline.distributed.Client") as mock_client_cls:
166 |             mock_client_instance = MagicMock(spec=distributed.Client)
    |
help: Remove assignment to unused variable `mock_cluster`

Found 2 errors.
No fixes available (2 hidden fixes can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/features.py:98: error: No overload variant of "clip" matches argument types "NumpyExtensionArray", "float", "float"  [call-overload]
/kgs_pipeline/features.py:98: note: Possible overload variants:
/kgs_pipeline/features.py:98: note:     def [_ScalarT: generic[Any]] clip(a: _ScalarT, a_min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., a_max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., out: None = ..., *, min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., dtype: None = ..., where: _SupportsArray[dtype[numpy.bool[builtins.bool]]] | _NestedSequence[_SupportsArray[dtype[numpy.bool[builtins.bool]]]] | builtins.bool | _NestedSequence[builtins.bool] | None = ..., order: Literal['K', 'A', 'C', 'F'] | None = ..., subok: bool = ..., signature: str | tuple[str | None, ...] = ..., casting: Literal['no', 'equiv', 'safe', 'same_kind', 'same_value', 'unsafe'] = ...) -> _ScalarT
/kgs_pipeline/features.py:98: note:     def clip(a: complex | str | bytes | generic[Any], a_min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., a_max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., out: None = ..., *, min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., dtype: None = ..., where: _SupportsArray[dtype[numpy.bool[builtins.bool]]] | _NestedSequence[_SupportsArray[dtype[numpy.bool[builtins.bool]]]] | builtins.bool | _NestedSequence[builtins.bool] | None = ..., order: Literal['K', 'A', 'C', 'F'] | None = ..., subok: bool = ..., signature: str | tuple[str | None, ...] = ..., casting: Literal['no', 'equiv', 'safe', 'same_kind', 'same_value', 'unsafe'] = ...) -> Any
/kgs_pipeline/features.py:98: note:     def [_ScalarT: generic[Any]] clip(a: _SupportsArray[dtype[_ScalarT]] | _NestedSequence[_SupportsArray[dtype[_ScalarT]]], a_min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., a_max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., out: None = ..., *, min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., dtype: None = ..., where: _SupportsArray[dtype[numpy.bool[builtins.bool]]] | _NestedSequence[_SupportsArray[dtype[numpy.bool[builtins.bool]]]] | builtins.bool | _NestedSequence[builtins.bool] | None = ..., order: Literal['K', 'A', 'C', 'F'] | None = ..., subok: bool = ..., signature: str | tuple[str | None, ...] = ..., casting: Literal['no', 'equiv', 'safe', 'same_kind', 'same_value', 'unsafe'] = ...) -> ndarray[tuple[Any, ...], dtype[_ScalarT]]
/kgs_pipeline/features.py:98: note:     def clip(a: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str], a_min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., a_max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., out: None = ..., *, min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., dtype: None = ..., where: _SupportsArray[dtype[numpy.bool[builtins.bool]]] | _NestedSequence[_SupportsArray[dtype[numpy.bool[builtins.bool]]]] | builtins.bool | _NestedSequence[builtins.bool] | None = ..., order: Literal['K', 'A', 'C', 'F'] | None = ..., subok: bool = ..., signature: str | tuple[str | None, ...] = ..., casting: Literal['no', 'equiv', 'safe', 'same_kind', 'same_value', 'unsafe'] = ...) -> ndarray[tuple[Any, ...], dtype[Any]]
/kgs_pipeline/features.py:98: note:     def [_ArrayT: ndarray[Any, Any]] clip(a: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str], a_min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | None, a_max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | None, out: _ArrayT, *, min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., dtype: type[Any] | dtype[Any] | _HasDType[dtype[Any]] | _HasNumPyDType[dtype[Any]] | tuple[Any, Any] | list[Any] | _DTypeDict | str | None = ..., where: _SupportsArray[dtype[numpy.bool[builtins.bool]]] | _NestedSequence[_SupportsArray[dtype[numpy.bool[builtins.bool]]]] | builtins.bool | _NestedSequence[builtins.bool] | None = ..., order: Literal['K', 'A', 'C', 'F'] | None = ..., subok: bool = ..., signature: str | tuple[str | None, ...] = ..., casting: Literal['no', 'equiv', 'safe', 'same_kind', 'same_value', 'unsafe'] = ...) -> _ArrayT
/kgs_pipeline/features.py:98: note:     def [_ArrayT: ndarray[Any, Any]] clip(a: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str], a_min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., a_max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., *, out: _ArrayT, min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., dtype: type[Any] | dtype[Any] | _HasDType[dtype[Any]] | _HasNumPyDType[dtype[Any]] | tuple[Any, Any] | list[Any] | _DTypeDict | str | None = ..., where: _SupportsArray[dtype[numpy.bool[builtins.bool]]] | _NestedSequence[_SupportsArray[dtype[numpy.bool[builtins.bool]]]] | builtins.bool | _NestedSequence[builtins.bool] | None = ..., order: Literal['K', 'A', 'C', 'F'] | None = ..., subok: bool = ..., signature: str | tuple[str | None, ...] = ..., casting: Literal['no', 'equiv', 'safe', 'same_kind', 'same_value', 'unsafe'] = ...) -> _ArrayT
/kgs_pipeline/features.py:98: note:     def clip(a: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str], a_min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., a_max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., out: None = ..., *, min: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., max: Buffer | _SupportsArray[dtype[Any]] | _NestedSequence[_SupportsArray[dtype[Any]]] | complex | bytes | str | _NestedSequence[complex | bytes | str] | _NoValueType | None = ..., dtype: type[Any] | dtype[Any] | _HasDType[dtype[Any]] | _HasNumPyDType[dtype[Any]] | tuple[Any, Any] | list[Any] | _DTypeDict | str | None = ..., where: _SupportsArray[dtype[numpy.bool[builtins.bool]]] | _NestedSequence[_SupportsArray[dtype[numpy.bool[builtins.bool]]]] | builtins.bool | _NestedSequence[builtins.bool] | None = ..., order: Literal['K', 'A', 'C', 'F'] | None = ..., subok: bool = ..., signature: str | tuple[str | None, ...] = ..., casting: Literal['no', 'equiv', 'safe', 'same_kind', 'same_value', 'unsafe'] = ...) -> Any
/kgs_pipeline/acquire.py:60: error: Item "NavigableString" of "Tag | NavigableString" has no attribute "get"  [union-attr]
/kgs_pipeline/pipeline.py:128: error: Incompatible types in assignment (expression has type "Callable[[dict[Any, Any]], None]", variable has type "Callable[[dict[Any, Any]], list[Path | None]]")  [assignment]
/kgs_pipeline/pipeline.py:130: error: Incompatible types in assignment (expression has type "Callable[[dict[Any, Any]], None]", variable has type "Callable[[dict[Any, Any]], list[Path | None]]")  [assignment]
/kgs_pipeline/pipeline.py:132: error: Incompatible types in assignment (expression has type "Callable[[dict[Any, Any]], None]", variable has type "Callable[[dict[Any, Any]], list[Path | None]]")  [assignment]
Found 5 errors in 3 files (checked 11 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, responses-0.5.1, mock-3.15.1, langsmith-0.7.35, repeat-0.9.4, cov-7.1.0, xdist-3.8.0, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 114 items

tests/test_acquire.py ....................                               [ 17%]
tests/test_features.py ......................FFFFFFFFF                   [ 44%]
tests/test_ingest.py .....................                               [ 63%]
tests/test_pipeline.py .....FFF....F                                     [ 74%]
tests/test_transform.py ..FF....................FFFFF                    [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
_____________________ test_apply_features_returns_dask_df ______________________
tests/test_features.py:301: in test_apply_features_returns_dask_df
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_____________________ test_apply_features_does_not_compute _____________________
tests/test_features.py:310: in test_apply_features_does_not_compute
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
___________________ test_apply_features_meta_matches_output ____________________
tests/test_features.py:328: in test_apply_features_meta_matches_output
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_________________________ test_features_writes_parquet _________________________
tests/test_features.py:381: in test_features_writes_parquet
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
________________________ test_features_parquet_readable ________________________
tests/test_features.py:398: in test_features_parquet_readable
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
____________________ test_features_expected_columns_present ____________________
tests/test_features.py:416: in test_features_expected_columns_present
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
______________ test_features_schema_consistent_across_partitions _______________
tests/test_features.py:451: in test_features_schema_consistent_across_partitions
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_____________________ test_features_production_nonnegative _____________________
tests/test_features.py:471: in test_features_production_nonnegative
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_________________ test_full_pipeline_ingest_transform_features _________________
tests/test_features.py:490: in test_full_pipeline_ingest_transform_features
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
__________________ test_init_dask_client_local_returns_client __________________
tests/test_pipeline.py:166: in test_init_dask_client_local_returns_client
    mock_client_instance = MagicMock(spec=distributed.Client)
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:2139: in __init__
    _safe_super(MagicMixin, self).__init__(*args, **kw)
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1121: in __init__
    _safe_super(CallableMixin, self).__init__(
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:460: in __init__
    self._mock_add_spec(spec, spec_set, _spec_as_instance, _eat_self)
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:511: in _mock_add_spec
    raise InvalidSpecError(f'Cannot spec a Mock object. [object={spec!r}]')
E   unittest.mock.InvalidSpecError: Cannot spec a Mock object. [object=<MagicMock name='Client' id='7258166176'>]
_____________________ test_init_dask_client_logs_dashboard _____________________
tests/test_pipeline.py:188: in test_init_dask_client_logs_dashboard
    mock_client_instance = MagicMock(spec=distributed.Client)
                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:2139: in __init__
    _safe_super(MagicMixin, self).__init__(*args, **kw)
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1121: in __init__
    _safe_super(CallableMixin, self).__init__(
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:460: in __init__
    self._mock_add_spec(spec, spec_set, _spec_as_instance, _eat_self)
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:511: in _mock_add_spec
    raise InvalidSpecError(f'Cannot spec a Mock object. [object={spec!r}]')
E   unittest.mock.InvalidSpecError: Cannot spec a Mock object. [object=<MagicMock name='Client' id='7251088240'>]
________________ test_init_dask_client_url_connects_to_address _________________
tests/test_pipeline.py:218: in test_init_dask_client_url_connects_to_address
    init_dask_client(config)
kgs_pipeline/pipeline.py:79: in init_dask_client
    client = distributed.Client(scheduler)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1139: in __call__
    return self._mock_call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1143: in _mock_call
    return self._execute_mock_call(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1204: in _execute_mock_call
    result = effect(*args, **kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^
tests/test_pipeline.py:213: in fake_client
    m = MagicMock(spec=distributed.Client)
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:2139: in __init__
    _safe_super(MagicMixin, self).__init__(*args, **kw)
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1121: in __init__
    _safe_super(CallableMixin, self).__init__(
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:460: in __init__
    self._mock_add_spec(spec, spec_set, _spec_as_instance, _eat_self)
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:511: in _mock_add_spec
    raise InvalidSpecError(f'Cannot spec a Mock object. [object={spec!r}]')
E   unittest.mock.InvalidSpecError: Cannot spec a Mock object. [object=<MagicMock name='Client' id='7257442544'>]
_________________ test_full_pipeline_end_to_end_no_exceptions __________________
tests/test_pipeline.py:377: in test_full_pipeline_end_to_end_no_exceptions
    run_transform(
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.0.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:191 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim
INFO     kgs_pipeline.transform:transform.py:197 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:226 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/processed/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/processed/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/interim/part.0.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/processed/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_full_pipeline_end_to_end_0/processed/part.2.parquet
_______________ test_parse_production_date_drops_negative_month ________________
tests/test_transform.py:106: in test_parse_production_date_drops_negative_month
    assert len(result) == 1
E   assert 2 == 1
E    +  where 2 = len(  MONTH-YEAR  PRODUCTION production_date\n0    -1-2024         0.0      2024-01-01\n1     6-2024        50.0      2024-06-01)
_______________________ test_parse_production_date_dtype _______________________
tests/test_transform.py:113: in test_parse_production_date_dtype
    assert result["production_date"].dtype == np.dtype("datetime64[ns]")
E   AssertionError: assert dtype('<M8[us]') == dtype('<M8[ns]')
E    +  where dtype('<M8[us]') = 0   2024-03-01\nName: production_date, dtype: datetime64[us].dtype
E    +  and   dtype('<M8[ns]') = <class 'numpy.dtype'>('datetime64[ns]')
E    +    where <class 'numpy.dtype'> = np.dtype
________________________ test_transform_writes_parquet _________________________
tests/test_transform.py:454: in test_transform_writes_parquet
    transform(transform_config)
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.0.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.1.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:191 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim
INFO     kgs_pipeline.transform:transform.py:197 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:226 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/processed/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/processed/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/interim/part.0.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_writes_parquet0/processed/part.4.parquet
_______________________ test_transform_parquet_readable ________________________
tests/test_transform.py:482: in test_transform_parquet_readable
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.0.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.1.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:191 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim
INFO     kgs_pipeline.transform:transform.py:197 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:226 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/processed/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_parquet_readabl0/processed/part.8.parquet
__________________________ test_transform_sort_order ___________________________
tests/test_transform.py:508: in test_transform_sort_order
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.0.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.1.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:191 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim
INFO     kgs_pipeline.transform:transform.py:197 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:226 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/processed/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_sort_order0/processed/part.8.parquet
______________________ test_transform_row_count_le_input _______________________
tests/test_transform.py:540: in test_transform_row_count_le_input
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.0.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.0.parquet
INFO     kgs_pipeline.transform:transform.py:191 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim
INFO     kgs_pipeline.transform:transform.py:197 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:226 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/processed/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_row_count_le_in0/interim/part.0.parquet
_______________________ test_transform_boundary_contract _______________________
tests/test_transform.py:566: in test_transform_boundary_contract
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
kgs_pipeline/transform.py:227: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.0.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:191 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim
INFO     kgs_pipeline.transform:transform.py:197 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:226 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/processed/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/processed/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-84/test_transform_boundary_contra0/interim/part.0.parquet
=========================== short test summary info ============================
FAILED tests/test_features.py::test_apply_features_returns_dask_df - ValueErr...
FAILED tests/test_features.py::test_apply_features_does_not_compute - ValueEr...
FAILED tests/test_features.py::test_apply_features_meta_matches_output - Valu...
FAILED tests/test_features.py::test_features_writes_parquet - ValueError: The...
FAILED tests/test_features.py::test_features_parquet_readable - ValueError: T...
FAILED tests/test_features.py::test_features_expected_columns_present - Value...
FAILED tests/test_features.py::test_features_schema_consistent_across_partitions
FAILED tests/test_features.py::test_features_production_nonnegative - ValueEr...
FAILED tests/test_features.py::test_full_pipeline_ingest_transform_features
FAILED tests/test_pipeline.py::test_init_dask_client_local_returns_client - u...
FAILED tests/test_pipeline.py::test_init_dask_client_logs_dashboard - unittes...
FAILED tests/test_pipeline.py::test_init_dask_client_url_connects_to_address
FAILED tests/test_pipeline.py::test_full_pipeline_end_to_end_no_exceptions - ...
FAILED tests/test_transform.py::test_parse_production_date_drops_negative_month
FAILED tests/test_transform.py::test_parse_production_date_dtype - AssertionE...
FAILED tests/test_transform.py::test_transform_writes_parquet - ValueError: T...
FAILED tests/test_transform.py::test_transform_parquet_readable - ValueError:...
FAILED tests/test_transform.py::test_transform_sort_order - ValueError: The c...
FAILED tests/test_transform.py::test_transform_row_count_le_input - ValueErro...
FAILED tests/test_transform.py::test_transform_boundary_contract - ValueError...
======================== 20 failed, 94 passed in 31.97s ========================

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, responses-0.5.1, mock-3.15.1, langsmith-0.7.35, repeat-0.9.4, cov-7.1.0, xdist-3.8.0, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 114 items / 114 deselected / 0 selected
Running teardown with pytest sessionfinish...

=========================== 114 deselected in 4.84s ============================

```

---

## Eval Run at 2026-04-27 19:38:13

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F841 Local variable `data_dict` is assigned to but never used
   --> tests/test_ingest.py:400:5
    |
398 |     ingest(config)
399 |
400 |     data_dict = load_data_dictionary(DATA_DICT_PATH)
    |     ^^^^^^^^^
401 |     df = dd.read_parquet(str(interim_dir)).compute()
    |
help: Remove assignment to unused variable `data_dict`

Found 1 error.
No fixes available (1 hidden fix can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/features.py:98: error: "NumpyExtensionArray" has no attribute "clip"  [attr-defined]
Found 1 error in 1 file (checked 11 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, responses-0.5.1, mock-3.15.1, langsmith-0.7.35, repeat-0.9.4, cov-7.1.0, xdist-3.8.0, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 114 items

tests/test_acquire.py ....................                               [ 17%]
tests/test_features.py ...........FFFFFF.....FFFFFFFFF                   [ 44%]
tests/test_ingest.py .....................                               [ 63%]
tests/test_pipeline.py ............F                                     [ 74%]
tests/test_transform.py ........................FFFFF                    [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
___________________________ test_decline_rate_value ____________________________
tests/test_features.py:205: in test_decline_rate_value
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:98: in add_decline_rate
    df["decline_rate"] = rate_series.clip(lower=-1.0, upper=10.0)
                         ^^^^^^^^^^^^^^^^
E   AttributeError: 'NumpyExtensionArray' object has no attribute 'clip'
________________________ test_decline_rate_clipped_min _________________________
tests/test_features.py:212: in test_decline_rate_clipped_min
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:98: in add_decline_rate
    df["decline_rate"] = rate_series.clip(lower=-1.0, upper=10.0)
                         ^^^^^^^^^^^^^^^^
E   AttributeError: 'NumpyExtensionArray' object has no attribute 'clip'
________________________ test_decline_rate_clipped_max _________________________
tests/test_features.py:219: in test_decline_rate_clipped_max
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:98: in add_decline_rate
    df["decline_rate"] = rate_series.clip(lower=-1.0, upper=10.0)
                         ^^^^^^^^^^^^^^^^
E   AttributeError: 'NumpyExtensionArray' object has no attribute 'clip'
__________________ test_decline_rate_within_bounds_unchanged ___________________
tests/test_features.py:226: in test_decline_rate_within_bounds_unchanged
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:98: in add_decline_rate
    df["decline_rate"] = rate_series.clip(lower=-1.0, upper=10.0)
                         ^^^^^^^^^^^^^^^^
E   AttributeError: 'NumpyExtensionArray' object has no attribute 'clip'
___________ test_decline_rate_zero_denominator_no_unclipped_extreme ____________
tests/test_features.py:233: in test_decline_rate_zero_denominator_no_unclipped_extreme
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:98: in add_decline_rate
    df["decline_rate"] = rate_series.clip(lower=-1.0, upper=10.0)
                         ^^^^^^^^^^^^^^^^
E   AttributeError: 'NumpyExtensionArray' object has no attribute 'clip'
___________________________ test_decline_rate_dtype ____________________________
tests/test_features.py:239: in test_decline_rate_dtype
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:98: in add_decline_rate
    df["decline_rate"] = rate_series.clip(lower=-1.0, upper=10.0)
                         ^^^^^^^^^^^^^^^^
E   AttributeError: 'NumpyExtensionArray' object has no attribute 'clip'
_____________________ test_apply_features_returns_dask_df ______________________
tests/test_features.py:301: in test_apply_features_returns_dask_df
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_____________________ test_apply_features_does_not_compute _____________________
tests/test_features.py:310: in test_apply_features_does_not_compute
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
___________________ test_apply_features_meta_matches_output ____________________
tests/test_features.py:328: in test_apply_features_meta_matches_output
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_________________________ test_features_writes_parquet _________________________
tests/test_features.py:381: in test_features_writes_parquet
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
________________________ test_features_parquet_readable ________________________
tests/test_features.py:398: in test_features_parquet_readable
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
____________________ test_features_expected_columns_present ____________________
tests/test_features.py:416: in test_features_expected_columns_present
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
______________ test_features_schema_consistent_across_partitions _______________
tests/test_features.py:451: in test_features_schema_consistent_across_partitions
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_____________________ test_features_production_nonnegative _____________________
tests/test_features.py:471: in test_features_production_nonnegative
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_________________ test_full_pipeline_ingest_transform_features _________________
tests/test_features.py:490: in test_full_pipeline_ingest_transform_features
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)
tests/test_features.py:367: in _write_and_run_ingest_transform
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
_________________ test_full_pipeline_end_to_end_no_exceptions __________________
tests/test_pipeline.py:383: in test_full_pipeline_end_to_end_no_exceptions
    run_transform(
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.0.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:189 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim
INFO     kgs_pipeline.transform:transform.py:195 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:222 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/processed/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/processed/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/processed/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_full_pipeline_end_to_end_0/interim/part.0.parquet
________________________ test_transform_writes_parquet _________________________
tests/test_transform.py:454: in test_transform_writes_parquet
    transform(transform_config)
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.0.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:189 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim
INFO     kgs_pipeline.transform:transform.py:195 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:222 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/processed/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/processed/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/processed/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_writes_parquet0/interim/part.0.parquet
_______________________ test_transform_parquet_readable ________________________
tests/test_transform.py:482: in test_transform_parquet_readable
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.0.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:189 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim
INFO     kgs_pipeline.transform:transform.py:195 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:222 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/processed/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/processed/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_parquet_readabl0/processed/part.5.parquet
__________________________ test_transform_sort_order ___________________________
tests/test_transform.py:508: in test_transform_sort_order
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.0.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:189 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim
INFO     kgs_pipeline.transform:transform.py:195 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:222 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/processed/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_sort_order0/interim/part.1.parquet
______________________ test_transform_row_count_le_input _______________________
tests/test_transform.py:540: in test_transform_row_count_le_input
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.0.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.1.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.0.parquet
INFO     kgs_pipeline.transform:transform.py:189 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim
INFO     kgs_pipeline.transform:transform.py:195 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:222 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/processed/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_row_count_le_in0/interim/part.0.parquet
_______________________ test_transform_boundary_contract _______________________
tests/test_transform.py:566: in test_transform_boundary_contract
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
kgs_pipeline/transform.py:223: in transform
    ddf.to_parquet(processed_dir, overwrite=True)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/_collection.py:3325: in to_parquet
    return to_parquet(self, path, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/dask_expr/io/parquet.py:661: in to_parquet
    out = out.compute(**compute_kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:377: in compute
    (result,) = compute(self, traverse=False, **kwargs)
                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/base.py:685: in compute
    results = schedule(expr, keys, **kwargs)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/dask/dataframe/utils.py:403: in check_matching_columns
    raise ValueError(
E   ValueError: The columns in the computed data do not match the columns in the provided metadata.
E   Order of columns does not match.
E   Actual:   ['production_date', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file']
E   Expected: ['LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'MONTH-YEAR', 'PRODUCT', 'WELLS', 'PRODUCTION', 'source_file', 'production_date']
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.ingest:ingest.py:135 Ingest: loading data dictionary from references/kgs_monthly_data_dictionary.csv
INFO     kgs_pipeline.ingest:ingest.py:147 Ingest: found 1 raw files → 10 partitions
INFO     kgs_pipeline.ingest:ingest.py:155 Ingest: writing interim Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.0.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.1.parquet
INFO     kgs_pipeline.ingest:ingest.py:157 Ingest: complete
INFO     kgs_pipeline.transform:transform.py:189 Transform: reading interim Parquet from /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim
INFO     kgs_pipeline.transform:transform.py:195 Transform: 10 input partitions → 10 output partitions
INFO     kgs_pipeline.transform:transform.py:222 Transform: writing processed Parquet to /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/processed
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.9.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.4.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/processed/part.6.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/processed/part.8.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/processed/part.5.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/processed/part.3.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/processed/part.7.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/processed/part.2.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.1.parquet
DEBUG    fsspec.local:local.py:383 open file: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-85/test_transform_boundary_contra0/interim/part.0.parquet
=========================== short test summary info ============================
FAILED tests/test_features.py::test_decline_rate_value - AttributeError: 'Num...
FAILED tests/test_features.py::test_decline_rate_clipped_min - AttributeError...
FAILED tests/test_features.py::test_decline_rate_clipped_max - AttributeError...
FAILED tests/test_features.py::test_decline_rate_within_bounds_unchanged - At...
FAILED tests/test_features.py::test_decline_rate_zero_denominator_no_unclipped_extreme
FAILED tests/test_features.py::test_decline_rate_dtype - AttributeError: 'Num...
FAILED tests/test_features.py::test_apply_features_returns_dask_df - ValueErr...
FAILED tests/test_features.py::test_apply_features_does_not_compute - ValueEr...
FAILED tests/test_features.py::test_apply_features_meta_matches_output - Valu...
FAILED tests/test_features.py::test_features_writes_parquet - ValueError: The...
FAILED tests/test_features.py::test_features_parquet_readable - ValueError: T...
FAILED tests/test_features.py::test_features_expected_columns_present - Value...
FAILED tests/test_features.py::test_features_schema_consistent_across_partitions
FAILED tests/test_features.py::test_features_production_nonnegative - ValueEr...
FAILED tests/test_features.py::test_full_pipeline_ingest_transform_features
FAILED tests/test_pipeline.py::test_full_pipeline_end_to_end_no_exceptions - ...
FAILED tests/test_transform.py::test_transform_writes_parquet - ValueError: T...
FAILED tests/test_transform.py::test_transform_parquet_readable - ValueError:...
FAILED tests/test_transform.py::test_transform_sort_order - ValueError: The c...
FAILED tests/test_transform.py::test_transform_row_count_le_input - ValueErro...
FAILED tests/test_transform.py::test_transform_boundary_contract - ValueError...
======================== 21 failed, 93 passed in 20.28s ========================

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, responses-0.5.1, mock-3.15.1, langsmith-0.7.35, repeat-0.9.4, cov-7.1.0, xdist-3.8.0, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 114 items / 114 deselected / 0 selected
Running teardown with pytest sessionfinish...

=========================== 114 deselected in 3.73s ============================

```

---

## Eval Run at 2026-04-27 19:46:08

**Status:** ❌ FAILED

### Failures:
- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, responses-0.5.1, mock-3.15.1, langsmith-0.7.35, repeat-0.9.4, cov-7.1.0, xdist-3.8.0, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 114 items / 114 deselected / 0 selected
Running teardown with pytest sessionfinish...

=========================== 114 deselected in 5.11s ============================

```

---

## Eval Run at 2026-04-27 19:54:51

**Status:** ❌ FAILED

### Failures:
- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, responses-0.5.1, mock-3.15.1, langsmith-0.7.35, repeat-0.9.4, cov-7.1.0, xdist-3.8.0, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 114 items / 114 deselected / 0 selected
Running teardown with pytest sessionfinish...

=========================== 114 deselected in 4.11s ============================

```

---

## Eval Run at 2026-04-27 22:20:31

**Status:** ✅ PASSED

---
