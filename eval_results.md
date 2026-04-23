
## Eval Run at 2026-04-23 10:27:05

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F821 Undefined name `distributed`
  --> kgs_pipeline/pipeline.py:69:48
   |
69 | def init_scheduler(config: dict[str, Any]) -> "distributed.Client":  # type: ignore[name-defined]
   |                                                ^^^^^^^^^^^
70 |     """Initialize Dask distributed scheduler and return a Client.
   |

F821 Undefined name `distributed`
   --> kgs_pipeline/pipeline.py:106:17
    |
104 |             dashboard_address=f":{dask_cfg.get('dashboard_port', 8787)}",
105 |         )
106 |         client: distributed.Client = Client(cluster)  # type: ignore[name-defined]
    |                 ^^^^^^^^^^^
107 |     else:
108 |         client = Client(scheduler)
    |

F821 Undefined name `pytest_mock`
   --> tests/test_acquire.py:183:65
    |
182 | class TestCachingLogic:
183 |     def test_cached_file_skipped(self, tmp_path: Path, mocker: "pytest_mock.MockerFixture") -> None:  # type: ignore[name-defined]
    |                                                                 ^^^^^^^^^^^
184 |         """A file that already exists with size > 0 is not re-downloaded."""
    |

F841 Local variable `mock_session` is assigned to but never used
   --> tests/test_acquire.py:189:9
    |
187 |         dest.write_text("existing content")
188 |
189 |         mock_session = MagicMock(spec=requests.Session)
    |         ^^^^^^^^^^^^
190 |
191 |         # Simulate cache-hit logic from acquire() — _download_with_retry should not
    |
help: Remove assignment to unused variable `mock_session`

Found 4 errors.
No fixes available (1 hidden fix can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/ingest.py:65: error: Incompatible types in assignment (expression has type "StringDtype[None] | str", variable has type "Int64Dtype | str")  [assignment]
/kgs_pipeline/ingest.py:67: error: Incompatible types in assignment (expression has type "Float64Dtype | str", variable has type "Int64Dtype | str")  [assignment]
/kgs_pipeline/ingest.py:71: error: Incompatible types in assignment (expression has type "CategoricalDtype", variable has type "Int64Dtype | str")  [assignment]
/kgs_pipeline/ingest.py:73: error: Incompatible types in assignment (expression has type "CategoricalDtype", variable has type "Int64Dtype | str")  [assignment]
/kgs_pipeline/ingest.py:179: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/ingest.py:179: note: Possible overload variants:
/kgs_pipeline/ingest.py:179: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/ingest.py:179: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/acquire.py:118: error: Invalid index type "str" for "NavigableString"; expected type "SupportsIndex | slice[SupportsIndex | None, SupportsIndex | None, SupportsIndex | None]"  [index]
/kgs_pipeline/acquire.py:118: note: Error code "index" not covered by "type: ignore" comment
/tests/test_pipeline.py:56: error: Need type annotation for "cfg"  [var-annotated]
/tests/test_pipeline.py:209: error: No return value expected  [return-value]
Found 8 errors in 3 files (checked 14 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
...............FFFF......FFFF...F............F...........F.....FFF...... [ 78%]
....................                                                     [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_________ TestAddCumulativeProduction.test_monotonically_nondecreasing _________
tests/test_features.py:56: in test_monotonically_nondecreasing
    df = _make_well_df(production_values=[50.0, 60.0, 70.0, 80.0])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
________ TestAddCumulativeProduction.test_shut_in_month_flat_cumulative ________
tests/test_features.py:63: in test_shut_in_month_flat_cumulative
    df = _make_well_df(production_values=[100.0, 0.0, 100.0])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
___ TestAddCumulativeProduction.test_first_month_zero_gives_zero_cumulative ____
tests/test_features.py:70: in test_first_month_zero_gives_zero_cumulative
    df = _make_well_df(production_values=[0.0, 100.0, 100.0])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
____________ TestAddCumulativeProduction.test_resumes_after_shut_in ____________
tests/test_features.py:75: in test_resumes_after_shut_in
    df = _make_well_df(production_values=[100.0, 0.0, 50.0])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
_______________ TestAddDeclineRate.test_clipped_below_minus_one ________________
tests/test_features.py:152: in test_clipped_below_minus_one
    df = _make_well_df(production_values=[100.0, 10.0])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
__________________ TestAddDeclineRate.test_clipped_above_ten ___________________
tests/test_features.py:158: in test_clipped_above_ten
    df = _make_well_df(production_values=[1.0, 100.0])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
_____________ TestAddDeclineRate.test_within_bounds_passes_through _____________
tests/test_features.py:164: in test_within_bounds_passes_through
    df = _make_well_df(production_values=[100.0, 80.0])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
___________ TestAddDeclineRate.test_two_consecutive_zeros_no_extreme ___________
tests/test_features.py:169: in test_two_consecutive_zeros_no_extreme
    df = _make_well_df(production_values=[100.0, 0.0, 0.0])
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
_____________ TestAddRollingFeatures.test_lag_1_equals_prior_month _____________
tests/test_features.py:210: in test_lag_1_equals_prior_month
    df = _make_well_df(production_values=vals)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:38: in _make_well_df
    return pd.DataFrame(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:769: in __init__
    mgr = dict_to_mgr(data, index, columns, dtype=dtype, copy=copy)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:460: in dict_to_mgr
    return arrays_to_mgr(arrays, columns, index, dtype=dtype, consolidate=copy)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:113: in arrays_to_mgr
    index = _extract_index(arrays)
            ^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/internals/construction.py:643: in _extract_index
    raise ValueError("All arrays must be of the same length")
E   ValueError: All arrays must be of the same length
___________ TestParseRawFile.test_empty_file_returns_empty_dataframe ___________
tests/test_ingest.py:146: in test_empty_file_returns_empty_dataframe
    df = parse_raw_file(p, dtype_map)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/ingest.py:123: in parse_raw_file
    raw = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/readers.py:873: in read_csv
    return _read(filepath_or_buffer, kwds)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/readers.py:300: in _read
    parser = TextFileReader(filepath_or_buffer, **kwds)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/readers.py:1645: in __init__
    self._engine = self._make_engine(f, self.engine)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/readers.py:1922: in _make_engine
    return mapping[engine](f, **self.options)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/io/parsers/c_parser_wrapper.py:95: in __init__
    self._reader = parsers.TextReader(src, **kwds)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/parsers.pyx:575: in pandas._libs.parsers.TextReader.__cinit__
    ???
E   pandas.errors.EmptyDataError: No columns to parse from file
______________ TestSetupLogging.test_adds_stream_and_file_handler ______________
tests/test_pipeline.py:94: in test_adds_stream_and_file_handler
    assert "StreamHandler" in handler_types
E   AssertionError: assert 'StreamHandler' in ['LogCaptureHandler', 'LogCaptureHandler']
___________ TestMain.test_only_acquire_called_when_stage_is_acquire ____________
tests/test_pipeline.py:189: in test_only_acquire_called_when_stage_is_acquire
    patch("kgs_pipeline.pipeline.load_config", return_value=_full_config(tmp_path)),
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1467: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1437: in get_original
    raise AttributeError(
E   AttributeError: <module 'kgs_pipeline.pipeline' from '/kgs_pipeline/pipeline.py'> does not have the attribute 'load_config'
___________________ TestMain.test_setup_logging_called_first ___________________
tests/test_pipeline.py:212: in test_setup_logging_called_first
    patch("kgs_pipeline.pipeline.load_config", return_value=_full_config(tmp_path)),
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1467: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1437: in get_original
    raise AttributeError(
E   AttributeError: <module 'kgs_pipeline.pipeline' from '/kgs_pipeline/pipeline.py'> does not have the attribute 'load_config'
_______________ TestMain.test_exception_stops_downstream_stages ________________
tests/test_pipeline.py:224: in test_exception_stops_downstream_stages
    patch("kgs_pipeline.pipeline.load_config", return_value=_full_config(tmp_path)),
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1467: in __enter__
    original, local = self.get_original()
                      ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1437: in get_original
    raise AttributeError(
E   AttributeError: <module 'kgs_pipeline.pipeline' from '/kgs_pipeline/pipeline.py'> does not have the attribute 'load_config'
=========================== short test summary info ============================
FAILED tests/test_features.py::TestAddCumulativeProduction::test_monotonically_nondecreasing
FAILED tests/test_features.py::TestAddCumulativeProduction::test_shut_in_month_flat_cumulative
FAILED tests/test_features.py::TestAddCumulativeProduction::test_first_month_zero_gives_zero_cumulative
FAILED tests/test_features.py::TestAddCumulativeProduction::test_resumes_after_shut_in
FAILED tests/test_features.py::TestAddDeclineRate::test_clipped_below_minus_one
FAILED tests/test_features.py::TestAddDeclineRate::test_clipped_above_ten - V...
FAILED tests/test_features.py::TestAddDeclineRate::test_within_bounds_passes_through
FAILED tests/test_features.py::TestAddDeclineRate::test_two_consecutive_zeros_no_extreme
FAILED tests/test_features.py::TestAddRollingFeatures::test_lag_1_equals_prior_month
FAILED tests/test_ingest.py::TestParseRawFile::test_empty_file_returns_empty_dataframe
FAILED tests/test_pipeline.py::TestSetupLogging::test_adds_stream_and_file_handler
FAILED tests/test_pipeline.py::TestMain::test_only_acquire_called_when_stage_is_acquire
FAILED tests/test_pipeline.py::TestMain::test_setup_logging_called_first - At...
FAILED tests/test_pipeline.py::TestMain::test_exception_stops_downstream_stages
14 failed, 78 passed, 13 deselected in 10.74s

```

---

## Eval Run at 2026-04-23 10:34:27

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/ingest.py:185: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "None"  [call-overload]
/kgs_pipeline/ingest.py:185: note: Possible overload variants:
/kgs_pipeline/ingest.py:185: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/ingest.py:185: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/acquire.py:120: error: Incompatible types in assignment (expression has type "str | list[str]", variable has type "str")  [assignment]
Found 2 errors in 2 files (checked 14 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
.........................FFFF............................F.............. [ 78%]
....................                                                     [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_______________ TestAddDeclineRate.test_clipped_below_minus_one ________________
tests/test_features.py:154: in test_clipped_below_minus_one
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:158: in add_decline_rate
    ].transform(_decline)
      ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/generic.py:786: in transform
    return self._transform(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1823: in _transform
    return self._transform_general(func, engine, engine_kwargs, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/generic.py:822: in _transform_general
    res = func(group, *args, **kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:151: in _decline
    rate = np.where(prev == 0.0, np.nan, (prod - prev) / prev)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/missing.pyx:415: in pandas._libs.missing.NAType.__bool__
    ???
E   TypeError: boolean value of NA is ambiguous
__________________ TestAddDeclineRate.test_clipped_above_ten ___________________
tests/test_features.py:160: in test_clipped_above_ten
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:158: in add_decline_rate
    ].transform(_decline)
      ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/generic.py:786: in transform
    return self._transform(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1823: in _transform
    return self._transform_general(func, engine, engine_kwargs, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/generic.py:822: in _transform_general
    res = func(group, *args, **kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:151: in _decline
    rate = np.where(prev == 0.0, np.nan, (prod - prev) / prev)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/missing.pyx:415: in pandas._libs.missing.NAType.__bool__
    ???
E   TypeError: boolean value of NA is ambiguous
_____________ TestAddDeclineRate.test_within_bounds_passes_through _____________
tests/test_features.py:166: in test_within_bounds_passes_through
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:158: in add_decline_rate
    ].transform(_decline)
      ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/generic.py:786: in transform
    return self._transform(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1823: in _transform
    return self._transform_general(func, engine, engine_kwargs, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/generic.py:822: in _transform_general
    res = func(group, *args, **kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:151: in _decline
    rate = np.where(prev == 0.0, np.nan, (prod - prev) / prev)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/missing.pyx:415: in pandas._libs.missing.NAType.__bool__
    ???
E   TypeError: boolean value of NA is ambiguous
___________ TestAddDeclineRate.test_two_consecutive_zeros_no_extreme ___________
tests/test_features.py:171: in test_two_consecutive_zeros_no_extreme
    result = add_decline_rate(df)
             ^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:158: in add_decline_rate
    ].transform(_decline)
      ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/generic.py:786: in transform
    return self._transform(
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1823: in _transform
    return self._transform_general(func, engine, engine_kwargs, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/generic.py:822: in _transform_general
    res = func(group, *args, **kwargs)
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:151: in _decline
    rate = np.where(prev == 0.0, np.nan, (prod - prev) / prev)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/missing.pyx:415: in pandas._libs.missing.NAType.__bool__
    ???
E   TypeError: boolean value of NA is ambiguous
______________ TestSetupLogging.test_adds_stream_and_file_handler ______________
tests/test_pipeline.py:101: in test_adds_stream_and_file_handler
    assert "FileHandler" in handler_types
E   AssertionError: assert 'FileHandler' in ['LogCaptureHandler', 'LogCaptureHandler']
=========================== short test summary info ============================
FAILED tests/test_features.py::TestAddDeclineRate::test_clipped_below_minus_one
FAILED tests/test_features.py::TestAddDeclineRate::test_clipped_above_ten - T...
FAILED tests/test_features.py::TestAddDeclineRate::test_within_bounds_passes_through
FAILED tests/test_features.py::TestAddDeclineRate::test_two_consecutive_zeros_no_extreme
FAILED tests/test_pipeline.py::TestSetupLogging::test_adds_stream_and_file_handler
5 failed, 87 passed, 13 deselected in 9.66s

```

---

## Eval Run at 2026-04-23 10:37:29

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
.........................F...............................F.............. [ 78%]
....................                                                     [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_______________ TestAddDeclineRate.test_clipped_below_minus_one ________________
tests/test_features.py:155: in test_clipped_below_minus_one
    assert result.iloc[1]["decline_rate"] == pytest.approx(-1.0)
E   assert np.float64(-0.9) == -1.0 ± 1.0e-06
E     
E     comparison failed
E     Obtained: -0.9
E     Expected: -1.0 ± 1.0e-06
______________ TestSetupLogging.test_adds_stream_and_file_handler ______________
tests/test_pipeline.py:103: in test_adds_stream_and_file_handler
    assert Path(cfg["logging"]["log_file"]).exists()
E   AssertionError: assert False
E    +  where False = exists()
E    +    where exists = PosixPath('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-5/test_adds_stream_and_file_hand0/logs/test.log').exists
E    +      where PosixPath('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-5/test_adds_stream_and_file_hand0/logs/test.log') = Path('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-5/test_adds_stream_and_file_hand0/logs/test.log')
=========================== short test summary info ============================
FAILED tests/test_features.py::TestAddDeclineRate::test_clipped_below_minus_one
FAILED tests/test_pipeline.py::TestSetupLogging::test_adds_stream_and_file_handler
2 failed, 90 passed, 13 deselected in 8.03s

```

---

## Eval Run at 2026-04-23 10:39:25

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
.........................................................F.............. [ 78%]
....................                                                     [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
______________ TestSetupLogging.test_adds_stream_and_file_handler ______________
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:960: in assert_called_once_with
    raise AssertionError(msg)
E   AssertionError: Expected 'FileHandler' to be called once. Called 0 times.

During handling of the above exception, another exception occurred:
tests/test_pipeline.py:99: in test_adds_stream_and_file_handler
    mock_fh.assert_called_once_with(cfg["logging"]["log_file"], encoding="utf-8")
E   AssertionError: Expected 'FileHandler' to be called once. Called 0 times.
=========================== short test summary info ============================
FAILED tests/test_pipeline.py::TestSetupLogging::test_adds_stream_and_file_handler
1 failed, 91 passed, 13 deselected in 8.05s

```

---

## Eval Run at 2026-04-23 10:41:11

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
.........................................................F.............. [ 78%]
....................                                                     [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
______________ TestSetupLogging.test_adds_stream_and_file_handler ______________
tests/test_pipeline.py:98: in test_adds_stream_and_file_handler
    setup_logging(cfg)
kgs_pipeline/pipeline.py:53: in setup_logging
    if not any(isinstance(h, logging.FileHandler) for h in root.handlers):
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/pipeline.py:53: in <genexpr>
    if not any(isinstance(h, logging.FileHandler) for h in root.handlers):
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: isinstance() arg 2 must be a type, a tuple of types, or a union
=========================== short test summary info ============================
FAILED tests/test_pipeline.py::TestSetupLogging::test_adds_stream_and_file_handler
1 failed, 91 passed, 13 deselected in 8.93s

```

---

## Eval Run at 2026-04-23 10:42:30

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
.........................................................F.............. [ 78%]
....................                                                     [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
______________ TestSetupLogging.test_adds_stream_and_file_handler ______________
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:949: in assert_called_with
    raise AssertionError(_error_message()) from cause
E   AssertionError: expected call not found.
E   Expected: FileHandler('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log', encoding='utf-8')
E     Actual: FileHandler(PosixPath('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log'), encoding='utf-8')

During handling of the above exception, another exception occurred:
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:961: in assert_called_once_with
    return self.assert_called_with(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   AssertionError: expected call not found.
E   Expected: FileHandler('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log', encoding='utf-8')
E     Actual: FileHandler(PosixPath('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log'), encoding='utf-8')
E   
E   pytest introspection follows:
E   
E   Args:
E   assert (PosixPath('/...s/test.log'),) == ('/private/va...gs/test.log',)
E     
E     At index 0 diff: PosixPath('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log') != '/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log'
E     Use -v to get more diff

During handling of the above exception, another exception occurred:
tests/test_pipeline.py:99: in test_adds_stream_and_file_handler
    mock_fh.assert_called_once_with(cfg["logging"]["log_file"], encoding="utf-8")
E   AssertionError: expected call not found.
E   Expected: FileHandler('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log', encoding='utf-8')
E     Actual: FileHandler(PosixPath('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log'), encoding='utf-8')
E   
E   pytest introspection follows:
E   
E   Args:
E   assert (PosixPath('/...s/test.log'),) == ('/private/va...gs/test.log',)
E     
E     At index 0 diff: PosixPath('/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log') != '/private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-8/test_adds_stream_and_file_hand0/logs/test.log'
E     Use -v to get more diff
=========================== short test summary info ============================
FAILED tests/test_pipeline.py::TestSetupLogging::test_adds_stream_and_file_handler
1 failed, 91 passed, 13 deselected in 8.51s

```

---

## Eval Run at 2026-04-23 10:43:46

**Status:** ✅ PASSED

---
