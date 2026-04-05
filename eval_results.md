
## Eval Run at 2026-04-01 10:46:09

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F841 Local variable `enc_ddf` is assigned to but never used
   --> tests/test_features.py:599:5
    |
597 |     )
598 |     ddf = dd.from_pandas(df, npartitions=1)
599 |     enc_ddf = encode_categoricals(ddf)
    |     ^^^^^^^
600 |
601 |     # Introduce unseen value in a new partition
    |
help: Remove assignment to unused variable `enc_ddf`

Found 1 error.
No fixes available (1 hidden fix can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/transform.py:10: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/ingest.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:12: error: Cannot find implementation or library stub for module named "sklearn.preprocessing"  [import-not-found]
/kgs_pipeline/acquire.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/acquire.py:11: note: Hint: "python3 -m pip install pandas-stubs"
/kgs_pipeline/acquire.py:11: note: (or run "mypy --install-types" to install all missing stub packages)
/kgs_pipeline/acquire.py:11: note: See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
/kgs_pipeline/acquire.py:12: error: Library stubs not installed for "requests"  [import-untyped]
/kgs_pipeline/acquire.py:12: note: Hint: "python3 -m pip install types-requests"
/tests/test_transform.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_ingest.py:6: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_features.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_acquire.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
Found 10 errors in 8 files (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
Running teardown with pytest sessionfinish...

==================================== ERRORS ====================================
___________________ ERROR collecting tests/test_features.py ____________________
ImportError while importing test module '/tests/test_features.py'.
Hint: make sure your test modules/packages have valid Python names.
Traceback:
../../../miniconda3/envs/dapi/lib/python3.12/importlib/__init__.py:90: in import_module
    return _bootstrap._gcd_import(name[level:], package, level)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_features.py:10: in <module>
    from kgs_pipeline.features import (
kgs_pipeline/features.py:12: in <module>
    from sklearn.preprocessing import LabelEncoder
E   ModuleNotFoundError: No module named 'sklearn'
___________________ ERROR collecting tests/test_transform.py ___________________
tests/test_transform.py:10: in <module>
    from kgs_pipeline.transform import (
kgs_pipeline/transform.py:226: in <module>
    def _parse_month_year(val: object) -> pd.Timestamp | pd.NaT:  # type: ignore[valid-type]
                                          ^^^^^^^^^^^^^^^^^^^^^
E   TypeError: unsupported operand type(s) for |: 'type' and 'NaTType'
=========================== short test summary info ============================
ERROR tests/test_features.py
ERROR tests/test_transform.py - TypeError: unsupported operand type(s) for |:...
!!!!!!!!!!!!!!!!!!! Interrupted: 2 errors during collection !!!!!!!!!!!!!!!!!!!!
2 errors in 5.11s

```

---

## Eval Run at 2026-04-01 10:54:02

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/transform.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/ingest.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:14: error: Cannot find implementation or library stub for module named "sklearn.preprocessing"  [import-not-found]
/kgs_pipeline/acquire.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/acquire.py:11: note: Hint: "python3 -m pip install pandas-stubs"
/kgs_pipeline/acquire.py:11: note: (or run "mypy --install-types" to install all missing stub packages)
/kgs_pipeline/acquire.py:11: note: See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
/kgs_pipeline/acquire.py:12: error: Library stubs not installed for "requests"  [import-untyped]
/kgs_pipeline/acquire.py:12: note: Hint: "python3 -m pip install types-requests"
/tests/test_transform.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_ingest.py:6: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_features.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_acquire.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
Found 10 errors in 8 files (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
...............................F..................................FFFF.. [ 45%]
.....FFFFFFFFF.......FF.................................F............... [ 91%]
............s.                                                           [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_____________________ test_compute_cumulative_shutin_flat ______________________
tests/test_features.py:224: in test_compute_cumulative_shutin_flat
    df["oil_bbl"] = [100.0, 150.0, 0.0, 200.0]
    ^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4672: in __setitem__
    self._set_item(key, value)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4872: in _set_item
    value, refs = self._sanitize_column(value)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:5742: in _sanitize_column
    com.require_length_match(value, self.index)
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/common.py:601: in require_length_match
    raise ValueError(
E   ValueError: Length of values (4) does not match length of index (3)
_________________ test_encode_categoricals_product_two_values __________________
tests/test_features.py:557: in test_encode_categoricals_product_two_values
    result = encode_categoricals(ddf).compute()
             ^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:379: in encode_categoricals
    raise ImportError("scikit-learn is required. Install with: pip install scikit-learn")
E   ImportError: scikit-learn is required. Install with: pip install scikit-learn
__________________ test_encode_categoricals_county_int_dtype ___________________
tests/test_features.py:571: in test_encode_categoricals_county_int_dtype
    result = encode_categoricals(ddf).compute()
             ^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:379: in encode_categoricals
    raise ImportError("scikit-learn is required. Install with: pip install scikit-learn")
E   ImportError: scikit-learn is required. Install with: pip install scikit-learn
________________ test_encode_categoricals_original_col_retained ________________
tests/test_features.py:585: in test_encode_categoricals_original_col_retained
    result = encode_categoricals(ddf).compute()
             ^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:379: in encode_categoricals
    raise ImportError("scikit-learn is required. Install with: pip install scikit-learn")
E   ImportError: scikit-learn is required. Install with: pip install scikit-learn
____________________ test_encode_categoricals_unseen_value _____________________
tests/test_features.py:599: in test_encode_categoricals_unseen_value
    encode_categoricals(ddf)
kgs_pipeline/features.py:379: in encode_categoricals
    raise ImportError("scikit-learn is required. Install with: pip install scikit-learn")
E   ImportError: scikit-learn is required. Install with: pip install scikit-learn
______________________ test_run_features_returns_dask_df _______________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:714: in test_run_features_returns_dask_df
    ddf = run_features(str(clean), str(out))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
_____________________ test_run_features_all_schema_columns _____________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:721: in test_run_features_all_schema_columns
    ddf = run_features(str(clean), str(out))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
____________________ test_run_features_no_negative_oil_bbl _____________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:730: in test_run_features_no_negative_oil_bbl
    ddf = run_features(str(clean), str(out))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
_____________________ test_run_features_cum_oil_monotonic ______________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:738: in test_run_features_cum_oil_monotonic
    ddf = run_features(str(clean), str(out))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
______________________ test_run_features_parquet_readable ______________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:749: in test_run_features_parquet_readable
    run_features(str(clean), str(out))
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
_____________ test_run_features_schema_stability_across_partitions _____________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:757: in test_run_features_schema_stability_across_partitions
    run_features(str(clean), str(out))
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
_____________________ test_run_features_output_file_count ______________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:768: in test_run_features_output_file_count
    run_features(str(clean), str(out))
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
_________________________ test_feature_column_presence _________________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:782: in test_feature_column_presence
    ddf = run_features(str(clean), str(out))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
_________________________ test_cumulative_monotonicity _________________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:792: in test_cumulative_monotonicity
    ddf = run_features(str(clean), str(out))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
___________________ test_schema_stability_across_partitions ____________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:864: in test_schema_stability_across_partitions
    run_features(str(clean), str(out))
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
_____________________________ test_lazy_evaluation _____________________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:877: in test_lazy_evaluation
    ddf = run_features(str(clean), str(out))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:493: in run_features
    .apply(compute_per_lease_features)
     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1650: in apply
    return self._python_apply_general(f, self._obj_with_exclusions)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/groupby.py:1687: in _python_apply_general
    values, mutated = self._grouper.apply_groupwise(f, data)
                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/groupby/ops.py:1035: in apply_groupwise
    res = f(group)
          ^^^^^^^^
kgs_pipeline/features.py:425: in compute_per_lease_features
    lease_id = df["LEASE_KID"].iloc[0] if len(df) > 0 else "unknown"
               ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
______________________ test_remove_duplicates_removes_one ______________________
tests/test_transform.py:241: in test_remove_duplicates_removes_one
    assert len(result) == 4
E   assert 3 == 4
E    +  where 3 = len(  LEASE_KID    LEASE DOR_CODE API_NUMBER  ... PRODUCT WELLS PRODUCTION source_file\n0        L0  Lease 0      123      ...0    test.txt\n2        L2  Lease 2      123      API-2  ...       O   2.0      300.0    test.txt\n\n[3 rows x 21 columns])
Test tests/test_transform.py::test_sort_stability was skipped. Reason: 
('/tests/test_transform.py', 566, 
'Skipped: Not enough partitions for sort stability test')
=========================== short test summary info ============================
FAILED tests/test_features.py::test_compute_cumulative_shutin_flat - ValueErr...
FAILED tests/test_features.py::test_encode_categoricals_product_two_values - ...
FAILED tests/test_features.py::test_encode_categoricals_county_int_dtype - Im...
FAILED tests/test_features.py::test_encode_categoricals_original_col_retained
FAILED tests/test_features.py::test_encode_categoricals_unseen_value - Import...
FAILED tests/test_features.py::test_run_features_returns_dask_df - KeyError: ...
FAILED tests/test_features.py::test_run_features_all_schema_columns - KeyErro...
FAILED tests/test_features.py::test_run_features_no_negative_oil_bbl - KeyErr...
FAILED tests/test_features.py::test_run_features_cum_oil_monotonic - KeyError...
FAILED tests/test_features.py::test_run_features_parquet_readable - KeyError:...
FAILED tests/test_features.py::test_run_features_schema_stability_across_partitions
FAILED tests/test_features.py::test_run_features_output_file_count - KeyError...
FAILED tests/test_features.py::test_feature_column_presence - KeyError: 'LEAS...
FAILED tests/test_features.py::test_cumulative_monotonicity - KeyError: 'LEAS...
FAILED tests/test_features.py::test_schema_stability_across_partitions - KeyE...
FAILED tests/test_features.py::test_lazy_evaluation - KeyError: 'LEASE_KID'
FAILED tests/test_transform.py::test_remove_duplicates_removes_one - assert 3...
17 failed, 140 passed, 1 skipped in 18.50s

```

---

## Eval Run at 2026-04-01 11:00:33

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/transform.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/ingest.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:14: error: Cannot find implementation or library stub for module named "sklearn.preprocessing"  [import-not-found]
/kgs_pipeline/acquire.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/acquire.py:11: note: Hint: "python3 -m pip install pandas-stubs"
/kgs_pipeline/acquire.py:11: note: (or run "mypy --install-types" to install all missing stub packages)
/kgs_pipeline/acquire.py:11: note: See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
/kgs_pipeline/acquire.py:12: error: Library stubs not installed for "requests"  [import-untyped]
/kgs_pipeline/acquire.py:12: note: Hint: "python3 -m pip install types-requests"
/tests/test_transform.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_ingest.py:6: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_features.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_acquire.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
Found 10 errors in 8 files (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
ERROR: /pyproject.toml: Invalid initial character for a key part (at line 42, column 19)


```

---

## Eval Run at 2026-04-01 11:05:17

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/transform.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/ingest.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:14: error: Cannot find implementation or library stub for module named "sklearn.preprocessing"  [import-not-found]
/kgs_pipeline/acquire.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/acquire.py:11: note: Hint: "python3 -m pip install pandas-stubs"
/kgs_pipeline/acquire.py:11: note: (or run "mypy --install-types" to install all missing stub packages)
/kgs_pipeline/acquire.py:11: note: See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
/kgs_pipeline/acquire.py:12: error: Library stubs not installed for "requests"  [import-untyped]
/kgs_pipeline/acquire.py:12: note: Hint: "python3 -m pip install types-requests"
/tests/test_transform.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_ingest.py:6: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_features.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_acquire.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
Found 10 errors in 8 files (checked 13 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
........................................................................ [ 45%]
......F.F...FF..........................................F............... [ 91%]
............s.                                                           [100%]Running teardown with pytest sessionfinish...

=================================== FAILURES ===================================
_____________________ test_run_features_all_schema_columns _____________________
tests/test_features.py:723: in test_run_features_all_schema_columns
    assert col in cols, f"Missing schema column: {col}"
E   AssertionError: Missing schema column: LEASE_KID
E   assert 'LEASE_KID' in ['production_date', 'oil_bbl', 'COUNTY', 'PRODUCING_ZONE', 'OPERATOR', 'LEASE', ...]
----------------------------- Captured stderr call -----------------------------
2026-04-01 11:05:03 | kgs_pipeline.features | INFO | Features stage complete. Output: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-1/test_run_features_all_schema_c0/features
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.features:features.py:516 Features stage complete. Output: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-1/test_run_features_all_schema_c0/features
_____________________ test_run_features_cum_oil_monotonic ______________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:739: in test_run_features_cum_oil_monotonic
    for lease_id in result["LEASE_KID"].unique():
                    ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
----------------------------- Captured stderr call -----------------------------
2026-04-01 11:05:04 | kgs_pipeline.features | INFO | Features stage complete. Output: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-1/test_run_features_cum_oil_mono0/features
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.features:features.py:516 Features stage complete. Output: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-1/test_run_features_cum_oil_mono0/features
_________________________ test_feature_column_presence _________________________
tests/test_features.py:784: in test_feature_column_presence
    assert col in cols, f"[TR-19] Missing column: {col}"
E   AssertionError: [TR-19] Missing column: LEASE_KID
E   assert 'LEASE_KID' in ['production_date', 'oil_bbl', 'COUNTY', 'PRODUCING_ZONE', 'OPERATOR', 'LEASE', ...]
----------------------------- Captured stderr call -----------------------------
2026-04-01 11:05:06 | kgs_pipeline.features | INFO | Features stage complete. Output: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-1/test_feature_column_presence0/features_presence
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.features:features.py:516 Features stage complete. Output: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-1/test_feature_column_presence0/features_presence
_________________________ test_cumulative_monotonicity _________________________
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3641: in get_loc
    return self._engine.get_loc(casted_key)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pandas/_libs/index.pyx:168: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/index.pyx:197: in pandas._libs.index.IndexEngine.get_loc
    ???
pandas/_libs/hashtable_class_helper.pxi:7668: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
pandas/_libs/hashtable_class_helper.pxi:7676: in pandas._libs.hashtable.PyObjectHashTable.get_item
    ???
E   KeyError: 'LEASE_KID'

The above exception was the direct cause of the following exception:
tests/test_features.py:793: in test_cumulative_monotonicity
    for lid in result["LEASE_KID"].unique():
               ^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/frame.py:4378: in __getitem__
    indexer = self.columns.get_loc(key)
              ^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/core/indexes/base.py:3648: in get_loc
    raise KeyError(key) from err
E   KeyError: 'LEASE_KID'
----------------------------- Captured stderr call -----------------------------
2026-04-01 11:05:06 | kgs_pipeline.features | INFO | Features stage complete. Output: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-1/test_cumulative_monotonicity0/features_mono
------------------------------ Captured log call -------------------------------
INFO     kgs_pipeline.features:features.py:516 Features stage complete. Output: /private/var/folders/9m/tncsl2m94d94czq217x9sttr0000gn/T/pytest-of-sirisurab/pytest-1/test_cumulative_monotonicity0/features_mono
______________________ test_remove_duplicates_removes_one ______________________
tests/test_transform.py:241: in test_remove_duplicates_removes_one
    assert len(result) == 4
E   assert 3 == 4
E    +  where 3 = len(  LEASE_KID    LEASE DOR_CODE API_NUMBER  ... PRODUCT WELLS PRODUCTION source_file\n0        L0  Lease 0      123      ...0    test.txt\n2        L2  Lease 2      123      API-2  ...       O   2.0      300.0    test.txt\n\n[3 rows x 21 columns])
Test tests/test_transform.py::test_sort_stability was skipped. Reason: 
('/tests/test_transform.py', 566, 
'Skipped: Not enough partitions for sort stability test')
=========================== short test summary info ============================
FAILED tests/test_features.py::test_run_features_all_schema_columns - Asserti...
FAILED tests/test_features.py::test_run_features_cum_oil_monotonic - KeyError...
FAILED tests/test_features.py::test_feature_column_presence - AssertionError:...
FAILED tests/test_features.py::test_cumulative_monotonicity - KeyError: 'LEAS...
FAILED tests/test_transform.py::test_remove_duplicates_removes_one - assert 3...
5 failed, 152 passed, 1 skipped in 16.02s

```

---

## Eval Run at 2026-04-01 11:12:00

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/transform.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/ingest.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/features.py:14: error: Cannot find implementation or library stub for module named "sklearn.preprocessing"  [import-not-found]
/kgs_pipeline/acquire.py:11: error: Library stubs not installed for "pandas"  [import-untyped]
/kgs_pipeline/acquire.py:11: note: Hint: "python3 -m pip install pandas-stubs"
/kgs_pipeline/acquire.py:11: note: (or run "mypy --install-types" to install all missing stub packages)
/kgs_pipeline/acquire.py:11: note: See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
/kgs_pipeline/acquire.py:12: error: Library stubs not installed for "requests"  [import-untyped]
/kgs_pipeline/acquire.py:12: note: Hint: "python3 -m pip install types-requests"
/tests/test_transform.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_ingest.py:6: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_features.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
/tests/test_acquire.py:7: error: Library stubs not installed for "pandas"  [import-untyped]
Found 10 errors in 8 files (checked 13 source files)

```

---

## Eval Run at 2026-04-01 11:16:03

**Status:** ✅ PASSED

---
