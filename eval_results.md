
## Eval Run at 2026-04-23 15:11:59

**Status:** ❌ FAILED

### Failures:
- **Linting:**
```
Linting failed. Fix these errors:
F841 Local variable `all_cols` is assigned to but never used
   --> tests/test_features.py:595:5
    |
593 |         "water_bbl_lag_1m",
594 |     ]
595 |     all_cols = list(df.columns) + [df.index.name] if df.index.name else list(df.columns)
    |     ^^^^^^^^
596 |     for col in tr19_cols:
597 |         assert col in df.columns or col in (df.index.name,), f"Missing TR-19 column: {col}"
    |
help: Remove assignment to unused variable `all_cols`

F841 Local variable `transform_cfg` is assigned to but never used
   --> tests/test_transform.py:133:5
    |
131 |     ingest(cfg)
132 |
133 |     transform_cfg = {
    |     ^^^^^^^^^^^^^
134 |         "transform": cfg["transform"],
135 |     }
    |
help: Remove assignment to unused variable `transform_cfg`

Found 2 errors.
No fixes available (2 hidden fixes can be enabled with the `--unsafe-fixes` option).

```

- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/transform.py:126: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/transform.py:126: note: Possible overload variants:
/kgs_pipeline/transform.py:126: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/transform.py:126: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/transform.py:132: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/transform.py:132: note: Possible overload variants:
/kgs_pipeline/transform.py:132: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/transform.py:132: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/transform.py:139: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/transform.py:139: note: Possible overload variants:
/kgs_pipeline/transform.py:139: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/transform.py:139: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/ingest.py:254: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "None"  [call-overload]
/kgs_pipeline/ingest.py:254: note: Possible overload variants:
/kgs_pipeline/ingest.py:254: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/ingest.py:254: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/features.py:281: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/features.py:281: note: Possible overload variants:
/kgs_pipeline/features.py:281: note:     def where(self, cond: Series[float] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float]], Series[builtins.bool]] | Callable[[float], builtins.bool], other: float | Series[float] | Callable[..., float | Series[float]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/features.py:281: note:     def where(self, cond: Series[float] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float]], Series[builtins.bool]] | Callable[[float], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[float]
/kgs_pipeline/features.py:281: note:     def where(self, cond: Series[float | Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float | Any]], Series[builtins.bool]] | Callable[[float | Any], builtins.bool], other: float | Any | Series[float | Any] | Callable[..., float | Any | Series[float | Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/features.py:281: note:     def where(self, cond: Series[float | Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float | Any]], Series[builtins.bool]] | Callable[[float | Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[float | Any]
/kgs_pipeline/features.py:281: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/features.py:281: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/features.py:292: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/features.py:292: note: Possible overload variants:
/kgs_pipeline/features.py:292: note:     def where(self, cond: Series[float] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float]], Series[builtins.bool]] | Callable[[float], builtins.bool], other: float | Series[float] | Callable[..., float | Series[float]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/features.py:292: note:     def where(self, cond: Series[float] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float]], Series[builtins.bool]] | Callable[[float], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[float]
/kgs_pipeline/features.py:292: note:     def where(self, cond: Series[float | Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float | Any]], Series[builtins.bool]] | Callable[[float | Any], builtins.bool], other: float | Any | Series[float | Any] | Callable[..., float | Any | Series[float | Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/features.py:292: note:     def where(self, cond: Series[float | Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float | Any]], Series[builtins.bool]] | Callable[[float | Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[float | Any]
/kgs_pipeline/features.py:292: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/features.py:292: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/kgs_pipeline/features.py:302: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "NAType"  [call-overload]
/kgs_pipeline/features.py:302: note: Possible overload variants:
/kgs_pipeline/features.py:302: note:     def where(self, cond: Series[float] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float]], Series[builtins.bool]] | Callable[[float], builtins.bool], other: float | Series[float] | Callable[..., float | Series[float]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/kgs_pipeline/features.py:302: note:     def where(self, cond: Series[float] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[float]], Series[builtins.bool]] | Callable[[float], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[float]
/kgs_pipeline/acquire.py:196: error: Invalid index type "str" for "NavigableString"; expected type "SupportsIndex | slice[SupportsIndex | None, SupportsIndex | None, SupportsIndex | None]"  [index]
/kgs_pipeline/acquire.py:198: error: Item "list[str]" of "str | list[str]" has no attribute "startswith"  [union-attr]
/tests/test_transform.py:80: error: No overload variant of "where" of "Series" matches argument types "Series[bool]", "None"  [call-overload]
/tests/test_transform.py:80: note: Possible overload variants:
/tests/test_transform.py:80: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: Any | Series[Any] | Callable[..., Any | Series[Any]] = ..., *, inplace: Literal[True], axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> None
/tests/test_transform.py:80: note:     def where(self, cond: Series[Any] | Series[builtins.bool] | ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | Callable[[Series[Any]], Series[builtins.bool]] | Callable[[Any], builtins.bool], other: str | bytes | date | datetime | timedelta | <13 more items> = ..., *, inplace: Literal[False] = ..., axis: Literal['index', 0] | None = ..., level: Hashable | None = ...) -> Series[Any]
/tests/test_features.py:138: error: No overload variant of "array" matches argument types "Series[Any]", "str"  [call-overload]
/tests/test_features.py:138: note: Possible overload variants:
/tests/test_features.py:138: note:     def array(data: Sequence[Just[float]], dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/tests/test_features.py:138: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void], copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:138: note:     def array(data: Sequence[NAType | None], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:138: note:     def array(data: Sequence[Timedelta] | Series[Timedelta] | TimedeltaArray | TimedeltaIndex | ndarray[tuple[int], dtype[timedelta64[timedelta | int | None]]], dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]'] | Literal['duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]'] | None = ..., copy: bool = ...) -> TimedeltaArray
/tests/test_features.py:138: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | Just[float] | NAType | None], dtype: BooleanDtype | Literal['boolean'], copy: bool = ...) -> BooleanArray
/tests/test_features.py:138: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | NAType | None], dtype: None = ..., copy: bool = ...) -> BooleanArray
/tests/test_features.py:138: note:     def array(data: ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | BooleanArray, dtype: BooleanDtype | Literal['boolean'] | None = ..., copy: bool = ...) -> BooleanArray
/tests/test_features.py:138: note:     def array(data: Sequence[float | integer[Any] | NAType | None], dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype, copy: bool = ...) -> IntegerArray
/tests/test_features.py:138: note:     def array(data: Sequence[int | integer[Any] | NAType | None], dtype: None = ..., copy: bool = ...) -> IntegerArray
/tests/test_features.py:138: note:     def array(data: ndarray[tuple[Any, ...], dtype[integer[Any]]] | IntegerArray, dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype | None = ..., copy: bool = ...) -> IntegerArray
/tests/test_features.py:138: note:     def array(data: Sequence[float | floating[Any] | NAType | None] | ndarray[tuple[Any, ...], dtype[floating[Any]]] | FloatingArray, dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/tests/test_features.py:138: note:     def array(data: tuple[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None, ...] | MutableSequence[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[Any]] | DatetimeArray, dtype: DatetimeTZDtype | Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | dtype[datetime64[date | int | None]] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]'], copy: bool = ...) -> DatetimeArray
/tests/test_features.py:138: note:     def array(data: Sequence[datetime | NaTType | None] | Sequence[datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[datetime64[date | int | None]]] | DatetimeArray, dtype: None = ..., copy: bool = ...) -> DatetimeArray
/tests/test_features.py:138: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Never], copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:138: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['pyarrow']] | Literal['string[pyarrow]'], copy: bool = ...) -> ArrowStringArray
/tests/test_features.py:138: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['python']] | Literal['string[python]'], copy: bool = ...) -> StringArray
/tests/test_features.py:138: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[None] | Literal['string'], copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:138: note:     def array(data: tuple[str | str_ | NAType | None, ...] | MutableSequence[str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[str_]] | BaseStringArray[None], dtype: None = ..., copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:138: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:138: note:     def array(data: ndarray[tuple[Any, ...], dtype[Any]] | NumpyExtensionArray | RangeIndex, dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:139: error: No overload variant of "array" matches argument types "Series[Any]", "str"  [call-overload]
/tests/test_features.py:139: note: Possible overload variants:
/tests/test_features.py:139: note:     def array(data: Sequence[Just[float]], dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/tests/test_features.py:139: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void], copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:139: note:     def array(data: Sequence[NAType | None], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:139: note:     def array(data: Sequence[Timedelta] | Series[Timedelta] | TimedeltaArray | TimedeltaIndex | ndarray[tuple[int], dtype[timedelta64[timedelta | int | None]]], dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]'] | Literal['duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]'] | None = ..., copy: bool = ...) -> TimedeltaArray
/tests/test_features.py:139: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | Just[float] | NAType | None], dtype: BooleanDtype | Literal['boolean'], copy: bool = ...) -> BooleanArray
/tests/test_features.py:139: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | NAType | None], dtype: None = ..., copy: bool = ...) -> BooleanArray
/tests/test_features.py:139: note:     def array(data: ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | BooleanArray, dtype: BooleanDtype | Literal['boolean'] | None = ..., copy: bool = ...) -> BooleanArray
/tests/test_features.py:139: note:     def array(data: Sequence[float | integer[Any] | NAType | None], dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype, copy: bool = ...) -> IntegerArray
/tests/test_features.py:139: note:     def array(data: Sequence[int | integer[Any] | NAType | None], dtype: None = ..., copy: bool = ...) -> IntegerArray
/tests/test_features.py:139: note:     def array(data: ndarray[tuple[Any, ...], dtype[integer[Any]]] | IntegerArray, dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype | None = ..., copy: bool = ...) -> IntegerArray
/tests/test_features.py:139: note:     def array(data: Sequence[float | floating[Any] | NAType | None] | ndarray[tuple[Any, ...], dtype[floating[Any]]] | FloatingArray, dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/tests/test_features.py:139: note:     def array(data: tuple[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None, ...] | MutableSequence[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[Any]] | DatetimeArray, dtype: DatetimeTZDtype | Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | dtype[datetime64[date | int | None]] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]'], copy: bool = ...) -> DatetimeArray
/tests/test_features.py:139: note:     def array(data: Sequence[datetime | NaTType | None] | Sequence[datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[datetime64[date | int | None]]] | DatetimeArray, dtype: None = ..., copy: bool = ...) -> DatetimeArray
/tests/test_features.py:139: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Never], copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:139: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['pyarrow']] | Literal['string[pyarrow]'], copy: bool = ...) -> ArrowStringArray
/tests/test_features.py:139: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['python']] | Literal['string[python]'], copy: bool = ...) -> StringArray
/tests/test_features.py:139: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[None] | Literal['string'], copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:139: note:     def array(data: tuple[str | str_ | NAType | None, ...] | MutableSequence[str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[str_]] | BaseStringArray[None], dtype: None = ..., copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:139: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:139: note:     def array(data: ndarray[tuple[Any, ...], dtype[Any]] | NumpyExtensionArray | RangeIndex, dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:140: error: No overload variant of "array" matches argument types "Series[Any]", "str"  [call-overload]
/tests/test_features.py:140: note: Possible overload variants:
/tests/test_features.py:140: note:     def array(data: Sequence[Just[float]], dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/tests/test_features.py:140: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void], copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:140: note:     def array(data: Sequence[NAType | None], dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:140: note:     def array(data: Sequence[Timedelta] | Series[Timedelta] | TimedeltaArray | TimedeltaIndex | ndarray[tuple[int], dtype[timedelta64[timedelta | int | None]]], dtype: Literal['timedelta64[s]', 'timedelta64[ms]', 'timedelta64[us]', 'timedelta64[ns]', 'm8[s]', 'm8[ms]', 'm8[us]', 'm8[ns]', '<m8[s]', '<m8[ms]', '<m8[us]', '<m8[ns]'] | Literal['duration[s][pyarrow]', 'duration[ms][pyarrow]', 'duration[us][pyarrow]', 'duration[ns][pyarrow]'] | None = ..., copy: bool = ...) -> TimedeltaArray
/tests/test_features.py:140: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | Just[float] | NAType | None], dtype: BooleanDtype | Literal['boolean'], copy: bool = ...) -> BooleanArray
/tests/test_features.py:140: note:     def array(data: Sequence[builtins.bool | numpy.bool[builtins.bool] | NAType | None], dtype: None = ..., copy: bool = ...) -> BooleanArray
/tests/test_features.py:140: note:     def array(data: ndarray[tuple[Any, ...], dtype[numpy.bool[builtins.bool]]] | BooleanArray, dtype: BooleanDtype | Literal['boolean'] | None = ..., copy: bool = ...) -> BooleanArray
/tests/test_features.py:140: note:     def array(data: Sequence[float | integer[Any] | NAType | None], dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype, copy: bool = ...) -> IntegerArray
/tests/test_features.py:140: note:     def array(data: Sequence[int | integer[Any] | NAType | None], dtype: None = ..., copy: bool = ...) -> IntegerArray
/tests/test_features.py:140: note:     def array(data: ndarray[tuple[Any, ...], dtype[integer[Any]]] | IntegerArray, dtype: Literal['Int8', 'Int16', 'Int32', 'Int64'] | Int8Dtype | Int16Dtype | Int32Dtype | Int64Dtype | Literal['UInt8', 'UInt16', 'UInt32', 'UInt64'] | UInt8Dtype | UInt16Dtype | UInt32Dtype | UInt64Dtype | None = ..., copy: bool = ...) -> IntegerArray
/tests/test_features.py:140: note:     def array(data: Sequence[float | floating[Any] | NAType | None] | ndarray[tuple[Any, ...], dtype[floating[Any]]] | FloatingArray, dtype: Literal['Float32', 'Float64'] | Float32Dtype | Float64Dtype | None = ..., copy: bool = ...) -> FloatingArray
/tests/test_features.py:140: note:     def array(data: tuple[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None, ...] | MutableSequence[Just[float] | str | datetime | datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[Any]] | DatetimeArray, dtype: DatetimeTZDtype | Literal['datetime64[s, UTC]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'datetime64[ns, UTC]'] | dtype[datetime64[date | int | None]] | Literal['datetime64[s]', 'datetime64[ms]', 'datetime64[us]', 'datetime64[ns]', 'M8[s]', 'M8[ms]', 'M8[us]', 'M8[ns]', '<M8[s]', '<M8[ms]', '<M8[us]', '<M8[ns]'], copy: bool = ...) -> DatetimeArray
/tests/test_features.py:140: note:     def array(data: Sequence[datetime | NaTType | None] | Sequence[datetime64[date | int | None] | NaTType | None] | ndarray[tuple[Any, ...], dtype[datetime64[date | int | None]]] | DatetimeArray, dtype: None = ..., copy: bool = ...) -> DatetimeArray
/tests/test_features.py:140: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Never], copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:140: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['pyarrow']] | Literal['string[pyarrow]'], copy: bool = ...) -> ArrowStringArray
/tests/test_features.py:140: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[Literal['python']] | Literal['string[python]'], copy: bool = ...) -> StringArray
/tests/test_features.py:140: note:     def array(data: tuple[Just[float] | str | str_ | NAType | None, ...] | MutableSequence[Just[float] | str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[Any]] | BaseStringArray[None], dtype: StringDtype[None] | Literal['string'], copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:140: note:     def array(data: tuple[str | str_ | NAType | None, ...] | MutableSequence[str | str_ | NAType | None] | ndarray[tuple[Any, ...], dtype[str_]] | BaseStringArray[None], dtype: None = ..., copy: bool = ...) -> BaseStringArray[None]
/tests/test_features.py:140: note:     def array(data: tuple[Any, ...] | MutableSequence[Any], dtype: None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_features.py:140: note:     def array(data: ndarray[tuple[Any, ...], dtype[Any]] | NumpyExtensionArray | RangeIndex, dtype: type[builtins.bool] | Literal['bool'] | type[int] | Literal['int'] | type[float] | Literal['float'] | type[complex] | Literal['complex'] | type[bytes] | Literal['bytes'] | type[object] | Literal['object'] | Literal['?', 'b1', 'bool_'] | type[numpy.bool[builtins.bool]] | Literal['b', 'i1', 'int8', 'byte', 'h', 'i2', 'int16', 'short', 'i', 'i4', 'int32', 'intc', 'l', 'long', 'l', 'i8', 'int64', 'int_', 'long', 'q', 'longlong', 'p', 'intp'] | type[signedinteger[_8Bit]] | type[signedinteger[_16Bit]] | type[signedinteger[_32Bit]] | type[signedinteger[_32Bit | _64Bit]] | type[signedinteger[_64Bit]] | type[signedinteger[_32Bit | _64Bit]] | Literal['B', 'u1', 'uint8', 'ubyte', 'H', 'u2', 'uint16', 'ushort', 'I', 'u4', 'uint32', 'uintc', 'L', 'ulong', 'L', 'u8', 'uint', 'ulong', 'uint64', 'Q', 'ulonglong', 'P', 'uintp'] | type[unsignedinteger[_8Bit]] | type[unsignedinteger[_16Bit]] | type[unsignedinteger[_32Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | type[unsignedinteger[_64Bit]] | type[unsignedinteger[_32Bit | _64Bit]] | Literal['e', 'f2', '<f2', 'float16', 'half'] | type[floating[_16Bit]] | Literal['f', 'f4', 'float32', 'single', 'd', 'f8', 'float64', 'double', 'g', 'f16', 'float128', 'longdouble'] | type[floating[_32Bit]] | type[float64] | type[floating[_64Bit | _96Bit | _128Bit]] | Literal['F', 'c8', 'complex64', 'csingle', 'D', 'c16', 'complex128', 'cdouble', 'G', 'c32', 'complex256', 'clongdouble'] | type[complexfloating[_32Bit, _32Bit]] | type[complex128] | type[complexfloating[_64Bit | _96Bit | _128Bit, _64Bit | _96Bit | _128Bit]] | Literal['U', 'str_', 'unicode'] | type[str_] | Literal['S', 'bytes_'] | type[bytes_] | Literal['object_', 'O'] | type[object_] | Literal['V', 'void'] | type[void] | None = ..., copy: bool = ...) -> NumpyExtensionArray
/tests/test_acquire.py:272: error: Argument 3 has incompatible type "*tuple[object, ...]"; expected "str"  [arg-type]
/tests/test_acquire.py:272: error: Argument 4 has incompatible type "**dict[str, object]"; expected "str"  [arg-type]
Found 15 errors in 7 files (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 6 deselected / 80 selected

tests/test_acquire.py ......F...F..                                      [ 16%]
tests/test_features.py .....FFFFFFFFFFFF........F.F                      [ 51%]
tests/test_ingest.py .............                                       [ 67%]
tests/test_pipeline.py .............                                     [ 83%]
tests/test_transform.py ......F......                                    [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
______________ test_resolve_download_url_http_error_returns_none _______________
tests/test_acquire.py:161: in test_resolve_download_url_http_error_returns_none
    result = resolve_download_url(
kgs_pipeline/acquire.py:190: in resolve_download_url
    soup = BeautifulSoup(resp.text, "html.parser")
                         ^^^^
E   UnboundLocalError: cannot access local variable 'resp' where it is not associated with a value
___________________ test_download_file_non_utf8_no_file_left ___________________
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1581: in __enter__
    setattr(self.target, self.attribute, new_attr)
E   TypeError: cannot set 'decode' attribute of immutable type 'bytes'

During handling of the above exception, another exception occurred:
tests/test_acquire.py:276: in test_download_file_non_utf8_no_file_left
    with patch.object(bytes, "decode", patched_decode):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1594: in __enter__
    if not self.__exit__(*sys.exc_info()):
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/unittest/mock.py:1603: in __exit__
    setattr(self.target, self.attribute, self.temp_original)
E   TypeError: cannot set 'decode' attribute of immutable type 'bytes'
_____________________ test_f2_cumulative_sums_known_values _____________________
tests/test_features.py:230: in test_f2_cumulative_sums_known_values
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
___________________ test_f2_cum_flat_across_zero_production ____________________
tests/test_features.py:248: in test_f2_cum_flat_across_zero_production
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
___________________ test_f2_cum_stays_zero_when_not_started ____________________
tests/test_features.py:264: in test_f2_cum_stays_zero_when_not_started
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
______________________ test_f2_gor_zero_oil_positive_gas _______________________
tests/test_features.py:274: in test_f2_gor_zero_oil_positive_gas
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
____________________________ test_f2_gor_both_zero _____________________________
tests/test_features.py:283: in test_f2_gor_both_zero
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
______________________ test_f2_gor_gas_zero_oil_positive _______________________2026-04-23 15:11:27,373 [INFO] distributed.core: Event loop was unresponsive in Nanny for 4.61s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.

tests/test_features.py:293: in test_f2_gor_gas_zero_oil_positive
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
_________________________ test_f2_water_cut_zero_water _________________________
tests/test_features.py:304: in test_f2_water_cut_zero_water
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
_________________________ test_f2_water_cut_all_water __________________________
tests/test_features.py:316: in test_f2_water_cut_all_water
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
_______________________ test_f2_decline_rate_clip_lower ________________________
tests/test_features.py:333: in test_f2_decline_rate_clip_lower
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
_______________________ test_f2_decline_rate_clip_upper ________________________
tests/test_features.py:346: in test_f2_decline_rate_clip_upper
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
______________________ test_f2_decline_rate_within_bounds ______________________
tests/test_features.py:359: in test_f2_decline_rate_within_bounds
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
_______________ test_f2_decline_rate_shutin_then_resume_clipped ________________
tests/test_features.py:378: in test_f2_decline_rate_shutin_then_resume_clipped
    result = add_cumulative_features(df)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^
kgs_pipeline/features.py:278: in add_cumulative_features
    with pd.option_context("mode.use_inf_as_na", True):
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/contextlib.py:137: in __enter__
    return next(self.gen)
           ^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in option_context
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:512: in <genexpr>
    undo = tuple((pat, get_option(pat)) for pat, val in ops)
                       ^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:187: in get_option
    key = _get_single_key(pat)
          ^^^^^^^^^^^^^^^^^^^^
../../../miniconda3/envs/dapi/lib/python3.12/site-packages/pandas/_config/config.py:131: in _get_single_key
    raise OptionError(f"No such keys(s): {pat!r}")
E   pandas.errors.OptionError: No such keys(s): 'mode.use_inf_as_na'
_______________________ test_f5_complete_column_set_tr19 _______________________
tests/test_features.py:569: in test_f5_complete_column_set_tr19
    features(cfg)
kgs_pipeline/features.py:452: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
_______________ test_f5_tr14_consistent_schema_across_partitions _______________
tests/test_features.py:614: in test_f5_tr14_consistent_schema_across_partitions
    features(cfg)
kgs_pipeline/features.py:452: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
___________________ test_meta_derivation_matches_live_output ___________________
tests/test_transform.py:237: in test_meta_derivation_matches_live_output
    assert str(meta[col].dtype) == str(live[col].dtype), f"dtype mismatch for {col}"
E   AssertionError: dtype mismatch for production_date
E   assert 'datetime64[ns]' == 'datetime64[us]'
E     
E     - datetime64[us]
E     ?            ^
E     + datetime64[ns]
E     ?            ^
=============================== warnings summary ===============================
tests/test_features.py::test_f1_out_of_set_product_handled
  /tests/test_features.py:104: Pandas4Warning: Constructing a Categorical with a dtype and values containing non-null entries not in that dtype's categories is deprecated and will raise in a future version.
    df["PRODUCT"] = pd.Categorical(df["PRODUCT"], categories=["O", "G"])

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_acquire.py::test_resolve_download_url_http_error_returns_none
FAILED tests/test_acquire.py::test_download_file_non_utf8_no_file_left - Type...
FAILED tests/test_features.py::test_f2_cumulative_sums_known_values - pandas....
FAILED tests/test_features.py::test_f2_cum_flat_across_zero_production - pand...
FAILED tests/test_features.py::test_f2_cum_stays_zero_when_not_started - pand...
FAILED tests/test_features.py::test_f2_gor_zero_oil_positive_gas - pandas.err...
FAILED tests/test_features.py::test_f2_gor_both_zero - pandas.errors.OptionEr...
FAILED tests/test_features.py::test_f2_gor_gas_zero_oil_positive - pandas.err...
FAILED tests/test_features.py::test_f2_water_cut_zero_water - pandas.errors.O...
FAILED tests/test_features.py::test_f2_water_cut_all_water - pandas.errors.Op...
FAILED tests/test_features.py::test_f2_decline_rate_clip_lower - pandas.error...
FAILED tests/test_features.py::test_f2_decline_rate_clip_upper - pandas.error...
FAILED tests/test_features.py::test_f2_decline_rate_within_bounds - pandas.er...
FAILED tests/test_features.py::test_f2_decline_rate_shutin_then_resume_clipped
FAILED tests/test_features.py::test_f5_complete_column_set_tr19 - ValueError:...
FAILED tests/test_features.py::test_f5_tr14_consistent_schema_across_partitions
FAILED tests/test_transform.py::test_meta_derivation_matches_live_output - As...
=========== 17 failed, 63 passed, 6 deselected, 1 warning in 54.68s ============
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1164, in emit
    self.flush()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1144, in flush
    self.stream.flush()
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/tornado/ioloop.py", line 945, in _run
    val = self.callback()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 608, in _measure_tick
    logger.info(
Message: 'Event loop was unresponsive in %s for %.2fs.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.'
Arguments: ('Scheduler', 4.627989053726196)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 7614, in retire_workers
    logger.info(
Message: "Retire worker addresses (stimulus_id='%s') %s"
Arguments: ('retire-workers-1776975087.829451', (0,))
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 611, in close
    logger.info("Closing Nanny at %r. Reason: %s", self.address_safe, reason)
Message: 'Closing Nanny at %r. Reason: %s'
Arguments: ('tcp://127.0.0.1:56795', 'nanny-close')
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 619, in close
    await self.kill(timeout=timeout, reason=reason)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 400, in kill
    await self.process.kill(reason=reason, timeout=timeout)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 858, in kill
    logger.info("Nanny asking worker to close. Reason: %s", reason)
Message: 'Nanny asking worker to close. Reason: %s'
Arguments: ('nanny-close',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6225, in handle_worker
    await self.handle_stream(comm=comm, extra={"worker": worker})
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 908, in handle_stream
    logger.info(
Message: "Received 'close-stream' from %s; closing."
Arguments: ('tcp://127.0.0.1:56799',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5444, in remove_worker
    logger.info(
Message: "Remove worker addr: tcp://127.0.0.1:56797 name: 0 (stimulus_id='handle-worker-cleanup-1776975088.318593')"
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5582, in remove_worker
    logger.info("Lost all workers")
Message: 'Lost all workers'
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 626, in close
    logger.info("Nanny at %r closed.", self.address_safe)
Message: 'Nanny at %r closed.'
Arguments: ('tcp://127.0.0.1:56795',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4343, in close
    logger.info("Closing scheduler. Reason: %s", reason)
Message: 'Closing scheduler. Reason: %s'
Arguments: ('unknown',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4371, in close
    logger.info("Scheduler closing all comms")
Message: 'Scheduler closing all comms'
Arguments: ()

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 80 deselected / 6 selected

tests/test_acquire.py ..                                                 [ 33%]
tests/test_features.py F                                                 [ 50%]
tests/test_ingest.py .                                                   [ 66%]
tests/test_pipeline.py F                                                 [ 83%]
tests/test_transform.py F                                                [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
___________________________ test_f5_tr26_integration ___________________________
tests/test_features.py:722: in test_f5_tr26_integration
    features(cfg)
kgs_pipeline/features.py:452: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
______________________ test_e2e_ingest_transform_features ______________________
tests/test_pipeline.py:599: in test_e2e_ingest_transform_features
    assert 10 <= ddf_processed.npartitions <= 50
E   assert 10 <= 2
E    +  where 2 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(ab2243a).npartitions
_______________________ test_transform_tr25_integration ________________________
tests/test_transform.py:519: in test_transform_tr25_integration
    assert 10 <= ddf.npartitions <= 50
E   assert 10 <= 1
E    +  where 1 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(a53bb8d).npartitions
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_tr26_integration - ValueError: The col...
FAILED tests/test_pipeline.py::test_e2e_ingest_transform_features - assert 10...
FAILED tests/test_transform.py::test_transform_tr25_integration - assert 10 <= 1
================== 3 failed, 3 passed, 80 deselected in 6.43s ==================

```

---

## Eval Run at 2026-04-23 15:21:18

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 6 deselected / 80 selected

tests/test_acquire.py .............                                      [ 16%]
tests/test_features.py .........................F.F                      [ 51%]
tests/test_ingest.py .............                                       [ 67%]
tests/test_pipeline.py .............                                     [ 83%]
tests/test_transform.py .............                                    [100%]Running teardown with pytest sessionfinish...
2026-04-23 15:20:41,381 [INFO] distributed.core: Event loop was unresponsive in Nanny for 3.17s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.


=================================== FAILURES ===================================
_______________________ test_f5_complete_column_set_tr19 _______________________
tests/test_features.py:569: in test_f5_complete_column_set_tr19
    features(cfg)
kgs_pipeline/features.py:456: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
_______________ test_f5_tr14_consistent_schema_across_partitions _______________
tests/test_features.py:613: in test_f5_tr14_consistent_schema_across_partitions
    features(cfg)
kgs_pipeline/features.py:456: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
=============================== warnings summary ===============================
tests/test_features.py::test_f1_out_of_set_product_handled
  /tests/test_features.py:104: Pandas4Warning: Constructing a Categorical with a dtype and values containing non-null entries not in that dtype's categories is deprecated and will raise in a future version.
    df["PRODUCT"] = pd.Categorical(df["PRODUCT"], categories=["O", "G"])

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_complete_column_set_tr19 - ValueError:...
FAILED tests/test_features.py::test_f5_tr14_consistent_schema_across_partitions
============ 2 failed, 78 passed, 6 deselected, 1 warning in 55.88s ============
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/tornado/ioloop.py", line 945, in _run
    val = self.callback()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 608, in _measure_tick
    logger.info(
Message: 'Event loop was unresponsive in %s for %.2fs.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.'
Arguments: ('Nanny', 4.056612014770508)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 7614, in retire_workers
    logger.info(
Message: "Retire worker addresses (stimulus_id='%s') %s"
Arguments: ('retire-workers-1776975645.4634361', (0,))
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 611, in close
    logger.info("Closing Nanny at %r. Reason: %s", self.address_safe, reason)
Message: 'Closing Nanny at %r. Reason: %s'
Arguments: ('tcp://127.0.0.1:56932', 'nanny-close')
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 619, in close
    await self.kill(timeout=timeout, reason=reason)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 400, in kill
    await self.process.kill(reason=reason, timeout=timeout)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 858, in kill
    logger.info("Nanny asking worker to close. Reason: %s", reason)
Message: 'Nanny asking worker to close. Reason: %s'
Arguments: ('nanny-close',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6225, in handle_worker
    await self.handle_stream(comm=comm, extra={"worker": worker})
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 908, in handle_stream
    logger.info(
Message: "Received 'close-stream' from %s; closing."
Arguments: ('tcp://127.0.0.1:56936',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5444, in remove_worker
    logger.info(
Message: "Remove worker addr: tcp://127.0.0.1:56934 name: 0 (stimulus_id='handle-worker-cleanup-1776975645.831284')"
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5582, in remove_worker
    logger.info("Lost all workers")
Message: 'Lost all workers'
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 626, in close
    logger.info("Nanny at %r closed.", self.address_safe)
Message: 'Nanny at %r closed.'
Arguments: ('tcp://127.0.0.1:56932',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4343, in close
    logger.info("Closing scheduler. Reason: %s", reason)
Message: 'Closing scheduler. Reason: %s'
Arguments: ('unknown',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4371, in close
    logger.info("Scheduler closing all comms")
Message: 'Scheduler closing all comms'
Arguments: ()

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 80 deselected / 6 selected

tests/test_acquire.py ..                                                 [ 33%]
tests/test_features.py F                                                 [ 50%]
tests/test_ingest.py .                                                   [ 66%]
tests/test_pipeline.py F                                                 [ 83%]
tests/test_transform.py F                                                [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
___________________________ test_f5_tr26_integration ___________________________
tests/test_features.py:721: in test_f5_tr26_integration
    features(cfg)
kgs_pipeline/features.py:456: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
______________________ test_e2e_ingest_transform_features ______________________
tests/test_pipeline.py:599: in test_e2e_ingest_transform_features
    assert 10 <= ddf_processed.npartitions <= 50
E   assert 10 <= 2
E    +  where 2 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(88fa11f).npartitions
_______________________ test_transform_tr25_integration ________________________
tests/test_transform.py:517: in test_transform_tr25_integration
    assert 10 <= ddf.npartitions <= 50
E   assert 10 <= 1
E    +  where 1 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(904f712).npartitions
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_tr26_integration - ValueError: The col...
FAILED tests/test_pipeline.py::test_e2e_ingest_transform_features - assert 10...
FAILED tests/test_transform.py::test_transform_tr25_integration - assert 10 <= 1
================== 3 failed, 3 passed, 80 deselected in 6.21s ==================

```

---

## Eval Run at 2026-04-23 15:25:24

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 6 deselected / 80 selected

tests/test_acquire.py .............                                      [ 16%]
tests/test_features.py .........................F.F                      [ 51%]
tests/test_ingest.py .............                                       [ 67%]
tests/test_pipeline.py .............                                     [ 83%]
tests/test_transform.py .............                                    [100%]Running teardown with pytest sessionfinish...
2026-04-23 15:24:49,242 [INFO] distributed.core: Event loop was unresponsive in Nanny for 4.32s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.


=================================== FAILURES ===================================
_______________________ test_f5_complete_column_set_tr19 _______________________
tests/test_features.py:569: in test_f5_complete_column_set_tr19
    features(cfg)
kgs_pipeline/features.py:457: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
_______________ test_f5_tr14_consistent_schema_across_partitions _______________
tests/test_features.py:613: in test_f5_tr14_consistent_schema_across_partitions
    features(cfg)
kgs_pipeline/features.py:457: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
=============================== warnings summary ===============================
tests/test_features.py::test_f1_out_of_set_product_handled
  /tests/test_features.py:104: Pandas4Warning: Constructing a Categorical with a dtype and values containing non-null entries not in that dtype's categories is deprecated and will raise in a future version.
    df["PRODUCT"] = pd.Categorical(df["PRODUCT"], categories=["O", "G"])

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_complete_column_set_tr19 - ValueError:...
FAILED tests/test_features.py::test_f5_tr14_consistent_schema_across_partitions
============ 2 failed, 78 passed, 6 deselected, 1 warning in 53.44s ============
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/tornado/ioloop.py", line 945, in _run
    val = self.callback()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 608, in _measure_tick
    logger.info(
Message: 'Event loop was unresponsive in %s for %.2fs.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.'
Arguments: ('Scheduler', 6.898796081542969)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 7614, in retire_workers
    logger.info(
Message: "Retire worker addresses (stimulus_id='%s') %s"
Arguments: ('retire-workers-1776975891.8797681', (0,))
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 611, in close
    logger.info("Closing Nanny at %r. Reason: %s", self.address_safe, reason)
Message: 'Closing Nanny at %r. Reason: %s'
Arguments: ('tcp://127.0.0.1:56966', 'nanny-close')
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 619, in close
    await self.kill(timeout=timeout, reason=reason)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 400, in kill
    await self.process.kill(reason=reason, timeout=timeout)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 858, in kill
    logger.info("Nanny asking worker to close. Reason: %s", reason)
Message: 'Nanny asking worker to close. Reason: %s'
Arguments: ('nanny-close',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6225, in handle_worker
    await self.handle_stream(comm=comm, extra={"worker": worker})
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 908, in handle_stream
    logger.info(
Message: "Received 'close-stream' from %s; closing."
Arguments: ('tcp://127.0.0.1:56970',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5444, in remove_worker
    logger.info(
Message: "Remove worker addr: tcp://127.0.0.1:56968 name: 0 (stimulus_id='handle-worker-cleanup-1776975892.2117429')"
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5582, in remove_worker
    logger.info("Lost all workers")
Message: 'Lost all workers'
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 626, in close
    logger.info("Nanny at %r closed.", self.address_safe)
Message: 'Nanny at %r closed.'
Arguments: ('tcp://127.0.0.1:56966',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4343, in close
    logger.info("Closing scheduler. Reason: %s", reason)
Message: 'Closing scheduler. Reason: %s'
Arguments: ('unknown',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4371, in close
    logger.info("Scheduler closing all comms")
Message: 'Scheduler closing all comms'
Arguments: ()

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 80 deselected / 6 selected

tests/test_acquire.py ..                                                 [ 33%]
tests/test_features.py F                                                 [ 50%]
tests/test_ingest.py .                                                   [ 66%]
tests/test_pipeline.py F                                                 [ 83%]
tests/test_transform.py F                                                [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
___________________________ test_f5_tr26_integration ___________________________
tests/test_features.py:721: in test_f5_tr26_integration
    features(cfg)
kgs_pipeline/features.py:457: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E     Extra:   ['LEASE_KID']
E     Missing: []
______________________ test_e2e_ingest_transform_features ______________________
tests/test_pipeline.py:599: in test_e2e_ingest_transform_features
    assert 10 <= ddf_processed.npartitions <= 50
E   assert 10 <= 2
E    +  where 2 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(8a3c02d).npartitions
_______________________ test_transform_tr25_integration ________________________
tests/test_transform.py:517: in test_transform_tr25_integration
    assert 10 <= ddf.npartitions <= 50
E   assert 10 <= 1
E    +  where 1 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(f922d1f).npartitions
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_tr26_integration - ValueError: The col...
FAILED tests/test_pipeline.py::test_e2e_ingest_transform_features - assert 10...
FAILED tests/test_transform.py::test_transform_tr25_integration - assert 10 <= 1
================== 3 failed, 3 passed, 80 deselected in 6.04s ==================

```

---

## Eval Run at 2026-04-23 15:29:59

**Status:** ❌ FAILED

### Failures:
- **Type check:**
```
Type check failed. Fix these errors:
/kgs_pipeline/features.py:83: error: Argument 3 to "insert" of "DataFrame" has incompatible type "IntegerArray"; expected "str | bytes | date | datetime | timedelta | <7 more items> | complex | integer[Any] | floating[Any] | complexfloating[Any, Any] | Sequence[Any] | ndarray[tuple[int], dtype[Any]] | Series[Any] | Index[Any] | None"  [arg-type]
Found 1 error in 1 file (checked 12 source files)

```

- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 6 deselected / 80 selected

tests/test_acquire.py .............                                      [ 16%]
tests/test_features.py .........................F.F                      [ 51%]
tests/test_ingest.py .............                                       [ 67%]
tests/test_pipeline.py .............                                     [ 83%]
tests/test_transform.py .............                                    [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
_______________________ test_f5_complete_column_set_tr19 _______________________
tests/test_features.py:569: in test_f5_complete_column_set_tr19
    features(cfg)
kgs_pipeline/features.py:463: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E   Actual:   ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file']
E   Expected: ['LEASE_KID', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl']
_______________ test_f5_tr14_consistent_schema_across_partitions _______________
tests/test_features.py:613: in test_f5_tr14_consistent_schema_across_partitions
    features(cfg)
kgs_pipeline/features.py:463: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E   Actual:   ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file']
E   Expected: ['LEASE_KID', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl']
=============================== warnings summary ===============================
tests/test_features.py::test_f1_out_of_set_product_handled
  /tests/test_features.py:104: Pandas4Warning: Constructing a Categorical with a dtype and values containing non-null entries not in that dtype's categories is deprecated and will raise in a future version.
    df["PRODUCT"] = pd.Categorical(df["PRODUCT"], categories=["O", "G"])

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_complete_column_set_tr19 - ValueError:...
FAILED tests/test_features.py::test_f5_tr14_consistent_schema_across_partitions
============ 2 failed, 78 passed, 6 deselected, 1 warning in 53.57s ============
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 7614, in retire_workers
    logger.info(
Message: "Retire worker addresses (stimulus_id='%s') %s"
Arguments: ('retire-workers-1776976167.1735911', (0,))
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 611, in close
    logger.info("Closing Nanny at %r. Reason: %s", self.address_safe, reason)
Message: 'Closing Nanny at %r. Reason: %s'
Arguments: ('tcp://127.0.0.1:57010', 'nanny-close')
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 619, in close
    await self.kill(timeout=timeout, reason=reason)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 400, in kill
    await self.process.kill(reason=reason, timeout=timeout)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 858, in kill
    logger.info("Nanny asking worker to close. Reason: %s", reason)
Message: 'Nanny asking worker to close. Reason: %s'
Arguments: ('nanny-close',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6225, in handle_worker
    await self.handle_stream(comm=comm, extra={"worker": worker})
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 908, in handle_stream
    logger.info(
Message: "Received 'close-stream' from %s; closing."
Arguments: ('tcp://127.0.0.1:57014',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5444, in remove_worker
    logger.info(
Message: "Remove worker addr: tcp://127.0.0.1:57012 name: 0 (stimulus_id='handle-worker-cleanup-1776976167.674387')"
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5582, in remove_worker
    logger.info("Lost all workers")
Message: 'Lost all workers'
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 626, in close
    logger.info("Nanny at %r closed.", self.address_safe)
Message: 'Nanny at %r closed.'
Arguments: ('tcp://127.0.0.1:57010',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4343, in close
    logger.info("Closing scheduler. Reason: %s", reason)
Message: 'Closing scheduler. Reason: %s'
Arguments: ('unknown',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4371, in close
    logger.info("Scheduler closing all comms")
Message: 'Scheduler closing all comms'
Arguments: ()

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 80 deselected / 6 selected

tests/test_acquire.py ..                                                 [ 33%]
tests/test_features.py F                                                 [ 50%]
tests/test_ingest.py .                                                   [ 66%]
tests/test_pipeline.py F                                                 [ 83%]
tests/test_transform.py F                                                [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
___________________________ test_f5_tr26_integration ___________________________
tests/test_features.py:721: in test_f5_tr26_integration
    features(cfg)
kgs_pipeline/features.py:463: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E   Actual:   ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file']
E   Expected: ['LEASE_KID', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl']
______________________ test_e2e_ingest_transform_features ______________________
tests/test_pipeline.py:599: in test_e2e_ingest_transform_features
    assert 10 <= ddf_processed.npartitions <= 50
E   assert 10 <= 2
E    +  where 2 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(e688223).npartitions
_______________________ test_transform_tr25_integration ________________________
tests/test_transform.py:517: in test_transform_tr25_integration
    assert 10 <= ddf.npartitions <= 50
E   assert 10 <= 1
E    +  where 1 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(79c089d).npartitions
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_tr26_integration - ValueError: The col...
FAILED tests/test_pipeline.py::test_e2e_ingest_transform_features - assert 10...
FAILED tests/test_transform.py::test_transform_tr25_integration - assert 10 <= 1
================== 3 failed, 3 passed, 80 deselected in 5.63s ==================

```

---

## Eval Run at 2026-04-23 15:33:15

**Status:** ❌ FAILED

### Failures:
- **Unit Tests:**
```
Unit Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 6 deselected / 80 selected

tests/test_acquire.py .............                                      [ 16%]
tests/test_features.py .........................F.F                      [ 51%]
tests/test_ingest.py .............                                       [ 67%]
tests/test_pipeline.py .............                                     [ 83%]
tests/test_transform.py .............                                    [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
_______________________ test_f5_complete_column_set_tr19 _______________________
tests/test_features.py:569: in test_f5_complete_column_set_tr19
    features(cfg)
kgs_pipeline/features.py:457: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E   Actual:   ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'oil_bbl_lag_1m', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'gas_mcf_lag_1m', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'water_bbl_lag_1m']
E   Expected: ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'oil_bbl_lag_1m', 'gas_mcf_lag_1m', 'water_bbl_lag_1m']
_______________ test_f5_tr14_consistent_schema_across_partitions _______________
tests/test_features.py:613: in test_f5_tr14_consistent_schema_across_partitions
    features(cfg)
kgs_pipeline/features.py:457: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E   Actual:   ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'oil_bbl_lag_1m', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'gas_mcf_lag_1m', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'water_bbl_lag_1m']
E   Expected: ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'oil_bbl_lag_1m', 'gas_mcf_lag_1m', 'water_bbl_lag_1m']
=============================== warnings summary ===============================
tests/test_features.py::test_f1_out_of_set_product_handled
  /tests/test_features.py:104: Pandas4Warning: Constructing a Categorical with a dtype and values containing non-null entries not in that dtype's categories is deprecated and will raise in a future version.
    df["PRODUCT"] = pd.Categorical(df["PRODUCT"], categories=["O", "G"])

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_complete_column_set_tr19 - ValueError:...
FAILED tests/test_features.py::test_f5_tr14_consistent_schema_across_partitions
============ 2 failed, 78 passed, 6 deselected, 1 warning in 52.52s ============
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/tornado/ioloop.py", line 945, in _run
    val = self.callback()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 608, in _measure_tick
    logger.info(
Message: 'Event loop was unresponsive in %s for %.2fs.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.'
Arguments: ('Nanny', 5.204569101333618)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 7614, in retire_workers
    logger.info(
Message: "Retire worker addresses (stimulus_id='%s') %s"
Arguments: ('retire-workers-1776976362.1482189', (0,))
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 611, in close
    logger.info("Closing Nanny at %r. Reason: %s", self.address_safe, reason)
Message: 'Closing Nanny at %r. Reason: %s'
Arguments: ('tcp://127.0.0.1:57043', 'nanny-close')
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 619, in close
    await self.kill(timeout=timeout, reason=reason)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 400, in kill
    await self.process.kill(reason=reason, timeout=timeout)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 858, in kill
    logger.info("Nanny asking worker to close. Reason: %s", reason)
Message: 'Nanny asking worker to close. Reason: %s'
Arguments: ('nanny-close',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6225, in handle_worker
    await self.handle_stream(comm=comm, extra={"worker": worker})
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 908, in handle_stream
    logger.info(
Message: "Received 'close-stream' from %s; closing."
Arguments: ('tcp://127.0.0.1:57047',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5444, in remove_worker
    logger.info(
Message: "Remove worker addr: tcp://127.0.0.1:57045 name: 0 (stimulus_id='handle-worker-cleanup-1776976362.52229')"
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4653, in add_worker
    await self.handle_worker(comm, address)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 6229, in handle_worker
    await self.remove_worker(
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 823, in wrapper
    return await func(*args, **kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 5582, in remove_worker
    logger.info("Lost all workers")
Message: 'Lost all workers'
Arguments: ()
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/nanny.py", line 626, in close
    logger.info("Nanny at %r closed.", self.address_safe)
Message: 'Nanny at %r closed.'
Arguments: ('tcp://127.0.0.1:57043',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4343, in close
    logger.info("Closing scheduler. Reason: %s", reason)
Message: 'Closing scheduler. Reason: %s'
Arguments: ('unknown',)
--- Logging error ---
Traceback (most recent call last):
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/logging/__init__.py", line 1163, in emit
    stream.write(msg + self.terminator)
ValueError: I/O operation on closed file.
Call stack:
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1032, in _bootstrap
    self._bootstrap_inner()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1075, in _bootstrap_inner
    self.run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/threading.py", line 1012, in run
    self._target(*self._args, **self._kwargs)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 468, in wrapper
    target()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/utils.py", line 576, in run_loop
    asyncio_run(amain(), loop_factory=get_loop_factory())
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 195, in run
    return runner.run(main)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 678, in run_until_complete
    self.run_forever()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 645, in run_forever
    self._run_once()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/base_events.py", line 1999, in _run_once
    handle._run()
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/asyncio/events.py", line 88, in _run
    self._context.run(self._callback, *self._args)
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/core.py", line 834, in _handle_comm
    result = await result
  File "/Users/sirisurab/miniconda3/envs/dapi/lib/python3.12/site-packages/distributed/scheduler.py", line 4371, in close
    logger.info("Scheduler closing all comms")
Message: 'Scheduler closing all comms'
Arguments: ()

```

- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 80 deselected / 6 selected

tests/test_acquire.py ..                                                 [ 33%]
tests/test_features.py F                                                 [ 50%]
tests/test_ingest.py .                                                   [ 66%]
tests/test_pipeline.py F                                                 [ 83%]
tests/test_transform.py F                                                [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
___________________________ test_f5_tr26_integration ___________________________
tests/test_features.py:721: in test_f5_tr26_integration
    features(cfg)
kgs_pipeline/features.py:457: in features
    ddf_final.to_parquet(str(features_dir), overwrite=True, write_index=False)
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
E   Actual:   ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'oil_bbl_lag_1m', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'gas_mcf_lag_1m', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'water_bbl_lag_1m']
E   Expected: ['LEASE_KID', 'production_date', 'oil_bbl', 'gas_mcf', 'water_bbl', 'LEASE', 'DOR_CODE', 'API_NUMBER', 'FIELD', 'PRODUCING_ZONE', 'OPERATOR', 'COUNTY', 'TOWNSHIP', 'TWN_DIR', 'RANGE', 'RANGE_DIR', 'SECTION', 'SPOT', 'LATITUDE', 'LONGITUDE', 'source_file', 'cum_oil', 'cum_gas', 'cum_water', 'gor', 'water_cut', 'decline_rate', 'oil_bbl_rolling_3m', 'oil_bbl_rolling_6m', 'gas_mcf_rolling_3m', 'gas_mcf_rolling_6m', 'water_bbl_rolling_3m', 'water_bbl_rolling_6m', 'oil_bbl_lag_1m', 'gas_mcf_lag_1m', 'water_bbl_lag_1m']
______________________ test_e2e_ingest_transform_features ______________________
tests/test_pipeline.py:599: in test_e2e_ingest_transform_features
    assert 10 <= ddf_processed.npartitions <= 50
E   assert 10 <= 2
E    +  where 2 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(df279a2).npartitions
_______________________ test_transform_tr25_integration ________________________
tests/test_transform.py:517: in test_transform_tr25_integration
    assert 10 <= ddf.npartitions <= 50
E   assert 10 <= 1
E    +  where 1 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(067f263).npartitions
=========================== short test summary info ============================
FAILED tests/test_features.py::test_f5_tr26_integration - ValueError: The col...
FAILED tests/test_pipeline.py::test_e2e_ingest_transform_features - assert 10...
FAILED tests/test_transform.py::test_transform_tr25_integration - assert 10 <= 1
================== 3 failed, 3 passed, 80 deselected in 5.83s ==================

```

---

## Eval Run at 2026-04-23 15:36:25

**Status:** ❌ FAILED

### Failures:
- **Integration Tests:**
```
Integration Tests failed. Fix these errors:
============================= test session starts ==============================
platform darwin -- Python 3.12.13, pytest-9.0.3, pluggy-1.6.0
rootdir: /Users/sirisurab/projects/dapi_poc/kgs
configfile: pytest.ini
plugins: anyio-4.12.1, mock-3.15.1, repeat-0.9.4, xdist-3.8.0, langsmith-0.7.32, asyncio-1.3.0, deepeval-3.9.7, rerunfailures-16.1
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 86 items / 80 deselected / 6 selected

tests/test_acquire.py ..                                                 [ 33%]
tests/test_features.py .                                                 [ 50%]
tests/test_ingest.py .                                                   [ 66%]
tests/test_pipeline.py F                                                 [ 83%]
tests/test_transform.py F                                                [100%]Running teardown with pytest sessionfinish...


=================================== FAILURES ===================================
______________________ test_e2e_ingest_transform_features ______________________
tests/test_pipeline.py:599: in test_e2e_ingest_transform_features
    assert 10 <= ddf_processed.npartitions <= 50
E   assert 10 <= 2
E    +  where 2 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(7c8b751).npartitions
_______________________ test_transform_tr25_integration ________________________
tests/test_transform.py:517: in test_transform_tr25_integration
    assert 10 <= ddf.npartitions <= 50
E   assert 10 <= 1
E    +  where 1 = Dask DataFrame Structure:\n                LEASE DOR_CODE API_NUMBER   FIELD PRODUCING_ZONE OPERATOR  COUNTY TOWNSHIP  ......    ...        ...         ...             ...\nDask Name: read_parquet, 1 expression\nExpr=ReadParquetFSSpec(051c10e).npartitions
=========================== short test summary info ============================
FAILED tests/test_pipeline.py::test_e2e_ingest_transform_features - assert 10...
FAILED tests/test_transform.py::test_transform_tr25_integration - assert 10 <= 1
================== 2 failed, 4 passed, 80 deselected in 5.99s ==================

```

---

## Eval Run at 2026-04-23 15:39:41

**Status:** ✅ PASSED

---
