# Ingest Stage — Task Specifications

## Overview

The ingest stage reads all raw KGS lease production `.txt` files from `data/raw/`,
enforces the canonical schema defined in `references/kgs_monthly_data_dictionary.csv`,
filters rows to year >= 2024, adds a `source_file` provenance column, and writes a
consolidated partitioned Parquet dataset to `data/interim/`. Processing is parallel
using Dask's distributed scheduler (CPU-bound, per ADR-001).

## Architecture constraints (from ADRs)

- **ADR-001**: Ingest is CPU-bound — use Dask distributed scheduler.
- **ADR-003**: Data dictionary is the single source of truth for schema, dtypes, and
  nullable status. No schema information may be inferred from the data.
- **ADR-004**: Output must be partitioned Parquet with `max(10, min(n, 50))` partitions.
- **ADR-005**: The task graph must remain lazy until the final Parquet write. Do not call
  `.compute()` inside any transformation function — return `dask.dataframe.DataFrame`.
- **ADR-006**: Log setup handled by the pipeline entry point — do not configure
  log handlers inside ingest functions.
- **TR-17**: Functions must return `dask.dataframe.DataFrame`, never `pandas.DataFrame`.
- **H1**: All column names must match the data dictionary canonical names at stage exit.
- **H2**: All columns must carry data-dictionary dtypes at stage exit.
- **H3**: Absent nullable columns → add as all-NA at correct dtype. Absent non-nullable
  columns → raise immediately. These two cases must never be collapsed into one handler.

## Canonical schema (from `references/kgs_monthly_data_dictionary.csv`)

| Column | dtype | nullable | notes |
|---|---|---|---|
| LEASE_KID | Int64 | no | pandas nullable integer |
| LEASE | string | yes | pandas StringDtype |
| DOR_CODE | Int64 | yes | pandas nullable integer |
| API_NUMBER | string | yes | pandas StringDtype |
| FIELD | string | yes | pandas StringDtype |
| PRODUCING_ZONE | string | yes | pandas StringDtype |
| OPERATOR | string | yes | pandas StringDtype |
| COUNTY | string | yes | pandas StringDtype |
| TOWNSHIP | Int64 | yes | pandas nullable integer |
| TWN_DIR | category | yes | allowed: `S`, `N` |
| RANGE | Int64 | yes | pandas nullable integer |
| RANGE_DIR | category | yes | allowed: `E`, `W` |
| SECTION | Int64 | yes | pandas nullable integer |
| SPOT | string | yes | pandas StringDtype |
| LATITUDE | float64 | yes | |
| LONGITUDE | float64 | yes | |
| MONTH-YEAR | string | no | pandas StringDtype; primary filter column |
| PRODUCT | category | no | allowed: `O`, `G` |
| WELLS | Int64 | yes | pandas nullable integer |
| PRODUCTION | float64 | yes | |
| URL | string | yes | pandas StringDtype |
| source_file | string | no | added by ingest; basename of source file |

### dtype mapping rules (from ADR-003)

- Data dictionary `int` + `nullable=no` → `int64` (standard, will raise on null)
- Data dictionary `int` + `nullable=yes` → `Int64` (pandas nullable integer)
- Data dictionary `float` → `float64`
- Data dictionary `string` → `pd.StringDtype()` (pandas StringDtype, nullable)
- Data dictionary `categorical` → `pd.CategoricalDtype(categories=[...], ordered=False)`
- `nullable=yes` column absent from source file → add as all-NA column at declared dtype
- `nullable=no` column absent from source file → raise `SchemaError` immediately

## Data filtering constraint (from `task-writer-kgs.md`)

After reading each raw file, filter rows to year >= 2024:
- Parse `MONTH-YEAR` column (format `"M-YYYY"`): split on `"-"`, take the last
  element as `year_str`.
- Drop rows where `year_str` is not numeric.
- Drop rows where `int(year_str) < 2024`.

This filtering must be implemented inside a partition-level function passed to
`map_partitions` — Dask's string accessor is unreliable on columns produced by
repartition or type casting (ADR-004, row 4).

## Inter-stage boundary contract (ingest → transform)

At stage exit, ingest guarantees:
- Column names conform to canonical schema (table above).
- All columns carry data-dictionary dtypes.
- All nullable absent columns have been added as all-NA columns at the correct dtype.
- Partitions: `max(10, min(n, 50))`.
- Sort: unsorted.
- `source_file` column is present and populated.

Transform relies on all of the above and must not re-derive schema or re-cast types.

---

## Task I-01: Data dictionary loader

**Module:** `kgs_pipeline/ingest.py`
**Function:** `load_data_dictionary(dict_path: str) -> dict[str, dict]`

**Description:** Read `references/kgs_monthly_data_dictionary.csv` and return a
mapping from column name to a dict containing `dtype`, `nullable` (bool), and
`categories` (list[str] or empty list). This mapping is the authoritative source for
all schema decisions in the ingest stage.

### Input

- `dict_path`: path to `references/kgs_monthly_data_dictionary.csv`.

### Processing logic (pseudo-code)

```
read the CSV with pandas
for each row:
    col_name = row["column"]
    dtype_str = row["dtype"]   # "int", "float", "string", "categorical"
    nullable = row["nullable"] == "yes"
    categories = split row["categories"] on "|" if present and non-empty else []
    store {dtype_str, nullable, categories} under col_name
return the mapping dict
```

### Output

`dict[str, dict]` — keyed by canonical column name, values contain `dtype` (str),
`nullable` (bool), `categories` (list[str]).

### Error handling

- If `dict_path` does not exist, raise `FileNotFoundError`.
- If any required column (`column`, `dtype`, `nullable`) is missing from the CSV,
  raise `KeyError`.

### Test cases (in `tests/test_ingest.py`)

- **Given** the actual `references/kgs_monthly_data_dictionary.csv`, **assert** that
  `LEASE_KID` maps to `dtype="int"`, `nullable=False`, `categories=[]`.
- **Given** the actual dictionary, **assert** that `PRODUCT` maps to
  `dtype="categorical"`, `nullable=False`, `categories=["O", "G"]`.
- **Given** the actual dictionary, **assert** that `TWN_DIR` maps to
  `categories=["S", "N"]`.
- **Given** a non-existent path, **assert** `FileNotFoundError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task I-02: Dtype resolver

**Module:** `kgs_pipeline/ingest.py`
**Function:** `resolve_pandas_dtype(dtype_str: str, nullable: bool, categories: list[str]) -> any`

**Description:** Convert a data dictionary dtype string into the corresponding pandas
dtype object, applying the nullable-aware mapping rules from ADR-003.

### Input

- `dtype_str`: one of `"int"`, `"float"`, `"string"`, `"categorical"`.
- `nullable`: `True` if the column allows nulls, `False` otherwise.
- `categories`: list of allowed category strings (only relevant when
  `dtype_str == "categorical"`).

### Mapping rules (from ADR-003)

```
"int"   + nullable=False  → numpy int64
"int"   + nullable=True   → pandas Int64 (nullable integer)
"float"                   → numpy float64  (always nullable via NaN)
"string"                  → pd.StringDtype()
"categorical"             → pd.CategoricalDtype(categories=categories, ordered=False)
```

### Output

A pandas-compatible dtype object suitable for use as the `dtype` argument in
`pd.read_csv` or `DataFrame.astype`.

### Error handling

- If `dtype_str` is not one of the four valid values, raise `ValueError` with the
  offending string.

### Test cases (in `tests/test_ingest.py`)

- **Given** `("int", False, [])`, **assert** the return value equals `numpy.int64`.
- **Given** `("int", True, [])`, **assert** the return value equals `pandas.Int64Dtype()`.
- **Given** `("float", True, [])`, **assert** the return value equals `numpy.float64`.
- **Given** `("string", True, [])`, **assert** the return value is `pd.StringDtype()`.
- **Given** `("categorical", True, ["O", "G"])`, **assert** the return value is
  `pd.CategoricalDtype(categories=["O", "G"], ordered=False)`.
- **Given** `("unknown", True, [])`, **assert** `ValueError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task I-03: Single-file reader

**Module:** `kgs_pipeline/ingest.py`
**Function:** `read_raw_file(file_path: str, data_dict: dict[str, dict]) -> pd.DataFrame`

**Description:** Read one raw KGS lease `.txt` file into a pandas DataFrame, enforce
canonical column names, apply data-dictionary dtypes, add the `source_file` column,
and filter rows to year >= 2024. Returns a single-partition pandas DataFrame.

### Input

- `file_path`: absolute or relative path to a raw `.txt` file in `data/raw/`.
- `data_dict`: the output of `load_data_dictionary`.

### Processing logic (pseudo-code)

```
read file as CSV (comma-separated) with UTF-8 encoding, all columns as str initially
    → on UnicodeDecodeError retry with latin-1 encoding
    → log a warning if latin-1 fallback is used

add source_file column = os.path.basename(file_path)

for each canonical column in data_dict:
    if column in DataFrame:
        resolve target dtype using resolve_pandas_dtype
        cast column to target dtype
            → on cast failure for non-nullable column: raise SchemaError
            → on cast failure for nullable column: coerce failures to NA and log warning
    else if nullable=no:
        raise SchemaError("Required column <col> absent from <file_path>")
    else:  # nullable=yes and absent
        add all-NA column at the target dtype

filter rows: keep only rows where MONTH-YEAR year component is numeric and >= 2024
    (split on "-", take last element, drop non-numeric or < 2024)

return the resulting DataFrame
```

### Custom exception

Define `SchemaError(Exception)` in `kgs_pipeline/ingest.py`. Raised when a
non-nullable column is missing or when a non-nullable column has cast failures.

### Output

`pd.DataFrame` — single-partition, canonical schema, year-filtered.

### Error handling

- Encoding: try UTF-8, fall back to latin-1, log warning on fallback.
- Missing non-nullable column: raise `SchemaError` immediately.
- Missing nullable column: add as all-NA column at the correct dtype, log INFO.
- Cast failures on non-nullable column: raise `SchemaError`.
- Cast failures on nullable column: coerce to NA and log a warning with the count of
  coerced values and the column name.
- Empty file (0 rows after filtering): log a warning and return an empty DataFrame
  with the correct schema — do not raise.

### Test cases (in `tests/test_ingest.py`)

- **Given** a synthetic `.txt` file with all canonical columns and year 2024 rows,
  **assert** the returned DataFrame has the correct dtypes for all columns and
  `source_file` is populated.
- **Given** a synthetic file with year 2023 rows only, **assert** the returned
  DataFrame is empty (year filter applied).
- **Given** a file missing a non-nullable column (`LEASE_KID`), **assert**
  `SchemaError` is raised.
- **Given** a file missing a nullable column (`FIELD`), **assert** the column is
  present in the output as all-NA with the correct dtype.
- **Given** a file with a non-numeric `MONTH-YEAR` value like `"-1-1965"`, **assert**
  that row is dropped (TR-05 — not a zero or null issue, just filtering).
- **Given** a UTF-8 unreadable file (latin-1 encoded), **assert** the file is read
  successfully and a warning is logged.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task I-04: Dask ingest runner

**Module:** `kgs_pipeline/ingest.py`
**Function:** `run_ingest(config: dict) -> dask.dataframe.DataFrame`

**Description:** Orchestrate the full ingest stage. Discover all `.txt` files in
`data/raw/`, read each file in parallel using Dask, concatenate into a single Dask
DataFrame with `max(10, min(n, 50))` partitions, and write the result to
`data/interim/` as partitioned Parquet. Returns the lazy Dask DataFrame (the
`.compute()` is triggered only by the Parquet write — not inside this function).

### Input

- `config`: the `ingest` section of `config.yaml` as a Python dict, with keys:
  `raw_dir`, `interim_dir`, `data_dictionary`.

### Processing logic (pseudo-code)

```
data_dict = load_data_dictionary(config["data_dictionary"])
raw_files = glob all .txt files in config["raw_dir"]
if no files found → log warning and return empty Dask DataFrame with canonical schema

for each file → wrap read_raw_file in dask.delayed
build a list of delayed pandas DataFrames

ddf = dask.dataframe.from_delayed(delayed_list, meta=<canonical schema meta>)

n_partitions = max(10, min(len(raw_files), 50))
ddf = ddf.repartition(npartitions=n_partitions)

write ddf to config["interim_dir"] as Parquet (pyarrow engine)
    → write_index=False, overwrite=True

log INFO: total partitions written, output directory
return ddf   ← lazy, already triggered via to_parquet
```

### Meta schema construction

Construct the `meta` argument for `dask.dataframe.from_delayed` by building an empty
pandas DataFrame with all canonical columns at their correct dtypes (using
`resolve_pandas_dtype`). Include the `source_file` column as `pd.StringDtype()`.

### Partition count formula

`n_partitions = max(10, min(len(raw_files), 50))`

The repartition call must be the last transformation before `to_parquet` (ADR-004).

### Output

`dask.dataframe.DataFrame` — lazy, canonical schema, year-filtered, partitioned.

### Lazy evaluation constraint (ADR-005, TR-17)

- Do not call `.compute()` inside `run_ingest`.
- The Parquet write (`to_parquet`) triggers computation — that is the only
  materialization point.
- Return type must be `dask.dataframe.DataFrame`.

### Error handling

- If `raw_dir` does not exist, raise `FileNotFoundError`.
- If no `.txt` files are found in `raw_dir`, log a warning and return an empty Dask
  DataFrame with the canonical meta schema.
- Individual file read errors from `read_raw_file` (e.g. `SchemaError`) are logged
  as warnings and that file is skipped — other files continue to be processed.

### Test cases (in `tests/test_ingest.py`)

- **TR-17 (Lazy evaluation):** **Given** a mocked `read_raw_file` returning a valid
  pandas DataFrame, **assert** `run_ingest` returns a `dask.dataframe.DataFrame`,
  not a `pandas.DataFrame`.
- **Given** 3 synthetic `.txt` files in `tmp_path/raw/`, **assert** the output
  Parquet files in `tmp_path/interim/` are readable.
- **TR-18 (Parquet readability):** **Assert** every Parquet file written to
  `interim_dir` can be read back by `dask.dataframe.read_parquet` without error.
- **TR-22 (Schema completeness):** Read at least 3 interim Parquet partitions and
  assert all expected columns (`LEASE_KID`, `LEASE`, `API_NUMBER`, `MONTH-YEAR`,
  `PRODUCT`, `PRODUCTION`, `WELLS`, `OPERATOR`, `COUNTY`, `source_file`) are present
  in every sampled partition.
- **Given** `raw_dir` does not exist, **assert** `FileNotFoundError` is raised.
- **Given** `raw_dir` exists but contains no `.txt` files, **assert** a warning is
  logged and the function returns without raising.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task I-05: Ingest stage integration tests

**Module:** `tests/test_ingest.py`

**Description:** End-to-end integration tests for the ingest stage using synthetic
raw files and a temporary directory. No real KGS server calls.

### Test cases

- **TR-18 (Parquet readability):** After calling `run_ingest` on synthetic data in
  `tmp_path`, assert every file in `interim_dir` is readable as Parquet by a fresh
  `dask.dataframe.read_parquet` call.

- **TR-22 (Schema completeness across partitions):** After `run_ingest`, sample at
  least 3 Parquet partition files and assert that every partition contains all
  expected columns: `LEASE_KID`, `LEASE`, `API_NUMBER`, `MONTH-YEAR`, `PRODUCT`,
  `PRODUCTION`, `WELLS`, `OPERATOR`, `COUNTY`, `source_file`. Assert no expected
  column is missing or renamed in any sampled partition.

- **Schema dtype validation:** After `run_ingest`, read the interim Parquet and
  assert dtypes match the canonical schema — `LEASE_KID` is `Int64`, `PRODUCTION`
  is `float64`, `PRODUCT` is category, `MONTH-YEAR` is string, `source_file` is
  string.

- **Year filter validation:** Provide synthetic raw files with rows spanning 2022–2024.
  Assert that only rows with year >= 2024 appear in the interim Parquet output.

**Definition of done:** All integration test cases pass, ruff and mypy report no
errors, `requirements.txt` updated with all third-party packages imported in this task.
