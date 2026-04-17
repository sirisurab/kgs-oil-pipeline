# Transform Stage — Task Specifications

## Overview

The transform stage reads the canonical interim Parquet dataset produced by ingest,
performs data cleaning (deduplication, null handling, outlier removal, invalid value
replacement), casts categorical columns to their declared value sets, derives a
`production_date` column from `MONTH-YEAR`, sets the entity index on `LEASE_KID`,
sorts by `production_date` within partitions, and writes the cleaned, analysis-ready
Parquet dataset to `data/processed/transform/`. Processing is parallel using Dask's
distributed scheduler (CPU-bound, per ADR-001).

## Architecture constraints (from ADRs)

- **ADR-001**: Transform is CPU-bound — use Dask distributed scheduler.
- **ADR-002**: All operations must be vectorized — no per-row iteration, no iterating
  over entity groups. Use grouped vectorized transforms.
- **ADR-003**: Categorical allowed values are defined in the data dictionary — values
  outside the declared set must be replaced with the appropriate null sentinel before
  casting, never allowed to propagate silently.
- **ADR-004**: Output must be `max(10, min(n, 50))` partitions. Repartition must be
  the last operation before `to_parquet`. Row filtering using string operations must
  be inside a partition-level function.
- **ADR-005**: Task graph remains lazy until the final Parquet write. No `.compute()`
  inside transformation functions.
- **TR-17**: All stage functions must return `dask.dataframe.DataFrame`.
- **TR-23**: Every `map_partitions` call must pass a `meta=` argument derived by
  calling the function on an empty DataFrame matching the input schema.

## Input state (from boundary-ingest-transform.md)

Transform receives from ingest:
- Canonical schema, data-dictionary dtypes enforced.
- All nullable absent columns filled with NA.
- Partitions: `max(10, min(n, 50))`, unsorted.
- No re-casting or schema reconciliation needed — transform may rely on types being correct.

## Output state (boundary contract to features)

At stage exit, transform guarantees:
- Entity-indexed on `LEASE_KID`.
- Sorted by `production_date` within each partition.
- `production_date` column added as `datetime64[ns]`.
- Categorical columns cast to declared category sets; values outside declared sets
  replaced with NA before casting.
- Invalid values (negatives where physically impossible, out-of-range values) replaced
  with the appropriate null sentinel.
- Duplicates removed; row count ≤ input row count.
- Partitions: `max(10, min(n, 50))`.

## Column derivation: `production_date`

`MONTH-YEAR` format is `"M-YYYY"` (e.g. `"1-2024"`, `"12-2024"`).
Derive `production_date` as a `datetime64[ns]` column representing the first day of
the indicated month:

```
split MONTH-YEAR on "-"
month = first element (int)
year = last element (int)
production_date = pd.to_datetime({"year": year, "month": month, "day": 1})
```

Rows where the derivation fails (non-numeric components, month=0, month=-1 for
cumulative markers per the data dictionary) must be dropped before the datetime
conversion. Log the count of dropped rows at INFO.

## Domain cleaning rules (from test-requirements and domain context)

- **TR-01 (Physical bounds):** `PRODUCTION` cannot be negative — replace negative
  values with NA and log a warning with the count.
- **TR-02 (Unit consistency):** Oil production (`PRODUCT == "O"`) above 50,000
  BBL/month for a single lease is almost certainly a unit error — flag these rows
  in a log warning (do not drop; retain with a warning flag column if specified
  in the config, otherwise just log).
- **TR-05 (Zero production):** Zeros in `PRODUCTION` are valid measurements and must
  remain as zeros, not be converted to NA.
- **TR-04 (Well completeness):** Not enforced at write time but tested: the processed
  data must preserve all monthly records per well so completeness can be verified.

## Categorical cleaning rules (ADR-003)

For `PRODUCT` (allowed: `O`, `G`) and `TWN_DIR` (allowed: `S`, `N`) and `RANGE_DIR`
(allowed: `E`, `W`):
- Before casting to `CategoricalDtype`, replace any value outside the declared set
  with `NA` (or `pd.NA`) using a vectorized mask operation.
- Cast to `pd.CategoricalDtype(categories=[...], ordered=False)`.
- Values outside the declared set must never appear in the output categorical column.

---

## Task T-01: `production_date` derivation

**Module:** `kgs_pipeline/transform.py`
**Function:** `add_production_date(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that parses
`MONTH-YEAR` into a `production_date` datetime column. Drops rows with unparseable
or non-monthly `MONTH-YEAR` values (month=0 cumulative, month=-1 starting cumulative,
non-numeric). Returns the partition with a new `production_date` column of type
`datetime64[ns]`.

### Input

`pd.DataFrame` — one partition, canonical schema, `MONTH-YEAR` as StringDtype.

### Processing logic (pseudo-code)

```
split MONTH-YEAR on "-" into month_part and year_part (last element)
mask_invalid = month_part is not numeric OR year_part is not numeric
             OR int(month_part) < 1 OR int(month_part) > 12
drop rows where mask_invalid is True; log count of dropped rows

construct production_date = pd.to_datetime(
    {"year": int(year_part), "month": int(month_part), "day": 1}
)
add production_date column to partition
return partition with production_date as datetime64[ns]
```

### Output

`pd.DataFrame` — same columns as input plus `production_date` (`datetime64[ns]`).
Input rows with invalid `MONTH-YEAR` are dropped.

### Error handling

- Non-numeric or out-of-range month/year components: drop row, log count at INFO.
- The `MONTH-YEAR` values `"0-<year>"` (yearly summary) and `"-1-<year>"`
  (starting cumulative) must be treated as invalid monthly records and dropped.

### `meta=` construction for `map_partitions`

Derive `meta` by calling `add_production_date` on an empty DataFrame that has all
canonical columns at their declared dtypes. The output must include `production_date`
as `datetime64[ns]`. Use the output of that call as the `meta=` argument.

### Test cases (in `tests/test_transform.py`)

- **Given** a partition with `MONTH-YEAR = "1-2024"`, **assert** `production_date`
  is `pd.Timestamp("2024-01-01")`.
- **Given** a partition with `MONTH-YEAR = "12-2024"`, **assert** `production_date`
  is `pd.Timestamp("2024-12-01")`.
- **Given** rows with `MONTH-YEAR = "0-2024"` (yearly marker), **assert** those rows
  are dropped.
- **Given** rows with `MONTH-YEAR = "-1-2024"` (cumulative marker), **assert** those
  rows are dropped.
- **Given** rows with non-numeric `MONTH-YEAR`, **assert** those rows are dropped.
- **TR-23**: Call `add_production_date` on `ddf._meta.copy()` (empty DataFrame) and
  assert the resulting column list and dtypes match the `meta=` argument used in the
  `map_partitions` call.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task T-02: Categorical cleaner

**Module:** `kgs_pipeline/transform.py`
**Function:** `clean_categoricals(partition: pd.DataFrame, categorical_cols: dict[str, list[str]]) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that replaces
out-of-vocabulary values in categorical columns with NA, then casts to
`pd.CategoricalDtype`. Operates on the columns specified in `categorical_cols`.

### Input

- `partition`: one pandas partition with canonical schema.
- `categorical_cols`: dict mapping column name to list of allowed categories, e.g.
  `{"PRODUCT": ["O", "G"], "TWN_DIR": ["S", "N"], "RANGE_DIR": ["E", "W"]}`.

### Processing logic (pseudo-code)

```
for each col_name, allowed in categorical_cols.items():
    mask_invalid = partition[col_name].notna() and partition[col_name] not in allowed
    set invalid values to pd.NA
    cast column to pd.CategoricalDtype(categories=allowed, ordered=False)
return partition
```

All masking and replacement must use vectorized operations — no row iteration.

### Output

`pd.DataFrame` — same schema as input, with categorical columns cast to
`CategoricalDtype` and out-of-vocabulary values replaced by NA.

### `meta=` construction

Derive `meta` by calling `clean_categoricals` on an empty DataFrame with the correct
input schema and the `categorical_cols` dict. Use the output schema as `meta=` for
`map_partitions`.

### Test cases (in `tests/test_transform.py`)

- **Given** a partition where `PRODUCT` has value `"X"` (not in `["O", "G"]`),
  **assert** that value is replaced by NA and the column is cast to CategoricalDtype.
- **Given** a partition with valid `PRODUCT` values `"O"` and `"G"`, **assert** both
  are retained and the column dtype is `CategoricalDtype`.
- **TR-23**: Verify the `meta=` argument matches actual function output on an empty
  DataFrame for each categorical column.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task T-03: Physical bounds cleaner

**Module:** `kgs_pipeline/transform.py`
**Function:** `clean_physical_bounds(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that enforces
physical constraints on production values. Replaces negative `PRODUCTION` values with
NA. Logs a warning for `PRODUCTION` > 50,000 BBL/month on oil records (TR-01, TR-02).
Preserves zero `PRODUCTION` values as valid measurements (TR-05).

### Input

`pd.DataFrame` — one partition, canonical schema, `production_date` added,
categoricals cleaned.

### Processing logic (pseudo-code)

```
# TR-01: negative production is physically impossible
negative_mask = PRODUCTION < 0
if any negative_mask:
    log WARNING "Replacing <count> negative PRODUCTION values with NA in partition"
    set PRODUCTION where negative_mask to NA

# TR-02: oil production > 50,000 BBL/month is likely a unit error
oil_high_mask = (PRODUCT == "O") AND (PRODUCTION > 50000)
if any oil_high_mask:
    log WARNING "<count> oil PRODUCTION values exceed 50,000 BBL/month — possible unit error"

# TR-05: zeros are preserved as-is — do NOT replace with NA
# (no action needed; zeros pass through unchanged)

return partition
```

### Output

`pd.DataFrame` — same schema, negative `PRODUCTION` replaced with NA, zero production
preserved.

### Test cases (in `tests/test_transform.py`)

- **TR-01**: **Given** a partition with `PRODUCTION = -100.0`, **assert** it is
  replaced with NA.
- **TR-05**: **Given** a partition with `PRODUCTION = 0.0`, **assert** it remains
  `0.0` (not NA).
- **TR-02**: **Given** an oil row with `PRODUCTION = 60000.0`, **assert** a warning
  is logged and the value is retained (not dropped or nulled).
- **Given** a partition with no invalid values, **assert** the DataFrame is returned
  unchanged.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task T-04: Deduplicator

**Module:** `kgs_pipeline/transform.py`
**Function:** `deduplicate(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that removes
duplicate rows. Deduplication key: `(LEASE_KID, MONTH-YEAR, PRODUCT)`. On duplicate
rows, retain the first occurrence.

### Input

`pd.DataFrame` — one partition, canonical schema.

### Processing logic (pseudo-code)

```
before_count = len(partition)
partition = partition.drop_duplicates(subset=["LEASE_KID", "MONTH-YEAR", "PRODUCT"], keep="first")
after_count = len(partition)
if before_count > after_count:
    log INFO "Dropped <before - after> duplicate rows in partition"
return partition
```

### Output

`pd.DataFrame` — same schema, duplicates removed. Row count ≤ input row count.

### Test cases (in `tests/test_transform.py`)

- **TR-15 (Row count reconciliation):** **Given** a partition with 3 identical rows
  (same `LEASE_KID`, `MONTH-YEAR`, `PRODUCT`), **assert** only 1 row is returned.
- **TR-15 (Idempotency):** **Given** a deduplicated partition, **assert** running
  `deduplicate` again produces the same output (same row count and values).
- **Given** a partition with no duplicates, **assert** the row count is unchanged.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task T-05: Entity indexer and sorter

**Module:** `kgs_pipeline/transform.py`
**Function:** `set_entity_index(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:** Set the Dask DataFrame index to `LEASE_KID` (the entity column)
using `ddf.set_index("LEASE_KID")`. Then sort within each partition by
`production_date` using `map_partitions`. Returns the indexed, sorted Dask DataFrame.

### Processing logic (pseudo-code)

```
ddf = ddf.set_index("LEASE_KID", sorted=False, drop=True)

# Sort within each partition by production_date
# (set_index distributed shuffle destroys any prior ordering — ADR transform H1)
ddf = ddf.map_partitions(
    lambda part: part.sort_values("production_date"),
    meta=<appropriate meta>
)
return ddf
```

### Constraint (H1 stage manifest)

The distributed shuffle triggered by `set_index` destroys any prior row ordering.
Sort by `production_date` must happen after `set_index`, not before.

### Output

`dask.dataframe.DataFrame` — indexed on `LEASE_KID`, sorted by `production_date`
within each partition.

### Test cases (in `tests/test_transform.py`)

- **TR-16 (Sort stability):** **Given** a synthetic Dask DataFrame with multiple
  partitions, after `set_entity_index`, assert that within each partition rows are
  sorted by `production_date` ascending.
- **Given** a DataFrame with 5 rows across 2 partitions for the same `LEASE_KID`,
  assert the index is `LEASE_KID` after the call.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task T-06: Transform runner

**Module:** `kgs_pipeline/transform.py`
**Function:** `run_transform(config: dict) -> dask.dataframe.DataFrame`

**Description:** Orchestrate the full transform stage. Read interim Parquet, apply
all cleaning and transformation operations in sequence via `map_partitions`, set the
entity index, repartition, and write the result to `data/processed/transform/`. Returns
the lazy Dask DataFrame.

### Input

- `config`: the `transform` section of `config.yaml` as a Python dict, with keys:
  `interim_dir`, `processed_dir`.

### Processing logic (pseudo-code)

```
ddf = dask.dataframe.read_parquet(config["interim_dir"])

# Apply partition-level transforms in sequence
categorical_cols = {"PRODUCT": ["O", "G"], "TWN_DIR": ["S", "N"], "RANGE_DIR": ["E", "W"]}
ddf = ddf.map_partitions(add_production_date, meta=<meta>)
ddf = ddf.map_partitions(clean_categoricals, categorical_cols, meta=<meta>)
ddf = ddf.map_partitions(clean_physical_bounds, meta=<meta>)
ddf = ddf.map_partitions(deduplicate, meta=<meta>)

ddf = set_entity_index(ddf)

n = ddf.npartitions
n_out = max(10, min(n, 50))
ddf = ddf.repartition(npartitions=n_out)   ← last op before write (ADR-004)

ddf.to_parquet(config["processed_dir"], engine="pyarrow", write_index=True, overwrite=True)

log INFO: partitions written, output directory
return ddf
```

### Lazy evaluation constraint (ADR-005, TR-17)

- Do not call `.compute()` inside `run_transform`.
- Return type must be `dask.dataframe.DataFrame`.

### `meta=` construction (TR-23)

For every `map_partitions` call, derive `meta` by calling the partition function on an
empty DataFrame with the correct input schema (use `ddf._meta.copy()`), not by
manually constructing a schema dict. The output column list, order, and dtypes must
match exactly.

### Error handling

- If `interim_dir` does not exist or is empty, raise `FileNotFoundError` with a
  descriptive message.
- Partition-level errors inside `map_partitions` functions propagate as Dask task
  graph failures and surface at `to_parquet` time.
- Log stage start and completion at INFO level with elapsed time.

### Test cases (in `tests/test_transform.py`)

- **TR-17**: **Assert** `run_transform` returns `dask.dataframe.DataFrame`, not
  `pandas.DataFrame`.
- **TR-11 (Data integrity):** **Given** 5 synthetic partitions with known LEASE_KID
  / MONTH-YEAR / PRODUCTION values, after `run_transform`, read back the processed
  Parquet and spot-check that PRODUCTION values for specific (LEASE_KID, MONTH-YEAR)
  combinations match the input values (sampling at least 5 rows across 3 partitions).
- **TR-12 (Data cleaning validation):** After `run_transform` on synthetic data
  containing nulls, negative PRODUCTION, and duplicates:
  - Assert no negative PRODUCTION values remain.
  - Assert dtype of `production_date` is `datetime64[ns]`.
  - Assert `PRODUCT` column dtype is CategoricalDtype.
- **TR-13 (Partition correctness):** Read processed Parquet and assert each partition
  file's `LEASE_KID` index values do not appear in any other partition (each partition
  is entity-coherent).
- **TR-14 (Schema stability):** Sample the schema from two different processed Parquet
  files and assert column names and dtypes match.
- **TR-15 (Deduplication):** Assert processed row count ≤ input row count.
- **TR-18 (Parquet readability):** Assert every processed Parquet file is readable by
  a fresh `dask.dataframe.read_parquet` call.
- **TR-23**: For each `map_partitions` call, verify the `meta=` argument matches the
  actual function output on an empty input DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task T-07: Transform stage domain and integration tests

**Module:** `tests/test_transform.py`

**Description:** Domain and integration tests that verify correctness of the complete
transform pipeline against synthetic data with known properties.

### Test cases

- **TR-04 (Well completeness check):** **Given** a synthetic dataset where lease
  `L001` has records for January, February, and March 2024 (no gaps), **assert** the
  processed output contains exactly 3 records for `L001` and no months are missing.

- **TR-05 (Zero production handling):** **Given** raw data with `PRODUCTION = 0.0`
  for a specific (LEASE_KID, MONTH-YEAR), **assert** the processed output retains
  that row with `PRODUCTION = 0.0` (not NA, not dropped).

- **TR-11 (Data integrity spot check):** **Given** a known set of (LEASE_KID,
  MONTH-YEAR, PRODUCTION) tuples in the input, **assert** the same tuples appear in
  the processed output (production values are preserved, not accidentally aggregated
  or dropped).

- **TR-12 (Data type validation):** After `run_transform`, **assert**:
  - `production_date` dtype is `datetime64[ns]`.
  - `PRODUCT` dtype is `CategoricalDtype`.
  - `PRODUCTION` dtype is `float64`.
  - `LEASE_KID` is the DataFrame index (Int64).

- **TR-13 (Partition correctness):** Read at least 3 processed Parquet partitions.
  For each partition, collect the unique `LEASE_KID` index values. **Assert** no
  `LEASE_KID` appears in more than one partition.

- **TR-14 (Schema stability):** Sample schema from two processed Parquet files.
  **Assert** column names and dtypes are identical.

- **TR-15 (Row count and idempotency):** **Assert** processed row count ≤ raw input
  row count. Run the cleaning pipeline twice on the same input; **assert** the output
  row count is the same both times.

- **TR-16 (Sort stability):** For a multi-partition output, read back two consecutive
  partitions for the same `LEASE_KID`. **Assert** the last `production_date` in
  partition N is ≤ the first `production_date` in partition N+1 for that entity.

**Definition of done:** All test cases pass, ruff and mypy report no errors,
`requirements.txt` updated with all third-party packages imported in this task.
