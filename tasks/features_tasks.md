# Features Stage — Task Specifications

## Overview

The features stage reads the entity-indexed, date-sorted, cleaned Parquet dataset from
`data/processed/transform/`, computes all derived features required for ML workflows,
and writes the final ML-ready Parquet dataset to `data/processed/features/`. Processing
is parallel using Dask's distributed scheduler (CPU-bound, per ADR-001). All
time-dependent features depend on correct temporal ordering within each entity group —
the sort established in transform must be preserved at the point each feature is
computed (H1 stage manifest).

## Architecture constraints (from ADRs)

- **ADR-001**: Features is CPU-bound — use Dask distributed scheduler.
- **ADR-002**: All operations must be vectorized — no per-row iteration, no iterating
  over entity groups. Use grouped `transform` operations.
- **ADR-004**: Output must be `max(10, min(n, 50))` partitions. Repartition is the
  last operation before `to_parquet`.
- **ADR-005**: Task graph remains lazy until the final Parquet write.
- **TR-17**: All stage functions must return `dask.dataframe.DataFrame`.
- **TR-23**: Every `map_partitions` call must pass a `meta=` argument derived by
  calling the function on an empty DataFrame matching the input schema.

## Input state (from boundary-transform-features.md)

Features receives from transform:
- Entity-indexed on `LEASE_KID`.
- Sorted by `production_date` within each partition.
- Categoricals cast and clean.
- Invalid values replaced with null sentinel.
- Partitions: `max(10, min(n, 50))`.
- No re-sorting, re-indexing, or re-casting needed.

## Output state

All input columns plus the following derived feature columns, written as Parquet to
`data/processed/features/`:

| Column | Description | dtype |
|---|---|---|
| production_date | First day of production month | datetime64[ns] |
| oil_bbl | Monthly oil production (PRODUCT=O rows) | float64 |
| gas_mcf | Monthly gas production (PRODUCT=G rows) | float64 |
| water_bbl | Monthly water production | float64 |
| cum_oil | Cumulative oil production per lease (Np) | float64 |
| cum_gas | Cumulative gas production per lease (Gp) | float64 |
| cum_water | Cumulative water production per lease (Wp) | float64 |
| gor | Gas-oil ratio: gas_mcf / oil_bbl | float64 |
| water_cut | Water cut: water_bbl / (oil_bbl + water_bbl) | float64 |
| decline_rate | Period-over-period oil decline rate, clipped to [-1.0, 10.0] | float64 |
| oil_roll_3 | 3-month rolling average of oil_bbl per lease | float64 |
| oil_roll_6 | 6-month rolling average of oil_bbl per lease | float64 |
| gas_roll_3 | 3-month rolling average of gas_mcf per lease | float64 |
| gas_roll_6 | 6-month rolling average of gas_mcf per lease | float64 |
| water_roll_3 | 3-month rolling average of water_bbl per lease | float64 |
| water_roll_6 | 6-month rolling average of water_bbl per lease | float64 |
| oil_lag_1 | Lag-1 oil_bbl (previous month's oil production) | float64 |
| gas_lag_1 | Lag-1 gas_mcf (previous month's gas production) | float64 |
| water_lag_1 | Lag-1 water_bbl (previous month's water production) | float64 |

### Notes on wide-to-long pivot

The raw transform output has one row per `(LEASE_KID, MONTH-YEAR, PRODUCT)` —
separate rows for oil (`O`) and gas (`G`). The features stage must pivot this
into one row per `(LEASE_KID, production_date)` with separate `oil_bbl` and
`gas_mcf` columns before computing any derived features. `water_bbl` is added
as a zero-filled column (KGS data does not report water separately at the lease
level; the column is required by downstream ML schema per TR-19).

### Pivot logic (pseudo-code)

```
oil_ddf  = ddf[ddf["PRODUCT"] == "O"][["LEASE_KID", "production_date", "PRODUCTION", ...]]
           .rename({"PRODUCTION": "oil_bbl"})
gas_ddf  = ddf[ddf["PRODUCT"] == "G"][["LEASE_KID", "production_date", "PRODUCTION"]]
           .rename({"PRODUCTION": "gas_mcf"})
merged   = oil_ddf.merge(gas_ddf, on=["LEASE_KID", "production_date"], how="outer")
merged["water_bbl"] = 0.0
fill NA in oil_bbl, gas_mcf, water_bbl with 0.0 after outer merge
```

---

## Task F-01: Wide-to-long pivot

**Module:** `kgs_pipeline/features.py`
**Function:** `pivot_oil_gas(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:** Pivot the transform output from one row per (LEASE_KID, MONTH-YEAR,
PRODUCT) to one row per (LEASE_KID, production_date) with separate `oil_bbl`,
`gas_mcf`, and `water_bbl` columns. This is the prerequisite for all per-well
feature computations.

### Input

`dask.dataframe.DataFrame` — entity-indexed on LEASE_KID, sorted by production_date,
contains `PRODUCT` (categorical, `O`/`G`) and `PRODUCTION` (float64) columns plus
all other canonical columns.

### Processing logic (pseudo-code)

```
reset index to move LEASE_KID from index to column (for merging)
separate oil rows (PRODUCT == "O") → keep LEASE_KID, production_date, PRODUCTION
    rename PRODUCTION → oil_bbl
    keep other canonical metadata columns (LEASE, OPERATOR, COUNTY, FIELD,
    PRODUCING_ZONE, API_NUMBER, LATITUDE, LONGITUDE, TOWNSHIP, RANGE, SECTION, etc.)
separate gas rows (PRODUCT == "G") → keep LEASE_KID, production_date, PRODUCTION
    rename PRODUCTION → gas_mcf
merge oil and gas on (LEASE_KID, production_date) with how="outer"
fill NA in oil_bbl with 0.0
fill NA in gas_mcf with 0.0
add water_bbl column filled with 0.0
return dask.dataframe with LEASE_KID and production_date as columns (index reset)
```

### Output

`dask.dataframe.DataFrame` — one row per (LEASE_KID, production_date), with columns
`oil_bbl`, `gas_mcf`, `water_bbl` and all retained metadata columns.

### Error handling

- If neither oil nor gas rows are present, return an empty Dask DataFrame with the
  correct schema.
- Log counts of oil rows, gas rows, and merged rows at INFO level.

### Test cases (in `tests/test_features.py`)

- **Given** a synthetic Dask DataFrame with 2 oil rows and 2 gas rows for the same
  LEASE_KID and different production_dates, **assert** the pivot produces 2 merged
  rows with both `oil_bbl` and `gas_mcf` populated.
- **Given** a lease with only oil production (no gas rows), **assert** `gas_mcf`
  is `0.0` (not NA) in the merged output.
- **Given** a lease with only gas production, **assert** `oil_bbl` is `0.0` (not NA).
- **Assert** `water_bbl` is `0.0` for all rows.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task F-02: Cumulative production

**Module:** `kgs_pipeline/features.py`
**Function:** `add_cumulative_production(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that computes
cumulative oil, gas, and water production per lease (`LEASE_KID`) within the partition.
Since the transform boundary contract guarantees temporal ordering within each
partition, grouped cumulative sums are valid within a single partition. Cumulative
columns are `cum_oil`, `cum_gas`, `cum_water`.

### Input

`pd.DataFrame` — one partition, pivoted schema, sorted by production_date within each
LEASE_KID group.

### Processing logic (pseudo-code)

```
for each of (oil_bbl → cum_oil), (gas_mcf → cum_gas), (water_bbl → cum_water):
    partition[cum_col] = partition.groupby("LEASE_KID")[src_col].cumsum()
return partition
```

All operations must use vectorized grouped `cumsum` — no iteration over LEASE_KID
groups.

### Output

`pd.DataFrame` — same columns plus `cum_oil`, `cum_gas`, `cum_water` (float64).

### Domain correctness (TR-03, TR-08)

- Cumulative values must be monotonically non-decreasing within each LEASE_KID group.
- Zero-production months must cause cumulative values to remain flat (not decrease).
- The cumulative sum of zero values (`oil_bbl = 0.0`) contributes zero to the running
  total — flat period, not a decrease.

### `meta=` construction (TR-23)

Derive `meta` by calling `add_cumulative_production` on an empty DataFrame matching the
input schema. Verify column list and dtypes match the `meta=` argument.

### Test cases (in `tests/test_features.py`)

- **TR-03 (Monotonicity):** **Given** a synthetic 6-month sequence for one lease,
  **assert** `cum_oil` is non-decreasing across all months.
- **TR-08a (Flat periods):** **Given** a sequence `[100, 0, 0, 50]` for `oil_bbl`,
  **assert** `cum_oil` is `[100, 100, 100, 150]` — zero months are flat, not reset.
- **TR-08b (Zero at start):** **Given** `oil_bbl = [0, 0, 100]`, **assert**
  `cum_oil = [0, 0, 100]`.
- **TR-08c (Resumption after shut-in):** **Given** `[50, 0, 30]`, **assert**
  `cum_oil = [50, 50, 80]`.
- **TR-23**: Verify `meta=` argument for the `map_partitions` call matches actual
  function output on an empty DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task F-03: GOR and water cut

**Module:** `kgs_pipeline/features.py`
**Function:** `add_ratio_features(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that computes
Gas-Oil Ratio (GOR) and water cut for each row. Handles zero denominators explicitly
per TR-06 and TR-10.

### Input

`pd.DataFrame` — one partition, pivoted schema with `oil_bbl`, `gas_mcf`, `water_bbl`.

### GOR formula (TR-06, TR-09d)

```
gor = gas_mcf / oil_bbl
```

Zero-denominator handling:
- `oil_bbl == 0` and `gas_mcf > 0`  → `gor = NaN` (undefined, not an exception)
- `oil_bbl == 0` and `gas_mcf == 0` → `gor = NaN` (shut-in well, both zero)
- `oil_bbl > 0`  and `gas_mcf == 0` → `gor = 0.0` (valid, no gas produced)

Implementation: use vectorized `where`/`mask` operations — do not trigger a
`ZeroDivisionError`.

### Water cut formula (TR-09e, TR-10)

```
water_cut = water_bbl / (oil_bbl + water_bbl)
```

Zero-denominator handling (TR-10):
- `oil_bbl == 0` and `water_bbl == 0` → `water_cut = NaN` (shut-in, undefined)
- `oil_bbl > 0`  and `water_bbl == 0` → `water_cut = 0.0` (valid, no water)
- `oil_bbl == 0` and `water_bbl > 0`  → `water_cut = 1.0` (valid, 100% water cut)

Neither 0.0 nor 1.0 should be treated as outliers or flagged.

### Output

`pd.DataFrame` — same columns plus `gor` (float64) and `water_cut` (float64).

### Test cases (in `tests/test_features.py`)

- **TR-06a**: **Given** `oil_bbl=0`, `gas_mcf=50`, **assert** `gor` is `NaN` (not
  an exception).
- **TR-06b**: **Given** `oil_bbl=0`, `gas_mcf=0`, **assert** `gor` is `NaN` or `0`
  (not an exception; document the sentinel in the spec).
- **TR-06c**: **Given** `oil_bbl=100`, `gas_mcf=0`, **assert** `gor == 0.0`.
- **TR-09d**: **Given** `oil_bbl=100`, `gas_mcf=500`, **assert** `gor == 5.0`.
- **TR-10a**: **Given** `water_bbl=0`, `oil_bbl=200`, **assert** `water_cut == 0.0`
  and the row is retained.
- **TR-10b**: **Given** `oil_bbl=0`, `water_bbl=100`, **assert** `water_cut == 1.0`
  and the row is retained.
- **TR-10c**: **Given** `oil_bbl=-10`, `water_bbl=110`, **assert** the value is
  outside `[0, 1]` — verify no special handling treats it as valid.
- **TR-23**: Verify `meta=` matches actual function output on empty DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task F-04: Decline rate

**Module:** `kgs_pipeline/features.py`
**Function:** `add_decline_rate(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that computes
the period-over-period oil production decline rate per lease and clips it to the
engineering bounds `[-1.0, 10.0]` (TR-07).

### Input

`pd.DataFrame` — one partition, pivoted schema with `oil_bbl`, sorted by
`production_date` within each `LEASE_KID`.

### Formula

```
decline_rate = (oil_bbl_prev - oil_bbl_curr) / oil_bbl_prev
```

where `oil_bbl_prev` is the previous month's oil production for the same LEASE_KID.

### Processing logic (pseudo-code)

```
prev_oil = partition.groupby("LEASE_KID")["oil_bbl"].shift(1)

# Handle zero previous production before computing the ratio (TR-07d)
# When prev_oil == 0 and oil_bbl == 0: decline_rate = 0.0 (shut-in, no change)
# When prev_oil == 0 and oil_bbl > 0: decline_rate = -1.0 (capped, came back online)
# When prev_oil > 0: decline_rate = (prev_oil - oil_bbl) / prev_oil
# When prev_oil is NaN (first month per lease): decline_rate = NaN

decline_rate = vectorized computation of formula above (use where/mask)
decline_rate = decline_rate.clip(lower=-1.0, upper=10.0)

partition["decline_rate"] = decline_rate
return partition
```

All operations must be vectorized using grouped `shift` and vectorized arithmetic —
no iteration over LEASE_KID groups.

### Output

`pd.DataFrame` — same columns plus `decline_rate` (float64), clipped to `[-1.0, 10.0]`.

### Test cases (in `tests/test_features.py`)

- **TR-07a**: **Given** a synthetic sequence where computed decline rate is `-1.5`,
  **assert** `decline_rate == -1.0` (clipped at lower bound).
- **TR-07b**: **Given** a computed decline rate of `15.0`, **assert**
  `decline_rate == 10.0` (clipped at upper bound).
- **TR-07c**: **Given** a computed decline rate of `0.3`, **assert**
  `decline_rate == 0.3` (within bounds, unchanged).
- **TR-07d**: **Given** a two-month sequence where `oil_bbl = [0, 0]` for the same
  lease, **assert** no unclipped extreme value is produced — the raw computation for
  zero-denominator is handled before the clip.
- **First month sentinel**: **Given** the first month of a lease's production,
  **assert** `decline_rate` is `NaN` (no previous month available).
- **TR-23**: Verify `meta=` matches actual function output on empty DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task F-05: Rolling averages

**Module:** `kgs_pipeline/features.py`
**Function:** `add_rolling_features(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that computes
3-month and 6-month rolling averages of `oil_bbl`, `gas_mcf`, and `water_bbl` per
lease. Rolling windows use `min_periods=1` unless spec requires partial windows to
be NaN (see TR-09b below).

### Input

`pd.DataFrame` — one partition, pivoted schema, sorted by `production_date` within
each `LEASE_KID`.

### Columns computed

```
oil_roll_3   = rolling mean of oil_bbl  (window=3, grouped by LEASE_KID)
oil_roll_6   = rolling mean of oil_bbl  (window=6, grouped by LEASE_KID)
gas_roll_3   = rolling mean of gas_mcf  (window=3, grouped by LEASE_KID)
gas_roll_6   = rolling mean of gas_mcf  (window=6, grouped by LEASE_KID)
water_roll_3 = rolling mean of water_bbl (window=3, grouped by LEASE_KID)
water_roll_6 = rolling mean of water_bbl (window=6, grouped by LEASE_KID)
```

### Window behavior for short histories (TR-09b)

When the rolling window is larger than the available history (e.g. first 2 months of
a well's life with `window=6`), the result must be `NaN` — not silently zero and not
a wrong partial value. Use `min_periods=window` so that partial windows return NaN.

### Processing logic (pseudo-code)

```
for each (src_col, window, out_col) in rolling spec:
    partition[out_col] = partition.groupby("LEASE_KID")[src_col]
                                  .transform(lambda s: s.rolling(window, min_periods=window).mean())
return partition
```

All operations must use vectorized grouped rolling — no iteration over LEASE_KID groups.

### Output

`pd.DataFrame` — same columns plus 6 new rolling average columns (float64).

### Test cases (in `tests/test_features.py`)

- **TR-09a (Correctness):** **Given** `oil_bbl = [100, 200, 300]` for a single lease,
  **assert** `oil_roll_3` for month 3 is `200.0`
  (mean of 100, 200, 300 = 200.0).
- **TR-09b (Partial window):** **Given** only 2 months of history for a lease,
  **assert** `oil_roll_3` is `NaN` for both months (window=3, min_periods=3, history < 3).
- **TR-09b (6-month window):** **Given** 5 months of history, **assert** `oil_roll_6`
  is `NaN` for all 5 months (window=6, min_periods=6, history < 6).
- **Given** 6 months of history, **assert** `oil_roll_6` for month 6 equals the mean
  of months 1–6 (hand-computed exact value).
- **TR-23**: Verify `meta=` matches actual function output on empty DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task F-06: Lag features

**Module:** `kgs_pipeline/features.py`
**Function:** `add_lag_features(partition: pd.DataFrame) -> pd.DataFrame`

**Description:** Partition-level function (used with `map_partitions`) that computes
lag-1 features for `oil_bbl`, `gas_mcf`, and `water_bbl` per lease. Lag features
are the previous month's production values.

### Input

`pd.DataFrame` — one partition, pivoted schema, sorted by `production_date` within
each `LEASE_KID`.

### Columns computed

```
oil_lag_1   = previous month's oil_bbl   (shift(1) within LEASE_KID group)
gas_lag_1   = previous month's gas_mcf   (shift(1) within LEASE_KID group)
water_lag_1 = previous month's water_bbl (shift(1) within LEASE_KID group)
```

### Processing logic (pseudo-code)

```
for each (src_col, out_col) in [("oil_bbl", "oil_lag_1"), ...]:
    partition[out_col] = partition.groupby("LEASE_KID")[src_col].shift(1)
return partition
```

First month per lease produces `NaN` lag (no previous month).

### Output

`pd.DataFrame` — same columns plus `oil_lag_1`, `gas_lag_1`, `water_lag_1` (float64).

### Test cases (in `tests/test_features.py`)

- **TR-09c (Lag-1 correctness):** **Given** `oil_bbl = [100, 200, 300]` for a single
  lease across 3 consecutive months, **assert** `oil_lag_1 = [NaN, 100, 200]`.
  Verified for at least 3 consecutive months.
- **Given** the first month of a lease's record, **assert** `oil_lag_1` is `NaN`.
- **Given** two different leases in the same partition, **assert** lag-1 values do
  not bleed across lease boundaries (lag for LEASE_KID A's first month is always NaN,
  not the last value of LEASE_KID B).
- **TR-23**: Verify `meta=` matches actual function output on empty DataFrame.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task F-07: Features runner

**Module:** `kgs_pipeline/features.py`
**Function:** `run_features(config: dict) -> dask.dataframe.DataFrame`

**Description:** Orchestrate the full features stage. Read processed Parquet from
`data/processed/transform/`, pivot the oil/gas rows, apply all feature computation
functions via `map_partitions`, repartition, and write the final ML-ready Parquet to
`data/processed/features/`. Returns the lazy Dask DataFrame.

### Input

- `config`: the `features` section of `config.yaml` as a Python dict, with keys:
  `processed_dir` (transform output), `output_dir` (features output).

### Processing logic (pseudo-code)

```
ddf = dask.dataframe.read_parquet(config["processed_dir"])

ddf = pivot_oil_gas(ddf)

# Apply all partition-level feature functions in sequence
ddf = ddf.map_partitions(add_cumulative_production, meta=<meta>)
ddf = ddf.map_partitions(add_ratio_features,        meta=<meta>)
ddf = ddf.map_partitions(add_decline_rate,           meta=<meta>)
ddf = ddf.map_partitions(add_rolling_features,       meta=<meta>)
ddf = ddf.map_partitions(add_lag_features,           meta=<meta>)

n_out = max(10, min(ddf.npartitions, 50))
ddf = ddf.repartition(npartitions=n_out)    ← last op before write (ADR-004)

ddf.to_parquet(config["output_dir"], engine="pyarrow", write_index=False, overwrite=True)

log INFO: partitions written, output directory
return ddf
```

### Lazy evaluation constraint (ADR-005, TR-17)

- Do not call `.compute()` inside `run_features`.
- The Parquet write is the only materialization point.
- Return type must be `dask.dataframe.DataFrame`.

### `meta=` construction (TR-23)

For each `map_partitions` call, derive `meta` by calling the partition function on
an empty DataFrame matching the input schema, not by manual construction. The output
column list, column order, and dtypes must match exactly.

### Error handling

- If `processed_dir` does not exist or is empty, raise `FileNotFoundError`.
- Log stage start and completion at INFO level with elapsed time.

### Test cases (in `tests/test_features.py`)

- **TR-17**: **Assert** `run_features` returns `dask.dataframe.DataFrame`, not
  `pandas.DataFrame`.
- **TR-18 (Parquet readability):** **Assert** every features Parquet file written to
  `output_dir` is readable by a fresh `dask.dataframe.read_parquet` call without error.
- **TR-19 (Feature column presence):** **Given** a synthetic single-lease input,
  after `run_features`, **assert** the output Parquet contains all required columns:
  `LEASE_KID`, `production_date`, `oil_bbl`, `gas_mcf`, `water_bbl`, `cum_oil`,
  `cum_gas`, `cum_water`, `gor`, `water_cut`, `decline_rate`, `oil_roll_3`,
  `oil_roll_6`, `gas_roll_3`, `gas_roll_6`, `water_roll_3`, `water_roll_6`,
  `oil_lag_1`, `gas_lag_1`, `water_lag_1`.
- **TR-14 (Schema stability):** Sample schema from two different features Parquet files
  and assert column names and dtypes are identical.
- **TR-23**: For each `map_partitions` call in `run_features`, verify the `meta=`
  argument matches the actual function output on an empty DataFrame.
- **Given** `processed_dir` does not exist, **assert** `FileNotFoundError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy
report no errors, `requirements.txt` updated with all third-party packages imported
in this task.

---

## Task F-08: Features domain and integration tests

**Module:** `tests/test_features.py`

**Description:** Domain correctness and integration tests for the complete features
pipeline using synthetic data with known exact values.

### Test cases

- **TR-03 (Decline curve monotonicity):** **Given** a synthetic 12-month well sequence
  with non-zero production, **assert** `cum_oil`, `cum_gas`, `cum_water` are
  monotonically non-decreasing across all 12 months.

- **TR-06 (GOR zero-denominator):** See Task F-03 test cases — verify all three
  cases (oil=0/gas>0, both=0, oil>0/gas=0) behave correctly end-to-end through
  `run_features`.

- **TR-07 (Decline rate clip bounds):** See Task F-04 test cases — verify lower
  bound (-1.0), upper bound (10.0), within bounds, and shut-in (both zero)
  through `run_features`.

- **TR-08 (Cumulative flat periods):** See Task F-02 test cases — verify zero-
  production flat periods, zero at start, and resumption after shut-in through
  `run_features`.

- **TR-09 (Feature calculation correctness):**
  - Rolling averages: given `oil_bbl = [100, 200, 300, 400, 500, 600]` for one lease,
    assert `oil_roll_3` for month 3 is `200.0`, month 6 is `500.0`. Verify by
    hand-computed exact values.
  - Partial window NaN: given only 2 months of history, assert `oil_roll_3 = NaN`.
  - Lag-1: given `oil_bbl = [100, 200, 300]`, assert `oil_lag_1 = [NaN, 100, 200]`
    for months 1, 2, 3.
  - GOR formula: verify `gor = gas_mcf / oil_bbl`, not inverted. Given
    `gas_mcf=500, oil_bbl=100`, assert `gor == 5.0`.
  - Water cut: verify denominator uses total liquid. Given `water_bbl=30, oil_bbl=70`,
    assert `water_cut == 0.3`.

- **TR-10 (Water cut boundary):**
  - `water_bbl=0, oil_bbl>0` → `water_cut == 0.0`, row retained.
  - `oil_bbl=0, water_bbl>0` → `water_cut == 1.0`, row retained.
  - Only values outside `[0, 1]` treated as invalid.

- **TR-19 (Feature column presence):** Assert the complete required column list is
  present in the features output for a synthetic single-lease input.

- **TR-23 (meta= schema consistency):** For `add_rolling_features`,
  `add_cumulative_production`, and `add_decline_rate`, call each function on an empty
  DataFrame matching the input schema and assert that the resulting column list and
  dtypes match the `meta=` argument used in the corresponding `map_partitions` call.

**Definition of done:** All test cases pass, ruff and mypy report no errors,
`requirements.txt` updated with all third-party packages imported in this task.

---

## Task F-09: Pipeline orchestrator

**Module:** `kgs_pipeline/pipeline.py`
**Function:** `main() -> None` (registered as CLI entry point)

**Description:** The top-level pipeline entry point that coordinates all four stages
in sequence. Reads `config.yaml`, initializes logging, initializes the Dask distributed
scheduler (after acquire, before ingest), runs each stage with per-stage timing and
error logging, and stops on any stage failure.

### CLI interface

The entry point must accept an optional `--stages` argument that takes a list of stage
names to run (e.g. `--stages acquire ingest`). When `--stages` is not provided,
defaults to running all four stages in order: `acquire ingest transform features`.

### Processing logic (pseudo-code)

```
parse CLI arguments (--stages, optional config path)
read config.yaml
set up dual-channel logging (console + log file) from config.logging section
    create log output directory if it does not exist
    log level and log file path from config

stages_to_run = parsed --stages argument or ["acquire", "ingest", "transform", "features"]

if "acquire" in stages_to_run:
    log INFO "Starting acquire stage"
    t0 = now()
    run_acquire(config["acquire"])
    log INFO "Acquire completed in <elapsed>s"

initialize Dask distributed client (if not already running) per config.dask:
    if config.dask.scheduler == "local":
        create LocalCluster with n_workers, threads_per_worker, memory_limit
        create Client from cluster
    else:
        create Client connecting to config.dask.scheduler URL
    log INFO Dask dashboard URL

for stage in ["ingest", "transform", "features"]:
    if stage in stages_to_run:
        log INFO "Starting <stage> stage"
        t0 = now()
        try:
            run_<stage>(config[stage])
            log INFO "<stage> completed in <elapsed>s"
        except Exception as e:
            log ERROR "<stage> failed: <e>"
            raise SystemExit(1)   ← downstream stages must not run

close Dask client on exit
```

### Logging requirements (ADR-006)

- Log configuration (file path, level) read from `config.yaml`; never hardcoded.
- Dual-channel: `StreamHandler` (console) and `FileHandler` (log file).
- Log output directory created at startup if absent.
- Log setup happens before any stage runs.
- Stages must not configure their own log handlers.

### Dask scheduler initialization (build-env-manifest)

- Initialized after `acquire` completes and before `ingest` begins.
- Uses settings from `config.dask`.
- If a client already exists (e.g. in tests), reuse it — do not create a second cluster.
- Dashboard URL logged at INFO level after initialization.

### Test cases (in `tests/test_pipeline.py` or appended to `tests/test_features.py`)

- **Given** `--stages acquire` CLI argument with all stages mocked, **assert** only
  `run_acquire` is called.
- **Given** `--stages ingest transform`, **assert** only `run_ingest` and
  `run_transform` are called (not acquire or features).
- **Given** `run_ingest` raises an exception, **assert** `run_transform` and
  `run_features` are not called and the process exits with a non-zero code.
- **Given** a valid `config.yaml`, **assert** logging is configured before any stage
  function is called.
- **Assert** the Dask client is initialized after acquire and before ingest.

**Definition of done:** Pipeline entry point implemented, all test cases pass, ruff
and mypy report no errors, `requirements.txt` updated with all third-party packages
imported in this task.
