# Features Stage — Task Specifications

## Context and Design Decisions

The features stage reads the transformed Parquet output from `data/interim/transformed/`,
computes all derived ML features (cumulative production, ratios, rolling averages, lag
features, decline rates, encoded categoricals), and writes ML-ready Parquet files to
`data/processed/`.

**Key ADRs governing this stage:**
- ADR-001: Use Dask distributed scheduler for CPU-bound features
- ADR-002: All operations must be vectorized — no per-row iteration; per-entity
  aggregations must use vectorized grouped transformations
- ADR-003: Meta derivation must share the same code path as function execution —
  derive meta by calling the actual function on a minimal real input sliced to zero rows
- ADR-004: Parquet inter-stage format; partition count = max(10, min(n, 50));
  repartition is the last structural operation before writing
- ADR-005: Task graph stays lazy until the final Parquet write; all independent feature
  computations batched into a single compute call
- ADR-006: Dual-channel logging; log config from config.yaml
- ADR-007: Python 3.11+; pytest for tests

**Input state (from transform boundary contract):**
- Entity-indexed on LEASE_KID
- Sorted by `production_date` within each partition
- Categoricals cast to declared category sets
- Invalid values replaced with appropriate null sentinel
- Partitioned to max(10, min(n, 50))
- No re-sorting, re-indexing, re-casting, or re-partitioning required before computation

**Output state:**
- All input columns plus all derived feature columns listed below
- ML-ready Parquet files in `data/processed/`

**Shared state hazards (from stage manifest):**
- H1: All time-dependent features require correct temporal ordering within each entity
  group — the sort from transform must be preserved at the point each feature is computed

**Package name:** `kgs_pipeline`
**Module path:** `kgs_pipeline/features.py`

**Input:**
- Partitioned Parquet in `data/interim/transformed/` (output of transform stage)

**Output:**
- Partitioned Parquet in `data/processed/` (`features.parquet` partitioned directory)
- Feature manifest JSON at `data/processed/feature_manifest.json`

---

## Feature Definitions

All feature columns computed by this stage, with their definitions:

### Cumulative production (per LEASE_KID, per PRODUCT)
| Feature column | Definition |
|---|---|
| `cum_oil` | Cumulative sum of PRODUCTION where PRODUCT=="O", grouped by LEASE_KID, sorted by production_date |
| `cum_gas` | Cumulative sum of PRODUCTION where PRODUCT=="G", grouped by LEASE_KID, sorted by production_date |

### Production ratios
| Feature column | Definition |
|---|---|
| `gor` | gas_mcf / oil_bbl per (LEASE_KID, MONTH-YEAR): gas PRODUCTION / oil PRODUCTION; NaN when oil==0 and gas>0; 0.0 when both==0 (see TR-06) |
| `water_cut` | Not computable from this dataset (no water production column in KGS data); set to `pd.NA` for all rows |

### Decline rate
| Feature column | Definition |
|---|---|
| `decline_rate` | Period-over-period production change per (LEASE_KID, PRODUCT): (prod_t - prod_{t-1}) / prod_{t-1}; clipped to [-1.0, 10.0] (TR-07); NaN for first record of each lease-product |

### Rolling averages (per LEASE_KID, per PRODUCT, sorted by production_date)
| Feature column | Definition |
|---|---|
| `rolling_3m_production` | 3-month rolling mean of PRODUCTION; min_periods=1 for the first records |
| `rolling_6m_production` | 6-month rolling mean of PRODUCTION; min_periods=1 for the first records |

**Rolling window behavior (TR-09b):** When the rolling window is larger than the
available history (e.g. first 2 months of a well's life), the result is the partial
window mean using `min_periods=1` — not NaN, not zero. The spec requires `min_periods=1`
explicitly. A downstream test may assert partial window behavior against hand-computed
values.

### Lag features (per LEASE_KID, per PRODUCT)
| Feature column | Definition |
|---|---|
| `lag_1_production` | PRODUCTION value from the immediately preceding month for the same LEASE_KID and PRODUCT; NaN for first record |

### Encoded categoricals
| Feature column | Definition |
|---|---|
| `county_code` | Integer label encoding of COUNTY; consistent mapping stored in feature manifest |
| `field_code` | Integer label encoding of FIELD; consistent mapping stored in feature manifest |
| `producing_zone_code` | Integer label encoding of PRODUCING_ZONE; consistent mapping stored in feature manifest |
| `operator_code` | Integer label encoding of OPERATOR; consistent mapping stored in feature manifest |
| `product_code` | Integer label encoding of PRODUCT; 0 for "O", 1 for "G" |

### Temporal features (from production_date)
| Feature column | Definition |
|---|---|
| `year` | Calendar year extracted from production_date |
| `month` | Calendar month (1–12) extracted from production_date |
| `quarter` | Calendar quarter (1–4) extracted from production_date |
| `days_in_month` | Number of days in the production month |

---

## Required output columns (TR-19)

The features Parquet output must contain all of the following columns (minimum required
set per TR-19). Any run that silently drops a feature column must be caught by the
column presence test:

`LEASE_KID`, `LEASE`, `OPERATOR`, `COUNTY`, `FIELD`, `PRODUCING_ZONE`, `PRODUCT`,
`PRODUCTION`, `WELLS`, `MONTH-YEAR`, `production_date`, `cum_oil`, `cum_gas`,
`gor`, `decline_rate`, `rolling_3m_production`, `rolling_6m_production`,
`lag_1_production`, `county_code`, `field_code`, `producing_zone_code`,
`operator_code`, `product_code`, `year`, `month`, `quarter`, `days_in_month`,
`has_date_gap`, `production_unit_flag`

---

## Task 01: Implement cumulative production features

**Module:** `kgs_pipeline/features.py`
**Function:** `add_cumulative_production(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute cumulative production features for oil and gas per lease. Operates on a pandas
partition (called via `map_partitions`). The partition is assumed to be sorted by
production_date (guaranteed by transform stage).

**Steps:**
- For oil rows (PRODUCT == "O"): compute cumulative sum of PRODUCTION grouped by
  LEASE_KID, using `groupby("LEASE_KID")["PRODUCTION"].cumsum()`; store as `cum_oil`
- For gas rows (PRODUCT == "G"): compute cumulative sum of PRODUCTION grouped by
  LEASE_KID; store as `cum_gas`
- For rows where the opposite product type: set `cum_oil` or `cum_gas` to `pd.NA`
  (a gas row has no `cum_oil` value)
- Return the DataFrame with `cum_oil` and `cum_gas` columns appended

**Critical correctness requirement (TR-03, TR-08):**
- Cumulative values must be monotonically non-decreasing within each (LEASE_KID, PRODUCT)
  group over time — a well cannot un-produce oil
- Zero-production months must produce a flat cumulative value equal to the prior month's
  cumulative (not a decrease, not a jump forward) (TR-08)

**Meta for map_partitions (ADR-003):**
- Derive meta by calling `add_cumulative_production` on a minimal one-row real input
  and slicing to zero rows

**Test cases (TR-03, TR-08):**
- Given a synthetic oil sequence `[100, 200, 0, 150]` for one lease, assert cumulative
  values are `[100, 300, 300, 450]` (zero month stays flat) (TR-08a)
- Given a sequence starting with zeros `[0, 0, 100]`, assert cumulative values are
  `[0, 0, 100]` (TR-08b — zero at start of record)
- Given resumption after shut-in `[100, 0, 50]`, assert cumulative is `[100, 100, 150]`
  (TR-08c)
- Assert the cumulative series is monotonically non-decreasing for any synthetic input (TR-03)
- Assert gas rows have `cum_oil = pd.NA` and oil rows have `cum_gas = pd.NA`

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 02: Implement GOR (Gas-Oil Ratio) feature

**Module:** `kgs_pipeline/features.py`
**Function:** `add_gor(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute Gas-Oil Ratio per (LEASE_KID, MONTH-YEAR) pair. The dataset has separate oil and
gas rows per lease per month; GOR requires pairing them. Operates on a pandas partition.

**GOR definition:** `gas_mcf / oil_bbl` where gas_mcf is PRODUCTION for PRODUCT=="G"
rows and oil_bbl is PRODUCTION for PRODUCT=="O" rows for the same LEASE_KID and MONTH-YEAR.

**Steps:**
- Pivot or merge to align oil and gas production for the same (LEASE_KID, MONTH-YEAR)
  within the partition — use a vectorized groupby/pivot approach
- Apply the zero-denominator rules (TR-06):
  - oil_bbl > 0: `gor = gas_mcf / oil_bbl`
  - oil_bbl == 0 and gas_mcf > 0: `gor = NaN` (not an exception — physically valid gas well)
  - oil_bbl == 0 and gas_mcf == 0 (shut-in): `gor = 0.0`
  - oil_bbl > 0 and gas_mcf == 0: `gor = 0.0`
- Assign the computed `gor` value back to each row (both oil and gas rows get the same
  GOR value for that lease-month; or assign NaN to the gas row and the GOR value to the
  oil row — document the choice in the function docstring)
- Return the DataFrame with `gor` column appended

**Implementation constraint (ADR-002, TR-06):**
- Must not raise `ZeroDivisionError` for any input combination
- Must not use per-row iteration; use vectorized operations

**Test cases (TR-06a, TR-06b, TR-06c):**
- Given oil_bbl=0, gas_mcf=100: assert `gor` is NaN (not an exception) (TR-06a)
- Given oil_bbl=0, gas_mcf=0 (shut-in): assert `gor` is 0.0 or NaN, not an exception
  (TR-06b)
- Given oil_bbl=100, gas_mcf=0: assert `gor` is 0.0 (TR-06c)
- Given oil_bbl=100, gas_mcf=50000: assert `gor` is 500.0
- Assert no row in the output raises `ZeroDivisionError` for any numeric input
- Given a high GOR resulting from near-zero oil production (physically valid pressure
  depletion scenario), assert the value is preserved without being flagged as invalid
  (TR-06 last paragraph)

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 03: Implement decline rate feature

**Module:** `kgs_pipeline/features.py`
**Function:** `add_decline_rate(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute period-over-period production decline rate per (LEASE_KID, PRODUCT). Operates
on a pandas partition sorted by production_date.

**Decline rate definition:**
`decline_rate = (prod_t - prod_{t-1}) / prod_{t-1}`

**Steps:**
- Compute `prod_{t-1}` using `groupby(["LEASE_KID", "PRODUCT"])["PRODUCTION"].shift(1)`
- Compute raw decline rate as `(PRODUCTION - lag_production) / lag_production`
- Handle zero denominator (shut-in month followed by production, or two consecutive
  shut-in months): when `lag_production == 0`, set the raw rate to 0.0 before clipping
  to avoid unclipped extreme values (TR-07d)
- Clip the result to the range [-1.0, 10.0] (TR-07)
- Set `decline_rate` to NaN for the first record of each (LEASE_KID, PRODUCT) group
  (no prior period available)
- Return the DataFrame with `decline_rate` column appended

**Test cases (TR-07a, TR-07b, TR-07c, TR-07d):**
- Given a decline rate computed as -2.0, assert the output is -1.0 (clipped lower bound) (TR-07a)
- Given a computed decline rate of 15.0, assert the output is 10.0 (clipped upper bound) (TR-07b)
- Given a computed decline rate of 0.5, assert the output is 0.5 (within bounds, unchanged) (TR-07c)
- Given consecutive zero-production months (PRODUCTION=0 both months), assert the raw
  computation produces 0.0 before clipping, and the clipped output is 0.0 (TR-07d)
- Given the first record of a lease (no prior period), assert `decline_rate` is NaN
- Use synthetic well sequences with known month-over-month changes to make assertions exact

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 04: Implement rolling average features

**Module:** `kgs_pipeline/features.py`
**Function:** `add_rolling_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute rolling 3-month and 6-month production averages per (LEASE_KID, PRODUCT).
Operates on a pandas partition sorted by production_date.

**Steps:**
- Compute 3-month rolling mean of PRODUCTION grouped by (LEASE_KID, PRODUCT),
  using `min_periods=1`, stored as `rolling_3m_production`
- Compute 6-month rolling mean of PRODUCTION grouped by (LEASE_KID, PRODUCT),
  using `min_periods=1`, stored as `rolling_6m_production`
- Return the DataFrame with both columns appended

**Rolling window behavior (TR-09a, TR-09b):**
- When window > available history, use `min_periods=1` so partial windows return
  the mean of available months — not NaN, not zero
- The first month's rolling_3m and rolling_6m both equal that month's production value

**Test cases (TR-09a, TR-09b):**
- Given an oil production sequence `[100, 200, 300, 400]` for one lease, assert:
  - `rolling_3m_production` = `[100, 150, 200, 300]` (hand-computed partial and full windows)
  - `rolling_6m_production` = `[100, 150, 200, 250]` (hand-computed, all partial windows)
- When rolling window is larger than available history (first 2 months of a well):
  assert result is the partial window mean, not NaN and not 0 (TR-09b)
- Assert the function is applied independently per (LEASE_KID, PRODUCT) — one lease's
  rolling values do not bleed into another lease's values

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 05: Implement lag feature

**Module:** `kgs_pipeline/features.py`
**Function:** `add_lag_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Compute lag-1 production feature per (LEASE_KID, PRODUCT). Operates on a pandas
partition sorted by production_date.

**Steps:**
- Compute lag-1 of PRODUCTION using `groupby(["LEASE_KID","PRODUCT"])["PRODUCTION"].shift(1)`
- Store as `lag_1_production`; NaN for first record of each (LEASE_KID, PRODUCT) group
- Return the DataFrame with `lag_1_production` column appended

**Test cases (TR-09c):**
- Given a production sequence `[100, 200, 300]` for one lease-product, assert
  `lag_1_production = [NaN, 100, 200]` (TR-09c)
- Verify lag-1 for month N equals the raw PRODUCTION value for month N-1 for at least
  3 consecutive months
- Assert `lag_1_production` is NaN for the first record of every (LEASE_KID, PRODUCT) group

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 06: Implement temporal features

**Module:** `kgs_pipeline/features.py`
**Function:** `add_temporal_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**
Derive calendar-based features from `production_date`. Operates on a pandas partition.

**Steps:**
- Extract `year` from `production_date.dt.year` (Int64)
- Extract `month` from `production_date.dt.month` (Int64)
- Extract `quarter` from `production_date.dt.quarter` (Int64)
- Compute `days_in_month` from `production_date.dt.days_in_month` (Int64)
- Return the DataFrame with all four columns appended

**Test cases:**
- Given `production_date = 2024-03-01`, assert `year=2024`, `month=3`, `quarter=1`,
  `days_in_month=31`
- Given `production_date = 2024-06-01`, assert `quarter=2`, `days_in_month=30`
- Given `production_date = 2024-02-01`, assert `days_in_month=29` (2024 is a leap year)

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 07: Implement categorical label encoding

**Module:** `kgs_pipeline/features.py`
**Function:** `add_label_encodings(df: pd.DataFrame, encoding_maps: dict[str, dict[str, int]]) -> pd.DataFrame`

**Description:**
Apply integer label encoding to categorical string columns. Operates on a pandas
partition. The encoding maps are computed globally before `map_partitions` is called
and passed as a parameter so all partitions use the same consistent mapping.

**Columns to encode:** COUNTY → `county_code`, FIELD → `field_code`,
PRODUCING_ZONE → `producing_zone_code`, OPERATOR → `operator_code`,
PRODUCT → `product_code`

**PRODUCT encoding:** fixed mapping `{"O": 0, "G": 1}` (not data-driven)

**Steps:**
- For each column/encoding pair, map string values to integer codes using the provided
  `encoding_maps` dict
- Values absent from the encoding map (unseen categories) map to -1 as a sentinel
- Cast each resulting column to `pd.Int64Dtype()`
- Return the DataFrame with all `*_code` columns appended

**Encoding map construction (separate function):**

**Function:** `build_encoding_maps(ddf: dd.DataFrame) -> dict[str, dict[str, int]]`
- Compute unique values for COUNTY, FIELD, PRODUCING_ZONE, OPERATOR across all partitions
- Sort each unique value list for deterministic ordering
- Assign integer codes 0, 1, 2, ... in sorted order
- Return a dict of dicts: `{"COUNTY": {"Nemaha": 0, "Barton": 1, ...}, ...}`
- This function calls `.compute()` to materialize unique values — it is the only
  permitted `.compute()` call before the final write in this stage

**Test cases:**
- Given `encoding_maps={"COUNTY": {"Nemaha": 0, "Barton": 1}}` and a row with
  `COUNTY="Nemaha"`, assert `county_code=0`
- Given a row with `COUNTY="Unknown"` (not in map), assert `county_code=-1`
- Assert `product_code=0` for PRODUCT=="O" and `product_code=1` for PRODUCT=="G"
- Assert all `*_code` columns have dtype `Int64`
- Given two partitions using the same `encoding_maps`, assert the same county name
  maps to the same integer in both partitions (consistency check)

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 08: Implement feature manifest writer

**Module:** `kgs_pipeline/features.py`
**Function:** `write_feature_manifest(feature_columns: list[str], encoding_maps: dict[str, dict[str, int]], output_path: Path) -> None`

**Description:**
Write a JSON manifest describing the feature columns and encoding maps to
`data/processed/feature_manifest.json`.

**Manifest structure:**
```json
{
  "feature_columns": [...list of all output column names...],
  "encoding_maps": {
    "COUNTY": {"Nemaha": 0, ...},
    "FIELD": {...},
    "PRODUCING_ZONE": {...},
    "OPERATOR": {...},
    "PRODUCT": {"O": 0, "G": 1}
  },
  "generated_at": "<ISO 8601 UTC timestamp>",
  "description": "Feature manifest for KGS oil and gas production ML features"
}
```

**Steps:**
- Construct the manifest dict from `feature_columns`, `encoding_maps`, and current UTC time
- Write to `output_path` using `json.dumps` with `indent=2`
- Create parent directories if they do not exist

**Test cases:**
- Given a list of feature columns and an encoding map, assert the written JSON file
  is readable and contains all expected keys
- Assert `feature_columns` in the manifest matches the input list exactly
- Assert `encoding_maps` contains entries for COUNTY, FIELD, PRODUCING_ZONE, OPERATOR, PRODUCT
- Assert `generated_at` is a valid ISO 8601 timestamp string

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 09: Implement features Parquet writer

**Module:** `kgs_pipeline/features.py`
**Function:** `write_features_parquet(ddf: dd.DataFrame, output_dir: Path, n_partitions: int) -> None`

**Description:**
Repartition and write the features Dask DataFrame as partitioned Parquet to `output_dir`.
The repartition must be the last structural operation before writing (ADR-004).

**Steps:**
- Repartition to `n_partitions` using `ddf.repartition(npartitions=n_partitions)`
- Write to `output_dir` using `ddf.to_parquet(output_dir, write_index=True, overwrite=True)`
- Log the output path and partition count

**Test cases (TR-18, TR-19):**
- Assert at least one `.parquet` file is written to `output_dir`
- Assert each written Parquet file is readable by `pandas.read_parquet` without error (TR-18)
- Assert the combined output DataFrame contains all required columns from the TR-19
  required column list (TR-19)
- Read two partition files and assert identical column names and dtypes (TR-14)

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 10: Implement features stage entry point

**Module:** `kgs_pipeline/features.py`
**Function:** `run_features(config: FeaturesConfig, client: distributed.Client) -> FeaturesSummary`

**Description:**
Orchestrate the full features stage: read transformed Parquet, build encoding maps,
apply all feature functions as a lazy Dask pipeline, write output Parquet and manifest.
All feature `map_partitions` calls are composed lazily and executed in a single write
(ADR-005, TR-17). The only permitted pre-write `.compute()` call is `build_encoding_maps`.

**Transformation sequence:**
1. Read transformed Parquet from `config.input_dir` as Dask DataFrame
2. Build encoding maps via `build_encoding_maps` (one `.compute()` call)
3. Apply `add_temporal_features` via `map_partitions`
4. Apply `add_cumulative_production` via `map_partitions`
5. Apply `add_decline_rate` via `map_partitions`
6. Apply `add_rolling_features` via `map_partitions`
7. Apply `add_lag_features` via `map_partitions`
8. Apply `add_gor` via `map_partitions`
9. Apply `add_label_encodings` (with encoding_maps) via `map_partitions`
10. Compute `n_partitions = max(10, min(ddf.npartitions, 50))`
11. Write output via `write_features_parquet`
12. Write feature manifest via `write_feature_manifest`

**No intermediate `.compute()` calls are permitted between steps 3–11** (ADR-005, TR-17).

**`FeaturesConfig` fields (dataclass):**
- `input_dir: Path` — transformed Parquet directory (output of transform)
- `output_dir: Path` — output directory for features Parquet
- `manifest_path: Path` — path for feature manifest JSON

**`FeaturesSummary` fields (dataclass):**
- `output_dir: Path`
- `manifest_path: Path`
- `partition_count: int`
- `feature_columns: list[str]`

**Test cases (TR-17, TR-19):**
- Assert the Dask DataFrame returned by each `map_partitions` call is of type
  `dask.dataframe.DataFrame` (not `pd.DataFrame`) — no intermediate `.compute()` (TR-17)
- Given a small synthetic transformed Parquet dataset, assert `run_features` completes
  without error and produces output Parquet files in `config.output_dir`
- Assert the combined output Parquet contains all required columns per TR-19
- Assert `FeaturesSummary.feature_columns` contains all derived feature column names

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 11: Implement map_partitions meta schema consistency tests for features

**Module:** `tests/test_features.py`

**Description:**
Write tests that verify the `meta=` argument for every `map_partitions` call in the
features stage matches the actual function output schema. TR-23.

**Test cases (TR-23):**
For each of the following features functions: `add_temporal_features`,
`add_cumulative_production`, `add_decline_rate`, `add_rolling_features`,
`add_lag_features`, `add_gor`, `add_label_encodings`:

- Construct a minimal synthetic input DataFrame matching the expected input schema
  (one or two rows with valid values, including `production_date` as `datetime64[ns]`)
- Call the function on that input and capture the output column list and dtypes
- Construct a zero-row meta DataFrame using `ddf._meta.copy()` and call the same
  function on it
- Assert the output column list from the actual call matches the column list from the
  meta call exactly (including order)
- Assert dtypes match for every column between actual and meta calls

**Definition of done:** Test cases implemented and passing, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 12: Implement physical bounds and domain correctness tests

**Module:** `tests/test_features.py`

**Description:**
Write domain-correctness tests for feature calculations against known synthetic inputs.
Covers TR-01, TR-06, TR-07, TR-08, TR-09, TR-10.

**Test cases:**

**TR-01 — Physical bounds validation:**
- Assert all PRODUCTION values in features output are >= 0 (no negative production)
- Assert all `gor` values in features output are either NaN or >= 0

**TR-06 — GOR zero-denominator handling:**
- oil_bbl=0, gas_mcf > 0 → `gor` is NaN (not exception) (TR-06a)
- oil_bbl=0, gas_mcf=0 → `gor` is 0.0 or NaN (not exception) (TR-06b)
- oil_bbl > 0, gas_mcf=0 → `gor` is 0.0 (TR-06c)

**TR-07 — Decline rate clip bounds:**
- Computed decline rate < -1.0 → clipped to exactly -1.0 (TR-07a)
- Computed decline rate > 10.0 → clipped to exactly 10.0 (TR-07b)
- Value within bounds → passes through unchanged (TR-07c)
- Both consecutive production values = 0 → `decline_rate` is 0.0 before clipping (TR-07d)

**TR-08 — Cumulative production flat periods:**
- Sequence with zero-production mid-sequence: cumulative stays flat (TR-08a)
- Sequence starting with zeros: cumulative starts at 0 and stays 0 (TR-08b)
- Resumption after shut-in: cumulative resumes from the flat value (TR-08c)

**TR-09 — Feature calculation correctness:**
- Rolling 3m and 6m averages match hand-computed values for a known sequence (TR-09a)
- Partial window (first 2 months) returns partial mean per `min_periods=1`, not NaN (TR-09b)
- Lag-1 for month N equals raw production for month N-1 for at least 3 months (TR-09c)
- GOR formula uses gas_mcf / oil_bbl — not swapped (TR-09d)

**TR-10 — Water cut boundary values:**
- `water_cut` column is `pd.NA` for all rows (no water production in KGS dataset);
  assert no rows have `water_cut` outside [0,1] or raise an error (TR-10)
  Note: since KGS data has no water production column, water_cut is set to NA for all
  rows; the test asserts the column is present and contains only NA or values in [0,1]

**Definition of done:** Test cases implemented and passing, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 13: Implement decline curve monotonicity and feature column presence tests

**Module:** `tests/test_features.py`

**Description:**
Verify cumulative production monotonicity (TR-03) and output column completeness (TR-19).

**Test cases:**

**TR-03 — Decline curve monotonicity:**
- Given a synthetic single-lease oil production sequence, assert `cum_oil` values are
  monotonically non-decreasing over time (each value >= the previous value)
- Assert no value in `cum_oil` or `cum_gas` is less than the immediately preceding
  value for the same (LEASE_KID, PRODUCT) when rows are ordered by `production_date`

**TR-19 — Feature column presence in output schema:**
- Run `run_features` on a synthetic single-well input with known values
- Assert the output DataFrame contains every column from the TR-19 required column list:
  `LEASE_KID`, `LEASE`, `OPERATOR`, `COUNTY`, `FIELD`, `PRODUCING_ZONE`, `PRODUCT`,
  `PRODUCTION`, `WELLS`, `MONTH-YEAR`, `production_date`, `cum_oil`, `cum_gas`,
  `gor`, `decline_rate`, `rolling_3m_production`, `rolling_6m_production`,
  `lag_1_production`, `county_code`, `field_code`, `producing_zone_code`,
  `operator_code`, `product_code`, `year`, `month`, `quarter`, `days_in_month`,
  `has_date_gap`, `production_unit_flag`
- Assert no column from this list is missing in the output (use an exhaustive
  set comparison, not a subset check)

**Definition of done:** Test cases implemented and passing, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 14: Implement pipeline entry point and orchestrator

**Module:** `kgs_pipeline/pipeline.py`
**Function:** `main(stages: list[str] | None = None) -> None`

**Description:**
Top-level pipeline entry point. Reads `config.yaml`, initializes logging, initializes
the Dask distributed scheduler, runs each requested stage in order, logs per-stage
timing, and stops execution on stage failure (build-env-manifest requirement).

**Steps:**
- Parse CLI arguments (optional list of stage names; default to all four:
  `["acquire", "ingest", "transform", "features"]`)
- Read `config.yaml` and construct stage-specific config dataclasses
- Set up dual-channel logging (console + file) using log config from `config.yaml`
- Create the `logs/` directory if it does not exist
- Run the `acquire` stage using Dask threaded scheduler (I/O-bound — ADR-001)
  - After acquire completes, initialize the Dask distributed scheduler from `config.yaml`:
    - If `dask.scheduler == "local"`: create a `distributed.LocalCluster` with
      `n_workers`, `threads_per_worker`, `memory_limit` from config
    - If `dask.scheduler` is a URL: connect to the remote scheduler
  - Log the Dask dashboard URL
- Run `ingest`, `transform`, `features` stages in order using the distributed client
- For each stage: log start time, call the stage entry point, log end time and duration
- On any stage failure: log the error at ERROR level and stop — do not run downstream stages
- Write a pipeline run summary JSON to `data/processed/pipeline_report.json` with:
  - `stages_run`, `stages_succeeded`, `stages_failed`, `total_duration_seconds`,
    per-stage timing, `run_timestamp`

**CLI arguments:**
- `--stages` (optional, nargs="+"): list of stage names to run; defaults to all four
- Stage names: `"acquire"`, `"ingest"`, `"transform"`, `"features"`

**Test cases:**
- Assert CLI argument parsing with `--stages acquire ingest` produces a stages list
  of `["acquire", "ingest"]`
- Assert CLI with no `--stages` argument defaults to all four stages
- Given mocked stage entry points that succeed, assert all four stages are called
  in the correct order
- Given a mocked `ingest` stage that raises an exception, assert `transform` and
  `features` are not called and the pipeline report reflects the failure
- Assert the pipeline report JSON is written to `data/processed/pipeline_report.json`
  after a run
- Assert the distributed scheduler is initialized after `acquire` and before `ingest`

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
