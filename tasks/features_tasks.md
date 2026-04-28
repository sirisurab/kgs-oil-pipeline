# Features Stage Tasks

**Module:** `kgs_pipeline/features.py`  
**Test file:** `tests/test_features.py`  
**Governing docs:** stage-manifest-features.md, boundary-transform-features.md,
ADR-001, ADR-002, ADR-003, ADR-004, ADR-005, ADR-006, ADR-007, build-env-manifest.md

---

## Task F-01: Compute cumulative production per entity

**Module:** `kgs_pipeline/features.py`  
**Function:** `add_cumulative_production(df: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition (used via `map_partitions`). The input is
entity-indexed on `LEASE_KID` and sorted by `production_date` within each entity group,
as guaranteed by boundary-transform-features.md. For each entity, compute cumulative
oil production (`cum_oil`), cumulative gas production (`cum_gas`), and cumulative water
production (`cum_water`) as running sums of the `PRODUCTION` column, grouped by the
entity index. Because `PRODUCTION` contains a mix of oil and gas in the same column
differentiated by the `PRODUCT` column, the function must separate oil and gas rows
before summing. Water production tracking is dependent on the columns present per the
data dictionary — if no water column is present in the input schema, `cum_water` must
be added as an all-`np.nan` column of dtype `float64`.

Cumulative values must be monotonically non-decreasing over time per entity — zero
production months must keep the cumulative flat, not decrease it (see TR-03, TR-08).

**Input:** `pd.DataFrame` — one partition from the processed Parquet dataset (entity-
indexed on `LEASE_KID`, sorted by `production_date`, schema as per
boundary-transform-features.md).  
**Output:** `pd.DataFrame` — same columns plus `cum_oil` (`float64`), `cum_gas`
(`float64`), `cum_water` (`float64`).

**Constraints:**
- Per-entity aggregation must use vectorized grouped transformations — see ADR-002.
- Temporal ordering within each entity group is guaranteed by boundary-transform-features.md
  — no re-sorting is needed.
- Float null sentinel is `np.nan` — see ADR-003.
- Cumulative sums must be computed as a `groupby(...).cumsum()` — no per-row iteration
  — see ADR-002.
- No retry logic, no caching.

**Test cases (TR-03, TR-08):**
- Given a single entity with production values [10, 20, 0, 5], assert `cum_oil` is
  [10, 30, 30, 35] (flat at zero-production month — TR-08).
- Assert `cum_oil` values are monotonically non-decreasing for every entity (TR-03).
- Given a sequence with zero-production months at the start, assert `cum_oil` starts at
  0.0 for those months and resumes correctly when production begins (TR-08).
- Given a sequence with zero-production months mid-sequence, assert `cum_oil` remains
  flat and then resumes correctly when production resumes (TR-08).
- Assert the return type is `pd.DataFrame` with `cum_oil`, `cum_gas`, `cum_water` columns
  of dtype `float64`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-02: Compute GOR and water cut

**Module:** `kgs_pipeline/features.py`  
**Function:** `add_ratio_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition. Compute Gas-Oil Ratio (`gor`) and water cut
(`water_cut`) per row using vectorized operations. Definitions:

- `gor = gas_mcf / oil_bbl` — where `gas_mcf` is the PRODUCTION value for rows where
  `PRODUCT == "G"` and `oil_bbl` is the PRODUCTION value for rows where `PRODUCT == "O"`
  for the same entity and month. When `oil_bbl == 0` and `gas_mcf > 0`, return `np.nan`
  (undefined — not a pipeline error, not an exception). When both are zero, return `np.nan`
  or `0.0` as defined by TR-06. When `oil_bbl > 0` and `gas_mcf == 0`, return `0.0`.
  Do not treat a legitimately high GOR from pressure depletion as a data error (TR-06).
- `water_cut = water_bbl / (oil_bbl + water_bbl)` — denominator is total liquid (not oil
  alone). When `water_bbl == 0` and `oil_bbl > 0`, return `0.0`. When `oil_bbl == 0` and
  `water_bbl > 0`, return `1.0`. Both boundary values are physically valid and must not
  be flagged or nulled (TR-10). Values outside [0, 1] are invalid and must be replaced
  with `np.nan`.

**Input:** `pd.DataFrame` — one partition; entity-indexed on `LEASE_KID`.  
**Output:** `pd.DataFrame` — same columns plus `gor` (`float64`) and `water_cut`
(`float64`).

**Constraints:**
- GOR zero-denominator handling: see TR-06. No `ZeroDivisionError` may be raised; no
  silent `NaN` swallowed without explicit handling.
- Water cut boundary values: see TR-10. Values of exactly 0.0 and 1.0 are valid.
- Float null sentinel is `np.nan` — see ADR-003.
- All operations must be vectorized — see ADR-002.
- No per-row iteration — see ADR-002.
- No retry logic, no caching.

**Test cases (TR-06, TR-09, TR-10):**
- Given `oil_bbl=0`, `gas_mcf=5`, assert `gor` is `np.nan` and no exception is raised
  (TR-06).
- Given `oil_bbl=0`, `gas_mcf=0`, assert `gor` is `np.nan` or `0.0` and no exception is
  raised (TR-06).
- Given `oil_bbl=10`, `gas_mcf=0`, assert `gor` is `0.0` (TR-06).
- Given `water_bbl=0`, `oil_bbl=100`, assert `water_cut` is `0.0` and the row is
  retained (TR-10).
- Given `oil_bbl=0`, `water_bbl=50`, assert `water_cut` is `1.0` and the row is retained
  (TR-10).
- Assert values outside [0, 1] for `water_cut` are replaced with `np.nan` (TR-01, TR-10).
- Assert `gor` formula is `gas / oil` — not unit-swapped (TR-09).
- Assert `water_cut` denominator is `oil + water` — not oil alone (TR-09).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-03: Compute decline rate

**Module:** `kgs_pipeline/features.py`  
**Function:** `add_decline_rate(df: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition. Compute the period-over-period production decline
rate (`decline_rate`) per entity as a derived column. Decline rate is defined as
`(production_t - production_{t-1}) / production_{t-1}` where production is the PRODUCTION
value for each monthly row. Handle the zero-denominator case (prior month is zero) before
clipping — do not produce an unclipped extreme value that is then clipped (see TR-07).
Clip the result to the range `[-1.0, 10.0]` — these are engineering ML feature stability
bounds, not physical constants (TR-07).

**Input:** `pd.DataFrame` — one partition; entity-indexed on `LEASE_KID`, sorted by
`production_date`.  
**Output:** `pd.DataFrame` — same columns plus `decline_rate` (`float64`).

**Constraints:**
- Per-entity lag computation must use vectorized grouped `shift` — see ADR-002.
- Zero-denominator handling must occur before clipping — see TR-07.
- Clip bounds: `[-1.0, 10.0]` — see TR-07.
- Float null sentinel is `np.nan` — see ADR-003.
- No per-row iteration — see ADR-002.
- No retry logic, no caching.

**Test cases (TR-07):**
- Given a well sequence where month N-1 PRODUCTION = 100 and month N PRODUCTION = 80,
  assert `decline_rate` for month N is `(80 - 100) / 100 = -0.20` (within bounds,
  unchanged).
- Given a computed decline rate below -1.0, assert it is clipped to exactly -1.0 (TR-07).
- Given a computed decline rate above 10.0, assert it is clipped to exactly 10.0 (TR-07).
- Given a value within `[-1.0, 10.0]`, assert it passes through unchanged (TR-07).
- Given a well with two consecutive zero-production months, assert the zero-denominator
  case is handled before clipping and does not produce an unclipped extreme value (TR-07).
- Assert the dtype of `decline_rate` is `float64`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-04: Compute rolling averages

**Module:** `kgs_pipeline/features.py`  
**Function:** `add_rolling_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition. Compute 3-month and 6-month rolling averages of
PRODUCTION per entity group, separately for oil and gas rows. Output columns:
`rolling_3m_oil` (`float64`), `rolling_6m_oil` (`float64`), `rolling_3m_gas` (`float64`),
`rolling_6m_gas` (`float64`). When the rolling window is larger than the available
history (e.g., the first 2 months of a well's life), the result must be `np.nan` or a
partial window value — not silently zero or a wrong value (TR-09).

**Input:** `pd.DataFrame` — one partition; entity-indexed on `LEASE_KID`, sorted by
`production_date`.  
**Output:** `pd.DataFrame` — same columns plus four rolling average columns
(`float64` each).

**Constraints:**
- Per-entity rolling must use vectorized grouped `rolling` — see ADR-002.
- Temporal ordering is guaranteed by boundary-transform-features.md — no re-sorting needed.
- Float null sentinel is `np.nan` — see ADR-003.
- No per-row iteration — see ADR-002.
- No retry logic, no caching.

**Test cases (TR-09):**
- Given a single entity with oil production [100, 200, 300, 400], assert `rolling_3m_oil`
  for month 3 (index 2, 0-based) is `(100+200+300)/3 = 200.0` — hand-computed (TR-09).
- Assert `rolling_3m_oil` for month 1 (insufficient history) is `np.nan` (not zero —
  TR-09).
- Given a 6-month rolling window with fewer than 6 months of history, assert the result
  for those early months is `np.nan` (TR-09).
- Assert rolling averages for a known sequence match hand-computed values exactly (TR-09).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-05: Compute lag features

**Module:** `kgs_pipeline/features.py`  
**Function:** `add_lag_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:**  
Operate on a single pandas partition. Compute lag-1 features for PRODUCTION per entity
group — producing `lag1_oil` and `lag1_gas` columns (both `float64`). Lag-1 for month N
equals the PRODUCTION value for month N-1 of the same entity. For the first month of
each entity's record, the lag value is `np.nan`.

**Input:** `pd.DataFrame` — one partition; entity-indexed on `LEASE_KID`, sorted by
`production_date`.  
**Output:** `pd.DataFrame` — same columns plus `lag1_oil` (`float64`) and `lag1_gas`
(`float64`).

**Constraints:**
- Per-entity lag must use vectorized grouped `shift` — see ADR-002.
- Temporal ordering is guaranteed by boundary-transform-features.md — no re-sorting needed.
- Float null sentinel is `np.nan` — see ADR-003.
- No per-row iteration — see ADR-002.
- No retry logic, no caching.

**Test cases (TR-09):**
- Given a single entity with oil production values [10, 20, 30] across 3 consecutive
  months, assert `lag1_oil` is [NaN, 10, 20] — verified for all 3 months (TR-09).
- Assert the lag-1 feature for month N equals the raw production value for month N-1
  for at least 3 consecutive months (TR-09).
- Assert `lag1_oil` dtype is `float64`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-06: Apply all feature functions via map_partitions

**Module:** `kgs_pipeline/features.py`  
**Function:** `apply_features(ddf: dd.DataFrame) -> dd.DataFrame`

**Description:**  
Apply `add_cumulative_production`, `add_ratio_features`, `add_decline_rate`,
`add_rolling_features`, and `add_lag_features` to every partition of the input Dask
DataFrame using `map_partitions`. Each `map_partitions` call must supply a `meta=`
argument derived by calling the actual function on a minimal real input sliced to zero
rows — no helper functions, no empty-frame builders (see ADR-003, TR-23). The Dask
DataFrame must remain lazy throughout; do not call `.compute()` (see ADR-005, TR-17).

**Input:** `ddf: dd.DataFrame` — processed Parquet dataset as delivered by
boundary-transform-features.md (entity-indexed on `LEASE_KID`, sorted by
`production_date`, categoricals cast, invalid values replaced, max(10,min(n,50))
partitions).  
**Output:** `dd.DataFrame` — lazy Dask DataFrame with all derived feature columns added.

**Constraints:**
- Meta derivation: call each actual function on a minimal real input sliced to zero rows
  — see ADR-003. No separate meta helpers.
- Temporal ordering within each entity group is guaranteed by boundary-transform-features.md
  — no re-sorting needed (stage-manifest-features.md H1).
- No `.compute()` calls inside this function — see ADR-005, TR-17.
- No retry logic, no caching.

**Test cases (TR-17, TR-23):**
- Assert the return type is `dd.DataFrame` (not `pd.DataFrame`) — TR-17.
- For each `map_partitions` call, verify the `meta=` argument matches the actual function
  output in column names, column order, and dtypes — TR-23.
- Assert `.compute()` is not called inside this function.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-07: Orchestrate features stage and write ML-ready Parquet

**Module:** `kgs_pipeline/features.py`  
**Function:** `features(config: dict) -> None`

**Description:**  
Orchestrate the full features stage. Read processed Parquet from
`config["features"]["processed_dir"]` using `dd.read_parquet`. Call `apply_features` to
build the full lazy feature computation graph. Repartition to `max(10, min(n, 50))` where
`n` is the partition count of the input (see ADR-004); repartition must be the last
operation before writing (see ADR-004). Write the result to
`config["features"]["output_dir"]` using `dask.dataframe.to_parquet`. The task graph must
remain lazy until the write operation (see ADR-005). Use the distributed scheduler per
ADR-001.

**Input:** `config: dict` — full config loaded from `config.yaml`.  
**Output:** None. Side effect: ML-ready Parquet files written to output directory.

**Constraints:**
- Scheduler: distributed — see ADR-001.
- Repartition is the last operation before write — see ADR-004.
- Task graph stays lazy until write — see ADR-005.
- Output state: all input columns plus all derived feature columns — see
  stage-manifest-features.md.
- Logging: see ADR-006.
- No retry logic, no caching.

**Test cases (TR-01, TR-14, TR-17, TR-18, TR-19, TR-26, TR-27):**
- Assert Parquet files are written and readable by `dd.read_parquet` without error (TR-18).
- Assert the output DataFrame contains all expected columns: `LEASE_KID`,
  `production_date`, `PRODUCTION`, `cum_oil`, `cum_gas`, `cum_water`, `gor`, `water_cut`,
  `decline_rate`, `rolling_3m_oil`, `rolling_6m_oil`, `rolling_3m_gas`, `rolling_6m_gas`,
  `lag1_oil`, `lag1_gas` (TR-19).
- Assert schema is identical across sampled partitions (TR-14).
- Assert production volumes are non-negative in the output (TR-01).
- Run `features()` on the transform Parquet output of TR-25 using `tmp_path` for all
  output paths; assert all derived feature columns are present (TR-26).
- Run the full pipeline (ingest → transform → features) on 2–3 raw files using `tmp_path`;
  assert no unhandled exceptions and all stage boundary contracts are satisfied (TR-27).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no
errors, requirements.txt updated with all third-party packages imported in this task.
