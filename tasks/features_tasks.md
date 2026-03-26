# Features Component — Task Specifications

## Overview

The features component is responsible for:
1. Loading the cleaned, processed Parquet data from `data/processed/` via Dask.
2. Engineering per-well ML-ready features: cumulative production (Np, Gp, Wp), month-over-month decline rate, rolling statistics, time-based features, Gas-Oil Ratio (GOR), water cut, Water-Oil Ratio (WOR), production lag features, and geographic/operator aggregate features.
3. Encoding categorical columns with consistent integer codes across all Dask partitions.
4. Writing the feature Parquet dataset to `data/processed/features/`, partitioned by `well_id`, with a fixed `pyarrow.schema()`.
5. Exporting a feature matrix CSV to `data/processed/features/feature_matrix.csv` for quick inspection.
6. Implementing the pipeline orchestrator (`kgs_pipeline/pipeline.py`) as a CLI entry point that runs all four pipeline stages in sequence, with structured logging, run metadata tracking, and a JSON summary output.

**Source modules:** `kgs_pipeline/features.py`, `kgs_pipeline/pipeline.py`
**Test files:** `tests/test_features.py`, `tests/test_pipeline.py`

---

## Design Decisions & Constraints

- Python 3.11+. All functions must carry full type hints. Both modules must have module-level docstrings.
- Use `dask.dataframe` throughout in `features.py`. Functions must return `dask.dataframe.DataFrame` unless otherwise stated. Never call `.compute()` in helper functions — only in `write_features_parquet()`.
- All per-well operations (cumulative sums, rolling windows, lag features) require that each well's records are sorted by `production_date` ascending before the calculation is applied. After sorting by `well_id` and `production_date` in the transform stage, the features stage must re-sort within each partition using `map_partitions` before applying window operations.
- Cumulative production is computed separately for each product type (`O` and `G`) per well. Oil cumulative = Np; gas cumulative = Gp.
- GOR = `gas_mcf / oil_bbl` per month per well. Since the processed data stores oil and gas in separate rows (one row per product per month per well), a pivot is required to compute GOR.
- Water production (`water_bbl`) is not present in the KGS dataset. The `water_cut` and `WOR` columns must be written as `NaN` (placeholder for future data enrichment) with a note in the module docstring.
- Rolling windows: 3-month and 6-month. For months where fewer than `window` observations are available (early in a well's life), use `min_periods=1` for rolling mean and `min_periods=2` for rolling standard deviation so partial windows return a value rather than `NaN`, unless otherwise required by test specifications.
- Decline rate is computed as `(production[t] - production[t-1]) / production[t-1]`. When `production[t-1] == 0` (shut-in month), the raw calculation is undefined; assign `NaN` before applying the clip. Then clip to `[CONFIG.decline_rate_clip_min, CONFIG.decline_rate_clip_max]` = `[-1.0, 10.0]`.
- Use `CONFIG` from `kgs_pipeline/config.py` and `setup_logging("features", CONFIG.logs_dir)` from `kgs_pipeline/utils.py`.
- Enforce a fixed Parquet output schema using `pyarrow.schema()` at write time.
- The global categorical encoding map must be computed **once** from the full dataset (by calling `.compute()` once on the unique values of each categorical column) before being broadcast to all Dask partitions via `map_partitions`. This guarantees consistent integer codes across all partitions for ML reproducibility.

### Expected features Parquet schema

| Column | dtype | Description |
|--------|-------|-------------|
| `well_id` | `string` | API number or `UNKNOWN_*` |
| `production_date` | `datetime64[ns]` | First day of production month |
| `product` | `string` | `O` or `G` |
| `production` | `float64` | Raw monthly production volume |
| `unit` | `string` | `BBL` or `MCF` |
| `lease_kid` | `int64` | KGS lease unique ID |
| `operator` | `string` | Operator name |
| `county` | `string` | County name |
| `field` | `string` | Field name |
| `producing_zone` | `string` | Producing formation |
| `latitude` | `float64` | NAD 1927 latitude |
| `longitude` | `float64` | NAD 1927 longitude |
| `cum_production` | `float64` | Cumulative production per well per product (Np for oil, Gp for gas) |
| `decline_rate` | `float64` | Month-over-month decline rate, clipped to [-1.0, 10.0] |
| `rolling_mean_3m` | `float64` | 3-month rolling average of production |
| `rolling_mean_6m` | `float64` | 6-month rolling average of production |
| `rolling_mean_12m` | `float64` | 12-month rolling average of production |
| `rolling_std_3m` | `float64` | 3-month rolling standard deviation of production |
| `rolling_std_6m` | `float64` | 6-month rolling standard deviation of production |
| `lag_1m` | `float64` | Production 1 month prior (lag-1 feature) |
| `lag_3m` | `float64` | Production 3 months prior (lag-3 feature) |
| `months_since_first_prod` | `int64` | Months elapsed since the well's first production date |
| `production_year` | `int32` | Calendar year of the production month |
| `production_month` | `int32` | Calendar month (1–12) |
| `production_quarter` | `int32` | Calendar quarter (1–4) |
| `gor` | `float64` | Gas-Oil Ratio (gas MCF / oil BBL); NaN if oil=0 or no oil row for that month |
| `water_cut` | `float64` | Water cut placeholder — `NaN` (no water data in KGS dataset) |
| `wor` | `float64` | Water-Oil Ratio placeholder — `NaN` (no water data in KGS dataset) |
| `county_encoded` | `int32` | Integer-encoded county (consistent global map) |
| `operator_encoded` | `int32` | Integer-encoded operator (consistent global map) |
| `producing_zone_encoded` | `int32` | Integer-encoded producing zone (consistent global map) |
| `field_encoded` | `int32` | Integer-encoded field name (consistent global map) |
| `product_encoded` | `int32` | Integer-encoded product type (`O`=0, `G`=1) |
| `outlier_flag` | `bool` | Inherited from transform stage |

---

## Task 22: Implement processed Parquet reader for features stage

**Module:** `kgs_pipeline/features.py`
**Function:** `load_processed_parquet(processed_dir: Path | None = None) -> dask.dataframe.DataFrame`

**Description:**
Load all Parquet files from `processed_dir` (default: `CONFIG.processed_dir`) using `dask.dataframe.read_parquet()`. Use the `pyarrow` engine. Return the lazy Dask DataFrame without calling `.compute()`. Log the partition count at INFO level.

**Error handling:**
- Raise `FileNotFoundError` if `processed_dir` does not exist.
- Raise `RuntimeError("No Parquet files found in processed_dir; run transform pipeline first.")` if no Parquet files are found.

**Dependencies:** dask.dataframe, pathlib, logging, kgs_pipeline.config

**Test cases (`tests/test_features.py`):**
- `@pytest.mark.unit` — Write a small fixture Parquet file to `tmp_path`; call `load_processed_parquet(tmp_path)` and assert return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Call with a non-existent directory; assert `FileNotFoundError`.
- `@pytest.mark.unit` — Call with empty directory; assert `RuntimeError`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (not pandas — lazy evaluation).
- `@pytest.mark.integration` — Call with `CONFIG.processed_dir`; assert `ddf.npartitions >= 1`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 23: Implement cumulative production feature

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_cumulative_production(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute the cumulative production column `cum_production` per well per product type:

1. Sort within each partition by `[well_id, product, production_date]` ascending using `map_partitions`.
2. Group by `[well_id, product]` and compute a cumulative sum of the `production` column using `.cumsum()`.
3. Assign the result as the `cum_production` column.
4. `cum_production` must be monotonically non-decreasing over time within each `[well_id, product]` group.
5. For zero-production months (shut-in wells), `cum_production` must equal the previous month's value (flat, not decreased).
6. Return the Dask DataFrame with `cum_production` added.

**Domain note:** Cumulative production (Np for oil, Gp for gas) is a fundamental decline curve analysis metric. It physically cannot decrease — once oil has been produced it cannot re-enter the reservoir. A flat period during shut-in is correct; a decrease indicates a pipeline bug.

**Error handling:**
- Raise `KeyError` if `production`, `well_id`, or `product` columns are absent.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, logging

**Test cases (`tests/test_features.py`):**

*Decline curve monotonicity (domain):*
- `@pytest.mark.unit` — Build a synthetic well sequence with monthly productions `[100, 80, 0, 60, 50]`; after `compute_cumulative_production` and `.compute()`, assert `cum_production` = `[100, 180, 180, 240, 290]` (monotonically non-decreasing).
- `@pytest.mark.unit` — Assert `cum_production` never decreases between consecutive rows within the same `[well_id, product]` group.

*Cumulative production flat periods (domain):*
- `@pytest.mark.unit` — Build a synthetic sequence with zero-production months mid-sequence: `[100, 50, 0, 0, 75]`; assert `cum_production` = `[100, 150, 150, 150, 225]` (flat during shut-in months, correct resumption).
- `@pytest.mark.unit` — Build a sequence with zero-production at the start (well not yet online): `[0, 0, 100, 80]`; assert `cum_production` = `[0, 0, 100, 180]`.
- `@pytest.mark.unit` — Build a sequence with zero-production at the end: `[100, 80, 0, 0]`; assert `cum_production` = `[100, 180, 180, 180]`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 24: Implement decline rate feature

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_decline_rate(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute the month-over-month production decline rate `decline_rate` per well per product:

1. Sort within each partition by `[well_id, product, production_date]` ascending using `map_partitions`.
2. Group by `[well_id, product]` and compute the lag-1 production value.
3. Compute raw decline rate as `(production[t] - production[t-1]) / production[t-1]`.
4. When `production[t-1] == 0.0` (denominator is zero — shut-in prior month), assign `decline_rate = NaN` **before** clipping. Do not allow division by zero to produce `inf` or raise an exception.
5. Clip the final `decline_rate` to `[CONFIG.decline_rate_clip_min, CONFIG.decline_rate_clip_max]` = `[-1.0, 10.0]`.
6. The first record for each well has no prior month; assign `NaN` for the first row.
7. Assign the result as the `decline_rate` column and return the updated Dask DataFrame.

**Domain note:** Decline rate is the primary indicator of reservoir performance. A rate of -1.0 means 100% decline (complete cessation). A rate above 10.0 would imply an 1000%+ increase — physically implausible as a sustained rate and is treated as a data artefact (e.g., well returning from extended shut-in after cumulative recalculation). The clip bounds are engineering design choices for ML feature stability.

**Error handling:**
- Raise `KeyError` if `production`, `well_id`, or `product` columns are absent.
- Division by zero must not raise; assign `NaN` as described.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, numpy, logging, kgs_pipeline.config

**Test cases (`tests/test_features.py`):**

*Decline rate clip bounds (domain):*
- `@pytest.mark.unit` — Build a well sequence where the computed raw decline rate would be `-2.5` (production drops from 100 to -150, but since negative production is already coerced to NaN in transform, test with production going from 100 to a very small positive value); alternatively build a sequence that yields raw decline < -1.0; assert the clipped value is exactly `-1.0`.
- `@pytest.mark.unit` — Build a well sequence where the raw decline rate would be `15.0` (production jumps from 10 to 160); assert the clipped value is exactly `10.0`.
- `@pytest.mark.unit` — Build a well sequence with a decline rate of `-0.5` (within bounds); assert the value passes through unchanged as `-0.5`.
- `@pytest.mark.unit` — Build a well sequence where `production[t-1] = 0.0` and `production[t] = 100.0` (well returning from shut-in); assert `decline_rate` for that row is `NaN`, not `inf`, and not an unclipped large value before clipping.
- `@pytest.mark.unit` — Assert the first row for each well has `decline_rate = NaN`.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 25: Implement rolling statistics features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_rolling_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute rolling window statistics of `production` per `[well_id, product]` group:

For each window size `w` in `CONFIG.rolling_windows` (default: `[3, 6, 12]`):
- `rolling_mean_{w}m`: rolling mean over the last `w` months, `min_periods=1`.
- `rolling_std_{w}m`: rolling standard deviation over the last `w` months, `min_periods=2` (returns `NaN` for windows with fewer than 2 observations).

Implementation must use `map_partitions` with pandas groupby rolling operations applied per partition. Note that because the data is partitioned by `well_id`, all records for a well are in a single partition — the rolling window calculation is correct within-partition.

Assign all computed columns to the Dask DataFrame and return it.

**Error handling:**
- Raise `KeyError` if `production`, `well_id`, or `product` columns are absent.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, pandas, logging, kgs_pipeline.config

**Test cases (`tests/test_features.py`):**

*Feature calculation correctness (domain):*
- `@pytest.mark.unit` — Build a single-well oil sequence with 6 known monthly production values `[100, 120, 80, 140, 100, 110]`; after `compute_rolling_features` and `.compute()`:
  - Assert `rolling_mean_3m` at month 3 (index 2) = `mean([100, 120, 80])` = `100.0`.
  - Assert `rolling_mean_3m` at month 4 (index 3) = `mean([120, 80, 140])` = `113.33...` (within 0.01 tolerance).
  - Assert `rolling_mean_6m` at month 6 (index 5) = `mean([100, 120, 80, 140, 100, 110])` = `108.33...` (within 0.01 tolerance).
- `@pytest.mark.unit` — Build a sequence with only 2 months of data; assert `rolling_mean_3m` for month 2 uses a partial window (`min_periods=1`) and returns a non-NaN value; assert `rolling_std_3m` for month 2 returns `NaN` (only 2 observations, `min_periods=2` met — actually returns a value; clarify: `rolling_std` with `min_periods=2` and window 3 on 2 observations returns a value; assert it is a valid finite float).
- `@pytest.mark.unit` — Build a sequence with only 1 month of data; assert `rolling_std_3m` is `NaN` (fewer than 2 observations for std dev).
- `@pytest.mark.unit` — Assert that columns `rolling_mean_3m`, `rolling_mean_6m`, `rolling_mean_12m`, `rolling_std_3m`, `rolling_std_6m` are all present in the output schema.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 26: Implement lag production features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_lag_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute lag production features per `[well_id, product]` group:

- `lag_1m`: production value from 1 month prior (shift of 1 within the sorted group).
- `lag_3m`: production value from 3 months prior (shift of 3 within the sorted group).

The first `n` rows per group (where `n` is the lag period) must have `NaN` for the corresponding lag column.

Implementation must sort within each partition by `[well_id, product, production_date]` ascending, then apply the group shift using `map_partitions`.

**Error handling:**
- Raise `KeyError` if `production`, `well_id`, or `product` columns are absent.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, pandas, logging

**Test cases (`tests/test_features.py`):**

*Lag feature correctness (domain):*
- `@pytest.mark.unit` — Build a single-well sequence with productions `[100, 80, 60, 40, 20]` (months 1–5); after `compute_lag_features` and `.compute()`:
  - Assert `lag_1m` at month 2 = `100.0`, month 3 = `80.0`, month 4 = `60.0`.
  - Assert `lag_1m` at month 1 = `NaN`.
  - Assert `lag_3m` at month 4 = `100.0`, month 5 = `80.0`.
  - Assert `lag_3m` at months 1, 2, 3 = `NaN`.
- `@pytest.mark.unit` — Assert `lag_1m` for month N equals exactly the raw production for month N-1 (test at least 3 consecutive months, verifying the lag-1 formula is `gas_mcf` or `oil_bbl` of the preceding month and not unit-swapped or incorrect).
- `@pytest.mark.unit` — Assert columns `lag_1m` and `lag_3m` are present in the output.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 27: Implement time-based features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_time_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute time-based features from `production_date` per well:

- `production_year`: calendar year as `int32`.
- `production_month`: calendar month (1–12) as `int32`.
- `production_quarter`: calendar quarter (1–4) as `int32`.
- `months_since_first_prod`: for each `[well_id, product]` group, compute the number of months elapsed since the well's first non-null `production_date`. The first production month is `0`. Result is `int64`.

**Error handling:**
- Raise `KeyError` if `production_date` or `well_id` columns are absent.
- Rows with `NaT` `production_date` must have `NaN` for `months_since_first_prod` and null for the year/month/quarter columns (do not drop).
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, pandas, logging

**Test cases (`tests/test_features.py`):**
- `@pytest.mark.unit` — Build a fixture with `production_date = Timestamp("2020-03-01")`; assert `production_year = 2020`, `production_month = 3`, `production_quarter = 1`.
- `@pytest.mark.unit` — Build a 3-row well sequence with dates 2020-01-01, 2020-02-01, 2020-03-01; assert `months_since_first_prod` = `[0, 1, 2]`.
- `@pytest.mark.unit` — Build a sequence with a gap: dates 2020-01-01, 2020-04-01; assert `months_since_first_prod` = `[0, 3]`.
- `@pytest.mark.unit` — Assert all four time columns are present in the output.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 28: Implement GOR, water cut, and WOR features

**Module:** `kgs_pipeline/features.py`
**Function:** `compute_ratio_features(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**
Compute ratio-based production metrics:

**GOR (Gas-Oil Ratio):**
Since each row in the processed data represents one product type (`O` or `G`) per well per month, GOR requires pivoting to align oil and gas on the same row:

1. Filter the DataFrame to `product == "O"` rows (oil) and `product == "G"` rows (gas) separately.
2. Perform a left join on `[well_id, production_date]` to align oil and gas production for the same well-month.
3. Compute `gor = gas_mcf / oil_bbl` where `gas_mcf` is the gas production and `oil_bbl` is the oil production.
4. GOR handling rules (domain-specified):
   - `oil_bbl > 0` and `gas_mcf >= 0`: compute normally; result is `gas_mcf / oil_bbl`.
   - `oil_bbl == 0` and `gas_mcf > 0`: assign `NaN` (mathematically undefined, physically valid gas-only state).
   - `oil_bbl == 0` and `gas_mcf == 0`: assign `NaN` or `0.0` as documented in the module (shut-in well).
   - `oil_bbl > 0` and `gas_mcf == 0`: assign `0.0` (producing oil with no associated gas).
5. Merge the computed `gor` column back onto the full DataFrame. Rows with `product == "G"` will have `gor = NaN` (GOR is defined for the well level, assigned to oil rows only).

**Water cut and WOR:**
- `water_cut`: assign `NaN` for all rows (KGS data does not include water production).
- `wor`: assign `NaN` for all rows.
- Add a comment in the code: `# water_bbl not available in KGS dataset; reserved for future enrichment`.

Return the updated Dask DataFrame with `gor`, `water_cut`, and `wor` columns added.

**Error handling:**
- Must not raise `ZeroDivisionError`. All division must be handled with `pandas.Series.where()` or equivalent.
- Must not call `.compute()`.

**Dependencies:** dask.dataframe, pandas, numpy, logging

**Test cases (`tests/test_features.py`):**

*GOR zero-denominator handling (domain):*
- `@pytest.mark.unit` — Build a fixture with one well-month where `oil_bbl=0` and `gas_mcf=100`; assert `gor = NaN` (not an exception, not `inf`).
- `@pytest.mark.unit` — Build a fixture with `oil_bbl=0` and `gas_mcf=0`; assert `gor` is `NaN` or `0.0` (not an exception).
- `@pytest.mark.unit` — Build a fixture with `oil_bbl=50` and `gas_mcf=0`; assert `gor = 0.0` (not `NaN`).
- `@pytest.mark.unit` — Build a fixture with `oil_bbl=100` and `gas_mcf=500`; assert `gor = 5.0`.
- `@pytest.mark.unit` — Assert GOR formula is `gas_mcf / oil_bbl` and not unit-swapped (i.e., the numerator is gas, denominator is oil).
- `@pytest.mark.unit` — Assert `water_cut` column is present and all values are `NaN`.
- `@pytest.mark.unit` — Assert `wor` column is present and all values are `NaN`.

*Water cut boundary values (domain):*
- `@pytest.mark.unit` — Document that `water_cut` is a `NaN` placeholder due to missing KGS water data; assert that the column exists and contains no non-NaN values in the output (since water data is unavailable, no outlier flagging should occur on this column).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 29: Implement categorical label encoding

**Module:** `kgs_pipeline/features.py`
**Functions:**
- `build_encoding_maps(ddf: dask.dataframe.DataFrame) -> dict[str, dict[str, int]]`
- `encode_categorical_features(ddf: dask.dataframe.DataFrame, encoding_maps: dict[str, dict[str, int]]) -> dask.dataframe.DataFrame`

**Description — `build_encoding_maps`:**
Compute a global encoding map (string → integer) for each categorical column: `county`, `operator`, `producing_zone`, `field`, `product`. For each column:
1. Call `.compute()` to materialise the unique values (this is one of two permitted `.compute()` calls in the features component — the other is in `write_features_parquet()`).
2. Sort the unique values alphabetically for deterministic encoding.
3. Map each value to a 0-based integer index.
4. Return a dict of dicts, e.g., `{"county": {"Barton": 0, "Ellis": 1, ...}, ...}`.

**Description — `encode_categorical_features`:**
Using the precomputed `encoding_maps`, add integer-encoded columns to the Dask DataFrame via `map_partitions`:
- `county_encoded` (int32)
- `operator_encoded` (int32)
- `producing_zone_encoded` (int32)
- `field_encoded` (int32)
- `product_encoded` (int32): `O` → 0, `G` → 1 (fixed, not from encoding map, for stability).

Unseen values (not in the encoding map) must map to `-1` (not raise a `KeyError`). Return the updated lazy Dask DataFrame.

**Error handling:**
- `build_encoding_maps` must call `.compute()` — this is intentional and documented.
- `encode_categorical_features` must not call `.compute()`.
- If a column required for encoding is absent, log a WARNING and skip encoding for that column.

**Dependencies:** dask.dataframe, pandas, logging

**Test cases (`tests/test_features.py`):**
- `@pytest.mark.unit` — Build a small fixture with 3 distinct `county` values; call `build_encoding_maps`; assert the returned map for `county` has 3 entries and values are 0, 1, 2.
- `@pytest.mark.unit` — Assert the encoding map is alphabetically sorted (deterministic).
- `@pytest.mark.unit` — Call `encode_categorical_features` with a known map; assert `county_encoded`, `operator_encoded`, `producing_zone_encoded`, `field_encoded`, `product_encoded` are all present in the output.
- `@pytest.mark.unit` — Assert `product_encoded` is `0` for `O` rows and `1` for `G` rows.
- `@pytest.mark.unit` — Pass a row with a `county` value not in the encoding map; assert `county_encoded = -1` (not an exception).
- `@pytest.mark.unit` — Assert return type of `encode_categorical_features` is `dask.dataframe.DataFrame`.

**Definition of done:** Both functions are implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 30: Implement features Parquet writer and CSV export

**Module:** `kgs_pipeline/features.py`
**Functions:**
- `write_features_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path | None = None) -> list[Path]`
- `export_feature_matrix_csv(ddf: dask.dataframe.DataFrame, output_dir: Path | None = None) -> Path`

**Description — `write_features_parquet`:**
Write the ML-ready Dask DataFrame to Parquet in `output_dir` (default: `CONFIG.features_dir`), partitioned by `well_id`. Use `pyarrow` engine. Enforce the expected schema by constructing a `pyarrow.schema()` object and passing it to `ddf.to_parquet(schema=...)`. This is one of the two permitted `.compute()` calls in the features component. Call `ensure_dir(output_dir)` before writing. Return a sorted list of all written `.parquet` file paths.

**Description — `export_feature_matrix_csv`:**
Write a sampled (up to 100,000 rows) version of the features DataFrame as a CSV to `output_dir / "feature_matrix.csv"`. Call `.compute()` on the sampled rows only. This is a convenience export for quick inspection — it is not ML-pipeline input. Use `ensure_dir(output_dir)`. Return the `Path` to the written CSV.

**Error handling:**
- If writing fails, log the exception at ERROR level and re-raise.
- Must not call `.compute()` except as explicitly described.

**Dependencies:** dask.dataframe, pyarrow, pathlib, logging, kgs_pipeline.config, kgs_pipeline.utils

**Test cases (`tests/test_features.py`):**
- `@pytest.mark.unit` — Build a small typed fixture Dask DataFrame; call `write_features_parquet(ddf, tmp_path)`; assert at least one `.parquet` file is written.
- `@pytest.mark.unit` — Read back one written Parquet file with `pandas.read_parquet`; assert all expected feature columns are present (spot-check the complete column list from the schema table above).
- `@pytest.mark.unit` — Assert every written Parquet file is readable by `dask.dataframe.read_parquet` without raising (Parquet readability test).
- `@pytest.mark.unit` — Call `export_feature_matrix_csv(ddf, tmp_path)`; assert the returned path exists and the CSV is parseable with `pandas.read_csv`.

**Feature column presence test (technical):**
- `@pytest.mark.unit` — Build a minimal single-well fixture and run all feature computation functions in sequence (Tasks 23–29). Call `write_features_parquet`. Read back the output and assert the output DataFrame contains **all** of the following columns: `well_id`, `production_date`, `product`, `production`, `unit`, `cum_production`, `decline_rate`, `rolling_mean_3m`, `rolling_mean_6m`, `rolling_mean_12m`, `rolling_std_3m`, `rolling_std_6m`, `lag_1m`, `lag_3m`, `months_since_first_prod`, `production_year`, `production_month`, `production_quarter`, `gor`, `water_cut`, `wor`, `county_encoded`, `operator_encoded`, `producing_zone_encoded`, `field_encoded`, `product_encoded`, `outlier_flag`.

**Schema stability test (technical):**
- `@pytest.mark.unit` — Write features Parquet from two fixture DataFrames representing two wells (one with all-null `latitude`); read each back and assert both have identical column names and dtypes.

**Definition of done:** Both functions are implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 31: Implement features orchestrator

**Module:** `kgs_pipeline/features.py`
**Function:** `run_features_pipeline(processed_dir: Path | None = None, output_dir: Path | None = None) -> list[Path]`

**Description:**
Orchestrate the full features workflow in sequence:
1. `load_processed_parquet(processed_dir)` → lazy Dask DataFrame.
2. `compute_cumulative_production(ddf)`.
3. `compute_decline_rate(ddf)`.
4. `compute_rolling_features(ddf)`.
5. `compute_lag_features(ddf)`.
6. `compute_time_features(ddf)`.
7. `compute_ratio_features(ddf)`.
8. `build_encoding_maps(ddf)` → encoding maps (calls `.compute()` once here).
9. `encode_categorical_features(ddf, encoding_maps)`.
10. `write_features_parquet(ddf, output_dir)` → written paths (calls `.compute()` via `to_parquet`).
11. `export_feature_matrix_csv(ddf, output_dir)`.

Return the list of written Parquet file paths. Log a summary at INFO level: input row count (from Parquet metadata), output Parquet file count, and elapsed time.

**Error handling:**
- If `load_processed_parquet` raises `RuntimeError`, propagate it.
- Log each stage at INFO level with elapsed time using `timer` decorator.

**Dependencies:** dask.dataframe, pathlib, logging, kgs_pipeline.config, kgs_pipeline.utils

**Test cases (`tests/test_features.py`):**
- `@pytest.mark.unit` — Run `run_features_pipeline` with all stage functions patched to return a small fixture DataFrame; assert the function returns a non-empty list and both `write_features_parquet` and `export_feature_matrix_csv` are called exactly once.
- `@pytest.mark.unit` — Patch `load_processed_parquet` to raise `RuntimeError`; assert `run_features_pipeline` propagates the error.
- `@pytest.mark.integration` — Run against real `CONFIG.processed_dir`; assert output files land in `CONFIG.features_dir`.

**Lazy Dask evaluation test (technical):**
- `@pytest.mark.unit` — For each of the feature computation functions (Tasks 23–29), assert that when called on a fixture `dask.dataframe.DataFrame` the return type is `dask.dataframe.DataFrame`, not `pandas.DataFrame`. This verifies no hidden `.compute()` calls exist.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 32: Implement pipeline orchestrator CLI

**Module:** `kgs_pipeline/pipeline.py`
**Functions:**
- `setup_pipeline_logging() -> logging.Logger`
- `run_pipeline(stages: list[str], years: list[int] | None, workers: int) -> dict`
- `main() -> None` (CLI entry point)

**Description — `setup_pipeline_logging`:**
Configure and return a root pipeline logger that writes structured logs with timestamps to both `logs/pipeline.log` (rotating, max 10 MB, 3 backups) and `stdout`. Use the `setup_logging` helper from `kgs_pipeline/utils.py` with name `"pipeline"`.

**Description — `run_pipeline`:**
Execute pipeline stages in the sequence: acquire → ingest → transform → features. Accept a list of stage names in `stages` (valid values: `"acquire"`, `"ingest"`, `"transform"`, `"features"`, `"all"`). If `stages == ["all"]`, run all four in sequence. Skip stages not in `stages`. Track:
- Start and end timestamps for the full pipeline run.
- Per-stage start time, end time, and status (`"success"` or `"failed"`).
- Total rows processed (read from Parquet metadata after each write step).
- File counts written per stage.

On stage failure, log the exception at ERROR level, mark the stage status as `"failed"`, and continue with the next stage if possible (acquire failure can still allow ingest to proceed with existing raw files; ingest failure blocks transform and features).

Return a `dict` with pipeline run metadata matching the structure saved to `data/pipeline_metadata.json`.

**Description — `main`:**
Parse CLI arguments using `argparse`:
- `--stages`: comma-separated list of stages to run (default: `"all"`).
- `--years`: comma-separated list of years to process (default: all years in `CONFIG.year_start`–`CONFIG.year_end`). This argument is informational and passed to acquire for filtering lease URLs by year if needed.
- `--workers`: number of Dask workers (default: `CONFIG.dask_n_workers`).

After parsing, call `run_pipeline(stages, years, workers)`. Serialize the returned metadata dict to `data/pipeline_metadata.json` using `json.dump` (with ISO 8601 datetime strings). Exit with code `0` on success, `1` if any stage failed.

**Dependencies:** argparse, json, logging, pathlib, datetime, kgs_pipeline.acquire, kgs_pipeline.ingest, kgs_pipeline.transform, kgs_pipeline.features, kgs_pipeline.config, kgs_pipeline.utils

**Test cases (`tests/test_pipeline.py`):**
- `@pytest.mark.unit` — Patch all four stage `run_*_pipeline` functions to return mock results; call `run_pipeline(["all"], None, 4)`; assert the returned dict contains keys `start_time`, `end_time`, `stages`, and `pipeline_metadata_path`.
- `@pytest.mark.unit` — Call `run_pipeline(["ingest", "transform"], None, 4)` with patched stages; assert only `run_ingest_pipeline` and `run_transform_pipeline` are called (not acquire or features).
- `@pytest.mark.unit` — Patch `run_acquire_pipeline` to raise an exception; assert `run_pipeline` does not raise, marks the acquire stage as `"failed"` in the returned dict, and continues to attempt the next stages.
- `@pytest.mark.unit` — Call `main()` with patched `run_pipeline`; assert `data/pipeline_metadata.json` is written and is valid JSON.
- `@pytest.mark.unit` — Call `main()` with `--stages acquire,ingest` as sys.argv; assert only the acquire and ingest stages are invoked.
- `@pytest.mark.integration` — Run the full pipeline end-to-end with 2 real lease URLs (mocked scraping returning fixture files); assert all four stages complete, output files exist in their respective directories, and `data/pipeline_metadata.json` is written.

**Definition of done:** All functions are implemented, test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
