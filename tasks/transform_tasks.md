# Transform Component — Task Specifications
**Module:** `kgs_pipeline/transform.py`  
**Test file:** `tests/test_transform.py`  
**Input directory:** `kgs/data/interim/`  
**Output directory:** `kgs/data/processed/`

---

## Design Decisions & Constraints

- All transformation functions consume and return `dask.dataframe.DataFrame` unless explicitly noted as operating on a single pandas partition (in which case they are wrapped via `map_partitions`).
- `.compute()` must NOT be called inside any transform function — the full Dask graph is built lazily and materialized only at the final `to_parquet` write.
- The canonical well identifier throughout this component is `well_id` (the exploded API number from the ingest step).
- The `product` column in the raw data uses `"O"` (oil, in BBL) and `"G"` (gas, in MCF). The transformation **pivots** these into separate numeric columns (`oil_bbl` and `gas_mcf`) so that each row represents one well × one month, with both oil and gas volumes on the same row.
- After pivoting, rows that had a zero-value production in the original data must remain as `0.0` (not `NaN`) in the pivoted column. Rows with no record at all for a product type in that month are `NaN` — this distinction is critical for ML feature engineering.
- Cumulative production columns `cumulative_oil_bbl` and `cumulative_gas_mcf` are computed per well, sorted by `production_date`, as a running sum using Dask-compatible window logic.
- The final processed output is partitioned Parquet written to `kgs/data/processed/wells/` with one partition per `well_id` (`partition_on=["well_id"]`).
- All column names in the output schema must be lowercase with underscores.
- Paths are sourced from `kgs_pipeline/config.py`.

---

## Output Schema (Processed Parquet)

| Column                  | Type           | Description                                              |
|-------------------------|----------------|----------------------------------------------------------|
| `well_id`               | `str`          | API well number (canonical well identifier)              |
| `lease_kid`             | `str`          | KGS Lease ID                                             |
| `lease`                 | `str`          | Lease name                                               |
| `dor_code`              | `str`          | Kansas Dept. of Revenue code                             |
| `field`                 | `str`          | Oil/gas field name                                       |
| `producing_zone`        | `str`          | Producing formation                                      |
| `operator`              | `str`          | Operator name                                            |
| `county`                | `str`          | Kansas county                                            |
| `township`              | `str`          | PLSS township number                                     |
| `twn_dir`               | `str`          | Township direction                                       |
| `range`                 | `str`          | PLSS range number                                        |
| `range_dir`             | `str`          | Range direction                                          |
| `section`               | `str`          | PLSS section                                             |
| `spot`                  | `str`          | Legal quarter description                                |
| `latitude`              | `float64`      | Well latitude (NAD 1927)                                 |
| `longitude`             | `float64`      | Well longitude                                           |
| `production_date`       | `datetime64`   | First day of the production month                        |
| `oil_bbl`               | `float64`      | Oil production in barrels (0.0 = reported zero)          |
| `gas_mcf`               | `float64`      | Gas production in MCF (0.0 = reported zero)              |
| `wells_count`           | `float64`      | Number of wells on the lease (from WELLS; limited quality) |
| `cumulative_oil_bbl`    | `float64`      | Running cumulative oil production per well               |
| `cumulative_gas_mcf`    | `float64`      | Running cumulative gas production per well               |

---

## Task 11: Implement interim Parquet reader

**Module:** `kgs_pipeline/transform.py`  
**Function:** `read_interim_parquet(interim_dir: Path) -> dask.dataframe.DataFrame`

**Description:**  
Read the interim Parquet dataset produced by the ingest step into a Dask DataFrame for transformation.

- Use `dask.dataframe.read_parquet` to read from `interim_dir / "kgs_monthly_raw.parquet"`.
- Return the resulting `dask.dataframe.DataFrame` without calling `.compute()`.
- Log the number of partitions at INFO level (use `ddf.npartitions`).

**Error handling:**
- If the Parquet path does not exist, raise `FileNotFoundError` with a descriptive message.
- If the Parquet files are unreadable (corrupt), let the Dask exception propagate after logging it as ERROR.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small Parquet file written to a temp directory, assert the function returns a `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Assert `.compute()` is not called (return type is `dask.dataframe.DataFrame`).
- `@pytest.mark.unit` — Given a path that does not exist, assert `FileNotFoundError` is raised.
- `@pytest.mark.integration` — Given the real interim Parquet at `INTERIM_DATA_DIR`, assert the result has more than 0 rows after `.compute()` and contains the column `well_id`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 12: Implement data type caster

**Module:** `kgs_pipeline/transform.py`  
**Function:** `cast_column_types(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Cast all columns to their correct types per the output schema. All columns arrive as `str` from the ingest step; this function applies the canonical dtypes.

Casting rules:
- `latitude`, `longitude`: cast to `float64`. Invalid (non-numeric) values become `NaN`.
- `production` (raw numeric string): cast to `float64`. Invalid values become `NaN`.
- `wells`: cast to `float64`. Invalid values become `NaN`. (Renamed to `wells_count`.)
- `production_date`: should already be `datetime64[ns]` from ingest; validate and re-cast if needed.
- `township`, `range`, `section`: cast to `str` (already str, but strip whitespace).
- All other string columns (`well_id`, `lease_kid`, `lease`, `dor_code`, `field`, `producing_zone`, `operator`, `county`, `twn_dir`, `range_dir`, `spot`, `product`, `lease`): strip leading/trailing whitespace; keep as `str`.
- Use `map_partitions` to apply casting partition-by-partition.
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called.

**Error handling:**
- Casting failures (e.g. a latitude value of `"N/A"`) must produce `NaN`, not raise exceptions. Use `pd.to_numeric(..., errors="coerce")` for numeric casts.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with `latitude = "39.99"`, assert the cast result has `latitude` as `float64` with value `39.99`.
- `@pytest.mark.unit` — Given `latitude = "N/A"`, assert the cast result is `NaN` (not an exception).
- `@pytest.mark.unit` — Given `production = "161.8"`, assert the cast result is `float64` value `161.8`.
- `@pytest.mark.unit` — Assert `production_date` is `datetime64[ns]` after casting.
- `@pytest.mark.unit` — Assert return type is `dask.dataframe.DataFrame` (no `.compute()` called).
- `@pytest.mark.unit` — Assert string columns have no leading or trailing whitespace after casting (check a sample value).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 13: Implement deduplicator

**Module:** `kgs_pipeline/transform.py`  
**Function:** `deduplicate(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Remove duplicate rows from the Dask DataFrame. A duplicate is defined as a row with the same `(well_id, production_date, product)` combination.

- Use `ddf.drop_duplicates(subset=["well_id", "production_date", "product"])` to remove duplicates.
- Keep the first occurrence of each duplicate group.
- Log the number of duplicates removed at INFO level (computed as the difference in row counts before and after — this may require `.compute()` in logging only, wrapped in a conditional debug flag, or deferred to a test assertion).
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called inside the function body.

**Error handling:**
- If any of the subset columns (`well_id`, `production_date`, `product`) are missing, raise `KeyError` with a descriptive message listing the missing columns.

**Dependencies:** `dask.dataframe`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with 4 rows where 2 rows share the same `(well_id, production_date, product)`, assert the result has 3 rows after deduplication.
- `@pytest.mark.unit` — Given a Dask DataFrame with no duplicates, assert the row count is unchanged.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing the `product` column, assert `KeyError` is raised.
- `@pytest.mark.unit` — Assert deduplication is **idempotent**: running `deduplicate` twice on the same input produces the same output as running it once (compare `.compute()` results in test).

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 14: Implement physical bounds validator

**Module:** `kgs_pipeline/transform.py`  
**Function:** `validate_physical_bounds(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Enforce domain-specific physical constraints on production values and flag or nullify violations.

Domain rules (must all be enforced):
1. **Non-negative production**: `production` column values must be >= 0. Negative values are physically impossible — set them to `NaN` and log a WARNING per violation batch.
2. **Oil rate ceiling**: For rows where `product == "O"`, `production > 50000` BBL/month for a single well is almost certainly a unit error. Flag these rows by setting a new boolean column `is_suspect_rate = True`; do NOT nullify the value (preserve for downstream review). All other rows have `is_suspect_rate = False`.
3. **Non-negative coordinate values**: `latitude` must be between 35.0 and 40.5 (approximate Kansas bounds); `longitude` must be between -102.5 and -94.5. Values outside these bounds are set to `NaN`.

- Apply all rules using `map_partitions`.
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called.

**Error handling:**
- If the `production` or `product` column is missing, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a row with `production = -50.0`, assert the output has `NaN` in the `production` column for that row.
- `@pytest.mark.unit` — Given a row with `product = "O"` and `production = 75000.0`, assert `is_suspect_rate = True` for that row.
- `@pytest.mark.unit` — Given a row with `product = "O"` and `production = 100.0`, assert `is_suspect_rate = False`.
- `@pytest.mark.unit` — Given a row with `latitude = 50.0` (outside Kansas), assert `latitude` becomes `NaN`.
- `@pytest.mark.unit` — Given a row with `longitude = -80.0` (outside Kansas), assert `longitude` becomes `NaN`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.
- `@pytest.mark.unit` — Given a DataFrame missing the `production` column, assert `KeyError` is raised.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 15: Implement product pivot

**Module:** `kgs_pipeline/transform.py`  
**Function:** `pivot_product_columns(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Transform the long-format data (one row per well × month × product type) into wide format (one row per well × month with separate `oil_bbl` and `gas_mcf` columns).

Domain context: the raw data has a `product` column (`"O"` or `"G"`) and a `production` column. After pivoting, each well-month has a single row with both `oil_bbl` and `gas_mcf`. If only oil was reported, `gas_mcf` is `NaN` (not `0.0`). If a well explicitly reported `0.0` production for oil, `oil_bbl` must be `0.0` (not `NaN`).

Pivot logic:
1. Use `map_partitions` to apply a pandas `pivot_table` (or equivalent groupby + unstack) on each partition.
2. Pivot on `product` column: `"O"` → `oil_bbl`, `"G"` → `gas_mcf`. Values come from the `production` column.
3. The aggregation function for the pivot must be `"first"` (to handle any residual duplicates) on the `production` values.
4. The group-by / index keys for the pivot are all columns except `product` and `production`: `well_id`, `production_date`, `lease_kid`, `lease`, `dor_code`, `field`, `producing_zone`, `operator`, `county`, `township`, `twn_dir`, `range`, `range_dir`, `section`, `spot`, `latitude`, `longitude`, `wells_count`, `is_suspect_rate`, `source_file`.
5. Rename the pivoted columns: `O` → `oil_bbl`, `G` → `gas_mcf`. If only one product type exists in a partition (e.g. no gas), add the missing column with `NaN` values.
6. Reset the index after pivoting so the group-by columns become regular columns again.
7. Ensure the `is_suspect_rate` column is preserved as boolean after pivoting.

- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called.
- The output must have the full output schema defined at the top of this spec (minus cumulative columns, which are added in the next step).

**Error handling:**
- If neither `"O"` nor `"G"` appears in the `product` column, raise `ValueError`.
- If required pivot key columns are missing, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a Dask DataFrame with 2 rows for the same well+month (one `"O"` with `production=100.0`, one `"G"` with `production=50.0`), assert the output has 1 row with `oil_bbl=100.0` and `gas_mcf=50.0`.
- `@pytest.mark.unit` — Given a well+month with only `"O"` recorded, assert the output row has `oil_bbl` set and `gas_mcf = NaN`.
- `@pytest.mark.unit` — Given a well+month where `"O"` production is `0.0`, assert the pivoted `oil_bbl = 0.0` (not `NaN`).
- `@pytest.mark.unit` — Assert the output DataFrame has columns `oil_bbl` and `gas_mcf`.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 16: Implement date gap filler

**Module:** `kgs_pipeline/transform.py`  
**Function:** `fill_date_gaps(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Ensure each well has a complete monthly date range from its first to last production date, with `NaN` inserted for missing months (not zero — missing data is distinct from zero production).

Domain context: KGS data frequently has gaps in monthly reporting (e.g. a well may report in January, March, and May but not February or April). These gaps represent missing data, not zero production. For ML time-series modelling, a complete and regular date index per well is required.

- Use `map_partitions` to apply gap-filling logic per partition, but since partitions may split wells across boundaries, first re-partition by `well_id` using `ddf.set_index("well_id")` or equivalent groupby approach to ensure all rows for a well are in the same partition before applying gap-filling.

  Implementation approach:
  1. Convert to pandas inside a `map_partitions` call on a well-grouped re-partition.
  2. For each unique `well_id` in the partition, compute the full monthly date range using `pandas.date_range(start=first_date, end=last_date, freq="MS")`.
  3. Reindex the well's time series to this full monthly range using `pd.DataFrame.reindex`.
  4. Fill non-production columns (metadata like `lease_kid`, `field`, `operator`, etc.) forward-fill from the nearest prior row using `ffill()` (these are static well attributes that don't change month-to-month).
  5. Leave `oil_bbl` and `gas_mcf` as `NaN` for the inserted gap months (do NOT fill with zero or ffill these).
  6. Reassemble all wells back into a single partition DataFrame, reset the index.

- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called except inside the `map_partitions` lambda (which executes partition-by-partition).

**Error handling:**
- If `well_id` or `production_date` columns are missing, raise `KeyError`.
- If a well has only one record (no range to fill), return it as-is.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a well with records for Jan-2020 and Mar-2020 (gap in Feb), assert the output has 3 rows for that well with Feb-2020 having `oil_bbl = NaN` and `gas_mcf = NaN`.
- `@pytest.mark.unit` — Assert that for the inserted gap row, non-production metadata columns (e.g. `county`, `operator`) are forward-filled (not `NaN`).
- `@pytest.mark.unit` — Given a well with a zero-production month (`oil_bbl = 0.0`) in the raw data, assert that after gap-filling the zero is preserved as `0.0` (not converted to `NaN`).
- `@pytest.mark.unit` — Given a well with only a single record, assert the output has exactly 1 row for that well.
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 17: Implement cumulative production calculator

**Module:** `kgs_pipeline/transform.py`  
**Function:** `compute_cumulative_production(ddf: dask.dataframe.DataFrame) -> dask.dataframe.DataFrame`

**Description:**  
Compute cumulative production columns (`cumulative_oil_bbl`, `cumulative_gas_mcf`) per well, sorted by `production_date`.

Domain context: Cumulative production (Np for oil, Gp for gas) is the total volume produced from a well from its first production date to any given month. It must be monotonically non-decreasing — a well cannot un-produce. In the gap-filled data, `NaN` months (missing data) must be handled by treating `NaN` as 0 in the running sum (i.e. we don't know production for that month, so we carry forward the cumulative total).

- Sort the DataFrame by `(well_id, production_date)` before computing cumulative sums.
- Use `map_partitions` on a well-partitioned Dask DataFrame (partition by `well_id`).
- Inside each partition, apply a `groupby("well_id")["oil_bbl"].cumsum()` — treating `NaN` as 0 for cumsum purposes only by using `fillna(0)` within the cumsum computation (original `oil_bbl` column must retain its `NaN` values).
- Store the result in `cumulative_oil_bbl` and `cumulative_gas_mcf`.
- The function must return a `dask.dataframe.DataFrame`; `.compute()` must NOT be called.

**Error handling:**
- If `oil_bbl` or `gas_mcf` columns are missing, raise `KeyError`.
- If `production_date` column is missing, raise `KeyError`.

**Dependencies:** `dask.dataframe`, `pandas`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a well with `oil_bbl = [100.0, 150.0, 200.0]` over 3 months, assert `cumulative_oil_bbl = [100.0, 250.0, 450.0]`.
- `@pytest.mark.unit` — Given a well with a `NaN` month in `oil_bbl` (gap), assert `cumulative_oil_bbl` carries forward the previous cumulative value (does not drop to NaN).
- `@pytest.mark.unit` — Assert `cumulative_oil_bbl` is monotonically non-decreasing for each well.
- `@pytest.mark.unit` — Assert `cumulative_gas_mcf` is monotonically non-decreasing for each well.
- `@pytest.mark.unit` — Given two wells in the same partition, assert cumulative sums are computed independently (not mixed across wells).
- `@pytest.mark.unit` — Assert the return type is `dask.dataframe.DataFrame`.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 18: Implement processed Parquet writer

**Module:** `kgs_pipeline/transform.py`  
**Function:** `write_processed_parquet(ddf: dask.dataframe.DataFrame, output_dir: Path) -> Path`

**Description:**  
Write the fully transformed Dask DataFrame to a well-partitioned Parquet dataset.

- Write to `output_dir / "wells"` using `dask.dataframe.to_parquet`.
- Partition by `well_id` using the `partition_on=["well_id"]` parameter. This creates one subdirectory per well under `output_dir / "wells"`.
- Use `write_index=False`, `overwrite=True`, and `compression="snappy"`.
- Return the `Path` to the `wells` output directory.
- Log the output path at INFO level.

**Error handling:**
- Create `output_dir` if it does not exist.
- Re-raise write exceptions after logging.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`

**Test cases:**
- `@pytest.mark.unit` — Given a small 4-row Dask DataFrame with 2 distinct `well_id` values, assert the function returns a `Path` ending in `"wells"`.
- `@pytest.mark.integration` — After writing, assert the output directory contains subdirectories (one per `well_id`).
- `@pytest.mark.integration` — Assert that each subdirectory (partition) contains `.parquet` files readable by `dask.dataframe.read_parquet`.
- `@pytest.mark.integration` — **Partition correctness**: Read back each partition file and assert all rows within it have the same `well_id` — no partition contains rows from multiple wells.
- `@pytest.mark.integration` — **Schema stability**: Sample the schema from 2 different well partitions and assert column names and dtypes are identical.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task 19: Implement transform pipeline orchestrator

**Module:** `kgs_pipeline/transform.py`  
**Function:** `run_transform_pipeline(interim_dir: Path, output_dir: Path) -> Path`

**Description:**  
Top-level entry point for the transform component. Chains all transformation steps and returns the path to the processed Parquet output.

Execution sequence:
1. `read_interim_parquet(interim_dir)` → raw Dask DataFrame
2. `cast_column_types(ddf)` → typed DataFrame
3. `deduplicate(ddf)` → deduplicated DataFrame
4. `validate_physical_bounds(ddf)` → bounds-validated DataFrame
5. `pivot_product_columns(ddf)` → wide-format DataFrame
6. `fill_date_gaps(ddf)` → gap-filled DataFrame
7. `compute_cumulative_production(ddf)` → cumulative columns added
8. `write_processed_parquet(ddf, output_dir)` → Parquet on disk

- Log start, end, and elapsed time of each step at INFO level.
- The full Dask computation graph is triggered only by `to_parquet` in step 8 — no `.compute()` calls in steps 1–7.
- Return the output `Path`.

**Error handling:**
- Propagate all exceptions after logging them with the step name.

**Dependencies:** `dask.dataframe`, `pathlib`, `logging`, `time`

**Test cases:**
- `@pytest.mark.unit` — Mock all sub-functions. Assert they are called in the correct sequence (1 → 8).
- `@pytest.mark.unit` — Assert no `.compute()` is called on any intermediate DataFrame (verify return types remain `dask.dataframe.DataFrame` at each step through mocks).
- `@pytest.mark.integration` — Given a real interim Parquet input (from `INTERIM_DATA_DIR`), run the full pipeline and assert the `wells` output directory exists and contains at least one `.parquet` file.
- `@pytest.mark.integration` — **Data integrity spot-check**: For 5 randomly selected `(well_id, production_date)` pairs, read the corresponding row from the raw interim Parquet and the processed Parquet, and assert `oil_bbl` (or `gas_mcf`) matches the original `production` value (accounting for the pivot).
- `@pytest.mark.integration` — **Row count reconciliation**: Assert processed row count >= raw row count after gap-filling, and deduplicated row count <= raw row count (before gap-filling).
- `@pytest.mark.integration` — **Sort stability**: For a sample well with multiple Parquet partition files, assert the last `production_date` in file N is earlier than the first `production_date` in file N+1.
- `@pytest.mark.integration` — **Well completeness check**: For 3 randomly selected wells, assert the count of monthly records equals the number of months between the first and last `production_date` (inclusive).
- `@pytest.mark.integration` — **Monotonicity**: For 3 randomly selected wells, assert `cumulative_oil_bbl` and `cumulative_gas_mcf` are monotonically non-decreasing over time.
- `@pytest.mark.integration` — **Zero preservation**: For wells that had `production = 0.0` in the raw data, assert `oil_bbl = 0.0` (not `NaN`) in the processed output.
- `@pytest.mark.integration` — **Parquet readability**: Assert every `.parquet` file written to `data/processed/wells/` can be read by a fresh `dask.dataframe.read_parquet` call without errors.

**Definition of done:** Function is implemented, all test cases pass, ruff and mypy report no errors, requirements.txt updated with all third-party packages imported in this task.
