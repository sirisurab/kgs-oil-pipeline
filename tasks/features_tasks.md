# Features Stage Tasks

**Module:** `kgs_pipeline/features.py`
**Test file:** `tests/test_features.py`

Read `stage-manifest-features.md`, `boundary-transform-features.md`, `ADRs.md`
(ADR-001, ADR-002, ADR-003, ADR-004, ADR-005, ADR-007, ADR-008), and
`build-env-manifest.md` before implementing any task in this file.

Input state is defined in `boundary-transform-features.md`. The features stage
receives entity-indexed, temporally-sorted, categorically-cast data with
`max(10, min(n, 50))` partitions. Per `boundary-transform-features.md`, no
re-sorting, re-indexing, re-casting, or re-partitioning is needed before feature
computation begins.

All time-dependent features require the sort established in transform to be
preserved at the point each feature is computed (`stage-manifest-features.md` H1).

All per-entity aggregations must use vectorized grouped transformations (ADR-002).
Per-row iteration is prohibited.

---

## Task F-01: Compute cumulative production volumes

**Module:** `kgs_pipeline/features.py`
**Function:** `add_cumulative_production(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition. For each unique
`LEASE_KID`/`PRODUCT` group within the partition, compute cumulative sums of
`PRODUCTION` and add them as new columns: `cum_production`. The cumulative sum must
be computed in temporal order (`production_date` ascending) — the sort is guaranteed
by `boundary-transform-features.md` and must be preserved at computation time per
`stage-manifest-features.md` H1. Shut-in months (zero `PRODUCTION`) must produce a
flat cumulative value equal to the prior month — not a decrease (TR-03, TR-08).
Return the partition with `cum_production` added; all other columns must be
unchanged.

All per-entity aggregations must use vectorized grouped `transform` — no iteration
over groups (ADR-002).

**Error handling:**
- If `PRODUCTION`, `LEASE_KID`, `PRODUCT`, or `production_date` are absent, raise
  `KeyError`.

**Dependencies:** pandas, logging

**Test cases (unit — TR-03, TR-08):**
- Given a synthetic well sequence with monotonically increasing production, assert
  `cum_production` is monotonically non-decreasing (TR-03).
- Given a well with a mid-sequence shut-in month (`PRODUCTION = 0.0`), assert
  `cum_production` is flat (equal to the prior month's value) for that month (TR-08a).
- Given a well where `PRODUCTION = 0.0` for the first month (well not yet online),
  assert `cum_production` is `0.0` for that month (TR-08b).
- Given a well that resumes production after shut-in, assert `cum_production` resumes
  correctly from the flat value (TR-08c).
- Given a partition missing `PRODUCTION`, assert `KeyError` is raised.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-02: Compute GOR and water cut

**Module:** `kgs_pipeline/features.py`
**Function:** `add_ratio_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition. The input carries `PRODUCTION`
values for both oil (`PRODUCT == "O"`) and gas (`PRODUCT == "G"`) rows within the
same lease. Pivot or align the two `PRODUCT` rows per `LEASE_KID`/`production_date`
to make oil (BBL) and gas (MCF) available as separate columns, then compute:

- `gor`: gas_mcf / oil_bbl — GOR in MCF/BBL. Handle the zero-denominator cases
  exactly as specified in TR-06: (a) `oil_bbl == 0` and `gas_mcf > 0` → `NaN`;
  (b) both zero → `0.0` or `NaN` (consistent sentinel); (c) `oil_bbl > 0` and
  `gas_mcf == 0` → `0.0`, not `NaN`. Do not treat high GOR as an error (TR-06).
- `water_cut`: water_bbl / (oil_bbl + water_bbl) — water cut as a fraction in
  [0, 1]. Handle boundary values: `water_bbl == 0` and `oil_bbl > 0` → `0.0`
  (valid, not an outlier — TR-10); `oil_bbl == 0` and `water_bbl > 0` → `1.0`
  (valid late-life state, not an error — TR-10). Only values outside `[0, 1]` are
  invalid (TR-01). The `PRODUCTION` column in KGS data covers oil and gas — water
  is not available as a separate column in the raw data; `water_cut` must be set
  to `NaN` where water data is unavailable.

Return the partition with `gor` and `water_cut` added; all other columns unchanged.

**Error handling:**
- If `PRODUCTION` or `PRODUCT` is absent, raise `KeyError`.
- ZeroDivisionError must not propagate — handle all zero-denominator cases
  explicitly (TR-06).

**Dependencies:** pandas, numpy, logging

**Test cases (unit — TR-01, TR-06, TR-09, TR-10):**
- Given `oil_bbl > 0` and `gas_mcf == 0`, assert `gor == 0.0`, not `NaN` (TR-06c).
- Given `oil_bbl == 0` and `gas_mcf > 0`, assert `gor` is `NaN`, not an exception
  (TR-06a).
- Given `oil_bbl == 0` and `gas_mcf == 0`, assert `gor` is `0.0` or `NaN` and no
  exception is raised (TR-06b).
- Given `water_bbl == 0` and `oil_bbl > 0`, assert `water_cut == 0.0` and the row
  is retained (TR-10a).
- Given `oil_bbl == 0` and `water_bbl > 0`, assert `water_cut == 1.0` and the row
  is retained — this is a valid end-of-life state (TR-10b).
- Assert `water_cut` formula uses `water_bbl / (oil_bbl + water_bbl)` (total liquid
  denominator, not oil alone — TR-09e).
- Assert `gor` formula uses `gas_mcf / oil_bbl` and not a unit-swapped variant
  (TR-09d).

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-03: Compute production decline rate

**Module:** `kgs_pipeline/features.py`
**Function:** `add_decline_rate(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition. For each `LEASE_KID`/`PRODUCT`
group, compute the period-over-period production decline rate as a derived column
`decline_rate`. After computing the raw decline rate, clip it to the range
`[-1.0, 10.0]` per TR-07 — these are ML feature stability bounds, not physical
constants. Handle the shut-in case (zero production in two consecutive months)
explicitly before clipping so that it does not produce an unclipped extreme value
(TR-07d). Return the partition with `decline_rate` added; all other columns unchanged.

All per-entity computations must use vectorized grouped transformations (ADR-002).

**Error handling:**
- If `PRODUCTION`, `LEASE_KID`, `PRODUCT`, or `production_date` are absent, raise
  `KeyError`.
- Division-by-zero in the raw decline calculation must not raise; handle it before
  the clip is applied (TR-07d).

**Dependencies:** pandas, numpy, logging

**Test cases (unit — TR-07):**
- Given a computed decline rate below `-1.0`, assert it is clipped to exactly `-1.0`
  (TR-07a).
- Given a computed decline rate above `10.0`, assert it is clipped to exactly `10.0`
  (TR-07b).
- Given a decline rate within `[-1.0, 10.0]`, assert it passes through unchanged
  (TR-07c).
- Given two consecutive months of zero production (shut-in), assert no unclipped
  extreme value is produced before clipping is applied (TR-07d).
- Use synthetic well sequences with known month-over-month changes to make
  assertions exact.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-04: Compute rolling averages and lag features

**Module:** `kgs_pipeline/features.py`
**Function:** `add_rolling_features(df: pd.DataFrame) -> pd.DataFrame`

**Description:** Operate on a single pandas partition. For each `LEASE_KID`/`PRODUCT`
group, compute rolling and lag features on `PRODUCTION` in temporal order. The
temporal sort from transform is preserved per `boundary-transform-features.md` H1
and must not be disturbed. Required derived columns:
- `rolling_3m`: 3-month rolling average of `PRODUCTION`
- `rolling_6m`: 6-month rolling average of `PRODUCTION`
- `lag_1`: lag-1 feature — value of `PRODUCTION` for the prior month

For the rolling windows, when available history is shorter than the window size
(e.g. first 2 months of a well's life), the result must be `NaN` or a partial-window
value — not silently zero or a wrong fill (TR-09b). The `lag_1` feature for month N
must equal the raw `PRODUCTION` value for month N-1 (TR-09c).

All per-entity computations must use vectorized grouped transformations (ADR-002).

Return the partition with `rolling_3m`, `rolling_6m`, and `lag_1` added; all other
columns unchanged.

**Error handling:**
- If `PRODUCTION` or `production_date` are absent, raise `KeyError`.

**Dependencies:** pandas, logging

**Test cases (unit — TR-09):**
- Given a known synthetic 6-month production sequence, assert `rolling_3m` and
  `rolling_6m` values match hand-computed expected values (TR-09a).
- Given a well with only 2 months of history, assert `rolling_3m` is `NaN` (or
  partial window as specified) — not silently zero (TR-09b).
- Given a 3-month sequence, assert `lag_1` for month 3 equals the `PRODUCTION`
  value for month 2 (TR-09c).
- Assert `lag_1` for the first month of a well's history is `NaN`.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.

---

## Task F-05: Orchestrate features stage

**Module:** `kgs_pipeline/features.py`
**Function:** `features(config: dict) -> dask.dataframe.DataFrame`

**Description:** Orchestrate the full features stage. Read the processed Parquet
from the path in `config`. Per `boundary-transform-features.md`, the input is
entity-indexed on `{entity_col}`, temporally sorted within each partition,
categoricals cast, and partitioned to `max(10, min(n, 50))` — no re-sorting,
re-indexing, re-casting, or re-partitioning is needed before feature computation.
Apply `add_cumulative_production`, `add_ratio_features`, `add_decline_rate`, and
`add_rolling_features` via `map_partitions`. For every `map_partitions` call,
derive `meta` by calling the actual function on a zero-row copy of `_meta` —
no independently constructed meta is permitted (ADR-003). Repartition to
`max(10, min(n, 50))` and write the result to `data/processed/` as Parquet.
Return the Dask DataFrame (lazy — do not call `.compute()`, per TR-17 and ADR-005).

**Error handling:**
- If the processed Parquet input path does not exist, raise `FileNotFoundError`.
- Per-partition errors must be surfaced; do not silently swallow them.

**Dependencies:** dask, dask.dataframe, pandas, pyarrow, logging, pathlib

**Test cases (unit — TR-17):**
- Assert the return type is `dask.dataframe.DataFrame`, not `pd.DataFrame`.

**Test cases (TR-19 — feature column presence, unit):**
- Using a synthetic single-well input, assert the output DataFrame contains all
  expected derived columns: `cum_production`, `gor`, `water_cut`, `decline_rate`,
  `rolling_3m`, `rolling_6m`, `lag_1`.

**Test cases (TR-14 — schema stability, unit):**
- Assert that column names and dtypes sampled from one partition match those sampled
  from another partition.

**Test cases (TR-23 — map_partitions meta consistency, unit):**
- For every `map_partitions` call wrapping `add_cumulative_production`,
  `add_ratio_features`, `add_decline_rate`, and `add_rolling_features`, assert the
  `meta=` argument matches the function output in column names, column order, and
  dtypes by calling the function on `ddf._meta.copy()`.

**Test cases (TR-18 — Parquet readability, integration):**
- Assert every Parquet file written to `tmp_path` is readable by a fresh
  `dask.dataframe.read_parquet` call.

**Test cases (TR-26 — features integration test, integration):**
- Run `features()` on the transform Parquet output of TR-25 with all output paths
  pointing to pytest's `tmp_path`.
- Assert features Parquet is readable.
- Assert all derived feature columns defined in `stage-manifest-features.md` are
  present.
- Assert schema is consistent across a sample of partitions.

**Test cases (TR-27 — end-to-end pipeline integration test, integration):**
- Run the full pipeline (ingest → transform → features) on 2–3 real raw source
  files with all output paths pointing to pytest's `tmp_path`.
- Assert ingest output satisfies all guarantees in `boundary-ingest-transform.md`.
- Assert transform output satisfies all guarantees in `boundary-transform-features.md`.
- Assert features output satisfies TR-26.
- Assert no unhandled exceptions are raised across the full run.

**Definition of done:** Function is implemented, test cases pass, ruff and mypy report
no errors, requirements.txt updated with all third-party packages imported in this task.
