# Task Index

## Pipeline Overview

This task index covers the KGS (Kansas Geological Survey) oil and gas production data pipeline
for 2020–2025. The pipeline acquires per-lease monthly production data from the KGS web portal,
ingests and partitions the raw files, cleans and validates the data, and engineers ML-ready
features. Each component is specified in a dedicated task file.

---

## Task Files

| # | Task File                        | Description                                                                                                                                                                                                                 |
|---|----------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | `tasks/acquire_tasks.md`         | Playwright-based async web-scraping workflow to download per-lease monthly production `.txt` files from the KGS portal, driven by the lease URL index, rate-limited to 5 concurrent requests via `asyncio.Semaphore(5)`.  |
| 2 | `tasks/ingest_tasks.md`          | Dask-based ingestion of all raw `lp*.txt` files, filtering of non-monthly records, left-join enrichment with lease-level metadata from the archive index, and writing a unified interim Parquet store partitioned by `LEASE_KID`. |
| 3 | `tasks/transform_tasks.md`       | Data cleaning: column renaming to `snake_case`, date parsing, dtype casting, per-well API number explosion, physical bound validation, unit labelling, deduplication, chronological sort, and writing processed Parquet partitioned by `well_id`. |
| 4 | `tasks/features_tasks.md`        | ML-ready feature engineering per well: time features, cumulative production (Np/Gp), decline rate, rolling statistics (3-month/6-month), GOR/WOR placeholders, operator and county aggregations, categorical label encoding, plus main pipeline orchestrator `pipeline.py`. |

---

## Pipeline Execution Order

```
run_acquire_pipeline()   →   run_ingest_pipeline()   →   run_clean_pipeline()   →   run_features_pipeline()
  kgs_pipeline/               kgs_pipeline/               kgs_pipeline/               kgs_pipeline/
  acquire.py                  ingest.py                   clean.py                    process.py
  data/raw/                   data/interim/               data/processed/             data/processed/features/
```

The top-level orchestrator `kgs_pipeline/pipeline.py` calls each stage in sequence and
supports selective stage execution via the `--stages` CLI argument.

---

## Module Map

| Module                     | Entry-Point Function       | Output Location              |
|----------------------------|----------------------------|------------------------------|
| `kgs_pipeline/config.py`   | _(constants only)_         | _(sets up data directories)_ |
| `kgs_pipeline/acquire.py`  | `run_acquire_pipeline()`   | `data/raw/`                  |
| `kgs_pipeline/ingest.py`   | `run_ingest_pipeline()`    | `data/interim/`              |
| `kgs_pipeline/clean.py`    | `run_clean_pipeline()`     | `data/processed/`            |
| `kgs_pipeline/process.py`  | `run_features_pipeline()`  | `data/processed/features/`   |
| `kgs_pipeline/pipeline.py` | `run_pipeline()`           | _(orchestrates all above)_   |

---

## Test File Map

| Test File                  | Covers                                                                     | Task Numbers  |
|----------------------------|----------------------------------------------------------------------------|---------------|
| `tests/test_acquire.py`    | URL loading, Playwright scraping, semaphore rate-limiting, orchestrator    | Tasks 01–05   |
| `tests/test_ingest.py`     | File discovery, raw CSV loading, monthly record filtering, metadata join, Parquet write, ingest orchestrator | Tasks 06–11 |
| `tests/test_clean.py`      | Column renaming, date parsing, dtype casting, API explosion, physical validation, unit labelling, deduplication, sorting, processed Parquet write, domain checks | Tasks 12–22 |
| `tests/test_process.py`    | Time features, cumulative production, decline rate, rolling statistics, GOR/WOR, aggregations, label encoding, features Parquet write, pipeline orchestrator, domain checks | Tasks 23–33 |

---

## Pytest Markers

| Marker                      | When to Use                                                                         |
|-----------------------------|-------------------------------------------------------------------------------------|
| `@pytest.mark.unit`         | No network access required; no files needed in `data/raw`, `data/interim`, or `data/processed`. |
| `@pytest.mark.integration`  | Requires network access **or** real data files on disk in `data/raw/`, `data/interim/`, or `data/processed/`. |

---

## Key Design Decisions

| Decision                                       | Rationale                                                                                                     |
|------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| Dask for all parallel processing               | Lazy evaluation, partition-level parallelism, scalable out-of-core computation across thousands of lease files |
| `asyncio.Semaphore(5)` for scraping            | Prevents overloading the KGS public academic server                                                           |
| Partition by `well_id` in processed/features   | Co-locates all records for a well for correct cumulative and rolling computations                              |
| Zero ≠ NaN distinction                         | Zeros are valid production measurements; nulls are missing data — ML models must treat them differently        |
| `outlier_flag` column instead of dropping      | Preserves data for human review; ML models can learn from flagged data if desired                             |
| Explicit `pyarrow.Schema` at write time        | Prevents schema drift when a partition has all-null columns and type inference guesses wrong                   |
| Global encoding map for categoricals           | Guarantees consistent integer codes across all Dask partitions for ML reproducibility                         |
| Single `.compute()` per pipeline stage         | All transformation functions remain lazy; computation deferred to the write step, maximising Dask graph optimisation |
| `LEASE_<lease_kid>` synthetic well IDs         | Preserves lease-level production records when no API number has been linked to a lease                        |
| Rate-limit with retry + exponential back-off   | Robust handling of transient KGS server timeouts without hammering the server                                  |
