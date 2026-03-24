# Task Index

| # | Task File | Description |
|---|-----------|-------------|
| 1 | `tasks/acquire_tasks.md` | Playwright-based async web scraper with `asyncio.Semaphore(5)` rate-limiting to download per-lease monthly production `.txt` files from the KGS portal, driven by URLs in `references/oil_leases_2020_present.txt`, orchestrated via Dask delayed with a single `.compute()` call. |
| 2 | `tasks/ingest_tasks.md` | Dask-based ingestion of all raw per-lease `.txt` files from `data/raw/`, metadata enrichment via left-join with the KGS lease archive index, filtering of non-monthly records (yearly and starting-cumulative rows), and writing a unified interim Parquet store to `data/interim/` partitioned by `LEASE_KID`. |
| 3 | `tasks/transform_tasks.md` | Data cleaning and well-level restructuring: column renaming to snake_case, dtype casting, date parsing, API number explosion to one-row-per-well, physical bounds validation with `outlier_flag`, zero-vs-null preservation, deduplication, unit labelling, chronological sort, and writing processed Parquet to `data/processed/` partitioned by `well_id`. |
| 4 | `tasks/features_tasks.md` | ML-ready feature engineering per well: cumulative production (Np/Gp), decline rate, 3-month and 6-month rolling statistics, time-since-first-production, GOR, water_cut and WOR placeholders, and globally consistent categorical label encoding; output written to `data/processed/features/` partitioned by `well_id`. |

## Pipeline Execution Order

```
run_acquire_pipeline()  →  run_ingest_pipeline()  →  run_transform_pipeline()  →  run_features_pipeline()
    acquire.py                  ingest.py                  transform.py                  features.py
    data/raw/                   data/interim/              data/processed/               data/processed/features/
```

## Component Summary

### Component 1 — Acquire (`kgs_pipeline/acquire.py`)
- **Tasks 01–05**
- Defines all configuration constants in `kgs_pipeline/config.py` (Task 01)
- Defines `ScrapingError(Exception)` custom exception (Task 02)
- Reads and deduplicates lease URLs from `references/oil_leases_2020_present.txt` (Task 03)
- Scrapes each lease page via Playwright async API, rate-limited to 5 concurrent requests (Task 04)
- Orchestrates the full async acquisition workflow with Dask delayed (Task 05)
- Downloads raw per-lease `.txt` files to `data/raw/`
- Idempotent: skips already-downloaded files on re-run

### Component 2 — Ingest (`kgs_pipeline/ingest.py`)
- **Tasks 06–11**
- Discovers all `.txt` files in `data/raw/` (Task 06)
- Reads each file into a lazy Dask DataFrame with encoding fallback and `source_file` traceability (Task 07)
- Concatenates all per-file Dask DataFrames, skipping malformed files (Task 08)
- Filters out yearly (Month=0) and starting-cumulative (Month=-1) records (Task 09)
- Left-joins with lease-level metadata from the archive index (Task 10)
- Writes unified interim Parquet to `data/interim/` partitioned by `LEASE_KID` (Task 11)
- All functions return lazy Dask DataFrames; `.compute()` is deferred to `to_parquet()`

### Component 3 — Transform (`kgs_pipeline/transform.py`)
- **Tasks 12–21**
- Extends `config.py` with transform-stage constants (Task 12)
- Renames columns to `snake_case`, casts to correct dtypes (Task 13)
- Parses `M-YYYY` string into `datetime64[ns]` `production_date` column (Task 14)
- Explodes comma-separated `api_number` to one row per well, assigns `well_id` (Task 15)
- Validates physical bounds: negative production → `NaN`, oil > 50k BBL → `outlier_flag=True` (Task 16)
- Assigns `unit` column: `"BBL"` for oil, `"MCF"` for gas (Task 17)
- Deduplicates on `[well_id, production_date, product]`, keeping last occurrence (Task 18)
- Sorts per-well records chronologically and repartitions by `well_id` (Task 19)
- Writes processed Parquet to `data/processed/` with PyArrow schema enforcement (Task 20)
- Orchestrates the full transform workflow and runs domain integrity tests (Task 21)
- Zero production preserved as `0.0`; missing data preserved as `NaN`

### Component 4 — Features (`kgs_pipeline/features.py`)
- **Tasks 22–31**
- Extends `config.py` with features-stage constants (Task 22)
- Computes per-well cumulative production `cumulative_production` (Np/Gp) (Task 23)
- Computes month-over-month `decline_rate`, clipped to `[-1.0, 10.0]` (Task 24)
- Computes 3-month and 6-month rolling mean and std dev (Task 25)
- Derives `months_since_first_prod`, `production_year`, `production_month` (Task 26)
- Computes gas-oil ratio `gor` via per-partition oil/gas pivot (Task 27)
- Adds `water_cut` and `wor` as `NaN` float64 placeholder columns (Task 28)
- Label-encodes categorical columns with globally consistent codes across all partitions (Task 29)
- Writes ML-ready feature Parquet to `data/processed/features/` with schema enforcement (Task 30)
- Orchestrates the full features workflow and runs domain integrity tests (Task 31)

## Test File Index

| Test File | Covers |
|-----------|--------|
| `tests/test_acquire.py` | Tasks 01–05: config constants, ScrapingError, URL loading, Playwright scraping, orchestrator |
| `tests/test_ingest.py` | Tasks 06–11: file discovery, raw file reading, concatenation, filtering, metadata join, Parquet write |
| `tests/test_transform.py` | Tasks 12–21: column renaming, dtype casting, date parsing, API explosion, physical validation, deduplication, sort, Parquet write |
| `tests/test_features.py` | Tasks 22–31: feature engineering, cumulative production, decline rate, rolling stats, time features, GOR, label encoding, Parquet write |

## Pytest Markers

| Marker | Meaning |
|--------|---------|
| `@pytest.mark.unit` | Does not require network access or files on disk in `data/raw/`, `data/interim/`, `data/processed/`, or `data/processed/features/` |
| `@pytest.mark.integration` | Requires network access, or data files in `data/raw/`, `data/interim/`, `data/processed/`, or `data/processed/features/` |

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Dask for all parallel processing | Enables lazy evaluation, partition-level parallelism, and scalable out-of-core computation across thousands of lease files |
| `asyncio.Semaphore(5)` for scraping | Prevents overloading the KGS server; KGS is a public academic resource with no rate-limit API |
| Partition by `well_id` in processed/features | Ensures all records for a well are co-located for correct per-well cumulative and rolling calculations |
| Zero ≠ NaN distinction | Zeros are valid production measurements (well shut in); nulls are missing data — ML models must treat them differently |
| `outlier_flag` column instead of dropping | Preserves flagged data for human review and lets ML models learn from anomalous observations if desired |
| `pyarrow.schema()` at write time | Prevents schema drift across well partitions when one well has all-null columns and type inference guesses wrong |
| Global encoding map in `encode_categorical_features()` | Guarantees consistent integer codes across all Dask partitions for ML reproducibility |
| Single `.compute()` per orchestrator | All pipeline functions remain lazy; computation is deferred until the write step, maximising Dask graph optimisation |
| `blocksize=None` in `read_csv()` | One Dask partition per raw file allows clean `source_file` column assignment without cross-file contamination |
| Synthetic `well_id` for leases with no API number | Preserves production data for leases not yet linked to individual well API numbers, avoiding data loss |
