# Task Index

| # | Task File | Description |
|---|-----------|-------------|
| 1 | `tasks/acquire_tasks.md` | Web-scraping workflow using Playwright and Dask to download per-lease monthly production `.txt` files from the KGS portal, driven by URLs in `oil_leases_2020_present.txt`, with rate-limiting via `asyncio.Semaphore(5)`. |
| 2 | `tasks/ingest_tasks.md` | Dask-based ingestion of all raw per-lease `.txt` files, metadata enrichment from the lease archive index, filtering of non-monthly records, and writing a unified interim Parquet store partitioned by `LEASE_KID`. |
| 3 | `tasks/transform_tasks.md` | Data cleaning, type casting, date parsing, well-level explosion of comma-separated API numbers, physical bound validation, deduplication, unit labelling, chronological sorting, and writing processed Parquet files partitioned by `well_id`. |
| 4 | `tasks/features_tasks.md` | ML-ready feature engineering per well: cumulative production (Np/Gp), decline rate, rolling statistics (3-month and 6-month), time-since-first-production, GOR/WOR/water-cut placeholders, and categorical label encoding; output written to a features Parquet store partitioned by `well_id`. |

## Pipeline Execution Order

```
run_acquire_pipeline()   →   run_ingest_pipeline()   →   run_transform_pipeline()   →   run_features_pipeline()
   acquire.py                    ingest.py                    transform.py                    features.py
   kgs/data/raw/                 kgs/data/interim/            kgs/data/processed/             kgs/data/processed/features/
```

## Component Summary

### Component 1 — Acquire (`kgs_pipeline/acquire.py`)
- **Tasks 01–05**
- Reads lease URLs from `kgs/data/external/oil_leases_2020_present.txt`
- Scrapes each lease page via Playwright async API
- Rate-limited to 5 concurrent requests (`asyncio.Semaphore(5)`)
- Downloads per-lease monthly `.txt` files to `kgs/data/raw/`
- Dask delayed graph with a single `.compute()` call in the orchestrator
- Custom `ScrapingError` exception class
- Idempotent: skips already-downloaded files on re-run

### Component 2 — Ingest (`kgs_pipeline/ingest.py`)
- **Tasks 06–11**
- Reads all `lp*.txt` files from `kgs/data/raw/` via `dask.dataframe.read_csv()`
- Filters out yearly (Month=0) and starting cumulative (Month=-1) records
- Left-joins with lease-level metadata from the archive index
- Adds `source_file` traceability column
- Writes unified interim Parquet to `kgs/data/interim/`, partitioned by `LEASE_KID`
- All functions return lazy Dask DataFrames; computation only in `write_interim_parquet()`

### Component 3 — Transform (`kgs_pipeline/transform.py`)
- **Tasks 12–21**
- Loads interim Parquet, parses `M-YYYY` dates to `datetime64[ns]`
- Renames all columns to `snake_case`, casts to correct dtypes
- Explodes comma-separated `api_number` into one row per well (`well_id`)
- Validates physical bounds (negative production → NaN, oil > 50k BBL → `outlier_flag`)
- Deduplicates on `[well_id, production_date, product]`
- Adds `unit` column (`BBL` for oil, `MCF` for gas)
- Sorts per-well records chronologically; repartitions by `well_id`
- Writes processed Parquet to `kgs/data/processed/`, partitioned by `well_id`
- Zero production preserved as `0.0`; missing data preserved as `NaN`

### Component 4 — Features (`kgs_pipeline/features.py`)
- **Tasks 22–31**
- Loads processed Parquet; engineers per-well features
- Cumulative production (`cumulative_production`) — Np for oil, Gp for gas
- Month-over-month decline rate (`decline_rate`) — clipped to `[-1.0, 10.0]`
- Rolling mean and std dev at 3-month and 6-month windows
- Time features: `months_since_first_prod`, `production_year`, `production_month`
- GOR computation via oil/gas pivot; `water_cut` and `wor` as NaN placeholders
- Label encoding for `county`, `operator`, `producing_zone`, `field_name`, `product`
- Writes ML-ready feature Parquet to `kgs/data/processed/features/`, partitioned by `well_id`

## Test File Index

| Test File | Covers |
|-----------|--------|
| `tests/test_acquire.py` | Tasks 01–05: scraping, URL loading, orchestrator |
| `tests/test_ingest.py` | Tasks 06–11: raw file reading, metadata join, Parquet write |
| `tests/test_transform.py` | Tasks 12–21: cleaning, validation, feature prep, domain checks |
| `tests/test_features.py` | Tasks 22–31: feature engineering, cumulative, decline, encoding |

## Pytest Markers Used

| Marker | Meaning |
|--------|---------|
| `@pytest.mark.unit` | Does not require network access or files on disk in `kgs/data/` |
| `@pytest.mark.integration` | Requires network access or data files in `kgs/data/raw/`, `kgs/data/interim/`, or `kgs/data/processed/` |
| `@pytest.mark.domain` | Domain-specific oil & gas physical law or production behaviour validation |

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Dask for all parallel processing | Enables lazy evaluation, partition-level parallelism, and scalable out-of-core computation across thousands of lease files |
| `asyncio.Semaphore(5)` for scraping | Prevents overloading the KGS server; KGS is a public academic resource with no rate-limit API |
| Partition by `well_id` in processed/features | Ensures all records for a well are co-located for correct per-well cumulative and rolling calculations |
| Zero ≠ NaN distinction | Zeros are valid production measurements; nulls are missing data — ML models must treat them differently |
| `outlier_flag` column instead of dropping | Preserves data for human review and lets ML models learn from flagged data if desired |
| `pyarrow.schema()` at write time | Prevents schema drift across well partitions when one well has all-null columns |
| Global encoding map in `encode_categorical_features()` | Guarantees consistent integer codes across all Dask partitions for ML reproducibility |
| Single `.compute()` per orchestrator | All pipeline functions remain lazy; computation is deferred until the write step, maximising Dask graph optimisation |
