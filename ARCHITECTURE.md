# KGS Pipeline - Architecture & Design

## System Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           KGS OIL & GAS PIPELINE                             │
│                         (2020-2025 Data Processing)                          │
└──────────────────────────────────────────────────────────────────────────────┘

INPUT DATA
──────────
  oil_leases_2020_present.txt
  (LEASE_KID, URL, operator, field, location, etc.)
           │
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    COMPONENT 1: ACQUIRE (Web Scraping)                       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ load_lease_urls()                                                       │
│  │  └─ Read lease index CSV                                                │
│  │  └─ Deduplicate on LEASE_KID                                           │
│  │  └─ Filter out null URLs                                               │
│  │  └─ Return: list[{lease_kid, url}]                                     │
│  │                                                                         │
│  ┌─ scrape_lease_page() [ASYNC + CONCURRENCY]                             │
│  │  └─ Playwright browser automation                                       │
│  │  └─ Click "Save Monthly Data to File" button                           │
│  │  └─ Download .txt file                                                 │
│  │  └─ Semaphore rate limiting (5 concurrent by default)                  │
│  │  └─ Return: Path | None                                                │
│  │                                                                         │
│  └─ run_acquire_pipeline() [DASK DELAYED]                                 │
│     └─ Schedule Dask delayed tasks                                         │
│     └─ Parallel execution with asyncio                                    │
│     └─ Return: list[Path] (downloaded files)                              │
│                                                                             │
│  Parallelism: Dask delayed + Asyncio semaphore                             │
│  Error handling: ScrapingError, logs warnings                              │
│  Output: data/raw/lp*.txt (raw production data)                            │
│                                                                             │
└──────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
       RAW DATA
    (lp*.txt files)
           │
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                    COMPONENT 2: INGEST (Read & Merge)                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ read_raw_lease_files()                                                 │
│  │  └─ Glob: raw/lp*.txt                                                  │
│  │  └─ Add source_file column                                             │
│  │  └─ Return: Dask lazy DataFrame                                        │
│  │                                                                         │
│  ┌─ filter_monthly_records()                                              │
│  │  └─ Remove monthly=0 (yearly totals)                                  │
│  │  └─ Remove month=-1 (starting cumulative)                             │
│  │  └─ Return: Dask lazy DataFrame (filtered)                            │
│  │                                                                         │
│  ┌─ read_lease_index()                                                    │
│  │  └─ Read external/oil_leases_2020_present.txt                         │
│  │  └─ Validate required columns                                         │
│  │  └─ Deduplicate by LEASE_KID                                          │
│  │  └─ Return: Dask lazy DataFrame (metadata)                            │
│  │                                                                         │
│  ┌─ merge_with_metadata()                                                 │
│  │  └─ Left join raw ← metadata on LEASE_KID                             │
│  │  └─ Enrich with: operator, field, county, location, etc.             │
│  │  └─ Return: Dask lazy DataFrame (enriched)                            │
│  │                                                                         │
│  └─ write_interim_parquet()                                               │
│     └─ Partition by LEASE_KID                                            │
│     └─ Write Parquet files                                               │
│                                                                             │
│  Parallelism: Lazy Dask evaluation, partitioned I/O                        │
│  Error handling: FileNotFoundError, KeyError                              │
│  Output: data/interim/ (Parquet, 1 partition per lease)                   │
│                                                                             │
└──────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
     INTERIM DATA
  (Parquet, by lease)
           │
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  COMPONENT 3: TRANSFORM (Clean & Standardize)               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ parse_dates()                                                          │
│  │  └─ Convert "M-YYYY" → production_date (timestamp)                     │
│  │                                                                         │
│  ┌─ cast_and_rename_columns()                                             │
│  │  └─ Rename to snake_case                                              │
│  │  └─ Strip whitespace                                                  │
│  │  └─ Cast to correct types (float, int, string)                        │
│  │  └─ Uppercase product codes                                           │
│  │                                                                         │
│  ┌─ explode_api_numbers()                                                 │
│  │  └─ Split "12345, 67890" → individual records                        │
│  │  └─ One row per well (API number)                                     │
│  │  └─ Rename: api_number → well_id                                     │
│  │                                                                         │
│  ┌─ validate_physical_bounds()                                            │
│  │  ├─ Production validation:                                            │
│  │  │  └─ Negative → NaN                                                │
│  │  │  └─ Oil > 50k BBL/month → outlier_flag=True                      │
│  │  ├─ Geographic validation (Kansas):                                   │
│  │  │  └─ Latitude: 36.9° - 40.1°N                                     │
│  │  │  └─ Longitude: -102.1° - -94.5°W                                 │
│  │  ├─ Product validation:                                              │
│  │  │  └─ Keep only: O (oil), G (gas)                                  │
│  │                                                                         │
│  ┌─ deduplicate_records()                                                 │
│  │  └─ Remove duplicate (well_id, production_date, product)             │
│  │  └─ Keep first occurrence                                            │
│  │                                                                         │
│  ┌─ add_unit_column()                                                     │
│  │  └─ O (oil) → BBL (barrels)                                           │
│  │  └─ G (gas) → MCF (thousand cubic feet)                              │
│  │                                                                         │
│  ├─ sort_by_well_and_date()                                              │
│  │  └─ Repartition by well_id                                           │
│  │  └─ Sort chronologically within partition                            │
│  │                                                                         │
│  └─ write_processed_parquet()                                             │
│     └─ Partition by well_id                                             │
│     └─ Explicit PyArrow schema (consistency)                            │
│     └─ Write to data/processed/                                         │
│                                                                             │
│  Parallelism: map_partitions, set_index, lazy evaluation                  │
│  Error handling: KeyError (missing columns), TypeError, OSError           │
│  Output: data/processed/ (Parquet, 1 partition per well)                  │
│                                                                             │
└──────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
    PROCESSED DATA
 (Parquet, by well_id,
  validated & standardized)
           │
           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│              COMPONENT 4: FEATURES (Feature Engineering)                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─ filter_2020_2025()                                                     │
│  │  └─ Temporal filter: 2020-01-01 to 2025-12-31                         │
│  │                                                                         │
│  ┌─ aggregate_by_well_month()                                             │
│  │  └─ Group by: well_id, year_month, product                           │
│  │  └─ Aggregate: sum, mean, std, count (production)                    │
│  │                                                                         │
│  ┌─ rolling_averages(window=12)                                           │
│  │  └─ 12-month rolling average per well                                │
│  │  └─ Column: rolling_avg_12mo                                         │
│  │                                                                         │
│  ┌─ cumulative_production()                                               │
│  │  └─ Cumulative sum per well                                          │
│  │  └─ Column: cumulative_production                                    │
│  │                                                                         │
│  ┌─ production_trend()                                                    │
│  │  └─ Compare first vs last rolling average                            │
│  │  └─ Classification: increasing (>5%) | decreasing (<-5%) | stable   │
│  │  └─ Column: production_trend                                         │
│  │                                                                         │
│  ┌─ compute_well_lifetime()                                               │
│  │  └─ Count months of production per well                             │
│  │  └─ Column: well_lifetime_months                                     │
│  │                                                                         │
│  ├─ standardize_numerics()                                               │
│  │  └─ Z-score normalization: (x - mean) / std                         │
│  │  └─ Creates *_zscore columns for ML models                          │
│  │                                                                         │
│  └─ write_features_parquet()                                              │
│     └─ No partitioning (single table for ML)                            │
│     └─ Write to data/features/                                         │
│                                                                             │
│  Parallelism: groupby + map_partitions, lazy aggregations                │
│  Error handling: KeyError, TypeError, OSError                            │
│  Output: data/features/ (Parquet, ML-ready features)                     │
│                                                                             │
└──────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
   FEATURE DATA (ML-READY)
┌──────────────────────────────────────────────────────────────────────────────┐
│  Columns (Example):                                                          │
│  - well_id                     (string)                                     │
│  - production_date / year_month (timestamp / period)                       │
│  - product                     (O or G)                                    │
│  - production_sum_month        (float, aggregated)                         │
│  - production_mean_month       (float)                                    │
│  - production_std_month        (float)                                    │
│  - rolling_avg_12mo            (float, 12-month rolling)                  │
│  - cumulative_production       (float, running total)                     │
│  - well_lifetime_months        (int, months in data)                      │
│  - production_trend            (string, trend classification)              │
│  - *_zscore                    (float, standardized versions)              │
│  - operator, field_name, county (string, metadata)                        │
│  - latitude, longitude         (float, coordinates)                       │
│  - outlier_flag                (bool)                                     │
│                                                                             │
│  Ready for: ML models, analytics, dashboards, reports                     │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data Transformation Sequence

```
Raw KGS Data (Multiple Records Per Well)
    │
    ├─ ACQUIRE: Web scrape lp*.txt files from KGS
    │ (Async + Concurrent with Playwright)
    │
    ▼
Raw Data (lp_001.txt, lp_002.txt, ...)
    │
    ├─ INGEST: Read files + Merge with metadata
    │ (Lazy Dask eval + Partition by LEASE_KID)
    │
    ▼
Interim Data (LEASE_KID partitions)
    │
    ├─ TRANSFORM: Clean, standardize, explode wells
    │ (Distributed transforms + Repartition by well_id)
    │
    │ Transformations:
    │ 1. Parse dates (M-YYYY → timestamp)
    │ 2. Rename columns (UPPER_CASE → snake_case)
    │ 3. Cast types (string → float, int)
    │ 4. Explode API numbers (1:many → 1:1 well records)
    │ 5. Validate bounds (remove invalid production/location)
    │ 6. Deduplicate (remove well-date-product duplicates)
    │ 7. Add units (BBL for oil, MCF for gas)
    │ 8. Sort chronologically (within well)
    │
    ▼
Processed Data (well_id partitions, validated)
    │
    ├─ FEATURES: Engineer time-series features
    │ (Groupby aggregations + Rolling windows)
    │
    │ Features:
    │ 1. Temporal filter (2020-2025)
    │ 2. Well-month aggregation (sum, mean, std, count)
    │ 3. Rolling averages (12-month window)
    │ 4. Cumulative production (running total)
    │ 5. Production trend (increasing/decreasing/stable)
    │ 6. Well lifetime (months in dataset)
    │ 7. Standardization (z-score normalization)
    │
    ▼
Feature Data (well-month granularity, ML-ready)
    │
    └─ Ready for:
       - Predictive models (decline curve, production forecast)
       - Clustering (operator, field, decline patterns)
       - Anomaly detection (outliers)
       - Analytics & dashboards
```

## Parallelism Strategy

### Acquire Component
```
┌─ Dask Delayed ────────────────────────┐
│                                        │
│  Task 1: Scrape Lease 1  ┐            │
│                           ├─ Async   │
│  Task 2: Scrape Lease 2  ┤ Semaphore │
│                           │ (5 max)   │
│  Task 3: Scrape Lease 3  ┘            │
│                                        │
│  Task N: Scrape Lease N                │
│                                        │
└────────────────────────────────────────┘
      │
      ├─ Compute() triggers:
      │  - Playwright browser pool
      │  - Asyncio event loop
      │  - Semaphore rate limiting
      │
      └─ Returns: [Path, Path, ..., None]
```

### Ingest & Transform Components
```
┌─ Dask Lazy Evaluation ─────────────────────┐
│                                             │
│  Partition 1 ─┐                            │
│               ├─ map_partitions()          │
│  Partition 2 ─┤ (distributed transform)    │
│               │                            │
│  Partition N ─┘                            │
│                                             │
│  No computation until:                     │
│  - .compute()                              │
│  - .to_parquet()                           │
│  - .to_csv()                               │
│                                             │
└─────────────────────────────────────────────┘
      │
      └─ Scales with:
         - Number of cores
         - Available RAM
         - Partition size
```

### Features Component
```
┌─ Grouped Aggregations ──────────────┐
│                                      │
│  Group (Well A, O):  ┐              │
│   └─ sum, mean, std ┤              │
│                     ├─ Parallel    │
│  Group (Well B, G):  ┤ groupby()   │
│   └─ sum, mean, std ├              │
│                     │              │
│  Group (Well N, X):  ┘              │
│                                      │
│  Rolling window (12-month) per group │
│  Cumulative sum per group            │
│                                      │
└──────────────────────────────────────┘
      │
      └─ map_partitions handles all groups
         independently & in parallel
```

## Error Handling & Robustness

### Acquire Errors
```
┌─ ScrapingError
│  └─ "Save Monthly Data to File" button not found
│     → Raise exception, log error, skip lease
│
├─ TimeoutError
│  └─ Page loading exceeded timeout
│     → Log warning, return None, continue
│
├─ FileNotFoundError
│  └─ Input file missing
│     → Raise exception, fail fast
│
└─ Generic Exception
   └─ Browser crash, network error, etc.
      → Log error, return None, continue
```

### Ingest Errors
```
┌─ FileNotFoundError
│  └─ Raw directory or lease index missing
│     → Raise, fail fast
│
├─ KeyError
│  └─ Required columns missing
│     → Raise, fail fast
│
└─ ValueError
   └─ CSV parsing error
      → Raise, fail fast
```

### Transform Errors
```
┌─ KeyError
│  └─ Mandatory columns (production, product, etc.) missing
│     → Raise, fail fast
│
├─ TypeError
│  └─ Input not a Dask DataFrame
│     → Raise, fail fast
│
├─ OSError
│  └─ Cannot write to output directory
│     → Raise, fail fast
│
└─ Data Quality Issues
   └─ Negative production, out-of-bounds coordinates, etc.
      → Log warning, set to NaN, continue
```

### Features Errors
```
┌─ KeyError
│  └─ Required column missing after aggregation
│     → Raise, fail fast
│
└─ OSError
   └─ Cannot write feature Parquet
      → Raise, fail fast
```

## Type System & Validation

```
Input Validation
  ├─ File existence (Path)
  ├─ DataFrame type (Dask or Pandas)
  ├─ Column presence & naming
  └─ Data types (float, int, string, datetime)
        │
        ▼
Processing
  ├─ Type casting (str → float, int)
  ├─ Null/NaN handling
  ├─ Domain validation (bounds, ranges)
  └─ Consistency checks (deduplication)
        │
        ▼
Output Validation
  ├─ Schema consistency (explicit PyArrow schema)
  ├─ Non-null assertions for key columns
  ├─ Parquet write success
  └─ File integrity
```

## Configuration Management

```
kgs_pipeline/config.py
├─ Directory paths (relative to project root)
│  ├─ RAW_DATA_DIR
│  ├─ INTERIM_DATA_DIR
│  ├─ PROCESSED_DATA_DIR
│  └─ FEATURES_DIR
│
├─ KGS URLs
│  ├─ KGS_BASE_URL (lease page)
│  └─ KGS_MONTH_SAVE_URL (data download)
│
├─ Scraping parameters
│  ├─ SCRAPE_CONCURRENCY (5)
│  └─ SCRAPE_TIMEOUT_MS (30000)
│
├─ Production units
│  ├─ OIL_UNIT (BBL)
│  ├─ GAS_UNIT (MCF)
│  └─ WATER_UNIT (BBL)
│
└─ Domain constraints
   └─ MAX_REALISTIC_OIL_BBL_PER_MONTH (50000)
```

## Storage & Partitioning Strategy

### Raw Data
- Format: `.txt` (CSV-like)
- Location: `data/raw/lp*.txt`
- Partitioning: None (one file per scrape job)
- Access: Sequential read

### Interim Data
- Format: Parquet
- Location: `data/interim/LEASE_KID=*/`
- Partitioning: By LEASE_KID
- Rationale: Group all lease production data together
- Access: Query by lease

### Processed Data
- Format: Parquet
- Location: `data/processed/well_id=*/`
- Partitioning: By well_id
- Rationale: One well = one partition, efficient well-level queries
- Access: Query by well, field, operator (via metadata columns)

### Features Data
- Format: Parquet
- Location: `data/features/`
- Partitioning: None (single table, loaded for ML)
- Rationale: Features are aggregated, compact, amenable to full load
- Access: ML pipeline, analytics, dashboards

## Performance Characteristics

```
Component       Time (est.)    Memory (est.)   Parallelism
─────────────────────────────────────────────────────────
Acquire         30-60 min      500 MB         5× async
Ingest          5-15 min       2-4 GB         Dask lazy
Transform       10-30 min      4-8 GB         map_partitions
Features        5-15 min       2-4 GB         groupby + rolling
─────────────────────────────────────────────────────────
Total (end-end) 50-120 min     8-16 GB        Mixed

Notes:
- Times depend on well count, system specs, network latency
- Dask scales horizontally with available cores
- Parquet I/O much faster than CSV
- Lazy evaluation defers computation until write
```

## Integration Points

### Upstream (Inputs)
```
External data sources
├─ KGS web portal (Acquire scrapes)
├─ oil_leases_2020_present.txt (Ingest merges)
└─ KGS data dictionaries (references)
```

### Downstream (Outputs)
```
Consumer systems
├─ ML pipelines (predict decline, production)
├─ Analytics dashboards (Tableau, Power BI)
├─ Reporting systems (monthly/quarterly reports)
├─ Data warehouse (Snowflake, Postgres, etc.)
└─ Jupyter notebooks (exploratory analysis)
```

## Monitoring & Logging

```
Each component logs:
├─ INFO: Start/completion, row counts, timing
├─ WARNING: Non-critical issues, missing data
└─ ERROR: Failures, exceptions, validation errors

Log format: timestamp | level | logger | message

Example:
  2024-01-15 10:23:45 INFO  acquire Starting acquire pipeline
  2024-01-15 10:23:46 INFO  acquire Loaded 1,234 unique leases
  2024-01-15 10:24:15 INFO  acquire Scraping lease L001...
  2024-01-15 10:25:30 WARNING acquire Downloaded timeout for lease L099
  2024-01-15 10:35:00 INFO  acquire Acquire complete: 1,200 successful, 34 failed
```

---

This architecture enables:
✅ Scalability: Parallel processing via Dask & async
✅ Reliability: Comprehensive error handling & logging
✅ Maintainability: Clear separation of concerns
✅ Testability: Each function independently testable
✅ Extensibility: Easy to add new transformations
