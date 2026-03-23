# KGS Oil and Gas Production Pipeline

A Python pipeline for downloading, ingesting, cleaning, and feature-engineering
Kansas Geological Survey (KGS) per-lease monthly oil and gas production data.

## Pipeline Stages

```
acquire.py → ingest.py → transform.py → features.py
data/raw/   data/interim/  data/processed/  data/processed/features/
```

| Stage | Module | Description |
|-------|--------|-------------|
| Acquire | `kgs_pipeline/acquire.py` | Playwright async scraper with rate-limiting; downloads per-lease `.txt` files |
| Ingest | `kgs_pipeline/ingest.py` | Dask-based ingestion, metadata enrichment, monthly-record filtering, Parquet write |
| Transform | `kgs_pipeline/transform.py` | Column standardisation, date parsing, API explosion, bounds validation, deduplication |
| Features | `kgs_pipeline/features.py` | Cumulative production, decline rate, rolling stats, GOR, label encoding |

## Quick Start

```bash
# Install dependencies
pip install -e ".[dev]"
playwright install chromium

# Run unit tests only (no network or data required)
make test-unit

# Run full pipeline
make pipeline
```

## Directory Structure

```
kgs_pipeline/
├── __init__.py
├── config.py       # All configuration constants
├── acquire.py      # Stage 1: Download raw .txt files from KGS
├── ingest.py       # Stage 2: Ingest to interim Parquet
├── transform.py    # Stage 3: Clean and restructure to well-level
└── features.py     # Stage 4: ML-ready feature engineering

data/
├── raw/            # Downloaded raw per-lease .txt files
├── interim/        # Parquet partitioned by LEASE_KID
├── processed/      # Parquet partitioned by well_id
└── processed/features/  # ML-ready feature Parquet

references/
├── oil_leases_2020_present.txt      # Lease index with URLs
├── kgs_archives_data_dictionary.csv
└── kgs_monthly_data_dictionary.csv

tests/
├── test_acquire.py
├── test_ingest.py
├── test_transform.py
└── test_features.py
```

## Configuration

All configuration is centralised in `kgs_pipeline/config.py`:

- `RAW_DATA_DIR`, `INTERIM_DATA_DIR`, `PROCESSED_DATA_DIR`, `FEATURES_DATA_DIR`
- `LEASE_INDEX_FILE`: path to the KGS lease archive index
- `KGS_BASE_URL`, `KGS_MONTH_SAVE_URL`: KGS portal URLs
- `MAX_CONCURRENT_REQUESTS`: Playwright concurrency limit (default 5)
- `OIL_OUTLIER_THRESHOLD_BBL`: Physical bounds threshold (50,000 BBL/month)
- `DECLINE_RATE_CLIP_MIN` / `DECLINE_RATE_CLIP_MAX`: Decline rate bounds

## Test Markers

- `@pytest.mark.unit` — No network or data files required
- `@pytest.mark.integration` — Requires real data in `data/raw/`, `data/interim/`, etc.

By default `pytest` runs only unit tests. Use `make test-integration` to run integration tests.
