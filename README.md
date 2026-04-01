# KGS Oil & Gas Production Pipeline

A Python data pipeline for acquiring, ingesting, transforming, and feature-engineering
Kansas Geological Survey (KGS) monthly lease-level oil and gas production data.

## Pipeline Stages

| Stage | Module | Description |
|-------|--------|-------------|
| Acquire | `kgs_pipeline/acquire.py` | Downloads per-lease production files from KGS CHASM server |
| Ingest | `kgs_pipeline/ingest.py` | Reads raw files, validates schema, writes interim Parquet |
| Transform | `kgs_pipeline/transform.py` | Cleans data: nulls, outliers, dedup, string standardisation |
| Features | `kgs_pipeline/features.py` | Engineers ML features: cumulative, GOR, rolling, lags, encoding |

## Setup

```bash
make env
make install
```

## Running the Pipeline

```bash
make acquire      # Download raw lease files from KGS
make ingest       # Parse raw files → interim Parquet
make transform    # Clean interim data → processed/clean Parquet
make features     # Feature engineering → processed/features Parquet
```

## Testing

```bash
make test         # Run unit tests (default: excludes integration tests)
pytest tests/ -v -m "unit"
pytest tests/ -v -m "integration"  # requires real data
```

## Code Quality

```bash
make lint         # ruff check
make typecheck    # mypy
```

## Data Directories

```
data/
├── external/     # Pre-filtered lease index (oil_leases_2024_present.txt)
├── raw/          # Downloaded raw .txt files per lease
├── interim/      # Consolidated Parquet after ingestion
└── processed/
    ├── clean/    # Cleaned Parquet after transform
    └── features/ # ML-ready Parquet after feature engineering
```

## Key Technical Details

- **Python 3.11+** required
- **Dask** for parallel processing and Parquet I/O
- **Requests + BeautifulSoup** for KGS web scraping
- Retry decorator with exponential backoff wraps all HTTP calls
- Idempotent: re-running any stage skips already-completed work
- All unit tests are self-contained with synthetic data (no network required)
