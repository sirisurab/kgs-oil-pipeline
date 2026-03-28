# KGS Oil Production Data Pipeline

A pipeline for acquiring, ingesting, transforming, and engineering ML-ready features from
Kansas Geological Survey (KGS) lease-level monthly oil and gas production data.

## Pipeline Stages

```
acquire → ingest → transform → features
data/raw/  data/interim/  data/processed/  data/processed/features/
```

| Stage | Module | Description |
|-------|--------|-------------|
| acquire | `kgs_pipeline/acquire.py` | Scrapes per-lease .txt files from KGS via BeautifulSoup |
| ingest | `kgs_pipeline/ingest.py` | Reads raw .txt files into partitioned interim Parquet |
| transform | `kgs_pipeline/transform.py` | Cleans, validates, deduplicates, writes processed Parquet |
| features | `kgs_pipeline/features.py` | Engineers ML features, writes feature Parquet + CSV |

## Setup

```bash
make env
source .venv/bin/activate
make install
```

## Usage

Run all stages:
```bash
python -m kgs_pipeline.pipeline --stages all
```

Run specific stages:
```bash
python -m kgs_pipeline.pipeline --stages acquire,ingest
python -m kgs_pipeline.pipeline --stages transform,features
```

Options:
- `--stages`: Comma-separated list of stages (acquire, ingest, transform, features, all)
- `--years`: Comma-separated list of years to process (default: 2020–2025)
- `--workers`: Number of Dask workers (default: 4)

## Testing

```bash
# Unit tests only (no network required)
pytest tests/ -m "not integration"

# All tests including integration
pytest tests/ -m "integration"
```

## Directory Structure

```
data/
├── external/        # Lease index file (oil_leases_2020_present.txt)
├── raw/             # Downloaded per-lease .txt files
├── interim/         # Interim Parquet (partitioned by LEASE_KID)
├── processed/       # Cleaned Parquet (partitioned by well_id) + cleaning_report.json
└── processed/features/  # ML feature Parquet + feature_matrix.csv
logs/                # Rotating log files per component
references/          # Data dictionaries
```

## Configuration

All parameters are configurable via `kgs_pipeline/config.py` (Pydantic BaseSettings).
Override with environment variables prefixed with `KGS_`, e.g.:

```bash
export KGS_DASK_N_WORKERS=8
export KGS_OIL_MAX_BBL_PER_MONTH=100000
```
