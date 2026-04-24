# KGS Oil Production Data Pipeline

A parallel data pipeline for KGS (Kansas Geological Survey) oil and gas lease
production data. Downloads, ingests, cleans, and feature-engineers production
records for ML/analytics workflows.

## Pipeline Stages

| Stage     | Description                                              |
|-----------|----------------------------------------------------------|
| acquire   | Downloads per-lease production files from KGS portal     |
| ingest    | Parses raw files, enforces canonical schema, writes Parquet |
| transform | Cleans data, derives production_date, entity-indexes     |
| features  | Computes rolling averages, cumulative, GOR, decline rate |

## Quick Start

```bash
# Create virtual environment and install dependencies
make venv
make install

# Run the full pipeline
make pipeline

# Run individual stages
make acquire
make ingest
make transform
make features
```

## Configuration

Edit `config.yaml` to configure all pipeline settings including data paths,
Dask cluster parameters, retry logic, and logging.

## Testing

```bash
# Unit tests only (no network required)
make test

# Integration tests (requires data files)
make test-integration

# All tests
make test-all
```

## Project Structure

```
kgs_pipeline/
├── __init__.py
├── acquire.py    # Task A1-A4: Download manifest, URL resolution, file download
├── ingest.py     # Task I1-I3: Schema loading, raw file parsing, Parquet write
├── transform.py  # Task T1-T3: Cleaning, production_date derivation, entity index
├── features.py   # Task F1-F5: Product reshape, cumulative, ratios, rolling features
└── pipeline.py   # Task P1-P6: Config loader, logging, Dask init, CLI entry point
```

## Data Sources

- Lease index: `data/external/oil_leases_2020_present.txt`
- Raw files: `data/raw/lp*.txt`
- Interim Parquet: `data/interim/`
- Processed Parquet: `data/processed/transform/`
- Feature Parquet: `data/processed/features/`
