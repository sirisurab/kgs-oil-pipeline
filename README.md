# KGS Oil & Gas Production Data Pipeline

End-to-end pipeline for acquiring, ingesting, transforming, and feature-engineering
Kansas Geological Survey (KGS) oil and gas lease production data (2024 onward).

## Components

| Module | Description |
|--------|-------------|
| `kgs_pipeline/config.py` | Configuration dataclass with env-var overrides |
| `kgs_pipeline/logging_utils.py` | JSON-structured logging factory |
| `kgs_pipeline/acquire.py` | Download KGS lease data files in parallel via Dask |
| `kgs_pipeline/ingest.py` | Load raw files into Dask DataFrames; write interim Parquet |
| `kgs_pipeline/transform.py` | Clean, standardise, pivot, and deduplicate data |
| `kgs_pipeline/features.py` | Feature engineering (GOR, cumulative, decline rate, rolling, lag) |
| `kgs_pipeline/pipeline.py` | Top-level orchestrator CLI |

## Setup

```bash
make env
make install
```

## Run Tests

```bash
make test
```

## Run the Full Pipeline

```bash
kgs-pipeline \
  --index-path data/external/oil_leases_2020_present.txt \
  --raw-dir data/raw \
  --interim-dir data/interim \
  --processed-dir data/processed \
  --features-dir data/features \
  --min-year 2024 \
  --workers 5
```

## Run Individual Stages

```bash
kgs-acquire  --min-year 2024 --workers 5
kgs-ingest   --min-year 2024
kgs-transform
kgs-features
```

## Data Directories

| Path | Description |
|------|-------------|
| `data/external/` | KGS lease index file |
| `data/raw/` | Downloaded raw lease `.txt` files |
| `data/interim/` | Consolidated interim Parquet files |
| `data/processed/` | Cleaned wide-format Parquet files |
| `data/features/` | ML-ready feature Parquet files + `manifest.json` |
