# KGS Oil Production Data Pipeline

A Dask-based pipeline for processing Kansas Geological Survey (KGS) oil and gas production data.

## Stages

1. **acquire** — Download raw lease files from KGS
2. **ingest** — Schema enforcement and interim Parquet output
3. **transform** — Date parsing, categorical casting, deduplication, gap-filling
4. **features** — ML-ready derived features

## Setup

```bash
make venv
source .venv/bin/activate
make install
```

## Usage

```bash
# Run all stages
kgs-pipeline

# Run individual stages
kgs-pipeline acquire
kgs-pipeline ingest
kgs-pipeline transform
kgs-pipeline features

# Or via Make
make pipeline
make acquire
make test
make lint
make typecheck
```

## Configuration

All settings are in `config.yaml`.
