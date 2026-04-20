# KGS Oil & Gas Production Data Pipeline

A four-stage data pipeline that acquires, ingests, transforms, and engineers features from
Kansas Geological Survey (KGS) lease production data.

## Stages

| Stage | Module | Description |
|-------|--------|-------------|
| acquire | `kgs_pipeline/acquire.py` | Download raw KGS lease production files |
| ingest | `kgs_pipeline/ingest.py` | Enforce canonical schema, write interim Parquet |
| transform | `kgs_pipeline/transform.py` | Parse dates, clean, index by entity, sort by date |
| features | `kgs_pipeline/features.py` | Compute ML-ready derived features |

## Setup

```bash
make install
```

## Run

```bash
# Full pipeline
make pipeline

# Individual stages
make acquire
make ingest
make transform
make features

# Or via the CLI
kgs-pipeline --stages acquire ingest transform features
kgs-pipeline --config path/to/config.yaml
```

## Test

```bash
make test
# or
pytest tests/ -v
```

## Configuration

All settings are read from `config.yaml`. See `config.yaml` for the full schema.

## Data directories

| Path | Contents |
|------|----------|
| `data/external/` | Lease index file |
| `data/raw/` | Downloaded raw `.txt` lease files |
| `data/interim/` | Schema-enforced Parquet (after ingest) |
| `data/features/` | Cleaned, entity-indexed Parquet (after transform) |
| `data/processed/` | ML-ready Parquet with all feature columns |
