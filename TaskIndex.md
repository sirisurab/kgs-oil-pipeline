# Task Index

KGS Oil & Gas Production Data Pipeline — 2024-2025
Task specification files for the downstream coding agent, listed in pipeline execution order.

| File | Description |
|------|-------------|
| `tasks/acquire_tasks.md`   | Data acquisition: lease index loading, HTTP download workflow, parallel Dask-based fetching of KGS lease production files, idempotency and integrity tests |
| `tasks/ingest_tasks.md`    | Data ingestion: raw file discovery, schema-aware CSV reader, year-range filtering, Dask DataFrame assembly, consolidated interim Parquet output |
| `tasks/transform_tasks.md` | Data cleaning and transformation: column standardisation, type casting, deduplication, oil/gas pivot to wide format, outlier flagging, sorted processed Parquet output |
| `tasks/features_tasks.md`  | Feature engineering: GOR, water cut, cumulative production, decline rate, rolling averages, lag features, production-rate features, metadata manifest, pipeline orchestrator CLI |
