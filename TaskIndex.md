# Task Index

| File | Description |
|------|-------------|
| tasks/acquire_tasks.md  | Lease index loading, URL scraping, parallel file downloading, idempotency, project scaffolding (pyproject.toml, Makefile, .gitignore) |
| tasks/ingest_tasks.md   | Raw file reading, schema validation, type coercion, date-range filtering, parallel Dask-based consolidation to interim Parquet |
| tasks/transform_tasks.md | Null imputation, deduplication, IQR outlier capping, string standardisation, production_date derivation, well-completeness diagnostics, cleaned Parquet output |
| tasks/features_tasks.md | Product pivot, cumulative production, GOR/water-cut, decline rate, well age, rolling averages, lag features, county/formation aggregates, label encoding, ML-ready Parquet output |
