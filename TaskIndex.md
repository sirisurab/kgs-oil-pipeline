# Task Index

- `tasks/acquire_tasks.md`  <- all acquire stage tasks: lease index loading, download workflow, parallel Dask-threaded file acquisition, idempotency/integrity validation, build environment setup
- `tasks/ingest_tasks.md`   <- all ingest stage tasks: schema loading, dtype resolution, single-file reading, Dask-distributed parallel ingestion, Parquet writing, schema completeness tests
- `tasks/transform_tasks.md` <- all transform stage tasks: production_date derivation, physical bounds validation, deduplication, categorical casting, well completeness, entity indexing and sort, Parquet writing, meta schema and integrity tests
- `tasks/features_tasks.md` <- all features stage tasks: cumulative production, GOR, decline rate, rolling averages, lag features, temporal features, label encoding, feature manifest, Parquet writing, pipeline entry point, domain and meta schema correctness tests
