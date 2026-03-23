.PHONY: install test test-unit test-integration lint typecheck clean acquire ingest transform features pipeline

install:
	pip install -e ".[dev]"
	playwright install chromium

test:
	pytest tests -v

test-unit:
	pytest tests -v -m unit

test-integration:
	pytest tests -v -m integration

lint:
	ruff check kgs_pipeline tests

typecheck:
	mypy kgs_pipeline

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache

acquire:
	python -c "from kgs_pipeline.acquire import run_acquire_pipeline; run_acquire_pipeline()"

ingest:
	python -c "from kgs_pipeline.ingest import run_ingest_pipeline; run_ingest_pipeline()"

transform:
	python -c "from kgs_pipeline.transform import run_transform_pipeline; run_transform_pipeline()"

features:
	python -c "from kgs_pipeline.features import run_features_pipeline; run_features_pipeline()"

pipeline:
	$(MAKE) acquire
	$(MAKE) ingest
	$(MAKE) transform
	$(MAKE) features
