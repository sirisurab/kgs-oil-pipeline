.PHONY: install test test-integration lint type-check clean acquire ingest transform features pipeline

install:
	pip install -e ".[dev]"
	playwright install chromium

test:
	pytest tests/ -m "not integration" -v

test-integration:
	pytest tests/ -m "integration" -v

lint:
	ruff check kgs_pipeline/ tests/

type-check:
	mypy kgs_pipeline/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -rf .pytest_cache .mypy_cache .ruff_cache

acquire:
	python -m kgs_pipeline.pipeline --stages acquire

ingest:
	python -m kgs_pipeline.pipeline --stages ingest

transform:
	python -m kgs_pipeline.pipeline --stages transform

features:
	python -m kgs_pipeline.pipeline --stages features

pipeline:
	python -m kgs_pipeline.pipeline --stages all
