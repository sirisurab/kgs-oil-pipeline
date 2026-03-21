.PHONY: help clean install lint type test run-acquire run-ingest run-transform run-features

help:
	@echo "KGS Oil & Gas Pipeline - Available targets:"
	@echo ""
	@echo "  install          Install dependencies"
	@echo "  lint             Run ruff linter"
	@echo "  type             Run mypy type checker"
	@echo "  test             Run pytest test suite"
	@echo "  clean            Remove temporary files and caches"
	@echo "  run-acquire      Run acquire pipeline (web scrape)"
	@echo "  run-ingest       Run ingest pipeline (read raw data)"
	@echo "  run-transform    Run transform pipeline (clean and process)"
	@echo "  run-features     Run features pipeline (engineer features)"
	@echo "  run-all          Run full pipeline (acquire → ingest → transform → features)"

install:
	pip install --upgrade pip
	pip install -r requirements.txt
	python -m mypy --install-types

lint:
	ruff check kgs_pipeline tests --fix

type:
	mypy kgs_pipeline --config-file mypy.ini

test:
	pytest tests -v --cov=kgs_pipeline --cov-report=html

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name '*.pyc' -delete
	rm -rf .pytest_cache .coverage htmlcov .mypy_cache .ruff_cache
	rm -rf dask-worker-space

run-acquire:
	python -c "from kgs_pipeline.acquire import run_acquire_pipeline; run_acquire_pipeline()"

run-ingest:
	python -c "from kgs_pipeline.ingest import run_ingest_pipeline; run_ingest_pipeline()"

run-transform:
	python -c "from kgs_pipeline.transform import run_transform_pipeline; run_transform_pipeline()"

run-features:
	python -c "from kgs_pipeline.features import run_features_pipeline; run_features_pipeline()"

run-all: run-acquire run-ingest run-transform run-features
	@echo "Full pipeline complete!"
