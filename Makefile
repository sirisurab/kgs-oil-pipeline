.PHONY: help install test lint clean

help:
	@echo "Available commands:"
	@echo "  make install     - Install dependencies from requirements.txt"
	@echo "  make test        - Run pytest test suite"
	@echo "  make lint        - Run ruff and mypy linters"
	@echo "  make clean       - Remove cache and generated files"
	@echo "  make acquire     - Run the acquisition pipeline"
	@echo "  make ingest      - Run the ingest pipeline"
	@echo "  make transform   - Run the transform pipeline"
	@echo "  make features    - Run the features pipeline"

install:
	pip install -r requirements.txt
	playwright install

test:
	pytest tests/ -v

lint:
	ruff check kgs_pipeline tests
	mypy kgs_pipeline --ignore-missing-imports

clean:
	rm -rf __pycache__ .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf dask-worker-space

.env:
	@echo "Creating .env file (add your settings here)"
	touch .env
