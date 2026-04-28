.PHONY: venv install acquire ingest transform features pipeline test lint typecheck

VENV_DIR := .venv

venv:
	python3 -m venv $(VENV_DIR)

install:
	pip install --upgrade pip
	pip install -e .[dev]

acquire:
	kgs-pipeline acquire

ingest:
	kgs-pipeline ingest

transform:
	kgs-pipeline transform

features:
	kgs-pipeline features

# Full pipeline: single entry-point invocation (no chaining of stage targets)
pipeline:
	kgs-pipeline

test:
	pytest tests

lint:
	ruff check kgs_pipeline/ tests/

typecheck:
	mypy kgs_pipeline/
