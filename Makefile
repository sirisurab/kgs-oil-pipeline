.PHONY: venv install acquire ingest transform features pipeline test lint typecheck

PYTHON := python3
VENV   := .venv
PIP    := $(VENV)/bin/pip

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e ".[dev]"

acquire:
	$(VENV)/bin/kgs-pipeline --stages acquire

ingest:
	$(VENV)/bin/kgs-pipeline --stages ingest

transform:
	$(VENV)/bin/kgs-pipeline --stages transform

features:
	$(VENV)/bin/kgs-pipeline --stages features

pipeline:
	$(VENV)/bin/kgs-pipeline

test:
	$(VENV)/bin/pytest tests/ -m "not integration" -v

test-all:
	$(VENV)/bin/pytest tests/ -v

lint:
	$(VENV)/bin/ruff check kgs_pipeline/ tests/

typecheck:
	$(VENV)/bin/mypy kgs_pipeline/
