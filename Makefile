.PHONY: venv install acquire ingest transform features pipeline test lint typecheck clean

PYTHON := python3
VENV   := .venv
PIP    := $(VENV)/bin/pip

# Environment setup targets per build-env-manifest.md
venv:
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/python -m pip install --upgrade pip

install: venv
	$(PIP) install -e ".[dev]"

# Individual stage targets — each runnable independently
acquire:
	$(VENV)/bin/kgs-pipeline --stages acquire

ingest:
	$(VENV)/bin/kgs-pipeline --stages ingest

transform:
	$(VENV)/bin/kgs-pipeline --stages transform

features:
	$(VENV)/bin/kgs-pipeline --stages features

# Full pipeline target: single invocation approach — invoke entry point once for all stages.
# Per build-env-manifest.md: never both chain stage targets as deps AND invoke entry point
# in the recipe body. This target uses the entry-point-invocation approach only.
pipeline:
	$(VENV)/bin/kgs-pipeline

# Test targets
test:
	$(VENV)/bin/pytest tests/ -m "unit" -v

test-integration:
	$(VENV)/bin/pytest tests/ -m "integration" -v

test-all:
	$(VENV)/bin/pytest tests/ -v

lint:
	$(VENV)/bin/ruff check kgs_pipeline/ tests/

typecheck:
	$(VENV)/bin/mypy kgs_pipeline/

clean:
	rm -rf $(VENV) __pycache__ kgs_pipeline/__pycache__ tests/__pycache__ .mypy_cache .ruff_cache .pytest_cache
