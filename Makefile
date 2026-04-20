.PHONY: venv install acquire ingest transform features pipeline test clean

PYTHON := python3
VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
KGS := $(VENV)/bin/kgs-pipeline

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]" || $(PIP) install -r requirements.txt
	$(PIP) install -e .

acquire:
	$(KGS) --stages acquire

ingest:
	$(KGS) --stages ingest

transform:
	$(KGS) --stages transform

features:
	$(KGS) --stages features

pipeline:
	$(KGS)

test:
	$(PYTEST) tests/ -v

clean:
	rm -rf data/raw/*.txt data/interim/ data/features/ data/processed/
