.PHONY: venv install acquire ingest transform features pipeline

PYTHON := python3
VENV := .venv
PIP := $(VENV)/bin/pip

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]" || $(PIP) install -e .
	$(PIP) install -r requirements.txt

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
