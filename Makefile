.PHONY: env install acquire ingest transform features pipeline

env:
	python3 -m venv .venv

install:
	pip install --upgrade pip
	pip install -e ".[dev]"

acquire:
	kgs-pipeline --stages acquire

ingest:
	kgs-pipeline --stages ingest

transform:
	kgs-pipeline --stages transform

features:
	kgs-pipeline --stages features

pipeline: acquire ingest transform features
	kgs-pipeline --stages acquire ingest transform features
