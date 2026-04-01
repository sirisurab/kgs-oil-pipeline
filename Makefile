.PHONY: env install acquire ingest transform features test lint typecheck

env:
	python3 -m venv .venv

install:
	.venv/bin/pip install --upgrade pip setuptools wheel
	.venv/bin/pip install -e ".[dev]"

acquire:
	python -m kgs_pipeline.acquire --index data/external/oil_leases_2020_present.txt --output-dir data/raw/

ingest:
	python -m kgs_pipeline.ingest --raw-dir data/raw/ --output-dir data/interim/

transform:
	python -m kgs_pipeline.transform --interim-dir data/interim/ --output-dir data/processed/clean/ --raw-dir data/raw/

features:
	python -m kgs_pipeline.features --clean-dir data/processed/clean/ --output-dir data/processed/features/

test:
	pytest tests/ -v

lint:
	ruff check kgs_pipeline/ tests/

typecheck:
	mypy kgs_pipeline/
