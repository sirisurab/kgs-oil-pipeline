.PHONY: env install test lint typecheck

env:
	python3 -m venv .venv

install:
	.venv/bin/pip install --upgrade pip setuptools wheel
	.venv/bin/pip install -e ".[dev]"

test:
	pytest tests/ -v

lint:
	ruff check kgs_pipeline/ tests/

typecheck:
	mypy kgs_pipeline/
