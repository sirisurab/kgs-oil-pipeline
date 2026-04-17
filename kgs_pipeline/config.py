"""Configuration loader for the KGS pipeline."""

from __future__ import annotations

from pathlib import Path

import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    """Load and return the pipeline configuration from a YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
