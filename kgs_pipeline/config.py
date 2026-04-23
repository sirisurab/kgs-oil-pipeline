"""Configuration loading for the KGS pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_REQUIRED_SECTIONS = {"acquire", "ingest", "transform", "features", "dask", "logging"}


def load_config(config_path: str | Path) -> dict[str, Any]:
    """Read config.yaml and return its contents as a plain dict.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Configuration dict with all top-level sections.

    Raises:
        FileNotFoundError: If config_path does not exist.
        ValueError: If the YAML is malformed or a required section is missing.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    try:
        with path.open("r", encoding="utf-8") as fh:
            config: dict[str, Any] = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"Malformed YAML in {path}: {exc}") from exc

    if not isinstance(config, dict):
        raise ValueError(f"Config file {path} must be a YAML mapping at the top level")

    for section in _REQUIRED_SECTIONS:
        if section not in config:
            raise ValueError(f"Required section '{section}' is missing from {path}")

    return config
