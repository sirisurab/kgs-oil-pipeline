"""Pipeline configuration loader."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


def load_config(path: str | Path = _DEFAULT_CONFIG_PATH) -> dict:
    """Load pipeline configuration from a YAML file.

    Args:
        path: Path to config.yaml. Defaults to repo-root config.yaml.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open() as fh:
        cfg: dict = yaml.safe_load(fh)
    logger.debug("Loaded config from %s", config_path)
    return cfg
