"""Tests for the configuration module (kgs_pipeline/config.py)."""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path

import pytest

from kgs_pipeline.config import ConfigError, PipelineConfig, PathsConfig, load_config


MINIMAL_CONFIG_YAML = """\
paths:
  lease_file: references/oil_leases_2020_present.txt
  raw_data_dir: data/raw
  interim_dir: data/interim
  processed_dir: data/processed
  encoders_dir: data/processed/encoders
  logs_dir: logs

scraping:
  max_concurrency: 5
  retry_attempts: 3
  retry_backoff_base: 2
  page_timeout_seconds: 30
  button_timeout_seconds: 10

ingestion:
  file_glob: "lease_*.txt"
  interim_filename: raw_combined.parquet

transform:
  dedup_keys:
    - lease_kid
    - production_date
    - product
    - production
  oil_outlier_threshold_bbl: 50000
  invalid_product_codes:
    - O
    - G

features:
  rolling_windows:
    - 3
    - 6
  gor_high_threshold: 100000
  categorical_columns:
    - county
    - operator
    - producing_zone
"""


@pytest.mark.unit
def test_pipeline_config_is_dataclass():
    """PipelineConfig is a Python dataclass."""
    assert dataclasses.is_dataclass(PipelineConfig)


@pytest.mark.unit
def test_paths_config_fields_are_paths(tmp_path: Path):
    """All PathsConfig fields are pathlib.Path instances."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(MINIMAL_CONFIG_YAML, encoding="utf-8")
    config = load_config(str(cfg_file))
    paths = config.paths
    for field_name in dataclasses.fields(paths):
        val = getattr(paths, field_name.name)
        assert isinstance(val, Path), f"{field_name.name} should be Path, got {type(val)}"


@pytest.mark.unit
def test_load_config_returns_pipeline_config(tmp_path: Path):
    """load_config returns PipelineConfig with all fields populated."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(MINIMAL_CONFIG_YAML, encoding="utf-8")
    config = load_config(str(cfg_file))
    assert isinstance(config, PipelineConfig)
    assert config.scraping.max_concurrency == 5


@pytest.mark.unit
def test_load_config_paths_are_absolute(tmp_path: Path):
    """Resolved path fields are absolute (start with /)."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(MINIMAL_CONFIG_YAML, encoding="utf-8")
    config = load_config(str(cfg_file))
    assert config.paths.raw_data_dir.is_absolute()


@pytest.mark.unit
def test_load_config_env_var_override(tmp_path: Path, monkeypatch):
    """KGS_RAW_DATA_DIR env var overrides paths.raw_data_dir."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(MINIMAL_CONFIG_YAML, encoding="utf-8")
    monkeypatch.setenv("KGS_RAW_DATA_DIR", "/override/raw")
    config = load_config(str(cfg_file))
    assert config.paths.raw_data_dir == Path("/override/raw")


@pytest.mark.unit
def test_load_config_missing_required_field_raises(tmp_path: Path):
    """ConfigError raised when a required path field is missing."""
    bad_yaml = """\
paths:
  lease_file: references/leases.txt
  raw_data_dir: data/raw
  interim_dir: data/interim
  processed_dir: data/processed
  encoders_dir: data/processed/encoders
  # logs_dir is missing

scraping:
  max_concurrency: 5
  retry_attempts: 3
  retry_backoff_base: 2
  page_timeout_seconds: 30
  button_timeout_seconds: 10

ingestion:
  file_glob: "lease_*.txt"
  interim_filename: raw_combined.parquet

transform:
  dedup_keys: [lease_kid]
  oil_outlier_threshold_bbl: 50000
  invalid_product_codes: [O, G]

features:
  rolling_windows: [3, 6]
  gor_high_threshold: 100000
  categorical_columns: [county]
"""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(bad_yaml, encoding="utf-8")
    with pytest.raises(ConfigError, match="logs_dir"):
        load_config(str(cfg_file))


@pytest.mark.unit
def test_load_config_invalid_yaml_raises(tmp_path: Path):
    """ConfigError raised for YAML with invalid syntax."""
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("paths: {unclosed: bracket\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(str(cfg_file))


@pytest.mark.unit
def test_load_config_max_concurrency_zero_raises(tmp_path: Path):
    """ConfigError raised when scraping.max_concurrency < 1."""
    bad_yaml = MINIMAL_CONFIG_YAML.replace("max_concurrency: 5", "max_concurrency: 0")
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(bad_yaml, encoding="utf-8")
    with pytest.raises(ConfigError, match="max_concurrency"):
        load_config(str(cfg_file))


@pytest.mark.unit
def test_load_config_file_not_found_raises():
    """FileNotFoundError raised for non-existent config path."""
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yaml")


@pytest.mark.integration
def test_load_config_project_root():
    """Integration: Load actual config.yaml from project root."""
    config = load_config()
    assert isinstance(config, PipelineConfig)
    for field in dataclasses.fields(config.paths):
        val = getattr(config.paths, field.name)
        assert isinstance(val, Path), f"{field.name} should be Path"
