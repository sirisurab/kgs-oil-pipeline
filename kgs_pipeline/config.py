"""Pipeline configuration module using Pydantic v2 BaseSettings.

Provides a validated, type-safe configuration singleton for the KGS oil production
data pipeline. All pipeline modules import CONFIG from this module.
"""

from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings


class PipelineConfig(BaseSettings):
    """All configurable constants for the KGS data pipeline."""

    # Data directories
    raw_dir: Path = Path("data/raw")
    interim_dir: Path = Path("data/interim")
    processed_dir: Path = Path("data/processed")
    features_dir: Path = Path("data/processed/features")
    external_dir: Path = Path("data/external")
    logs_dir: Path = Path("logs")

    # Source files
    lease_index_file: Path = Path("data/external/oil_leases_2020_present.txt")

    # Year range
    year_start: int = 2020
    year_end: int = 2025

    # KGS web portal URLs
    kgs_base_url: str = "https://chasm.kgs.ku.edu/ords"
    kgs_main_lease_path: str = "oil.ogl5.MainLease"
    kgs_month_save_path: str = "oil.ogl5.MonthSave"

    # Concurrency
    max_concurrent_requests: int = 5
    scrape_timeout_ms: int = 30000
    http_retry_attempts: int = 3
    http_retry_backoff_s: float = 2.0

    # Production units
    oil_unit: str = "BBL"
    gas_unit: str = "MCF"
    water_unit: str = "BBL"

    # Outlier thresholds
    oil_max_bbl_per_month: float = 50000.0

    # Feature engineering
    rolling_windows: list[int] = [3, 6, 12]
    decline_rate_clip_min: float = -1.0
    decline_rate_clip_max: float = 10.0

    # Dask
    dask_n_workers: int = 4
    parquet_engine: str = "pyarrow"

    # Logging
    log_level: str = "INFO"

    @field_validator("year_start")
    @classmethod
    def validate_year_start(cls, v: int) -> int:
        if v < 2000:
            raise ValueError("year_start must be >= 2000")
        return v

    @field_validator("year_end")
    @classmethod
    def validate_year_end(cls, v: int) -> int:
        if v > 2030:
            raise ValueError("year_end must be <= 2030")
        return v

    @model_validator(mode="after")
    def validate_year_range(self) -> "PipelineConfig":
        if self.year_start > self.year_end:
            raise ValueError("year_start must be <= year_end")
        return self

    @field_validator("max_concurrent_requests")
    @classmethod
    def validate_max_concurrent_requests(cls, v: int) -> int:
        if not 1 <= v <= 20:
            raise ValueError("max_concurrent_requests must be between 1 and 20")
        return v

    @field_validator("oil_max_bbl_per_month")
    @classmethod
    def validate_oil_max_bbl(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("oil_max_bbl_per_month must be > 0")
        return v

    model_config = {"env_prefix": "KGS_", "extra": "ignore"}


CONFIG = PipelineConfig()
