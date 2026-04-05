"""Pipeline configuration — all tuneable constants with env-var overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Config:
    LEASE_INDEX_PATH: str = field(
        default_factory=lambda: os.environ.get(
            "LEASE_INDEX_PATH", "data/external/oil_leases_2020_present.txt"
        )
    )
    RAW_DATA_DIR: str = field(
        default_factory=lambda: os.environ.get("RAW_DATA_DIR", "data/raw")
    )
    INTERIM_DATA_DIR: str = field(
        default_factory=lambda: os.environ.get("INTERIM_DATA_DIR", "data/interim")
    )
    PROCESSED_DATA_DIR: str = field(
        default_factory=lambda: os.environ.get("PROCESSED_DATA_DIR", "data/processed")
    )
    FEATURES_DATA_DIR: str = field(
        default_factory=lambda: os.environ.get("FEATURES_DATA_DIR", "data/features")
    )
    MAX_WORKERS: int = field(
        default_factory=lambda: int(os.environ.get("MAX_WORKERS", "5"))
    )
    SLEEP_SECONDS: float = field(
        default_factory=lambda: float(os.environ.get("SLEEP_SECONDS", "0.5"))
    )
    MIN_YEAR: int = field(
        default_factory=lambda: int(os.environ.get("MIN_YEAR", "2024"))
    )
    MONTH_SAVE_URL_TEMPLATE: str = (
        "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"
    )
    MAIN_LEASE_URL_PREFIX: str = (
        "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc="
    )


# Module-level singleton used by default throughout the pipeline
config = Config()
