"""Shared pytest fixtures for kgs_pipeline tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def sample_production_df() -> pd.DataFrame:
    """Minimal production DataFrame matching the ingest schema."""
    return pd.DataFrame({
        "LEASE_KID": pd.array([1001, 1001, 1002], dtype="int64"),
        "LEASE": pd.array(["Lease A", "Lease A", "Lease B"], dtype=pd.StringDtype()),
        "DOR_CODE": pd.array([100, 100, 200], dtype=pd.Int64Dtype()),
        "API_NUMBER": pd.array(["15-001", "15-001", "15-002"], dtype=pd.StringDtype()),
        "FIELD": pd.array(["Field1", "Field1", "Field2"], dtype=pd.StringDtype()),
        "PRODUCING_ZONE": pd.array(["Lansing", "Lansing", "Herington"], dtype=pd.StringDtype()),
        "OPERATOR": pd.array(["Op A", "Op A", "Op B"], dtype=pd.StringDtype()),
        "COUNTY": pd.array(["Douglas", "Douglas", "Ellis"], dtype=pd.StringDtype()),
        "TOWNSHIP": pd.array([10, 10, 15], dtype=pd.Int64Dtype()),
        "TWN_DIR": pd.array(["S", "S", "S"], dtype=pd.StringDtype()),
        "RANGE": pd.array([20, 20, 25], dtype=pd.Int64Dtype()),
        "RANGE_DIR": pd.array(["W", "W", "W"], dtype=pd.StringDtype()),
        "SECTION": pd.array([5, 5, 12], dtype=pd.Int64Dtype()),
        "SPOT": pd.array(["NE", "NE", "SW"], dtype=pd.StringDtype()),
        "LATITUDE": pd.array([38.5, 38.5, 39.0], dtype=pd.Float64Dtype()),
        "LONGITUDE": pd.array([-95.5, -95.5, -99.0], dtype=pd.Float64Dtype()),
        "MONTH-YEAR": pd.array(["1-2024", "2-2024", "1-2024"], dtype=pd.StringDtype()),
        "PRODUCT": pd.array(["O", "O", "G"], dtype=pd.StringDtype()),
        "WELLS": pd.array([1, 1, 2], dtype=pd.Int64Dtype()),
        "PRODUCTION": pd.array([100.0, 200.0, 5000.0], dtype=pd.Float64Dtype()),
        "source_file": pd.array(["lp1.txt", "lp1.txt", "lp2.txt"], dtype=pd.StringDtype()),
    })


@pytest.fixture
def base_config() -> dict:
    """Base pipeline configuration for tests."""
    return {
        "acquire": {
            "lease_index_path": "data/external/oil_leases_2020_present.txt",
            "raw_output_dir": "data/raw",
            "month_save_url_template": (
                "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc={lease_id}"
            ),
            "min_year": 2024,
            "max_workers": 5,
            "worker_sleep_seconds": 0.0,
            "request_timeout_seconds": 30,
            "max_retries": 3,
            "retry_backoff_factor": 1.0,
        },
        "ingest": {
            "raw_input_dir": "data/raw",
            "interim_output_dir": "data/interim",
            "file_glob": "*.txt",
            "min_year": 2024,
        },
        "transform": {
            "interim_input_dir": "data/interim",
            "processed_output_dir": "data/processed",
            "max_oil_bbl_per_month": 50000.0,
            "deduplicate_subset": ["LEASE_KID", "MONTH-YEAR", "PRODUCT"],
        },
        "features": {
            "processed_input_dir": "data/processed",
            "features_output_dir": "data/features",
            "rolling_windows": [3, 6],
            "decline_rate_clip_min": -1.0,
            "decline_rate_clip_max": 10.0,
            "oil_outlier_threshold_bbl": 50000.0,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
        },
        "logging": {"log_file": "logs/pipeline.log", "level": "INFO"},
    }
