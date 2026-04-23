"""Shared pytest fixtures for the KGS pipeline test suite."""

from __future__ import annotations

import io
import textwrap
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helper: build a canonical raw CSV string
# ---------------------------------------------------------------------------

_CANONICAL_COLUMNS = [
    "LEASE_KID", "LEASE", "DOR_CODE", "API_NUMBER", "FIELD", "PRODUCING_ZONE",
    "OPERATOR", "COUNTY", "TOWNSHIP", "TWN_DIR", "RANGE", "RANGE_DIR",
    "SECTION", "SPOT", "LATITUDE", "LONGITUDE", "MONTH-YEAR", "PRODUCT",
    "WELLS", "PRODUCTION",
]


def _make_raw_rows(
    lease_kids: list[int],
    month_years: list[str],
    product: str = "O",
    production: float = 150.0,
) -> list[dict[str, Any]]:
    rows = []
    for lk in lease_kids:
        for my in month_years:
            rows.append({
                "LEASE_KID": lk,
                "LEASE": f"TEST LEASE {lk}",
                "DOR_CODE": 99999,
                "API_NUMBER": f"15-001-{lk:05d}",
                "FIELD": "TEST FIELD",
                "PRODUCING_ZONE": "Lansing Group",
                "OPERATOR": "Test Operator LLC",
                "COUNTY": "Douglas",
                "TOWNSHIP": 12,
                "TWN_DIR": "S",
                "RANGE": 20,
                "RANGE_DIR": "E",
                "SECTION": 5,
                "SPOT": "NENE",
                "LATITUDE": 38.9,
                "LONGITUDE": -95.2,
                "MONTH-YEAR": my,
                "PRODUCT": product,
                "WELLS": 2,
                "PRODUCTION": production,
            })
    return rows


@pytest.fixture()
def sample_raw_df() -> pd.DataFrame:
    """Synthetic raw DataFrame matching canonical schema, 2024 dates."""
    rows = _make_raw_rows(
        lease_kids=[1001, 1002, 1003],
        month_years=["1-2024", "2-2024", "3-2024", "4-2024", "5-2024", "6-2024"],
    )
    return pd.DataFrame(rows)


@pytest.fixture()
def sample_clean_df() -> pd.DataFrame:
    """Synthetic DataFrame after transform stage (has production_date)."""
    rows = _make_raw_rows(
        lease_kids=[1001, 1002],
        month_years=["1-2024", "2-2024", "3-2024", "4-2024", "5-2024", "6-2024"],
    )
    df = pd.DataFrame(rows)
    df["production_date"] = pd.to_datetime(
        df["MONTH-YEAR"].map(
            lambda my: f"{my.split('-')[1]}-{int(my.split('-')[0]):02d}-01"
        )
    )
    df["PRODUCTION"] = df["PRODUCTION"].astype("Float64")
    df["LEASE_KID"] = df["LEASE_KID"].astype("Int64")
    df["PRODUCT"] = df["PRODUCT"].astype(pd.CategoricalDtype(categories=["O", "G"]))
    return df


@pytest.fixture()
def sample_processed_df(sample_clean_df: pd.DataFrame) -> pd.DataFrame:
    """Synthetic DataFrame with cumulative and ratio features added."""
    df = sample_clean_df.copy()
    df["cum_production"] = df.groupby("LEASE_KID", observed=True)["PRODUCTION"].transform(
        "cumsum"
    )
    df["gor"] = 0.0
    df["water_cut"] = float("nan")
    df["decline_rate"] = 0.0
    df["rolling_3m"] = float("nan")
    df["rolling_6m"] = float("nan")
    df["lag_1"] = float("nan")
    return df


@pytest.fixture()
def mock_config(tmp_path: Path) -> dict[str, Any]:
    """Minimal pipeline config with all paths pointing to tmp_path."""
    raw_dir = tmp_path / "raw"
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    features_dir = tmp_path / "features"
    raw_dir.mkdir()

    return {
        "acquire": {
            "index_path": str(Path("data/external/oil_leases_2020_present.txt")),
            "raw_dir": str(raw_dir),
        },
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_path": str(interim_dir),
            "min_year": 2024,
            "dict_path": str(Path("references/kgs_monthly_data_dictionary.csv")),
        },
        "transform": {
            "interim_path": str(interim_dir),
            "processed_path": str(processed_dir),
        },
        "features": {
            "processed_path": str(processed_dir),
            "output_path": str(features_dir),
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
        "logging": {
            "log_file": str(tmp_path / "logs" / "pipeline.log"),
            "level": "INFO",
        },
    }


@pytest.fixture()
def tmp_data_dirs(tmp_path: Path) -> dict[str, Path]:
    """Create standard data subdirectory structure under tmp_path."""
    dirs = {
        "raw": tmp_path / "raw",
        "interim": tmp_path / "interim",
        "processed": tmp_path / "processed",
        "features": tmp_path / "features",
        "logs": tmp_path / "logs",
    }
    for d in dirs.values():
        d.mkdir(parents=True)
    return dirs


def make_raw_csv_file(
    path: Path,
    lease_kids: list[int] | None = None,
    month_years: list[str] | None = None,
    product: str = "O",
    production: float = 150.0,
) -> Path:
    """Write a minimal canonical raw CSV file for testing."""
    if lease_kids is None:
        lease_kids = [1001, 1002]
    if month_years is None:
        month_years = ["1-2024", "2-2024", "3-2024"]
    rows = _make_raw_rows(lease_kids, month_years, product, production)
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    return path
