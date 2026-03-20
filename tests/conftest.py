"""Pytest configuration and fixtures."""

import pytest
import tempfile  # noqa: F401
from pathlib import Path
import pandas as pd  # type: ignore[import-untyped]
import dask.dataframe as dd


@pytest.fixture
def sample_lease_data() -> pd.DataFrame:
    """Sample lease data for testing."""
    return pd.DataFrame({
        "LEASE_KID": ["L001", "L002", "L001"],
        "URL": [
            "https://example.com/lease1",
            "https://example.com/lease2",
            "https://example.com/lease1_dup",
        ],
    })


@pytest.fixture
def sample_raw_data() -> pd.DataFrame:
    """Sample raw monthly data."""
    return pd.DataFrame({
        "LEASE_KID": ["L001", "L001"],
        "MONTH_YEAR": ["1-2020", "2-2020"],
        "API_NUMBER": ["15-131-20143", "15-131-20089"],
        "PRODUCTION": [100.0, 150.0],
        "PRODUCT": ["O", "O"],
        "COUNTY": ["SEDGWICK", "SEDGWICK"],
        "FIELD": ["FIELD1", "FIELD1"],
        "OPERATOR": ["OP1", "OP1"],
    })


@pytest.fixture
def sample_interim_data() -> pd.DataFrame:
    """Sample interim (ingested) data."""
    return pd.DataFrame({
        "well_id": ["15-131-20143", "15-131-20143", "15-131-20089", "15-131-20089"],
        "production_date": [
            pd.Timestamp("2020-01-01"),
            pd.Timestamp("2020-02-01"),
            pd.Timestamp("2020-01-01"),
            pd.Timestamp("2020-02-01"),
        ],
        "oil_bbl": [100.0, 150.0, 200.0, 250.0],
        "gas_mcf": [10.0, 15.0, 20.0, 25.0],
        "lease_kid": ["L001", "L001", "L002", "L002"],
        "county": ["SEDGWICK", "SEDGWICK", "BUTLER", "BUTLER"],
        "field": ["F1", "F1", "F2", "F2"],
        "producing_zone": ["Z1", "Z1", "Z2", "Z2"],
        "operator": ["OP1", "OP1", "OP2", "OP2"],
    })


@pytest.fixture
def sample_processed_data() -> pd.DataFrame:
    """Sample processed (transformed) data."""
    return pd.DataFrame({
        "well_id": ["W1", "W1", "W2", "W2"],
        "production_date": [
            pd.Timestamp("2020-01-01"),
            pd.Timestamp("2020-02-01"),
            pd.Timestamp("2020-01-01"),
            pd.Timestamp("2020-02-01"),
        ],
        "oil_bbl": [100.0, 120.0, 200.0, 180.0],
        "gas_mcf": [10.0, 12.0, 20.0, 18.0],
        "cumulative_oil_bbl": [100.0, 220.0, 200.0, 380.0],
        "cumulative_gas_mcf": [10.0, 22.0, 20.0, 38.0],
        "county": ["SEDGWICK", "SEDGWICK", "BUTLER", "BUTLER"],
        "field": ["F1", "F1", "F2", "F2"],
        "producing_zone": ["Z1", "Z1", "Z2", "Z2"],
        "operator": ["OP1", "OP1", "OP2", "OP2"],
    })


@pytest.fixture
def tmp_parquet_dir(tmp_path: Path) -> Path:
    """Create a temporary directory with parquet files."""
    df = pd.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"],
    })
    parquet_file = tmp_path / "test.parquet"
    df.to_parquet(parquet_file)
    return tmp_path


@pytest.fixture
def dask_dataframe_small() -> dd.DataFrame:
    """Small Dask DataFrame for testing."""
    df = pd.DataFrame({
        "well_id": ["W1", "W1", "W2"],
        "production_date": [
            pd.Timestamp("2020-01-01"),
            pd.Timestamp("2020-02-01"),
            pd.Timestamp("2020-01-01"),
        ],
        "oil_bbl": [100.0, 120.0, 200.0],
        "gas_mcf": [10.0, 12.0, 20.0],
    })
    return dd.from_pandas(df, npartitions=1)


def pytest_configure(config: pytest.Config) -> None:  # type: ignore[name-defined]
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
