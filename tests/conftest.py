"""Shared pytest fixtures for the KGS pipeline test suite."""

from pathlib import Path

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pytest


KGS_COLUMNS = [
    "LEASE_KID",
    "LEASE",
    "DOR_CODE",
    "API_NUMBER",
    "FIELD",
    "PRODUCING_ZONE",
    "OPERATOR",
    "COUNTY",
    "TOWNSHIP",
    "TWN_DIR",
    "RANGE",
    "RANGE_DIR",
    "SECTION",
    "SPOT",
    "LATITUDE",
    "LONGITUDE",
    "MONTH_YEAR",
    "PRODUCT",
    "WELLS",
    "PRODUCTION",
    "source_file",
]

RAW_CSV_HEADER = (
    "LEASE KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,COUNTY,"
    "TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,"
    "MONTH-YEAR,PRODUCT,WELLS,PRODUCTION,URL"
)

RAW_CSV_ROW_TEMPLATE = (
    "{lease_kid},TEST LEASE,DOR123,{api},{field},Lansing Group,Test Op LLC,"
    "{county},1,S,14,E,1,NENENE,38.5,-98.5,"
    "{month_year},{product},{wells},{production},"
    "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc={lease_kid}"
)


def _make_raw_csv_content(n_rows: int = 5, lease_kid: int = 1001, prefix: str = "lp") -> str:
    """Generate minimal KGS-format CSV content for testing."""
    rows = [RAW_CSV_HEADER]
    for i in range(n_rows):
        rows.append(
            RAW_CSV_ROW_TEMPLATE.format(
                lease_kid=lease_kid,
                api=f"1500100{i:04d}",
                field="KANASKA",
                county="Ellis",
                month_year=f"{i + 1}-2021",
                product="O",
                wells=2,
                production=100 + i * 10,
            )
        )
    return "\n".join(rows) + "\n"


@pytest.fixture
def raw_csv_content() -> str:
    """Sample KGS-format CSV content."""
    return _make_raw_csv_content()


@pytest.fixture
def raw_csv_file(tmp_path: Path) -> Path:
    """A single valid lp*.txt file in tmp_path."""
    p = tmp_path / "lp1001.txt"
    p.write_text(_make_raw_csv_content(lease_kid=1001), encoding="utf-8")
    return p


@pytest.fixture
def three_raw_csv_files(tmp_path: Path) -> list[Path]:
    """Three valid lp*.txt files in tmp_path."""
    paths = []
    for i, kid in enumerate([1001, 1002, 1003]):
        p = tmp_path / f"lp{kid}.txt"
        p.write_text(_make_raw_csv_content(lease_kid=kid), encoding="utf-8")
        paths.append(p)
    return sorted(paths)


@pytest.fixture
def sample_interim_df() -> pd.DataFrame:
    """Sample pandas DataFrame matching the interim Parquet schema."""
    return pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001, 1002], dtype="int64"),
            "LEASE": ["TEST LEASE A", "TEST LEASE A", "TEST LEASE B"],
            "DOR_CODE": ["DOR001", "DOR001", "DOR002"],
            "API_NUMBER": ["1500112345, 1500167890", "1500112345", "1500299999"],
            "FIELD": ["KANASKA", "KANASKA", "HUGOTON"],
            "PRODUCING_ZONE": ["Lansing Group", "Lansing Group", "Hugoton"],
            "OPERATOR": ["Buckeye West LLC", "Buckeye West LLC", "Pioneer Energy"],
            "COUNTY": ["Nemaha", "Nemaha", "Stevens"],
            "TOWNSHIP": ["1", "1", "34"],
            "TWN_DIR": ["S", "S", "S"],
            "RANGE": ["14", "14", "37"],
            "RANGE_DIR": ["E", "E", "W"],
            "SECTION": ["1", "1", "15"],
            "SPOT": ["NENENE", "NENENE", "SWNW"],
            "LATITUDE": [39.999519, 39.999519, 37.012345],
            "LONGITUDE": [-95.78905, -95.78905, -101.23456],
            "MONTH_YEAR": ["1-2020", "3-2020", "6-2021"],
            "PRODUCT": ["O", "O", "G"],
            "WELLS": pd.array([2, 2, 1], dtype="Int64"),
            "PRODUCTION": [161.8, 163.74, 500.0],
            "source_file": ["lp1001", "lp1001", "lp1002"],
        }
    )


@pytest.fixture
def sample_interim_ddf(sample_interim_df: pd.DataFrame) -> dd.DataFrame:
    """Sample Dask DataFrame from the interim schema fixture."""
    return dd.from_pandas(sample_interim_df, npartitions=1)


@pytest.fixture
def sample_processed_df() -> pd.DataFrame:
    """Sample pandas DataFrame matching the processed Parquet schema."""
    return pd.DataFrame(
        {
            "well_id": ["1500112345", "1500112345", "1500299999"],
            "lease_kid": pd.array([1001, 1001, 1002], dtype="int64"),
            "lease": ["TEST LEASE A", "TEST LEASE A", "TEST LEASE B"],
            "dor_code": ["DOR001", "DOR001", "DOR002"],
            "field": ["KANASKA", "KANASKA", "HUGOTON"],
            "producing_zone": ["Lansing Group", "Lansing Group", "Hugoton"],
            "operator": ["Buckeye West LLC", "Buckeye West LLC", "Pioneer Energy"],
            "county": ["Nemaha", "Nemaha", "Stevens"],
            "township": ["1", "1", "34"],
            "twn_dir": ["S", "S", "S"],
            "range_": ["14", "14", "37"],
            "range_dir": ["E", "E", "W"],
            "section": ["1", "1", "15"],
            "spot": ["NENENE", "NENENE", "SWNW"],
            "latitude": [39.999519, 39.999519, 37.012345],
            "longitude": [-95.78905, -95.78905, -101.23456],
            "production_date": pd.to_datetime(["2020-01-01", "2020-03-01", "2021-06-01"]),
            "product": ["O", "O", "G"],
            "wells": pd.array([2, 2, 1], dtype="Int64"),
            "production": [161.8, 163.74, 500.0],
            "unit": ["BBL", "BBL", "MCF"],
            "outlier_flag": [False, False, False],
            "source_file": ["lp1001", "lp1001", "lp1002"],
        }
    )


@pytest.fixture
def sample_processed_ddf(sample_processed_df: pd.DataFrame) -> dd.DataFrame:
    """Sample Dask DataFrame from the processed schema fixture."""
    return dd.from_pandas(sample_processed_df, npartitions=1)


@pytest.fixture
def well_sequence_df() -> pd.DataFrame:
    """A single-well oil production sequence for feature testing."""
    dates = pd.date_range("2020-01-01", periods=12, freq="MS")
    productions = [100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 10.0, 5.0, 0.0]
    return pd.DataFrame(
        {
            "well_id": ["W001"] * 12,
            "product": ["O"] * 12,
            "production_date": dates,
            "production": productions,
            "unit": ["BBL"] * 12,
            "lease_kid": pd.array([1001] * 12, dtype="int64"),
            "operator": ["Test Op"] * 12,
            "county": ["Ellis"] * 12,
            "field": ["KANASKA"] * 12,
            "producing_zone": ["Lansing Group"] * 12,
            "latitude": [38.5] * 12,
            "longitude": [-98.5] * 12,
            "outlier_flag": [False] * 12,
        }
    )
