"""Test cases for ingest component."""

import tempfile
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.ingest import (
    filter_monthly_records,
    merge_with_metadata,
    read_lease_index,
    read_raw_lease_files,
)


@pytest.fixture
def temp_raw_files():
    """Create temporary raw .txt lease files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create two lp*.txt files
        df1 = pd.DataFrame(
            {
                "LEASE_KID": ["L001", "L001", "L001"],
                "API_NUMBER": ["12345", "12345", "12345"],
                "MONTH-YEAR": ["1-2020", "2-2020", "0-2020"],  # Last is yearly
                "PRODUCT": ["O", "G", "O"],
                "PRODUCTION": [100, 50, 200],
            }
        )
        df1.to_csv(tmpdir / "lp_001.txt", index=False)

        df2 = pd.DataFrame(
            {
                "LEASE_KID": ["L002"],
                "API_NUMBER": ["54321"],
                "MONTH-YEAR": ["3-2020"],
                "PRODUCT": ["O"],
                "PRODUCTION": [150],
            }
        )
        df2.to_csv(tmpdir / "lp_002.txt", index=False)

        yield tmpdir


@pytest.fixture
def temp_lease_index():
    """Create temporary lease index file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        df = pd.DataFrame(
            {
                "LEASE_KID": ["L001", "L002"],
                "LEASE": ["Lease 1", "Lease 2"],
                "DOR_CODE": ["D001", "D002"],
                "FIELD": ["Field A", "Field B"],
                "PRODUCING_ZONE": ["Zone A", "Zone B"],
                "OPERATOR": ["Operator 1", "Operator 2"],
                "COUNTY": ["County A", "County B"],
                "TOWNSHIP": ["T01", "T02"],
                "TWN_DIR": ["N", "N"],
                "RANGE": ["R01", "R02"],
                "RANGE_DIR": ["W", "W"],
                "SECTION": ["01", "02"],
                "SPOT": ["A", "B"],
                "LATITUDE": [38.5, 38.6],
                "LONGITUDE": [-98.5, -98.6],
            }
        )
        df.to_csv(tmpdir / "leases.csv", index=False)
        yield tmpdir / "leases.csv"


def test_read_raw_lease_files_success(temp_raw_files):
    """Given directory with lp*.txt files, return Dask DataFrame."""
    ddf = read_raw_lease_files(temp_raw_files)

    assert isinstance(ddf, dd.DataFrame)
    assert "source_file" in ddf.columns
    assert len(ddf) >= 3  # At least 3 rows from df1


def test_read_raw_lease_files_not_found():
    """Given nonexistent directory, raise FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        read_raw_lease_files(Path("/nonexistent/dir"))


def test_read_raw_lease_files_no_matches():
    """Given directory with no lp*.txt files, raise FileNotFoundError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        # Create a .txt file that doesn't match lp*.txt
        (tmpdir / "other.txt").touch()

        with pytest.raises(FileNotFoundError):
            read_raw_lease_files(tmpdir)


def test_read_lease_index_success(temp_lease_index):
    """Given valid lease index, return Dask DataFrame with metadata."""
    ddf = read_lease_index(temp_lease_index)

    assert isinstance(ddf, dd.DataFrame)
    assert "LEASE_KID" in ddf.columns
    assert "OPERATOR" in ddf.columns


def test_read_lease_index_not_found():
    """Given nonexistent file, raise FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        read_lease_index(Path("/nonexistent/leases.csv"))


def test_read_lease_index_missing_columns():
    """Given index with missing required columns, raise KeyError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        # Create index with missing FIELD column
        df = pd.DataFrame(
            {
                "LEASE_KID": ["L001"],
                "LEASE": ["Lease 1"],
                # Missing many required columns
            }
        )
        df.to_csv(tmpdir / "bad_index.csv", index=False)

        with pytest.raises(KeyError):
            read_lease_index(tmpdir / "bad_index.csv")


def test_filter_monthly_records_success(temp_raw_files):
    """Given DataFrame with mixed monthly/yearly records, filter to monthly only."""
    ddf = read_raw_lease_files(temp_raw_files)
    filtered = filter_monthly_records(ddf)

    # Should remove records with month "0" (yearly)
    result_df = filtered.compute()
    if "MONTH-YEAR" in result_df.columns:
        months = result_df["MONTH-YEAR"].str.split("-").str[0]
        assert not months.isin(["0", "-1"]).any()


def test_filter_monthly_records_missing_column():
    """Given DataFrame without MONTH_YEAR/MONTH-YEAR, raise KeyError."""
    df = pd.DataFrame({"other_col": [1, 2, 3]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        filter_monthly_records(ddf)


def test_merge_with_metadata_success(temp_raw_files, temp_lease_index):
    """Given raw data and metadata, return merged Dask DataFrame."""
    raw_ddf = read_raw_lease_files(temp_raw_files)
    meta_ddf = read_lease_index(temp_lease_index)

    merged = merge_with_metadata(raw_ddf, meta_ddf)

    assert isinstance(merged, dd.DataFrame)
    assert "LEASE_KID" in merged.columns
    # Merged should have more columns than raw
    assert len(merged.columns) > len(raw_ddf.columns)


def test_merge_with_metadata_missing_key():
    """Given DataFrames without LEASE_KID, raise KeyError."""
    df_raw = pd.DataFrame({"other_col": [1, 2]})
    df_meta = pd.DataFrame({"other_col": [3, 4]})
    ddf_raw = dd.from_pandas(df_raw, npartitions=1)
    ddf_meta = dd.from_pandas(df_meta, npartitions=1)

    with pytest.raises(KeyError):
        merge_with_metadata(ddf_raw, ddf_meta)
