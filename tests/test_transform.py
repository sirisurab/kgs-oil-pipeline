"""Test cases for transform component."""

import tempfile
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.transform import (
    add_unit_column,
    cast_and_rename_columns,
    deduplicate_records,
    explode_api_numbers,
    parse_dates,
    validate_physical_bounds,
)


@pytest.fixture
def sample_interim_data():
    """Create sample interim-like DataFrame."""
    df = pd.DataFrame(
        {
            "LEASE_KID": ["L001", "L001", "L002"],
            "LEASE": ["Lease A", "Lease A", "Lease B"],
            "API_NUMBER": ["12345, 67890", "12345", "11111"],
            "MONTH_YEAR": ["1-2020", "2-2020", "3-2020"],
            "PRODUCT": ["O", "G", "O"],
            "PRODUCTION": [100.0, 50.0, 150.0],
            "LATITUDE": [38.5, 38.6, 39.0],
            "LONGITUDE": [-98.5, -98.4, -97.5],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    return ddf


def test_parse_dates_success(sample_interim_data):
    """Given DataFrame with MONTH_YEAR, add production_date column."""
    result = parse_dates(sample_interim_data)
    result_df = result.compute()

    assert "production_date" in result_df.columns
    assert "MONTH_YEAR" not in result_df.columns
    assert result_df["production_date"].dtype == "datetime64[ns]"
    assert result_df["production_date"].iloc[0].year == 2020
    assert result_df["production_date"].iloc[0].month == 1


def test_parse_dates_missing_column():
    """Given DataFrame without MONTH_YEAR/MONTH-YEAR, raise KeyError."""
    df = pd.DataFrame({"other_col": [1, 2]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        parse_dates(ddf)


def test_cast_and_rename_columns_success(sample_interim_data):
    """Given DataFrame, rename to snake_case and cast types."""
    result = cast_and_rename_columns(sample_interim_data)
    result_df = result.compute()

    assert "lease_kid" in result_df.columns
    assert "well_id" not in result_df.columns  # API_NUMBER renamed, not yet exploded
    assert result_df["product"].iloc[0] == "O"  # Uppercase
    assert isinstance(result_df["latitude"].iloc[0], float)


def test_cast_and_rename_columns_missing_mandatory():
    """Given DataFrame missing mandatory columns, raise KeyError."""
    df = pd.DataFrame({"LEASE_KID": ["L001"]})  # Missing other required cols
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError, match="Mandatory"):
        cast_and_rename_columns(ddf)


def test_explode_api_numbers_success(sample_interim_data):
    """Given DataFrame with comma-separated API numbers, explode to individual wells."""
    # First rename API_NUMBER to api_number to match function expectations
    renamed = sample_interim_data.rename(columns={"API_NUMBER": "api_number"})
    sample_interim_data_with_api = renamed

    # Re-add LEASE_KID and PRODUCT columns for later use
    sample_interim_data_with_api = sample_interim_data_with_api.assign(
        LEASE_KID=sample_interim_data["LEASE_KID"]
    )

    # Rename to match function input expectations
    sample_interim_data_with_api = sample_interim_data_with_api.rename(
        columns={"LEASE_KID": "lease_kid", "api_number": "api_number"}
    )

    # Actually, let's create a properly formatted input
    df = pd.DataFrame(
        {
            "lease_kid": ["L001", "L001"],
            "api_number": ["12345, 67890", "11111"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)

    result = explode_api_numbers(ddf)
    result_df = result.compute()

    # Should have 3 rows: L001 has 2 APIs, L001 has 1 API (wait, re-reading)
    # First row has 2 APIs, second row has 1 API = 3 total
    assert len(result_df) == 3
    assert "well_id" in result_df.columns
    assert "api_number" not in result_df.columns


def test_explode_api_numbers_missing_column():
    """Given DataFrame without api_number, raise KeyError."""
    df = pd.DataFrame({"other_col": [1, 2]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        explode_api_numbers(ddf)


def test_validate_physical_bounds_success(sample_interim_data):
    """Given DataFrame with out-of-range values, flag outliers and clean bounds."""
    # Add some outliers
    df = sample_interim_data.compute()
    df.loc[0, "production"] = -10.0  # Negative
    df.loc[1, "production"] = 100000.0  # Outlier for oil
    df.loc[1, "product"] = "O"
    df.loc[2, "latitude"] = 41.0  # Out of Kansas bounds

    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf)
    result_df = result.compute()

    # Negative should become NaN
    assert pd.isna(result_df.loc[0, "production"])

    # Outlier should be flagged
    assert result_df.loc[1, "outlier_flag"] == True

    # Out-of-bounds latitude should be NaN
    assert pd.isna(result_df.loc[2, "latitude"])

    # Only valid products should remain
    assert result_df["product"].isin(["O", "G"]).all()


def test_validate_physical_bounds_missing_production():
    """Given DataFrame without production column, raise KeyError."""
    df = pd.DataFrame({"product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        validate_physical_bounds(ddf)


def test_deduplicate_records_success(sample_interim_data):
    """Given DataFrame with duplicates, remove by well-date-product."""
    df = sample_interim_data.compute()
    # Add duplicate row
    df = pd.concat(
        [
            df,
            pd.DataFrame(
                {
                    "LEASE_KID": ["L001"],
                    "LEASE": ["Lease A"],
                    "API_NUMBER": ["12345"],
                    "MONTH_YEAR": ["1-2020"],
                    "PRODUCT": ["O"],
                    "PRODUCTION": [100.0],
                    "LATITUDE": [38.5],
                    "LONGITUDE": [-98.5],
                }
            ),
        ],
        ignore_index=True,
    )

    # Prepare for deduplication (need production_date and well_id)
    df["production_date"] = pd.to_datetime("2020-01-01")
    df["well_id"] = df["API_NUMBER"]

    ddf = dd.from_pandas(df, npartitions=1)
    result = deduplicate_records(ddf)
    result_df = result.compute()

    # Should remove the duplicate
    assert len(result_df) < len(df)


def test_deduplicate_records_missing_column():
    """Given DataFrame without well_id, raise KeyError."""
    df = pd.DataFrame({"production_date": ["2020-01-01"]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        deduplicate_records(ddf)


def test_add_unit_column_success(sample_interim_data):
    """Given DataFrame with product, add unit column."""
    result = add_unit_column(sample_interim_data)
    result_df = result.compute()

    assert "unit" in result_df.columns
    assert result_df.loc[result_df["PRODUCT"] == "O", "unit"].iloc[0] == "BBL"
    assert result_df.loc[result_df["PRODUCT"] == "G", "unit"].iloc[0] == "MCF"


def test_add_unit_column_missing_product():
    """Given DataFrame without product, raise KeyError."""
    df = pd.DataFrame({"other_col": [1, 2]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        add_unit_column(ddf)
