"""Test cases for features component."""

import tempfile
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.features import (
    aggregate_by_well_month,
    compute_well_lifetime,
    cumulative_production,
    filter_2020_2025,
    production_trend,
    rolling_averages,
)


@pytest.fixture
def sample_processed_data():
    """Create sample processed-like DataFrame."""
    df = pd.DataFrame(
        {
            "well_id": ["W001", "W001", "W001", "W002", "W002"],
            "production_date": pd.to_datetime(
                [
                    "2020-01-15",
                    "2020-02-15",
                    "2025-12-15",
                    "2019-12-15",
                    "2020-01-15",
                ]
            ),
            "product": ["O", "O", "O", "G", "G"],
            "production": [100.0, 110.0, 120.0, 50.0, 55.0],
            "operator": ["Op1", "Op1", "Op1", "Op2", "Op2"],
            "field_name": ["Field A", "Field A", "Field A", "Field B", "Field B"],
            "latitude": [38.5, 38.5, 38.5, 39.0, 39.0],
            "longitude": [-98.5, -98.5, -98.5, -97.5, -97.5],
            "lease_name": ["Lease1", "Lease1", "Lease1", "Lease2", "Lease2"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    return ddf


def test_filter_2020_2025_success(sample_processed_data):
    """Given DataFrame with dates, filter to 2020-2025."""
    result = filter_2020_2025(sample_processed_data)
    result_df = result.compute()

    # Should exclude 2019-12-15 record (W002)
    assert result_df["production_date"].min().year >= 2020
    assert result_df["production_date"].max().year <= 2025
    assert len(result_df) == 4  # Exclude 1 out-of-range record


def test_filter_2020_2025_missing_column():
    """Given DataFrame without production_date, raise KeyError."""
    df = pd.DataFrame({"other_col": [1, 2]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        filter_2020_2025(ddf)


def test_aggregate_by_well_month_success(sample_processed_data):
    """Given production records, aggregate by well-month."""
    result = aggregate_by_well_month(sample_processed_data)
    result_df = result.compute()

    assert "year_month" in result_df.columns
    assert "production_sum_month" in result_df.columns
    assert "production_mean_month" in result_df.columns
    assert "production_count_month" in result_df.columns

    # Should have fewer rows than input (aggregated)
    assert len(result_df) <= len(sample_processed_data)


def test_aggregate_by_well_month_missing_column():
    """Given DataFrame missing required columns, raise KeyError."""
    df = pd.DataFrame({"well_id": ["W001"]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        aggregate_by_well_month(ddf)


def test_rolling_averages_success(sample_processed_data):
    """Given aggregated data, compute rolling average."""
    # First aggregate
    agg_data = aggregate_by_well_month(sample_processed_data)
    agg_df = agg_data.compute()

    # Ensure production_sum_month exists
    assert "production_sum_month" in agg_df.columns

    result = rolling_averages(agg_data, window=12)
    result_df = result.compute()

    assert "rolling_avg_12mo" in result_df.columns


def test_rolling_averages_missing_column():
    """Given DataFrame without production_sum_month, raise KeyError."""
    df = pd.DataFrame({"well_id": ["W001"]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        rolling_averages(ddf)


def test_cumulative_production_success(sample_processed_data):
    """Given aggregated data, compute cumulative production."""
    # First aggregate
    agg_data = aggregate_by_well_month(sample_processed_data)
    agg_df = agg_data.compute()

    # Ensure production_sum_month exists
    assert "production_sum_month" in agg_df.columns

    result = cumulative_production(agg_data)
    result_df = result.compute()

    assert "cumulative_production" in result_df.columns


def test_cumulative_production_missing_column():
    """Given DataFrame without production_sum_month, raise KeyError."""
    df = pd.DataFrame({"well_id": ["W001"]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        cumulative_production(ddf)


def test_production_trend_success(sample_processed_data):
    """Given aggregated data with rolling avg, compute trend."""
    # First aggregate
    agg_data = aggregate_by_well_month(sample_processed_data)
    
    # Compute rolling first
    rolling_data = rolling_averages(agg_data, window=12)
    rolling_df = rolling_data.compute()

    assert "rolling_avg_12mo" in rolling_df.columns

    result = production_trend(rolling_data)
    result_df = result.compute()

    assert "production_trend" in result_df.columns
    assert result_df["production_trend"].isin(
        ["increasing", "decreasing", "stable", "insufficient_data"]
    ).all()


def test_production_trend_missing_rolling_avg():
    """Given DataFrame without rolling_avg_12mo, raise KeyError."""
    df = pd.DataFrame({"well_id": ["W001"]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        production_trend(ddf)


def test_compute_well_lifetime_success(sample_processed_data):
    """Given aggregated data, compute well lifetime in months."""
    # First aggregate
    agg_data = aggregate_by_well_month(sample_processed_data)

    result = compute_well_lifetime(agg_data)
    result_df = result.compute()

    assert "well_lifetime_months" in result_df.columns
    assert (result_df["well_lifetime_months"] > 0).any()


def test_compute_well_lifetime_missing_column():
    """Given DataFrame without year_month, raise KeyError."""
    df = pd.DataFrame({"well_id": ["W001"]})
    ddf = dd.from_pandas(df, npartitions=1)

    with pytest.raises(KeyError):
        compute_well_lifetime(ddf)
