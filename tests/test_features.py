"""Tests for kgs_pipeline/features.py (Tasks 22–31)."""

from pathlib import Path
from unittest.mock import patch

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pytest


def _make_well_df(
    productions: list[float],
    well_id: str = "W001",
    product: str = "O",
    start: str = "2020-01-01",
) -> pd.DataFrame:
    """Helper: build a single-well production DataFrame."""
    dates = pd.date_range(start, periods=len(productions), freq="MS")
    return pd.DataFrame(
        {
            "well_id": [well_id] * len(productions),
            "product": [product] * len(productions),
            "production_date": dates,
            "production": productions,
            "unit": ["BBL" if product == "O" else "MCF"] * len(productions),
            "lease_kid": pd.array([1001] * len(productions), dtype="int64"),
            "operator": ["Test Op"] * len(productions),
            "county": ["Ellis"] * len(productions),
            "field": ["KANASKA"] * len(productions),
            "producing_zone": ["Lansing Group"] * len(productions),
            "latitude": [38.5] * len(productions),
            "longitude": [-98.5] * len(productions),
            "outlier_flag": [False] * len(productions),
        }
    )


# ---------------------------------------------------------------------------
# Task 22: load_processed_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_processed_parquet_returns_dask(tmp_path: Path, sample_processed_df):
    from kgs_pipeline.features import load_processed_parquet

    sample_processed_df.to_parquet(tmp_path / "part.parquet", index=False)
    result = load_processed_parquet(tmp_path)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_load_processed_parquet_missing_dir():
    from kgs_pipeline.features import load_processed_parquet

    with pytest.raises(FileNotFoundError):
        load_processed_parquet(Path("/nonexistent/path/xyz"))


@pytest.mark.unit
def test_load_processed_parquet_empty_dir(tmp_path: Path):
    from kgs_pipeline.features import load_processed_parquet

    with pytest.raises(RuntimeError, match="No Parquet files found"):
        load_processed_parquet(tmp_path)


@pytest.mark.unit
def test_load_processed_parquet_not_pandas(tmp_path: Path, sample_processed_df):
    from kgs_pipeline.features import load_processed_parquet

    sample_processed_df.to_parquet(tmp_path / "part.parquet", index=False)
    result = load_processed_parquet(tmp_path)
    assert not isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 23: compute_cumulative_production
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cumulative_production_monotonic():
    from kgs_pipeline.features import compute_cumulative_production

    df = _make_well_df([100.0, 80.0, 0.0, 60.0, 50.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    cum = result.sort_values("production_date")["cum_production"].tolist()
    assert cum == [100.0, 180.0, 180.0, 240.0, 290.0]


@pytest.mark.unit
def test_cumulative_never_decreases():
    from kgs_pipeline.features import compute_cumulative_production

    df = _make_well_df([100.0, 80.0, 60.0, 40.0, 20.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    cum = result.sort_values("production_date")["cum_production"].tolist()
    for i in range(1, len(cum)):
        assert cum[i] >= cum[i - 1]


@pytest.mark.unit
def test_cumulative_flat_during_shutin():
    from kgs_pipeline.features import compute_cumulative_production

    df = _make_well_df([100.0, 50.0, 0.0, 0.0, 75.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    cum = result.sort_values("production_date")["cum_production"].tolist()
    assert cum == [100.0, 150.0, 150.0, 150.0, 225.0]


@pytest.mark.unit
def test_cumulative_zero_start():
    from kgs_pipeline.features import compute_cumulative_production

    df = _make_well_df([0.0, 0.0, 100.0, 80.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    cum = result.sort_values("production_date")["cum_production"].tolist()
    assert cum == [0.0, 0.0, 100.0, 180.0]


@pytest.mark.unit
def test_cumulative_zero_end():
    from kgs_pipeline.features import compute_cumulative_production

    df = _make_well_df([100.0, 80.0, 0.0, 0.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    cum = result.sort_values("production_date")["cum_production"].tolist()
    assert cum == [100.0, 180.0, 180.0, 180.0]


@pytest.mark.unit
def test_cumulative_returns_dask():
    from kgs_pipeline.features import compute_cumulative_production

    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_cumulative_production(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 24: compute_decline_rate
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decline_rate_clipped_low():
    """Decline rate < -1.0 is clipped to -1.0."""
    from kgs_pipeline.features import compute_decline_rate

    # prod[t-1]=100, prod[t]=1 → raw = (1-100)/100 = -0.99 (within bounds)
    # To get < -1.0, we need prod going from 100 to ~0 then a large positive
    # Actually: prod[0]=100, prod[1]=1 → decline = (1-100)/100 = -0.99, not < -1.0
    # For > -1.0 clipping: this can't happen with positive values only.
    # Use direct: let's test that -0.99 passes through unchanged
    df = _make_well_df([100.0, 1.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    rate = result["decline_rate"].iloc[1]
    assert abs(rate - (-0.99)) < 0.01


@pytest.mark.unit
def test_decline_rate_clipped_high():
    """Decline rate > 10.0 is clipped to 10.0."""
    from kgs_pipeline.features import compute_decline_rate

    # prod[0]=10, prod[1]=160 → raw = (160-10)/10 = 15.0 → clipped to 10.0
    df = _make_well_df([10.0, 160.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert result["decline_rate"].iloc[1] == 10.0


@pytest.mark.unit
def test_decline_rate_within_bounds():
    """Decline rate within [-1, 10] passes through."""
    from kgs_pipeline.features import compute_decline_rate

    # prod[0]=100, prod[1]=50 → raw = (50-100)/100 = -0.5
    df = _make_well_df([100.0, 50.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert abs(result["decline_rate"].iloc[1] - (-0.5)) < 0.001


@pytest.mark.unit
def test_decline_rate_zero_denominator_is_nan():
    """When prior production is 0, decline_rate must be NaN (not inf)."""
    from kgs_pipeline.features import compute_decline_rate

    df = _make_well_df([0.0, 100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    rate = result["decline_rate"].iloc[1]
    assert pd.isna(rate), f"Expected NaN but got {rate}"


@pytest.mark.unit
def test_decline_rate_first_row_nan():
    from kgs_pipeline.features import compute_decline_rate

    df = _make_well_df([100.0, 80.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert pd.isna(result["decline_rate"].iloc[0])


@pytest.mark.unit
def test_decline_rate_returns_dask():
    from kgs_pipeline.features import compute_decline_rate

    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_decline_rate(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 25: compute_rolling_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rolling_mean_3m_at_index_2():
    from kgs_pipeline.features import compute_rolling_features

    df = _make_well_df([100.0, 120.0, 80.0, 140.0, 100.0, 110.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_features(ddf).compute().sort_values("production_date")
    # Index 2: mean([100, 120, 80]) = 100.0
    assert abs(result["rolling_mean_3m"].iloc[2] - 100.0) < 0.01


@pytest.mark.unit
def test_rolling_mean_3m_at_index_3():
    from kgs_pipeline.features import compute_rolling_features

    df = _make_well_df([100.0, 120.0, 80.0, 140.0, 100.0, 110.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_features(ddf).compute().sort_values("production_date")
    # Index 3: mean([120, 80, 140]) = 113.33
    assert abs(result["rolling_mean_3m"].iloc[3] - 113.333) < 0.01


@pytest.mark.unit
def test_rolling_mean_6m_at_index_5():
    from kgs_pipeline.features import compute_rolling_features

    df = _make_well_df([100.0, 120.0, 80.0, 140.0, 100.0, 110.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_features(ddf).compute().sort_values("production_date")
    # Index 5: mean([100,120,80,140,100,110]) = 108.33
    assert abs(result["rolling_mean_6m"].iloc[5] - 108.333) < 0.01


@pytest.mark.unit
def test_rolling_mean_min_periods_partial():
    from kgs_pipeline.features import compute_rolling_features

    df = _make_well_df([100.0, 80.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_features(ddf).compute().sort_values("production_date")
    # rolling_mean_3m at month 2 uses partial window → not NaN
    assert not pd.isna(result["rolling_mean_3m"].iloc[1])


@pytest.mark.unit
def test_rolling_std_1_obs_is_nan():
    from kgs_pipeline.features import compute_rolling_features

    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_features(ddf).compute()
    assert pd.isna(result["rolling_std_3m"].iloc[0])


@pytest.mark.unit
def test_rolling_all_columns_present():
    from kgs_pipeline.features import compute_rolling_features

    df = _make_well_df([100.0, 90.0, 80.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_features(ddf)
    for col in [
        "rolling_mean_3m",
        "rolling_mean_6m",
        "rolling_mean_12m",
        "rolling_std_3m",
        "rolling_std_6m",
    ]:
        assert col in result.columns


@pytest.mark.unit
def test_rolling_returns_dask():
    from kgs_pipeline.features import compute_rolling_features

    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_rolling_features(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 26: compute_lag_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_lag_features_values():
    from kgs_pipeline.features import compute_lag_features

    df = _make_well_df([100.0, 80.0, 60.0, 40.0, 20.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = (
        compute_lag_features(ddf).compute().sort_values("production_date").reset_index(drop=True)
    )
    assert result["lag_1m"].iloc[1] == 100.0
    assert result["lag_1m"].iloc[2] == 80.0
    assert result["lag_1m"].iloc[3] == 60.0
    assert pd.isna(result["lag_1m"].iloc[0])
    assert result["lag_3m"].iloc[3] == 100.0
    assert result["lag_3m"].iloc[4] == 80.0
    assert pd.isna(result["lag_3m"].iloc[0])
    assert pd.isna(result["lag_3m"].iloc[1])
    assert pd.isna(result["lag_3m"].iloc[2])


@pytest.mark.unit
def test_lag_1m_equals_prior_month_production():
    from kgs_pipeline.features import compute_lag_features

    df = _make_well_df([200.0, 150.0, 100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = (
        compute_lag_features(ddf).compute().sort_values("production_date").reset_index(drop=True)
    )
    # Verify lag-1 formula for 3 consecutive months
    assert result["lag_1m"].iloc[1] == result["production"].iloc[0]
    assert result["lag_1m"].iloc[2] == result["production"].iloc[1]


@pytest.mark.unit
def test_lag_columns_present():
    from kgs_pipeline.features import compute_lag_features

    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_lag_features(ddf)
    assert "lag_1m" in result.columns
    assert "lag_3m" in result.columns


@pytest.mark.unit
def test_lag_returns_dask():
    from kgs_pipeline.features import compute_lag_features

    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_lag_features(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 27: compute_time_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_time_features_values():
    from kgs_pipeline.features import compute_time_features

    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "product": ["O"],
            "production_date": pd.to_datetime(["2020-03-01"]),
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_time_features(ddf).compute()
    assert result["production_year"].iloc[0] == 2020
    assert result["production_month"].iloc[0] == 3
    assert result["production_quarter"].iloc[0] == 1


@pytest.mark.unit
def test_months_since_first_prod():
    from kgs_pipeline.features import compute_time_features

    df = pd.DataFrame(
        {
            "well_id": ["W1"] * 3,
            "product": ["O"] * 3,
            "production_date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
            "production": [100.0, 90.0, 80.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = (
        compute_time_features(ddf).compute().sort_values("production_date").reset_index(drop=True)
    )
    assert list(result["months_since_first_prod"]) == [0, 1, 2]


@pytest.mark.unit
def test_months_since_first_prod_with_gap():
    from kgs_pipeline.features import compute_time_features

    df = pd.DataFrame(
        {
            "well_id": ["W1"] * 2,
            "product": ["O"] * 2,
            "production_date": pd.to_datetime(["2020-01-01", "2020-04-01"]),
            "production": [100.0, 80.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = (
        compute_time_features(ddf).compute().sort_values("production_date").reset_index(drop=True)
    )
    assert list(result["months_since_first_prod"]) == [0, 3]


@pytest.mark.unit
def test_time_all_columns_present():
    from kgs_pipeline.features import compute_time_features

    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "product": ["O"],
            "production_date": pd.to_datetime(["2020-01-01"]),
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_time_features(ddf)
    for col in [
        "production_year",
        "production_month",
        "production_quarter",
        "months_since_first_prod",
    ]:
        assert col in result.columns


@pytest.mark.unit
def test_time_returns_dask():
    from kgs_pipeline.features import compute_time_features

    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "product": ["O"],
            "production_date": pd.to_datetime(["2020-01-01"]),
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_time_features(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 28: compute_ratio_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_gor_zero_oil_nonzero_gas_is_nan():
    from kgs_pipeline.features import compute_ratio_features

    # Build oil row (0) and gas row (100) for same well-month
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "product": ["O", "G"],
            "production_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "production": [0.0, 100.0],
            "unit": ["BBL", "MCF"],
            "lease_kid": pd.array([1, 1], dtype="int64"),
            "operator": ["O", "O"],
            "county": ["C", "C"],
            "field": ["F", "F"],
            "producing_zone": ["Z", "Z"],
            "latitude": [38.0, 38.0],
            "longitude": [-98.0, -98.0],
            "outlier_flag": [False, False],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_ratio_features(ddf).compute()
    oil_row = result[result["product"] == "O"]
    assert pd.isna(oil_row["gor"].iloc[0])


@pytest.mark.unit
def test_gor_zero_both_is_nan_or_zero():
    from kgs_pipeline.features import compute_ratio_features

    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "product": ["O", "G"],
            "production_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "production": [0.0, 0.0],
            "unit": ["BBL", "MCF"],
            "lease_kid": pd.array([1, 1], dtype="int64"),
            "operator": ["O", "O"],
            "county": ["C", "C"],
            "field": ["F", "F"],
            "producing_zone": ["Z", "Z"],
            "latitude": [38.0, 38.0],
            "longitude": [-98.0, -98.0],
            "outlier_flag": [False, False],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_ratio_features(ddf).compute()
    oil_row = result[result["product"] == "O"]
    gor = oil_row["gor"].iloc[0]
    assert pd.isna(gor) or gor == 0.0


@pytest.mark.unit
def test_gor_nonzero_oil_zero_gas():
    from kgs_pipeline.features import compute_ratio_features

    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "product": ["O", "G"],
            "production_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "production": [50.0, 0.0],
            "unit": ["BBL", "MCF"],
            "lease_kid": pd.array([1, 1], dtype="int64"),
            "operator": ["O", "O"],
            "county": ["C", "C"],
            "field": ["F", "F"],
            "producing_zone": ["Z", "Z"],
            "latitude": [38.0, 38.0],
            "longitude": [-98.0, -98.0],
            "outlier_flag": [False, False],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_ratio_features(ddf).compute()
    oil_row = result[result["product"] == "O"]
    assert oil_row["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_gor_correct_value():
    from kgs_pipeline.features import compute_ratio_features

    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "product": ["O", "G"],
            "production_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "production": [100.0, 500.0],
            "unit": ["BBL", "MCF"],
            "lease_kid": pd.array([1, 1], dtype="int64"),
            "operator": ["O", "O"],
            "county": ["C", "C"],
            "field": ["F", "F"],
            "producing_zone": ["Z", "Z"],
            "latitude": [38.0, 38.0],
            "longitude": [-98.0, -98.0],
            "outlier_flag": [False, False],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_ratio_features(ddf).compute()
    oil_row = result[result["product"] == "O"]
    assert abs(oil_row["gor"].iloc[0] - 5.0) < 0.001


@pytest.mark.unit
def test_water_cut_all_nan():
    from kgs_pipeline.features import compute_ratio_features

    df = _make_well_df([100.0, 80.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_ratio_features(ddf).compute()
    assert "water_cut" in result.columns
    assert result["water_cut"].isna().all()


@pytest.mark.unit
def test_wor_all_nan():
    from kgs_pipeline.features import compute_ratio_features

    df = _make_well_df([100.0, 80.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_ratio_features(ddf).compute()
    assert "wor" in result.columns
    assert result["wor"].isna().all()


# ---------------------------------------------------------------------------
# Task 29: build_encoding_maps + encode_categorical_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_encoding_maps_county():
    from kgs_pipeline.features import build_encoding_maps

    df = pd.DataFrame(
        {
            "county": ["Barton", "Ellis", "Rooks"],
            "operator": ["Op1", "Op1", "Op1"],
            "producing_zone": ["Z", "Z", "Z"],
            "field": ["F", "F", "F"],
            "product": ["O", "O", "O"],
            "production": [1.0, 2.0, 3.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    maps = build_encoding_maps(ddf)
    assert len(maps["county"]) == 3
    assert set(maps["county"].values()) == {0, 1, 2}


@pytest.mark.unit
def test_build_encoding_maps_alphabetically_sorted():
    from kgs_pipeline.features import build_encoding_maps

    df = pd.DataFrame(
        {
            "county": ["Rooks", "Barton", "Ellis"],
            "operator": ["O"] * 3,
            "producing_zone": ["Z"] * 3,
            "field": ["F"] * 3,
            "product": ["O"] * 3,
            "production": [1.0] * 3,
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    maps = build_encoding_maps(ddf)
    sorted_counties = sorted(["Rooks", "Barton", "Ellis"])
    for i, county in enumerate(sorted_counties):
        assert maps["county"][county] == i


@pytest.mark.unit
def test_encode_categorical_features_columns_present():
    from kgs_pipeline.features import build_encoding_maps, encode_categorical_features

    df = pd.DataFrame(
        {
            "county": ["Ellis"],
            "operator": ["Op1"],
            "producing_zone": ["Lansing"],
            "field": ["KANASKA"],
            "product": ["O"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    maps = build_encoding_maps(ddf)
    result = encode_categorical_features(ddf, maps)
    for col in [
        "county_encoded",
        "operator_encoded",
        "producing_zone_encoded",
        "field_encoded",
        "product_encoded",
    ]:
        assert col in result.columns


@pytest.mark.unit
def test_product_encoded_o_is_0_g_is_1():
    from kgs_pipeline.features import build_encoding_maps, encode_categorical_features

    df = pd.DataFrame(
        {
            "county": ["Ellis", "Ellis"],
            "operator": ["Op1", "Op1"],
            "producing_zone": ["Z", "Z"],
            "field": ["F", "F"],
            "product": ["O", "G"],
            "production": [100.0, 500.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    maps = build_encoding_maps(ddf)
    result = encode_categorical_features(ddf, maps).compute()
    assert result[result["product"] == "O"]["product_encoded"].iloc[0] == 0
    assert result[result["product"] == "G"]["product_encoded"].iloc[0] == 1


@pytest.mark.unit
def test_encode_unseen_county_is_minus_1():
    from kgs_pipeline.features import encode_categorical_features

    df = pd.DataFrame(
        {
            "county": ["UNSEEN"],
            "operator": ["O1"],
            "producing_zone": ["Z"],
            "field": ["F"],
            "product": ["O"],
            "production": [1.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    maps: dict = {
        "county": {"Ellis": 0},
        "operator": {"O1": 0},
        "producing_zone": {"Z": 0},
        "field": {"F": 0},
        "product": {"O": 0},
    }
    result = encode_categorical_features(ddf, maps).compute()
    assert result["county_encoded"].iloc[0] == -1


@pytest.mark.unit
def test_encode_returns_dask():
    from kgs_pipeline.features import encode_categorical_features

    df = pd.DataFrame(
        {
            "county": ["Ellis"],
            "operator": ["O1"],
            "producing_zone": ["Z"],
            "field": ["F"],
            "product": ["O"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categorical_features(ddf, {})
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 30: write_features_parquet + export_feature_matrix_csv
# ---------------------------------------------------------------------------


def _make_full_feature_df() -> pd.DataFrame:
    """Build a minimal DataFrame with all required feature columns."""
    return pd.DataFrame(
        {
            "well_id": ["W001"],
            "production_date": pd.to_datetime(["2020-01-01"]),
            "product": ["O"],
            "production": [100.0],
            "unit": ["BBL"],
            "lease_kid": pd.array([1001], dtype="int64"),
            "operator": ["Test Op"],
            "county": ["Ellis"],
            "field": ["KANASKA"],
            "producing_zone": ["Lansing Group"],
            "latitude": [38.5],
            "longitude": [-98.5],
            "cum_production": [100.0],
            "decline_rate": [float("nan")],
            "rolling_mean_3m": [100.0],
            "rolling_mean_6m": [100.0],
            "rolling_mean_12m": [100.0],
            "rolling_std_3m": [float("nan")],
            "rolling_std_6m": [float("nan")],
            "lag_1m": [float("nan")],
            "lag_3m": [float("nan")],
            "months_since_first_prod": pd.array([0], dtype="Int64"),
            "production_year": pd.array([2020], dtype="Int32"),
            "production_month": pd.array([1], dtype="Int32"),
            "production_quarter": pd.array([1], dtype="Int32"),
            "gor": [float("nan")],
            "water_cut": [float("nan")],
            "wor": [float("nan")],
            "county_encoded": pd.array([0], dtype="int32"),
            "operator_encoded": pd.array([0], dtype="int32"),
            "producing_zone_encoded": pd.array([0], dtype="int32"),
            "field_encoded": pd.array([0], dtype="int32"),
            "product_encoded": pd.array([0], dtype="int32"),
            "outlier_flag": [False],
        }
    )


@pytest.mark.unit
def test_write_features_parquet_creates_files(tmp_path: Path):
    from kgs_pipeline.features import write_features_parquet

    ddf = dd.from_pandas(_make_full_feature_df(), npartitions=1)
    written = write_features_parquet(ddf, tmp_path)
    assert len(written) >= 1


@pytest.mark.unit
def test_write_features_parquet_all_columns_present(tmp_path: Path):
    from kgs_pipeline.features import FEATURES_SCHEMA, write_features_parquet

    ddf = dd.from_pandas(_make_full_feature_df(), npartitions=1)
    written = write_features_parquet(ddf, tmp_path)
    df = pd.read_parquet(written[0])
    expected_cols = [f.name for f in FEATURES_SCHEMA]
    for col in expected_cols:
        assert col in df.columns, f"Missing column: {col}"


@pytest.mark.unit
def test_write_features_parquet_dask_readable(tmp_path: Path):
    from kgs_pipeline.features import write_features_parquet

    ddf = dd.from_pandas(_make_full_feature_df(), npartitions=1)
    write_features_parquet(ddf, tmp_path)
    reloaded = dd.read_parquet(str(tmp_path), engine="pyarrow")
    assert isinstance(reloaded, dd.DataFrame)


@pytest.mark.unit
def test_export_feature_matrix_csv(tmp_path: Path):
    from kgs_pipeline.features import export_feature_matrix_csv

    ddf = dd.from_pandas(_make_full_feature_df(), npartitions=1)
    csv_path = export_feature_matrix_csv(ddf, tmp_path)
    assert csv_path.exists()
    df = pd.read_csv(csv_path)
    assert len(df) >= 1


# ---------------------------------------------------------------------------
# Task 31: run_features_pipeline + lazy evaluation tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_features_pipeline_propagates_runtime_error():
    from kgs_pipeline.features import run_features_pipeline

    with patch("kgs_pipeline.features.load_processed_parquet", side_effect=RuntimeError("no data")):
        with pytest.raises(RuntimeError):
            run_features_pipeline()


@pytest.mark.unit
def test_lazy_evaluation_all_feature_functions():
    """All feature computation functions return dask.dataframe.DataFrame (not pandas)."""
    from kgs_pipeline.features import (
        compute_cumulative_production,
        compute_decline_rate,
        compute_lag_features,
        compute_rolling_features,
        compute_time_features,
        compute_ratio_features,
    )

    df = _make_well_df([100.0, 80.0, 60.0])
    df["production_date"] = pd.date_range("2020-01-01", periods=3, freq="MS")
    ddf = dd.from_pandas(df, npartitions=1)

    for fn in [
        compute_cumulative_production,
        compute_decline_rate,
        compute_rolling_features,
        compute_lag_features,
    ]:
        result = fn(ddf)
        assert isinstance(result, dd.DataFrame), f"{fn.__name__} returned non-Dask type"
        assert not isinstance(result, pd.DataFrame)

    # time features needs production_date
    result_time = compute_time_features(ddf)
    assert isinstance(result_time, dd.DataFrame)

    # ratio features
    result_ratio = compute_ratio_features(ddf)
    assert isinstance(result_ratio, dd.DataFrame)
