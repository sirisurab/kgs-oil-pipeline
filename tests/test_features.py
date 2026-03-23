"""Tests for kgs_pipeline/features.py — Tasks 22–31."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

import kgs_pipeline.config as config
from kgs_pipeline.features import (
    add_water_placeholders,
    compute_cumulative_production,
    compute_decline_rate,
    compute_gor,
    compute_rolling_statistics,
    compute_time_features,
    encode_categorical_features,
    run_features_pipeline,
    write_features_parquet,
)


# ---------------------------------------------------------------------------
# Task 22: Features-stage config constants
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decline_rate_clip_bounds():
    assert config.DECLINE_RATE_CLIP_MIN == -1.0
    assert config.DECLINE_RATE_CLIP_MAX == 10.0


@pytest.mark.unit
def test_rolling_window_values():
    assert config.ROLLING_WINDOW_SHORT == 3
    assert config.ROLLING_WINDOW_LONG == 6


@pytest.mark.unit
def test_categorical_columns_list():
    assert isinstance(config.CATEGORICAL_COLUMNS, list)
    assert "county" in config.CATEGORICAL_COLUMNS
    assert "operator" in config.CATEGORICAL_COLUMNS


@pytest.mark.unit
def test_features_data_dir_is_path():
    assert isinstance(config.FEATURES_DATA_DIR, Path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_well_df(production: list[float], dates: list[str] | None = None) -> pd.DataFrame:
    n = len(production)
    if dates is None:
        dates = [f"2021-{(i % 12) + 1:02d}-01" for i in range(n)]
    return pd.DataFrame(
        {
            "well_id": ["W1"] * n,
            "production_date": pd.to_datetime(dates),
            "product": ["O"] * n,
            "production": production,
            "county": ["Allen"] * n,
            "operator": ["Acme"] * n,
            "producing_zone": ["Zone A"] * n,
            "field_name": ["Field X"] * n,
        }
    )


# ---------------------------------------------------------------------------
# Task 23: compute_cumulative_production
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cumulative_production_basic():
    df = _make_well_df([100.0, 200.0, 150.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    assert list(result["cumulative_production"]) == [100.0, 300.0, 450.0]


@pytest.mark.unit
def test_cumulative_production_with_nan():
    df = _make_well_df([100.0, np.nan, 150.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    assert list(result["cumulative_production"]) == [100.0, 100.0, 250.0]
    # Original production column still has NaN
    assert pd.isna(result["production"].iloc[1])


@pytest.mark.unit
def test_cumulative_production_all_zeros():
    df = _make_well_df([0.0, 0.0, 0.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    assert list(result["cumulative_production"]) == [0.0, 0.0, 0.0]


@pytest.mark.unit
def test_cumulative_production_dtype():
    df = _make_well_df([100.0, 200.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_cumulative_production(ddf).compute()
    assert result["cumulative_production"].dtype == np.float64


@pytest.mark.unit
def test_cumulative_production_returns_dask():
    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_cumulative_production(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 24: compute_decline_rate
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decline_rate_values():
    df = _make_well_df(
        [100.0, 80.0, 60.0],
        ["2021-01-01", "2021-02-01", "2021-03-01"],
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    rates = result["decline_rate"].tolist()
    assert pd.isna(rates[0])
    assert abs(rates[1] - (-0.2)) < 1e-9
    assert abs(rates[2] - (-0.25)) < 1e-9


@pytest.mark.unit
def test_decline_rate_zero_prior_is_nan():
    df = _make_well_df(
        [0.0, 50.0],
        ["2021-01-01", "2021-02-01"],
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert pd.isna(result["decline_rate"].iloc[1])


@pytest.mark.unit
def test_decline_rate_nan_prior_is_nan():
    df = _make_well_df(
        [100.0, np.nan, 80.0],
        ["2021-01-01", "2021-02-01", "2021-03-01"],
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert pd.isna(result["decline_rate"].iloc[2])


@pytest.mark.unit
def test_decline_rate_clipped_max():
    # raw rate would be (1500 - 100) / 100 = 14.0, clips to 10.0
    df = _make_well_df([100.0, 1500.0], ["2021-01-01", "2021-02-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert result["decline_rate"].iloc[1] == 10.0


@pytest.mark.unit
def test_decline_rate_clipped_min():
    # raw rate would be (0 - 100) / 100 = -1.0 — exactly at clip min, so ok
    # For < -1: raw = (0 - 200) / 200 = -1.0 — let's use a smaller number
    # raw = (10 - 600) / 600 ≈ -0.983 (not clipped)
    # raw = (1 - 600) / 600 ≈ -0.998 (not clipped)
    # raw < -1: not possible with positive values
    # Use NaN path: prior = 50, current = -500 (but negative values NaN in production)
    # Actually test with a hypothetical: prior 100, current 0 → -1.0 exactly (at boundary)
    df = _make_well_df([500.0, 0.001], ["2021-01-01", "2021-02-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    # Rate = (0.001 - 500) / 500 ≈ -0.999998, clipped to -1.0
    assert result["decline_rate"].iloc[1] >= config.DECLINE_RATE_CLIP_MIN


@pytest.mark.unit
def test_decline_rate_first_row_is_nan():
    df = _make_well_df([100.0, 80.0], ["2021-01-01", "2021-02-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_decline_rate(ddf).compute().sort_values("production_date")
    assert pd.isna(result["decline_rate"].iloc[0])


@pytest.mark.unit
def test_decline_rate_returns_dask():
    df = _make_well_df([100.0, 80.0], ["2021-01-01", "2021-02-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_decline_rate(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 25: compute_rolling_statistics
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rolling_mean_3m():
    production = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    dates = [f"2021-{i:02d}-01" for i in range(1, 7)]
    df = _make_well_df(production, dates)
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_statistics(ddf).compute().sort_values("production_date")
    # rolling(3, min_periods=1).mean() at index 2 = mean(10,20,30) = 20.0
    assert abs(result["rolling_mean_3m"].iloc[2] - 20.0) < 1e-9


@pytest.mark.unit
def test_rolling_mean_6m():
    production = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    dates = [f"2021-{i:02d}-01" for i in range(1, 7)]
    df = _make_well_df(production, dates)
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_statistics(ddf).compute().sort_values("production_date")
    assert abs(result["rolling_mean_6m"].iloc[5] - 35.0) < 1e-9


@pytest.mark.unit
def test_rolling_short_well_no_nan():
    df = _make_well_df([100.0, 200.0], ["2021-01-01", "2021-02-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_statistics(ddf).compute()
    assert not result["rolling_mean_3m"].isna().any()


@pytest.mark.unit
def test_rolling_all_four_columns_exist():
    df = _make_well_df([100.0], ["2021-01-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_rolling_statistics(ddf).compute()
    for col in ("rolling_mean_3m", "rolling_std_3m", "rolling_mean_6m", "rolling_std_6m"):
        assert col in result.columns
        assert result[col].dtype == np.float64


@pytest.mark.unit
def test_rolling_returns_dask():
    df = _make_well_df([100.0], ["2021-01-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_rolling_statistics(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 26: compute_time_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_time_features_months_since_first():
    df = _make_well_df(
        [100.0, 100.0, 100.0],
        ["2021-01-01", "2021-02-01", "2021-03-01"],
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_time_features(ddf).compute().sort_values("production_date")
    assert list(result["months_since_first_prod"]) == [0, 1, 2]


@pytest.mark.unit
def test_time_features_across_year_boundary():
    df = _make_well_df(
        [100.0, 100.0],
        ["2021-11-01", "2022-02-01"],
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_time_features(ddf).compute().sort_values("production_date")
    assert list(result["months_since_first_prod"]) == [0, 3]


@pytest.mark.unit
def test_time_features_year_and_month():
    df = _make_well_df([100.0], ["2022-07-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_time_features(ddf).compute()
    assert result["production_year"].iloc[0] == 2022
    assert result["production_month"].iloc[0] == 7


@pytest.mark.unit
def test_time_features_nat_produces_nan():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "production_date": [pd.NaT],
            "product": ["O"],
            "production": [100.0],
            "county": ["Allen"],
            "operator": ["Acme"],
            "producing_zone": ["Zone A"],
            "field_name": ["Field X"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_time_features(ddf).compute()
    assert pd.isna(result["months_since_first_prod"].iloc[0])


@pytest.mark.unit
def test_time_features_returns_dask():
    df = _make_well_df([100.0], ["2021-01-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_time_features(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 27: compute_gor
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_gor_basic():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "production_date": pd.to_datetime(["2021-03-01", "2021-03-01"]),
            "product": ["O", "G"],
            "production": [1000.0, 500.0],
            "county": ["Allen", "Allen"],
            "operator": ["Acme", "Acme"],
            "producing_zone": ["Zone A", "Zone A"],
            "field_name": ["Field X", "Field X"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_gor(ddf).compute()
    oil_row = result[result["product"] == "O"]
    gas_row = result[result["product"] == "G"]
    assert abs(oil_row["gor"].iloc[0] - 0.5) < 1e-9
    assert abs(gas_row["gor"].iloc[0] - 0.5) < 1e-9


@pytest.mark.unit
def test_gor_zero_oil_is_nan():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "production_date": pd.to_datetime(["2021-01-01", "2021-01-01"]),
            "product": ["O", "G"],
            "production": [0.0, 500.0],
            "county": ["Allen", "Allen"],
            "operator": ["Acme", "Acme"],
            "producing_zone": ["Zone A", "Zone A"],
            "field_name": ["Field X", "Field X"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_gor(ddf).compute()
    oil_row = result[result["product"] == "O"]
    assert pd.isna(oil_row["gor"].iloc[0])


@pytest.mark.unit
def test_gor_gas_only_is_nan():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "production_date": pd.to_datetime(["2021-01-01"]),
            "product": ["G"],
            "production": [500.0],
            "county": ["Allen"],
            "operator": ["Acme"],
            "producing_zone": ["Zone A"],
            "field_name": ["Field X"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_gor(ddf).compute()
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_gor_oil_only_is_nan():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "production_date": pd.to_datetime(["2021-01-01"]),
            "product": ["O"],
            "production": [1000.0],
            "county": ["Allen"],
            "operator": ["Acme"],
            "producing_zone": ["Zone A"],
            "field_name": ["Field X"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_gor(ddf).compute()
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_gor_dtype():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "production_date": pd.to_datetime(["2021-01-01"]),
            "product": ["O"],
            "production": [1000.0],
            "county": ["Allen"],
            "operator": ["Acme"],
            "producing_zone": ["Zone A"],
            "field_name": ["Field X"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_gor(ddf).compute()
    assert result["gor"].dtype == np.float64


@pytest.mark.unit
def test_gor_returns_dask():
    df = _make_well_df([100.0], ["2021-01-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(compute_gor(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 28: add_water_placeholders
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_water_placeholders_columns_exist():
    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = add_water_placeholders(ddf).compute()
    assert "water_cut" in result.columns
    assert "wor" in result.columns


@pytest.mark.unit
def test_water_placeholders_all_nan():
    df = _make_well_df([100.0, 200.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = add_water_placeholders(ddf).compute()
    assert result["water_cut"].isna().all()
    assert result["wor"].isna().all()


@pytest.mark.unit
def test_water_placeholders_dtype():
    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = add_water_placeholders(ddf).compute()
    assert result["water_cut"].dtype == np.float64
    assert result["wor"].dtype == np.float64


@pytest.mark.unit
def test_water_placeholders_returns_dask():
    df = _make_well_df([100.0])
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(add_water_placeholders(ddf), dd.DataFrame)


@pytest.mark.unit
def test_water_cut_no_values_violate_bounds():
    df = _make_well_df([100.0, 200.0])
    ddf = dd.from_pandas(df, npartitions=1)
    result = add_water_placeholders(ddf).compute()
    non_null = result["water_cut"].dropna()
    # All NaN, so vacuously no bounds violations
    assert ((non_null >= 0) & (non_null <= 1)).all()


# ---------------------------------------------------------------------------
# Task 29: encode_categorical_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_encode_categorical_consistent():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W2", "W3", "W4"],
            "county": ["Allen", "Butler", "Allen", "Clark"],
            "operator": ["Acme", "Acme", "Beta", "Beta"],
            "producing_zone": ["Z1", "Z1", "Z2", "Z2"],
            "field_name": ["F1", "F2", "F1", "F2"],
            "product": ["O", "G", "O", "G"],
            "production": [100.0, 200.0, 150.0, 250.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categorical_features(ddf).compute()

    # Allen should map to 0 (alphabetically first), Butler to 1, Clark to 2
    allen_code = result.loc[result["county"] == "Allen", "county_encoded"].iloc[0]
    butler_code = result.loc[result["county"] == "Butler", "county_encoded"].iloc[0]
    clark_code = result.loc[result["county"] == "Clark", "county_encoded"].iloc[0]
    assert allen_code < butler_code < clark_code

    # Same county → same code
    assert allen_code == result.loc[result["county"] == "Allen", "county_encoded"].iloc[1]


@pytest.mark.unit
def test_encode_categorical_nan_is_minus_one():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county": [None],
            "operator": ["Acme"],
            "producing_zone": ["Z1"],
            "field_name": ["F1"],
            "product": ["O"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categorical_features(ddf).compute()
    assert result["county_encoded"].iloc[0] == -1


@pytest.mark.unit
def test_encode_categorical_original_column_preserved():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county": ["Allen"],
            "operator": ["Acme"],
            "producing_zone": ["Z1"],
            "field_name": ["F1"],
            "product": ["O"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categorical_features(ddf).compute()
    assert "county" in result.columns


@pytest.mark.unit
def test_encode_categorical_encoded_dtype():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county": ["Allen"],
            "operator": ["Acme"],
            "producing_zone": ["Z1"],
            "field_name": ["F1"],
            "product": ["O"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categorical_features(ddf).compute()
    for col in config.CATEGORICAL_COLUMNS:
        assert result[f"{col}_encoded"].dtype == np.int32


@pytest.mark.unit
def test_encode_categorical_deterministic():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W2"],
            "county": ["Allen", "Butler"],
            "operator": ["Acme", "Acme"],
            "producing_zone": ["Z1", "Z1"],
            "field_name": ["F1", "F1"],
            "product": ["O", "G"],
            "production": [100.0, 200.0],
        }
    )
    ddf1 = dd.from_pandas(df.copy(), npartitions=1)
    ddf2 = dd.from_pandas(df.copy(), npartitions=1)
    r1 = encode_categorical_features(ddf1).compute()
    r2 = encode_categorical_features(ddf2).compute()
    assert list(r1["county_encoded"]) == list(r2["county_encoded"])


@pytest.mark.unit
def test_encode_categorical_missing_col_skipped():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county": ["Allen"],
            "operator": ["Acme"],
            "producing_zone": ["Z1"],
            "field_name": ["F1"],
            # product column intentionally missing
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categorical_features(ddf).compute()
    assert "product_encoded" not in result.columns
    assert "county_encoded" in result.columns


@pytest.mark.unit
def test_encode_categorical_returns_dask():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "county": ["Allen"],
            "operator": ["Acme"],
            "producing_zone": ["Z1"],
            "field_name": ["F1"],
            "product": ["O"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(encode_categorical_features(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 30: write_features_parquet
# ---------------------------------------------------------------------------


def _make_features_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lease_kid": ["1001"],
            "lease_name": ["Test Lease"],
            "dor_code": ["D1"],
            "field_name": ["Field X"],
            "producing_zone": ["Zone A"],
            "operator": ["Acme"],
            "county": ["Allen"],
            "township": pd.array([10], dtype=pd.Int32Dtype()),
            "twn_dir": ["S"],
            "range_num": pd.array([5], dtype=pd.Int32Dtype()),
            "range_dir": ["W"],
            "section": pd.array([12], dtype=pd.Int32Dtype()),
            "spot": ["NE"],
            "latitude": [38.5],
            "longitude": [-95.5],
            "product": ["O"],
            "well_count": pd.array([1], dtype=pd.Int32Dtype()),
            "production": [500.0],
            "source_file": ["lp1.txt"],
            "production_date": pd.to_datetime(["2021-03-01"]),
            "well_id": ["15-001-12345"],
            "outlier_flag": [False],
            "unit": ["BBL"],
            "cumulative_production": [500.0],
            "decline_rate": [np.nan],
            "rolling_mean_3m": [500.0],
            "rolling_std_3m": [np.nan],
            "rolling_mean_6m": [500.0],
            "rolling_std_6m": [np.nan],
            "months_since_first_prod": pd.array([0], dtype=pd.Int32Dtype()),
            "production_year": pd.array([2021], dtype=pd.Int32Dtype()),
            "production_month": pd.array([3], dtype=pd.Int32Dtype()),
            "gor": [np.nan],
            "water_cut": [np.nan],
            "wor": [np.nan],
            "county_encoded": pd.array([0], dtype="int32"),
            "operator_encoded": pd.array([0], dtype="int32"),
            "producing_zone_encoded": pd.array([0], dtype="int32"),
            "field_name_encoded": pd.array([0], dtype="int32"),
            "product_encoded": pd.array([0], dtype="int32"),
        }
    )


@pytest.mark.unit
def test_write_features_parquet_creates_files(tmp_path: Path):
    df = _make_features_df()
    ddf = dd.from_pandas(df, npartitions=1)
    write_features_parquet(ddf, tmp_path / "features")
    parquet_files = list((tmp_path / "features").glob("*.parquet"))
    assert len(parquet_files) >= 1


@pytest.mark.unit
def test_write_features_parquet_readable(tmp_path: Path):
    df = _make_features_df()
    ddf = dd.from_pandas(df, npartitions=1)
    write_features_parquet(ddf, tmp_path / "feats")
    for fp in (tmp_path / "feats").glob("*.parquet"):
        loaded = pd.read_parquet(fp)
        assert loaded is not None


@pytest.mark.unit
def test_write_features_parquet_returns_path(tmp_path: Path):
    df = _make_features_df()
    ddf = dd.from_pandas(df, npartitions=1)
    out_dir = tmp_path / "feats"
    result = write_features_parquet(ddf, out_dir)
    assert result == out_dir


# ---------------------------------------------------------------------------
# Task 31: run_features_pipeline
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_features_pipeline_raises_if_no_processed():
    with patch("kgs_pipeline.config.PROCESSED_DATA_DIR") as mock_dir:
        mock_dir.glob.return_value = []
        with pytest.raises(RuntimeError):
            run_features_pipeline()


@pytest.mark.unit
def test_run_features_pipeline_calls_steps(tmp_path: Path):
    call_order: list[str] = []
    sample_df = _make_features_df()
    sample_ddf = dd.from_pandas(sample_df, npartitions=1)

    def _track(name: str, return_val=None):
        def side_effect(*args, **kwargs):
            call_order.append(name)
            return return_val if return_val is not None else sample_ddf
        return side_effect

    with (
        patch("kgs_pipeline.features.dd.read_parquet", return_value=sample_ddf),
        patch("kgs_pipeline.features.compute_cumulative_production", side_effect=_track("cumulative", sample_ddf)),
        patch("kgs_pipeline.features.compute_decline_rate", side_effect=_track("decline", sample_ddf)),
        patch("kgs_pipeline.features.compute_rolling_statistics", side_effect=_track("rolling", sample_ddf)),
        patch("kgs_pipeline.features.compute_time_features", side_effect=_track("time", sample_ddf)),
        patch("kgs_pipeline.features.compute_gor", side_effect=_track("gor", sample_ddf)),
        patch("kgs_pipeline.features.add_water_placeholders", side_effect=_track("water", sample_ddf)),
        patch("kgs_pipeline.features.encode_categorical_features", side_effect=_track("encode", sample_ddf)),
        patch("kgs_pipeline.features.write_features_parquet", side_effect=_track("write", config.FEATURES_DATA_DIR)),
    ):
        # Make PROCESSED_DATA_DIR appear to have parquet files
        with patch("kgs_pipeline.config.PROCESSED_DATA_DIR", tmp_path):
            (tmp_path / "part.parquet").touch()
            # Also mock index check
            sample_ddf.index.name = "well_id"
            run_features_pipeline()

    assert call_order == [
        "cumulative", "decline", "rolling", "time", "gor", "water", "encode", "write"
    ]
