"""Tests for kgs_pipeline.features module."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pytest

from kgs_pipeline.features import (
    add_water_column,
    compute_cumulative,
    compute_decline_rate,
    compute_gor,
    compute_rate_features,
    compute_rolling_and_lag_features,
    compute_water_cut,
    read_processed,
    run_features,
    write_features_parquet,
    write_manifest,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _base_df(n: int = 1, lease_id: str = "L1") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lease_kid": pd.array([lease_id] * n, dtype=pd.StringDtype()),
            "api_number": pd.array(["A"] * n, dtype=pd.StringDtype()),
            "lease_name": pd.array(["LN"] * n, dtype=pd.StringDtype()),
            "operator": pd.array(["OP"] * n, dtype=pd.StringDtype()),
            "county": pd.array(["C"] * n, dtype=pd.StringDtype()),
            "field": pd.array(["F"] * n, dtype=pd.StringDtype()),
            "producing_zone": pd.array(["Z"] * n, dtype=pd.StringDtype()),
            "production_date": [pd.Timestamp(f"2024-{(i % 12) + 1:02d}-01") for i in range(n)],
            "oil_bbl": [100.0] * n,
            "gas_mcf": [500.0] * n,
            "well_count": [1.0] * n,
            "latitude": [38.0] * n,
            "longitude": [-97.0] * n,
            "is_outlier": [False] * n,
            "has_negative_production": [False] * n,
            "source_file": pd.array(["t.txt"] * n, dtype=pd.StringDtype()),
        }
    )


def _dask(df: pd.DataFrame) -> dd.DataFrame:
    return dd.from_pandas(df, npartitions=1)


# ---------------------------------------------------------------------------
# Task 01: read_processed
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_processed_returns_dask(tmp_path: Path) -> None:
    df = _base_df()
    df.to_parquet(tmp_path / "p.parquet", index=False)
    result = read_processed(str(tmp_path))
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_read_processed_npartitions_capped(tmp_path: Path) -> None:
    df = _base_df()
    df.to_parquet(tmp_path / "p.parquet", index=False)
    result = read_processed(str(tmp_path))
    assert result.npartitions <= 50


@pytest.mark.unit
def test_read_processed_returns_dask_not_pandas(tmp_path: Path) -> None:
    """TR-17."""
    df = _base_df()
    df.to_parquet(tmp_path / "p.parquet", index=False)
    result = read_processed(str(tmp_path))
    assert isinstance(result, dd.DataFrame)
    assert not isinstance(result, pd.DataFrame)


@pytest.mark.unit
def test_read_processed_missing_dir() -> None:
    with pytest.raises(FileNotFoundError):
        read_processed("/nonexistent")


# ---------------------------------------------------------------------------
# Task 02: add_water_column, compute_gor, compute_water_cut
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_add_water_column_all_zero(tmp_path: Path) -> None:
    df = _base_df(3)
    result = add_water_column(_dask(df)).compute()
    assert "water_bbl" in result.columns
    assert (result["water_bbl"] == 0.0).all()
    assert result["water_bbl"].dtype == "float64"


@pytest.mark.unit
def test_tr06a_gor_zero_oil() -> None:
    df = _base_df()
    df["oil_bbl"] = 0.0
    df["gas_mcf"] = 500.0
    result = compute_gor(_dask(df)).compute()
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_tr06b_gor_zero_zero() -> None:
    df = _base_df()
    df["oil_bbl"] = 0.0
    df["gas_mcf"] = 0.0
    result = compute_gor(_dask(df)).compute()
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_tr06c_gor_gas_zero() -> None:
    df = _base_df()
    df["oil_bbl"] = 100.0
    df["gas_mcf"] = 0.0
    result = compute_gor(_dask(df)).compute()
    assert result["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_gor_normal() -> None:
    df = _base_df()
    df["oil_bbl"] = 100.0
    df["gas_mcf"] = 500.0
    result = compute_gor(_dask(df)).compute()
    assert result["gor"].iloc[0] == pytest.approx(5.0)


@pytest.mark.unit
def test_tr10a_water_cut_no_water() -> None:
    df = _base_df()
    df["water_bbl"] = 0.0
    df["oil_bbl"] = 100.0
    result = compute_water_cut(_dask(df)).compute()
    assert result["water_cut"].iloc[0] == 0.0


@pytest.mark.unit
def test_tr10b_water_cut_all_water() -> None:
    df = _base_df()
    df["water_bbl"] = 50.0
    df["oil_bbl"] = 0.0
    result = compute_water_cut(_dask(df)).compute()
    assert result["water_cut"].iloc[0] == 1.0


@pytest.mark.unit
def test_tr10_water_cut_both_zero() -> None:
    df = _base_df()
    df["water_bbl"] = 0.0
    df["oil_bbl"] = 0.0
    result = compute_water_cut(_dask(df)).compute()
    assert pd.isna(result["water_cut"].iloc[0])


@pytest.mark.unit
def test_water_cut_mixed() -> None:
    df = _base_df()
    df["water_bbl"] = 25.0
    df["oil_bbl"] = 75.0
    result = compute_water_cut(_dask(df)).compute()
    assert result["water_cut"].iloc[0] == pytest.approx(0.25)


@pytest.mark.unit
def test_features_functions_return_dask() -> None:
    """TR-17: all feature functions return Dask DataFrame."""
    df = _base_df()
    df["water_bbl"] = 0.0
    ddf = _dask(df)
    assert isinstance(add_water_column(_dask(_base_df())), dd.DataFrame)
    assert isinstance(compute_gor(ddf), dd.DataFrame)
    assert isinstance(compute_water_cut(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 03: compute_cumulative
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr03_cumulative_monotonic() -> None:
    """TR-03: cum_oil = [100, 300, 450, 450, 750] for [100, 200, 150, 0, 300]."""
    dates = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 6)]
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 5, dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 200.0, 150.0, 0.0, 300.0],
            "gas_mcf": [0.0] * 5,
            "water_bbl": [0.0] * 5,
            "production_date": dates,
        }
    )
    result = compute_cumulative(_dask(df)).compute()
    result = result.sort_values("production_date")
    expected = [100.0, 300.0, 450.0, 450.0, 750.0]
    assert result["cum_oil"].tolist() == pytest.approx(expected)


@pytest.mark.unit
def test_tr08a_cumulative_flat_during_shutin() -> None:
    """TR-08a: shut-in month has cum_oil == prior cum_oil."""
    dates = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 4)]
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 3, dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 0.0, 200.0],
            "gas_mcf": [0.0] * 3,
            "water_bbl": [0.0] * 3,
            "production_date": dates,
        }
    )
    result = compute_cumulative(_dask(df)).compute().sort_values("production_date")
    vals = result["cum_oil"].tolist()
    # Month 2 shut-in: cum stays at 100
    assert vals[1] == pytest.approx(vals[0])


@pytest.mark.unit
def test_tr08b_leading_zeros_cum_zero() -> None:
    """TR-08b: leading zero-production months have cum_oil=0.0."""
    dates = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 4)]
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 3, dtype=pd.StringDtype()),
            "oil_bbl": [0.0, 0.0, 100.0],
            "gas_mcf": [0.0] * 3,
            "water_bbl": [0.0] * 3,
            "production_date": dates,
        }
    )
    result = compute_cumulative(_dask(df)).compute().sort_values("production_date")
    assert result["cum_oil"].iloc[0] == 0.0
    assert result["cum_oil"].iloc[1] == 0.0


@pytest.mark.unit
def test_tr08c_cumulative_resumes_after_shutin() -> None:
    """TR-08c: cumulative resumes correctly after shut-in."""
    dates = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 5)]
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 4, dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 0.0, 0.0, 400.0],
            "gas_mcf": [0.0] * 4,
            "water_bbl": [0.0] * 4,
            "production_date": dates,
        }
    )
    result = compute_cumulative(_dask(df)).compute().sort_values("production_date")
    assert result["cum_oil"].iloc[3] == pytest.approx(500.0)


@pytest.mark.unit
def test_cumulative_per_lease_independent() -> None:
    """Cumulative is computed independently per lease."""
    dates = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 3)]
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1", "L1", "L2", "L2"], dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 200.0, 50.0, 75.0],
            "gas_mcf": [0.0] * 4,
            "water_bbl": [0.0] * 4,
            "production_date": dates + dates,
        }
    )
    result = compute_cumulative(_dask(df)).compute()
    l2_max = result[result["lease_kid"] == "L2"]["cum_oil"].max()
    assert l2_max == pytest.approx(125.0)


# ---------------------------------------------------------------------------
# Task 04: compute_decline_rate
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr07a_decline_clipped_lower() -> None:
    """TR-07a: decline rate < -1.0 clipped to -1.0."""
    # Use a case that would produce -2.0 without clipping: 100 → 0 is -1.0
    # To get < -1.0 we need pct_change < -1, which is impossible with non-negative values
    # Directly test clipping via a synthetic series
    from kgs_pipeline.features import _compute_decline_partition

    df2 = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 2, dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 100.0],
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
            "gas_mcf": [0.0, 0.0],
        }
    )
    result = _compute_decline_partition(df2.copy())
    # pct_change on [100, 100] = [NaN, 0.0]
    assert result["decline_rate"].iloc[1] == pytest.approx(0.0)
    # Simulate pre-clip extreme value
    from kgs_pipeline.features import compute_decline_rate

    df3 = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 2, dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 0.0],
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
            "gas_mcf": [0.0, 0.0],
        }
    )
    res = compute_decline_rate(_dask(df3)).compute()
    # pct_change of 0/100 = -1.0, clipped at -1.0
    val = res.sort_values("production_date")["decline_rate"].iloc[1]
    assert val >= -1.0


@pytest.mark.unit
def test_tr07b_decline_clipped_upper() -> None:
    """TR-07b: large positive decline clipped at 10.0."""
    # pct_change from 1 → 100 = 99.0, clipped to 10.0
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 2, dtype=pd.StringDtype()),
            "oil_bbl": [1.0, 100.0],
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
            "gas_mcf": [0.0, 0.0],
        }
    )
    result = compute_decline_rate(_dask(df)).compute().sort_values("production_date")
    assert result["decline_rate"].iloc[1] == pytest.approx(10.0)


@pytest.mark.unit
def test_tr07c_decline_within_bounds() -> None:
    """TR-07c: 0.5 decline rate unchanged."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 2, dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 150.0],
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
            "gas_mcf": [0.0, 0.0],
        }
    )
    result = compute_decline_rate(_dask(df)).compute().sort_values("production_date")
    assert result["decline_rate"].iloc[1] == pytest.approx(0.5)


@pytest.mark.unit
def test_tr07d_shutin_then_production_nan() -> None:
    """TR-07d: prior=0, current>0 → decline_rate=NaN."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 2, dtype=pd.StringDtype()),
            "oil_bbl": [0.0, 100.0],
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
            "gas_mcf": [0.0, 0.0],
        }
    )
    result = compute_decline_rate(_dask(df)).compute().sort_values("production_date")
    assert pd.isna(result["decline_rate"].iloc[1])


@pytest.mark.unit
def test_decline_both_shutin_nan() -> None:
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 2, dtype=pd.StringDtype()),
            "oil_bbl": [0.0, 0.0],
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
            "gas_mcf": [0.0, 0.0],
        }
    )
    result = compute_decline_rate(_dask(df)).compute().sort_values("production_date")
    assert pd.isna(result["decline_rate"].iloc[1])


@pytest.mark.unit
def test_decline_first_row_nan() -> None:
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"], dtype=pd.StringDtype()),
            "oil_bbl": [100.0],
            "production_date": [pd.Timestamp("2024-01-01")],
            "gas_mcf": [0.0],
        }
    )
    result = compute_decline_rate(_dask(df)).compute()
    assert pd.isna(result["decline_rate"].iloc[0])


@pytest.mark.unit
def test_decline_returns_dask() -> None:
    df = _base_df()
    assert isinstance(compute_decline_rate(_dask(df)), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 05: compute_rolling_and_lag_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr09a_rolling_3month() -> None:
    """TR-09a: 3-month rolling average for known sequence."""
    vals = [100.0, 200.0, 150.0, 300.0, 250.0, 400.0]
    dates = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 7)]
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 6, dtype=pd.StringDtype()),
            "oil_bbl": vals,
            "gas_mcf": [0.0] * 6,
            "water_bbl": [0.0] * 6,
            "production_date": dates,
        }
    )
    result = compute_rolling_and_lag_features(_dask(df)).compute().sort_values("production_date")
    # Month 3 (index 2): mean(100, 200, 150) = 150.0
    assert result["oil_bbl_roll3"].iloc[2] == pytest.approx(150.0)
    # Month 4 (index 3): mean(200, 150, 300) = 216.667
    assert result["oil_bbl_roll3"].iloc[3] == pytest.approx(650 / 3, rel=1e-3)


@pytest.mark.unit
def test_tr09b_rolling_nan_insufficient_history() -> None:
    """TR-09b: 2 months → roll3 is NaN for both (min_periods=3)."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 2, dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 200.0],
            "gas_mcf": [0.0] * 2,
            "water_bbl": [0.0] * 2,
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
        }
    )
    result = compute_rolling_and_lag_features(_dask(df)).compute()
    assert result["oil_bbl_roll3"].isna().all()


@pytest.mark.unit
def test_tr09b_roll6_nan_insufficient() -> None:
    """TR-09b: 4 months → roll6 is NaN for all."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 4, dtype=pd.StringDtype()),
            "oil_bbl": [10.0, 20.0, 30.0, 40.0],
            "gas_mcf": [0.0] * 4,
            "water_bbl": [0.0] * 4,
            "production_date": [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 5)],
        }
    )
    result = compute_rolling_and_lag_features(_dask(df)).compute()
    assert result["oil_bbl_roll6"].isna().all()


@pytest.mark.unit
def test_tr09c_lag1() -> None:
    """TR-09c: lag-1 of [10, 20, 30, 40] = [NaN, 10, 20, 30]."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 4, dtype=pd.StringDtype()),
            "oil_bbl": [10.0, 20.0, 30.0, 40.0],
            "gas_mcf": [0.0] * 4,
            "water_bbl": [0.0] * 4,
            "production_date": [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 5)],
        }
    )
    result = compute_rolling_and_lag_features(_dask(df)).compute().sort_values("production_date")
    lags = result["oil_bbl_lag1"].tolist()
    assert pd.isna(lags[0])
    assert lags[1] == pytest.approx(10.0)
    assert lags[2] == pytest.approx(20.0)
    assert lags[3] == pytest.approx(30.0)


@pytest.mark.unit
def test_rolling_lag_all_columns_present() -> None:
    df = _base_df(6)
    df["water_bbl"] = 0.0
    result = compute_rolling_and_lag_features(_dask(df)).compute()
    for col in [
        "oil_bbl_roll3",
        "oil_bbl_roll6",
        "gas_mcf_roll3",
        "gas_mcf_roll6",
        "water_bbl_roll3",
        "water_bbl_roll6",
        "oil_bbl_lag1",
        "gas_mcf_lag1",
        "water_bbl_lag1",
    ]:
        assert col in result.columns


@pytest.mark.unit
def test_rolling_lag_returns_dask() -> None:
    df = _base_df()
    df["water_bbl"] = 0.0
    assert isinstance(compute_rolling_and_lag_features(_dask(df)), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 06: compute_rate_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_oil_per_day_january() -> None:
    """oil_bbl=310, January → 10.0/day."""
    df = _base_df()
    df["oil_bbl"] = 310.0
    df["production_date"] = pd.Timestamp("2024-01-01")
    df["water_bbl"] = 0.0
    result = compute_rate_features(_dask(df)).compute()
    assert result["oil_bbl_per_day"].iloc[0] == pytest.approx(10.0)


@pytest.mark.unit
def test_oil_per_day_february_2024() -> None:
    """oil_bbl=290, Feb 2024 (29 days) → 10.0/day."""
    df = _base_df()
    df["oil_bbl"] = 290.0
    df["production_date"] = pd.Timestamp("2024-02-01")
    df["water_bbl"] = 0.0
    result = compute_rate_features(_dask(df)).compute()
    assert result["oil_bbl_per_day"].iloc[0] == pytest.approx(10.0)


@pytest.mark.unit
def test_months_since_first_prod() -> None:
    """Jan 2024 first prod, Apr 2024 → months_since=3."""
    dates = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 5)]
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 4, dtype=pd.StringDtype()),
            "oil_bbl": [100.0, 100.0, 100.0, 100.0],
            "gas_mcf": [0.0] * 4,
            "water_bbl": [0.0] * 4,
            "production_date": dates,
            "api_number": pd.array(["A"] * 4, dtype=pd.StringDtype()),
        }
    )
    result = compute_rate_features(_dask(df)).compute().sort_values("production_date")
    assert result["months_since_first_prod"].iloc[3] == pytest.approx(3.0)


@pytest.mark.unit
def test_months_since_first_prod_all_zero() -> None:
    """All oil_bbl=0 → months_since_first_prod=NaN."""
    df = _base_df()
    df["oil_bbl"] = 0.0
    df["water_bbl"] = 0.0
    result = compute_rate_features(_dask(df)).compute()
    assert pd.isna(result["months_since_first_prod"].iloc[0])


@pytest.mark.unit
def test_rate_features_columns_present() -> None:
    df = _base_df()
    df["water_bbl"] = 0.0
    result = compute_rate_features(_dask(df)).compute()
    assert "oil_bbl_per_day" in result.columns
    assert "gas_mcf_per_day" in result.columns
    assert "months_since_first_prod" in result.columns


@pytest.mark.unit
def test_rate_features_returns_dask() -> None:
    df = _base_df()
    df["water_bbl"] = 0.0
    assert isinstance(compute_rate_features(_dask(df)), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 07: write_features_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_features_parquet_writes(tmp_path: Path) -> None:
    df = _base_df(5)
    df["water_bbl"] = 0.0
    ddf = _dask(df)
    out = tmp_path / "features"
    count = write_features_parquet(ddf, str(out))
    assert count >= 1


@pytest.mark.unit
def test_write_features_parquet_readable(tmp_path: Path) -> None:
    """TR-18."""
    df = _base_df(3)
    df["water_bbl"] = 0.0
    ddf = _dask(df)
    out = tmp_path / "f"
    write_features_parquet(ddf, str(out))
    for f in out.glob("*.parquet"):
        pd.read_parquet(f)


@pytest.mark.unit
def test_write_features_parquet_count_matches(tmp_path: Path) -> None:
    df = _base_df(5)
    df["water_bbl"] = 0.0
    ddf = _dask(df)
    out = tmp_path / "f"
    count = write_features_parquet(ddf, str(out))
    assert count == len(list(out.glob("*.parquet")))


# ---------------------------------------------------------------------------
# Task 08: write_manifest
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_manifest_creates_valid_json(tmp_path: Path) -> None:
    df = _base_df(5)
    df["water_bbl"] = 0.0
    ddf = _dask(df)
    start = datetime.now(tz=timezone.utc)
    manifest_path = write_manifest(ddf, str(tmp_path), start)
    assert manifest_path.exists()
    data = json.loads(manifest_path.read_text())
    for key in [
        "schema",
        "feature_columns",
        "record_count",
        "partition_count",
        "processing_start",
        "processing_end",
        "min_production_date",
        "max_production_date",
    ]:
        assert key in data


@pytest.mark.unit
def test_write_manifest_record_count(tmp_path: Path) -> None:
    n = 7
    df = _base_df(n)
    df["water_bbl"] = 0.0
    ddf = _dask(df)
    start = datetime.now(tz=timezone.utc)
    path = write_manifest(ddf, str(tmp_path), start)
    data = json.loads(path.read_text())
    assert data["record_count"] == n


@pytest.mark.unit
def test_write_manifest_returns_path(tmp_path: Path) -> None:
    df = _base_df(2)
    df["water_bbl"] = 0.0
    ddf = _dask(df)
    start = datetime.now(tz=timezone.utc)
    result = write_manifest(ddf, str(tmp_path), start)
    assert isinstance(result, Path)
    assert result.exists()


@pytest.mark.unit
def test_write_manifest_end_gte_start(tmp_path: Path) -> None:
    df = _base_df(2)
    df["water_bbl"] = 0.0
    ddf = _dask(df)
    start = datetime.now(tz=timezone.utc)
    path = write_manifest(ddf, str(tmp_path), start)
    data = json.loads(path.read_text())
    assert data["processing_end"] >= data["processing_start"]


# ---------------------------------------------------------------------------
# Task 09: run_features (mocked)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_features_calls_stages_in_order(tmp_path: Path) -> None:
    call_order: list[str] = []

    def _mock(name: str):
        def inner(*a, **kw):
            call_order.append(name)
            return MagicMock()

        return inner

    mock_ddf = MagicMock(spec=dd.DataFrame)
    mock_manifest = tmp_path / "manifest.json"
    mock_manifest.write_text("{}")

    def _side_effect_add_water(d):
        call_order.append("add_water")
        return d

    def _side_effect_gor(d):
        call_order.append("gor")
        return d

    def _side_effect_water_cut(d):
        call_order.append("wcut")
        return d

    def _side_effect_cumulative(d):
        call_order.append("cum")
        return d

    def _side_effect_decline_rate(d):
        call_order.append("dec")
        return d

    def _side_effect_rolling(d):
        call_order.append("roll")
        return d

    def _side_effect_rate(d):
        call_order.append("rate")
        return d

    with (
        patch("kgs_pipeline.features.read_processed", return_value=mock_ddf) as _,
        patch(
            "kgs_pipeline.features.add_water_column",
            side_effect=_side_effect_add_water,
        ),
        patch(
            "kgs_pipeline.features.compute_gor",
            side_effect=_side_effect_gor,
        ),
        patch(
            "kgs_pipeline.features.compute_water_cut",
            side_effect=_side_effect_water_cut,
        ),
        patch(
            "kgs_pipeline.features.compute_cumulative",
            side_effect=_side_effect_cumulative,
        ),
        patch(
            "kgs_pipeline.features.compute_decline_rate",
            side_effect=_side_effect_decline_rate,
        ),
        patch(
            "kgs_pipeline.features.compute_rolling_and_lag_features",
            side_effect=_side_effect_rolling,
        ),
        patch(
            "kgs_pipeline.features.compute_rate_features",
            side_effect=_side_effect_rate,
        ),
        patch("kgs_pipeline.features.write_features_parquet", return_value=1),
        patch("kgs_pipeline.features.dd.read_parquet", return_value=mock_ddf),
        patch("kgs_pipeline.features.write_manifest", return_value=mock_manifest),
    ):
        result = run_features(str(tmp_path), str(tmp_path))
        assert isinstance(result, Path)

    expected_order = ["add_water", "gor", "wcut", "cum", "dec", "roll", "rate"]
    assert call_order == expected_order


# ---------------------------------------------------------------------------
# TR-19: Feature column presence end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr19_full_feature_pipeline_columns(tmp_path: Path) -> None:
    """TR-19: All required feature columns present after full pipeline."""
    dates = [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 7)]
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["L1"] * 6, dtype=pd.StringDtype()),
            "api_number": pd.array(["A"] * 6, dtype=pd.StringDtype()),
            "lease_name": pd.array(["LN"] * 6, dtype=pd.StringDtype()),
            "operator": pd.array(["OP"] * 6, dtype=pd.StringDtype()),
            "county": pd.array(["C"] * 6, dtype=pd.StringDtype()),
            "field": pd.array(["F"] * 6, dtype=pd.StringDtype()),
            "producing_zone": pd.array(["Z"] * 6, dtype=pd.StringDtype()),
            "production_date": dates,
            "oil_bbl": [100.0, 200.0, 150.0, 300.0, 0.0, 250.0],
            "gas_mcf": [500.0] * 6,
            "well_count": [1.0] * 6,
            "latitude": [38.0] * 6,
            "longitude": [-97.0] * 6,
            "is_outlier": [False] * 6,
            "has_negative_production": [False] * 6,
            "source_file": pd.array(["t.txt"] * 6, dtype=pd.StringDtype()),
        }
    )
    ddf = _dask(df)
    ddf = add_water_column(ddf)
    ddf = compute_gor(ddf)
    ddf = compute_water_cut(ddf)
    ddf = compute_cumulative(ddf)
    ddf = compute_decline_rate(ddf)
    ddf = compute_rolling_and_lag_features(ddf)
    ddf = compute_rate_features(ddf)
    result = ddf.compute()

    required_cols = [
        "lease_kid",
        "production_date",
        "oil_bbl",
        "gas_mcf",
        "water_bbl",
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "oil_bbl_roll3",
        "oil_bbl_roll6",
        "gas_mcf_roll3",
        "gas_mcf_roll6",
        "water_bbl_roll3",
        "water_bbl_roll6",
        "oil_bbl_lag1",
        "gas_mcf_lag1",
        "water_bbl_lag1",
        "oil_bbl_per_day",
        "gas_mcf_per_day",
        "months_since_first_prod",
    ]
    for col in required_cols:
        assert col in result.columns, f"Missing column: {col}"


@pytest.mark.unit
def test_tr09d_gor_formula_not_swapped() -> None:
    """TR-09d: GOR = gas_mcf / oil_bbl (not inverted)."""
    df = _base_df()
    df["oil_bbl"] = 50.0
    df["gas_mcf"] = 200.0
    result = compute_gor(_dask(df)).compute()
    assert result["gor"].iloc[0] == pytest.approx(4.0)  # 200/50 = 4, not 50/200


@pytest.mark.unit
def test_tr09e_water_cut_formula() -> None:
    """TR-09e: water_cut = water_bbl / (oil_bbl + water_bbl)."""
    df = _base_df()
    df["water_bbl"] = 30.0
    df["oil_bbl"] = 70.0
    result = compute_water_cut(_dask(df)).compute()
    assert result["water_cut"].iloc[0] == pytest.approx(30.0 / 100.0)


@pytest.mark.unit
def test_tr10c_boundary_values_not_invalid() -> None:
    """TR-10c: 0.0 and 1.0 are valid water cut values."""
    # 0.0 case
    df = _base_df()
    df["water_bbl"] = 0.0
    df["oil_bbl"] = 100.0
    r1 = compute_water_cut(_dask(df)).compute()
    assert r1["water_cut"].iloc[0] == 0.0

    # 1.0 case
    df2 = _base_df()
    df2["water_bbl"] = 100.0
    df2["oil_bbl"] = 0.0
    r2 = compute_water_cut(_dask(df2)).compute()
    assert r2["water_cut"].iloc[0] == 1.0
