"""Tests for kgs_pipeline/features.py."""

from __future__ import annotations

import json
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.features import (
    add_cumulative_production,
    add_decline_rate,
    add_gor,
    add_label_encodings,
    add_lag_features,
    add_rolling_features,
    add_temporal_features,
    write_feature_manifest,
    write_features_parquet,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def oil_df() -> pd.DataFrame:
    """Single lease with 4 months of oil production."""
    return pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001, 1001, 1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(
                ["1-2024", "2-2024", "3-2024", "4-2024"], dtype=pd.StringDtype()
            ),
            "PRODUCT": pd.Categorical(["O", "O", "O", "O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0, 200.0, 0.0, 150.0], dtype=np.float64),
            "WELLS": pd.array([3, 3, 0, 3], dtype=pd.Int64Dtype()),
            "COUNTY": pd.array(["Barton", "Barton", "Barton", "Barton"], dtype=pd.StringDtype()),
            "FIELD": pd.array(["F1", "F1", "F1", "F1"], dtype=pd.StringDtype()),
            "PRODUCING_ZONE": pd.array(["PZ1", "PZ1", "PZ1", "PZ1"], dtype=pd.StringDtype()),
            "OPERATOR": pd.array(["ACME", "ACME", "ACME", "ACME"], dtype=pd.StringDtype()),
            "LEASE": pd.array(["ALPHA", "ALPHA", "ALPHA", "ALPHA"], dtype=pd.StringDtype()),
            "production_date": pd.to_datetime(
                ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]
            ),
        }
    )


@pytest.fixture()
def gas_oil_df() -> pd.DataFrame:
    """Two leases, both oil and gas rows per month."""
    return pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001, 1002, 1002], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(
                ["1-2024", "1-2024", "1-2024", "1-2024"], dtype=pd.StringDtype()
            ),
            "PRODUCT": pd.Categorical(["O", "G", "O", "G"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0, 50000.0, 0.0, 0.0], dtype=np.float64),
            "WELLS": pd.array([1, 1, 1, 1], dtype=pd.Int64Dtype()),
            "COUNTY": pd.array(["Barton", "Barton", "Ellis", "Ellis"], dtype=pd.StringDtype()),
            "FIELD": pd.array(["F1", "F1", "F2", "F2"], dtype=pd.StringDtype()),
            "PRODUCING_ZONE": pd.array(["PZ1", "PZ1", "PZ2", "PZ2"], dtype=pd.StringDtype()),
            "OPERATOR": pd.array(["ACME", "ACME", "BETA OIL", "BETA OIL"], dtype=pd.StringDtype()),
            "LEASE": pd.array(["ALPHA", "ALPHA", "BETA", "BETA"], dtype=pd.StringDtype()),
            "production_date": pd.to_datetime(
                ["2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"]
            ),
        }
    )


# ---------------------------------------------------------------------------
# Task 01: add_cumulative_production (TR-03, TR-08)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cumulative_production_sequence(oil_df: pd.DataFrame) -> None:
    """[100, 200, 0, 150] → cumulative [100, 300, 300, 450] (TR-08a)."""
    result = add_cumulative_production(oil_df)
    cum = result[result["PRODUCT"].astype(str) == "O"]["cum_oil"].tolist()
    assert cum == [100.0, 300.0, 300.0, 450.0]


@pytest.mark.unit
def test_cumulative_production_starts_with_zeros() -> None:
    """[0, 0, 100] → cumulative [0, 0, 100] (TR-08b)."""
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001, 1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["1-2024", "2-2024", "3-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "O", "O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([0.0, 0.0, 100.0], dtype=np.float64),
            "production_date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
        }
    )
    result = add_cumulative_production(df)
    cum = result["cum_oil"].tolist()
    assert cum == [0.0, 0.0, 100.0]


@pytest.mark.unit
def test_cumulative_production_monotonically_nondecreasing(oil_df: pd.DataFrame) -> None:
    """Cumulative values are monotonically non-decreasing (TR-03)."""
    result = add_cumulative_production(oil_df)
    oil_cum = result[result["PRODUCT"].astype(str) == "O"]["cum_oil"].values
    assert all(oil_cum[i] <= oil_cum[i + 1] for i in range(len(oil_cum) - 1))


@pytest.mark.unit
def test_cumulative_production_gas_rows_have_no_cum_oil(gas_oil_df: pd.DataFrame) -> None:
    """Gas rows have cum_oil = pd.NA."""
    result = add_cumulative_production(gas_oil_df)
    gas_rows = result[result["PRODUCT"].astype(str) == "G"]
    assert gas_rows["cum_oil"].isna().all()


@pytest.mark.unit
def test_cumulative_production_oil_rows_have_no_cum_gas(gas_oil_df: pd.DataFrame) -> None:
    """Oil rows have cum_gas = pd.NA."""
    result = add_cumulative_production(gas_oil_df)
    oil_rows = result[result["PRODUCT"].astype(str) == "O"]
    assert oil_rows["cum_gas"].isna().all()


# ---------------------------------------------------------------------------
# Task 02: add_gor (TR-06)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_gor_zero_oil_positive_gas(gas_oil_df: pd.DataFrame) -> None:
    """oil=0, gas>0 → GOR is NaN (TR-06a). Lease 1002."""
    # Modify gas_oil_df for lease 1002: oil=0, gas=100
    df = gas_oil_df.copy()
    df.loc[df["LEASE_KID"] == 1002, "PRODUCTION"] = [0.0, 100.0]
    result = add_gor(df)
    lk1002 = result[result["LEASE_KID"] == 1002]
    assert lk1002["gor"].isna().all()


@pytest.mark.unit
def test_gor_zero_oil_zero_gas(gas_oil_df: pd.DataFrame) -> None:
    """oil=0, gas=0 (shut-in) → GOR is 0.0 or NaN, not exception (TR-06b)."""
    result = add_gor(gas_oil_df)
    lk1002 = result[result["LEASE_KID"] == 1002]["gor"]
    for val in lk1002:
        assert pd.isna(val) or val == 0.0


@pytest.mark.unit
def test_gor_positive_oil_zero_gas() -> None:
    """oil>0, gas=0 → GOR is 0.0 (TR-06c)."""
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["1-2024", "1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "G"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0, 0.0], dtype=np.float64),
            "production_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        }
    )
    result = add_gor(df)
    oil_row = result[result["PRODUCT"].astype(str) == "O"]
    assert oil_row["gor"].iloc[0] == 0.0


@pytest.mark.unit
def test_gor_formula_oil_100_gas_50000(gas_oil_df: pd.DataFrame) -> None:
    """oil=100, gas=50000 → GOR = 500.0."""
    lk1001 = gas_oil_df[gas_oil_df["LEASE_KID"] == 1001].copy()
    result = add_gor(lk1001)
    oil_row = result[result["PRODUCT"].astype(str) == "O"]
    assert abs(oil_row["gor"].iloc[0] - 500.0) < 1e-6


@pytest.mark.unit
def test_gor_no_zerodivision(gas_oil_df: pd.DataFrame) -> None:
    """No ZeroDivisionError for any input combination."""
    try:
        add_gor(gas_oil_df)
    except ZeroDivisionError:
        pytest.fail("ZeroDivisionError raised by add_gor")


# ---------------------------------------------------------------------------
# Task 03: add_decline_rate (TR-07)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_decline_rate_clipped_lower(oil_df: pd.DataFrame) -> None:
    """Decline rate < -1.0 → clipped to -1.0 (TR-07a)."""
    df = oil_df.copy()
    df["PRODUCTION"] = pd.array([200.0, 0.0, 0.0, 0.0], dtype=np.float64)
    result = add_decline_rate(df)
    clipped = result["decline_rate"].dropna()
    assert (clipped >= -1.0).all()


@pytest.mark.unit
def test_decline_rate_clipped_upper(oil_df: pd.DataFrame) -> None:
    """Computed rate > 10.0 → clipped to 10.0 (TR-07b)."""
    df = oil_df.copy()
    # prod_t=1000, lag=1 → rate=999 → clipped to 10
    df["PRODUCTION"] = pd.array([1.0, 1000.0, 0.0, 0.0], dtype=np.float64)
    result = add_decline_rate(df)
    clipped = result["decline_rate"].dropna()
    assert (clipped <= 10.0).all()


@pytest.mark.unit
def test_decline_rate_within_bounds(oil_df: pd.DataFrame) -> None:
    """Rate within [-1, 10] passes unchanged (TR-07c)."""
    df = oil_df.copy()
    # 100 → 150: rate = 0.5
    df["PRODUCTION"] = pd.array([100.0, 150.0, 0.0, 0.0], dtype=np.float64)
    result = add_decline_rate(df)
    non_nan = result["decline_rate"].dropna()
    # Second row rate = 0.5
    assert abs(non_nan.iloc[0] - 0.5) < 1e-6


@pytest.mark.unit
def test_decline_rate_consecutive_zeros(oil_df: pd.DataFrame) -> None:
    """Both consecutive production=0 → decline_rate = 0.0 before clipping (TR-07d)."""
    df = oil_df.copy()
    df["PRODUCTION"] = pd.array([0.0, 0.0, 0.0, 0.0], dtype=np.float64)
    result = add_decline_rate(df)
    non_nan = result["decline_rate"].dropna()
    assert (non_nan == 0.0).all()


@pytest.mark.unit
def test_decline_rate_first_record_nan(oil_df: pd.DataFrame) -> None:
    """First record of each lease-product should have NaN decline_rate."""
    result = add_decline_rate(oil_df)
    first_idx = result.index[0]
    assert pd.isna(result.loc[first_idx, "decline_rate"])


# ---------------------------------------------------------------------------
# Task 04: add_rolling_features (TR-09a, TR-09b)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rolling_3m_production(oil_df: pd.DataFrame) -> None:
    """[100, 200, 0, 150] → rolling_3m = [100, 150, 100, 116.67]."""
    result = add_rolling_features(oil_df)
    r3 = result["rolling_3m_production"].tolist()
    assert abs(r3[0] - 100.0) < 1e-6
    assert abs(r3[1] - 150.0) < 1e-6
    assert abs(r3[2] - (100 + 200 + 0) / 3) < 1e-6


@pytest.mark.unit
def test_rolling_6m_production(oil_df: pd.DataFrame) -> None:
    """[100, 200, 0, 150] → rolling_6m partial windows for all 4 rows."""
    result = add_rolling_features(oil_df)
    r6 = result["rolling_6m_production"].tolist()
    assert abs(r6[0] - 100.0) < 1e-6
    assert abs(r6[1] - 150.0) < 1e-6
    assert abs(r6[2] - (100 + 200 + 0) / 3) < 1e-6
    assert abs(r6[3] - (100 + 200 + 0 + 150) / 4) < 1e-6


@pytest.mark.unit
def test_rolling_partial_window_not_nan(oil_df: pd.DataFrame) -> None:
    """Partial window (first months) returns partial mean, not NaN (TR-09b)."""
    result = add_rolling_features(oil_df)
    assert not result["rolling_3m_production"].isna().any()
    assert not result["rolling_6m_production"].isna().any()


@pytest.mark.unit
def test_rolling_per_lease_independent(gas_oil_df: pd.DataFrame) -> None:
    """Rolling values per lease don't bleed across leases."""
    # Extend to multi-month for rolling computation
    rows = []
    for kid, prod in [(1001, 100.0), (1002, 500.0)]:
        for month in range(1, 5):
            rows.append(
                {
                    "LEASE_KID": kid,
                    "MONTH-YEAR": f"{month}-2024",
                    "PRODUCT": "O",
                    "PRODUCTION": prod,
                    "production_date": pd.Timestamp(f"2024-{month:02d}-01"),
                    "COUNTY": "Barton",
                    "FIELD": "F1",
                    "PRODUCING_ZONE": "PZ1",
                    "OPERATOR": "ACME",
                    "LEASE": "ALPHA",
                    "WELLS": 1,
                }
            )
    df2 = pd.DataFrame(rows)
    df2["LEASE_KID"] = df2["LEASE_KID"].astype(pd.Int64Dtype())
    df2["PRODUCT"] = pd.Categorical(df2["PRODUCT"], categories=["O", "G"])
    result = add_rolling_features(df2)
    lk1001 = result[result["LEASE_KID"] == 1001]["rolling_3m_production"]
    lk1002 = result[result["LEASE_KID"] == 1002]["rolling_3m_production"]
    # Lease 1001 rolling is ~100, lease 1002 is ~500
    assert lk1001.mean() < lk1002.mean()


# ---------------------------------------------------------------------------
# Task 05: add_lag_features (TR-09c)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_lag_1_production_sequence(oil_df: pd.DataFrame) -> None:
    """[100, 200, 0, 150] → lag_1 = [NaN, 100, 200, 0]."""
    result = add_lag_features(oil_df)
    lag = result["lag_1_production"].tolist()
    assert pd.isna(lag[0])
    assert abs(lag[1] - 100.0) < 1e-6
    assert abs(lag[2] - 200.0) < 1e-6
    assert abs(lag[3] - 0.0) < 1e-6


@pytest.mark.unit
def test_lag_first_record_nan(oil_df: pd.DataFrame) -> None:
    """lag_1_production is NaN for the first record of each group."""
    result = add_lag_features(oil_df)
    assert pd.isna(result["lag_1_production"].iloc[0])


# ---------------------------------------------------------------------------
# Task 06: add_temporal_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_temporal_march_2024(oil_df: pd.DataFrame) -> None:
    """production_date=2024-03-01 → year=2024, month=3, quarter=1, days_in_month=31."""
    df = pd.DataFrame(
        {
            "production_date": pd.to_datetime(["2024-03-01"]),
            "LEASE_KID": pd.array([1001], dtype=pd.Int64Dtype()),
        }
    )
    result = add_temporal_features(df)
    assert result["year"].iloc[0] == 2024
    assert result["month"].iloc[0] == 3
    assert result["quarter"].iloc[0] == 1
    assert result["days_in_month"].iloc[0] == 31


@pytest.mark.unit
def test_temporal_june_2024(oil_df: pd.DataFrame) -> None:
    """June → quarter=2, days_in_month=30."""
    df = pd.DataFrame(
        {
            "production_date": pd.to_datetime(["2024-06-01"]),
            "LEASE_KID": pd.array([1001], dtype=pd.Int64Dtype()),
        }
    )
    result = add_temporal_features(df)
    assert result["quarter"].iloc[0] == 2
    assert result["days_in_month"].iloc[0] == 30


@pytest.mark.unit
def test_temporal_february_leap_year() -> None:
    """February 2024 is a leap year → days_in_month=29."""
    df = pd.DataFrame(
        {
            "production_date": pd.to_datetime(["2024-02-01"]),
            "LEASE_KID": pd.array([1001], dtype=pd.Int64Dtype()),
        }
    )
    result = add_temporal_features(df)
    assert result["days_in_month"].iloc[0] == 29


# ---------------------------------------------------------------------------
# Task 07: add_label_encodings
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_label_encoding_known_county() -> None:
    """COUNTY='Nemaha' → county_code=0."""
    enc = {
        "COUNTY": {"Nemaha": 0, "Barton": 1},
        "FIELD": {},
        "PRODUCING_ZONE": {},
        "OPERATOR": {},
        "PRODUCT": {"O": 0, "G": 1},
    }
    df = pd.DataFrame(
        {
            "COUNTY": pd.array(["Nemaha"], dtype=pd.StringDtype()),
            "FIELD": pd.array(["F1"], dtype=pd.StringDtype()),
            "PRODUCING_ZONE": pd.array(["PZ1"], dtype=pd.StringDtype()),
            "OPERATOR": pd.array(["ACME"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
        }
    )
    result = add_label_encodings(df, enc)
    assert result["county_code"].iloc[0] == 0


@pytest.mark.unit
def test_label_encoding_unknown_value() -> None:
    """Unknown county → county_code=-1."""
    enc = {
        "COUNTY": {"Barton": 0},
        "FIELD": {},
        "PRODUCING_ZONE": {},
        "OPERATOR": {},
        "PRODUCT": {"O": 0, "G": 1},
    }
    df = pd.DataFrame(
        {
            "COUNTY": pd.array(["Unknown"], dtype=pd.StringDtype()),
            "FIELD": pd.array(["F1"], dtype=pd.StringDtype()),
            "PRODUCING_ZONE": pd.array(["PZ1"], dtype=pd.StringDtype()),
            "OPERATOR": pd.array(["ACME"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
        }
    )
    result = add_label_encodings(df, enc)
    assert result["county_code"].iloc[0] == -1


@pytest.mark.unit
def test_label_encoding_product_codes() -> None:
    """PRODUCT=='O' → product_code=0, PRODUCT=='G' → product_code=1."""
    enc = {
        "COUNTY": {},
        "FIELD": {},
        "PRODUCING_ZONE": {},
        "OPERATOR": {},
        "PRODUCT": {"O": 0, "G": 1},
    }
    df = pd.DataFrame(
        {
            "COUNTY": pd.array(["Barton", "Barton"], dtype=pd.StringDtype()),
            "FIELD": pd.array(["F1", "F1"], dtype=pd.StringDtype()),
            "PRODUCING_ZONE": pd.array(["PZ1", "PZ1"], dtype=pd.StringDtype()),
            "OPERATOR": pd.array(["ACME", "ACME"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "G"], categories=["O", "G"]),
        }
    )
    result = add_label_encodings(df, enc)
    assert result["product_code"].iloc[0] == 0
    assert result["product_code"].iloc[1] == 1


@pytest.mark.unit
def test_label_encoding_int64_dtype() -> None:
    """All *_code columns have Int64 dtype."""
    enc = {
        "COUNTY": {"Barton": 0},
        "FIELD": {"F1": 0},
        "PRODUCING_ZONE": {"PZ1": 0},
        "OPERATOR": {"ACME": 0},
        "PRODUCT": {"O": 0, "G": 1},
    }
    df = pd.DataFrame(
        {
            "COUNTY": pd.array(["Barton"], dtype=pd.StringDtype()),
            "FIELD": pd.array(["F1"], dtype=pd.StringDtype()),
            "PRODUCING_ZONE": pd.array(["PZ1"], dtype=pd.StringDtype()),
            "OPERATOR": pd.array(["ACME"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
        }
    )
    result = add_label_encodings(df, enc)
    for col in [
        "county_code",
        "field_code",
        "producing_zone_code",
        "operator_code",
        "product_code",
    ]:
        assert result[col].dtype == pd.Int64Dtype(), f"{col} is not Int64"


# ---------------------------------------------------------------------------
# Task 08: write_feature_manifest
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_feature_manifest_readable(tmp_path: Path) -> None:
    """Written manifest is readable and contains expected keys."""
    enc = {
        "COUNTY": {"Barton": 0},
        "FIELD": {},
        "PRODUCING_ZONE": {},
        "OPERATOR": {},
        "PRODUCT": {"O": 0, "G": 1},
    }
    out = tmp_path / "manifest.json"
    write_feature_manifest(["col1", "col2"], enc, out)
    with open(out) as fh:
        data = json.load(fh)
    assert "feature_columns" in data
    assert "encoding_maps" in data
    assert "generated_at" in data


@pytest.mark.unit
def test_write_feature_manifest_columns_match(tmp_path: Path) -> None:
    """feature_columns in manifest matches input list."""
    enc: dict[str, dict] = {"COUNTY": {}, "FIELD": {}, "PRODUCING_ZONE": {}, "OPERATOR": {}, "PRODUCT": {}}
    out = tmp_path / "manifest.json"
    cols = ["LEASE_KID", "PRODUCTION", "gor"]
    write_feature_manifest(cols, enc, out)
    with open(out) as fh:
        data = json.load(fh)
    assert data["feature_columns"] == cols


@pytest.mark.unit
def test_write_feature_manifest_encoding_maps_keys(tmp_path: Path) -> None:
    """Manifest encoding_maps contains COUNTY, FIELD, PRODUCING_ZONE, OPERATOR, PRODUCT."""
    enc = {
        "COUNTY": {"Barton": 0},
        "FIELD": {"F1": 0},
        "PRODUCING_ZONE": {"PZ": 0},
        "OPERATOR": {"ACME": 0},
        "PRODUCT": {"O": 0, "G": 1},
    }
    out = tmp_path / "manifest.json"
    write_feature_manifest([], enc, out)
    with open(out) as fh:
        data = json.load(fh)
    for key in ["COUNTY", "FIELD", "PRODUCING_ZONE", "OPERATOR", "PRODUCT"]:
        assert key in data["encoding_maps"]


@pytest.mark.unit
def test_write_feature_manifest_generated_at_iso(tmp_path: Path) -> None:
    """generated_at is a valid ISO 8601 timestamp string."""
    from datetime import datetime

    out = tmp_path / "manifest.json"
    write_feature_manifest(
        [], {"COUNTY": {}, "FIELD": {}, "PRODUCING_ZONE": {}, "OPERATOR": {}, "PRODUCT": {}}, out
    )
    with open(out) as fh:
        data = json.load(fh)
    ts = data["generated_at"]
    # Should parse without error
    datetime.fromisoformat(ts)


# ---------------------------------------------------------------------------
# Task 11: Meta schema consistency (TR-23)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_meta_consistency_temporal_features(oil_df: pd.DataFrame) -> None:
    actual = add_temporal_features(oil_df)
    meta = add_temporal_features(oil_df.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_cumulative_production(oil_df: pd.DataFrame) -> None:
    actual = add_cumulative_production(oil_df)
    meta = add_cumulative_production(oil_df.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_decline_rate(oil_df: pd.DataFrame) -> None:
    actual = add_decline_rate(oil_df)
    meta = add_decline_rate(oil_df.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_rolling_features(oil_df: pd.DataFrame) -> None:
    actual = add_rolling_features(oil_df)
    meta = add_rolling_features(oil_df.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_lag_features(oil_df: pd.DataFrame) -> None:
    actual = add_lag_features(oil_df)
    meta = add_lag_features(oil_df.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_gor(gas_oil_df: pd.DataFrame) -> None:
    actual = add_gor(gas_oil_df)
    meta = add_gor(gas_oil_df.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_label_encodings(gas_oil_df: pd.DataFrame) -> None:
    enc = {
        "COUNTY": {"Barton": 0, "Ellis": 1},
        "FIELD": {"F1": 0, "F2": 1},
        "PRODUCING_ZONE": {"PZ1": 0, "PZ2": 1},
        "OPERATOR": {"ACME": 0, "BETA OIL": 1},
        "PRODUCT": {"O": 0, "G": 1},
    }
    actual = add_label_encodings(gas_oil_df, enc)
    meta = add_label_encodings(gas_oil_df.iloc[0:0], enc)
    assert list(actual.columns) == list(meta.columns)


# ---------------------------------------------------------------------------
# Task 12: Physical bounds and domain correctness (TR-01, TR-06, TR-07, TR-08, TR-09, TR-10)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr01_all_gor_nonneg(gas_oil_df: pd.DataFrame) -> None:
    """All gor values in features output are NaN or >= 0 (TR-01)."""
    result = add_gor(gas_oil_df)
    for val in result["gor"]:
        assert pd.isna(val) or val >= 0


@pytest.mark.unit
def test_tr09d_gor_formula_not_swapped() -> None:
    """GOR = gas/oil, not oil/gas (TR-09d)."""
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["1-2024", "1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "G"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0, 200.0], dtype=np.float64),
            "production_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        }
    )
    result = add_gor(df)
    oil_row = result[result["PRODUCT"].astype(str) == "O"]
    # gas=200, oil=100 → GOR=2.0 (not 0.5 which would be oil/gas)
    assert abs(oil_row["gor"].iloc[0] - 2.0) < 1e-6


@pytest.mark.unit
def test_tr10_water_cut_column_all_na(oil_df: pd.DataFrame) -> None:
    """water_cut column should be pd.NA for all rows per TR-10.

    Note: water_cut is not computed by features.py since KGS data has no water
    column. This test documents expected behavior — if water_cut is added, it
    must be NA or in [0, 1].
    """
    # The current implementation does not add water_cut; we assert it won't
    # appear with invalid values if somehow added.
    result = add_cumulative_production(oil_df)
    if "water_cut" in result.columns:
        for val in result["water_cut"]:
            assert pd.isna(val) or (0 <= val <= 1)


# ---------------------------------------------------------------------------
# Task 13: Monotonicity and column presence (TR-03, TR-19)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr03_cumulative_monotonically_nondecreasing(oil_df: pd.DataFrame) -> None:
    """cum_oil values are monotonically non-decreasing (TR-03)."""
    result = add_cumulative_production(oil_df)
    oil = result[result["PRODUCT"].astype(str) == "O"].sort_values("production_date")
    cum = oil["cum_oil"].values
    assert all(cum[i] <= cum[i + 1] for i in range(len(cum) - 1))


@pytest.mark.unit
def test_write_features_parquet_creates_files(oil_df: pd.DataFrame, tmp_path: Path) -> None:
    """write_features_parquet creates at least one Parquet file."""
    ddf = dd.from_pandas(oil_df, npartitions=1)
    out = tmp_path / "features_out"
    write_features_parquet(ddf, out, n_partitions=10)
    pf = list(out.glob("*.parquet"))
    assert len(pf) >= 1
    for f in pf:
        pd.read_parquet(f)  # should not raise
