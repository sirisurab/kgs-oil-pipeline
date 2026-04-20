"""Tests for kgs_pipeline/features.py."""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.features import (
    REQUIRED_FEATURE_COLUMNS,
    add_cumulative_production,
    add_decline_rate,
    add_lag_features,
    add_ratios,
    add_rolling_features,
    apply_feature_transforms,
    check_schema_stability,
    encode_categoricals,
    validate_feature_correctness,
    validate_features_schema,
    write_features,
)


# ---------------------------------------------------------------------------
# Helper: minimal partition DataFrame
# ---------------------------------------------------------------------------

def _make_partition(n: int = 6, lease_id: str = "L1") -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=n, freq="MS")
    return pd.DataFrame({
        "LEASE_KID": pd.array([lease_id] * n, dtype="string"),
        "PRODUCT": pd.Categorical(["O"] * n, categories=["G", "O"]),
        "production_date": dates,
        "PRODUCTION": pd.array([100.0, 200.0, 300.0, 400.0, 500.0, 600.0][:n], dtype="Float64"),
        "WELLS": pd.array([1] * n, dtype="Int64"),
        "TWN_DIR": pd.Categorical(["S"] * n, categories=["N", "S"]),
        "RANGE_DIR": pd.Categorical(["E"] * n, categories=["E", "W"]),
    })


# ---------------------------------------------------------------------------
# FEA-01: add_cumulative_production
# ---------------------------------------------------------------------------

def test_cum_production_values() -> None:
    df = _make_partition(4)
    df["PRODUCTION"] = pd.array([100.0, 150.0, 0.0, 200.0], dtype="Float64")
    result = add_cumulative_production(df)
    cum = result["cum_production"].tolist()
    assert cum == pytest.approx([100.0, 250.0, 250.0, 450.0])


def test_cum_production_zero_at_start() -> None:
    df = _make_partition(3)
    df["PRODUCTION"] = pd.array([0.0, 100.0, 200.0], dtype="Float64")
    result = add_cumulative_production(df)
    cum = result["cum_production"].tolist()
    assert cum == pytest.approx([0.0, 100.0, 300.0])


def test_cum_production_monotone() -> None:
    df = _make_partition(6)
    result = add_cumulative_production(df)
    vals = result["cum_production"].tolist()
    assert all(vals[i] <= vals[i + 1] for i in range(len(vals) - 1))


def test_cum_production_missing_col() -> None:
    df = pd.DataFrame({"LEASE_KID": ["L1"], "production_date": pd.to_datetime(["2024-01-01"])})
    with pytest.raises(KeyError, match="PRODUCTION"):
        add_cumulative_production(df)


# ---------------------------------------------------------------------------
# FEA-02: add_ratios
# ---------------------------------------------------------------------------

def test_ratios_gor_undefined_zero_oil_nonzero_gas() -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["L1"],
        "PRODUCT": pd.Categorical(["G"], categories=["G", "O"]),
        "production_date": pd.to_datetime(["2024-01-01"]),
        "PRODUCTION": pd.array([100.0], dtype="Float64"),
        "WELLS": pd.array([1], dtype="Int64"),
        "TWN_DIR": pd.Categorical(["S"], categories=["N", "S"]),
        "RANGE_DIR": pd.Categorical(["E"], categories=["E", "W"]),
    })
    result = add_ratios(df)
    assert pd.isna(result["gor"].iloc[0])


def test_ratios_gor_zero_for_pos_oil_zero_gas() -> None:
    df = _make_partition(1)  # PRODUCT = O
    df["PRODUCTION"] = pd.array([200.0], dtype="Float64")
    result = add_ratios(df)
    # oil row, gas=0 → gor=0
    assert result["gor"].iloc[0] == pytest.approx(0.0)


def test_ratios_gor_both_zero() -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["L1"],
        "PRODUCT": pd.Categorical(["G"], categories=["G", "O"]),
        "production_date": pd.to_datetime(["2024-01-01"]),
        "PRODUCTION": pd.array([0.0], dtype="Float64"),
        "WELLS": pd.array([0], dtype="Int64"),
        "TWN_DIR": pd.Categorical(["S"], categories=["N", "S"]),
        "RANGE_DIR": pd.Categorical(["E"], categories=["E", "W"]),
    })
    result = add_ratios(df)
    assert pd.isna(result["gor"].iloc[0])


def test_ratios_water_cut_zero_water() -> None:
    df = _make_partition(1)
    df["PRODUCTION"] = pd.array([150.0], dtype="Float64")
    result = add_ratios(df)
    assert result["water_cut"].iloc[0] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# FEA-03: add_decline_rate
# ---------------------------------------------------------------------------

def test_decline_rate_values() -> None:
    df = _make_partition(3)
    df["PRODUCTION"] = pd.array([100.0, 80.0, 60.0], dtype="Float64")
    result = add_decline_rate(df)
    dr = result["decline_rate"].tolist()
    # Month 1: NaN (no prior)
    assert pd.isna(dr[0])
    # Month 2: (80-100)/100 = -0.2
    assert dr[1] == pytest.approx(-0.2)
    # Month 3: (60-80)/80 = -0.25
    assert dr[2] == pytest.approx(-0.25)


def test_decline_rate_clipped_low() -> None:
    df = _make_partition(2)
    df["PRODUCTION"] = pd.array([100.0, 0.0], dtype="Float64")  # -1.0 decline
    result = add_decline_rate(df)
    assert result["decline_rate"].iloc[1] == pytest.approx(-1.0)


def test_decline_rate_zero_prev_is_nan() -> None:
    df = _make_partition(2)
    df["PRODUCTION"] = pd.array([0.0, 50.0], dtype="Float64")
    result = add_decline_rate(df)
    assert pd.isna(result["decline_rate"].iloc[1])


def test_decline_rate_clipped_high() -> None:
    # A large jump > 10x (more than +10.0 rate)
    df = _make_partition(2)
    df["PRODUCTION"] = pd.array([1.0, 200.0], dtype="Float64")
    result = add_decline_rate(df)
    # (200-1)/1 = 199 → clipped to 10.0
    assert result["decline_rate"].iloc[1] == pytest.approx(10.0)


def test_decline_rate_within_bounds_unchanged() -> None:
    df = _make_partition(2)
    df["PRODUCTION"] = pd.array([100.0, 150.0], dtype="Float64")
    result = add_decline_rate(df)
    # (150-100)/100 = 0.5
    assert result["decline_rate"].iloc[1] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# FEA-04: add_rolling_features
# ---------------------------------------------------------------------------

def test_rolling_3m_values() -> None:
    df = _make_partition(6)
    result = add_rolling_features(df)
    roll3 = result["rolling_3m_production"].tolist()
    # position 0: mean of [100] = 100
    assert roll3[0] == pytest.approx(100.0)
    # position 1: mean of [100, 200] = 150
    assert roll3[1] == pytest.approx(150.0)
    # position 2: mean of [100, 200, 300] = 200
    assert roll3[2] == pytest.approx(200.0)


def test_rolling_6m_all_values() -> None:
    df = _make_partition(6)
    result = add_rolling_features(df)
    roll6_last = result["rolling_6m_production"].iloc[-1]
    assert roll6_last == pytest.approx(350.0)  # mean([100..600]) = 350


def test_rolling_partial_window() -> None:
    df = _make_partition(2)
    result = add_rolling_features(df)
    # 2 values, window=6 → partial: mean of 2 available
    roll6_2 = result["rolling_6m_production"].iloc[1]
    assert roll6_2 == pytest.approx(150.0)


def test_rolling_columns_present() -> None:
    df = _make_partition(3)
    result = add_rolling_features(df)
    assert "rolling_3m_production" in result.columns
    assert "rolling_6m_production" in result.columns


# ---------------------------------------------------------------------------
# FEA-05: add_lag_features
# ---------------------------------------------------------------------------

def test_lag_1m_value() -> None:
    df = _make_partition(3)
    df["PRODUCTION"] = pd.array([100.0, 150.0, 200.0], dtype="Float64")
    result = add_lag_features(df)
    # lag_1m for month 3 = 150
    assert result["lag_1m_production"].iloc[2] == pytest.approx(150.0)


def test_lag_1m_first_is_nan() -> None:
    df = _make_partition(3)
    result = add_lag_features(df)
    assert pd.isna(result["lag_1m_production"].iloc[0])


def test_lag_3m_first_three_are_nan() -> None:
    df = _make_partition(4)
    result = add_lag_features(df)
    for i in range(3):
        assert pd.isna(result["lag_3m_production"].iloc[i])
    # month 4 lag = first month's production
    assert result["lag_3m_production"].iloc[3] == pytest.approx(100.0)


def test_lag_columns_present() -> None:
    df = _make_partition(2)
    result = add_lag_features(df)
    for col in ("lag_1m_production", "lag_3m_production", "lag_6m_production"):
        assert col in result.columns


# ---------------------------------------------------------------------------
# FEA-06: encode_categoricals
# ---------------------------------------------------------------------------

def test_encode_product_code_present() -> None:
    df = _make_partition(2)
    result = encode_categoricals(df)
    assert "product_code" in result.columns
    # O is index 1 in ["G", "O"]
    assert result["product_code"].iloc[0] == 1


def test_encode_twn_dir_null_gives_minus1() -> None:
    df = _make_partition(1)
    df["TWN_DIR"] = pd.Categorical([None], categories=["N", "S"])
    result = encode_categoricals(df)
    assert result["twn_dir_code"].iloc[0] == -1


def test_encode_originals_preserved() -> None:
    df = _make_partition(2)
    result = encode_categoricals(df)
    assert "PRODUCT" in result.columns
    assert "TWN_DIR" in result.columns
    assert "RANGE_DIR" in result.columns


def test_encode_range_dir_code_present() -> None:
    df = _make_partition(1)
    result = encode_categoricals(df)
    assert "range_dir_code" in result.columns


# ---------------------------------------------------------------------------
# FEA-07: apply_feature_transforms
# ---------------------------------------------------------------------------

def _make_full_dask_df() -> dd.DataFrame:
    df = _make_partition(6)
    # Add columns needed for full pipeline
    for col in ["LEASE", "DOR_CODE", "API_NUMBER", "FIELD", "PRODUCING_ZONE",
                "OPERATOR", "COUNTY", "TOWNSHIP", "RANGE", "SECTION", "SPOT",
                "LATITUDE", "LONGITUDE", "source_file"]:
        df[col] = pd.array(["test"] * 6, dtype="string")
    return dd.from_pandas(df, npartitions=1)


def test_apply_feature_transforms_returns_dask() -> None:
    ddf = _make_full_dask_df()
    result = apply_feature_transforms(ddf)
    assert isinstance(result, dd.DataFrame)


def test_apply_feature_transforms_all_derived_columns() -> None:
    ddf = _make_full_dask_df()
    result = apply_feature_transforms(ddf)
    computed = result.compute()
    expected_derived = [
        "cum_production", "gor", "water_cut", "decline_rate",
        "rolling_3m_production", "rolling_6m_production",
        "lag_1m_production", "lag_3m_production", "lag_6m_production",
        "product_code", "twn_dir_code", "range_dir_code",
    ]
    for col in expected_derived:
        assert col in computed.columns, f"Missing: {col}"


# ---------------------------------------------------------------------------
# FEA-08: validate_features_schema
# ---------------------------------------------------------------------------

def test_validate_features_schema_passes() -> None:
    df = pd.DataFrame({col: [1] for col in REQUIRED_FEATURE_COLUMNS})
    ddf = dd.from_pandas(df, npartitions=1)
    validate_features_schema(ddf)  # Should not raise


def test_validate_features_schema_missing_gor() -> None:
    cols = [c for c in REQUIRED_FEATURE_COLUMNS if c != "gor"]
    df = pd.DataFrame({col: [1] for col in cols})
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(ValueError, match="gor"):
        validate_features_schema(ddf)


def test_validate_features_schema_multiple_missing() -> None:
    cols = [c for c in REQUIRED_FEATURE_COLUMNS if c not in ("cum_production", "water_cut")]
    df = pd.DataFrame({col: [1] for col in cols})
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(ValueError):
        validate_features_schema(ddf)


# ---------------------------------------------------------------------------
# FEA-09: check_schema_stability
# ---------------------------------------------------------------------------

def test_check_schema_stability_identical(tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
    for i in range(2):
        df.to_parquet(tmp_path / f"part_{i}.parquet")
    check_schema_stability(tmp_path)  # Should not raise


def test_check_schema_stability_extra_column(tmp_path: Path) -> None:
    df1 = pd.DataFrame({"a": [1], "b": [2.0]})
    df2 = pd.DataFrame({"a": [1], "b": [2.0], "c": ["x"]})
    df1.to_parquet(tmp_path / "part_0.parquet")
    df2.to_parquet(tmp_path / "part_1.parquet")
    with pytest.raises(ValueError):
        check_schema_stability(tmp_path)


# ---------------------------------------------------------------------------
# FEA-11: validate_feature_correctness
# ---------------------------------------------------------------------------

def _make_valid_features_df() -> pd.DataFrame:
    df = _make_partition(4)
    df = add_cumulative_production(df)
    df = add_ratios(df)
    df = add_decline_rate(df)
    return df


def test_validate_correct_df_returns_empty() -> None:
    df = _make_valid_features_df()
    errors = validate_feature_correctness(df)
    assert errors == []


def test_validate_negative_production() -> None:
    df = _make_valid_features_df()
    df = df.copy()
    df.loc[df.index[0], "PRODUCTION"] = -1.0
    errors = validate_feature_correctness(df)
    assert len(errors) >= 1


def test_validate_decreasing_cum_production() -> None:
    df = _make_valid_features_df()
    df = df.copy()
    df["cum_production"] = [100.0, 90.0, 95.0, 100.0]
    errors = validate_feature_correctness(df)
    assert any("cum_production" in e for e in errors)


def test_validate_out_of_bounds_decline_rate() -> None:
    df = _make_valid_features_df()
    df = df.copy()
    df["decline_rate"] = [None, 15.0, -0.5, 0.1]
    errors = validate_feature_correctness(df)
    assert any("decline_rate" in e for e in errors)


def test_validate_water_cut_out_of_range() -> None:
    df = _make_valid_features_df()
    df = df.copy()
    df["water_cut"] = [0.5, 1.5, 0.0, 0.2]
    errors = validate_feature_correctness(df)
    assert any("water_cut" in e for e in errors)
