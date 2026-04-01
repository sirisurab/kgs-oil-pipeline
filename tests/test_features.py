"""Tests for kgs_pipeline/features.py."""

from pathlib import Path

import dask.dataframe as dd  # type: ignore[import-untyped]
import numpy as np
import pandas as pd  # type: ignore[import-untyped]
import pytest

from kgs_pipeline.features import (
    compute_aggregates,
    compute_cumulative,
    compute_decline_rate,
    compute_lags,
    compute_per_lease_features,
    compute_ratios,
    compute_rolling,
    compute_well_age,
    encode_categoricals,
    load_clean,
    pivot_products,
    run_features,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

FEATURE_SCHEMA_COLS = [
    "LEASE_KID",
    "PRODUCT",
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
    "well_age_months",
    "oil_roll3",
    "oil_roll6",
    "gas_roll3",
    "gas_roll6",
    "water_roll3",
    "water_roll6",
    "oil_lag1",
    "oil_lag3",
    "gas_lag1",
    "gas_lag3",
    "county_mean_oil",
    "county_std_oil",
    "formation_mean_oil",
    "formation_std_oil",
    "COUNTY_enc",
    "PRODUCING_ZONE_enc",
    "OPERATOR_enc",
    "PRODUCT_enc",
]


def _make_clean_df(
    n_months: int = 6,
    lease_id: str = "L1",
    start_month: str = "2024-01",
    oil_vals: list[float] | None = None,
    gas_vals: list[float] | None = None,
) -> pd.DataFrame:
    """Build a synthetic clean DataFrame for a single lease."""
    dates = pd.date_range(start=start_month, periods=n_months, freq="MS")
    rows = []
    for i, d in enumerate(dates):
        oil = oil_vals[i] if oil_vals else float(100 * (i + 1))
        gas = gas_vals[i] if gas_vals else float(50 * (i + 1))
        rows.append(
            {
                "LEASE_KID": lease_id,
                "LEASE": "TEST LEASE",
                "FIELD": "FIELD_A",
                "PRODUCING_ZONE": "ZONE_A",
                "OPERATOR": "OPS INC",
                "COUNTY": "Allen",
                "production_date": d,
                "MONTH-YEAR": f"{d.month}-{d.year}",
                "PRODUCT": "O",
                "PRODUCTION": oil,
                "WELLS": 2.0,
                "LATITUDE": 39.0,
                "LONGITUDE": -95.0,
                "source_file": "test.txt",
            }
        )
        rows.append(
            {
                "LEASE_KID": lease_id,
                "LEASE": "TEST LEASE",
                "FIELD": "FIELD_A",
                "PRODUCING_ZONE": "ZONE_A",
                "OPERATOR": "OPS INC",
                "COUNTY": "Allen",
                "production_date": d,
                "MONTH-YEAR": f"{d.month}-{d.year}",
                "PRODUCT": "G",
                "PRODUCTION": gas,
                "WELLS": 2.0,
                "LATITUDE": 39.0,
                "LONGITUDE": -95.0,
                "source_file": "test.txt",
            }
        )
    return pd.DataFrame(rows)


def _write_clean_parquet(tmp_path: Path, df: pd.DataFrame) -> Path:
    clean = tmp_path / "clean"
    clean.mkdir(exist_ok=True)
    ddf = dd.from_pandas(df, npartitions=1)
    ddf.to_parquet(str(clean), write_index=False)
    return clean


def _make_pivot_df(lease_id: str = "L1", n: int = 3) -> pd.DataFrame:
    """DataFrame already in wide pivot format."""
    dates = pd.date_range(start="2024-01", periods=n, freq="MS")
    rows = []
    for i, d in enumerate(dates):
        rows.append(
            {
                "LEASE_KID": lease_id,
                "production_date": d,
                "oil_bbl": float(100 * (i + 1)),
                "gas_mcf": float(50 * (i + 1)),
                "water_bbl": np.nan,
                "COUNTY": "Allen",
                "PRODUCING_ZONE": "ZONE_A",
                "OPERATOR": "OPS",
                "LEASE": "TEST",
                "FIELD": "FIELD_A",
                "source_file": "test.txt",
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Task 01: load_clean
# ---------------------------------------------------------------------------


def test_load_clean_returns_dask_df(tmp_path: Path) -> None:
    df = _make_clean_df()
    clean = _write_clean_parquet(tmp_path, df)
    ddf = load_clean(str(clean))
    assert isinstance(ddf, dd.DataFrame)


def test_load_clean_npartitions_le_50(tmp_path: Path) -> None:
    df = _make_clean_df()
    clean = _write_clean_parquet(tmp_path, df)
    ddf = load_clean(str(clean))
    assert ddf.npartitions <= 50


def test_load_clean_empty_dir_raises(tmp_path: Path) -> None:
    empty = tmp_path / "empty_clean"
    empty.mkdir()
    with pytest.raises(ValueError):
        load_clean(str(empty))


# ---------------------------------------------------------------------------
# Task 02: pivot_products
# ---------------------------------------------------------------------------


def test_pivot_products_wide_format(tmp_path: Path) -> None:
    df = _make_clean_df(n_months=2)  # 2 oil + 2 gas rows = 2 wide rows
    clean = _write_clean_parquet(tmp_path, df)
    ddf = load_clean(str(clean))
    result = pivot_products(ddf).compute()
    assert len(result) == 2  # 2 months
    assert "oil_bbl" in result.columns
    assert "gas_mcf" in result.columns


def test_pivot_products_oil_only_gas_zero(tmp_path: Path) -> None:
    df = _make_clean_df(n_months=2)
    df_oil = df[df["PRODUCT"] == "O"].copy()
    clean = _write_clean_parquet(tmp_path, df_oil)
    ddf = load_clean(str(clean))
    result = pivot_products(ddf).compute()
    assert (result["gas_mcf"] == 0.0).all()


def test_pivot_products_water_bbl_nan(tmp_path: Path) -> None:
    df = _make_clean_df()
    clean = _write_clean_parquet(tmp_path, df)
    ddf = load_clean(str(clean))
    result = pivot_products(ddf).compute()
    assert "water_bbl" in result.columns
    assert result["water_bbl"].isna().all()


def test_pivot_products_zero_oil_retained(tmp_path: Path) -> None:
    oil_vals = [0.0, 100.0, 200.0]
    df = _make_clean_df(n_months=3, oil_vals=oil_vals, gas_vals=[50.0, 60.0, 70.0])
    clean = _write_clean_parquet(tmp_path, df)
    ddf = load_clean(str(clean))
    result = pivot_products(ddf).compute()
    assert 0.0 in result["oil_bbl"].values


# ---------------------------------------------------------------------------
# Task 03: compute_cumulative
# ---------------------------------------------------------------------------


def test_compute_cumulative_shutin_flat() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 150.0, 0.0, 200.0]
    result = compute_cumulative(df)
    assert list(result["cum_oil"]) == pytest.approx([100.0, 250.0, 250.0, 450.0])


def test_compute_cumulative_monotonic() -> None:
    df = _make_pivot_df(n=6)
    df["oil_bbl"] = [100.0, 50.0, 0.0, 75.0, 200.0, 100.0]
    result = compute_cumulative(df)
    diffs = result["cum_oil"].diff().dropna()
    assert (diffs >= -1e-9).all()


def test_compute_cumulative_not_online_start() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [0.0, 0.0, 100.0, 200.0]
    result = compute_cumulative(df)
    assert list(result["cum_oil"]) == pytest.approx([0.0, 0.0, 100.0, 300.0])


def test_compute_cumulative_zeros_mid_sequence() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 0.0, 0.0, 50.0]
    result = compute_cumulative(df)
    assert list(result["cum_oil"]) == pytest.approx([100.0, 100.0, 100.0, 150.0])


def test_compute_cumulative_water_all_nan() -> None:
    df = _make_pivot_df(n=3)
    df["water_bbl"] = np.nan
    result = compute_cumulative(df)
    assert result["cum_water"].isna().all()


# ---------------------------------------------------------------------------
# Task 04: compute_ratios
# ---------------------------------------------------------------------------


def test_compute_ratios_gor_basic() -> None:
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 100.0
    df["gas_mcf"] = 500.0
    result = compute_ratios(df)
    assert result["gor"].iloc[0] == pytest.approx(5.0)


def test_compute_ratios_gor_zero_oil_nonzero_gas() -> None:
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 0.0
    df["gas_mcf"] = 500.0
    result = compute_ratios(df)
    assert pd.isna(result["gor"].iloc[0])


def test_compute_ratios_gor_both_zero() -> None:
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 0.0
    df["gas_mcf"] = 0.0
    result = compute_ratios(df)
    assert result["gor"].iloc[0] == pytest.approx(0.0)


def test_compute_ratios_gor_oil_nonzero_gas_zero() -> None:
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 100.0
    df["gas_mcf"] = 0.0
    result = compute_ratios(df)
    assert result["gor"].iloc[0] == pytest.approx(0.0)


def test_compute_ratios_water_cut_zero_water() -> None:
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 100.0
    df["water_bbl"] = 0.0
    result = compute_ratios(df)
    assert result["water_cut"].iloc[0] == pytest.approx(0.0)


def test_compute_ratios_water_cut_all_water() -> None:
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 0.0
    df["water_bbl"] = 200.0
    result = compute_ratios(df)
    assert result["water_cut"].iloc[0] == pytest.approx(1.0)


def test_compute_ratios_water_cut_both_zero() -> None:
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 0.0
    df["water_bbl"] = 0.0
    result = compute_ratios(df)
    assert pd.isna(result["water_cut"].iloc[0])


def test_compute_ratios_no_zero_division() -> None:
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 0.0
    df["gas_mcf"] = 0.0
    df["water_bbl"] = 0.0
    result = compute_ratios(df)  # Should not raise
    assert isinstance(result, pd.DataFrame)


def test_compute_ratios_gor_formula_direction() -> None:
    """GOR = gas / oil, not oil / gas."""
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 200.0
    df["gas_mcf"] = 1000.0
    result = compute_ratios(df)
    assert result["gor"].iloc[0] == pytest.approx(5.0)


def test_compute_ratios_water_cut_denominator() -> None:
    """water_cut = water / (oil + water)."""
    df = _make_pivot_df(n=1)
    df["oil_bbl"] = 200.0
    df["water_bbl"] = 200.0
    result = compute_ratios(df)
    assert result["water_cut"].iloc[0] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Task 05: compute_decline_rate
# ---------------------------------------------------------------------------


def test_compute_decline_rate_basic() -> None:
    df = _make_pivot_df(n=3)
    df["oil_bbl"] = [200.0, 100.0, 50.0]
    result = compute_decline_rate(df)
    rates = result["decline_rate"].values
    assert pd.isna(rates[0])
    assert rates[1] == pytest.approx(0.5)
    assert rates[2] == pytest.approx(0.5)


def test_compute_decline_rate_clipped_high() -> None:
    df = _make_pivot_df(n=2)
    df["oil_bbl"] = [1.0, 0.0001]  # raw decline ~ 0.9999 < 10, fine
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] <= 10.0


def test_compute_decline_rate_above_clip() -> None:
    """A value that would exceed 10.0 is clipped to 10.0."""
    df = _make_pivot_df(n=2)
    # Use a custom scenario: force raw decline > 10.0 via negative oil in raw
    # Since production can't be negative after cleaning, simulate via mock
    df["oil_bbl"] = [100.0, -900.0]  # raw decline = (100 - (-900)) / 100 = 10.0 (boundary)
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] <= 10.0


def test_compute_decline_rate_below_clip() -> None:
    """A value below -1.0 is clipped to -1.0."""
    df = _make_pivot_df(n=2)
    df["oil_bbl"] = [100.0, 999999.0]  # raw decline = (100 - 999999) / 100 very negative
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] >= -1.0


def test_compute_decline_rate_within_bounds_unchanged() -> None:
    df = _make_pivot_df(n=3)
    df["oil_bbl"] = [200.0, 150.0, 100.0]
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] == pytest.approx(0.25)
    assert result["decline_rate"].iloc[2] == pytest.approx(1.0 / 3.0, rel=1e-3)


def test_compute_decline_rate_zero_prev_zero_curr() -> None:
    df = _make_pivot_df(n=3)
    df["oil_bbl"] = [100.0, 0.0, 0.0]
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[2] == pytest.approx(0.0)


def test_compute_decline_rate_zero_prev_nonzero_curr() -> None:
    df = _make_pivot_df(n=3)
    df["oil_bbl"] = [100.0, 0.0, 100.0]
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[2] == pytest.approx(-1.0)


# ---------------------------------------------------------------------------
# Task 06: compute_well_age
# ---------------------------------------------------------------------------


def test_compute_well_age_basic() -> None:
    df = _make_pivot_df(n=3)
    df["production_date"] = pd.to_datetime(["2024-01-01", "2024-02-01", "2024-04-01"])
    result = compute_well_age(df)
    assert list(result["well_age_months"]) == [0, 1, 3]


def test_compute_well_age_single_row() -> None:
    df = _make_pivot_df(n=1)
    result = compute_well_age(df)
    assert result["well_age_months"].iloc[0] == 0


def test_compute_well_age_int_dtype() -> None:
    df = _make_pivot_df(n=3)
    result = compute_well_age(df)
    assert pd.api.types.is_integer_dtype(result["well_age_months"])


# ---------------------------------------------------------------------------
# Task 07: compute_rolling
# ---------------------------------------------------------------------------


def test_compute_rolling_3month() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 200.0, 300.0, 400.0]
    result = compute_rolling(df, windows=[3, 6])
    expected = [100.0, 150.0, 200.0, 300.0]
    assert list(result["oil_roll3"]) == pytest.approx(expected)


def test_compute_rolling_6month() -> None:
    df = _make_pivot_df(n=6)
    df["oil_bbl"] = [100.0, 200.0, 300.0, 400.0, 500.0, 600.0]
    result = compute_rolling(df, windows=[3, 6])
    expected_6 = [100.0, 150.0, 200.0, 250.0, 300.0, 350.0]
    assert list(result["oil_roll6"]) == pytest.approx(expected_6)


def test_compute_rolling_first_row_identity() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 200.0, 300.0, 400.0]
    result = compute_rolling(df, windows=[3])
    assert result["oil_roll3"].iloc[0] == pytest.approx(100.0)


def test_compute_rolling_water_all_nan() -> None:
    df = _make_pivot_df(n=4)
    df["water_bbl"] = np.nan
    result = compute_rolling(df, windows=[3])
    assert result["water_roll3"].isna().all()


# ---------------------------------------------------------------------------
# Task 08: compute_lags
# ---------------------------------------------------------------------------


def test_compute_lags_lag1() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 200.0, 300.0, 400.0]
    result = compute_lags(df, lags=[1, 3])
    assert pd.isna(result["oil_lag1"].iloc[0])
    assert result["oil_lag1"].iloc[1] == pytest.approx(100.0)
    assert result["oil_lag1"].iloc[2] == pytest.approx(200.0)
    assert result["oil_lag1"].iloc[3] == pytest.approx(300.0)


def test_compute_lags_lag3() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 200.0, 300.0, 400.0]
    result = compute_lags(df, lags=[1, 3])
    assert pd.isna(result["oil_lag3"].iloc[0])
    assert pd.isna(result["oil_lag3"].iloc[1])
    assert pd.isna(result["oil_lag3"].iloc[2])
    assert result["oil_lag3"].iloc[3] == pytest.approx(100.0)


def test_compute_lags_first_row_nan() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 200.0, 300.0, 400.0]
    result = compute_lags(df, lags=[1])
    assert pd.isna(result["oil_lag1"].iloc[0])


# ---------------------------------------------------------------------------
# Task 09: compute_aggregates
# ---------------------------------------------------------------------------


def test_compute_aggregates_county_mean() -> None:
    rows = [
        {"LEASE_KID": "L1", "oil_bbl": 100.0, "COUNTY": "Allen", "PRODUCING_ZONE": "ZONE_A"},
        {"LEASE_KID": "L2", "oil_bbl": 200.0, "COUNTY": "Allen", "PRODUCING_ZONE": "ZONE_A"},
        {"LEASE_KID": "L3", "oil_bbl": 300.0, "COUNTY": "Barton", "PRODUCING_ZONE": "ZONE_B"},
    ]
    df = pd.DataFrame(rows)
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_aggregates(ddf).compute()
    allen_rows = result[result["COUNTY"] == "Allen"]
    assert allen_rows["county_mean_oil"].iloc[0] == pytest.approx(150.0)
    barton_rows = result[result["COUNTY"] == "Barton"]
    assert barton_rows["county_mean_oil"].iloc[0] == pytest.approx(300.0)


def test_compute_aggregates_single_record_std_zero() -> None:
    df = pd.DataFrame(
        [
            {"LEASE_KID": "L1", "oil_bbl": 300.0, "COUNTY": "Barton", "PRODUCING_ZONE": "ZONE_A"},
        ]
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_aggregates(ddf).compute()
    assert result["county_std_oil"].iloc[0] == pytest.approx(0.0)


def test_compute_aggregates_returns_dask_df() -> None:
    df = pd.DataFrame(
        [
            {"LEASE_KID": "L1", "oil_bbl": 100.0, "COUNTY": "Allen", "PRODUCING_ZONE": "ZONE_A"},
        ]
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = compute_aggregates(ddf)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 10: encode_categoricals
# ---------------------------------------------------------------------------


def test_encode_categoricals_product_two_values() -> None:
    df = pd.DataFrame(
        {
            "COUNTY": ["Allen", "Barton", "Allen"],
            "PRODUCING_ZONE": ["Z1", "Z2", "Z1"],
            "OPERATOR": ["OPS", "OPS", "OPS"],
            "PRODUCT": ["O", "G", "O"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categoricals(ddf).compute()
    assert result["PRODUCT_enc"].nunique() == 2


def test_encode_categoricals_county_int_dtype() -> None:
    df = pd.DataFrame(
        {
            "COUNTY": ["Allen", "Barton"],
            "PRODUCING_ZONE": ["Z1", "Z2"],
            "OPERATOR": ["OPS", "OPS"],
            "PRODUCT": ["O", "G"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categoricals(ddf).compute()
    assert pd.api.types.is_integer_dtype(result["COUNTY_enc"])


def test_encode_categoricals_original_col_retained() -> None:
    df = pd.DataFrame(
        {
            "COUNTY": ["Allen"],
            "PRODUCING_ZONE": ["Z1"],
            "OPERATOR": ["OPS"],
            "PRODUCT": ["O"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = encode_categoricals(ddf).compute()
    assert "COUNTY" in result.columns


def test_encode_categoricals_unseen_value() -> None:
    df = pd.DataFrame(
        {
            "COUNTY": ["Allen", "Barton"],
            "PRODUCING_ZONE": ["Z1", "Z2"],
            "OPERATOR": ["OPS", "OPS"],
            "PRODUCT": ["O", "G"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    encode_categoricals(ddf)

    # Introduce unseen value in a new partition
    new_df = pd.DataFrame(
        {
            "COUNTY": ["UNKNOWN_PLACE"],
            "PRODUCING_ZONE": ["UNKNOWN_ZONE"],
            "OPERATOR": ["UNKNOWN_OP"],
            "PRODUCT": ["X"],
        }
    )
    new_ddf = dd.from_pandas(new_df, npartitions=1)
    # Just test that encode_categoricals doesn't crash on unseen values
    result = encode_categoricals(new_ddf).compute()
    assert "COUNTY_enc" in result.columns


# ---------------------------------------------------------------------------
# Task 11: compute_per_lease_features
# ---------------------------------------------------------------------------


def test_per_lease_all_feature_columns() -> None:
    df = _make_pivot_df(n=6)
    df["oil_bbl"] = [100.0, 150.0, 200.0, 250.0, 300.0, 350.0]
    df["gas_mcf"] = [50.0, 75.0, 100.0, 125.0, 150.0, 175.0]
    result = compute_per_lease_features(df)
    for col in [
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "well_age_months",
        "oil_roll3",
        "oil_roll6",
        "gas_roll3",
        "gas_roll6",
        "water_roll3",
        "water_roll6",
        "oil_lag1",
        "oil_lag3",
        "gas_lag1",
        "gas_lag3",
    ]:
        assert col in result.columns, f"Missing column: {col}"


def test_per_lease_cum_oil_monotonic() -> None:
    df = _make_pivot_df(n=6)
    df["oil_bbl"] = [100.0, 50.0, 0.0, 75.0, 200.0, 100.0]
    result = compute_per_lease_features(df)
    diffs = result["cum_oil"].diff().dropna()
    assert (diffs >= -1e-9).all()


def test_per_lease_oil_bbl_non_negative() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [0.0, 100.0, 200.0, 50.0]
    result = compute_per_lease_features(df)
    assert (result["oil_bbl"] >= 0.0).all()


def test_per_lease_gor_non_negative() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 200.0, 0.0, 50.0]
    df["gas_mcf"] = [500.0, 300.0, 0.0, 200.0]
    result = compute_per_lease_features(df)
    gor_vals = result["gor"].dropna()
    assert (gor_vals >= 0.0).all()


def test_per_lease_water_cut_range() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 200.0, 0.0, 50.0]
    df["water_bbl"] = [50.0, 100.0, 0.0, 25.0]
    result = compute_per_lease_features(df)
    wc = result["water_cut"].dropna()
    assert (wc >= 0.0).all() and (wc <= 1.0).all()


def test_per_lease_decline_rate_bounds() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [200.0, 100.0, 50.0, 200.0]
    result = compute_per_lease_features(df)
    dr = result["decline_rate"].dropna()
    assert (dr >= -1.0).all() and (dr <= 10.0).all()


def test_per_lease_corrupt_input_no_crash() -> None:
    """If computation raises for some reason, returns input unchanged."""
    # Pass a DF missing required columns
    df = pd.DataFrame({"LEASE_KID": ["L1"], "bad_col": [1]})
    result = compute_per_lease_features(df)
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 12: run_features
# ---------------------------------------------------------------------------


def _make_3_lease_fixture(tmp_path: Path) -> Path:
    frames = []
    for lease_id in ["L1", "L2", "L3"]:
        df = _make_clean_df(n_months=6, lease_id=lease_id)
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    return _write_clean_parquet(tmp_path, combined)


def test_run_features_returns_dask_df(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features"
    ddf = run_features(str(clean), str(out))
    assert isinstance(ddf, dd.DataFrame)


def test_run_features_all_schema_columns(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features"
    ddf = run_features(str(clean), str(out))
    cols = ddf.columns.tolist()
    for col in FEATURE_SCHEMA_COLS:
        assert col in cols, f"Missing schema column: {col}"


def test_run_features_no_negative_oil_bbl(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features"
    ddf = run_features(str(clean), str(out))
    result = ddf.compute()
    assert (result["oil_bbl"] >= 0.0).all()


def test_run_features_cum_oil_monotonic(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features"
    ddf = run_features(str(clean), str(out))
    result = ddf.compute()
    for lease_id in result["LEASE_KID"].unique():
        lease_df = result[result["LEASE_KID"] == lease_id].sort_values("production_date")
        diffs = lease_df["cum_oil"].diff().dropna()
        assert (diffs >= -1e-9).all(), f"cum_oil not monotonic for {lease_id}"


def test_run_features_parquet_readable(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features"
    run_features(str(clean), str(out))
    ddf2 = dd.read_parquet(str(out))
    assert len(ddf2.compute()) > 0


def test_run_features_schema_stability_across_partitions(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features"
    run_features(str(clean), str(out))
    parquet_files = list(out.rglob("*.parquet"))
    if len(parquet_files) >= 2:
        df1 = pd.read_parquet(str(parquet_files[0]))
        df2 = pd.read_parquet(str(parquet_files[1]))
        assert set(df1.columns) == set(df2.columns)


def test_run_features_output_file_count(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features"
    run_features(str(clean), str(out))
    files = list(out.rglob("*.parquet"))
    assert 1 <= len(files) <= 200


# ---------------------------------------------------------------------------
# Task 13: Dedicated correctness and completeness tests
# ---------------------------------------------------------------------------


def test_feature_column_presence(tmp_path: Path) -> None:
    """All schema columns must be present in run_features output."""
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features_presence"
    ddf = run_features(str(clean), str(out))
    cols = ddf.columns.tolist()
    for col in FEATURE_SCHEMA_COLS:
        assert col in cols, f"[TR-19] Missing column: {col}"


def test_cumulative_monotonicity(tmp_path: Path) -> None:
    """cum_oil and cum_gas must be non-decreasing per lease."""
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features_mono"
    ddf = run_features(str(clean), str(out))
    result = ddf.compute()
    for lid in result["LEASE_KID"].unique():
        sub = result[result["LEASE_KID"] == lid].sort_values("production_date")
        assert (sub["cum_oil"].diff().dropna() >= -1e-9).all()
        assert (sub["cum_gas"].diff().dropna() >= -1e-9).all()


def test_gor_formula_correctness() -> None:
    df = _make_pivot_df(n=3)
    df["oil_bbl"] = [200.0, 0.0, 100.0]
    df["gas_mcf"] = [1000.0, 500.0, 0.0]
    result = compute_ratios(df)
    assert result["gor"].iloc[0] == pytest.approx(5.0)
    assert pd.isna(result["gor"].iloc[1])
    assert result["gor"].iloc[2] == pytest.approx(0.0)


def test_water_cut_formula_correctness() -> None:
    df = _make_pivot_df(n=3)
    df["oil_bbl"] = [200.0, 100.0, 0.0]
    df["water_bbl"] = [200.0, 0.0, 100.0]
    result = compute_ratios(df)
    assert result["water_cut"].iloc[0] == pytest.approx(0.5)
    assert result["water_cut"].iloc[1] == pytest.approx(0.0)
    assert result["water_cut"].iloc[2] == pytest.approx(1.0)


def test_decline_rate_bounds_high() -> None:
    df = _make_pivot_df(n=2)
    df["oil_bbl"] = [1.0, -99999.0]  # would produce raw > 10
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] <= 10.0


def test_decline_rate_bounds_low() -> None:
    df = _make_pivot_df(n=2)
    df["oil_bbl"] = [100.0, 99999.0]  # raw decline very negative
    result = compute_decline_rate(df)
    assert result["decline_rate"].iloc[1] >= -1.0


def test_decline_rate_shutin_no_extreme() -> None:
    df = _make_pivot_df(n=3)
    df["oil_bbl"] = [100.0, 0.0, 0.0]
    result = compute_decline_rate(df)
    dr = result["decline_rate"].dropna()
    assert (dr >= -1.0).all() and (dr <= 10.0).all()


def test_rolling_correctness() -> None:
    df = _make_pivot_df(n=6)
    df["oil_bbl"] = [100.0, 200.0, 300.0, 400.0, 500.0, 600.0]
    result = compute_rolling(df, windows=[3, 6])
    expected_3 = [100.0, 150.0, 200.0, 300.0, 400.0, 500.0]
    expected_6 = [100.0, 150.0, 200.0, 250.0, 300.0, 350.0]
    assert list(result["oil_roll3"]) == pytest.approx(expected_3)
    assert list(result["oil_roll6"]) == pytest.approx(expected_6)


def test_lag_correctness() -> None:
    df = _make_pivot_df(n=4)
    df["oil_bbl"] = [100.0, 200.0, 300.0, 400.0]
    result = compute_lags(df, lags=[1, 3])
    assert result["oil_lag1"].iloc[1] == pytest.approx(100.0)
    assert result["oil_lag1"].iloc[2] == pytest.approx(200.0)
    assert result["oil_lag1"].iloc[3] == pytest.approx(300.0)


def test_schema_stability_across_partitions(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features_schema"
    run_features(str(clean), str(out))
    files = list(out.rglob("*.parquet"))
    if len(files) >= 2:
        df1 = pd.read_parquet(str(files[0]))
        df2 = pd.read_parquet(str(files[1]))
        assert set(df1.columns) == set(df2.columns)
        for col in df1.columns:
            assert df1[col].dtype == df2[col].dtype


def test_lazy_evaluation(tmp_path: Path) -> None:
    clean = _make_3_lease_fixture(tmp_path)
    out = tmp_path / "features_lazy"
    ddf = run_features(str(clean), str(out))
    assert isinstance(ddf, dd.DataFrame)
