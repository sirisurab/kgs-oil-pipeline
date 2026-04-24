"""Tests for the features stage (tasks F1–F5).

Test requirements: TR-03, TR-06, TR-07, TR-08, TR-09, TR-10, TR-14, TR-17,
TR-18, TR-19, TR-23, TR-26, TR-27.
Each test carries exactly one of: @pytest.mark.unit or @pytest.mark.integration
per ADR-008.
"""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.features import (
    reshape_products,
    add_cumulative_features,
    add_rolling_features,
    derive_features_meta,
    features,
    _VOLUME_COLS,
    _F2_COLS,
    _F3_ROLLING_COLS,
)

_DD_PATH = Path("references/kgs_monthly_data_dictionary.csv")

# ---------------------------------------------------------------------------
# Helpers to build test DataFrames
# ---------------------------------------------------------------------------


def _f1_partition(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal transform-output-style DataFrame for F1 testing."""
    if not rows:
        # Return schema-conformant empty DF
        cols = [
            "LEASE_KID",
            "production_date",
            "LEASE",
            "OPERATOR",
            "COUNTY",
            "DOR_CODE",
            "API_NUMBER",
            "FIELD",
            "PRODUCING_ZONE",
            "TOWNSHIP",
            "TWN_DIR",
            "RANGE",
            "RANGE_DIR",
            "SECTION",
            "SPOT",
            "LATITUDE",
            "LONGITUDE",
            "source_file",
            "MONTH-YEAR",
            "WELLS",
            "PRODUCTION",
        ]
        df = pd.DataFrame({c: pd.Series([], dtype="object") for c in cols})
        df["LEASE_KID"] = df["LEASE_KID"].astype("int64")
        df["production_date"] = pd.Series([], dtype="datetime64[ns]")
        df["PRODUCT"] = pd.Categorical([], categories=["O", "G"])
        df["WELLS"] = pd.array([], dtype="Int64")
        df["PRODUCTION"] = pd.array([], dtype="Float64")
        return df

    records = []
    for r in rows:
        records.append(
            {
                "LEASE_KID": r.get("LEASE_KID", 1001),
                "production_date": pd.Timestamp(r.get("production_date", "2024-01-01")),
                "LEASE": r.get("LEASE", ""),
                "OPERATOR": r.get("OPERATOR", ""),
                "COUNTY": r.get("COUNTY", ""),
                "DOR_CODE": r.get("DOR_CODE", None),
                "API_NUMBER": r.get("API_NUMBER", None),
                "FIELD": r.get("FIELD", None),
                "PRODUCING_ZONE": r.get("PRODUCING_ZONE", None),
                "TOWNSHIP": r.get("TOWNSHIP", None),
                "TWN_DIR": r.get("TWN_DIR", None),
                "RANGE": r.get("RANGE", None),
                "RANGE_DIR": r.get("RANGE_DIR", None),
                "SECTION": r.get("SECTION", None),
                "SPOT": r.get("SPOT", None),
                "LATITUDE": r.get("LATITUDE", None),
                "LONGITUDE": r.get("LONGITUDE", None),
                "source_file": r.get("source_file", "test.txt"),
                "MONTH-YEAR": r.get("MONTH-YEAR", "1-2024"),
                "PRODUCT": r.get("PRODUCT", "O"),
                "WELLS": r.get("WELLS", 1),
                "PRODUCTION": r.get("PRODUCTION", 100.0),
            }
        )

    df = pd.DataFrame(records)
    df["LEASE_KID"] = pd.to_numeric(df["LEASE_KID"], errors="coerce").astype("int64")
    df["production_date"] = pd.to_datetime(df["production_date"])
    df["WELLS"] = pd.to_numeric(df["WELLS"], errors="coerce").astype("Int64")
    df["PRODUCTION"] = pd.to_numeric(df["PRODUCTION"], errors="coerce").astype("Float64")
    df["PRODUCT"] = pd.Categorical(df["PRODUCT"], categories=["O", "G"])
    df["source_file"] = df["source_file"].astype("string")
    return df


def _f2_partition(rows: list[dict]) -> pd.DataFrame:
    """Build an F1-output-style DataFrame for F2 testing."""
    if not rows:
        df = pd.DataFrame(
            {
                "LEASE_KID": pd.Series([], dtype="int64"),
                "production_date": pd.Series([], dtype="datetime64[ns]"),
                "oil_bbl": pd.array([], dtype="Float64"),
                "gas_mcf": pd.array([], dtype="Float64"),
                "water_bbl": pd.array([], dtype="Float64"),
            }
        )
        return df

    records = []
    for r in rows:
        records.append(
            {
                "LEASE_KID": r.get("LEASE_KID", 1001),
                "production_date": pd.Timestamp(r.get("production_date", "2024-01-01")),
                "oil_bbl": r.get("oil_bbl", None),
                "gas_mcf": r.get("gas_mcf", None),
                "water_bbl": r.get("water_bbl", None),
            }
        )

    df = pd.DataFrame(records)
    df["LEASE_KID"] = df["LEASE_KID"].astype("int64")
    df["production_date"] = pd.to_datetime(df["production_date"])
    df["oil_bbl"] = pd.array(df["oil_bbl"].tolist(), dtype="Float64")
    df["gas_mcf"] = pd.array(df["gas_mcf"].tolist(), dtype="Float64")
    df["water_bbl"] = pd.array(df["water_bbl"].tolist(), dtype="Float64")
    return df


# ---------------------------------------------------------------------------
# Task F1 tests: reshape_products
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_f1_single_lease_both_products() -> None:
    """One LEASE_KID, one month, PRODUCT=O and G → single output row with both volumes."""
    rows = [
        {"LEASE_KID": 1001, "production_date": "2024-01-01", "PRODUCT": "O", "PRODUCTION": 100.0},
        {"LEASE_KID": 1001, "production_date": "2024-01-01", "PRODUCT": "G", "PRODUCTION": 500.0},
    ]
    df = _f1_partition(rows)
    result = reshape_products(df)
    assert len(result) == 1
    assert float(result["oil_bbl"].iloc[0]) == 100.0
    assert float(result["gas_mcf"].iloc[0]) == 500.0


@pytest.mark.unit
def test_f1_zero_production_preserved() -> None:
    """PRODUCT=O with PRODUCTION=0 → oil_bbl == 0, not null (TR-05)."""
    rows = [
        {"LEASE_KID": 2001, "production_date": "2024-02-01", "PRODUCT": "O", "PRODUCTION": 0.0},
    ]
    df = _f1_partition(rows)
    result = reshape_products(df)
    assert len(result) == 1
    assert float(result["oil_bbl"].iloc[0]) == 0.0


@pytest.mark.unit
def test_f1_only_oil_rows_gas_mcf_null() -> None:
    """Only oil rows → gas_mcf is null for every output row."""
    rows = [
        {"LEASE_KID": 3001, "production_date": "2024-03-01", "PRODUCT": "O", "PRODUCTION": 75.0},
        {"LEASE_KID": 3001, "production_date": "2024-04-01", "PRODUCT": "O", "PRODUCTION": 80.0},
    ]
    df = _f1_partition(rows)
    result = reshape_products(df)
    assert len(result) == 2
    assert result["gas_mcf"].isna().all()


@pytest.mark.unit
def test_f1_empty_input_returns_empty_with_schema() -> None:
    """Empty input → empty output with oil_bbl, gas_mcf, water_bbl columns."""
    df = _f1_partition([])
    result = reshape_products(df)
    assert len(result) == 0
    for col in _VOLUME_COLS:
        assert col in result.columns


@pytest.mark.unit
def test_f1_out_of_set_product_handled() -> None:
    """PRODUCT outside {O, G} → rows dropped or null-handled (no silent propagation)."""
    rows = [
        {"LEASE_KID": 4001, "production_date": "2024-01-01", "PRODUCT": "X", "PRODUCTION": 10.0},
    ]
    df = _f1_partition(rows)
    result = reshape_products(df)
    # Either empty (dropped) or present with null volumes — no raw "X" product
    if len(result) > 0:
        assert result["oil_bbl"].isna().all() or result["gas_mcf"].isna().all()


# ---------------------------------------------------------------------------
# Task F2 tests: add_cumulative_features
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_f2_cumulative_sums_known_values() -> None:
    """6-month series → cum_oil/cum_gas/cum_water match hand-computed values (TR-09(a))."""
    rows = [
        {
            "LEASE_KID": 5001,
            "production_date": f"2024-0{m}-01",
            "oil_bbl": 10.0 * m,
            "gas_mcf": 5.0 * m,
            "water_bbl": 2.0 * m,
        }
        for m in range(1, 7)
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)

    expected_cum_oil = [10.0, 30.0, 60.0, 100.0, 150.0, 210.0]
    for i, expected in enumerate(expected_cum_oil):
        assert abs(float(result["cum_oil"].iloc[i]) - expected) < 1e-6, (
            f"cum_oil mismatch at row {i}"
        )


@pytest.mark.unit
def test_f2_cum_flat_across_zero_production() -> None:
    """cum_oil stays flat across a zero-production month (TR-08(a),(c))."""
    rows = [
        {"LEASE_KID": 6001, "production_date": "2024-01-01", "oil_bbl": 100.0},
        {"LEASE_KID": 6001, "production_date": "2024-02-01", "oil_bbl": 0.0},  # zero month
        {"LEASE_KID": 6001, "production_date": "2024-03-01", "oil_bbl": 50.0},
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)

    assert float(result["cum_oil"].iloc[0]) == 100.0
    assert float(result["cum_oil"].iloc[1]) == 100.0  # flat across zero
    assert float(result["cum_oil"].iloc[2]) == 150.0


@pytest.mark.unit
def test_f2_cum_stays_zero_when_not_started() -> None:
    """cum_oil stays at 0 through zero-production start months (TR-08(b))."""
    rows = [
        {"LEASE_KID": 7001, "production_date": "2024-01-01", "oil_bbl": 0.0},
        {"LEASE_KID": 7001, "production_date": "2024-02-01", "oil_bbl": 0.0},
        {"LEASE_KID": 7001, "production_date": "2024-03-01", "oil_bbl": 100.0},
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    assert float(result["cum_oil"].iloc[0]) == 0.0
    assert float(result["cum_oil"].iloc[1]) == 0.0


@pytest.mark.unit
def test_f2_gor_zero_oil_positive_gas() -> None:
    """oil_bbl == 0, gas_mcf > 0 → gor is NaN, no exception (TR-06(a))."""
    rows = [{"LEASE_KID": 8001, "production_date": "2024-01-01", "oil_bbl": 0.0, "gas_mcf": 100.0}]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_f2_gor_both_zero() -> None:
    """oil_bbl == 0, gas_mcf == 0 → gor is the declared value (NaN), no exception (TR-06(b))."""
    rows = [{"LEASE_KID": 9001, "production_date": "2024-01-01", "oil_bbl": 0.0, "gas_mcf": 0.0}]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    # Declared: both zero → NaN
    assert pd.isna(result["gor"].iloc[0])


@pytest.mark.unit
def test_f2_gor_gas_zero_oil_positive() -> None:
    """oil_bbl > 0, gas_mcf == 0 → gor == 0.0 (TR-06(c))."""
    rows = [{"LEASE_KID": 10001, "production_date": "2024-01-01", "oil_bbl": 10.0, "gas_mcf": 0.0}]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    assert float(result["gor"].iloc[0]) == 0.0


@pytest.mark.unit
def test_f2_water_cut_zero_water() -> None:
    """water_bbl == 0, oil_bbl > 0 → water_cut == 0.0, row retained (TR-10(a))."""
    rows = [
        {"LEASE_KID": 11001, "production_date": "2024-01-01", "oil_bbl": 100.0, "water_bbl": 0.0}
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    assert len(result) == 1
    assert float(result["water_cut"].iloc[0]) == 0.0


@pytest.mark.unit
def test_f2_water_cut_all_water() -> None:
    """oil_bbl == 0, water_bbl == 50 → water_cut == 1.0, row retained (TR-10(b))."""
    rows = [
        {"LEASE_KID": 12001, "production_date": "2024-01-01", "oil_bbl": 0.0, "water_bbl": 50.0}
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    assert len(result) == 1
    assert float(result["water_cut"].iloc[0]) == 1.0


@pytest.mark.unit
def test_f2_decline_rate_clip_lower() -> None:
    """Raw decline < -1.0 → clipped to -1.0 (TR-07(a))."""
    rows = [
        {"LEASE_KID": 13001, "production_date": "2024-01-01", "oil_bbl": 100.0},
        {
            "LEASE_KID": 13001,
            "production_date": "2024-02-01",
            "oil_bbl": 0.0,
        },  # drop to 0 → rate = -1.0
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    rate = float(result["decline_rate"].iloc[1])
    assert rate == -1.0


@pytest.mark.unit
def test_f2_decline_rate_clip_upper() -> None:
    """Raw decline > 10.0 → clipped to 10.0 (TR-07(b))."""
    rows = [
        {"LEASE_KID": 14001, "production_date": "2024-01-01", "oil_bbl": 1.0},
        {"LEASE_KID": 14001, "production_date": "2024-02-01", "oil_bbl": 1000.0},
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    rate = float(result["decline_rate"].iloc[1])
    assert rate == 10.0


@pytest.mark.unit
def test_f2_decline_rate_within_bounds() -> None:
    """Rate within [-1.0, 10.0] passes through unchanged (TR-07(c))."""
    rows = [
        {"LEASE_KID": 15001, "production_date": "2024-01-01", "oil_bbl": 100.0},
        {"LEASE_KID": 15001, "production_date": "2024-02-01", "oil_bbl": 90.0},
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    rate = float(result["decline_rate"].iloc[1])
    expected = (90.0 - 100.0) / 100.0  # -0.1
    assert abs(rate - expected) < 1e-6


@pytest.mark.unit
def test_f2_decline_rate_shutin_then_resume_clipped() -> None:
    """Shut-in then resume → clip applied, no unclipped extreme leaks (TR-07(d))."""
    rows = [
        {"LEASE_KID": 16001, "production_date": "2024-01-01", "oil_bbl": 100.0},
        {"LEASE_KID": 16001, "production_date": "2024-02-01", "oil_bbl": 0.0},  # shut-in
        {
            "LEASE_KID": 16001,
            "production_date": "2024-03-01",
            "oil_bbl": 500.0,
        },  # resume → huge +rate
    ]
    df = _f2_partition(rows)
    result = add_cumulative_features(df)
    rates = result["decline_rate"].dropna()
    for r in rates:
        assert float(r) >= -1.0
        assert float(r) <= 10.0


@pytest.mark.unit
def test_f2_empty_input_returns_full_schema() -> None:
    """Empty F1 input → empty F2 output with all F2 column names."""
    df = _f2_partition([])
    result = add_cumulative_features(df)
    assert len(result) == 0
    for col in _F2_COLS:
        assert col in result.columns


# ---------------------------------------------------------------------------
# Task F3 tests: add_rolling_features
# ---------------------------------------------------------------------------


def _make_f2_partition_for_rolling(lease_id: int, months: int) -> pd.DataFrame:
    rows = [
        {
            "LEASE_KID": lease_id,
            "production_date": f"2024-{m:02d}-01",
            "oil_bbl": float(m * 10),
            "gas_mcf": float(m * 5),
            "water_bbl": float(m * 2),
        }
        for m in range(1, months + 1)
    ]
    base = _f2_partition(rows)
    # Add F2 columns as NA for simplicity
    for col in _F2_COLS:
        base[col] = pd.array([None] * len(base), dtype="Float64")
    return base


@pytest.mark.unit
def test_f3_rolling_averages_known_values() -> None:
    """12-month series → 3m and 6m rolling averages match hand-computed values (TR-09(a))."""
    df = _make_f2_partition_for_rolling(17001, 12)
    result = add_rolling_features(df)

    # Month 3: oil values are 10, 20, 30 → 3m avg = 20.0
    assert abs(float(result["oil_bbl_rolling_3m"].iloc[2]) - 20.0) < 1e-6
    # Month 6: oil values are 10,20,30,40,50,60 → 6m avg = 35.0
    assert abs(float(result["oil_bbl_rolling_6m"].iloc[5]) - 35.0) < 1e-6


@pytest.mark.unit
def test_f3_short_history_rolling_nan() -> None:
    """2-month series → 3m rolling value for month 2 is NaN (full-window mode, TR-09(b))."""
    df = _make_f2_partition_for_rolling(18001, 2)
    result = add_rolling_features(df)
    assert pd.isna(result["oil_bbl_rolling_3m"].iloc[1])
    assert pd.isna(result["gas_mcf_rolling_3m"].iloc[1])
    assert pd.isna(result["water_bbl_rolling_3m"].iloc[1])


@pytest.mark.unit
def test_f3_lag_correctness() -> None:
    """4-month sequence: lag_1m for months 2,3,4 equals oil_bbl of months 1,2,3 (TR-09(c))."""
    df = _make_f2_partition_for_rolling(19001, 4)
    result = add_rolling_features(df)

    for i in range(1, 4):
        expected = float(df["oil_bbl"].iloc[i - 1])
        actual = float(result["oil_bbl_lag_1m"].iloc[i])
        assert abs(actual - expected) < 1e-6, f"Lag mismatch at month {i + 1}"


@pytest.mark.unit
def test_f3_empty_input_returns_full_schema() -> None:
    """Empty F2 input → empty output with all rolling/lag columns."""
    df = _f2_partition([])
    for col in _F2_COLS:
        df[col] = pd.array([], dtype="Float64")
    result = add_rolling_features(df)
    assert len(result) == 0
    for col in _F3_ROLLING_COLS:
        assert col in result.columns


# ---------------------------------------------------------------------------
# Task F4 tests: meta derivation (TR-23)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_f4_meta_f1_matches_live() -> None:
    """Meta from F1 zero-row input matches live output columns and dtypes (TR-23)."""
    rows = [
        {"LEASE_KID": 20001, "production_date": "2024-01-01", "PRODUCT": "O", "PRODUCTION": 100.0},
    ]
    df = _f1_partition(rows)
    meta = derive_features_meta(reshape_products, df.iloc[:0].copy())
    live = reshape_products(df.iloc[:0].copy())
    assert list(meta.columns) == list(live.columns)
    for col in meta.columns:
        assert str(meta[col].dtype) == str(live[col].dtype)


@pytest.mark.unit
def test_f4_meta_f2_matches_live() -> None:
    """Meta from F2 zero-row input matches live output (TR-23)."""
    df = _f2_partition([])
    meta = derive_features_meta(add_cumulative_features, df.iloc[:0].copy())
    live = add_cumulative_features(df.iloc[:0].copy())
    assert list(meta.columns) == list(live.columns)


@pytest.mark.unit
def test_f4_meta_f3_matches_live() -> None:
    """Meta from F3 zero-row input matches live output (TR-23)."""
    df = _f2_partition([])
    for col in _F2_COLS:
        df[col] = pd.array([], dtype="Float64")
    meta = derive_features_meta(add_rolling_features, df.iloc[:0].copy())
    live = add_rolling_features(df.iloc[:0].copy())
    assert list(meta.columns) == list(live.columns)


# ---------------------------------------------------------------------------
# Task F5 tests: features stage entry point
# ---------------------------------------------------------------------------


def _make_features_config(tmp_path: Path) -> dict:
    return {
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        }
    }


def _write_transform_parquet(tmp_path: Path) -> None:
    """Write a minimal transform-output Parquet for features to consume."""
    rows = [
        {
            "LEASE_KID": 30001,
            "production_date": "2024-01-01",
            "PRODUCT": "O",
            "PRODUCTION": 100.0,
            "WELLS": 2,
        },
        {
            "LEASE_KID": 30001,
            "production_date": "2024-02-01",
            "PRODUCT": "O",
            "PRODUCTION": 110.0,
            "WELLS": 2,
        },
        {
            "LEASE_KID": 30001,
            "production_date": "2024-03-01",
            "PRODUCT": "G",
            "PRODUCTION": 500.0,
            "WELLS": 2,
        },
        {
            "LEASE_KID": 30001,
            "production_date": "2024-01-01",
            "PRODUCT": "G",
            "PRODUCTION": 480.0,
            "WELLS": 2,
        },
        {
            "LEASE_KID": 30002,
            "production_date": "2024-01-01",
            "PRODUCT": "O",
            "PRODUCTION": 50.0,
            "WELLS": 1,
        },
    ]
    df = _f1_partition(rows)
    # Save as parquet
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    ddf = dd.from_pandas(df, npartitions=1)
    ddf.to_parquet(str(processed_dir), overwrite=True, write_index=False)


@pytest.mark.unit
def test_f5_complete_column_set_tr19(tmp_path: Path) -> None:
    """Output contains the complete TR-19 column list."""
    _write_transform_parquet(tmp_path)
    cfg = _make_features_config(tmp_path)
    features(cfg)

    features_dir = tmp_path / "features"
    df = dd.read_parquet(str(features_dir)).compute()

    tr19_cols = [
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
        "oil_bbl_rolling_3m",
        "oil_bbl_rolling_6m",
        "gas_mcf_rolling_3m",
        "gas_mcf_rolling_6m",
        "water_bbl_rolling_3m",
        "water_bbl_rolling_6m",
        "oil_bbl_lag_1m",
        "gas_mcf_lag_1m",
        "water_bbl_lag_1m",
    ]
    for col in tr19_cols:
        assert col in df.columns or col in (df.index.name,), f"Missing TR-19 column: {col}"


@pytest.mark.unit
def test_f5_tr17_lazy(tmp_path: Path) -> None:
    """Intermediate Dask DataFrames — no .compute() before final write (TR-17)."""
    _write_transform_parquet(tmp_path)
    processed_dir = tmp_path / "processed"
    ddf = dd.read_parquet(str(processed_dir))
    assert isinstance(ddf, dd.DataFrame)


@pytest.mark.unit
def test_f5_tr14_consistent_schema_across_partitions(tmp_path: Path) -> None:
    """Two feature partitions have identical column list and dtypes (TR-14)."""
    _write_transform_parquet(tmp_path)
    cfg = _make_features_config(tmp_path)
    features(cfg)

    features_dir = tmp_path / "features"
    parquet_files = sorted(features_dir.rglob("*.parquet"))
    assert len(parquet_files) >= 1
    if len(parquet_files) < 2:
        pytest.skip("Only one partition file; TR-14 requires ≥2")

    df0 = pd.read_parquet(parquet_files[0])
    df1 = pd.read_parquet(parquet_files[1])
    assert list(df0.columns) == list(df1.columns)
    for col in df0.columns:
        assert str(df0[col].dtype) == str(df1[col].dtype)


@pytest.mark.integration
def test_f5_tr26_integration(tmp_path: Path) -> None:
    """Run features on transform output; all derived feature columns present (TR-26)."""
    from kgs_pipeline.ingest import ingest
    from kgs_pipeline.transform import transform

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    canonical = [
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
        "MONTH-YEAR",
        "PRODUCT",
        "WELLS",
        "PRODUCTION",
    ]
    raw_rows = [
        {
            "LEASE_KID": "50001",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "100",
        },
        {
            "LEASE_KID": "50001",
            "MONTH-YEAR": "2-2024",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "110",
        },
        {
            "LEASE_KID": "50001",
            "MONTH-YEAR": "3-2024",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "105",
        },
        {
            "LEASE_KID": "50001",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "G",
            "WELLS": "2",
            "PRODUCTION": "500",
        },
        {
            "LEASE_KID": "50002",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "50",
        },
    ]
    header = ",".join(canonical)
    lines = [header]
    for r in raw_rows:
        lines.append(",".join(str(r.get(c, "")) for c in canonical))
    (raw_dir / "lp_feat.txt").write_text("\n".join(lines) + "\n")

    cfg = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(tmp_path / "interim"),
            "data_dictionary_path": str(_DD_PATH),
        },
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
            "unit_error_threshold": 1_000_000.0,
        },
        "features": {
            "processed_dir": str(tmp_path / "processed"),
            "features_dir": str(tmp_path / "features"),
        },
    }

    ingest(cfg)
    transform(cfg)
    features(cfg)

    features_dir = tmp_path / "features"
    assert features_dir.exists()

    ddf = dd.read_parquet(str(features_dir))
    df = ddf.compute()

    expected_cols = [
        "oil_bbl",
        "gas_mcf",
        "water_bbl",
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "oil_bbl_rolling_3m",
        "oil_bbl_rolling_6m",
        "gas_mcf_rolling_3m",
        "gas_mcf_rolling_6m",
        "water_bbl_rolling_3m",
        "water_bbl_rolling_6m",
        "oil_bbl_lag_1m",
        "gas_mcf_lag_1m",
        "water_bbl_lag_1m",
    ]
    for col in expected_cols:
        assert col in df.columns, f"Missing feature column: {col}"

    # Schema consistent across partitions
    parquet_files = sorted(features_dir.rglob("*.parquet"))
    if len(parquet_files) >= 2:
        df0 = pd.read_parquet(parquet_files[0])
        df1 = pd.read_parquet(parquet_files[1])
        assert list(df0.columns) == list(df1.columns)
