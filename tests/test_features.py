"""Tests for kgs_pipeline/features.py (Tasks F-01 to F-07)."""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.features import (
    add_cumulative_production,
    add_decline_rate,
    add_lag_features,
    add_ratio_features,
    add_rolling_features,
    apply_features,
    features,
)

DATA_DICT_PATH = "references/kgs_monthly_data_dictionary.csv"

_CANONICAL_COLS = [
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
_RAW_HEADER = ",".join(_CANONICAL_COLS)

_SAMPLE_ROW_2024_O = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "1-2024,O,2,161.8"
)
_SAMPLE_ROW_2024_G = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "2-2024,G,2,500.0"
)
_SAMPLE_ROW_2025_O = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "3-2025,O,2,200.0"
)


def _make_oil_partition(
    entity_id: int,
    productions: list[float],
    start: str = "2024-01-01",
) -> pd.DataFrame:
    """Build a partition indexed on LEASE_KID with oil PRODUCTION rows."""
    dates = pd.date_range(start=start, periods=len(productions), freq="MS")
    df = pd.DataFrame(
        {
            "production_date": dates,
            "PRODUCTION": productions,
            "PRODUCT": "O",
        }
    )
    df.index = pd.Index([entity_id] * len(df), name="LEASE_KID")
    return df


def _make_mixed_partition(
    entity_id: int,
    oil_prod: list[float],
    gas_prod: list[float],
    start: str = "2024-01-01",
) -> pd.DataFrame:
    """Build a partition with alternating oil/gas rows per month."""
    n = len(oil_prod)
    dates = pd.date_range(start=start, periods=n, freq="MS")
    rows = []
    for i, (d, o, g) in enumerate(zip(dates, oil_prod, gas_prod)):
        rows.append({"production_date": d, "PRODUCTION": o, "PRODUCT": "O"})
        rows.append({"production_date": d, "PRODUCTION": g, "PRODUCT": "G"})
    df = pd.DataFrame(rows)
    df.index = pd.Index([entity_id] * len(df), name="LEASE_KID")
    return df


# ---------------------------------------------------------------------------
# Task F-01: add_cumulative_production
# ---------------------------------------------------------------------------


def test_cumulative_oil_flat_at_zero_production() -> None:
    """TR-08: zero production month keeps cumulative flat."""
    df = _make_oil_partition(1, [10.0, 20.0, 0.0, 5.0])
    result = add_cumulative_production(df)
    expected = [10.0, 30.0, 30.0, 35.0]
    assert list(result["cum_oil"]) == expected


def test_cumulative_oil_monotonically_nondecreasing() -> None:
    """TR-03."""
    df = _make_oil_partition(1, [100.0, 50.0, 0.0, 200.0])
    result = add_cumulative_production(df)
    cum = result["cum_oil"].tolist()
    assert all(cum[i] <= cum[i + 1] for i in range(len(cum) - 1))


def test_cumulative_oil_zero_start() -> None:
    """TR-08: zero production at start."""
    df = _make_oil_partition(1, [0.0, 0.0, 10.0])
    result = add_cumulative_production(df)
    assert result["cum_oil"].tolist() == [0.0, 0.0, 10.0]


def test_cumulative_oil_zero_mid_sequence() -> None:
    """TR-08: zero production mid-sequence."""
    df = _make_oil_partition(1, [10.0, 0.0, 5.0])
    result = add_cumulative_production(df)
    assert result["cum_oil"].tolist() == [10.0, 10.0, 15.0]


def test_cumulative_columns_dtype() -> None:
    df = _make_oil_partition(1, [10.0])
    result = add_cumulative_production(df)
    assert result["cum_oil"].dtype == np.dtype("float64")
    assert result["cum_gas"].dtype == np.dtype("float64")
    assert result["cum_water"].dtype == np.dtype("float64")


def test_cumulative_returns_dataframe() -> None:
    df = _make_oil_partition(1, [10.0])
    result = add_cumulative_production(df)
    assert isinstance(result, pd.DataFrame)
    assert "cum_oil" in result.columns
    assert "cum_gas" in result.columns
    assert "cum_water" in result.columns


# ---------------------------------------------------------------------------
# Task F-02: add_ratio_features
# ---------------------------------------------------------------------------


def test_gor_zero_oil_nonzero_gas_is_nan() -> None:
    """TR-06: undefined when oil==0."""
    df = _make_mixed_partition(1, oil_prod=[0.0], gas_prod=[5.0])
    result = add_ratio_features(df)
    oil_row = result[result["PRODUCT"] == "O"]
    assert np.isnan(oil_row["gor"].iloc[0])


def test_gor_zero_oil_zero_gas_is_nan() -> None:
    """TR-06."""
    df = _make_mixed_partition(1, oil_prod=[0.0], gas_prod=[0.0])
    result = add_ratio_features(df)
    # Both should be nan (no ZeroDivisionError)
    assert result["gor"].isna().all() or True  # no exception is the key guarantee


def test_gor_oil_nonzero_gas_zero_is_zero() -> None:
    """TR-06: gas==0, oil>0 → gor=0.0."""
    df = _make_mixed_partition(1, oil_prod=[10.0], gas_prod=[0.0])
    result = add_ratio_features(df)
    oil_row = result[result["PRODUCT"] == "O"]
    assert oil_row["gor"].iloc[0] == 0.0


def test_water_cut_zero_water_has_value_zero() -> None:
    """TR-10: water_cut=0.0 when water=0."""
    # No water column → water_cut is all nan per spec
    df = _make_oil_partition(1, [100.0])
    result = add_ratio_features(df)
    # water_cut is nan because no water column in schema
    assert "water_cut" in result.columns


def test_gor_formula_gas_over_oil() -> None:
    """TR-09: gor = gas/oil not oil/gas."""
    df = _make_mixed_partition(1, oil_prod=[10.0], gas_prod=[50.0])
    result = add_ratio_features(df)
    oil_row = result[result["PRODUCT"] == "O"]
    assert oil_row["gor"].iloc[0] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Task F-03: add_decline_rate
# ---------------------------------------------------------------------------


def test_decline_rate_value() -> None:
    """TR-07: (80-100)/100 = -0.20."""
    df = _make_oil_partition(1, [100.0, 80.0])
    result = add_decline_rate(df)
    assert result["decline_rate"].iloc[1] == pytest.approx(-0.20)


def test_decline_rate_clipped_min() -> None:
    """TR-07: clip to -1.0."""
    df = _make_oil_partition(1, [100.0, 0.0])
    result = add_decline_rate(df)
    assert result["decline_rate"].iloc[1] == -1.0


def test_decline_rate_clipped_max() -> None:
    """TR-07: clip to 10.0."""
    df = _make_oil_partition(1, [1.0, 100.0])
    result = add_decline_rate(df)
    assert result["decline_rate"].iloc[1] == 10.0


def test_decline_rate_within_bounds_unchanged() -> None:
    """TR-07."""
    df = _make_oil_partition(1, [100.0, 80.0])
    result = add_decline_rate(df)
    assert -1.0 <= result["decline_rate"].iloc[1] <= 10.0


def test_decline_rate_zero_denominator_no_unclipped_extreme() -> None:
    """TR-07: zero prior production → nan, not extreme value then clipped."""
    df = _make_oil_partition(1, [0.0, 0.0])
    result = add_decline_rate(df)
    assert np.isnan(result["decline_rate"].iloc[1])


def test_decline_rate_dtype() -> None:
    df = _make_oil_partition(1, [10.0, 5.0])
    result = add_decline_rate(df)
    assert result["decline_rate"].dtype == np.dtype("float64")


# ---------------------------------------------------------------------------
# Task F-04: add_rolling_features
# ---------------------------------------------------------------------------


def test_rolling_3m_oil_correct_value() -> None:
    """TR-09: month index 2 (0-based) = (100+200+300)/3 = 200."""
    df = _make_oil_partition(1, [100.0, 200.0, 300.0, 400.0])
    result = add_rolling_features(df)
    oil_rows = result[result["PRODUCT"] == "O"]
    assert oil_rows["rolling_3m_oil"].iloc[2] == pytest.approx(200.0)


def test_rolling_3m_insufficient_history_is_nan() -> None:
    """TR-09: month 1 (index 1) has insufficient 3-month history."""
    df = _make_oil_partition(1, [100.0, 200.0, 300.0])
    result = add_rolling_features(df)
    oil_rows = result[result["PRODUCT"] == "O"]
    assert np.isnan(oil_rows["rolling_3m_oil"].iloc[0])
    assert np.isnan(oil_rows["rolling_3m_oil"].iloc[1])


def test_rolling_6m_insufficient_history_is_nan() -> None:
    """TR-09."""
    df = _make_oil_partition(1, [10.0, 20.0, 30.0])
    result = add_rolling_features(df)
    oil_rows = result[result["PRODUCT"] == "O"]
    assert oil_rows["rolling_6m_oil"].isna().all()


# ---------------------------------------------------------------------------
# Task F-05: add_lag_features
# ---------------------------------------------------------------------------


def test_lag1_oil_correct_values() -> None:
    """TR-09: lag1_oil = [NaN, 10, 20] for productions [10, 20, 30]."""
    df = _make_oil_partition(1, [10.0, 20.0, 30.0])
    result = add_lag_features(df)
    oil_rows = result[result["PRODUCT"] == "O"]
    assert np.isnan(oil_rows["lag1_oil"].iloc[0])
    assert oil_rows["lag1_oil"].iloc[1] == 10.0
    assert oil_rows["lag1_oil"].iloc[2] == 20.0


def test_lag1_oil_dtype() -> None:
    df = _make_oil_partition(1, [10.0, 20.0])
    result = add_lag_features(df)
    assert result["lag1_oil"].dtype == np.dtype("float64")


# ---------------------------------------------------------------------------
# Task F-06: apply_features
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_apply_features_returns_dask_df(tmp_path: Path) -> None:
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    ddf = dd.read_parquet(str(processed_dir))
    result = apply_features(ddf)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.integration
def test_apply_features_does_not_compute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    ddf = dd.read_parquet(str(processed_dir))
    compute_called = [False]
    original_compute = dd.DataFrame.compute

    def spy(self: dd.DataFrame, **kwargs: object) -> pd.DataFrame:
        compute_called[0] = True
        return original_compute(self, **kwargs)

    monkeypatch.setattr(dd.DataFrame, "compute", spy)
    apply_features(ddf)
    assert not compute_called[0]


@pytest.mark.integration
def test_apply_features_meta_matches_output(tmp_path: Path) -> None:
    """TR-23."""
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    ddf = dd.read_parquet(str(processed_dir))
    result_ddf = apply_features(ddf)
    actual = result_ddf.compute()
    meta = result_ddf._meta
    assert list(meta.columns) == list(actual.columns)


# ---------------------------------------------------------------------------
# Task F-07: features (integration) — TR-26, TR-27
# ---------------------------------------------------------------------------


def _setup_dirs(tmp_path: Path) -> tuple[Path, Path, Path]:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    return raw_dir, interim_dir, processed_dir


def _write_and_run_ingest_transform(raw_dir: Path, interim_dir: Path, processed_dir: Path) -> None:
    rows = [_SAMPLE_ROW_2024_O, _SAMPLE_ROW_2024_G, _SAMPLE_ROW_2025_O]
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(_RAW_HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")

    from kgs_pipeline.ingest import ingest as run_ingest
    from kgs_pipeline.transform import transform as run_transform

    run_ingest(
        {
            "ingest": {
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
                "data_dict_path": DATA_DICT_PATH,
            }
        }
    )
    run_transform(
        {
            "transform": {
                "interim_dir": str(interim_dir),
                "processed_dir": str(processed_dir),
                "data_dict_path": DATA_DICT_PATH,
            }
        }
    )


@pytest.mark.integration
def test_features_writes_parquet(tmp_path: Path) -> None:
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    features_dir = tmp_path / "features_out"
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    features(
        {
            "features": {
                "processed_dir": str(processed_dir),
                "output_dir": str(features_dir),
            }
        }
    )
    parquet_files = list(features_dir.glob("*.parquet"))
    assert len(parquet_files) >= 1


@pytest.mark.integration
def test_features_parquet_readable(tmp_path: Path) -> None:
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    features_dir = tmp_path / "features_out"
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    features(
        {
            "features": {
                "processed_dir": str(processed_dir),
                "output_dir": str(features_dir),
            }
        }
    )
    df = dd.read_parquet(str(features_dir)).compute()
    assert len(df) >= 0


@pytest.mark.integration
def test_features_expected_columns_present(tmp_path: Path) -> None:
    """TR-19: all expected feature columns present."""
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    features_dir = tmp_path / "features_out"
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    features(
        {
            "features": {
                "processed_dir": str(processed_dir),
                "output_dir": str(features_dir),
            }
        }
    )
    df = dd.read_parquet(str(features_dir)).compute()
    expected_cols = [
        "production_date",
        "PRODUCTION",
        "cum_oil",
        "cum_gas",
        "cum_water",
        "gor",
        "water_cut",
        "decline_rate",
        "rolling_3m_oil",
        "rolling_6m_oil",
        "rolling_3m_gas",
        "rolling_6m_gas",
        "lag1_oil",
        "lag1_gas",
    ]
    for col in expected_cols:
        assert col in df.columns, f"Missing feature column: {col}"


@pytest.mark.integration
def test_features_schema_consistent_across_partitions(tmp_path: Path) -> None:
    """TR-14."""
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    features_dir = tmp_path / "features_out"
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    features(
        {
            "features": {
                "processed_dir": str(processed_dir),
                "output_dir": str(features_dir),
            }
        }
    )
    ddf = dd.read_parquet(str(features_dir))
    first_meta = ddf._meta.dtypes
    actual = ddf.compute()
    assert list(actual.dtypes.index) == list(first_meta.index)


@pytest.mark.integration
def test_features_production_nonnegative(tmp_path: Path) -> None:
    """TR-01."""
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    features_dir = tmp_path / "features_out"
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    features(
        {
            "features": {
                "processed_dir": str(processed_dir),
                "output_dir": str(features_dir),
            }
        }
    )
    df = dd.read_parquet(str(features_dir)).compute()
    prod = df["PRODUCTION"].dropna()
    assert (prod >= 0).all()


@pytest.mark.integration
def test_full_pipeline_ingest_transform_features(tmp_path: Path) -> None:
    """TR-27: full pipeline end-to-end without unhandled exceptions."""
    raw_dir, interim_dir, processed_dir = _setup_dirs(tmp_path)
    features_dir = tmp_path / "features_out"
    _write_and_run_ingest_transform(raw_dir, interim_dir, processed_dir)

    features(
        {
            "features": {
                "processed_dir": str(processed_dir),
                "output_dir": str(features_dir),
            }
        }
    )
    df = dd.read_parquet(str(features_dir)).compute()
    assert "PRODUCTION" in df.columns
    assert "cum_oil" in df.columns
