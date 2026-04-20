"""Tests for kgs_pipeline/transform.py."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.transform import (
    apply_partition_transforms,
    check_well_completeness,
    clean_invalid_values,
    deduplicate,
    index_and_sort,
    parse_production_date,
    validate_production_units,
    write_transform,
)


# ---------------------------------------------------------------------------
# TRN-01: parse_production_date
# ---------------------------------------------------------------------------

def _make_df(**kwargs: list) -> pd.DataFrame:
    return pd.DataFrame(kwargs)


def test_parse_production_date_basic() -> None:
    df = _make_df(**{"MONTH-YEAR": ["1-2024", "12-2025"]})
    result = parse_production_date(df)
    assert "production_date" in result.columns
    assert "MONTH-YEAR" not in result.columns
    dates = pd.to_datetime(result["production_date"])
    assert dates.iloc[0] == pd.Timestamp("2024-01-01")
    assert dates.iloc[1] == pd.Timestamp("2025-12-01")


def test_parse_production_date_drops_month_zero() -> None:
    df = _make_df(**{"MONTH-YEAR": ["0-2024", "1-2024"]})
    result = parse_production_date(df)
    assert len(result) == 1


def test_parse_production_date_no_month_year_col() -> None:
    df = _make_df(col=["a", "b"])
    result = parse_production_date(df)
    assert "MONTH-YEAR" not in result.columns


def test_parse_production_date_malformed_gives_nat() -> None:
    df = _make_df(**{"MONTH-YEAR": ["invalid-date"]})
    result = parse_production_date(df)
    assert pd.isna(result["production_date"].iloc[0])


# ---------------------------------------------------------------------------
# TRN-02: clean_invalid_values
# ---------------------------------------------------------------------------

def _base_row() -> dict:
    return {
        "LEASE_KID": pd.array([1], dtype="Int64"),
        "PRODUCT": pd.Categorical(["O"], categories=["G", "O"]),
        "production_date": pd.to_datetime(["2024-01-01"]),
        "PRODUCTION": pd.array([100.0], dtype="Float64"),
        "WELLS": pd.array([2], dtype="Int64"),
    }


def test_clean_keeps_zero_production() -> None:
    data = _base_row()
    data["PRODUCTION"] = pd.array([0.0], dtype="Float64")
    df = pd.DataFrame(data)
    result = clean_invalid_values(df)
    assert len(result) == 1
    assert float(result["PRODUCTION"].iloc[0]) == 0.0


def test_clean_drops_negative_production(caplog: pytest.LogCaptureFixture) -> None:
    data = _base_row()
    data["PRODUCTION"] = pd.array([-50.0], dtype="Float64")
    df = pd.DataFrame(data)
    with caplog.at_level(logging.WARNING):
        result = clean_invalid_values(df)
    assert result.empty
    assert any("PRODUCTION < 0" in r.message for r in caplog.records)


def test_clean_drops_nat_production_date() -> None:
    data = _base_row()
    data["production_date"] = [pd.NaT]
    df = pd.DataFrame(data)
    result = clean_invalid_values(df)
    assert result.empty


def test_clean_drops_null_lease_kid() -> None:
    data = _base_row()
    data["LEASE_KID"] = pd.array([pd.NA], dtype="Int64")
    df = pd.DataFrame(data)
    result = clean_invalid_values(df)
    assert result.empty


def test_clean_null_production_imputed_with_active_wells() -> None:
    data = {
        "LEASE_KID": pd.array([1], dtype="Int64"),
        "PRODUCT": pd.Categorical(["O"], categories=["G", "O"]),
        "production_date": pd.to_datetime(["2024-01-01"]),
        "PRODUCTION": pd.array([pd.NA], dtype="Float64"),
        "WELLS": pd.array([2], dtype="Int64"),
    }
    df = pd.DataFrame(data)
    result = clean_invalid_values(df)
    assert len(result) == 1
    assert float(result["PRODUCTION"].iloc[0]) == 0.0


# ---------------------------------------------------------------------------
# TRN-03: validate_production_units
# ---------------------------------------------------------------------------

def test_validate_flags_high_oil(caplog: pytest.LogCaptureFixture) -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["1001"],
        "PRODUCT": pd.Categorical(["O"], categories=["G", "O"]),
        "PRODUCTION": [60000.0],
    })
    with caplog.at_level(logging.WARNING):
        result = validate_production_units(df)
    assert len(result) == 1  # No rows dropped
    assert any("60000" in r.message or "unit error" in r.message for r in caplog.records)


def test_validate_no_warning_for_low_oil(caplog: pytest.LogCaptureFixture) -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["1001"],
        "PRODUCT": pd.Categorical(["O"], categories=["G", "O"]),
        "PRODUCTION": [49000.0],
    })
    with caplog.at_level(logging.WARNING):
        result = validate_production_units(df)
    assert len(result) == 1
    assert not any("unit error" in r.message for r in caplog.records)


def test_validate_no_flag_for_high_gas(caplog: pytest.LogCaptureFixture) -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["2001"],
        "PRODUCT": pd.Categorical(["G"], categories=["G", "O"]),
        "PRODUCTION": [200000.0],
    })
    with caplog.at_level(logging.WARNING):
        result = validate_production_units(df)
    assert len(result) == 1
    assert not any("unit error" in r.message for r in caplog.records)


def test_validate_returns_same_row_count() -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["1", "2"],
        "PRODUCT": pd.Categorical(["O", "G"], categories=["G", "O"]),
        "PRODUCTION": [60000.0, 5000.0],
    })
    result = validate_production_units(df)
    assert len(result) == len(df)


# ---------------------------------------------------------------------------
# TRN-04: deduplicate
# ---------------------------------------------------------------------------

def test_deduplicate_removes_duplicates() -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["1", "1", "2"],
        "PRODUCT": ["O", "O", "G"],
        "production_date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-02-01"]),
        "PRODUCTION": [100.0, 100.0, 200.0],
    })
    result = deduplicate(df)
    assert len(result) == 2


def test_deduplicate_idempotent() -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["1", "1", "2"],
        "PRODUCT": ["O", "O", "G"],
        "production_date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-02-01"]),
        "PRODUCTION": [100.0, 100.0, 200.0],
    })
    once = deduplicate(df)
    twice = deduplicate(once)
    assert len(once) == len(twice)


def test_deduplicate_no_duplicates() -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["1", "2", "3"],
        "PRODUCT": ["O", "G", "O"],
        "production_date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
    })
    result = deduplicate(df)
    assert len(result) == 3


def test_deduplicate_output_leq_input() -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["1", "1"],
        "PRODUCT": ["O", "O"],
        "production_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
    })
    result = deduplicate(df)
    assert len(result) <= len(df)


# ---------------------------------------------------------------------------
# TRN-05: index_and_sort
# ---------------------------------------------------------------------------

def _make_dask_df() -> dd.DataFrame:
    df = pd.DataFrame({
        "LEASE_KID": pd.array([1, 1, 2, 2], dtype="int64"),
        "PRODUCT": pd.Categorical(["O", "O", "G", "G"], categories=["G", "O"]),
        "production_date": pd.to_datetime(["2024-03-01", "2024-01-01", "2024-02-01", "2024-04-01"]),
        "PRODUCTION": pd.array([300.0, 100.0, 200.0, 400.0], dtype="Float64"),
    })
    return dd.from_pandas(df, npartitions=2)


def test_index_and_sort_returns_dask_df() -> None:
    ddf = _make_dask_df()
    result = index_and_sort(ddf)
    assert isinstance(result, dd.DataFrame)


def test_index_and_sort_production_date_sorted() -> None:
    ddf = _make_dask_df()
    result = index_and_sort(ddf)
    computed = result.compute()
    for _, grp in computed.groupby(level=0):
        dates = grp["production_date"].values
        assert all(dates[i] <= dates[i + 1] for i in range(len(dates) - 1))


def test_index_and_sort_missing_lease_kid() -> None:
    df = pd.DataFrame({"production_date": pd.to_datetime(["2024-01-01"])})
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(KeyError, match="LEASE_KID"):
        index_and_sort(ddf)


def test_index_and_sort_missing_production_date() -> None:
    df = pd.DataFrame({"LEASE_KID": pd.array([1], dtype="int64")})
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(KeyError, match="production_date"):
        index_and_sort(ddf)


# ---------------------------------------------------------------------------
# TRN-06: check_well_completeness
# ---------------------------------------------------------------------------

def test_check_well_completeness_no_gap(caplog: pytest.LogCaptureFixture) -> None:
    dates = pd.date_range("2024-01-01", periods=12, freq="MS")
    df = pd.DataFrame({
        "LEASE_KID": ["1001"] * 12,
        "PRODUCT": ["O"] * 12,
        "production_date": dates,
    })
    with caplog.at_level(logging.WARNING):
        result = check_well_completeness(df)
    assert len(result) == len(df)
    assert not any("gap" in r.message for r in caplog.records)


def test_check_well_completeness_with_gap(caplog: pytest.LogCaptureFixture) -> None:
    df = pd.DataFrame({
        "LEASE_KID": ["1001", "1001", "1001"],
        "PRODUCT": ["O", "O", "O"],
        "production_date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-04-01"]),
    })
    with caplog.at_level(logging.WARNING):
        result = check_well_completeness(df)
    assert len(result) == 3  # Diagnostic only
    assert any("gap" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# TRN-07: apply_partition_transforms
# ---------------------------------------------------------------------------

def test_apply_partition_transforms_returns_dask_df() -> None:
    df = pd.DataFrame({
        "LEASE_KID": pd.array([1, 1], dtype="int64"),
        "PRODUCT": pd.Categorical(["O", "G"], categories=["G", "O"]),
        "MONTH-YEAR": pd.array(["1-2024", "2-2024"], dtype="string"),
        "PRODUCTION": pd.array([100.0, 200.0], dtype="Float64"),
        "WELLS": pd.array([1, 1], dtype="Int64"),
    })
    ddf = dd.from_pandas(df, npartitions=1)
    result = apply_partition_transforms(ddf)
    assert isinstance(result, dd.DataFrame)


def test_apply_partition_transforms_adds_production_date() -> None:
    df = pd.DataFrame({
        "LEASE_KID": pd.array([1], dtype="int64"),
        "PRODUCT": pd.Categorical(["O"], categories=["G", "O"]),
        "MONTH-YEAR": pd.array(["1-2024"], dtype="string"),
        "PRODUCTION": pd.array([100.0], dtype="Float64"),
        "WELLS": pd.array([1], dtype="Int64"),
    })
    ddf = dd.from_pandas(df, npartitions=1)
    result = apply_partition_transforms(ddf)
    computed = result.compute()
    assert "production_date" in computed.columns


# ---------------------------------------------------------------------------
# TRN-08: write_transform
# ---------------------------------------------------------------------------

def test_write_transform_creates_parquet(tmp_path: Path) -> None:
    df = pd.DataFrame({
        "LEASE_KID": pd.array([1, 2, 3], dtype="int64"),
        "production_date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
        "PRODUCTION": pd.array([100.0, 200.0, 300.0], dtype="Float64"),
    })
    ddf = dd.from_pandas(df, npartitions=1)
    config = {"transform": {"processed_dir": str(tmp_path / "transform")}}
    write_transform(ddf, config)
    parquet_files = list((tmp_path / "transform").glob("*.parquet"))
    assert len(parquet_files) >= 1


def test_write_transform_readable_by_pandas(tmp_path: Path) -> None:
    df = pd.DataFrame({
        "LEASE_KID": pd.array([1], dtype="int64"),
        "PRODUCTION": pd.array([50.0], dtype="Float64"),
    })
    ddf = dd.from_pandas(df, npartitions=1)
    config = {"transform": {"processed_dir": str(tmp_path / "transform2")}}
    write_transform(ddf, config)
    out_dir = tmp_path / "transform2"
    for pf in out_dir.glob("*.parquet"):
        read_back = pd.read_parquet(pf)
        assert "LEASE_KID" in read_back.columns
