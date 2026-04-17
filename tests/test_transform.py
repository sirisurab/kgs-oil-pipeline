"""Tests for kgs_pipeline/transform.py."""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.ingest import load_schema
from kgs_pipeline.transform import (
    cast_categoricals,
    check_well_completeness,
    derive_production_date,
    index_and_sort,
    remove_duplicates,
    validate_physical_bounds,
    write_transform_parquet,
)

DICT_PATH = Path("references/kgs_monthly_data_dictionary.csv")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def base_schema() -> dict:
    return load_schema(DICT_PATH)


@pytest.fixture()
def minimal_df() -> pd.DataFrame:
    """Minimal DataFrame matching ingest output schema."""
    return pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001, 1002], dtype=pd.Int64Dtype()),
            "LEASE": pd.array(["Alpha", "Alpha", "Beta"], dtype=pd.StringDtype()),
            "DOR_CODE": pd.array([pd.NA, pd.NA, pd.NA], dtype=pd.Int64Dtype()),
            "API_NUMBER": pd.array([pd.NA, pd.NA, pd.NA], dtype=pd.StringDtype()),
            "FIELD": pd.array(["F1", "F1", "F2"], dtype=pd.StringDtype()),
            "PRODUCING_ZONE": pd.array(["PZ1", "PZ1", "PZ2"], dtype=pd.StringDtype()),
            "OPERATOR": pd.array(["ACME", "ACME", "BETA OIL"], dtype=pd.StringDtype()),
            "COUNTY": pd.array(["Barton", "Barton", "Ellis"], dtype=pd.StringDtype()),
            "TOWNSHIP": pd.array([15, 15, 20], dtype=pd.Int64Dtype()),
            "TWN_DIR": pd.Categorical(["S", "S", "S"], categories=["S", "N"]),
            "RANGE": pd.array([10, 10, 12], dtype=pd.Int64Dtype()),
            "RANGE_DIR": pd.Categorical(["W", "W", "W"], categories=["E", "W"]),
            "SECTION": pd.array([12, 12, 6], dtype=pd.Int64Dtype()),
            "SPOT": pd.array(["NE", "NE", "SW"], dtype=pd.StringDtype()),
            "LATITUDE": pd.array([38.5, 38.5, 38.7], dtype=np.float64),
            "LONGITUDE": pd.array([-99.2, -99.2, -99.5], dtype=np.float64),
            "MONTH-YEAR": pd.array(["3-2024", "6-2024", "1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "O", "G"], categories=["O", "G"]),
            "WELLS": pd.array([3, 3, 2], dtype=pd.Int64Dtype()),
            "PRODUCTION": pd.array([500.0, 600.0, 1000.0], dtype=np.float64),
            "source_file": pd.array(
                ["lp001.txt", "lp001.txt", "lp002.txt"], dtype=pd.StringDtype()
            ),
        }
    )


# ---------------------------------------------------------------------------
# Task 01: derive_production_date
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_derive_production_date_basic(minimal_df: pd.DataFrame) -> None:
    result = derive_production_date(minimal_df)
    assert "production_date" in result.columns
    assert result["production_date"].dtype == "datetime64[ns]"


@pytest.mark.unit
def test_derive_production_date_march_2024(minimal_df: pd.DataFrame) -> None:
    result = derive_production_date(minimal_df)
    march_row = result[result["MONTH-YEAR"] == "3-2024"]
    assert march_row["production_date"].iloc[0] == pd.Timestamp("2024-03-01")


@pytest.mark.unit
def test_derive_production_date_drops_month_zero() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1002], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["0-2024", "1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "G"], categories=["O", "G"]),
            "PRODUCTION": pd.array([0.0, 500.0], dtype=np.float64),
            "source_file": pd.array(["x.txt", "x.txt"], dtype=pd.StringDtype()),
        }
    )
    result = derive_production_date(df)
    assert len(result) == 1
    assert result["MONTH-YEAR"].iloc[0] == "1-2024"


@pytest.mark.unit
def test_derive_production_date_drops_month_minus_one() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["-1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0], dtype=np.float64),
            "source_file": pd.array(["x.txt"], dtype=pd.StringDtype()),
        }
    )
    result = derive_production_date(df)
    assert len(result) == 0


@pytest.mark.unit
def test_derive_production_date_december(minimal_df: pd.DataFrame) -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["12-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0], dtype=np.float64),
            "source_file": pd.array(["x.txt"], dtype=pd.StringDtype()),
        }
    )
    result = derive_production_date(df)
    assert result["production_date"].iloc[0] == pd.Timestamp("2024-12-01")


# ---------------------------------------------------------------------------
# Task 02: validate_physical_bounds
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_physical_bounds_negative_production() -> None:
    df = pd.DataFrame(
        {
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([-5.0], dtype=np.float64),
            "WELLS": pd.array([2], dtype=pd.Int64Dtype()),
        }
    )
    result = validate_physical_bounds(df)
    assert pd.isna(result["PRODUCTION"].iloc[0])


@pytest.mark.unit
def test_validate_physical_bounds_zero_preserved() -> None:
    df = pd.DataFrame(
        {
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([0.0], dtype=np.float64),
            "WELLS": pd.array([1], dtype=pd.Int64Dtype()),
        }
    )
    result = validate_physical_bounds(df)
    assert result["PRODUCTION"].iloc[0] == 0.0


@pytest.mark.unit
def test_validate_physical_bounds_positive_unchanged() -> None:
    df = pd.DataFrame(
        {
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0], dtype=np.float64),
            "WELLS": pd.array([1], dtype=pd.Int64Dtype()),
        }
    )
    result = validate_physical_bounds(df)
    assert result["PRODUCTION"].iloc[0] == 100.0


@pytest.mark.unit
def test_validate_physical_bounds_oil_flag_over_50000() -> None:
    df = pd.DataFrame(
        {
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([60000.0], dtype=np.float64),
            "WELLS": pd.array([1], dtype=pd.Int64Dtype()),
        }
    )
    result = validate_physical_bounds(df)
    assert result["production_unit_flag"].iloc[0]


@pytest.mark.unit
def test_validate_physical_bounds_gas_no_flag() -> None:
    df = pd.DataFrame(
        {
            "PRODUCT": pd.Categorical(["G"], categories=["O", "G"]),
            "PRODUCTION": pd.array([60000.0], dtype=np.float64),
            "WELLS": pd.array([1], dtype=pd.Int64Dtype()),
        }
    )
    result = validate_physical_bounds(df)
    assert not result["production_unit_flag"].iloc[0]


@pytest.mark.unit
def test_validate_physical_bounds_negative_wells() -> None:
    df = pd.DataFrame(
        {
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0], dtype=np.float64),
            "WELLS": pd.array([-1], dtype=pd.Int64Dtype()),
        }
    )
    result = validate_physical_bounds(df)
    assert pd.isna(result["WELLS"].iloc[0])


# ---------------------------------------------------------------------------
# Task 03: remove_duplicates
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_remove_duplicates_removes_dup() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["1-2024", "1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0, 200.0], dtype=np.float64),
        }
    )
    result = remove_duplicates(df)
    assert len(result) == 1


@pytest.mark.unit
def test_remove_duplicates_no_dup_unchanged() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1002], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["1-2024", "1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "G"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0, 200.0], dtype=np.float64),
        }
    )
    result = remove_duplicates(df)
    assert len(result) == 2


@pytest.mark.unit
def test_remove_duplicates_idempotent() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001, 1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["1-2024", "1-2024", "1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "O", "O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0, 200.0, 300.0], dtype=np.float64),
        }
    )
    once = remove_duplicates(df)
    twice = remove_duplicates(once)
    assert len(once) == len(twice)


@pytest.mark.unit
def test_remove_duplicates_count_leq_input() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1002, 1001], dtype=pd.Int64Dtype()),
            "MONTH-YEAR": pd.array(["1-2024", "1-2024", "1-2024"], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(["O", "G", "O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([100.0, 200.0, 150.0], dtype=np.float64),
        }
    )
    result = remove_duplicates(df)
    assert len(result) <= len(df)


# ---------------------------------------------------------------------------
# Task 04: cast_categoricals
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_cast_categoricals_out_of_set_becomes_na(base_schema: dict) -> None:
    df = pd.DataFrame(
        {
            "PRODUCT": ["O", "X", "G"],
            "TWN_DIR": ["S", "N", "Z"],
            "RANGE_DIR": ["W", "E", "X"],
        }
    )
    result = cast_categoricals(df, base_schema)
    assert pd.isna(result["PRODUCT"].iloc[1])  # X → NA
    assert pd.isna(result["TWN_DIR"].iloc[2])  # Z → NA
    assert pd.isna(result["RANGE_DIR"].iloc[2])  # X → NA


@pytest.mark.unit
def test_cast_categoricals_valid_values_preserved(base_schema: dict) -> None:
    df = pd.DataFrame(
        {
            "PRODUCT": ["O", "G"],
            "TWN_DIR": ["S", "N"],
            "RANGE_DIR": ["W", "E"],
        }
    )
    result = cast_categoricals(df, base_schema)
    assert list(result["TWN_DIR"].cat.categories) == ["S", "N"]


@pytest.mark.unit
def test_cast_categoricals_product_categories(base_schema: dict) -> None:
    df = pd.DataFrame({"PRODUCT": ["O", "G"]})
    result = cast_categoricals(df, base_schema)
    assert set(result["PRODUCT"].cat.categories) == {"O", "G"}


# ---------------------------------------------------------------------------
# Task 05: check_well_completeness
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_check_well_completeness_no_gap() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001, 1001], dtype=pd.Int64Dtype()),
            "production_date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
        }
    )
    result = check_well_completeness(df)
    assert not result["has_date_gap"].all()


@pytest.mark.unit
def test_check_well_completeness_with_gap() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1001], dtype=pd.Int64Dtype()),
            "production_date": pd.to_datetime(["2024-01-01", "2024-03-01"]),
        }
    )
    result = check_well_completeness(df)
    # Jan and March present, Feb missing → gap
    assert result["has_date_gap"].all()


@pytest.mark.unit
def test_check_well_completeness_single_month() -> None:
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001], dtype=pd.Int64Dtype()),
            "production_date": pd.to_datetime(["2024-01-01"]),
        }
    )
    result = check_well_completeness(df)
    assert not result["has_date_gap"].iloc[0]


# ---------------------------------------------------------------------------
# Task 06: index_and_sort
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_index_and_sort_index_is_lease_kid(minimal_df: pd.DataFrame) -> None:
    df_with_dates = derive_production_date(minimal_df)
    ddf = dd.from_pandas(df_with_dates, npartitions=1)
    result = index_and_sort(ddf)
    assert result.index.name == "LEASE_KID"


@pytest.mark.unit
def test_index_and_sort_partitions_sorted(minimal_df: pd.DataFrame) -> None:
    df_with_dates = derive_production_date(minimal_df)
    ddf = dd.from_pandas(df_with_dates, npartitions=1)
    result = index_and_sort(ddf)
    computed = result.compute()
    dates = computed["production_date"].values
    assert all(dates[i] <= dates[i + 1] for i in range(len(dates) - 1))


# ---------------------------------------------------------------------------
# Task 08: write_transform_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_transform_parquet_creates_files(minimal_df: pd.DataFrame, tmp_path: Path) -> None:
    df_with_dates = derive_production_date(minimal_df)
    df_validated = validate_physical_bounds(df_with_dates)
    ddf = dd.from_pandas(df_validated, npartitions=1)
    out = tmp_path / "transformed"
    write_transform_parquet(ddf, out, n_partitions=10)
    parquet_files = list(out.glob("*.parquet"))
    assert len(parquet_files) >= 1
    for pf in parquet_files:
        pd.read_parquet(pf)  # should not raise


# ---------------------------------------------------------------------------
# Task 10: Meta schema consistency (TR-23)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_meta_consistency_derive_production_date(minimal_df: pd.DataFrame) -> None:
    actual = derive_production_date(minimal_df)
    zero_row = minimal_df.iloc[0:0].copy()
    meta = derive_production_date(zero_row)
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_validate_physical_bounds(minimal_df: pd.DataFrame) -> None:
    actual = validate_physical_bounds(minimal_df)
    meta = validate_physical_bounds(minimal_df.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_remove_duplicates(minimal_df: pd.DataFrame) -> None:
    actual = remove_duplicates(minimal_df)
    meta = remove_duplicates(minimal_df.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_cast_categoricals(minimal_df: pd.DataFrame, base_schema: dict) -> None:
    actual = cast_categoricals(minimal_df, base_schema)
    meta = cast_categoricals(minimal_df.iloc[0:0], base_schema)
    assert list(actual.columns) == list(meta.columns)


@pytest.mark.unit
def test_meta_consistency_check_well_completeness(minimal_df: pd.DataFrame) -> None:
    df_with_dates = derive_production_date(minimal_df)
    actual = check_well_completeness(df_with_dates)
    meta = check_well_completeness(df_with_dates.iloc[0:0])
    assert list(actual.columns) == list(meta.columns)


# ---------------------------------------------------------------------------
# Task 12: Zero production preservation (TR-05)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_zero_production_preserved() -> None:
    """PRODUCTION = 0.0 must stay 0.0 after validate_physical_bounds (not NA)."""
    df = pd.DataFrame(
        {
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([0.0], dtype=np.float64),
            "WELLS": pd.array([1], dtype=pd.Int64Dtype()),
        }
    )
    result = validate_physical_bounds(df)
    assert result["PRODUCTION"].iloc[0] == 0.0
    assert not pd.isna(result["PRODUCTION"].iloc[0])


@pytest.mark.unit
def test_negative_production_becomes_na() -> None:
    """PRODUCTION = -1.0 must become pd.NA after validate_physical_bounds."""
    df = pd.DataFrame(
        {
            "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            "PRODUCTION": pd.array([-1.0], dtype=np.float64),
            "WELLS": pd.array([1], dtype=pd.Int64Dtype()),
        }
    )
    result = validate_physical_bounds(df)
    assert pd.isna(result["PRODUCTION"].iloc[0])
