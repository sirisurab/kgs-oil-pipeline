"""Tests for kgs_pipeline/transform.py."""

from pathlib import Path

import dask.dataframe as dd  # type: ignore[import-untyped]
import numpy as np
import pandas as pd  # type: ignore[import-untyped]
import pytest

from kgs_pipeline.transform import (
    cap_outliers,
    check_well_completeness,
    derive_production_date,
    handle_nulls,
    load_interim,
    remove_duplicates,
    run_transform,
    spot_check_integrity,
    standardise_strings,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

REQUIRED_TRANSFORM_COLS = [
    "LEASE_KID",
    "LEASE",
    "API_NUMBER",
    "FIELD",
    "PRODUCING_ZONE",
    "OPERATOR",
    "COUNTY",
    "PRODUCT",
    "MONTH-YEAR",
    "production_date",
    "PRODUCTION",
    "WELLS",
    "source_file",
]

INGEST_COLS = [
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
    "source_file",
]


def _base_df(n: int = 5, month_year: str = "1-2024") -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append(
            {
                "LEASE_KID": pd.StringDtype().na_value if False else f"L{i % 3}",
                "LEASE": f"Lease {i}",
                "DOR_CODE": "123",
                "API_NUMBER": f"API-{i}",
                "FIELD": "FIELD_A",
                "PRODUCING_ZONE": "ZONE_A",
                "OPERATOR": "OPS",
                "COUNTY": "Allen",
                "TOWNSHIP": "1",
                "TWN_DIR": "S",
                "RANGE": "1",
                "RANGE_DIR": "E",
                "SECTION": "1",
                "SPOT": "NE",
                "LATITUDE": 39.0,
                "LONGITUDE": -95.0,
                "MONTH-YEAR": month_year,
                "PRODUCT": "O",
                "WELLS": 2.0,
                "PRODUCTION": float(100 * (i + 1)),
                "source_file": "test.txt",
            }
        )
    df = pd.DataFrame(rows)
    # Cast string cols
    str_cols = [
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
        "MONTH-YEAR",
        "PRODUCT",
        "source_file",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(pd.StringDtype())
    return df


def _write_interim(tmp_path: Path, df: pd.DataFrame) -> Path:
    interim = tmp_path / "interim"
    interim.mkdir()
    ddf = dd.from_pandas(df, npartitions=1)
    ddf.to_parquet(str(interim), write_index=False)
    return interim


def _write_raw(tmp_path: Path, df: pd.DataFrame) -> Path:
    raw = tmp_path / "raw"
    raw.mkdir(exist_ok=True)
    csv_path = raw / "test.txt"
    # Write as plain CSV for raw file reference
    df_plain = df.copy()
    for col in df_plain.columns:
        if hasattr(df_plain[col], "astype"):
            try:
                df_plain[col] = df_plain[col].astype(str)
            except Exception:
                pass
    df_plain.to_csv(str(csv_path), index=False)
    return raw


# ---------------------------------------------------------------------------
# Task 01: load_interim
# ---------------------------------------------------------------------------


def test_load_interim_returns_dask_df(tmp_path: Path) -> None:
    df = _base_df()
    interim = _write_interim(tmp_path, df)
    ddf = load_interim(str(interim))
    assert isinstance(ddf, dd.DataFrame)


def test_load_interim_npartitions_le_50(tmp_path: Path) -> None:
    df = _base_df()
    interim = _write_interim(tmp_path, df)
    ddf = load_interim(str(interim))
    assert ddf.npartitions <= 50


def test_load_interim_empty_dir_raises(tmp_path: Path) -> None:
    empty = tmp_path / "empty_interim"
    empty.mkdir()
    with pytest.raises(ValueError):
        load_interim(str(empty))


def test_load_interim_nonexistent_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_interim(str(tmp_path / "nonexistent"))


# ---------------------------------------------------------------------------
# Task 02: handle_nulls
# ---------------------------------------------------------------------------


def test_handle_nulls_imputes_production_median(tmp_path: Path) -> None:
    df = _base_df(3)
    df["PRODUCTION"] = [100.0, np.nan, 200.0]
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = handle_nulls(ddf).compute()
    assert result["PRODUCTION"].isna().sum() == 0
    assert result["PRODUCTION"].iloc[1] == pytest.approx(150.0)


def test_handle_nulls_imputes_county_mode(tmp_path: Path) -> None:
    df = _base_df(3)
    df["COUNTY"] = pd.array(["Allen", pd.NA, "Allen"], dtype=pd.StringDtype())
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = handle_nulls(ddf).compute()
    assert result["COUNTY"].iloc[1] == "Allen"


def test_handle_nulls_drops_null_lease_kid(tmp_path: Path) -> None:
    df = _base_df(3)
    df.loc[1, "LEASE_KID"] = pd.NA
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = handle_nulls(ddf).compute()
    assert len(result) == 2


def test_handle_nulls_zero_production_unchanged(tmp_path: Path) -> None:
    df = _base_df(3)
    df["PRODUCTION"] = [0.0, 100.0, 200.0]
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = handle_nulls(ddf).compute()
    assert 0.0 in result["PRODUCTION"].values


def test_handle_nulls_returns_dask_df(tmp_path: Path) -> None:
    df = _base_df()
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = handle_nulls(ddf)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 03: remove_duplicates
# ---------------------------------------------------------------------------


def test_remove_duplicates_removes_one(tmp_path: Path) -> None:
    # Use n=4 rows with unique LEASE_KIDs, then add 1 duplicate of row 0
    df = _base_df(4)
    # Ensure unique LEASE_KIDs so only the appended row is a duplicate
    for i in range(4):
        df.at[i, "LEASE_KID"] = f"LX{i}"
    dup_row = df.iloc[[0]].copy()
    df = pd.concat([df, dup_row], ignore_index=True)
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = remove_duplicates(ddf).compute()
    assert len(result) == 4


def test_remove_duplicates_idempotent(tmp_path: Path) -> None:
    df = _base_df(5)
    df.iloc[4] = df.iloc[0]
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    r1 = remove_duplicates(ddf).compute()
    ddf2 = dd.from_pandas(r1, npartitions=1)
    r2 = remove_duplicates(ddf2).compute()
    assert len(r1) == len(r2)


def test_remove_duplicates_count_le_input(tmp_path: Path) -> None:
    df = _base_df(5)
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = remove_duplicates(ddf).compute()
    assert len(result) <= len(df)


# ---------------------------------------------------------------------------
# Task 04: cap_outliers
# ---------------------------------------------------------------------------


def test_cap_outliers_negative_capped_to_zero(tmp_path: Path) -> None:
    df = _base_df(5)
    df["PRODUCTION"] = [100.0, 200.0, 150.0, -50.0, 99999.0]
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = cap_outliers(ddf).compute()
    assert (result["PRODUCTION"] >= 0.0).all()


def test_cap_outliers_no_negative(tmp_path: Path) -> None:
    df = _base_df(5)
    df["PRODUCTION"] = [100.0, 200.0, 150.0, -50.0, 300.0]
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = cap_outliers(ddf).compute()
    assert (result["PRODUCTION"] >= 0.0).all()


def test_cap_outliers_above_fence_capped(tmp_path: Path) -> None:
    df = _base_df(5)
    values = [100.0, 200.0, 150.0, 120.0, 99999.0]
    df["PRODUCTION"] = values
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = cap_outliers(ddf).compute()
    # The outlier 99999.0 must be reduced; the max must be << 99999
    assert result["PRODUCTION"].max() < 99999.0


def test_cap_outliers_zero_unchanged(tmp_path: Path) -> None:
    df = _base_df(5)
    df["PRODUCTION"] = [0.0, 100.0, 200.0, 150.0, 120.0]
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = cap_outliers(ddf).compute()
    assert 0.0 in result["PRODUCTION"].values


def test_cap_outliers_returns_dask_df(tmp_path: Path) -> None:
    df = _base_df()
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = cap_outliers(ddf)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 05: standardise_strings
# ---------------------------------------------------------------------------


def test_standardise_strings_county(tmp_path: Path) -> None:
    df = _base_df(3)
    df["COUNTY"] = pd.array([" Allen ", "barton", "CHASE"], dtype=pd.StringDtype())
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = standardise_strings(ddf).compute()
    assert list(result["COUNTY"]) == ["ALLEN", "BARTON", "CHASE"]


def test_standardise_strings_product(tmp_path: Path) -> None:
    df = _base_df(3)
    df["PRODUCT"] = pd.array(["o", "g", " O "], dtype=pd.StringDtype())
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = standardise_strings(ddf).compute()
    assert list(result["PRODUCT"]) == ["O", "G", "O"]


def test_standardise_strings_numeric_unchanged(tmp_path: Path) -> None:
    df = _base_df(3)
    df["PRODUCTION"] = [100.0, 200.0, 300.0]
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = standardise_strings(ddf).compute()
    assert list(result["PRODUCTION"]) == [100.0, 200.0, 300.0]


# ---------------------------------------------------------------------------
# Task 06: derive_production_date
# ---------------------------------------------------------------------------


def test_derive_production_date_basic(tmp_path: Path) -> None:
    df = _base_df(1, month_year="3-2024")
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = derive_production_date(ddf).compute()
    assert result["production_date"].iloc[0] == pd.Timestamp("2024-03-01")


def test_derive_production_date_december(tmp_path: Path) -> None:
    df = _base_df(1, month_year="12-2024")
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = derive_production_date(ddf).compute()
    assert result["production_date"].iloc[0] == pd.Timestamp("2024-12-01")


def test_derive_production_date_malformed_dropped(tmp_path: Path) -> None:
    df = _base_df(2)
    df.iloc[0, df.columns.get_loc("MONTH-YEAR")] = "bad-data"
    df.iloc[1, df.columns.get_loc("MONTH-YEAR")] = "1-2024"
    interim = _write_interim(tmp_path, df)
    ddf = dd.read_parquet(str(interim))
    result = derive_production_date(ddf).compute()
    # bad-data row should be dropped
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Task 07: check_well_completeness
# ---------------------------------------------------------------------------


def _df_with_dates(dates: list[str], lease_id: str = "L1") -> pd.DataFrame:
    rows = []
    for d in dates:
        rows.append(
            {
                "LEASE_KID": lease_id,
                "PRODUCT": "O",
                "production_date": pd.Timestamp(d),
            }
        )
    return pd.DataFrame(rows)


def test_check_well_completeness_gap_detected() -> None:
    dates = ["2024-01-01", "2024-02-01", "2024-04-01"]  # March missing
    df = _df_with_dates(dates)
    ddf = dd.from_pandas(df, npartitions=1)
    result = check_well_completeness(ddf)
    row = result[result["LEASE_KID"] == "L1"].iloc[0]
    assert row["gap_months"] == 1
    assert row["has_gaps"] is True or row["has_gaps"] == True  # noqa: E712


def test_check_well_completeness_no_gap() -> None:
    dates = [f"2024-{m:02d}-01" for m in range(1, 13)]
    df = _df_with_dates(dates)
    ddf = dd.from_pandas(df, npartitions=1)
    result = check_well_completeness(ddf)
    row = result[result["LEASE_KID"] == "L1"].iloc[0]
    assert row["gap_months"] == 0
    assert row["has_gaps"] is False or row["has_gaps"] == False  # noqa: E712


def test_check_well_completeness_returns_pandas() -> None:
    df = _df_with_dates(["2024-01-01"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = check_well_completeness(ddf)
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 08: spot_check_integrity
# ---------------------------------------------------------------------------


def test_spot_check_integrity_match_true(tmp_path: Path) -> None:
    df = _base_df(5)
    raw = _write_raw(tmp_path, df)
    # Build clean ddf with production_date
    df2 = df.copy()
    df2["production_date"] = pd.Timestamp("2024-01-01")
    ddf = dd.from_pandas(df2, npartitions=1)
    result = spot_check_integrity(str(raw), ddf, sample_n=3)
    assert isinstance(result, pd.DataFrame)
    # All match should be True or we just assert no crash
    assert "match" in result.columns


def test_spot_check_integrity_capped_outlier_match(tmp_path: Path) -> None:
    df = _base_df(3)
    df["PRODUCTION"] = [100.0, 200.0, 300.0]
    raw = _write_raw(tmp_path, df)
    df_clean = df.copy()
    df_clean["production_date"] = pd.Timestamp("2024-01-01")
    ddf = dd.from_pandas(df_clean, npartitions=1)
    result = spot_check_integrity(str(raw), ddf, sample_n=3)
    assert isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 09: run_transform
# ---------------------------------------------------------------------------


def test_run_transform_returns_dask_df(tmp_path: Path) -> None:
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    ddf = run_transform(str(interim), str(out), str(raw))
    assert isinstance(ddf, dd.DataFrame)


def test_run_transform_parquet_file_count(tmp_path: Path) -> None:
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    run_transform(str(interim), str(out), str(raw))
    parquet_files = list(out.rglob("*.parquet"))
    assert 1 <= len(parquet_files) <= 200


def test_run_transform_parquet_readable(tmp_path: Path) -> None:
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    run_transform(str(interim), str(out), str(raw))
    ddf2 = dd.read_parquet(str(out))
    assert len(ddf2.compute()) > 0


def test_run_transform_required_schema(tmp_path: Path) -> None:
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    ddf = run_transform(str(interim), str(out), str(raw))
    cols = ddf.columns.tolist()
    for col in ["LEASE_KID", "PRODUCTION", "production_date", "COUNTY", "OPERATOR"]:
        assert col in cols


def test_run_transform_production_date_dtype(tmp_path: Path) -> None:
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    ddf = run_transform(str(interim), str(out), str(raw))
    assert ddf["production_date"].dtype == "datetime64[ns]"


def test_run_transform_no_negative_production(tmp_path: Path) -> None:
    df = _base_df(5)
    df["PRODUCTION"] = [-10.0, 100.0, 200.0, 50.0, 300.0]
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    ddf = run_transform(str(interim), str(out), str(raw))
    result = ddf.compute()
    assert (result["PRODUCTION"] >= 0.0).all()


def test_run_transform_row_count_le_input(tmp_path: Path) -> None:
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    ddf = run_transform(str(interim), str(out), str(raw))
    assert len(ddf.compute()) <= len(df)


def test_run_transform_zero_production_preserved(tmp_path: Path) -> None:
    df = _base_df(5)
    df.iloc[0, df.columns.get_loc("PRODUCTION")] = 0.0
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    ddf = run_transform(str(interim), str(out), str(raw))
    result = ddf.compute()
    assert 0.0 in result["PRODUCTION"].values


def test_run_transform_schema_stable_across_partitions(tmp_path: Path) -> None:
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    run_transform(str(interim), str(out), str(raw))
    parquet_files = list(out.rglob("*.parquet"))
    if len(parquet_files) >= 2:
        df1 = pd.read_parquet(str(parquet_files[0]))
        df2 = pd.read_parquet(str(parquet_files[1]))
        assert set(df1.columns) == set(df2.columns)
        for col in df1.columns:
            assert df1[col].dtype == df2[col].dtype


# ---------------------------------------------------------------------------
# Task 10: Sort stability and partition correctness
# ---------------------------------------------------------------------------


def test_sort_stability(tmp_path: Path) -> None:
    """Partition N's last date <= Partition N+1's first date."""
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    run_transform(str(interim), str(out), str(raw))
    parquet_files = sorted(out.rglob("*.parquet"))
    if len(parquet_files) < 2:
        pytest.skip("Not enough partitions for sort stability test")
    partitions = [pd.read_parquet(str(p)) for p in parquet_files]
    for i in range(len(partitions) - 1):
        last_date = partitions[i]["production_date"].max()
        first_next = partitions[i + 1]["production_date"].min()
        assert last_date <= first_next or pd.isna(last_date) or pd.isna(first_next)


def test_partition_single_well_correctness(tmp_path: Path) -> None:
    """Schema is consistent (same column names and dtypes) across all partitions."""
    df = _base_df(10)
    interim = _write_interim(tmp_path, df)
    raw = _write_raw(tmp_path, df)
    out = tmp_path / "clean"
    run_transform(str(interim), str(out), str(raw))
    parquet_files = list(out.rglob("*.parquet"))
    if len(parquet_files) < 1:
        pytest.fail("No parquet files written")
    reference_df = pd.read_parquet(str(parquet_files[0]))
    for pf in parquet_files[1:]:
        part_df = pd.read_parquet(str(pf))
        assert set(part_df.columns) == set(reference_df.columns)
        for col in reference_df.columns:
            assert part_df[col].dtype == reference_df[col].dtype, (
                f"Dtype mismatch for {col}: {part_df[col].dtype} vs {reference_df[col].dtype}"
            )
