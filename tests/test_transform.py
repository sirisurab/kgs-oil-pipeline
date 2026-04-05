"""Tests for kgs_pipeline.transform module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pytest

from kgs_pipeline.transform import (
    deduplicate,
    flag_outliers,
    parse_production_date,
    pivot_products,
    read_interim,
    run_transform,
    sort_by_well_date,
    standardise_columns,
    write_processed_parquet,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_interim_df(rows: list[dict] | None = None) -> pd.DataFrame:
    """Build a minimal interim-schema DataFrame."""
    defaults = {
        "LEASE_KID": "1",
        "LEASE": "L1",
        "DOR_CODE": "DC",
        "API_NUMBER": "API001",
        "FIELD": "F",
        "PRODUCING_ZONE": "Z",
        "OPERATOR": "OP",
        "COUNTY": "ALLEN",
        "TOWNSHIP": "T",
        "TWN_DIR": "N",
        "RANGE": "R",
        "RANGE_DIR": "E",
        "SECTION": "1",
        "SPOT": "NE",
        "LATITUDE": 38.0,
        "LONGITUDE": -97.0,
        "MONTH_YEAR": "1-2024",
        "PRODUCT": "O",
        "WELLS": 1.0,
        "PRODUCTION": 100.0,
        "source_file": "test.txt",
    }
    if rows is None:
        rows = [defaults]
    else:
        rows = [{**defaults, **r} for r in rows]
    return pd.DataFrame(rows)


def _dask(df: pd.DataFrame) -> dd.DataFrame:
    return dd.from_pandas(df, npartitions=1)


def _make_wide_df(rows: list[dict] | None = None) -> pd.DataFrame:
    defaults = {
        "lease_kid": "1",
        "lease_name": "L1",
        "api_number": "API001",
        "operator": "OP",
        "county": "ALLEN",
        "field": "F",
        "producing_zone": "Z",
        "well_count": 1.0,
        "latitude": 38.0,
        "longitude": -97.0,
        "oil_bbl": 100.0,
        "gas_mcf": 0.0,
        "production_date": pd.Timestamp("2024-01-01"),
        "source_file": "test.txt",
    }
    if rows is None:
        rows = [defaults]
    else:
        rows = [{**defaults, **r} for r in rows]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Task 01: read_interim
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_interim_returns_dask(tmp_path: Path) -> None:
    df = pd.DataFrame({"col": [1, 2]})
    df.to_parquet(tmp_path / "part.parquet", index=False)
    result = read_interim(str(tmp_path))
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_read_interim_return_type_is_dask(tmp_path: Path) -> None:
    """TR-17."""
    df = pd.DataFrame({"col": [1]})
    df.to_parquet(tmp_path / "p.parquet", index=False)
    result = read_interim(str(tmp_path))
    assert isinstance(result, dd.DataFrame)
    assert not isinstance(result, pd.DataFrame)


@pytest.mark.unit
def test_read_interim_npartitions_capped(tmp_path: Path) -> None:
    df = pd.DataFrame({"col": range(10)})
    for i in range(5):
        df.to_parquet(tmp_path / f"part{i}.parquet", index=False)
    result = read_interim(str(tmp_path))
    assert result.npartitions <= 50


@pytest.mark.unit
def test_read_interim_missing_dir() -> None:
    with pytest.raises(FileNotFoundError):
        read_interim("/nonexistent/path")


# ---------------------------------------------------------------------------
# Task 02: parse_production_date
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_production_date_filters_nonmonthly() -> None:
    df = _make_interim_df(
        [
            {"MONTH_YEAR": "1-2024"},
            {"MONTH_YEAR": "0-2024"},
            {"MONTH_YEAR": "-1-2024"},
            {"MONTH_YEAR": "12-2024"},
        ]
    )
    ddf = _dask(df)
    result = parse_production_date(ddf).compute()
    assert len(result) == 2
    assert "production_date" in result.columns
    assert pd.api.types.is_datetime64_any_dtype(result["production_date"])


@pytest.mark.unit
def test_parse_production_date_correct_date() -> None:
    df = _make_interim_df([{"MONTH_YEAR": "6-2024"}])
    result = parse_production_date(_dask(df)).compute()
    assert result["production_date"].iloc[0] == pd.Timestamp("2024-06-01")


@pytest.mark.unit
def test_parse_production_date_unparseable_dropped() -> None:
    df = _make_interim_df([{"MONTH_YEAR": "abc-2024"}, {"MONTH_YEAR": "3-2024"}])
    result = parse_production_date(_dask(df)).compute()
    assert len(result) == 1


@pytest.mark.unit
def test_parse_production_date_no_month_year_column() -> None:
    df = _make_interim_df([{"MONTH_YEAR": "1-2024"}])
    result = parse_production_date(_dask(df)).compute()
    assert "MONTH_YEAR" not in result.columns


@pytest.mark.unit
def test_parse_production_date_returns_dask() -> None:
    df = _make_interim_df()
    result = parse_production_date(_dask(df))
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 03: standardise_columns
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_standardise_columns_renames() -> None:
    df = _make_interim_df()
    df["production_date"] = pd.Timestamp("2024-01-01")
    df = df.drop(columns=["MONTH_YEAR"])
    result = standardise_columns(_dask(df)).compute()
    assert "lease_kid" in result.columns
    assert "operator" in result.columns
    assert "county" in result.columns
    assert "production" in result.columns
    assert "well_count" in result.columns


@pytest.mark.unit
def test_standardise_columns_production_coerced() -> None:
    df = _make_interim_df([{"PRODUCTION": "abc"}])
    df["production_date"] = pd.Timestamp("2024-01-01")
    df = df.drop(columns=["MONTH_YEAR"])
    result = standardise_columns(_dask(df)).compute()
    assert pd.isna(result["production"].iloc[0])


@pytest.mark.unit
def test_standardise_columns_returns_dask() -> None:
    df = _make_interim_df()
    result = standardise_columns(_dask(df))
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_standardise_columns_drops_dor_code() -> None:
    df = _make_interim_df()
    result = standardise_columns(_dask(df)).compute()
    assert "DOR_CODE" not in result.columns


# ---------------------------------------------------------------------------
# Task 04: deduplicate
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_deduplicate_removes_duplicates() -> None:
    base = {
        "lease_kid": pd.array(["1"] * 10, dtype=pd.StringDtype()),
        "product": pd.array(["O"] * 10, dtype=pd.StringDtype()),
        "production_date": [pd.Timestamp("2024-01-01")] * 7 + [pd.Timestamp("2024-02-01")] * 3,
        "oil_bbl": [100.0] * 10,
    }
    df = pd.DataFrame(base)
    result = deduplicate(_dask(df)).compute()
    # 7 dupes of jan → 1, 3 dupes of feb → 1 → 2 rows
    assert len(result) == 2


@pytest.mark.unit
def test_deduplicate_no_change_if_unique() -> None:
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["1", "2"], dtype=pd.StringDtype()),
            "product": pd.array(["O", "G"], dtype=pd.StringDtype()),
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")],
            "oil_bbl": [100.0, 200.0],
        }
    )
    result = deduplicate(_dask(df)).compute()
    assert len(result) == 2


@pytest.mark.unit
def test_deduplicate_returns_dask() -> None:
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["1"], dtype=pd.StringDtype()),
            "product": pd.array(["O"], dtype=pd.StringDtype()),
            "production_date": [pd.Timestamp("2024-01-01")],
        }
    )
    result = deduplicate(_dask(df))
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_tr15a_dedup_count_lte_original() -> None:
    """TR-15a: deduplicated count <= original count."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["1"] * 5, dtype=pd.StringDtype()),
            "product": pd.array(["O"] * 5, dtype=pd.StringDtype()),
            "production_date": [pd.Timestamp("2024-01-01")] * 3 + [pd.Timestamp("2024-02-01")] * 2,
            "val": range(5),
        }
    )
    result = deduplicate(_dask(df)).compute()
    assert len(result) <= 5


@pytest.mark.unit
def test_tr15b_dedup_idempotent() -> None:
    """TR-15b: running deduplication twice yields same result."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["1", "1"], dtype=pd.StringDtype()),
            "product": pd.array(["O", "O"], dtype=pd.StringDtype()),
            "production_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
            "val": [1, 2],
        }
    )
    ddf = _dask(df)
    r1 = deduplicate(ddf).compute()
    ddf2 = dd.from_pandas(r1, npartitions=1)
    r2 = deduplicate(ddf2).compute()
    assert r1.shape == r2.shape


# ---------------------------------------------------------------------------
# Task 05: pivot_products
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_pivot_products_oil_and_gas() -> None:
    df = pd.DataFrame(
        [
            {
                "lease_kid": "1",
                "production_date": pd.Timestamp("2024-01-01"),
                "api_number": "A",
                "operator": "OP",
                "county": "C",
                "field": "F",
                "producing_zone": "Z",
                "well_count": 1.0,
                "latitude": 38.0,
                "longitude": -97.0,
                "source_file": "t.txt",
                "product": "O",
                "production": 100.0,
                "lease_name": "L",
            },
            {
                "lease_kid": "1",
                "production_date": pd.Timestamp("2024-01-01"),
                "api_number": "A",
                "operator": "OP",
                "county": "C",
                "field": "F",
                "producing_zone": "Z",
                "well_count": 1.0,
                "latitude": 38.0,
                "longitude": -97.0,
                "source_file": "t.txt",
                "product": "G",
                "production": 500.0,
                "lease_name": "L",
            },
        ]
    )
    result = pivot_products(_dask(df)).compute()
    assert len(result) == 1
    assert result["oil_bbl"].iloc[0] == 100.0
    assert result["gas_mcf"].iloc[0] == 500.0


@pytest.mark.unit
def test_pivot_products_oil_only_gas_zero() -> None:
    df = pd.DataFrame(
        [
            {
                "lease_kid": "1",
                "production_date": pd.Timestamp("2024-01-01"),
                "api_number": "A",
                "operator": "OP",
                "county": "C",
                "field": "F",
                "producing_zone": "Z",
                "well_count": 1.0,
                "latitude": 38.0,
                "longitude": -97.0,
                "source_file": "t.txt",
                "product": "O",
                "production": 200.0,
                "lease_name": "L",
            },
        ]
    )
    result = pivot_products(_dask(df)).compute()
    assert result["oil_bbl"].iloc[0] == 200.0
    assert result["gas_mcf"].iloc[0] == 0.0


@pytest.mark.unit
def test_tr05_zero_production_not_nan() -> None:
    """TR-05: zero production must remain 0.0, not NaN."""
    df = pd.DataFrame(
        [
            {
                "lease_kid": "1",
                "production_date": pd.Timestamp("2024-01-01"),
                "api_number": "A",
                "operator": "OP",
                "county": "C",
                "field": "F",
                "producing_zone": "Z",
                "well_count": 1.0,
                "latitude": 38.0,
                "longitude": -97.0,
                "source_file": "t.txt",
                "product": "O",
                "production": 0.0,
                "lease_name": "L",
            },
        ]
    )
    result = pivot_products(_dask(df)).compute()
    assert result["oil_bbl"].iloc[0] == 0.0
    assert not pd.isna(result["oil_bbl"].iloc[0])


@pytest.mark.unit
def test_pivot_products_drops_product_column() -> None:
    df = pd.DataFrame(
        [
            {
                "lease_kid": "1",
                "production_date": pd.Timestamp("2024-01-01"),
                "api_number": "A",
                "operator": "OP",
                "county": "C",
                "field": "F",
                "producing_zone": "Z",
                "well_count": 1.0,
                "latitude": 38.0,
                "longitude": -97.0,
                "source_file": "t.txt",
                "product": "O",
                "production": 100.0,
                "lease_name": "L",
            },
        ]
    )
    result = pivot_products(_dask(df)).compute()
    assert "product" not in result.columns
    assert "production" not in result.columns


@pytest.mark.unit
def test_pivot_products_returns_dask() -> None:
    df = pd.DataFrame(
        [
            {
                "lease_kid": "1",
                "production_date": pd.Timestamp("2024-01-01"),
                "api_number": "A",
                "operator": "OP",
                "county": "C",
                "field": "F",
                "producing_zone": "Z",
                "well_count": 1.0,
                "latitude": 38.0,
                "longitude": -97.0,
                "source_file": "t.txt",
                "product": "O",
                "production": 100.0,
                "lease_name": "L",
            },
        ]
    )
    result = pivot_products(_dask(df))
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 06: flag_outliers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr01_negative_production_flagged() -> None:
    """TR-01: negative oil_bbl → has_negative_production=True, row retained."""
    df = _make_wide_df([{"oil_bbl": -5.0}])
    result = flag_outliers(_dask(df)).compute()
    assert result["has_negative_production"].iloc[0]
    assert len(result) == 1


@pytest.mark.unit
def test_tr02_oil_outlier_flagged() -> None:
    """TR-02: oil_bbl > 50000 → is_outlier=True."""
    df = _make_wide_df([{"oil_bbl": 100_000.0}])
    result = flag_outliers(_dask(df)).compute()
    assert result["is_outlier"].iloc[0]


@pytest.mark.unit
def test_tr02_oil_below_threshold_not_outlier() -> None:
    df = _make_wide_df([{"oil_bbl": 49_999.0}])
    result = flag_outliers(_dask(df)).compute()
    assert not result["is_outlier"].iloc[0]


@pytest.mark.unit
def test_flag_outliers_gas_threshold() -> None:
    df = _make_wide_df([{"gas_mcf": 600_000.0}])
    result = flag_outliers(_dask(df)).compute()
    assert result["is_outlier"].iloc[0]


@pytest.mark.unit
def test_flag_outliers_returns_dask() -> None:
    df = _make_wide_df()
    result = flag_outliers(_dask(df))
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_flag_outliers_columns_present() -> None:
    df = _make_wide_df()
    result = flag_outliers(_dask(df)).compute()
    assert "is_outlier" in result.columns
    assert "has_negative_production" in result.columns


# ---------------------------------------------------------------------------
# Task 07: sort_by_well_date
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr16_sort_ascending() -> None:
    """TR-16: rows sorted ascending by production_date per lease."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["1", "1", "2", "2"], dtype=pd.StringDtype()),
            "production_date": [
                pd.Timestamp("2024-03-01"),
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-02-01"),
                pd.Timestamp("2024-04-01"),
            ],
            "oil_bbl": [300.0, 100.0, 200.0, 400.0],
        }
    )
    result = sort_by_well_date(_dask(df)).compute()
    lease1 = result[result["lease_kid"] == "1"]["production_date"].tolist()
    assert lease1 == sorted(lease1)


@pytest.mark.unit
def test_sort_by_well_date_returns_dask() -> None:
    df = _make_wide_df()
    result = sort_by_well_date(_dask(df))
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_tr04_sort_preserves_row_count() -> None:
    """TR-04: all 12 monthly rows for a lease survive sort."""
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["1"] * 12, dtype=pd.StringDtype()),
            "production_date": [pd.Timestamp(f"2024-{m:02d}-01") for m in range(1, 13)],
            "oil_bbl": [100.0] * 12,
        }
    )
    result = sort_by_well_date(_dask(df)).compute()
    assert len(result[result["lease_kid"] == "1"]) == 12


# ---------------------------------------------------------------------------
# Task 08: write_processed_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_processed_parquet_writes(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "lease_kid": pd.array(["1"] * 10, dtype=pd.StringDtype()),
            "oil_bbl": [100.0] * 10,
            "production_date": [pd.Timestamp("2024-01-01")] * 10,
        }
    )
    ddf = dd.from_pandas(df, npartitions=5)
    out = tmp_path / "processed"
    count = write_processed_parquet(ddf, str(out))
    assert count >= 1
    assert len(list(out.glob("*.parquet"))) == count


@pytest.mark.unit
def test_write_processed_parquet_readable(tmp_path: Path) -> None:
    """TR-18: written Parquet files are readable by pd.read_parquet."""
    df = pd.DataFrame({"val": [1, 2, 3]})
    ddf = dd.from_pandas(df, npartitions=1)
    out = tmp_path / "p"
    write_processed_parquet(ddf, str(out))
    for f in out.glob("*.parquet"):
        pd.read_parquet(f)


@pytest.mark.unit
def test_write_processed_parquet_count_matches(tmp_path: Path) -> None:
    df = pd.DataFrame({"val": range(10)})
    ddf = dd.from_pandas(df, npartitions=5)
    out = tmp_path / "out"
    count = write_processed_parquet(ddf, str(out))
    assert count == len(list(out.glob("*.parquet")))


# ---------------------------------------------------------------------------
# Task 09: run_transform + domain tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_transform_calls_stages(tmp_path: Path) -> None:
    with (
        patch("kgs_pipeline.transform.read_interim") as m1,
        patch("kgs_pipeline.transform.parse_production_date") as m2,
        patch("kgs_pipeline.transform.standardise_columns") as m3,
        patch("kgs_pipeline.transform.deduplicate") as m4,
        patch("kgs_pipeline.transform.pivot_products") as m5,
        patch("kgs_pipeline.transform.flag_outliers") as m6,
        patch("kgs_pipeline.transform.sort_by_well_date") as m7,
        patch("kgs_pipeline.transform.write_processed_parquet", return_value=3) as m8,
    ):
        mock_ddf = MagicMock()
        m1.return_value = mock_ddf
        m2.return_value = mock_ddf
        m3.return_value = mock_ddf
        m4.return_value = mock_ddf
        m5.return_value = mock_ddf
        m6.return_value = mock_ddf
        m7.return_value = mock_ddf

        result = run_transform(str(tmp_path), str(tmp_path / "out"))
        assert result == 3
        m1.assert_called_once()
        m2.assert_called_once()
        m8.assert_called_once()


@pytest.mark.unit
def test_tr01_negative_retained_in_pipeline() -> None:
    """TR-01: row with oil_bbl=-1 has has_negative_production=True and is retained."""
    df = _make_wide_df([{"oil_bbl": -1.0, "gas_mcf": 0.0}])
    result = flag_outliers(_dask(df)).compute()
    neg_rows = result[result["has_negative_production"]]
    assert len(neg_rows) == 1


@pytest.mark.unit
def test_tr02_outlier_flag() -> None:
    """TR-02: oil_bbl=100001 → is_outlier=True."""
    df = _make_wide_df([{"oil_bbl": 100_001.0}])
    result = flag_outliers(_dask(df)).compute()
    assert result["is_outlier"].iloc[0]


@pytest.mark.unit
def test_tr04_well_completeness() -> None:
    """TR-04: 12 monthly rows survive the parse_production_date step."""
    df = _make_interim_df([{"MONTH_YEAR": f"{m}-2024"} for m in range(1, 13)])
    result = parse_production_date(_dask(df)).compute()
    assert len(result) == 12


@pytest.mark.unit
def test_tr05_zero_production_oil() -> None:
    """TR-05: production=0.0 for product O → oil_bbl=0.0 (not NaN)."""
    df = pd.DataFrame(
        [
            {
                "lease_kid": "1",
                "production_date": pd.Timestamp("2024-01-01"),
                "api_number": "A",
                "operator": "OP",
                "county": "C",
                "field": "F",
                "producing_zone": "Z",
                "well_count": 1.0,
                "latitude": 38.0,
                "longitude": -97.0,
                "source_file": "t.txt",
                "product": "O",
                "production": 0.0,
                "lease_name": "L",
            }
        ]
    )
    result = pivot_products(_dask(df)).compute()
    assert result["oil_bbl"].iloc[0] == 0.0
    assert not pd.isna(result["oil_bbl"].iloc[0])


@pytest.mark.unit
def test_tr17_all_functions_return_dask() -> None:
    """TR-17: all transform functions return dask.dataframe.DataFrame."""
    df_interim = _make_interim_df()
    ddf_interim = _dask(df_interim)

    r1 = parse_production_date(ddf_interim)
    assert isinstance(r1, dd.DataFrame)

    df_std = _make_interim_df()
    df_std["production_date"] = pd.Timestamp("2024-01-01")
    df_std = df_std.drop(columns=["MONTH_YEAR"])
    r2 = standardise_columns(_dask(df_std))
    assert isinstance(r2, dd.DataFrame)

    df_wide = _make_wide_df()
    r3 = flag_outliers(_dask(df_wide))
    assert isinstance(r3, dd.DataFrame)

    r4 = sort_by_well_date(_dask(df_wide))
    assert isinstance(r4, dd.DataFrame)
