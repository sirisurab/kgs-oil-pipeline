"""Unit tests for kgs_pipeline/transform.py."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.transform import (
    cast_categoricals,
    clean_production_values,
    deduplicate,
    fill_date_gaps,
    parse_production_date,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_partition(
    lease_kids: list[int] | None = None,
    month_years: list[str] | None = None,
    products: list[str] | None = None,
    productions: list[float] | None = None,
) -> pd.DataFrame:
    if lease_kids is None:
        lease_kids = [1001]
    if month_years is None:
        month_years = ["1-2024", "2-2024", "3-2024"]
    if products is None:
        products = ["O"] * len(month_years)
    if productions is None:
        productions = [100.0] * len(month_years)

    rows = []
    for lk in lease_kids:
        for my, prod_type, production in zip(month_years, products, productions):
            rows.append({
                "LEASE_KID": lk,
                "LEASE": f"Lease {lk}",
                "MONTH-YEAR": my,
                "PRODUCT": prod_type,
                "PRODUCTION": production,
                "WELLS": 2,
                "OPERATOR": "Test Op",
                "COUNTY": "Douglas",
                "TWN_DIR": "S",
                "RANGE_DIR": "E",
                "source_file": "test",
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Task T-01: parse_production_date
# ---------------------------------------------------------------------------


class TestParseProductionDate:
    def test_valid_dates_parsed_correctly(self) -> None:
        df = _make_partition(month_years=["1-2024", "12-2024"])
        result = parse_production_date(df)
        assert "production_date" in result.columns
        dates = result["production_date"].tolist()
        assert pd.Timestamp("2024-01-01") in dates
        assert pd.Timestamp("2024-12-01") in dates

    def test_drops_month_zero_rows(self) -> None:
        df = _make_partition(month_years=["0-2024", "3-2024"])
        result = parse_production_date(df)
        assert len(result) == 1
        assert result.iloc[0]["MONTH-YEAR"] == "3-2024"

    def test_drops_month_minus_one_rows(self) -> None:
        df = _make_partition(month_years=["-1-2024", "5-2024"])
        result = parse_production_date(df)
        assert len(result) == 1

    def test_drops_unparseable_rows(self) -> None:
        df = _make_partition(month_years=["bad-data", "6-2024"])
        result = parse_production_date(df)
        assert len(result) == 1

    def test_month_year_column_retained(self) -> None:
        df = _make_partition(month_years=["3-2024"])
        result = parse_production_date(df)
        assert "MONTH-YEAR" in result.columns

    def test_raises_keyerror_on_missing_column(self) -> None:
        df = pd.DataFrame({"OTHER": ["1-2024"]})
        with pytest.raises(KeyError):
            parse_production_date(df)


# ---------------------------------------------------------------------------
# Task T-02: clean_production_values
# ---------------------------------------------------------------------------


class TestCleanProductionValues:
    def test_negative_replaced_with_na(self) -> None:
        df = _make_partition(month_years=["1-2024"], productions=[-100.0])
        result = clean_production_values(df)
        assert pd.isna(result.iloc[0]["PRODUCTION"])

    def test_zero_preserved(self) -> None:
        df = _make_partition(month_years=["1-2024"], productions=[0.0])
        result = clean_production_values(df)
        val = result.iloc[0]["PRODUCTION"]
        assert val == 0.0 or (not pd.isna(val) and float(val) == 0.0)

    def test_oil_above_50k_replaced_with_na(self) -> None:
        df = _make_partition(
            month_years=["1-2024"], products=["O"], productions=[60000.0]
        )
        result = clean_production_values(df)
        assert pd.isna(result.iloc[0]["PRODUCTION"])

    def test_gas_above_50k_retained(self) -> None:
        df = _make_partition(
            month_years=["1-2024"], products=["G"], productions=[60000.0]
        )
        result = clean_production_values(df)
        assert not pd.isna(result.iloc[0]["PRODUCTION"])

    def test_raises_keyerror_missing_production(self) -> None:
        df = pd.DataFrame({"PRODUCT": ["O"]})
        with pytest.raises(KeyError, match="PRODUCTION"):
            clean_production_values(df)

    def test_raises_keyerror_missing_product(self) -> None:
        df = pd.DataFrame({"PRODUCTION": [100.0]})
        with pytest.raises(KeyError, match="PRODUCT"):
            clean_production_values(df)


# ---------------------------------------------------------------------------
# Task T-03: deduplicate
# ---------------------------------------------------------------------------


class TestDeduplicate:
    def test_drops_exact_key_duplicates(self) -> None:
        df = pd.concat([
            _make_partition(lease_kids=[1001], month_years=["1-2024"]),
            _make_partition(lease_kids=[1001], month_years=["1-2024"]),
        ], ignore_index=True)
        result = deduplicate(df)
        assert len(result) == 1

    def test_result_le_input(self) -> None:
        df = _make_partition(
            lease_kids=[1001, 1002],
            month_years=["1-2024", "2-2024"],
        )
        result = deduplicate(df)
        assert len(result) <= len(df)

    def test_idempotent(self) -> None:
        df = _make_partition(lease_kids=[1001], month_years=["1-2024", "2-2024"])
        once = deduplicate(df)
        twice = deduplicate(once)
        assert len(once) == len(twice)

    def test_no_duplicates_no_rows_dropped(self) -> None:
        df = _make_partition(lease_kids=[1001], month_years=["1-2024", "2-2024"])
        result = deduplicate(df)
        assert len(result) == len(df)

    def test_raises_keyerror_missing_column(self) -> None:
        df = pd.DataFrame({"MONTH-YEAR": ["1-2024"], "PRODUCT": ["O"]})
        with pytest.raises(KeyError, match="LEASE_KID"):
            deduplicate(df)


# ---------------------------------------------------------------------------
# Task T-04: cast_categoricals
# ---------------------------------------------------------------------------


class TestCastCategoricals:
    def test_product_cast_to_categorical(self) -> None:
        df = _make_partition(month_years=["1-2024"])
        result = cast_categoricals(df)
        assert isinstance(result["PRODUCT"].dtype, pd.CategoricalDtype)
        assert list(result["PRODUCT"].cat.categories) == ["O", "G"]

    def test_unknown_product_replaced_with_na(self) -> None:
        df = _make_partition(month_years=["1-2024"])
        df.loc[0, "PRODUCT"] = "X"
        result = cast_categoricals(df)
        assert pd.isna(result.iloc[0]["PRODUCT"])

    def test_twn_dir_range_dir_cast(self) -> None:
        df = _make_partition(month_years=["1-2024"])
        result = cast_categoricals(df)
        assert isinstance(result["TWN_DIR"].dtype, pd.CategoricalDtype)
        assert isinstance(result["RANGE_DIR"].dtype, pd.CategoricalDtype)

    def test_raises_keyerror_missing_column(self) -> None:
        df = pd.DataFrame({"PRODUCT": ["O"], "TWN_DIR": ["S"]})
        # Missing RANGE_DIR
        with pytest.raises(KeyError, match="RANGE_DIR"):
            cast_categoricals(df)


# ---------------------------------------------------------------------------
# Task T-05: fill_date_gaps
# ---------------------------------------------------------------------------


class TestFillDateGaps:
    def _prep_df(
        self,
        month_years: list[str],
        lease_kid: int = 1001,
        product: str = "O",
    ) -> pd.DataFrame:
        rows = []
        for my in month_years:
            parts = my.split("-")
            dt = pd.Timestamp(year=int(parts[1]), month=int(parts[0]), day=1)
            rows.append({
                "LEASE_KID": lease_kid,
                "PRODUCT": product,
                "production_date": dt,
                "PRODUCTION": 100.0,
                "MONTH-YEAR": my,
                "source_file": "test",
            })
        return pd.DataFrame(rows)

    def test_inserts_gap_row(self) -> None:
        df = self._prep_df(["1-2024", "3-2024"])
        result = fill_date_gaps(df)
        feb = pd.Timestamp("2024-02-01")
        assert feb in result["production_date"].values

    def test_gap_row_has_zero_production(self) -> None:
        df = self._prep_df(["1-2024", "3-2024"])
        result = fill_date_gaps(df)
        feb_rows = result[result["production_date"] == pd.Timestamp("2024-02-01")]
        assert len(feb_rows) == 1
        assert float(feb_rows.iloc[0]["PRODUCTION"]) == 0.0

    def test_no_gaps_no_rows_inserted(self) -> None:
        df = self._prep_df(["1-2024", "2-2024", "3-2024"])
        result = fill_date_gaps(df)
        assert len(result) == 3

    def test_row_count_matches_span(self) -> None:
        df = self._prep_df(["1-2024", "2-2024", "4-2024"])
        result = fill_date_gaps(df)
        # Jan–Apr = 4 months
        assert len(result) == 4

    def test_raises_keyerror_missing_production_date(self) -> None:
        df = pd.DataFrame({
            "LEASE_KID": [1001],
            "PRODUCT": ["O"],
            "PRODUCTION": [100.0],
        })
        with pytest.raises(KeyError):
            fill_date_gaps(df)


# ---------------------------------------------------------------------------
# Task T-06: transform (integration)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTransform:
    def test_returns_dask_dataframe(self, tmp_path: Path) -> None:
        from kgs_pipeline.transform import transform

        interim = _write_interim(tmp_path)
        processed = tmp_path / "processed"

        cfg = {"transform": {"interim_path": str(interim), "processed_path": str(processed)}}
        result = transform(cfg)
        assert isinstance(result, dd.DataFrame)

    def test_raises_when_interim_missing(self, tmp_path: Path) -> None:
        from kgs_pipeline.transform import transform

        cfg = {
            "transform": {
                "interim_path": str(tmp_path / "no_such_dir"),
                "processed_path": str(tmp_path / "processed"),
            }
        }
        with pytest.raises(FileNotFoundError):
            transform(cfg)

    def test_production_values_preserved(self, tmp_path: Path) -> None:
        from kgs_pipeline.transform import transform

        interim = _write_interim(tmp_path, production=123.4)
        processed = tmp_path / "processed"
        cfg = {"transform": {"interim_path": str(interim), "processed_path": str(processed)}}
        transform(cfg)

        read_back = dd.read_parquet(str(processed)).compute()
        assert (read_back["PRODUCTION"].dropna() == 123.4).any()

    def test_map_partitions_meta_consistency(self, tmp_path: Path) -> None:
        """TR-23: meta must match function output."""
        import dask.dataframe as dd
        from kgs_pipeline.ingest import build_dtype_map, _build_empty_frame

        dtype_map = build_dtype_map(Path("references/kgs_monthly_data_dictionary.csv"))
        base_meta = _build_empty_frame(dtype_map)

        # parse_production_date meta
        result_meta = parse_production_date(base_meta.copy())
        assert "production_date" in result_meta.columns


def _write_interim(tmp_path: Path, production: float = 150.0) -> Path:
    """Helper: write minimal interim Parquet for transform tests."""
    from tests.conftest import make_raw_csv_file
    from kgs_pipeline.ingest import build_dtype_map, parse_raw_file, filter_date_range

    raw = tmp_path / "raw_for_transform"
    raw.mkdir(exist_ok=True)
    make_raw_csv_file(raw / "t1.txt", production=production)
    make_raw_csv_file(raw / "t2.txt", lease_kids=[2001], production=production)

    dtype_map = build_dtype_map(Path("references/kgs_monthly_data_dictionary.csv"))
    parts = []
    for fp in raw.glob("*.txt"):
        df = parse_raw_file(fp, dtype_map)
        df = filter_date_range(df)
        parts.append(df)

    combined = pd.concat(parts, ignore_index=True)
    interim = tmp_path / "interim_for_transform"
    interim.mkdir(exist_ok=True)

    ddf = dd.from_pandas(combined, npartitions=2)
    ddf.to_parquet(str(interim), write_index=False, overwrite=True)
    return interim
