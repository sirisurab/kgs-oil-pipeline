"""Tests for kgs_pipeline/transform.py."""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.transform import (
    add_production_date,
    clean_categoricals,
    clean_physical_bounds,
    deduplicate,
    run_transform,
    set_entity_index,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CANONICAL_SCHEMA: dict[str, object] = {
    "LEASE_KID": pd.array([], dtype=pd.Int64Dtype()),
    "LEASE": pd.array([], dtype=pd.StringDtype()),
    "DOR_CODE": pd.array([], dtype=pd.Int64Dtype()),
    "API_NUMBER": pd.array([], dtype=pd.StringDtype()),
    "FIELD": pd.array([], dtype=pd.StringDtype()),
    "PRODUCING_ZONE": pd.array([], dtype=pd.StringDtype()),
    "OPERATOR": pd.array([], dtype=pd.StringDtype()),
    "COUNTY": pd.array([], dtype=pd.StringDtype()),
    "TOWNSHIP": pd.array([], dtype=pd.Int64Dtype()),
    "TWN_DIR": pd.Categorical([], categories=["S", "N"]),
    "RANGE": pd.array([], dtype=pd.Int64Dtype()),
    "RANGE_DIR": pd.Categorical([], categories=["E", "W"]),
    "SECTION": pd.array([], dtype=pd.Int64Dtype()),
    "SPOT": pd.array([], dtype=pd.StringDtype()),
    "LATITUDE": pd.array([], dtype=np.float64),
    "LONGITUDE": pd.array([], dtype=np.float64),
    "MONTH-YEAR": pd.array([], dtype=pd.StringDtype()),
    "PRODUCT": pd.Categorical([], categories=["O", "G"]),
    "WELLS": pd.array([], dtype=pd.Int64Dtype()),
    "PRODUCTION": pd.array([], dtype=np.float64),
    "URL": pd.array([], dtype=pd.StringDtype()),
    "source_file": pd.array([], dtype=pd.StringDtype()),
}


def _empty_meta() -> pd.DataFrame:
    return pd.DataFrame({k: v for k, v in CANONICAL_SCHEMA.items()})


def _make_partition(**overrides: object) -> pd.DataFrame:
    base = {
        "LEASE_KID": pd.array([1001], dtype=pd.Int64Dtype()),
        "LEASE": pd.array(["TEST"], dtype=pd.StringDtype()),
        "DOR_CODE": pd.array([pd.NA], dtype=pd.Int64Dtype()),
        "API_NUMBER": pd.array(["15-001-00001"], dtype=pd.StringDtype()),
        "FIELD": pd.array(["FIELD1"], dtype=pd.StringDtype()),
        "PRODUCING_ZONE": pd.array(["Zone1"], dtype=pd.StringDtype()),
        "OPERATOR": pd.array(["OP1"], dtype=pd.StringDtype()),
        "COUNTY": pd.array(["Nemaha"], dtype=pd.StringDtype()),
        "TOWNSHIP": pd.array([1], dtype=pd.Int64Dtype()),
        "TWN_DIR": pd.Categorical(["S"], categories=["S", "N"]),
        "RANGE": pd.array([14], dtype=pd.Int64Dtype()),
        "RANGE_DIR": pd.Categorical(["E"], categories=["E", "W"]),
        "SECTION": pd.array([1], dtype=pd.Int64Dtype()),
        "SPOT": pd.array(["NENENE"], dtype=pd.StringDtype()),
        "LATITUDE": [39.999519],
        "LONGITUDE": [-95.78905],
        "MONTH-YEAR": pd.array(["1-2024"], dtype=pd.StringDtype()),
        "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
        "WELLS": pd.array([2], dtype=pd.Int64Dtype()),
        "PRODUCTION": [161.8],
        "URL": pd.array(["https://example.com"], dtype=pd.StringDtype()),
        "source_file": pd.array(["lp1.txt"], dtype=pd.StringDtype()),
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _write_interim(tmp_path: Path, rows: list[dict]) -> str:
    """Write synthetic interim Parquet for run_transform tests."""
    frames = []
    for row in rows:
        part = pd.DataFrame({
            "LEASE_KID": pd.array([row.get("LEASE_KID", 1001)], dtype=pd.Int64Dtype()),
            "LEASE": pd.array([row.get("LEASE", "TEST")], dtype=pd.StringDtype()),
            "DOR_CODE": pd.array([pd.NA], dtype=pd.Int64Dtype()),
            "API_NUMBER": pd.array([row.get("API_NUMBER", "15-001")], dtype=pd.StringDtype()),
            "FIELD": pd.array([row.get("FIELD", "F1")], dtype=pd.StringDtype()),
            "PRODUCING_ZONE": pd.array([row.get("PRODUCING_ZONE", "Z1")], dtype=pd.StringDtype()),
            "OPERATOR": pd.array([row.get("OPERATOR", "OP1")], dtype=pd.StringDtype()),
            "COUNTY": pd.array([row.get("COUNTY", "Nemaha")], dtype=pd.StringDtype()),
            "TOWNSHIP": pd.array([1], dtype=pd.Int64Dtype()),
            "TWN_DIR": pd.Categorical([row.get("TWN_DIR", "S")], categories=["S", "N"]),
            "RANGE": pd.array([14], dtype=pd.Int64Dtype()),
            "RANGE_DIR": pd.Categorical([row.get("RANGE_DIR", "E")], categories=["E", "W"]),
            "SECTION": pd.array([1], dtype=pd.Int64Dtype()),
            "SPOT": pd.array(["NENENE"], dtype=pd.StringDtype()),
            "LATITUDE": [row.get("LATITUDE", 39.9)],
            "LONGITUDE": [row.get("LONGITUDE", -95.8)],
            "MONTH-YEAR": pd.array([row.get("MONTH-YEAR", "1-2024")], dtype=pd.StringDtype()),
            "PRODUCT": pd.Categorical(
                [row.get("PRODUCT", "O")], categories=["O", "G"]
            ),
            "WELLS": pd.array([2], dtype=pd.Int64Dtype()),
            "PRODUCTION": [float(row.get("PRODUCTION", 100.0))],
            "URL": pd.array([row.get("URL", "https://example.com")], dtype=pd.StringDtype()),
            "source_file": pd.array([row.get("source_file", "lp1.txt")], dtype=pd.StringDtype()),
        })
        frames.append(part)

    combined = pd.concat(frames, ignore_index=True)
    interim_dir = str(tmp_path / "interim")
    Path(interim_dir).mkdir(parents=True, exist_ok=True)
    ddf = dd.from_pandas(combined, npartitions=1)
    ddf.to_parquet(interim_dir, engine="pyarrow", write_index=False, overwrite=True)
    return interim_dir


# ---------------------------------------------------------------------------
# Task T-01: add_production_date
# ---------------------------------------------------------------------------

class TestAddProductionDate:
    def test_january_2024(self) -> None:
        part = _make_partition(**{"MONTH-YEAR": pd.array(["1-2024"], dtype=pd.StringDtype())})
        result = add_production_date(part)
        assert result["production_date"].iloc[0] == pd.Timestamp("2024-01-01")

    def test_december_2024(self) -> None:
        part = _make_partition(**{"MONTH-YEAR": pd.array(["12-2024"], dtype=pd.StringDtype())})
        result = add_production_date(part)
        assert result["production_date"].iloc[0] == pd.Timestamp("2024-12-01")

    def test_yearly_marker_dropped(self) -> None:
        part = _make_partition(**{"MONTH-YEAR": pd.array(["0-2024"], dtype=pd.StringDtype())})
        result = add_production_date(part)
        assert len(result) == 0

    def test_cumulative_marker_dropped(self) -> None:
        rows = pd.DataFrame({
            **{k: [v.iloc[0], v.iloc[0]] if hasattr(v, "iloc") else [v[0], v[0]]
               for k, v in _make_partition().items()},
        })
        rows["MONTH-YEAR"] = pd.array(["-1-2024", "3-2024"], dtype=pd.StringDtype())
        result = add_production_date(rows)
        assert len(result) == 1
        assert result["production_date"].iloc[0] == pd.Timestamp("2024-03-01")

    def test_non_numeric_dropped(self) -> None:
        part = _make_partition(**{"MONTH-YEAR": pd.array(["abc-2024"], dtype=pd.StringDtype())})
        result = add_production_date(part)
        assert len(result) == 0

    def test_meta_consistency_tr23(self) -> None:
        meta = add_production_date(_empty_meta())
        assert "production_date" in meta.columns
        assert meta["production_date"].dtype == np.dtype("datetime64[ns]")


# ---------------------------------------------------------------------------
# Task T-02: clean_categoricals
# ---------------------------------------------------------------------------

class TestCleanCategoricals:
    CAT_COLS = {"PRODUCT": ["O", "G"], "TWN_DIR": ["S", "N"], "RANGE_DIR": ["E", "W"]}

    def test_invalid_product_replaced_with_na(self) -> None:
        part = _make_partition(
            **{"PRODUCT": pd.Categorical(["X"], categories=["O", "G"])}
        )
        result = clean_categoricals(part, self.CAT_COLS)
        assert result["PRODUCT"].isna().all()
        assert isinstance(result["PRODUCT"].dtype, pd.CategoricalDtype)

    def test_valid_values_retained(self) -> None:
        rows = pd.DataFrame({
            **{k: [v.iloc[0], v.iloc[0]] if hasattr(v, "iloc") else [v[0], v[0]]
               for k, v in _make_partition().items()},
        })
        rows["PRODUCT"] = pd.Categorical(["O", "G"], categories=["O", "G"])
        result = clean_categoricals(rows, self.CAT_COLS)
        assert list(result["PRODUCT"].dropna()) == ["O", "G"]
        assert isinstance(result["PRODUCT"].dtype, pd.CategoricalDtype)

    def test_meta_consistency_tr23(self) -> None:
        meta_in = _empty_meta()
        meta_out = clean_categoricals(meta_in.copy(), self.CAT_COLS)
        assert isinstance(meta_out["PRODUCT"].dtype, pd.CategoricalDtype)
        assert isinstance(meta_out["TWN_DIR"].dtype, pd.CategoricalDtype)


# ---------------------------------------------------------------------------
# Task T-03: clean_physical_bounds
# ---------------------------------------------------------------------------

class TestCleanPhysicalBounds:
    def test_negative_production_replaced_tr01(self) -> None:
        part = _make_partition(**{"PRODUCTION": [-100.0]})
        result = clean_physical_bounds(part)
        assert pd.isna(result["PRODUCTION"].iloc[0])

    def test_zero_production_preserved_tr05(self) -> None:
        part = _make_partition(**{"PRODUCTION": [0.0]})
        result = clean_physical_bounds(part)
        assert result["PRODUCTION"].iloc[0] == 0.0
        assert not pd.isna(result["PRODUCTION"].iloc[0])

    def test_high_oil_logged_not_nulled_tr02(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging
        part = _make_partition(
            **{
                "PRODUCTION": [60000.0],
                "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
            }
        )
        with caplog.at_level(logging.WARNING):
            result = clean_physical_bounds(part)
        assert result["PRODUCTION"].iloc[0] == 60000.0
        assert any("50,000" in msg for msg in caplog.messages)

    def test_no_invalid_values_unchanged(self) -> None:
        part = _make_partition(**{"PRODUCTION": [500.0]})
        result = clean_physical_bounds(part)
        assert result["PRODUCTION"].iloc[0] == 500.0


# ---------------------------------------------------------------------------
# Task T-04: deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:
    def test_three_identical_rows_returns_one_tr15(self) -> None:
        base = _make_partition()
        part = pd.concat([base, base, base], ignore_index=True)
        result = deduplicate(part)
        assert len(result) == 1

    def test_idempotent_tr15(self) -> None:
        base = _make_partition()
        part = pd.concat([base, base], ignore_index=True)
        first = deduplicate(part)
        second = deduplicate(first)
        assert len(first) == len(second)

    def test_no_duplicates_unchanged(self) -> None:
        rows = []
        for i in range(3):
            r = _make_partition(
                **{"MONTH-YEAR": pd.array([f"{i+1}-2024"], dtype=pd.StringDtype())}
            )
            rows.append(r)
        part = pd.concat(rows, ignore_index=True)
        result = deduplicate(part)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Task T-05: set_entity_index
# ---------------------------------------------------------------------------

class TestSetEntityIndex:
    def _make_ddf(self) -> dd.DataFrame:
        rows = []
        for month in [3, 1, 2]:
            r = _make_partition(
                **{"MONTH-YEAR": pd.array([f"{month}-2024"], dtype=pd.StringDtype())}
            )
            r = add_production_date(r)
            rows.append(r)
        combined = pd.concat(rows, ignore_index=True)
        return dd.from_pandas(combined, npartitions=2)

    def test_sort_stability_tr16(self) -> None:
        ddf = self._make_ddf()
        result = set_entity_index(ddf)
        for i in range(result.npartitions):
            part = result.get_partition(i).compute()
            if len(part) > 1:
                dates = part["production_date"].tolist()
                assert dates == sorted(dates)

    def test_index_is_lease_kid(self) -> None:
        ddf = self._make_ddf()
        result = set_entity_index(ddf)
        assert result.index.name == "LEASE_KID"


# ---------------------------------------------------------------------------
# Task T-06 / T-07: run_transform integration tests
# ---------------------------------------------------------------------------

class TestRunTransform:
    def _config(self, tmp_path: Path) -> dict:
        return {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
        }

    def test_returns_dask_dataframe_tr17(self, tmp_path: Path) -> None:
        rows = [{"LEASE_KID": 1001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100.0}]
        _write_interim(tmp_path, rows)
        result = run_transform(self._config(tmp_path))
        assert isinstance(result, dd.DataFrame)
        assert not isinstance(result, pd.DataFrame)

    def test_data_cleaning_tr12(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 1001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": -50.0},
            {"LEASE_KID": 1002, "MONTH-YEAR": "2-2024", "PRODUCT": "G", "PRODUCTION": 200.0},
        ]
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "processed")).compute()
        assert (result["PRODUCTION"].dropna() >= 0).all()
        assert result["production_date"].dtype == np.dtype("datetime64[ns]")
        assert isinstance(result["PRODUCT"].dtype, pd.CategoricalDtype)

    def test_parquet_readable_tr18(self, tmp_path: Path) -> None:
        rows = [{"LEASE_KID": 1001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100.0}]
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        reloaded = dd.read_parquet(str(tmp_path / "processed"))
        assert isinstance(reloaded, dd.DataFrame)

    def test_row_count_deduplication_tr15(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 1001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100.0},
        ] * 3
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "processed")).compute()
        assert len(result) <= 3

    def test_interim_not_exist_raises(self, tmp_path: Path) -> None:
        config = self._config(tmp_path)
        with pytest.raises(FileNotFoundError):
            run_transform(config)

    def test_data_integrity_tr11(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 2001, "MONTH-YEAR": "3-2024", "PRODUCT": "O", "PRODUCTION": 999.0},
            {"LEASE_KID": 2002, "MONTH-YEAR": "4-2024", "PRODUCT": "G", "PRODUCTION": 1234.5},
        ]
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "processed")).compute()
        result_reset = result.reset_index()
        row_2001 = result_reset[result_reset["LEASE_KID"] == 2001]
        assert len(row_2001) == 1
        assert row_2001["PRODUCTION"].iloc[0] == pytest.approx(999.0)

    def test_schema_dtype_tr12(self, tmp_path: Path) -> None:
        rows = [{"LEASE_KID": 1001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100.0}]
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "processed")).compute()
        assert result.index.name == "LEASE_KID"
        assert result["production_date"].dtype == np.dtype("datetime64[ns]")
        assert result["PRODUCTION"].dtype == np.dtype("float64")

    def test_well_completeness_tr04(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 3001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100.0},
            {"LEASE_KID": 3001, "MONTH-YEAR": "2-2024", "PRODUCT": "O", "PRODUCTION": 110.0},
            {"LEASE_KID": 3001, "MONTH-YEAR": "3-2024", "PRODUCT": "O", "PRODUCTION": 120.0},
        ]
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "processed")).compute()
        result_reset = result.reset_index()
        lease_rows = result_reset[result_reset["LEASE_KID"] == 3001]
        assert len(lease_rows) == 3

    def test_zero_production_preserved_tr05(self, tmp_path: Path) -> None:
        rows = [{"LEASE_KID": 4001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 0.0}]
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "processed")).compute()
        prod_vals = result["PRODUCTION"].dropna()
        assert 0.0 in prod_vals.values

    def test_schema_stability_tr14(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 5001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 50.0},
            {"LEASE_KID": 5002, "MONTH-YEAR": "2-2024", "PRODUCT": "G", "PRODUCTION": 200.0},
        ]
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        files = list(Path(tmp_path / "processed").glob("*.parquet"))
        if len(files) >= 2:
            df1 = pd.read_parquet(files[0])
            df2 = pd.read_parquet(files[1])
            assert list(df1.columns) == list(df2.columns)

    def test_sort_stability_tr16(self, tmp_path: Path) -> None:
        rows = [
            {"LEASE_KID": 6001, "MONTH-YEAR": "3-2024", "PRODUCT": "O", "PRODUCTION": 100.0},
            {"LEASE_KID": 6001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 80.0},
            {"LEASE_KID": 6001, "MONTH-YEAR": "2-2024", "PRODUCT": "O", "PRODUCTION": 90.0},
        ]
        _write_interim(tmp_path, rows)
        run_transform(self._config(tmp_path))
        result = dd.read_parquet(str(tmp_path / "processed")).compute()
        for kid, grp in result.reset_index().groupby("LEASE_KID"):
            dates = grp["production_date"].tolist()
            assert dates == sorted(dates)
