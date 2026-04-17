"""Tests for kgs_pipeline/ingest.py."""

from __future__ import annotations

import os
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.ingest import (
    SchemaError,
    load_data_dictionary,
    read_raw_file,
    resolve_pandas_dtype,
    run_ingest,
)

DICT_PATH = "references/kgs_monthly_data_dictionary.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CANONICAL_COLUMNS = [
    "LEASE_KID", "LEASE", "DOR_CODE", "API_NUMBER", "FIELD", "PRODUCING_ZONE",
    "OPERATOR", "COUNTY", "TOWNSHIP", "TWN_DIR", "RANGE", "RANGE_DIR", "SECTION",
    "SPOT", "LATITUDE", "LONGITUDE", "MONTH-YEAR", "PRODUCT", "WELLS",
    "PRODUCTION", "URL",
]

_MINIMAL_ROW = {
    "LEASE_KID": "1001135839",
    "LEASE": "SNYDER",
    "DOR_CODE": "122608",
    "API_NUMBER": "15-131-20143",
    "FIELD": "KANASKA",
    "PRODUCING_ZONE": "Lansing Group",
    "OPERATOR": "Buckeye West, LLC",
    "COUNTY": "Nemaha",
    "TOWNSHIP": "1",
    "TWN_DIR": "S",
    "RANGE": "14",
    "RANGE_DIR": "E",
    "SECTION": "1",
    "SPOT": "NENENE",
    "LATITUDE": "39.999519",
    "LONGITUDE": "-95.78905",
    "MONTH-YEAR": "1-2024",
    "PRODUCT": "O",
    "WELLS": "2",
    "PRODUCTION": "161.8",
    "URL": "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839",
}


def _write_csv(tmp_path: Path, rows: list[dict], name: str = "test.txt") -> str:
    df = pd.DataFrame(rows)
    fpath = tmp_path / name
    df.to_csv(fpath, index=False)
    return str(fpath)


# ---------------------------------------------------------------------------
# Task I-01: load_data_dictionary
# ---------------------------------------------------------------------------

class TestLoadDataDictionary:
    def test_lease_kid_spec(self) -> None:
        dd_dict = load_data_dictionary(DICT_PATH)
        assert dd_dict["LEASE_KID"]["dtype"] == "int"
        assert dd_dict["LEASE_KID"]["nullable"] is False
        assert dd_dict["LEASE_KID"]["categories"] == []

    def test_product_spec(self) -> None:
        dd_dict = load_data_dictionary(DICT_PATH)
        assert dd_dict["PRODUCT"]["dtype"] == "categorical"
        assert dd_dict["PRODUCT"]["nullable"] is False
        assert dd_dict["PRODUCT"]["categories"] == ["O", "G"]

    def test_twn_dir_categories(self) -> None:
        dd_dict = load_data_dictionary(DICT_PATH)
        assert dd_dict["TWN_DIR"]["categories"] == ["S", "N"]

    def test_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_data_dictionary("/nonexistent/dict.csv")


# ---------------------------------------------------------------------------
# Task I-02: resolve_pandas_dtype
# ---------------------------------------------------------------------------

class TestResolvePandasDtype:
    def test_int_not_nullable(self) -> None:
        result = resolve_pandas_dtype("int", False, [])
        assert result == np.dtype("int64")

    def test_int_nullable(self) -> None:
        result = resolve_pandas_dtype("int", True, [])
        assert result == pd.Int64Dtype()

    def test_float(self) -> None:
        result = resolve_pandas_dtype("float", True, [])
        assert result == np.dtype("float64")

    def test_string(self) -> None:
        result = resolve_pandas_dtype("string", True, [])
        assert isinstance(result, pd.StringDtype)

    def test_categorical(self) -> None:
        result = resolve_pandas_dtype("categorical", True, ["O", "G"])
        expected = pd.CategoricalDtype(categories=["O", "G"], ordered=False)
        assert result == expected

    def test_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown"):
            resolve_pandas_dtype("unknown", True, [])


# ---------------------------------------------------------------------------
# Task I-03: read_raw_file
# ---------------------------------------------------------------------------

class TestReadRawFile:
    def _data_dict(self) -> dict[str, dict]:
        return load_data_dictionary(DICT_PATH)

    def test_correct_dtypes_and_source_file(self, tmp_path: Path) -> None:
        fpath = _write_csv(tmp_path, [_MINIMAL_ROW])
        data_dict = self._data_dict()
        df = read_raw_file(fpath, data_dict)
        assert not df.empty
        assert df["LEASE_KID"].dtype == pd.Int64Dtype()
        assert df["PRODUCTION"].dtype == np.dtype("float64")
        assert df["PRODUCT"].dtype == pd.CategoricalDtype(["O", "G"])
        assert df["source_file"].dtype == pd.StringDtype()
        assert df["source_file"].iloc[0] == os.path.basename(fpath)

    def test_year_filter_removes_old_rows(self, tmp_path: Path) -> None:
        old_row = {**_MINIMAL_ROW, "MONTH-YEAR": "6-2023"}
        fpath = _write_csv(tmp_path, [old_row])
        data_dict = self._data_dict()
        df = read_raw_file(fpath, data_dict)
        assert df.empty

    def test_missing_non_nullable_raises(self, tmp_path: Path) -> None:
        row = {k: v for k, v in _MINIMAL_ROW.items() if k != "LEASE_KID"}
        fpath = _write_csv(tmp_path, [row])
        data_dict = self._data_dict()
        with pytest.raises(SchemaError):
            read_raw_file(fpath, data_dict)

    def test_missing_nullable_column_added_as_na(self, tmp_path: Path) -> None:
        row = {k: v for k, v in _MINIMAL_ROW.items() if k != "FIELD"}
        fpath = _write_csv(tmp_path, [row])
        data_dict = self._data_dict()
        df = read_raw_file(fpath, data_dict)
        assert "FIELD" in df.columns
        assert df["FIELD"].isna().all()

    def test_non_numeric_month_year_dropped(self, tmp_path: Path) -> None:
        row_invalid = {**_MINIMAL_ROW, "MONTH-YEAR": "-1-1965"}
        row_valid = {**_MINIMAL_ROW, "MONTH-YEAR": "3-2024"}
        fpath = _write_csv(tmp_path, [row_invalid, row_valid])
        data_dict = self._data_dict()
        df = read_raw_file(fpath, data_dict)
        assert len(df) == 1
        assert df["MONTH-YEAR"].iloc[0] == "3-2024"

    def test_latin1_fallback(self, tmp_path: Path) -> None:
        """File with latin-1 encoding (non-UTF-8 bytes) should be read successfully."""
        df_in = pd.DataFrame([_MINIMAL_ROW])
        fpath = tmp_path / "latin.txt"
        df_in.to_csv(fpath, index=False, encoding="latin-1")
        data_dict = self._data_dict()
        df = read_raw_file(str(fpath), data_dict)
        assert not df.empty


# ---------------------------------------------------------------------------
# Task I-04 / I-05: run_ingest
# ---------------------------------------------------------------------------

class TestRunIngest:
    def _config(self, tmp_path: Path) -> dict:
        return {
            "raw_dir": str(tmp_path / "raw"),
            "interim_dir": str(tmp_path / "interim"),
            "data_dictionary": DICT_PATH,
        }

    def _write_raw_files(self, tmp_path: Path, n: int = 3) -> None:
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        for i in range(n):
            row = {**_MINIMAL_ROW, "LEASE_KID": str(1001 + i), "MONTH-YEAR": "1-2024"}
            df = pd.DataFrame([row])
            df.to_csv(raw_dir / f"lp{i}.txt", index=False)

    def test_returns_dask_dataframe_tr17(self, tmp_path: Path) -> None:
        self._write_raw_files(tmp_path, n=2)
        config = self._config(tmp_path)
        result = run_ingest(config)
        assert isinstance(result, dd.DataFrame)
        assert not isinstance(result, pd.DataFrame)

    def test_parquet_readable_tr18(self, tmp_path: Path) -> None:
        self._write_raw_files(tmp_path, n=3)
        config = self._config(tmp_path)
        run_ingest(config)
        interim_dir = tmp_path / "interim"
        reloaded = dd.read_parquet(str(interim_dir))
        assert isinstance(reloaded, dd.DataFrame)
        assert len(reloaded.compute()) > 0

    def test_schema_completeness_tr22(self, tmp_path: Path) -> None:
        self._write_raw_files(tmp_path, n=3)
        config = self._config(tmp_path)
        run_ingest(config)
        interim_dir = tmp_path / "interim"
        reloaded = dd.read_parquet(str(interim_dir)).compute()
        expected = [
            "LEASE_KID", "LEASE", "API_NUMBER", "MONTH-YEAR", "PRODUCT",
            "PRODUCTION", "WELLS", "OPERATOR", "COUNTY", "source_file",
        ]
        for col in expected:
            assert col in reloaded.columns, f"Missing column: {col}"

    def test_raw_dir_not_exist_raises(self, tmp_path: Path) -> None:
        config = self._config(tmp_path)
        with pytest.raises(FileNotFoundError):
            run_ingest(config)

    def test_no_txt_files_returns_empty_no_raise(self, tmp_path: Path) -> None:
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        config = self._config(tmp_path)
        result = run_ingest(config)
        assert isinstance(result, dd.DataFrame)

    def test_year_filter_in_output(self, tmp_path: Path) -> None:
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        rows_mixed = [
            {**_MINIMAL_ROW, "LEASE_KID": "1001", "MONTH-YEAR": "6-2022"},
            {**_MINIMAL_ROW, "LEASE_KID": "1001", "MONTH-YEAR": "3-2024"},
        ]
        df = pd.DataFrame(rows_mixed)
        df.to_csv(raw_dir / "lp1.txt", index=False)
        config = self._config(tmp_path)
        run_ingest(config)
        result = dd.read_parquet(str(tmp_path / "interim")).compute()
        years = result["MONTH-YEAR"].apply(lambda x: int(str(x).split("-")[-1]))
        assert (years >= 2024).all()

    def test_dtype_validation(self, tmp_path: Path) -> None:
        self._write_raw_files(tmp_path, n=2)
        config = self._config(tmp_path)
        run_ingest(config)
        result = dd.read_parquet(str(tmp_path / "interim")).compute()
        assert result["LEASE_KID"].dtype == pd.Int64Dtype()
        assert result["PRODUCTION"].dtype == np.dtype("float64")
        assert hasattr(result["PRODUCT"].dtype, "categories")
