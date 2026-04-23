"""Unit and integration tests for kgs_pipeline/ingest.py."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.ingest import (
    build_dtype_map,
    filter_date_range,
    parse_raw_file,
)
from tests.conftest import make_raw_csv_file

DICT_PATH = Path("references/kgs_monthly_data_dictionary.csv")

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Task I-01: build_dtype_map
# ---------------------------------------------------------------------------


class TestBuildDtypeMap:
    def test_contains_all_columns(self) -> None:
        dtype_map = build_dtype_map(DICT_PATH)
        expected = [
            "LEASE_KID", "LEASE", "DOR_CODE", "API_NUMBER", "FIELD",
            "PRODUCING_ZONE", "OPERATOR", "COUNTY", "TOWNSHIP", "TWN_DIR",
            "RANGE", "RANGE_DIR", "SECTION", "SPOT", "LATITUDE", "LONGITUDE",
            "MONTH-YEAR", "PRODUCT", "WELLS", "PRODUCTION",
        ]
        for col in expected:
            assert col in dtype_map, f"Missing column: {col}"

    def test_nullable_int_uses_Int64(self) -> None:
        dtype_map = build_dtype_map(DICT_PATH)
        # WELLS is nullable=yes integer
        assert dtype_map["WELLS"] == pd.Int64Dtype()

    def test_nonnullable_int_uses_int64(self) -> None:
        dtype_map = build_dtype_map(DICT_PATH)
        # LEASE_KID is nullable=no integer
        assert dtype_map["LEASE_KID"] == "int64"

    def test_nullable_string_uses_StringDtype(self) -> None:
        dtype_map = build_dtype_map(DICT_PATH)
        # LEASE is nullable=yes string
        assert dtype_map["LEASE"] == pd.StringDtype()

    def test_float_columns_use_Float64(self) -> None:
        dtype_map = build_dtype_map(DICT_PATH)
        # PRODUCTION is float nullable=yes
        assert dtype_map["PRODUCTION"] == pd.Float64Dtype()

    def test_categorical_columns_use_CategoricalDtype(self) -> None:
        dtype_map = build_dtype_map(DICT_PATH)
        assert isinstance(dtype_map["PRODUCT"], pd.CategoricalDtype)
        assert isinstance(dtype_map["TWN_DIR"], pd.CategoricalDtype)

    def test_raises_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError):
            build_dtype_map("/no/such/path.csv")


# ---------------------------------------------------------------------------
# Task I-02: parse_raw_file
# ---------------------------------------------------------------------------


class TestParseRawFile:
    def test_returns_correct_dtypes(self, tmp_path: Path) -> None:
        p = make_raw_csv_file(tmp_path / "lease1.txt")
        dtype_map = build_dtype_map(DICT_PATH)
        df = parse_raw_file(p, dtype_map)
        assert isinstance(df["PRODUCTION"].dtype, pd.Float64Dtype)
        assert isinstance(df["LEASE"].dtype, pd.StringDtype)

    def test_missing_nullable_column_added_as_na(self, tmp_path: Path) -> None:
        # Write a CSV without FIELD (nullable=yes)
        import pandas as _pd
        rows = {
            "LEASE_KID": [1001],
            "LEASE": ["Test"],
            "DOR_CODE": [99],
            "API_NUMBER": ["15-001-00001"],
            # FIELD is omitted
            "PRODUCING_ZONE": ["Zone A"],
            "OPERATOR": ["Op"],
            "COUNTY": ["Douglas"],
            "TOWNSHIP": [12],
            "TWN_DIR": ["S"],
            "RANGE": [20],
            "RANGE_DIR": ["E"],
            "SECTION": [5],
            "SPOT": ["NENE"],
            "LATITUDE": [38.9],
            "LONGITUDE": [-95.2],
            "MONTH-YEAR": ["1-2024"],
            "PRODUCT": ["O"],
            "WELLS": [2],
            "PRODUCTION": [150.0],
        }
        p = tmp_path / "no_field.txt"
        _pd.DataFrame(rows).to_csv(p, index=False)
        dtype_map = build_dtype_map(DICT_PATH)
        df = parse_raw_file(p, dtype_map)
        assert "FIELD" in df.columns
        assert df["FIELD"].isna().all()

    def test_raises_for_missing_non_nullable_column(self, tmp_path: Path) -> None:
        # Write CSV without LEASE_KID (nullable=no)
        df = pd.DataFrame({"MONTH-YEAR": ["1-2024"], "PRODUCT": ["O"]})
        p = tmp_path / "no_lk.txt"
        df.to_csv(p, index=False)
        dtype_map = build_dtype_map(DICT_PATH)
        with pytest.raises(ValueError, match="LEASE_KID"):
            parse_raw_file(p, dtype_map)

    def test_empty_file_returns_empty_dataframe(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.txt"
        p.write_text("")
        dtype_map = build_dtype_map(DICT_PATH)
        df = parse_raw_file(p, dtype_map)
        assert len(df) == 0
        assert "LEASE_KID" in df.columns

    def test_latin1_file_parsed_without_error(self, tmp_path: Path, caplog: Any) -> None:
        import logging
        rows = {
            "LEASE_KID": [1001],
            "LEASE": ["Sch\xe4fer"],  # latin-1 character
            "DOR_CODE": [99],
            "API_NUMBER": ["15-001-00001"],
            "FIELD": ["Test"],
            "PRODUCING_ZONE": ["Zone"],
            "OPERATOR": ["Op"],
            "COUNTY": ["Douglas"],
            "TOWNSHIP": [12],
            "TWN_DIR": ["S"],
            "RANGE": [20],
            "RANGE_DIR": ["E"],
            "SECTION": [5],
            "SPOT": ["NENE"],
            "LATITUDE": [38.9],
            "LONGITUDE": [-95.2],
            "MONTH-YEAR": ["1-2024"],
            "PRODUCT": ["O"],
            "WELLS": [2],
            "PRODUCTION": [150.0],
        }
        p = tmp_path / "latin1.txt"
        content = pd.DataFrame(rows).to_csv(index=False).encode("latin-1")
        p.write_bytes(content)
        dtype_map = build_dtype_map(DICT_PATH)
        with caplog.at_level(logging.WARNING):
            df = parse_raw_file(p, dtype_map)
        assert len(df) == 1
        assert any("latin-1" in r.message for r in caplog.records)

    def test_source_file_column_present(self, tmp_path: Path) -> None:
        p = make_raw_csv_file(tmp_path / "myfile.txt")
        dtype_map = build_dtype_map(DICT_PATH)
        df = parse_raw_file(p, dtype_map)
        assert "source_file" in df.columns
        assert all(df["source_file"] == "myfile")


# ---------------------------------------------------------------------------
# Task I-03: filter_date_range
# ---------------------------------------------------------------------------


class TestFilterDateRange:
    def test_keeps_2024_and_2025(self) -> None:
        df = pd.DataFrame({
            "MONTH-YEAR": ["1-2022", "6-2023", "3-2024", "11-2025"],
        })
        result = filter_date_range(df)
        assert set(result["MONTH-YEAR"]) == {"3-2024", "11-2025"}

    def test_drops_malformed_without_exception(self) -> None:
        df = pd.DataFrame({
            "MONTH-YEAR": ["-1-1965", "0-1966", "5-2024"],
        })
        result = filter_date_range(df)
        assert len(result) == 1

    def test_min_year_param(self) -> None:
        df = pd.DataFrame({
            "MONTH-YEAR": ["1-2024", "1-2025", "1-2023"],
        })
        result = filter_date_range(df, min_year=2025)
        assert set(result["MONTH-YEAR"]) == {"1-2025"}

    def test_all_before_min_year_returns_empty(self) -> None:
        df = pd.DataFrame({
            "MONTH-YEAR": ["1-2022", "2-2023"],
            "X": [1, 2],
        })
        result = filter_date_range(df)
        assert len(result) == 0
        assert list(result.columns) == list(df.columns)

    def test_raises_keyerror_on_missing_column(self) -> None:
        df = pd.DataFrame({"OTHER": ["1-2024"]})
        with pytest.raises(KeyError):
            filter_date_range(df)


# ---------------------------------------------------------------------------
# Task I-04: ingest (integration)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIngest:
    def test_returns_dask_dataframe(self, tmp_path: Path) -> None:
        from kgs_pipeline.ingest import ingest

        raw = tmp_path / "raw"
        raw.mkdir()
        make_raw_csv_file(raw / "lease1.txt")
        make_raw_csv_file(raw / "lease2.txt", lease_kids=[2001, 2002])
        interim = tmp_path / "interim"

        cfg = _make_ingest_config(str(raw), str(interim))
        result = ingest(cfg)
        assert isinstance(result, dd.DataFrame)

    def test_raises_when_no_txt_files(self, tmp_path: Path) -> None:
        from kgs_pipeline.ingest import ingest

        raw = tmp_path / "raw_empty"
        raw.mkdir()
        interim = tmp_path / "interim"
        cfg = _make_ingest_config(str(raw), str(interim))
        with pytest.raises(FileNotFoundError):
            ingest(cfg)

    def test_parquet_is_readable_after_ingest(self, tmp_path: Path) -> None:
        from kgs_pipeline.ingest import ingest

        raw = tmp_path / "raw"
        raw.mkdir()
        make_raw_csv_file(raw / "lease1.txt")
        interim = tmp_path / "interim"

        cfg = _make_ingest_config(str(raw), str(interim))
        ingest(cfg)

        read_back = dd.read_parquet(str(interim))
        assert isinstance(read_back, dd.DataFrame)

    def test_schema_completeness(self, tmp_path: Path) -> None:
        from kgs_pipeline.ingest import ingest

        raw = tmp_path / "raw"
        raw.mkdir()
        make_raw_csv_file(raw / "lease1.txt")
        make_raw_csv_file(raw / "lease2.txt", lease_kids=[2001])
        make_raw_csv_file(raw / "lease3.txt", lease_kids=[3001])
        interim = tmp_path / "interim"

        cfg = _make_ingest_config(str(raw), str(interim))
        ingest(cfg)

        read_back = dd.read_parquet(str(interim)).compute()
        required_cols = [
            "LEASE_KID", "LEASE", "API_NUMBER", "MONTH-YEAR", "PRODUCT",
            "PRODUCTION", "WELLS", "OPERATOR", "COUNTY", "source_file",
        ]
        for col in required_cols:
            assert col in read_back.columns, f"Missing column: {col}"

    def test_malformed_file_skipped_valid_processed(self, tmp_path: Path) -> None:
        from kgs_pipeline.ingest import ingest

        raw = tmp_path / "raw"
        raw.mkdir()
        make_raw_csv_file(raw / "good.txt")
        # Malformed file — missing required LEASE_KID column
        bad = raw / "bad.txt"
        bad.write_text("MONTH-YEAR,PRODUCT\n1-2024,O\n")
        interim = tmp_path / "interim"

        cfg = _make_ingest_config(str(raw), str(interim))
        # Should not raise — bad file is logged and skipped
        result = ingest(cfg)
        assert isinstance(result, dd.DataFrame)


def _make_ingest_config(raw_dir: str, interim_path: str) -> dict[str, Any]:
    return {
        "ingest": {
            "raw_dir": raw_dir,
            "interim_path": interim_path,
            "min_year": 2024,
            "dict_path": str(Path("references/kgs_monthly_data_dictionary.csv")),
        }
    }
