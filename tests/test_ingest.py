"""Tests for kgs_pipeline/ingest.py."""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.ingest import (
    EXPECTED_COLUMNS,
    enforce_schema,
    filter_by_year,
    ingest_file,
    load_schema,
    read_raw_file,
    validate_interim_schema,
    write_interim,
)

_DICT_PATH = "references/kgs_monthly_data_dictionary.csv"


# ---------------------------------------------------------------------------
# ING-01: load_schema
# ---------------------------------------------------------------------------


def test_load_schema_returns_21_keys() -> None:
    schema = load_schema(_DICT_PATH)
    assert len(schema) == 20


def test_load_schema_lease_kid_non_nullable_int64() -> None:
    schema = load_schema(_DICT_PATH)
    assert schema["LEASE_KID"]["nullable"] is False
    assert schema["LEASE_KID"]["dtype"] == "int64"


def test_load_schema_wells_nullable_int64() -> None:
    schema = load_schema(_DICT_PATH)
    assert schema["WELLS"]["nullable"] is True
    assert schema["WELLS"]["dtype"] == "Int64"


def test_load_schema_product_categories() -> None:
    schema = load_schema(_DICT_PATH)
    assert schema["PRODUCT"]["categories"] == ["O", "G"]


def test_load_schema_township_nullable_int64() -> None:
    schema = load_schema(_DICT_PATH)
    assert schema["TOWNSHIP"]["nullable"] is True
    assert schema["TOWNSHIP"]["dtype"] == "Int64"


def test_load_schema_file_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        load_schema("/nonexistent/data_dict.csv")


# ---------------------------------------------------------------------------
# ING-02: read_raw_file
# ---------------------------------------------------------------------------


def _write_kgs_file(tmp_path: Path, rows: list[dict]) -> Path:
    df = pd.DataFrame(rows)
    p = tmp_path / "test_lease.txt"
    df.to_csv(p, index=False, quoting=1)
    return p


def test_read_raw_file_adds_source_file(tmp_path: Path) -> None:
    rows = [
        {"LEASE_KID": "1", "PRODUCT": "O", "MONTH-YEAR": "1-2024", "PRODUCTION": "100.0"},
    ]
    p = _write_kgs_file(tmp_path, rows)
    df = read_raw_file(p)
    assert "source_file" in df.columns
    assert df["source_file"].iloc[0] == p.name


def test_read_raw_file_empty_bytes(tmp_path: Path) -> None:
    p = tmp_path / "empty.txt"
    p.write_bytes(b"")
    df = read_raw_file(p)
    assert df.empty


def test_read_raw_file_header_only(tmp_path: Path) -> None:
    p = tmp_path / "header.txt"
    p.write_text("LEASE_KID,PRODUCT\n", encoding="utf-8")
    df = read_raw_file(p)
    assert df.empty


def test_read_raw_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_raw_file(tmp_path / "missing.txt")


# ---------------------------------------------------------------------------
# ING-03: filter_by_year
# ---------------------------------------------------------------------------


def test_filter_by_year_keeps_2024_onwards() -> None:
    df = pd.DataFrame(
        {
            "MONTH-YEAR": ["1-2024", "12-2025", "3-2022", "-1-1965", "0-1966"],
            "val": [1, 2, 3, 4, 5],
        }
    )
    result = filter_by_year(df, min_year=2024)
    assert set(result["MONTH-YEAR"]) == {"1-2024", "12-2025"}


def test_filter_by_year_all_before_2024() -> None:
    df = pd.DataFrame({"MONTH-YEAR": ["1-2020", "6-2022"], "val": [1, 2]})
    result = filter_by_year(df, min_year=2024)
    assert result.empty


def test_filter_by_year_no_non_numeric_tokens() -> None:
    df = pd.DataFrame({"MONTH-YEAR": ["1-2024", "2-2025"], "val": [1, 2]})
    result = filter_by_year(df, min_year=2024)
    assert len(result) == 2


# ---------------------------------------------------------------------------
# ING-04: enforce_schema
# ---------------------------------------------------------------------------


def test_enforce_schema_adds_nullable_column() -> None:
    schema = load_schema(_DICT_PATH)
    # LEASE is nullable string
    df = pd.DataFrame({"LEASE_KID": ["1"], "PRODUCT": ["O"], "MONTH-YEAR": ["1-2024"]})
    result = enforce_schema(df, schema)
    assert "LEASE" in result.columns
    assert result["LEASE"].isna().all()


def test_enforce_schema_raises_for_non_nullable_absent() -> None:
    schema = load_schema(_DICT_PATH)
    df = pd.DataFrame({"PRODUCT": ["O"], "MONTH-YEAR": ["1-2024"]})
    with pytest.raises(ValueError, match="LEASE_KID"):
        enforce_schema(df, schema)


def test_enforce_schema_product_out_of_set_replaced() -> None:
    schema = load_schema(_DICT_PATH)
    df = pd.DataFrame(
        {
            "LEASE_KID": ["1"],
            "PRODUCT": ["X"],
            "MONTH-YEAR": ["1-2024"],
        }
    )
    result = enforce_schema(df, schema)
    assert result["PRODUCT"].isna().all()


def test_enforce_schema_drops_extra_column() -> None:
    schema = load_schema(_DICT_PATH)
    df = pd.DataFrame(
        {
            "LEASE_KID": ["1"],
            "PRODUCT": ["O"],
            "MONTH-YEAR": ["1-2024"],
            "URL": ["http://extra.com"],
        }
    )
    result = enforce_schema(df, schema)
    assert "URL" not in result.columns


def test_enforce_schema_twn_dir_categorical() -> None:
    schema = load_schema(_DICT_PATH)
    df = pd.DataFrame(
        {
            "LEASE_KID": ["1"],
            "PRODUCT": ["O"],
            "MONTH-YEAR": ["1-2024"],
            "TWN_DIR": ["S"],
        }
    )
    result = enforce_schema(df, schema)
    assert hasattr(result["TWN_DIR"], "cat")
    assert set(result["TWN_DIR"].cat.categories) == {"S", "N"}


# ---------------------------------------------------------------------------
# ING-05: ingest_file
# ---------------------------------------------------------------------------


def _make_full_raw_file(tmp_path: Path) -> Path:
    rows = [
        {
            "LEASE_KID": "1001",
            "LEASE": "TEST",
            "DOR_CODE": "5001",
            "API_NUMBER": "15-001-00001",
            "FIELD": "KANASKA",
            "PRODUCING_ZONE": "Lansing",
            "OPERATOR": "TestCo",
            "COUNTY": "Ellis",
            "TOWNSHIP": "1",
            "TWN_DIR": "S",
            "RANGE": "14",
            "RANGE_DIR": "E",
            "SECTION": "1",
            "SPOT": "NE",
            "LATITUDE": "38.5",
            "LONGITUDE": "-99.0",
            "MONTH-YEAR": my,
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "100.0",
        }
        for my in ["1-2024", "6-2025", "3-2022", "12-2020"]
    ]
    df = pd.DataFrame(rows)
    p = tmp_path / "lease_file.txt"
    df.to_csv(p, index=False, quoting=1)
    return p


def test_ingest_file_year_filter(tmp_path: Path) -> None:
    p = _make_full_raw_file(tmp_path)
    schema = load_schema(_DICT_PATH)
    result = ingest_file(p, schema, min_year=2024)
    assert not result.empty
    # Only 2024+ rows
    assert all(int(my.split("-")[-1]) >= 2024 for my in result.get("MONTH-YEAR", pd.Series()))


def test_ingest_file_all_before_min_year(tmp_path: Path) -> None:
    rows = [{"LEASE_KID": "1", "PRODUCT": "O", "MONTH-YEAR": "1-2000", "PRODUCTION": "50.0"}]
    df = pd.DataFrame(rows)
    p = tmp_path / "old.txt"
    df.to_csv(p, index=False, quoting=1)
    schema = load_schema(_DICT_PATH)
    result = ingest_file(p, schema, min_year=2024)
    assert result.empty


def test_ingest_file_missing_non_nullable_raises(tmp_path: Path) -> None:
    # Missing LEASE_KID
    rows = [{"PRODUCT": "O", "MONTH-YEAR": "1-2024"}]
    df = pd.DataFrame(rows)
    p = tmp_path / "bad.txt"
    df.to_csv(p, index=False, quoting=1)
    schema = load_schema(_DICT_PATH)
    with pytest.raises(ValueError, match="LEASE_KID"):
        ingest_file(p, schema, min_year=2024)


# ---------------------------------------------------------------------------
# ING-07 + ING-08: write_interim and validate_interim_schema
# ---------------------------------------------------------------------------


def test_write_interim_creates_parquet(tmp_path: Path) -> None:
    rows = {
        "LEASE_KID": pd.array([1, 2, 3], dtype="int64"),
        "PRODUCT": pd.Categorical(["O", "G", "O"], categories=["G", "O"]),
        "MONTH-YEAR": pd.array(["1-2024", "2-2024", "3-2024"], dtype="string"),
    }
    df = pd.DataFrame(rows)
    ddf = dd.from_pandas(df, npartitions=1)

    config = {"ingest": {"interim_dir": str(tmp_path / "interim")}}
    write_interim(ddf, config)

    parquet_files = list((tmp_path / "interim").glob("*.parquet"))
    assert len(parquet_files) >= 1


def test_validate_interim_schema_passes(tmp_path: Path) -> None:
    interim = tmp_path / "interim"
    interim.mkdir()
    for i in range(3):
        df = pd.DataFrame({col: ["val"] for col in EXPECTED_COLUMNS})
        df.to_parquet(interim / f"part_{i}.parquet")
    validate_interim_schema(interim, EXPECTED_COLUMNS)  # Should not raise


def test_validate_interim_schema_missing_column(tmp_path: Path) -> None:
    interim = tmp_path / "interim"
    interim.mkdir()
    cols_missing_operator = [c for c in EXPECTED_COLUMNS if c != "OPERATOR"]
    df = pd.DataFrame({col: ["x"] for col in cols_missing_operator})
    df.to_parquet(interim / "part_0.parquet")
    with pytest.raises(ValueError, match="OPERATOR"):
        validate_interim_schema(interim, EXPECTED_COLUMNS)


def test_validate_interim_schema_missing_source_file(tmp_path: Path) -> None:
    interim = tmp_path / "interim"
    interim.mkdir()
    cols = [c for c in EXPECTED_COLUMNS if c != "source_file"]
    df = pd.DataFrame({col: ["x"] for col in cols})
    df.to_parquet(interim / "part_0.parquet")
    with pytest.raises(ValueError, match="source_file"):
        validate_interim_schema(interim, EXPECTED_COLUMNS)
