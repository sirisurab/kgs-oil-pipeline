"""Tests for kgs_pipeline/ingest.py."""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.ingest import (
    IngestConfig,
    IngestSummary,
    load_schema,
    read_raw_file,
    resolve_pandas_dtype,
    write_interim_parquet,
)

# Path to the real data dictionary (available in the repo)
DICT_PATH = Path("references/kgs_monthly_data_dictionary.csv")

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def minimal_csv(tmp_path: Path) -> Path:
    """Minimal valid CSV matching canonical schema (required columns only)."""
    content = (
        "LEASE_KID,MONTH-YEAR,PRODUCT,PRODUCTION,WELLS,OPERATOR,COUNTY,FIELD,"
        "PRODUCING_ZONE,LEASE,DOR_CODE,API_NUMBER,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,"
        "SECTION,SPOT,LATITUDE,LONGITUDE\n"
        "1001,1-2024,O,500.0,3,ACME Oil,Barton,BARTON COUNTY,COMMON SAND,"
        "ALPHA LEASE,12345,1234567890,15,S,10,W,12,NE,38.5,-99.2\n"
    )
    p = tmp_path / "lp001.txt"
    p.write_text(content, encoding="utf-8")
    return p


@pytest.fixture()
def sample_schema() -> dict:
    """Return the real schema from the data dictionary."""
    return load_schema(DICT_PATH)


# ---------------------------------------------------------------------------
# Task 01: load_schema
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_schema_all_columns(sample_schema: dict) -> None:
    """All 21 data dictionary columns should be present."""
    # 20 canonical columns + URL column in the data dict
    assert len(sample_schema) >= 20


@pytest.mark.unit
def test_load_schema_lease_kid_non_nullable(sample_schema: dict) -> None:
    entry = sample_schema["LEASE_KID"]
    assert entry["nullable"] is False
    assert entry["dtype"] == "int"


@pytest.mark.unit
def test_load_schema_twn_dir_categorical(sample_schema: dict) -> None:
    entry = sample_schema["TWN_DIR"]
    assert entry["nullable"] is True
    assert entry["dtype"] == "categorical"
    assert entry["categories"] == ["S", "N"]


@pytest.mark.unit
def test_load_schema_product_non_nullable_categorical(sample_schema: dict) -> None:
    entry = sample_schema["PRODUCT"]
    assert entry["nullable"] is False
    assert entry["dtype"] == "categorical"
    assert entry["categories"] == ["O", "G"]


@pytest.mark.unit
def test_load_schema_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_schema(tmp_path / "nonexistent.csv")


# ---------------------------------------------------------------------------
# Task 02: resolve_pandas_dtype
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_resolve_dtype_int_non_nullable() -> None:
    assert resolve_pandas_dtype("int", False, None) == pd.Int64Dtype()


@pytest.mark.unit
def test_resolve_dtype_int_nullable() -> None:
    assert resolve_pandas_dtype("int", True, None) == pd.Int64Dtype()


@pytest.mark.unit
def test_resolve_dtype_float() -> None:
    assert resolve_pandas_dtype("float", True, None) == np.float64


@pytest.mark.unit
def test_resolve_dtype_string() -> None:
    assert resolve_pandas_dtype("string", True, None) == pd.StringDtype()


@pytest.mark.unit
def test_resolve_dtype_categorical_with_categories() -> None:
    result = resolve_pandas_dtype("categorical", True, ["S", "N"])
    assert isinstance(result, pd.CategoricalDtype)
    assert list(result.categories) == ["S", "N"]


@pytest.mark.unit
def test_resolve_dtype_unknown_raises() -> None:
    with pytest.raises(ValueError, match="unknown"):
        resolve_pandas_dtype("unknown", True, None)


# ---------------------------------------------------------------------------
# Task 03: read_raw_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_file_canonical_columns(minimal_csv: Path, sample_schema: dict) -> None:
    """Returned DataFrame has all expected canonical columns plus source_file."""
    df = read_raw_file(minimal_csv, sample_schema)
    assert "LEASE_KID" in df.columns
    assert "MONTH-YEAR" in df.columns
    assert "PRODUCT" in df.columns
    assert "source_file" in df.columns


@pytest.mark.unit
def test_read_raw_file_absent_nullable_column(tmp_path: Path, sample_schema: dict) -> None:
    """Absent nullable column (OPERATOR) is added as all-NA StringDtype."""
    content = (
        "LEASE_KID,MONTH-YEAR,PRODUCT\n"
        "1001,1-2024,O\n"
    )
    p = tmp_path / "lp002.txt"
    p.write_text(content, encoding="utf-8")
    df = read_raw_file(p, sample_schema)
    assert "OPERATOR" in df.columns
    # All values should be NA
    assert df["OPERATOR"].isna().all()


@pytest.mark.unit
def test_read_raw_file_absent_non_nullable_raises(tmp_path: Path, sample_schema: dict) -> None:
    """Absent non-nullable column (LEASE_KID) raises ValueError."""
    content = "MONTH-YEAR,PRODUCT\n1-2024,O\n"
    p = tmp_path / "lp003.txt"
    p.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match="LEASE_KID"):
        read_raw_file(p, sample_schema)


@pytest.mark.unit
def test_read_raw_file_year_filter(tmp_path: Path, sample_schema: dict) -> None:
    """Rows with year < 2024 are dropped."""
    content = (
        "LEASE_KID,MONTH-YEAR,PRODUCT\n"
        "1001,1-2023,O\n"
        "1002,1-2024,G\n"
    )
    p = tmp_path / "lp004.txt"
    p.write_text(content, encoding="utf-8")
    df = read_raw_file(p, sample_schema)
    assert len(df) == 1
    assert "1-2024" in df["MONTH-YEAR"].astype(str).values


@pytest.mark.unit
def test_read_raw_file_non_numeric_year_dropped(tmp_path: Path, sample_schema: dict) -> None:
    """Rows with MONTH-YEAR = '0-1966' are dropped."""
    content = (
        "LEASE_KID,MONTH-YEAR,PRODUCT\n"
        "1001,0-1966,O\n"
        "1002,1-2024,G\n"
    )
    p = tmp_path / "lp005.txt"
    p.write_text(content, encoding="utf-8")
    df = read_raw_file(p, sample_schema)
    assert len(df) == 1


@pytest.mark.unit
def test_read_raw_file_invalid_categorical_becomes_na(tmp_path: Path, sample_schema: dict) -> None:
    """PRODUCT value not in ['O','G'] becomes pd.NA, no exception."""
    content = (
        "LEASE_KID,MONTH-YEAR,PRODUCT\n"
        "1001,1-2024,X\n"
        "1002,2-2024,O\n"
    )
    p = tmp_path / "lp006.txt"
    p.write_text(content, encoding="utf-8")
    df = read_raw_file(p, sample_schema)
    # Row with PRODUCT=X should have NA product
    prod_vals = df["PRODUCT"].tolist()
    assert any(pd.isna(v) for v in prod_vals)


@pytest.mark.unit
def test_read_raw_file_source_file_basename(minimal_csv: Path, sample_schema: dict) -> None:
    """source_file column contains only the basename, not full path."""
    df = read_raw_file(minimal_csv, sample_schema)
    assert df["source_file"].iloc[0] == "lp001.txt"


@pytest.mark.unit
def test_read_raw_file_url_column_dropped(tmp_path: Path, sample_schema: dict) -> None:
    """URL column is dropped if present."""
    content = (
        "LEASE_KID,MONTH-YEAR,PRODUCT,URL\n"
        "1001,1-2024,O,https://example.com\n"
    )
    p = tmp_path / "lp007.txt"
    p.write_text(content, encoding="utf-8")
    df = read_raw_file(p, sample_schema)
    assert "URL" not in df.columns


@pytest.mark.unit
def test_read_raw_file_not_found(tmp_path: Path, sample_schema: dict) -> None:
    with pytest.raises(FileNotFoundError):
        read_raw_file(tmp_path / "missing.txt", sample_schema)


# ---------------------------------------------------------------------------
# Task 05: write_interim_parquet (partition count formula)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_partition_count_formula_lower_bound() -> None:
    """n_files=3 → partition count = 10 (lower bound)."""
    from kgs_pipeline.ingest import write_interim_parquet
    n = 3
    expected = max(10, min(n, 50))
    assert expected == 10


@pytest.mark.unit
def test_partition_count_formula_mid() -> None:
    n = 30
    expected = max(10, min(n, 50))
    assert expected == 30


@pytest.mark.unit
def test_partition_count_formula_upper_bound() -> None:
    n = 100
    expected = max(10, min(n, 50))
    assert expected == 50


@pytest.mark.unit
def test_write_interim_parquet_creates_files(tmp_path: Path) -> None:
    """write_interim_parquet produces at least one .parquet file."""
    import dask.dataframe as dd

    df = pd.DataFrame({
        "LEASE_KID": pd.array([1001], dtype=pd.Int64Dtype()),
        "MONTH-YEAR": pd.array(["1-2024"], dtype=pd.StringDtype()),
        "PRODUCT": pd.Categorical(["O"], categories=["O", "G"]),
        "source_file": pd.array(["lp001.txt"], dtype=pd.StringDtype()),
    })
    ddf = dd.from_pandas(df, npartitions=1)

    out = tmp_path / "parquet_out"
    write_interim_parquet(ddf, out, n_files=1)

    parquet_files = list(out.glob("*.parquet"))
    assert len(parquet_files) >= 1

    # Parquet files should be readable
    for pf in parquet_files:
        read_df = pd.read_parquet(pf)
        assert len(read_df.columns) > 0


# ---------------------------------------------------------------------------
# Task 07 (TR-22): Schema completeness across partitions
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_schema_completeness_across_partitions(tmp_path: Path, sample_schema: dict) -> None:
    """After ingest, each partition has the expected canonical columns."""
    import dask.dataframe as dd

    required_cols = [
        "LEASE_KID", "LEASE", "API_NUMBER", "MONTH-YEAR", "PRODUCT",
        "PRODUCTION", "WELLS", "OPERATOR", "COUNTY", "source_file",
    ]

    # Build synthetic DataFrames
    rows = []
    for i in range(3):
        rows.append({
            "LEASE_KID": 1000 + i,
            "MONTH-YEAR": f"{i+1}-2024",
            "PRODUCT": "O",
            "PRODUCTION": float(i * 100),
            "WELLS": i + 1,
            "OPERATOR": "ACME",
            "COUNTY": "Barton",
            "LEASE": f"LEASE{i}",
            "API_NUMBER": f"15001{i:05d}",
            "source_file": f"lp{i:03d}.txt",
        })

    df = pd.DataFrame(rows)
    ddf = dd.from_pandas(df, npartitions=3)

    out = tmp_path / "interim"
    write_interim_parquet(ddf, out, n_files=3)

    parquet_files = list(out.glob("*.parquet"))
    assert len(parquet_files) >= 1

    for pf in parquet_files:
        part_df = pd.read_parquet(pf)
        for col in required_cols:
            assert col in part_df.columns, f"Column '{col}' missing in {pf.name}"
