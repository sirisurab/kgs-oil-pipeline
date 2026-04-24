"""Tests for the ingest stage (tasks I1–I3).

Test requirements: TR-17, TR-18, TR-22, TR-24, TR-27.
Each test carries exactly one of: @pytest.mark.unit or @pytest.mark.integration
per ADR-008.
"""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pytest

from kgs_pipeline.ingest import (
    Schema,
    load_schema,
    read_raw_file,
    ingest,
)

# Path to the project data dictionary
_DD_PATH = Path("references/kgs_monthly_data_dictionary.csv")

# Canonical column names from the data dictionary (non-source_file)
_CANONICAL_COLS = [
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
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def make_raw_csv_content(rows: list[dict]) -> str:
    """Build a CSV string with canonical columns + rows."""
    header = ",".join(_CANONICAL_COLS)
    lines = [header]
    for r in rows:
        line = ",".join(str(r.get(c, "")) for c in _CANONICAL_COLS)
        lines.append(line)
    return "\n".join(lines) + "\n"


@pytest.fixture()
def schema() -> Schema:
    return load_schema(_DD_PATH)


@pytest.fixture()
def raw_file_mixed_years(tmp_path: Path) -> Path:
    """Raw file with rows spanning 2022, 2023, 2024, 2025."""
    rows = [
        {
            "LEASE_KID": "1001",
            "LEASE": "A",
            "MONTH-YEAR": "6-2022",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "100",
        },
        {
            "LEASE_KID": "1001",
            "LEASE": "A",
            "MONTH-YEAR": "1-2023",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "120",
        },
        {
            "LEASE_KID": "1001",
            "LEASE": "A",
            "MONTH-YEAR": "3-2024",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "150",
        },
        {
            "LEASE_KID": "1001",
            "LEASE": "A",
            "MONTH-YEAR": "1-2025",
            "PRODUCT": "G",
            "WELLS": "2",
            "PRODUCTION": "500",
        },
    ]
    p = tmp_path / "lp1001.txt"
    p.write_text(make_raw_csv_content(rows))
    return p


@pytest.fixture()
def raw_file_all_pre2024(tmp_path: Path) -> Path:
    """Raw file where every row is pre-2024."""
    rows = [
        {
            "LEASE_KID": "2001",
            "LEASE": "B",
            "MONTH-YEAR": "1-2020",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "50",
        },
        {
            "LEASE_KID": "2001",
            "LEASE": "B",
            "MONTH-YEAR": "6-2023",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "60",
        },
    ]
    p = tmp_path / "lp2001.txt"
    p.write_text(make_raw_csv_content(rows))
    return p


@pytest.fixture()
def raw_file_bad_month_year(tmp_path: Path) -> Path:
    """Raw file with rows whose MONTH-YEAR has non-numeric year component."""
    rows = [
        {
            "LEASE_KID": "3001",
            "LEASE": "C",
            "MONTH-YEAR": "-1-1965",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "30",
        },
        {
            "LEASE_KID": "3001",
            "LEASE": "C",
            "MONTH-YEAR": "0-1966",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "35",
        },
        {
            "LEASE_KID": "3001",
            "LEASE": "C",
            "MONTH-YEAR": "2-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "40",
        },
    ]
    p = tmp_path / "lp3001.txt"
    p.write_text(make_raw_csv_content(rows))
    return p


@pytest.fixture()
def raw_file_out_of_set_product(tmp_path: Path) -> Path:
    """Raw file with PRODUCT value outside {O, G}."""
    rows = [
        {
            "LEASE_KID": "4001",
            "LEASE": "D",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "X",
            "WELLS": "1",
            "PRODUCTION": "20",
        },
        {
            "LEASE_KID": "4001",
            "LEASE": "D",
            "MONTH-YEAR": "2-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "25",
        },
    ]
    p = tmp_path / "lp4001.txt"
    p.write_text(make_raw_csv_content(rows))
    return p


@pytest.fixture()
def raw_file_missing_nullable_col(tmp_path: Path) -> Path:
    """Raw file missing a nullable column (LATITUDE)."""
    cols_without_lat = [c for c in _CANONICAL_COLS if c != "LATITUDE"]
    header = ",".join(cols_without_lat)
    rows_data = [
        {
            "LEASE_KID": "5001",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "10",
        },
    ]
    lines = [header]
    for r in rows_data:
        line = ",".join(str(r.get(c, "")) for c in cols_without_lat)
        lines.append(line)
    content = "\n".join(lines) + "\n"
    p = tmp_path / "lp5001.txt"
    p.write_text(content)
    return p


@pytest.fixture()
def raw_file_missing_non_nullable_col(tmp_path: Path) -> Path:
    """Raw file missing a non-nullable column (LEASE_KID)."""
    cols_without_kid = [c for c in _CANONICAL_COLS if c != "LEASE_KID"]
    header = ",".join(cols_without_kid)
    content = header + "\n1-2024,O,1,100\n"
    p = tmp_path / "lp_bad.txt"
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# Task I1 tests: load_schema
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_schema_contains_all_columns(schema: Schema) -> None:
    """Schema contains every column from the data dictionary in order."""
    for col in _CANONICAL_COLS:
        assert schema.has_column(col), f"Column {col!r} missing from schema"
    assert schema.column_names()[: len(_CANONICAL_COLS)] == _CANONICAL_COLS


@pytest.mark.unit
def test_schema_nullable_dtypes(schema: Schema) -> None:
    """nullable=yes → nullable-aware pandas dtype; nullable=no → non-nullable."""
    # LEASE_KID is non-nullable int → int64
    kid = schema["LEASE_KID"]
    assert not kid.nullable
    assert kid.pandas_dtype == "int64"

    # WELLS is nullable int → Int64
    wells = schema["WELLS"]
    assert wells.nullable
    assert wells.pandas_dtype == "Int64"

    # PRODUCTION is nullable float → Float64
    prod = schema["PRODUCTION"]
    assert prod.nullable
    assert prod.pandas_dtype == "Float64"


@pytest.mark.unit
def test_schema_categorical_values(schema: Schema) -> None:
    """PRODUCT exposes {O, G}; TWN_DIR exposes {S, N}."""
    assert set(schema["PRODUCT"].categories) == {"O", "G"}
    assert set(schema["TWN_DIR"].categories) == {"S", "N"}


# ---------------------------------------------------------------------------
# Task I2 tests: read_raw_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_file_filters_year(raw_file_mixed_years: Path, schema: Schema) -> None:
    """Only rows with year >= 2024 are returned; each column has correct dtype."""
    df = read_raw_file(raw_file_mixed_years, schema)
    assert len(df) == 2  # 3-2024 and 1-2025
    months_years = set(df["MONTH-YEAR"].tolist())
    assert "6-2022" not in months_years
    assert "1-2023" not in months_years
    # Check dtypes
    assert str(df["LEASE_KID"].dtype) == "int64"
    assert str(df["PRODUCTION"].dtype) == "Float64"


@pytest.mark.unit
def test_read_raw_file_missing_nullable_col(
    raw_file_missing_nullable_col: Path, schema: Schema
) -> None:
    """Missing nullable=yes column → added as all-NA with correct dtype."""
    df = read_raw_file(raw_file_missing_nullable_col, schema)
    assert "LATITUDE" in df.columns
    assert df["LATITUDE"].isna().all()
    assert str(df["LATITUDE"].dtype) == "Float64"


@pytest.mark.unit
def test_read_raw_file_missing_non_nullable_raises(
    raw_file_missing_non_nullable_col: Path, schema: Schema
) -> None:
    """Missing nullable=no column (LEASE_KID) → raises ValueError."""
    with pytest.raises(ValueError, match="LEASE_KID"):
        read_raw_file(raw_file_missing_non_nullable_col, schema)


@pytest.mark.unit
def test_read_raw_file_out_of_set_product(
    raw_file_out_of_set_product: Path, schema: Schema
) -> None:
    """PRODUCT value outside {O, G} → replaced with null before categorical casting."""
    df = read_raw_file(raw_file_out_of_set_product, schema)
    # Row with PRODUCT=X should have NA in PRODUCT
    na_mask = df["PRODUCT"].isna()
    assert na_mask.any()


@pytest.mark.unit
def test_read_raw_file_all_pre2024_returns_empty_schema(
    raw_file_all_pre2024: Path, schema: Schema
) -> None:
    """All rows pre-2024 → empty DataFrame with full canonical schema (H2)."""
    df = read_raw_file(raw_file_all_pre2024, schema)
    assert len(df) == 0
    for col in _CANONICAL_COLS:
        assert col in df.columns, f"Column {col!r} missing from empty result"


@pytest.mark.unit
def test_read_raw_file_bad_month_year_dropped(
    raw_file_bad_month_year: Path, schema: Schema
) -> None:
    """Rows with MONTH-YEAR = '-1-1965' or '0-1966' are dropped."""
    df = read_raw_file(raw_file_bad_month_year, schema)
    # Only the 2-2024 row should survive
    assert len(df) == 1
    assert df["MONTH-YEAR"].iloc[0] == "2-2024"


# ---------------------------------------------------------------------------
# Task I3 tests: ingest
# ---------------------------------------------------------------------------


def make_ingest_config(tmp_path: Path, raw_dir: Path) -> dict:
    return {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(tmp_path / "interim"),
            "data_dictionary_path": str(_DD_PATH),
        }
    }


def make_small_raw_files(raw_dir: Path, n: int = 3) -> None:
    """Write n small synthetic raw files to raw_dir."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        rows = [
            {
                "LEASE_KID": str(1000 + i),
                "LEASE": f"L{i}",
                "MONTH-YEAR": "1-2024",
                "PRODUCT": "O",
                "WELLS": "1",
                "PRODUCTION": str(100 + i * 10),
            },
            {
                "LEASE_KID": str(1000 + i),
                "LEASE": f"L{i}",
                "MONTH-YEAR": "2-2024",
                "PRODUCT": "O",
                "WELLS": "1",
                "PRODUCTION": str(110 + i * 10),
            },
        ]
        content = make_raw_csv_content(rows)
        (raw_dir / f"lp{1000 + i}.txt").write_text(content)


@pytest.mark.unit
def test_ingest_output_parquet_readable(tmp_path: Path) -> None:
    """Ingest writes Parquet that is readable; all expected columns present (TR-18)."""
    raw_dir = tmp_path / "raw"
    make_small_raw_files(raw_dir, n=3)
    cfg = make_ingest_config(tmp_path, raw_dir)
    ingest(cfg)

    interim_dir = Path(cfg["ingest"]["interim_dir"])
    df = dd.read_parquet(str(interim_dir)).compute()
    for col in _CANONICAL_COLS:
        assert col in df.columns, f"Column {col!r} missing from ingest output"
    assert "source_file" in df.columns


@pytest.mark.unit
def test_ingest_columns_carry_dictionary_dtypes(tmp_path: Path) -> None:
    """Every column in ingest output carries its data-dictionary dtype."""
    raw_dir = tmp_path / "raw"
    make_small_raw_files(raw_dir, n=2)
    cfg = make_ingest_config(tmp_path, raw_dir)
    ingest(cfg)

    interim_dir = Path(cfg["ingest"]["interim_dir"])
    schema = load_schema(_DD_PATH)
    df = dd.read_parquet(str(interim_dir)).compute()

    for col_schema in schema.columns:
        col = col_schema.name
        if col not in df.columns:
            continue
        expected_dtype = col_schema.pandas_dtype
        actual = str(df[col].dtype)
        if expected_dtype == "category":
            assert actual == "category", f"{col}: expected category, got {actual}"
        elif expected_dtype == "Int64":
            assert actual in ("Int64", "int64"), f"{col}: expected Int64-compatible, got {actual}"
        elif expected_dtype == "Float64":
            assert actual in ("Float64", "float64"), (
                f"{col}: expected Float64-compatible, got {actual}"
            )
        elif expected_dtype == "string":
            assert actual in ("string", "object"), (
                f"{col}: expected string-compatible, got {actual}"
            )


@pytest.mark.unit
def test_ingest_returns_dask_dataframe_intermediate(tmp_path: Path) -> None:
    """Intermediate results are Dask DataFrames — no internal .compute() (TR-17)."""
    import dask.dataframe as dd_mod

    raw_dir = tmp_path / "raw"
    make_small_raw_files(raw_dir, n=2)

    # Verify that the read_parquet call produces a Dask DF (not pandas)
    cfg = make_ingest_config(tmp_path, raw_dir)
    ingest(cfg)

    interim_dir = Path(cfg["ingest"]["interim_dir"])
    ddf = dd_mod.read_parquet(str(interim_dir))
    assert isinstance(ddf, dd_mod.DataFrame), "Expected a Dask DataFrame"


@pytest.mark.unit
def test_ingest_tr22_columns_in_every_partition(tmp_path: Path) -> None:
    """Sample partitions from interim output; required columns present (TR-22)."""
    required_tr22 = [
        "LEASE_KID",
        "LEASE",
        "API_NUMBER",
        "MONTH-YEAR",
        "PRODUCT",
        "PRODUCTION",
        "WELLS",
        "OPERATOR",
        "COUNTY",
        "source_file",
    ]
    raw_dir = tmp_path / "raw"
    make_small_raw_files(raw_dir, n=3)
    cfg = make_ingest_config(tmp_path, raw_dir)
    ingest(cfg)

    interim_dir = Path(cfg["ingest"]["interim_dir"])
    parquet_files = sorted(interim_dir.rglob("*.parquet"))
    assert len(parquet_files) >= 3

    for pf in parquet_files[:3]:
        part = pd.read_parquet(pf)
        for col in required_tr22:
            assert col in part.columns, f"Column {col!r} missing from partition {pf.name}"


@pytest.mark.integration
def test_ingest_tr24_integration(tmp_path: Path) -> None:
    """Run ingest on synthetic raw files; assert all boundary-ingest-transform guarantees (TR-24)."""
    raw_dir = tmp_path / "raw"
    make_small_raw_files(raw_dir, n=3)

    cfg = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(tmp_path / "interim"),
            "data_dictionary_path": str(_DD_PATH),
        }
    }
    ingest(cfg)

    interim_dir = Path(cfg["ingest"]["interim_dir"])
    assert interim_dir.exists()

    # Readable (TR-18)
    ddf = dd.read_parquet(str(interim_dir))
    assert isinstance(ddf, dd.DataFrame)

    df = ddf.compute()
    # All canonical columns present
    for col in _CANONICAL_COLS:
        assert col in df.columns

    # Partition count within contract
    assert 10 <= ddf.npartitions <= 50

    # Schema: check key dtypes
    schema = load_schema(_DD_PATH)
    for col_schema in schema.columns:
        col = col_schema.name
        if col not in df.columns:
            continue
        expected = col_schema.pandas_dtype
        actual = str(df[col].dtype)
        if expected == "category":
            assert actual == "category"
        elif expected in ("Int64",):
            assert actual in ("Int64", "int64")
        elif expected in ("Float64",):
            assert actual in ("Float64", "float64")
