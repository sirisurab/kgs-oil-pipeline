"""Tests for kgs_pipeline/ingest.py."""

from pathlib import Path

import dask.dataframe as dd  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]
import pytest

from kgs_pipeline.ingest import (
    SchemaError,
    coerce_types,
    filter_date_range,
    read_raw_file,
    run_ingest,
    validate_schema,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

HEADER = (
    "LEASE_KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,COUNTY,"
    "TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,"
    "MONTH-YEAR,PRODUCT,WELLS,PRODUCTION"
)


def _row(
    lease_id: str = "L1",
    lease: str = "TEST",
    month_year: str = "1-2024",
    product: str = "O",
    production: str = "100.0",
    wells: str = "2",
    operator: str = "OPS",
    county: str = "Allen",
) -> str:
    return (
        f"{lease_id},{lease},123,API-1,FIELD,ZONE,{operator},{county},"
        f"1,S,1,E,1,NE,39.0,-95.0,{month_year},{product},{wells},{production}"
    )


def _write_fixture(path: Path, rows: list[str]) -> Path:
    path.write_text(HEADER + "\n" + "\n".join(rows))
    return path


# ---------------------------------------------------------------------------
# Task 01: read_raw_file
# ---------------------------------------------------------------------------


def test_read_raw_file_returns_df(tmp_path: Path) -> None:
    f = _write_fixture(tmp_path / "test.txt", [_row()])
    df = read_raw_file(str(f))
    assert len(df) == 1
    assert "source_file" in df.columns
    assert df["source_file"].iloc[0] == "test.txt"


def test_read_raw_file_header_only_raises(tmp_path: Path) -> None:
    f = tmp_path / "empty.txt"
    f.write_text(HEADER + "\n")
    with pytest.raises(ValueError, match="no data rows"):
        read_raw_file(str(f))


def test_read_raw_file_not_found_raises() -> None:
    with pytest.raises(FileNotFoundError):
        read_raw_file("/nonexistent/file.txt")


def test_read_raw_file_strips_column_whitespace(tmp_path: Path) -> None:
    f = tmp_path / "spaces.txt"
    f.write_text(" LEASE_KID , MONTH-YEAR \n" + "L1,1-2024\n")
    df = read_raw_file(str(f))
    assert "LEASE_KID" in df.columns
    assert "MONTH-YEAR" in df.columns


# ---------------------------------------------------------------------------
# Task 02: validate_schema
# ---------------------------------------------------------------------------


def _make_df_with_cols(cols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=cols)


def test_validate_schema_all_required() -> None:
    from kgs_pipeline.config import REQUIRED_COLUMNS

    df = _make_df_with_cols(REQUIRED_COLUMNS + ["EXTRA_COL"])
    validate_schema(df, "dummy.txt")  # should not raise


def test_validate_schema_missing_raises() -> None:
    cols = [
        "LEASE_KID",
        "LEASE",
        "API_NUMBER",
        "MONTH-YEAR",
        "PRODUCT",
        "WELLS",
        "OPERATOR",
        "COUNTY",
        "source_file",
    ]
    # Missing PRODUCTION
    df = _make_df_with_cols(cols)
    with pytest.raises(SchemaError, match="PRODUCTION"):
        validate_schema(df, "dummy.txt")


def test_validate_schema_empty_df_no_raise() -> None:
    from kgs_pipeline.config import REQUIRED_COLUMNS

    df = pd.DataFrame(columns=REQUIRED_COLUMNS)
    validate_schema(df, "dummy.txt")


# ---------------------------------------------------------------------------
# Task 03: coerce_types
# ---------------------------------------------------------------------------


def test_coerce_types_numeric() -> None:
    from kgs_pipeline.config import REQUIRED_COLUMNS

    row = {c: "value" for c in REQUIRED_COLUMNS}
    row["PRODUCTION"] = "161.8"
    row["WELLS"] = "2"
    df = pd.DataFrame([row])
    df = coerce_types(df)
    assert df["PRODUCTION"].iloc[0] == pytest.approx(161.8)


def test_coerce_types_nan_on_invalid() -> None:
    from kgs_pipeline.config import REQUIRED_COLUMNS

    row = {c: "value" for c in REQUIRED_COLUMNS}
    row["PRODUCTION"] = "N/A"
    df = pd.DataFrame([row])
    df = coerce_types(df)
    assert pd.isna(df["PRODUCTION"].iloc[0])


def test_coerce_types_string_dtype() -> None:
    from kgs_pipeline.config import REQUIRED_COLUMNS

    row = {c: "value" for c in REQUIRED_COLUMNS}
    row["PRODUCTION"] = "100"
    df = pd.DataFrame([row])
    df = coerce_types(df)
    string_cols = [
        c for c in df.columns if c not in ["PRODUCTION", "WELLS", "LATITUDE", "LONGITUDE"]
    ]
    for col in string_cols:
        assert df[col].dtype == pd.StringDtype(), f"{col} should be StringDtype"


# ---------------------------------------------------------------------------
# Task 04: filter_date_range
# ---------------------------------------------------------------------------


def _df_with_month_years(values: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"MONTH-YEAR": values, "PRODUCTION": ["100"] * len(values)})


def test_filter_date_range_keeps_2024_and_2025() -> None:
    df = _df_with_month_years(["1-2024", "6-2023", "12-2025"])
    result = filter_date_range(df)
    assert len(result) == 2


def test_filter_date_range_drops_yearly_summary() -> None:
    df = _df_with_month_years(["0-2024", "1-2024"])
    result = filter_date_range(df)
    assert len(result) == 1
    assert result["MONTH-YEAR"].iloc[0] == "1-2024"


def test_filter_date_range_drops_cumulative_sentinel() -> None:
    df = _df_with_month_years(["-1-2024", "1-2024"])
    result = filter_date_range(df)
    assert len(result) == 1


def test_filter_date_range_empty_result() -> None:
    df = _df_with_month_years(["1-2020", "6-2023"])
    result = filter_date_range(df)
    assert len(result) == 0


def test_filter_date_range_zero_production_kept() -> None:
    df = pd.DataFrame({"MONTH-YEAR": ["1-2024"], "PRODUCTION": [0.0]})
    result = filter_date_range(df)
    assert len(result) == 1
    assert result["PRODUCTION"].iloc[0] == 0.0


# ---------------------------------------------------------------------------
# Task 05: run_ingest
# ---------------------------------------------------------------------------


def _make_kgs_txt(path: Path, rows_2024: int = 2, rows_2023: int = 2) -> None:
    """Write a minimal KGS .txt file with mixed years."""
    lines = [HEADER]
    for i in range(rows_2024):
        lines.append(_row(lease_id=f"L{i}", month_year="1-2024", production="100.0"))
    for i in range(rows_2023):
        lines.append(_row(lease_id=f"L{i}", month_year="6-2023", production="50.0"))
    path.write_text("\n".join(lines))


def test_run_ingest_returns_dask_df(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"
    for i in range(3):
        _make_kgs_txt(raw / f"lease_{i}.txt")
    ddf = run_ingest(str(raw), str(out))
    assert isinstance(ddf, dd.DataFrame)


def test_run_ingest_required_columns(tmp_path: Path) -> None:
    from kgs_pipeline.config import REQUIRED_COLUMNS

    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"
    _make_kgs_txt(raw / "lease.txt")
    ddf = run_ingest(str(raw), str(out))
    for col in REQUIRED_COLUMNS:
        assert col in ddf.columns, f"Missing column: {col}"


def test_run_ingest_filters_pre2024(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"
    for i in range(3):
        _make_kgs_txt(raw / f"lease_{i}.txt", rows_2024=2, rows_2023=2)
    ddf = run_ingest(str(raw), str(out))
    df = ddf.compute()
    assert len(df) == 6  # 3 files × 2 rows from 2024


def test_run_ingest_partial_failure(tmp_path: Path) -> None:
    """One valid file + one invalid file: should succeed on valid file."""
    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"
    # Valid file
    _make_kgs_txt(raw / "valid.txt")
    # Invalid file: missing required columns
    (raw / "invalid.txt").write_text("COL_A,COL_B\n1,2\n")
    ddf = run_ingest(str(raw), str(out))
    assert isinstance(ddf, dd.DataFrame)


def test_run_ingest_parquet_file_count(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"
    _make_kgs_txt(raw / "lease.txt")
    run_ingest(str(raw), str(out))
    parquet_files = list(out.rglob("*.parquet"))
    assert 1 <= len(parquet_files) <= 200


def test_run_ingest_parquet_readable(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"
    _make_kgs_txt(raw / "lease.txt")
    run_ingest(str(raw), str(out))
    ddf2 = dd.read_parquet(str(out))
    assert len(ddf2.compute()) > 0


def test_run_ingest_zero_production_retained(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"
    f = raw / "lease_zero.txt"
    lines = [HEADER, _row(month_year="1-2024", production="0.0")]
    f.write_text("\n".join(lines))
    ddf = run_ingest(str(raw), str(out))
    df = ddf.compute()
    assert 0.0 in df["PRODUCTION"].values


def test_run_ingest_no_txt_files_raises(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"
    with pytest.raises(ValueError, match="No .txt files"):
        run_ingest(str(raw), str(out))


# ---------------------------------------------------------------------------
# Task 06: Schema completeness across partitions
# ---------------------------------------------------------------------------


def test_schema_completeness_across_partitions(tmp_path: Path) -> None:
    """Assert all required columns present in each Parquet partition."""
    from kgs_pipeline.config import REQUIRED_COLUMNS

    raw = tmp_path / "raw"
    raw.mkdir()
    out = tmp_path / "interim"

    # Create enough rows to generate >= 1 partition (may be just 1 for small data)
    for i in range(5):
        _make_kgs_txt(raw / f"lease_{i}.txt", rows_2024=4, rows_2023=0)

    run_ingest(str(raw), str(out))
    parquet_files = sorted(out.rglob("*.parquet"))
    assert len(parquet_files) >= 1

    for pf in parquet_files[:3]:
        part_df = pd.read_parquet(str(pf))
        for col in REQUIRED_COLUMNS:
            assert col in part_df.columns, f"Column {col} missing in {pf.name}"
