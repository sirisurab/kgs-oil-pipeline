"""Tests for kgs_pipeline.ingest module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pytest

from kgs_pipeline.ingest import (
    EXPECTED_COLUMNS,
    build_dask_dataframe,
    discover_raw_files,
    filter_by_year,
    read_raw_file,
    run_ingest,
    write_interim_parquet,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CSV_HEADER = (
    "LEASE KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,"
    "COUNTY,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,"
    "LATITUDE,LONGITUDE,MONTH-YEAR,PRODUCT,WELLS,PRODUCTION\n"
)


def _make_csv_row(
    lease_kid: str = "1",
    month_year: str = "1-2024",
    product: str = "O",
    production: str = "100",
) -> str:
    return (
        f"{lease_kid},LEASE_{lease_kid},DC,API001,FIELD,ZONE,OP,ALLEN,"
        f"TS,N,R,E,1,NE,38.0,-97.0,{month_year},{product},1,{production}\n"
    )


def _write_raw_file(path: Path, rows: list[str]) -> None:
    path.write_text(_CSV_HEADER + "".join(rows))


# ---------------------------------------------------------------------------
# Task 01: discover_raw_files
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_discover_raw_files_returns_txt(tmp_path: Path) -> None:
    for name in ("a.txt", "b.txt", "c.txt", "d.csv"):
        (tmp_path / name).write_text("data")
    result = discover_raw_files(str(tmp_path))
    assert len(result) == 3
    assert all(p.suffix == ".txt" for p in result)


@pytest.mark.unit
def test_discover_raw_files_excludes_hidden(tmp_path: Path) -> None:
    (tmp_path / ".hidden.txt").write_text("hidden")
    (tmp_path / "a.txt").write_text("data")
    (tmp_path / "b.txt").write_text("data")
    result = discover_raw_files(str(tmp_path))
    assert len(result) == 2
    assert all(not p.name.startswith(".") for p in result)


@pytest.mark.unit
def test_discover_raw_files_missing_dir() -> None:
    with pytest.raises(FileNotFoundError):
        discover_raw_files("/nonexistent/path")


@pytest.mark.unit
def test_discover_raw_files_empty_dir(tmp_path: Path) -> None:
    result = discover_raw_files(str(tmp_path))
    assert result == []


# ---------------------------------------------------------------------------
# Task 02: read_raw_file
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_file_standard(tmp_path: Path) -> None:
    f = tmp_path / "lease.txt"
    _write_raw_file(f, [_make_csv_row() for _ in range(3)])
    df = read_raw_file(f)
    assert df.shape[0] == 3
    assert "LEASE_KID" in df.columns
    assert "source_file" in df.columns
    assert "MONTH_YEAR" in df.columns  # normalised from MONTH-YEAR


@pytest.mark.unit
def test_read_raw_file_missing_column_filled(tmp_path: Path) -> None:
    # Write CSV without COUNTY
    rows_no_county = _CSV_HEADER.replace(",COUNTY", "") + _make_csv_row().replace(",ALLEN", "")
    f = tmp_path / "lease.txt"
    f.write_text(rows_no_county)
    df = read_raw_file(f)
    assert "COUNTY" in df.columns


@pytest.mark.unit
def test_read_raw_file_latin1_encoding(tmp_path: Path) -> None:
    content = _CSV_HEADER + _make_csv_row()
    f = tmp_path / "lease.txt"
    f.write_bytes(content.encode("latin-1"))
    df = read_raw_file(f)
    assert not df.empty


@pytest.mark.unit
def test_read_raw_file_empty_file(tmp_path: Path) -> None:
    f = tmp_path / "empty.txt"
    f.write_bytes(b"")
    df = read_raw_file(f)
    assert df.empty
    assert all(col in df.columns for col in EXPECTED_COLUMNS)


@pytest.mark.unit
def test_read_raw_file_parse_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    f = tmp_path / "bad.txt"
    f.write_text('col1,col2\n"unclosed quote\n')
    # Should return empty df, not raise
    df = read_raw_file(f)
    assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 03: filter_by_year
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_filter_by_year_basic() -> None:
    data = pd.DataFrame(
        {
            "MONTH_YEAR": ["1-2023", "12-2024", "6-2025", "0-2022"],
            "val": [1, 2, 3, 4],
        }
    )
    ddf = dd.from_pandas(data, npartitions=1)
    result = filter_by_year(ddf, min_year=2024).compute()
    assert len(result) == 2


@pytest.mark.unit
def test_filter_by_year_boundary_include() -> None:
    data = pd.DataFrame({"MONTH_YEAR": ["1-2024"], "val": [1]})
    ddf = dd.from_pandas(data, npartitions=1)
    result = filter_by_year(ddf, min_year=2024).compute()
    assert len(result) == 1


@pytest.mark.unit
def test_filter_by_year_boundary_exclude() -> None:
    data = pd.DataFrame({"MONTH_YEAR": ["12-2023"], "val": [1]})
    ddf = dd.from_pandas(data, npartitions=1)
    result = filter_by_year(ddf, min_year=2024).compute()
    assert len(result) == 0


@pytest.mark.unit
def test_filter_by_year_null_excluded() -> None:
    data = pd.DataFrame({"MONTH_YEAR": [None, "1-2024"], "val": [1, 2]})
    ddf = dd.from_pandas(data, npartitions=1)
    result = filter_by_year(ddf, min_year=2024).compute()
    assert len(result) == 1


@pytest.mark.unit
def test_filter_by_year_returns_dask() -> None:
    data = pd.DataFrame({"MONTH_YEAR": ["1-2024"], "val": [1]})
    ddf = dd.from_pandas(data, npartitions=1)
    result = filter_by_year(ddf, min_year=2024)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 04: build_dask_dataframe
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_dask_dataframe_returns_dask(tmp_path: Path) -> None:
    """TR-17: return type must be dask.dataframe.DataFrame."""
    files = []
    for i in range(2):
        f = tmp_path / f"lease{i}.txt"
        _write_raw_file(f, [_make_csv_row(month_year="1-2024"), _make_csv_row(month_year="1-2023")])
        files.append(f)
    result = build_dask_dataframe(files, min_year=2024)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_build_dask_dataframe_empty_raises() -> None:
    with pytest.raises(ValueError):
        build_dask_dataframe([])


@pytest.mark.unit
def test_build_dask_dataframe_source_file(tmp_path: Path) -> None:
    f = tmp_path / "lease.txt"
    _write_raw_file(f, [_make_csv_row()])
    result = build_dask_dataframe([f])
    assert "source_file" in result.columns


@pytest.mark.unit
def test_build_dask_dataframe_expected_columns(tmp_path: Path) -> None:
    files = []
    for i in range(3):
        f = tmp_path / f"l{i}.txt"
        _write_raw_file(f, [_make_csv_row(month_year="6-2024"), _make_csv_row(month_year="6-2023")])
        files.append(f)
    result = build_dask_dataframe(files)
    for col in EXPECTED_COLUMNS:
        assert col in result.columns


# ---------------------------------------------------------------------------
# Task 05: write_interim_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_interim_parquet_writes_files(tmp_path: Path) -> None:
    rows = [_make_csv_row(month_year="1-2024") for _ in range(10)]
    data_rows = []
    for r in rows:
        parts = r.strip().split(",")
        data_rows.append(parts)
    # Build a minimal Dask df
    df = pd.DataFrame(
        {
            **{
                col: pd.array(["x"] * 10, dtype=pd.StringDtype())
                for col in [
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
                    "MONTH_YEAR",
                    "PRODUCT",
                    "source_file",
                ]
            },
            "LATITUDE": [38.0] * 10,
            "LONGITUDE": [-97.0] * 10,
            "PRODUCTION": [100.0] * 10,
            "WELLS": [1.0] * 10,
        }
    )
    ddf = dd.from_pandas(df, npartitions=3)
    out_dir = tmp_path / "interim"
    count = write_interim_parquet(ddf, str(out_dir))
    assert count >= 1
    assert len(list(out_dir.glob("*.parquet"))) == count


@pytest.mark.unit
def test_write_interim_parquet_creates_dir(tmp_path: Path) -> None:
    new_dir = tmp_path / "new" / "interim"
    df = pd.DataFrame({"LEASE_KID": pd.array(["1"], dtype=pd.StringDtype()), "PRODUCTION": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    write_interim_parquet(ddf, str(new_dir))
    assert new_dir.exists()


@pytest.mark.unit
def test_write_interim_parquet_count_matches(tmp_path: Path) -> None:
    df = pd.DataFrame({"col": pd.array(["a", "b", "c"], dtype=pd.StringDtype())})
    ddf = dd.from_pandas(df, npartitions=3)
    out = tmp_path / "out"
    count = write_interim_parquet(ddf, str(out))
    assert count == len(list(out.glob("*.parquet")))


# ---------------------------------------------------------------------------
# Task 06: run_ingest orchestrator
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_ingest_calls_stages(tmp_path: Path) -> None:
    with (
        patch("kgs_pipeline.ingest.discover_raw_files", return_value=[Path("f.txt")]) as m1,
        patch("kgs_pipeline.ingest.build_dask_dataframe", return_value=MagicMock()) as m2,
        patch("kgs_pipeline.ingest.write_interim_parquet", return_value=5) as m3,
    ):
        result = run_ingest(str(tmp_path), str(tmp_path / "out"))
        assert result == 5
        m1.assert_called_once()
        m2.assert_called_once()
        m3.assert_called_once()


# ---------------------------------------------------------------------------
# Task 07: TR-17 lazy evaluation
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_tr17_build_dask_returns_dask_not_pandas(tmp_path: Path) -> None:
    files = []
    for i in range(2):
        f = tmp_path / f"l{i}.txt"
        _write_raw_file(f, [_make_csv_row()])
        files.append(f)
    result = build_dask_dataframe(files)
    assert isinstance(result, dd.DataFrame)
    assert not isinstance(result, pd.DataFrame)
