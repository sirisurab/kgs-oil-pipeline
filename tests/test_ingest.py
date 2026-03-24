"""Tests for kgs_pipeline/ingest.py — Tasks 06–11."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pytest

import kgs_pipeline.config as config
from kgs_pipeline.ingest import (
    concatenate_raw_files,
    discover_raw_files,
    enrich_with_lease_metadata,
    filter_monthly_records,
    read_raw_file,
    run_ingest_pipeline,
    write_interim_parquet,
)


# ---------------------------------------------------------------------------
# Task 06: discover_raw_files
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_discover_raw_files_returns_txt_only(tmp_path: Path):
    (tmp_path / "a.txt").write_text("data")
    (tmp_path / "b.txt").write_text("data")
    (tmp_path / "c.txt").write_text("data")
    (tmp_path / "d.csv").write_text("data")

    result = discover_raw_files(tmp_path)
    assert len(result) == 3
    assert all(p.suffix == ".txt" for p in result)
    assert result == sorted(result)


@pytest.mark.unit
def test_discover_raw_files_nonexistent_dir():
    with pytest.raises(FileNotFoundError):
        discover_raw_files(Path("/nonexistent/dir"))


@pytest.mark.unit
def test_discover_raw_files_empty_dir(tmp_path: Path):
    result = discover_raw_files(tmp_path)
    assert result == []


@pytest.mark.integration
def test_discover_raw_files_real_dir():
    if not config.RAW_DATA_DIR.exists() or not list(config.RAW_DATA_DIR.glob("*.txt")):
        pytest.skip("No raw files available")
    result = discover_raw_files(config.RAW_DATA_DIR)
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(p, Path) for p in result)


# ---------------------------------------------------------------------------
# Task 07: read_raw_file
# ---------------------------------------------------------------------------

SAMPLE_CSV = """\
LEASE KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,COUNTY,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,LATITUDE,LONGITUDE,MONTH-YEAR,PRODUCT,WELLS,PRODUCTION
1001,Test Lease,ABC123,15-001-12345,Test Field,Test Zone,ACME Oil,Allen,10,S,5,W,12,NE,38.5,-95.5,3-2021,O,1,500.0
1001,Test Lease,ABC123,15-001-12345,Test Field,Test Zone,ACME Oil,Allen,10,S,5,W,12,NE,38.5,-95.5,4-2021,O,1,450.0
1001,Test Lease,ABC123,15-001-12345,Test Field,Test Zone,ACME Oil,Allen,10,S,5,W,12,NE,38.5,-95.5,5-2021,G,1,2000.0
1001,Test Lease,ABC123,15-001-12345,Test Field,Test Zone,ACME Oil,Allen,10,S,5,W,12,NE,38.5,-95.5,6-2021,O,1,400.0
1001,Test Lease,ABC123,15-001-12345,Test Field,Test Zone,ACME Oil,Allen,10,S,5,W,12,NE,38.5,-95.5,7-2021,O,1,380.0
"""


@pytest.mark.unit
def test_read_raw_file_returns_dask_df(tmp_path: Path):
    fp = tmp_path / "lp564.txt"
    fp.write_text(SAMPLE_CSV)

    result = read_raw_file(fp)
    assert isinstance(result, dd.DataFrame)
    assert "LEASE_KID" in result.columns
    assert "source_file" in result.columns


@pytest.mark.unit
def test_read_raw_file_is_lazy(tmp_path: Path):
    fp = tmp_path / "lp100.txt"
    fp.write_text(SAMPLE_CSV)

    result = read_raw_file(fp)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_read_raw_file_latin1_fallback(tmp_path: Path):
    fp = tmp_path / "latin1.txt"
    # Write latin-1 encoded content with a non-UTF-8 byte
    content = "LEASE KID,LEASE,MONTH-YEAR,PRODUCT,PRODUCTION\n1001,Caf\xe9,3-2021,O,100.0\n"
    fp.write_bytes(content.encode("latin-1"))

    result = read_raw_file(fp)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_read_raw_file_not_found():
    with pytest.raises(FileNotFoundError):
        read_raw_file(Path("/nonexistent/file.txt"))


@pytest.mark.integration
def test_read_raw_file_real():
    raw_files = list(config.RAW_DATA_DIR.glob("*.txt"))
    if not raw_files:
        pytest.skip("No raw files available")
    result = read_raw_file(raw_files[0])
    assert "LEASE_KID" in result.columns
    assert "source_file" in result.columns
    computed = result.compute()
    assert computed.iloc[0]["source_file"] == raw_files[0].name


# ---------------------------------------------------------------------------
# Task 08: concatenate_raw_files
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_concatenate_raw_files_rows(tmp_path: Path):
    for i in range(3):
        fp = tmp_path / f"lp{i}.txt"
        fp.write_text(SAMPLE_CSV)

    files = sorted(tmp_path.glob("*.txt"))
    result = concatenate_raw_files(files)
    assert isinstance(result, dd.DataFrame)
    computed = result.compute()
    # 5 rows per file × 3 files = 15
    assert len(computed) == 15


@pytest.mark.unit
def test_concatenate_raw_files_empty_list():
    with pytest.raises(ValueError, match="empty"):
        concatenate_raw_files([])


@pytest.mark.unit
def test_concatenate_raw_files_skips_malformed(tmp_path: Path):
    good1 = tmp_path / "good1.txt"
    good1.write_text(SAMPLE_CSV)
    good2 = tmp_path / "good2.txt"
    good2.write_text(SAMPLE_CSV)
    bad = tmp_path / "bad.txt"
    # Write a binary file that cannot be parsed as CSV at all
    bad.write_bytes(b"\x00\x01\x02\x03")

    with patch("kgs_pipeline.ingest.read_raw_file") as mock_read:

        def side_effect(fp: Path):
            if "bad" in fp.name:
                raise ValueError("malformed")
            return dd.from_pandas(pd.read_csv(fp, dtype=str), npartitions=1)

        mock_read.side_effect = side_effect
        result = concatenate_raw_files([good1, good2, bad])

    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_concatenate_raw_files_is_lazy(tmp_path: Path):
    fp = tmp_path / "lp1.txt"
    fp.write_text(SAMPLE_CSV)
    result = concatenate_raw_files([fp])
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 09: filter_monthly_records
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_filter_monthly_records_keeps_valid(tmp_path: Path):
    df = pd.DataFrame(
        {
            "LEASE_KID": ["1", "2", "3", "4", "5"],
            "MONTH-YEAR": ["1-2021", "6-2021", "12-2021", "0-2021", "-1-2021"],
            "PRODUCTION": [100.0, 200.0, 300.0, 400.0, 500.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = filter_monthly_records(ddf)
    computed = result.compute()
    assert len(computed) == 3


@pytest.mark.unit
def test_filter_monthly_records_drops_null_month_year():
    df = pd.DataFrame(
        {
            "LEASE_KID": ["1", "2"],
            "MONTH-YEAR": ["3-2021", None],
            "PRODUCTION": [100.0, 200.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = filter_monthly_records(ddf).compute()
    assert len(result) == 1


@pytest.mark.unit
def test_filter_monthly_records_all_non_monthly():
    df = pd.DataFrame(
        {
            "LEASE_KID": ["1", "2"],
            "MONTH-YEAR": ["0-2021", "-1-2021"],
            "PRODUCTION": [100.0, 200.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = filter_monthly_records(ddf).compute()
    assert len(result) == 0


@pytest.mark.unit
def test_filter_monthly_records_return_type():
    df = pd.DataFrame({"LEASE_KID": ["1"], "MONTH-YEAR": ["1-2021"], "PRODUCTION": [100.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(filter_monthly_records(ddf), dd.DataFrame)


@pytest.mark.unit
def test_filter_monthly_records_missing_column():
    df = pd.DataFrame({"LEASE_KID": ["1"], "PRODUCTION": [100.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(KeyError):
        filter_monthly_records(ddf)


# ---------------------------------------------------------------------------
# Task 10: enrich_with_lease_metadata
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_enrich_with_lease_metadata_joins_correctly(tmp_path: Path):
    prod_df = pd.DataFrame(
        {
            "LEASE_KID": ["1001", "1002", "1003"],
            "MONTH-YEAR": ["3-2021", "4-2021", "5-2021"],
            "PRODUCTION": [100.0, 200.0, 300.0],
        }
    )
    meta_file = tmp_path / "meta.txt"
    meta_file.write_text(
        "LEASE KID,OPERATOR,COUNTY,URL\n"
        "1001,Acme Oil,Allen,https://kgs.ku.edu/1001\n"
        "1002,Beta Corp,Butler,https://kgs.ku.edu/1002\n"
        "9999,Other,Chase,https://kgs.ku.edu/9999\n"
    )

    ddf = dd.from_pandas(prod_df, npartitions=1)
    result = enrich_with_lease_metadata(ddf, meta_file).compute()

    assert len(result) == 3
    assert "OPERATOR" in result.columns
    # Unmatched lease 1003 should have NaN operator
    row_1003 = result[result["LEASE_KID"] == "1003"]
    assert row_1003["OPERATOR"].isna().all()


@pytest.mark.unit
def test_enrich_returns_dask_df(tmp_path: Path):
    df = pd.DataFrame({"LEASE_KID": ["1"], "PRODUCTION": [100.0]})
    meta_file = tmp_path / "meta.txt"
    meta_file.write_text("LEASE KID,OPERATOR\n1,Acme\n")
    ddf = dd.from_pandas(df, npartitions=1)
    result = enrich_with_lease_metadata(ddf, meta_file)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_enrich_file_not_found(tmp_path: Path):
    df = pd.DataFrame({"LEASE_KID": ["1"], "PRODUCTION": [100.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(FileNotFoundError):
        enrich_with_lease_metadata(ddf, tmp_path / "nonexistent.txt")


@pytest.mark.unit
def test_enrich_missing_lease_kid_column(tmp_path: Path):
    df = pd.DataFrame({"PRODUCTION": [100.0]})
    meta_file = tmp_path / "meta.txt"
    meta_file.write_text("LEASE KID,OPERATOR\n1,Acme\n")
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(KeyError):
        enrich_with_lease_metadata(ddf, meta_file)


# ---------------------------------------------------------------------------
# Task 11: write_interim_parquet and run_ingest_pipeline
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_interim_parquet_creates_files(tmp_path: Path):
    df = pd.DataFrame(
        {
            "LEASE_KID": ["1001", "1001", "1002", "1002", "1002"],
            "MONTH-YEAR": ["1-2021", "2-2021", "1-2021", "2-2021", "3-2021"],
            "PRODUCTION": [100.0, 110.0, 200.0, 210.0, 220.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    write_interim_parquet(ddf, tmp_path / "interim")
    parquet_files = list((tmp_path / "interim").glob("*.parquet"))
    assert len(parquet_files) > 0
    loaded = pd.read_parquet(parquet_files[0])
    assert loaded is not None


@pytest.mark.unit
def test_run_ingest_pipeline_raises_if_no_files():
    with patch("kgs_pipeline.ingest.discover_raw_files", return_value=[]):
        with pytest.raises(RuntimeError, match="No raw files"):
            run_ingest_pipeline()


@pytest.mark.unit
def test_write_interim_parquet_creates_dir(tmp_path: Path):
    new_dir = tmp_path / "new_interim_dir"
    assert not new_dir.exists()
    df = pd.DataFrame({"LEASE_KID": ["A"], "PRODUCTION": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    write_interim_parquet(ddf, new_dir)
    assert new_dir.exists()


@pytest.mark.unit
def test_run_ingest_pipeline_calls_steps_in_order(tmp_path: Path):
    """Assert run_ingest_pipeline calls each step exactly once in correct order."""
    call_order: list[str] = []

    sample_df = pd.DataFrame(
        {
            "LEASE_KID": ["1001"],
            "MONTH-YEAR": ["3-2021"],
            "PRODUCTION": [100.0],
        }
    )
    sample_ddf = dd.from_pandas(sample_df, npartitions=1)

    with (
        patch(
            "kgs_pipeline.ingest.discover_raw_files", return_value=[tmp_path / "x.txt"]
        ) as mock_disc,
        patch("kgs_pipeline.ingest.concatenate_raw_files", return_value=sample_ddf) as mock_concat,
        patch("kgs_pipeline.ingest.filter_monthly_records", return_value=sample_ddf) as mock_filter,
        patch(
            "kgs_pipeline.ingest.enrich_with_lease_metadata", return_value=sample_ddf
        ) as mock_enrich,
        patch(
            "kgs_pipeline.ingest.write_interim_parquet", return_value=config.INTERIM_DATA_DIR
        ) as mock_write,
    ):

        def track(name: str, fn):
            def wrapper(*args, **kwargs):
                call_order.append(name)
                return fn(*args, **kwargs)

            return wrapper

        mock_disc.side_effect = track("discover", lambda *a, **k: [tmp_path / "x.txt"])
        mock_concat.side_effect = track("concat", lambda *a, **k: sample_ddf)
        mock_filter.side_effect = track("filter", lambda *a, **k: sample_ddf)
        mock_enrich.side_effect = track("enrich", lambda *a, **k: sample_ddf)
        mock_write.side_effect = track("write", lambda *a, **k: config.INTERIM_DATA_DIR)

        run_ingest_pipeline()

    assert call_order == ["discover", "concat", "filter", "enrich", "write"]
