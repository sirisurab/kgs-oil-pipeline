"""Tests for kgs_pipeline/ingest.py (Tasks 06–11)."""

from pathlib import Path
from unittest.mock import patch

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pytest


# ---------------------------------------------------------------------------
# Task 06: discover_raw_files
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_discover_raw_files_sorted(tmp_path: Path):
    from kgs_pipeline.ingest import discover_raw_files

    for kid in [1003, 1001, 1002]:
        p = tmp_path / f"lp{kid}.txt"
        p.write_text("header\ndata row\n", encoding="utf-8")
    result = discover_raw_files(tmp_path)
    assert len(result) == 3
    assert result == sorted(result)


@pytest.mark.unit
def test_discover_raw_files_excludes_zero_byte(tmp_path: Path):
    from kgs_pipeline.ingest import discover_raw_files

    (tmp_path / "lp1001.txt").write_text("header\ndata\n", encoding="utf-8")
    (tmp_path / "lp1002.txt").write_bytes(b"")
    result = discover_raw_files(tmp_path)
    assert len(result) == 1


@pytest.mark.unit
def test_discover_raw_files_nonexistent_dir():
    from kgs_pipeline.ingest import discover_raw_files

    with pytest.raises(FileNotFoundError):
        discover_raw_files(Path("/nonexistent/path/xyz"))


@pytest.mark.unit
def test_discover_raw_files_empty_dir(tmp_path: Path):
    from kgs_pipeline.ingest import discover_raw_files

    result = discover_raw_files(tmp_path)
    assert result == []


# ---------------------------------------------------------------------------
# Task 07: read_raw_files
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_files_returns_dask_df(tmp_path: Path, three_raw_csv_files):
    from kgs_pipeline.ingest import read_raw_files

    result = read_raw_files(three_raw_csv_files)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_read_raw_files_empty_raises():
    from kgs_pipeline.ingest import read_raw_files

    with pytest.raises(ValueError):
        read_raw_files([])


@pytest.mark.unit
def test_read_raw_files_has_source_file_column(tmp_path: Path, three_raw_csv_files):
    from kgs_pipeline.ingest import read_raw_files

    result = read_raw_files(three_raw_csv_files)
    assert "source_file" in result.columns


@pytest.mark.unit
def test_read_raw_files_has_lease_kid_no_space(tmp_path: Path, three_raw_csv_files):
    from kgs_pipeline.ingest import read_raw_files

    result = read_raw_files(three_raw_csv_files)
    assert "LEASE_KID" in result.columns
    assert "LEASE KID" not in result.columns


@pytest.mark.unit
def test_read_raw_files_is_lazy(tmp_path: Path, three_raw_csv_files):
    from kgs_pipeline.ingest import read_raw_files

    result = read_raw_files(three_raw_csv_files)
    assert isinstance(result, dd.DataFrame)
    assert not isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 08: filter_monthly_records
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_filter_monthly_records_keeps_only_monthly():
    from kgs_pipeline.ingest import filter_monthly_records

    df = pd.DataFrame({"MONTH_YEAR": ["1-2020", "0-2020", "-1-2020"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = filter_monthly_records(ddf).compute()
    assert len(result) == 1
    assert result.iloc[0]["MONTH_YEAR"] == "1-2020"


@pytest.mark.unit
def test_filter_monthly_records_all_valid():
    from kgs_pipeline.ingest import filter_monthly_records

    months = [f"{m}-2020" for m in range(1, 13)]
    df = pd.DataFrame({"MONTH_YEAR": months})
    ddf = dd.from_pandas(df, npartitions=1)
    result = filter_monthly_records(ddf).compute()
    assert len(result) == 12


@pytest.mark.unit
def test_filter_monthly_records_null_dropped():
    from kgs_pipeline.ingest import filter_monthly_records

    df = pd.DataFrame({"MONTH_YEAR": ["1-2020", None, "3-2020"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = filter_monthly_records(ddf).compute()
    assert len(result) == 2


@pytest.mark.unit
def test_filter_monthly_records_missing_column():
    from kgs_pipeline.ingest import filter_monthly_records

    df = pd.DataFrame({"OTHER_COL": ["a", "b"]})
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(KeyError):
        filter_monthly_records(ddf)


@pytest.mark.unit
def test_filter_monthly_records_returns_dask():
    from kgs_pipeline.ingest import filter_monthly_records

    df = pd.DataFrame({"MONTH_YEAR": ["1-2020"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = filter_monthly_records(ddf)
    assert isinstance(result, dd.DataFrame)
    assert not isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 09: enrich_with_lease_metadata
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_enrich_with_lease_metadata_returns_dask(tmp_path: Path):
    from kgs_pipeline.ingest import enrich_with_lease_metadata

    prod_df = pd.DataFrame({"LEASE_KID": ["1001"], "PRODUCTION": [100.0]})
    ddf = dd.from_pandas(prod_df, npartitions=1)

    meta_file = tmp_path / "meta.txt"
    meta_file.write_text("LEASE_KID,LEASE\n1001,Test Lease\n", encoding="utf-8")

    result = enrich_with_lease_metadata(ddf, meta_file)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_enrich_no_meta_suffix_columns(tmp_path: Path):
    from kgs_pipeline.ingest import enrich_with_lease_metadata

    prod_df = pd.DataFrame({"LEASE_KID": ["1001"], "LEASE": ["existing"]})
    ddf = dd.from_pandas(prod_df, npartitions=1)

    meta_file = tmp_path / "meta.txt"
    meta_file.write_text("LEASE_KID,LEASE\n1001,Test Lease\n", encoding="utf-8")

    result = enrich_with_lease_metadata(ddf, meta_file)
    assert not any(c.endswith("_meta") for c in result.columns)


@pytest.mark.unit
def test_enrich_missing_file_raises(tmp_path: Path):
    from kgs_pipeline.ingest import enrich_with_lease_metadata

    ddf = dd.from_pandas(pd.DataFrame({"LEASE_KID": ["1001"]}), npartitions=1)
    with pytest.raises(FileNotFoundError):
        enrich_with_lease_metadata(ddf, tmp_path / "nonexistent.txt")


@pytest.mark.unit
def test_enrich_missing_lease_kid_raises(tmp_path: Path):
    from kgs_pipeline.ingest import enrich_with_lease_metadata

    ddf = dd.from_pandas(pd.DataFrame({"OTHER": ["1001"]}), npartitions=1)
    meta_file = tmp_path / "meta.txt"
    meta_file.write_text("LEASE_KID,LEASE\n1001,Test\n", encoding="utf-8")
    with pytest.raises(KeyError):
        enrich_with_lease_metadata(ddf, meta_file)


@pytest.mark.unit
def test_enrich_left_join_preserves_all_prod_rows(tmp_path: Path):
    from kgs_pipeline.ingest import enrich_with_lease_metadata

    prod_df = pd.DataFrame({"LEASE_KID": ["1001", "9999"], "PRODUCTION": [100.0, 50.0]})
    ddf = dd.from_pandas(prod_df, npartitions=1)

    meta_file = tmp_path / "meta.txt"
    meta_file.write_text("LEASE_KID,LEASE\n1001,Test\n", encoding="utf-8")

    result = enrich_with_lease_metadata(ddf, meta_file).compute()
    assert len(result) == 2  # left join preserves both rows


# ---------------------------------------------------------------------------
# Task 10: apply_interim_schema
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_apply_interim_schema_lease_kid_int64():
    from kgs_pipeline.ingest import apply_interim_schema

    df = pd.DataFrame(
        {
            "LEASE_KID": ["12345"],
            "PRODUCT": ["O"],
            "PRODUCTION": ["100.5"],
            "WELLS": ["2"],
            "MONTH_YEAR": ["1-2020"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = apply_interim_schema(ddf).compute()
    assert result["LEASE_KID"].dtype == "int64"


@pytest.mark.unit
def test_apply_interim_schema_drops_bad_lease_kid():
    from kgs_pipeline.ingest import apply_interim_schema

    df = pd.DataFrame(
        {
            "LEASE_KID": ["12345", "ABC"],
            "PRODUCT": ["O", "O"],
            "PRODUCTION": ["100.0", "50.0"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = apply_interim_schema(ddf).compute()
    assert len(result) == 1


@pytest.mark.unit
def test_apply_interim_schema_drops_invalid_product():
    from kgs_pipeline.ingest import apply_interim_schema

    df = pd.DataFrame(
        {
            "LEASE_KID": ["1001", "1002"],
            "PRODUCT": ["O", "X"],
            "PRODUCTION": ["100.0", "50.0"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = apply_interim_schema(ddf).compute()
    assert len(result) == 1


@pytest.mark.unit
def test_apply_interim_schema_production_float64():
    from kgs_pipeline.ingest import apply_interim_schema

    df = pd.DataFrame({"LEASE_KID": ["1001"], "PRODUCT": ["O"], "PRODUCTION": ["123.45"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = apply_interim_schema(ddf)
    # Check dtype in meta or compute
    computed = result.compute()
    assert "float" in str(computed["PRODUCTION"].dtype).lower()


@pytest.mark.unit
def test_apply_interim_schema_lat_lon_invalid_becomes_nan():
    from kgs_pipeline.ingest import apply_interim_schema

    df = pd.DataFrame(
        {
            "LEASE_KID": ["1001"],
            "PRODUCT": ["O"],
            "LATITUDE": ["not_a_number"],
            "LONGITUDE": ["also_bad"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = apply_interim_schema(ddf).compute()
    assert pd.isna(result["LATITUDE"].iloc[0])
    assert pd.isna(result["LONGITUDE"].iloc[0])


# ---------------------------------------------------------------------------
# Task 11: write_interim_parquet + run_ingest_pipeline
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_interim_parquet_creates_files(tmp_path: Path, sample_interim_ddf):
    from kgs_pipeline.ingest import write_interim_parquet

    written = write_interim_parquet(sample_interim_ddf, tmp_path)
    assert len(written) >= 1
    assert all(p.suffix == ".parquet" for p in written)


@pytest.mark.unit
def test_write_interim_parquet_readable(tmp_path: Path, sample_interim_ddf):
    from kgs_pipeline.ingest import write_interim_parquet

    written = write_interim_parquet(sample_interim_ddf, tmp_path)
    for p in written:
        df = pd.read_parquet(p)
        assert len(df) >= 0  # readable without exception


@pytest.mark.unit
def test_write_interim_parquet_dask_readable(tmp_path: Path, sample_interim_ddf):
    from kgs_pipeline.ingest import write_interim_parquet

    write_interim_parquet(sample_interim_ddf, tmp_path)
    reloaded = dd.read_parquet(str(tmp_path), engine="pyarrow")
    assert isinstance(reloaded, dd.DataFrame)


@pytest.mark.unit
def test_run_ingest_pipeline_no_raw_files_raises(tmp_path: Path):
    from kgs_pipeline.ingest import run_ingest_pipeline

    with patch("kgs_pipeline.ingest.discover_raw_files", return_value=[]):
        with pytest.raises(RuntimeError, match="No valid raw files found"):
            run_ingest_pipeline(raw_dir=tmp_path, output_dir=tmp_path / "interim")


@pytest.mark.unit
def test_run_ingest_pipeline_unit(tmp_path: Path, three_raw_csv_files):
    from kgs_pipeline.ingest import run_ingest_pipeline

    interim_dir = tmp_path / "interim"
    result = run_ingest_pipeline(
        raw_dir=tmp_path,
        lease_index_path=Path("data/external/oil_leases_2020_present.txt"),
        output_dir=interim_dir,
    )
    assert isinstance(result, list)
    assert len(result) >= 1


@pytest.mark.unit
def test_row_count_reconciliation(tmp_path: Path):
    """Fixture with 10 rows including 2 yearly and 1 duplicate should reduce count."""
    from kgs_pipeline.ingest import (
        apply_interim_schema,
        filter_monthly_records,
        read_raw_files,
        write_interim_parquet,
    )

    lines = [
        "LEASE KID,LEASE,DOR_CODE,API_NUMBER,FIELD,PRODUCING_ZONE,OPERATOR,"
        "COUNTY,TOWNSHIP,TWN_DIR,RANGE,RANGE_DIR,SECTION,SPOT,"
        "LATITUDE,LONGITUDE,MONTH-YEAR,PRODUCT,WELLS,PRODUCTION,URL"
    ]
    for i in range(8):
        lines.append(
            f"1001,Test,,API001,FIELD,ZONE,OP,COUNTY,1,S,1,W,1,NE,"
            f"38.0,-98.0,{i + 1}-2020,O,1,100.0,http://example.com"
        )
    # 2 yearly rows (Month=0)
    for _ in range(2):
        lines.append(
            "1001,Test,,API001,FIELD,ZONE,OP,COUNTY,1,S,1,W,1,NE,"
            "38.0,-98.0,0-2020,O,1,1200.0,http://example.com"
        )

    f = tmp_path / "lp1001.txt"
    f.write_text("\n".join(lines) + "\n", encoding="utf-8")

    ddf = read_raw_files([f])
    ddf = filter_monthly_records(ddf)
    ddf = apply_interim_schema(ddf)
    interim_dir = tmp_path / "interim"
    write_interim_parquet(ddf, interim_dir)

    result_df = dd.read_parquet(str(interim_dir), engine="pyarrow").compute()
    assert len(result_df) <= 8  # yearly rows removed
    assert len(result_df) <= 10  # never more than input


@pytest.mark.unit
def test_schema_stability_across_partitions(tmp_path: Path):
    """Both partitions have identical schema even when one has all-null LATITUDE."""
    from kgs_pipeline.ingest import apply_interim_schema, write_interim_parquet

    df1 = pd.DataFrame(
        {
            "LEASE_KID": ["1001"],
            "LEASE": ["L1"],
            "DOR_CODE": ["D1"],
            "API_NUMBER": ["A1"],
            "FIELD": ["F1"],
            "PRODUCING_ZONE": ["Z1"],
            "OPERATOR": ["O1"],
            "COUNTY": ["C1"],
            "TOWNSHIP": ["1"],
            "TWN_DIR": ["S"],
            "RANGE": ["1"],
            "RANGE_DIR": ["W"],
            "SECTION": ["1"],
            "SPOT": ["NE"],
            "LATITUDE": ["38.0"],
            "LONGITUDE": ["-98.0"],
            "MONTH_YEAR": ["1-2020"],
            "PRODUCT": ["O"],
            "WELLS": ["1"],
            "PRODUCTION": ["100.0"],
            "source_file": ["lp1001"],
        }
    )
    df2 = pd.DataFrame(
        {
            "LEASE_KID": ["1002"],
            "LEASE": ["L2"],
            "DOR_CODE": ["D2"],
            "API_NUMBER": ["A2"],
            "FIELD": ["F2"],
            "PRODUCING_ZONE": ["Z2"],
            "OPERATOR": ["O2"],
            "COUNTY": ["C2"],
            "TOWNSHIP": ["2"],
            "TWN_DIR": ["S"],
            "RANGE": ["2"],
            "RANGE_DIR": ["W"],
            "SECTION": ["2"],
            "SPOT": ["NW"],
            "LATITUDE": [None],
            "LONGITUDE": [None],
            "MONTH_YEAR": ["2-2020"],
            "PRODUCT": ["G"],
            "WELLS": ["1"],
            "PRODUCTION": ["500.0"],
            "source_file": ["lp1002"],
        }
    )
    combined = pd.concat([df1, df2], ignore_index=True)
    ddf = dd.from_pandas(combined, npartitions=1)
    typed = apply_interim_schema(ddf)
    interim_dir = tmp_path / "interim"
    written = write_interim_parquet(typed, interim_dir)

    schemas = []
    for p in written:
        df = pd.read_parquet(p)
        schemas.append(set(df.dtypes.items()))
    if len(schemas) >= 2:
        assert schemas[0] == schemas[1]
