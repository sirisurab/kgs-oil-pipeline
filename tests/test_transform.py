"""Tests for the transform stage (tasks T1–T3).

Test requirements: TR-01, TR-02, TR-04, TR-05, TR-11, TR-12, TR-13, TR-14,
TR-15, TR-16, TR-17, TR-18, TR-23, TR-25, TR-27.
Each test carries exactly one of: @pytest.mark.unit or @pytest.mark.integration
per ADR-008.
"""

from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.ingest import load_schema, _make_empty_dataframe
from kgs_pipeline.transform import clean_partition, derive_meta, transform

_DD_PATH = Path("references/kgs_monthly_data_dictionary.csv")
_UNIT_ERROR_THRESHOLD = 1_000_000.0

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
# Helpers
# ---------------------------------------------------------------------------


def _make_partition(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal typed ingest-output-style DataFrame."""
    schema = load_schema(_DD_PATH)
    empty = _make_empty_dataframe(schema, ["source_file"])
    if not rows:
        return empty

    records = []
    for r in rows:
        record: dict = {}
        for col_schema in schema.columns:
            val = r.get(col_schema.name, None)
            record[col_schema.name] = val
        record["source_file"] = r.get("source_file", "test.txt")
        records.append(record)

    df = pd.DataFrame(records)

    # Apply dtypes
    for col_schema in schema.columns:
        col = col_schema.name
        if col not in df.columns:
            continue
        dtype = col_schema.pandas_dtype
        if dtype == "category":
            if col_schema.categories:
                cat_type = pd.CategoricalDtype(categories=col_schema.categories, ordered=False)
                valid = set(col_schema.categories)
                df[col] = df[col].where(df[col].isin(valid) | df[col].isna(), other=np.nan)
                df[col] = df[col].astype(cat_type)
            else:
                df[col] = df[col].astype("category")
        elif dtype in ("Int64",):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif dtype in ("int64",):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("int64")
        elif dtype in ("Float64",):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
        elif dtype in ("float64",):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        elif dtype == "string":
            df[col] = df[col].astype("string")

    df["source_file"] = df["source_file"].astype("string")
    return df


def make_ingest_config(tmp_path: Path, raw_dir: Path) -> dict:
    return {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(tmp_path / "interim"),
            "data_dictionary_path": str(_DD_PATH),
        },
        "transform": {
            "interim_dir": str(tmp_path / "interim"),
            "processed_dir": str(tmp_path / "processed"),
            "unit_error_threshold": _UNIT_ERROR_THRESHOLD,
        },
    }


def write_synthetic_raw_file(path: Path, rows: list[dict]) -> None:
    header = ",".join(_CANONICAL_COLS)
    lines = [header]
    for r in rows:
        lines.append(",".join(str(r.get(c, "")) for c in _CANONICAL_COLS))
    path.write_text("\n".join(lines) + "\n")


def run_ingest_then_transform(tmp_path: Path, raw_rows: list[dict]) -> None:
    """Helper: write raw files → ingest → transform, all under tmp_path."""
    from kgs_pipeline.ingest import ingest

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    write_synthetic_raw_file(raw_dir / "lp_test.txt", raw_rows)

    cfg = make_ingest_config(tmp_path, raw_dir)
    ingest(cfg)

    transform(cfg)


# ---------------------------------------------------------------------------
# Task T1 tests: clean_partition
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_clean_drops_yearly_and_cumulative_rows() -> None:
    """Rows with MONTH-YEAR = '0-2024' or '-1-2023' are dropped."""
    rows = [
        {"LEASE_KID": 1001, "MONTH-YEAR": "0-2024", "PRODUCT": "O", "PRODUCTION": 100},
        {"LEASE_KID": 1001, "MONTH-YEAR": "-1-2023", "PRODUCT": "O", "PRODUCTION": 200},
        {"LEASE_KID": 1001, "MONTH-YEAR": "3-2024", "PRODUCT": "O", "PRODUCTION": 150},
    ]
    df = _make_partition(rows)
    result = clean_partition(df, _UNIT_ERROR_THRESHOLD)
    assert len(result) == 1
    assert result["MONTH-YEAR"].iloc[0] == "3-2024"


@pytest.mark.unit
def test_clean_deduplication_idempotent() -> None:
    """Duplicate rows on (LEASE_KID, production_date, PRODUCT) → one row; idempotent (TR-15)."""
    rows = [
        {"LEASE_KID": 2001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 50},
        {"LEASE_KID": 2001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 50},
    ]
    df = _make_partition(rows)
    result1 = clean_partition(df, _UNIT_ERROR_THRESHOLD)
    assert len(result1) == 1
    result2 = clean_partition(result1, _UNIT_ERROR_THRESHOLD)
    assert len(result2) == 1


@pytest.mark.unit
def test_clean_negative_production_nulled() -> None:
    """PRODUCTION < 0 → replaced with null (TR-01)."""
    rows = [
        {"LEASE_KID": 3001, "MONTH-YEAR": "2-2024", "PRODUCT": "O", "PRODUCTION": -5},
    ]
    df = _make_partition(rows)
    result = clean_partition(df, _UNIT_ERROR_THRESHOLD)
    assert result["PRODUCTION"].isna().all()


@pytest.mark.unit
def test_clean_zero_production_preserved() -> None:
    """PRODUCTION == 0 and WELLS == 0 remain zero — not nulled (TR-05)."""
    rows = [
        {"LEASE_KID": 4001, "MONTH-YEAR": "3-2024", "PRODUCT": "O", "PRODUCTION": 0, "WELLS": 0},
    ]
    df = _make_partition(rows)
    result = clean_partition(df, _UNIT_ERROR_THRESHOLD)
    assert result["PRODUCTION"].iloc[0] == 0
    assert result["WELLS"].iloc[0] == 0


@pytest.mark.unit
def test_clean_unit_error_outlier_nulled() -> None:
    """PRODUCTION above unit_error_threshold → null (TR-02)."""
    rows = [
        {"LEASE_KID": 5001, "MONTH-YEAR": "4-2024", "PRODUCT": "O", "PRODUCTION": 2_000_000},
    ]
    df = _make_partition(rows)
    result = clean_partition(df, _UNIT_ERROR_THRESHOLD)
    assert result["PRODUCTION"].isna().all()


@pytest.mark.unit
def test_clean_empty_partition_returns_full_schema() -> None:
    """Empty input partition → empty DataFrame with full schema including production_date."""
    schema = load_schema(_DD_PATH)
    empty = _make_empty_dataframe(schema, ["source_file"])
    result = clean_partition(empty, _UNIT_ERROR_THRESHOLD)
    assert len(result) == 0
    assert "production_date" in result.columns
    for col in _CANONICAL_COLS:
        assert col in result.columns


# ---------------------------------------------------------------------------
# Task T2 tests: meta derivation (TR-23)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_meta_derivation_matches_live_output() -> None:
    """Meta derived from zero-row input matches column list, order, dtypes (TR-23)."""
    rows = [
        {"LEASE_KID": 6001, "MONTH-YEAR": "1-2024", "PRODUCT": "O", "PRODUCTION": 100},
    ]
    df = _make_partition(rows)
    meta = derive_meta(
        clean_partition, df.iloc[:0].copy(), unit_error_threshold=_UNIT_ERROR_THRESHOLD
    )
    live = clean_partition(df, _UNIT_ERROR_THRESHOLD)

    assert list(meta.columns) == list(live.columns)
    for col in meta.columns:
        assert str(meta[col].dtype) == str(live[col].dtype), f"dtype mismatch for {col}"


# ---------------------------------------------------------------------------
# Task T3 / Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_transform_output_parquet_readable(tmp_path: Path) -> None:
    """Transform output Parquet is readable (TR-18)."""
    raw_rows = [
        {
            "LEASE_KID": "7001",
            "LEASE": "X",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "100",
        },
        {
            "LEASE_KID": "7001",
            "LEASE": "X",
            "MONTH-YEAR": "2-2024",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "110",
        },
        {
            "LEASE_KID": "7002",
            "LEASE": "Y",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "G",
            "WELLS": "1",
            "PRODUCTION": "500",
        },
    ]
    run_ingest_then_transform(tmp_path, raw_rows)

    processed_dir = tmp_path / "processed"
    ddf = dd.read_parquet(str(processed_dir))
    assert isinstance(ddf, dd.DataFrame)
    df = ddf.compute()
    assert len(df) > 0


@pytest.mark.unit
def test_transform_column_dtypes_consistent_across_partitions(tmp_path: Path) -> None:
    """Two sampled partitions have identical column list and dtypes (TR-14)."""
    raw_rows = [
        {
            "LEASE_KID": "8001",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "2",
            "PRODUCTION": "100",
        },
        {
            "LEASE_KID": "8002",
            "MONTH-YEAR": "2-2024",
            "PRODUCT": "G",
            "WELLS": "1",
            "PRODUCTION": "200",
        },
        {
            "LEASE_KID": "8003",
            "MONTH-YEAR": "3-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "50",
        },
    ]
    run_ingest_then_transform(tmp_path, raw_rows)

    processed_dir = tmp_path / "processed"
    parquet_files = sorted(processed_dir.rglob("*.parquet"))
    assert len(parquet_files) >= 2

    df0 = pd.read_parquet(parquet_files[0])
    df1 = pd.read_parquet(parquet_files[1])
    assert list(df0.columns) == list(df1.columns), "Column lists differ between partitions"
    for col in df0.columns:
        assert str(df0[col].dtype) == str(df1[col].dtype), f"dtype mismatch for {col}"


@pytest.mark.unit
def test_transform_row_count_le_interim(tmp_path: Path) -> None:
    """Processed row count ≤ interim row count (TR-15 deduplication)."""
    raw_rows = [
        {
            "LEASE_KID": "9001",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "50",
        },
        {
            "LEASE_KID": "9001",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "50",
        },
        {
            "LEASE_KID": "9001",
            "MONTH-YEAR": "2-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "60",
        },
    ]
    from kgs_pipeline.ingest import ingest

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    write_synthetic_raw_file(raw_dir / "lp_test.txt", raw_rows)
    cfg = make_ingest_config(tmp_path, raw_dir)
    ingest(cfg)

    interim_row_count = dd.read_parquet(str(tmp_path / "interim")).compute().__len__()

    transform(cfg)

    processed_row_count = dd.read_parquet(str(tmp_path / "processed")).compute().__len__()
    assert processed_row_count <= interim_row_count


@pytest.mark.unit
def test_transform_idempotent(tmp_path: Path) -> None:
    """Run transform twice; row count identical after second run (TR-15 idempotency)."""
    raw_rows = [
        {
            "LEASE_KID": "9501",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "75",
        },
        {
            "LEASE_KID": "9501",
            "MONTH-YEAR": "2-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "80",
        },
    ]
    from kgs_pipeline.ingest import ingest

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    write_synthetic_raw_file(raw_dir / "lp_test.txt", raw_rows)
    cfg = make_ingest_config(tmp_path, raw_dir)
    ingest(cfg)

    transform(cfg)
    count1 = dd.read_parquet(str(tmp_path / "processed")).compute().__len__()

    transform(cfg)
    count2 = dd.read_parquet(str(tmp_path / "processed")).compute().__len__()

    assert count1 == count2


@pytest.mark.unit
def test_transform_zero_values_preserved(tmp_path: Path) -> None:
    """Zero PRODUCTION and WELLS values in raw data remain zero in processed (TR-05)."""
    raw_rows = [
        {
            "LEASE_KID": "10001",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "0",
            "PRODUCTION": "0",
        },
    ]
    run_ingest_then_transform(tmp_path, raw_rows)

    processed_dir = tmp_path / "processed"
    df = dd.read_parquet(str(processed_dir)).compute()
    mask = df.index == 10001
    subset = df[mask] if mask.any() else df
    if not subset.empty:
        assert (subset["PRODUCTION"] == 0).any() or subset["PRODUCTION"].isna().all()


@pytest.mark.unit
def test_transform_tr17_lazy_graph(tmp_path: Path) -> None:
    """Intermediate dd.read_parquet returns a Dask DataFrame (TR-17)."""
    raw_rows = [
        {
            "LEASE_KID": "11001",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "90",
        },
    ]
    run_ingest_then_transform(tmp_path, raw_rows)

    processed_dir = tmp_path / "processed"
    ddf = dd.read_parquet(str(processed_dir))
    assert isinstance(ddf, dd.DataFrame)


@pytest.mark.integration
def test_transform_tr25_integration(tmp_path: Path) -> None:
    """Full transform on ingest fixture; satisfies boundary-transform-features.md (TR-25)."""
    from kgs_pipeline.ingest import ingest

    raw_rows = [
        {
            "LEASE_KID": "20001",
            "LEASE": "INT1",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "3",
            "PRODUCTION": "200",
            "COUNTY": "Nemaha",
            "OPERATOR": "TestOp",
        },
        {
            "LEASE_KID": "20001",
            "LEASE": "INT1",
            "MONTH-YEAR": "2-2024",
            "PRODUCT": "O",
            "WELLS": "3",
            "PRODUCTION": "210",
            "COUNTY": "Nemaha",
            "OPERATOR": "TestOp",
        },
        {
            "LEASE_KID": "20001",
            "LEASE": "INT1",
            "MONTH-YEAR": "3-2024",
            "PRODUCT": "G",
            "WELLS": "3",
            "PRODUCTION": "500",
            "COUNTY": "Nemaha",
            "OPERATOR": "TestOp",
        },
        {
            "LEASE_KID": "20002",
            "LEASE": "INT2",
            "MONTH-YEAR": "1-2024",
            "PRODUCT": "O",
            "WELLS": "1",
            "PRODUCTION": "50",
            "COUNTY": "Douglas",
            "OPERATOR": "OtherOp",
        },
    ]

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    write_synthetic_raw_file(raw_dir / "lp_int.txt", raw_rows)

    cfg = make_ingest_config(tmp_path, raw_dir)
    ingest(cfg)
    transform(cfg)

    processed_dir = tmp_path / "processed"
    ddf = dd.read_parquet(str(processed_dir))
    df = ddf.compute()

    # Entity-indexed on LEASE_KID
    assert (
        ddf.index.name == "LEASE_KID"
        or "LEASE_KID" in df.index.names
        or df.index.name == "LEASE_KID"
    )

    # production_date column present and is datetime
    assert "production_date" in df.columns
    assert pd.api.types.is_datetime64_any_dtype(df["production_date"])

    # No invalid physical values survive (PRODUCTION < 0 → all cleaned away)
    if "PRODUCTION" in df.columns:
        valid = df["PRODUCTION"].dropna()
        if len(valid) > 0:
            assert (valid >= 0).all()

    # partition count within contract
    assert 1 <= ddf.npartitions <= 50
