"""Tests for kgs_pipeline/ingest.py (Tasks I-01 to I-05)."""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.ingest import (
    build_dask_dataframe,
    build_dtype_map,
    ingest,
    load_data_dictionary,
    read_raw_file,
)

DATA_DICT_PATH = "references/kgs_monthly_data_dictionary.csv"

# Canonical columns from data dictionary (in order)
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

_RAW_HEADER = ",".join(_CANONICAL_COLS)

_SAMPLE_ROW_2024 = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "1-2024,O,2,161.8"
)
_SAMPLE_ROW_2025 = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "6-2025,O,2,200.0"
)
_SAMPLE_ROW_2023 = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "3-2023,O,2,100.0"
)
_SAMPLE_ROW_BAD_YEAR = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "-1-1965,O,2,50.0"
)
_SAMPLE_ROW_ZERO_YEAR = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "0-1966,O,2,50.0"
)


def _write_raw_file(path: Path, rows: list[str]) -> str:
    content = _RAW_HEADER + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# Task I-01: load_data_dictionary
# ---------------------------------------------------------------------------


def test_load_data_dictionary_columns() -> None:
    df = load_data_dictionary(DATA_DICT_PATH)
    assert set(df.columns) == {"column", "description", "dtype", "nullable", "categories"}


def test_load_data_dictionary_known_dtypes() -> None:
    df = load_data_dictionary(DATA_DICT_PATH)
    known = {"int", "float", "string", "categorical"}
    for val in df["dtype"]:
        assert val in known, f"Unknown dtype: {val}"


def test_load_data_dictionary_no_null_column_or_dtype() -> None:
    df = load_data_dictionary(DATA_DICT_PATH)
    assert df["column"].notna().all()
    assert df["dtype"].notna().all()
    assert (df["column"] != "").all()
    assert (df["dtype"] != "").all()


# ---------------------------------------------------------------------------
# Task I-02: build_dtype_map
# ---------------------------------------------------------------------------


def test_build_dtype_map_int_not_nullable() -> None:
    data_dict = pd.DataFrame(
        [
            {
                "column": "LEASE_KID",
                "description": "",
                "dtype": "int",
                "nullable": "no",
                "categories": "",
            }
        ]
    )
    mapping = build_dtype_map(data_dict)
    assert mapping["LEASE_KID"] == "int64"


def test_build_dtype_map_int_nullable() -> None:
    data_dict = pd.DataFrame(
        [
            {
                "column": "DOR_CODE",
                "description": "",
                "dtype": "int",
                "nullable": "yes",
                "categories": "",
            }
        ]
    )
    mapping = build_dtype_map(data_dict)
    assert mapping["DOR_CODE"] == pd.Int64Dtype()


def test_build_dtype_map_float() -> None:
    data_dict = pd.DataFrame(
        [
            {
                "column": "PRODUCTION",
                "description": "",
                "dtype": "float",
                "nullable": "yes",
                "categories": "",
            }
        ]
    )
    mapping = build_dtype_map(data_dict)
    assert mapping["PRODUCTION"] == "float64"


def test_build_dtype_map_string() -> None:
    data_dict = pd.DataFrame(
        [
            {
                "column": "LEASE",
                "description": "",
                "dtype": "string",
                "nullable": "yes",
                "categories": "",
            }
        ]
    )
    mapping = build_dtype_map(data_dict)
    assert mapping["LEASE"] == pd.StringDtype()


def test_build_dtype_map_categorical() -> None:
    data_dict = pd.DataFrame(
        [
            {
                "column": "PRODUCT",
                "description": "",
                "dtype": "categorical",
                "nullable": "no",
                "categories": "O|G",
            }
        ]
    )
    mapping = build_dtype_map(data_dict)
    assert mapping["PRODUCT"] == pd.StringDtype()


# ---------------------------------------------------------------------------
# Task I-03: read_raw_file
# ---------------------------------------------------------------------------


def test_read_raw_file_has_expected_columns(tmp_path: Path) -> None:
    fpath = tmp_path / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_2024])
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)
    df = read_raw_file(str(fpath), dtype_map, data_dict)
    assert "source_file" in df.columns
    for col in _CANONICAL_COLS:
        assert col in df.columns


def test_read_raw_file_year_filter(tmp_path: Path) -> None:
    fpath = tmp_path / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_2023, _SAMPLE_ROW_2024, _SAMPLE_ROW_2025])
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)
    df = read_raw_file(str(fpath), dtype_map, data_dict)
    years = df["MONTH-YEAR"].str.split("-").str[-1].astype(int)
    assert (years >= 2024).all()


def test_read_raw_file_drops_non_numeric_year_rows(tmp_path: Path) -> None:
    fpath = tmp_path / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_BAD_YEAR, _SAMPLE_ROW_ZERO_YEAR, _SAMPLE_ROW_2024])
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)
    df = read_raw_file(str(fpath), dtype_map, data_dict)
    # -1-1965 and 0-1966 both have year < 2024 so filtered; only 2024 row remains
    assert len(df) == 1


def test_read_raw_file_missing_required_column_raises(tmp_path: Path) -> None:
    fpath = tmp_path / "lp001.txt"
    # LEASE_KID is nullable=no; omit it
    bad_cols = [c for c in _CANONICAL_COLS if c != "LEASE_KID"]
    bad_header = ",".join(bad_cols)
    row_values = _SAMPLE_ROW_2024.split(",")[1:]  # drop first value
    fpath.write_text(bad_header + "\n" + ",".join(row_values) + "\n", encoding="utf-8")
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)
    with pytest.raises(ValueError, match="LEASE_KID"):
        read_raw_file(str(fpath), dtype_map, data_dict)


def test_read_raw_file_missing_nullable_column_added_as_na(tmp_path: Path) -> None:
    fpath = tmp_path / "lp001.txt"
    # Remove LEASE (nullable=yes) from the file
    cols_without_lease = [c for c in _CANONICAL_COLS if c != "LEASE"]
    header = ",".join(cols_without_lease)
    # Build a row without the LEASE value
    vals = _SAMPLE_ROW_2024.split(",")
    lease_idx = _CANONICAL_COLS.index("LEASE")
    row_vals = vals[:lease_idx] + vals[lease_idx + 1 :]
    fpath.write_text(header + "\n" + ",".join(row_vals) + "\n", encoding="utf-8")
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)
    df = read_raw_file(str(fpath), dtype_map, data_dict)
    assert "LEASE" in df.columns
    assert df["LEASE"].isna().all()


def test_read_raw_file_empty_result_has_correct_schema(tmp_path: Path) -> None:
    fpath = tmp_path / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_2023])  # All rows will be filtered out
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)
    df = read_raw_file(str(fpath), dtype_map, data_dict)
    assert len(df) == 0
    for col in _CANONICAL_COLS:
        assert col in df.columns


# ---------------------------------------------------------------------------
# Task I-04: build_dask_dataframe
# ---------------------------------------------------------------------------


def test_build_dask_dataframe_returns_dask_df(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    for i in range(3):
        fpath = raw_dir / f"lp00{i}.txt"
        _write_raw_file(fpath, [_SAMPLE_ROW_2024])
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)
    ddf = build_dask_dataframe(str(raw_dir), dtype_map, data_dict)
    assert isinstance(ddf, dd.DataFrame)


def test_build_dask_dataframe_correct_columns(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    fpath = raw_dir / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_2024])
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)
    ddf = build_dask_dataframe(str(raw_dir), dtype_map, data_dict)
    assert "source_file" in ddf.columns
    for col in _CANONICAL_COLS:
        assert col in ddf.columns


def test_build_dask_dataframe_does_not_compute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    fpath = raw_dir / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_2024])
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)

    compute_called = [False]
    original_compute = dd.DataFrame.compute

    def spy_compute(self: dd.DataFrame, **kwargs: object) -> pd.DataFrame:
        compute_called[0] = True
        return original_compute(self, **kwargs)

    monkeypatch.setattr(dd.DataFrame, "compute", spy_compute)
    build_dask_dataframe(str(raw_dir), dtype_map, data_dict)
    assert not compute_called[0]


# ---------------------------------------------------------------------------
# Task I-05: ingest (integration)
# ---------------------------------------------------------------------------


def _make_ingest_config(raw_dir: str, interim_dir: str) -> dict:
    return {
        "ingest": {
            "raw_dir": raw_dir,
            "interim_dir": interim_dir,
            "data_dict_path": DATA_DICT_PATH,
        },
        "dask": {
            "scheduler": "local",
            "n_workers": 1,
            "threads_per_worker": 1,
            "memory_limit": "1GB",
            "dashboard_port": 8787,
        },
    }


def test_ingest_writes_parquet(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    fpath = raw_dir / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_2024, _SAMPLE_ROW_2025])
    config = _make_ingest_config(str(raw_dir), str(interim_dir))
    ingest(config)
    parquet_files = list(interim_dir.glob("*.parquet"))
    assert len(parquet_files) >= 1


def test_ingest_parquet_readable(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    fpath = raw_dir / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_2024])
    config = _make_ingest_config(str(raw_dir), str(interim_dir))
    ingest(config)
    df = dd.read_parquet(str(interim_dir)).compute()
    assert len(df) > 0


def test_ingest_expected_columns_present(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    fpath = raw_dir / "lp001.txt"
    _write_raw_file(fpath, [_SAMPLE_ROW_2024])
    config = _make_ingest_config(str(raw_dir), str(interim_dir))
    ingest(config)
    df = dd.read_parquet(str(interim_dir)).compute()
    expected = [
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
    for col in expected:
        assert col in df.columns


@pytest.mark.integration
def test_ingest_boundary_contract(tmp_path: Path) -> None:
    """TR-24: output satisfies boundary-ingest-transform guarantees."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    for i in range(2):
        fpath = raw_dir / f"lp00{i}.txt"
        _write_raw_file(fpath, [_SAMPLE_ROW_2024, _SAMPLE_ROW_2025])
    config = _make_ingest_config(str(raw_dir), str(interim_dir))
    ingest(config)

    df = dd.read_parquet(str(interim_dir)).compute()

    # All data-dictionary columns present
    for col in _CANONICAL_COLS:
        assert col in df.columns, f"Missing column: {col}"

    # LEASE_KID is non-nullable int → must be int64
    assert df["LEASE_KID"].dtype == np.dtype("int64")
    # PRODUCTION is float64
    assert df["PRODUCTION"].dtype == np.dtype("float64")
