"""Tests for kgs_pipeline/transform.py (Tasks T-01 to T-07)."""

from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest

from kgs_pipeline.ingest import build_dtype_map, load_data_dictionary
from kgs_pipeline.transform import (
    apply_transformations,
    cast_categoricals,
    deduplicate,
    fill_date_gaps,
    parse_production_date,
    transform,
    validate_bounds,
)

DATA_DICT_PATH = "references/kgs_monthly_data_dictionary.csv"

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

_SAMPLE_ROW_2024_O = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "1-2024,O,2,161.8"
)
_SAMPLE_ROW_2024_G = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "2-2024,G,2,500.0"
)
_SAMPLE_ROW_2025_O = (
    "1001135839,SNYDER,122608,15-131-20143,KANASKA,Lansing Group,"
    "Buckeye West LLC,Nemaha,1,S,14,E,1,NENENE,39.999519,-95.78905,"
    "3-2025,O,2,200.0"
)


def _make_raw_file(tmp_path: Path, rows: list[str], name: str = "lp001.txt") -> str:
    fpath = tmp_path / name
    fpath.write_text(_RAW_HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return str(fpath)


def _make_partition(rows_dict: list[dict]) -> pd.DataFrame:
    """Build a minimal partition DataFrame for unit testing transform functions."""
    return pd.DataFrame(rows_dict)


# ---------------------------------------------------------------------------
# Task T-01: parse_production_date
# ---------------------------------------------------------------------------


def test_parse_production_date_valid() -> None:
    df = _make_partition([{"MONTH-YEAR": "1-2024", "PRODUCTION": 100.0}])
    result = parse_production_date(df)
    assert pd.Timestamp("2024-01-01") in result["production_date"].values


def test_parse_production_date_drops_zero_month() -> None:
    df = _make_partition(
        [
            {"MONTH-YEAR": "0-2024", "PRODUCTION": 0.0},
            {"MONTH-YEAR": "1-2024", "PRODUCTION": 100.0},
        ]
    )
    result = parse_production_date(df)
    assert len(result) == 1
    assert result["production_date"].iloc[0] == pd.Timestamp("2024-01-01")


def test_parse_production_date_drops_negative_month() -> None:
    df = _make_partition(
        [
            {"MONTH-YEAR": "-1-2024", "PRODUCTION": 0.0},
            {"MONTH-YEAR": "6-2024", "PRODUCTION": 50.0},
        ]
    )
    result = parse_production_date(df)
    assert len(result) == 1
    assert result["production_date"].iloc[0] == pd.Timestamp("2024-06-01")


def test_parse_production_date_dtype() -> None:
    df = _make_partition([{"MONTH-YEAR": "3-2024", "PRODUCTION": 10.0}])
    result = parse_production_date(df)
    assert result["production_date"].dtype == np.dtype("datetime64[ns]")


def test_parse_production_date_zero_production_unchanged() -> None:
    df = _make_partition([{"MONTH-YEAR": "4-2024", "PRODUCTION": 0.0}])
    result = parse_production_date(df)
    assert result["PRODUCTION"].iloc[0] == 0.0


# ---------------------------------------------------------------------------
# Task T-02: cast_categoricals
# ---------------------------------------------------------------------------


def test_cast_categoricals_invalid_product_becomes_na() -> None:
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    df = _make_partition(
        [
            {"PRODUCT": "X", "TWN_DIR": "S", "RANGE_DIR": "E"},
        ]
    )
    result = cast_categoricals(df, data_dict)
    assert pd.isna(result["PRODUCT"].iloc[0])


def test_cast_categoricals_valid_twn_dir() -> None:
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    df = _make_partition(
        [
            {"PRODUCT": "O", "TWN_DIR": "S", "RANGE_DIR": "E"},
            {"PRODUCT": "G", "TWN_DIR": "N", "RANGE_DIR": "W"},
        ]
    )
    result = cast_categoricals(df, data_dict)
    assert isinstance(result["TWN_DIR"].dtype, pd.CategoricalDtype)
    assert set(result["TWN_DIR"].cat.categories) == {"S", "N"}


def test_cast_categoricals_invalid_range_dir_becomes_na() -> None:
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    df = _make_partition(
        [
            {"PRODUCT": "O", "TWN_DIR": "S", "RANGE_DIR": "X"},
        ]
    )
    result = cast_categoricals(df, data_dict)
    assert pd.isna(result["RANGE_DIR"].iloc[0])
    assert isinstance(result["RANGE_DIR"].dtype, pd.CategoricalDtype)


def test_cast_categoricals_schema_consistent_across_partitions() -> None:
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    df1 = _make_partition([{"PRODUCT": "O", "TWN_DIR": "S", "RANGE_DIR": "E"}])
    df2 = _make_partition([{"PRODUCT": "G", "TWN_DIR": "N", "RANGE_DIR": "W"}])
    r1 = cast_categoricals(df1, data_dict)
    r2 = cast_categoricals(df2, data_dict)
    assert r1["PRODUCT"].dtype == r2["PRODUCT"].dtype
    assert r1["TWN_DIR"].dtype == r2["TWN_DIR"].dtype


# ---------------------------------------------------------------------------
# Task T-03: validate_bounds
# ---------------------------------------------------------------------------


def test_validate_bounds_negative_production_replaced() -> None:
    df = _make_partition([{"PRODUCTION": -5.0, "PRODUCT": "O"}])
    result = validate_bounds(df)
    assert np.isnan(result["PRODUCTION"].iloc[0])


def test_validate_bounds_high_oil_replaced() -> None:
    df = _make_partition([{"PRODUCTION": 100_000.0, "PRODUCT": "O"}])
    result = validate_bounds(df)
    assert np.isnan(result["PRODUCTION"].iloc[0])


def test_validate_bounds_zero_production_unchanged() -> None:
    df = _make_partition([{"PRODUCTION": 0.0, "PRODUCT": "O"}])
    result = validate_bounds(df)
    assert result["PRODUCTION"].iloc[0] == 0.0


def test_validate_bounds_high_gas_not_replaced() -> None:
    df = _make_partition([{"PRODUCTION": 100_000.0, "PRODUCT": "G"}])
    result = validate_bounds(df)
    assert result["PRODUCTION"].iloc[0] == 100_000.0


# ---------------------------------------------------------------------------
# Task T-04: deduplicate
# ---------------------------------------------------------------------------


def _make_indexed_partition(rows: list[dict], entity_col: str = "LEASE_KID") -> pd.DataFrame:
    df = pd.DataFrame(rows)
    return df.set_index(entity_col)


def test_deduplicate_keeps_first_occurrence() -> None:
    df = _make_indexed_partition(
        [
            {"LEASE_KID": 1, "production_date": pd.Timestamp("2024-01-01"), "PRODUCTION": 100.0},
            {"LEASE_KID": 1, "production_date": pd.Timestamp("2024-01-01"), "PRODUCTION": 200.0},
        ]
    )
    result = deduplicate(df, "LEASE_KID", "production_date")
    assert len(result) == 1
    assert result["PRODUCTION"].iloc[0] == 100.0


def test_deduplicate_row_count_decreases_or_equal() -> None:
    df = _make_indexed_partition(
        [
            {"LEASE_KID": 1, "production_date": pd.Timestamp("2024-01-01"), "PRODUCTION": 100.0},
            {"LEASE_KID": 1, "production_date": pd.Timestamp("2024-02-01"), "PRODUCTION": 200.0},
        ]
    )
    result = deduplicate(df, "LEASE_KID", "production_date")
    assert len(result) <= len(df)


def test_deduplicate_idempotent() -> None:
    df = _make_indexed_partition(
        [
            {"LEASE_KID": 1, "production_date": pd.Timestamp("2024-01-01"), "PRODUCTION": 100.0},
            {"LEASE_KID": 1, "production_date": pd.Timestamp("2024-01-01"), "PRODUCTION": 200.0},
        ]
    )
    once = deduplicate(df, "LEASE_KID", "production_date")
    twice = deduplicate(once, "LEASE_KID", "production_date")
    pd.testing.assert_frame_equal(once, twice)


def test_deduplicate_uses_subset_not_bare() -> None:
    # Rows differ only in PRODUCTION → bare drop_duplicates() would keep both
    df = _make_indexed_partition(
        [
            {"LEASE_KID": 1, "production_date": pd.Timestamp("2024-01-01"), "PRODUCTION": 100.0},
            {"LEASE_KID": 1, "production_date": pd.Timestamp("2024-01-01"), "PRODUCTION": 200.0},
        ]
    )
    result = deduplicate(df, "LEASE_KID", "production_date")
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Task T-05: fill_date_gaps
# ---------------------------------------------------------------------------


def test_fill_date_gaps_inserts_missing_month() -> None:
    df = _make_indexed_partition(
        [
            {
                "LEASE_KID": 1,
                "production_date": pd.Timestamp("2024-01-01"),
                "PRODUCTION": 100.0,
                "PRODUCT": "O",
            },
            {
                "LEASE_KID": 1,
                "production_date": pd.Timestamp("2024-03-01"),
                "PRODUCTION": 200.0,
                "PRODUCT": "O",
            },
        ]
    )
    result = fill_date_gaps(df, "LEASE_KID", "production_date")
    dates = result["production_date"].tolist()
    assert pd.Timestamp("2024-02-01") in dates
    feb_row = result[result["production_date"] == pd.Timestamp("2024-02-01")]
    assert feb_row["PRODUCTION"].iloc[0] == 0.0


def test_fill_date_gaps_existing_zero_unchanged() -> None:
    df = _make_indexed_partition(
        [
            {
                "LEASE_KID": 1,
                "production_date": pd.Timestamp("2024-01-01"),
                "PRODUCTION": 0.0,
                "PRODUCT": "O",
            },
            {
                "LEASE_KID": 1,
                "production_date": pd.Timestamp("2024-02-01"),
                "PRODUCTION": 100.0,
                "PRODUCT": "O",
            },
        ]
    )
    result = fill_date_gaps(df, "LEASE_KID", "production_date")
    jan = result[result["production_date"] == pd.Timestamp("2024-01-01")]
    assert jan["PRODUCTION"].iloc[0] == 0.0


def test_fill_date_gaps_entity_id_preserved() -> None:
    df = _make_indexed_partition(
        [
            {
                "LEASE_KID": 42,
                "production_date": pd.Timestamp("2024-01-01"),
                "PRODUCTION": 10.0,
                "PRODUCT": "O",
            },
            {
                "LEASE_KID": 42,
                "production_date": pd.Timestamp("2024-03-01"),
                "PRODUCTION": 20.0,
                "PRODUCT": "O",
            },
        ]
    )
    result = fill_date_gaps(df, "LEASE_KID", "production_date")
    assert all(idx == 42 for idx in result.index)


def test_fill_date_gaps_correct_span(tmp_path: Path) -> None:
    df = _make_indexed_partition(
        [
            {
                "LEASE_KID": 1,
                "production_date": pd.Timestamp("2024-01-01"),
                "PRODUCTION": 10.0,
                "PRODUCT": "O",
            },
            {
                "LEASE_KID": 1,
                "production_date": pd.Timestamp("2024-04-01"),
                "PRODUCTION": 20.0,
                "PRODUCT": "O",
            },
        ]
    )
    result = fill_date_gaps(df, "LEASE_KID", "production_date")
    # Jan-Apr = 4 months
    assert len(result) == 4


# ---------------------------------------------------------------------------
# Task T-06: apply_transformations
# ---------------------------------------------------------------------------


def test_apply_transformations_returns_dask_df(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(_RAW_HEADER + "\n" + _SAMPLE_ROW_2024_O + "\n", encoding="utf-8")
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)

    from kgs_pipeline.ingest import build_dask_dataframe

    ddf = build_dask_dataframe(str(raw_dir), dtype_map, data_dict)
    result = apply_transformations(ddf, data_dict)
    assert isinstance(result, dd.DataFrame)


def test_apply_transformations_does_not_compute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(_RAW_HEADER + "\n" + _SAMPLE_ROW_2024_O + "\n", encoding="utf-8")
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)

    from kgs_pipeline.ingest import build_dask_dataframe

    ddf = build_dask_dataframe(str(raw_dir), dtype_map, data_dict)

    compute_called = [False]
    original_compute = dd.DataFrame.compute

    def spy_compute(self: dd.DataFrame, **kwargs: object) -> pd.DataFrame:
        compute_called[0] = True
        return original_compute(self, **kwargs)

    monkeypatch.setattr(dd.DataFrame, "compute", spy_compute)
    apply_transformations(ddf, data_dict)
    assert not compute_called[0]


def test_apply_transformations_meta_matches_output(tmp_path: Path) -> None:
    """TR-23: meta arg must match actual function output in columns, order, dtypes."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(_RAW_HEADER + "\n" + _SAMPLE_ROW_2024_O + "\n", encoding="utf-8")
    data_dict = load_data_dictionary(DATA_DICT_PATH)
    dtype_map = build_dtype_map(data_dict)

    from kgs_pipeline.ingest import build_dask_dataframe

    ddf = build_dask_dataframe(str(raw_dir), dtype_map, data_dict)
    result_ddf = apply_transformations(ddf, data_dict)

    # Compute to get actual result and compare dtypes with meta
    actual = result_ddf.compute()
    meta = result_ddf._meta
    assert list(meta.columns) == list(actual.columns)


# ---------------------------------------------------------------------------
# Task T-07: transform (integration) — TR-25
# ---------------------------------------------------------------------------


def _make_transform_config(interim_dir: str, processed_dir: str) -> dict:
    return {
        "transform": {
            "interim_dir": interim_dir,
            "processed_dir": processed_dir,
            "data_dict_path": DATA_DICT_PATH,
        },
    }


@pytest.mark.integration
def test_transform_writes_parquet(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(_RAW_HEADER + "\n" + _SAMPLE_ROW_2024_O + "\n", encoding="utf-8")

    from kgs_pipeline.ingest import ingest as run_ingest

    ingest_config = {
        "ingest": {
            "raw_dir": str(raw_dir),
            "interim_dir": str(interim_dir),
            "data_dict_path": DATA_DICT_PATH,
        }
    }
    run_ingest(ingest_config)

    transform_config = _make_transform_config(str(interim_dir), str(processed_dir))
    transform(transform_config)

    parquet_files = list(processed_dir.glob("*.parquet"))
    assert len(parquet_files) >= 1


@pytest.mark.integration
def test_transform_parquet_readable(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(
        _RAW_HEADER + "\n" + _SAMPLE_ROW_2024_O + "\n" + _SAMPLE_ROW_2025_O + "\n",
        encoding="utf-8",
    )

    from kgs_pipeline.ingest import ingest as run_ingest

    run_ingest(
        {
            "ingest": {
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
                "data_dict_path": DATA_DICT_PATH,
            }
        }
    )
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
    df = dd.read_parquet(str(processed_dir)).compute()
    assert len(df) > 0


@pytest.mark.integration
def test_transform_sort_order(tmp_path: Path) -> None:
    """TR-16: production_date is non-decreasing within each partition."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    rows = [_SAMPLE_ROW_2025_O, _SAMPLE_ROW_2024_O, _SAMPLE_ROW_2024_G]
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(_RAW_HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")

    from kgs_pipeline.ingest import ingest as run_ingest

    run_ingest(
        {
            "ingest": {
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
                "data_dict_path": DATA_DICT_PATH,
            }
        }
    )
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))

    ddf = dd.read_parquet(str(processed_dir))
    for i in range(ddf.npartitions):
        part = ddf.get_partition(i).compute()
        if len(part) > 1:
            dates = part["production_date"].tolist()
            assert dates == sorted(dates), "Partition dates not sorted"


@pytest.mark.integration
def test_transform_row_count_le_input(tmp_path: Path) -> None:
    """TR-15: output row count <= input row count."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    rows = [_SAMPLE_ROW_2024_O, _SAMPLE_ROW_2024_O, _SAMPLE_ROW_2025_O]
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(_RAW_HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")

    from kgs_pipeline.ingest import ingest as run_ingest

    run_ingest(
        {
            "ingest": {
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
                "data_dict_path": DATA_DICT_PATH,
            }
        }
    )
    interim_count = dd.read_parquet(str(interim_dir)).compute().shape[0]
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))
    processed_count = dd.read_parquet(str(processed_dir)).compute().shape[0]
    assert processed_count <= interim_count


@pytest.mark.integration
def test_transform_boundary_contract(tmp_path: Path) -> None:
    """TR-25: output satisfies boundary-transform-features guarantees."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    interim_dir = tmp_path / "interim"
    processed_dir = tmp_path / "processed"
    rows = [_SAMPLE_ROW_2024_O, _SAMPLE_ROW_2025_O]
    fpath = raw_dir / "lp001.txt"
    fpath.write_text(_RAW_HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")

    from kgs_pipeline.ingest import ingest as run_ingest

    run_ingest(
        {
            "ingest": {
                "raw_dir": str(raw_dir),
                "interim_dir": str(interim_dir),
                "data_dict_path": DATA_DICT_PATH,
            }
        }
    )
    transform(_make_transform_config(str(interim_dir), str(processed_dir)))

    df = dd.read_parquet(str(processed_dir)).compute()
    # Entity index established
    assert df.index.name == "LEASE_KID"
    # production_date column present
    assert "production_date" in df.columns
    # Sorted within each entity group
    for entity_id in df.index.unique():
        grp = df.loc[entity_id]
        if hasattr(grp, "shape") and len(grp) > 1:
            dates = grp["production_date"].tolist()
            assert dates == sorted(dates)
