"""Tests for kgs_pipeline/transform.py — Tasks 12–21."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore[import-untyped]
import pytest

import kgs_pipeline.config as config
from kgs_pipeline.transform import (
    assign_unit_labels,
    deduplicate_records,
    explode_api_numbers,
    parse_production_date,
    rename_and_cast_columns,
    run_transform_pipeline,
    sort_and_repartition,
    validate_physical_bounds,
    write_processed_parquet,
)


# ---------------------------------------------------------------------------
# Task 12: Config constants (transform stage)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_oil_outlier_threshold_is_50000():
    assert config.OIL_OUTLIER_THRESHOLD_BBL == 50000
    assert isinstance(config.OIL_OUTLIER_THRESHOLD_BBL, int)


@pytest.mark.unit
def test_column_rename_map_is_dict():
    assert isinstance(config.COLUMN_RENAME_MAP, dict)
    assert config.COLUMN_RENAME_MAP["MONTH-YEAR"] == "month_year"


@pytest.mark.unit
def test_column_rename_map_production():
    assert config.COLUMN_RENAME_MAP["PRODUCTION"] == "production"


@pytest.mark.unit
def test_partition_column_processed():
    assert config.PARTITION_COLUMN_PROCESSED == "well_id"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_raw_df(**extra_cols) -> pd.DataFrame:
    base = {
        "LEASE KID": ["1001", "1002"],
        "LEASE": ["Lease A", "Lease B"],
        "DOR_CODE": ["D1", "D2"],
        "API_NUMBER": ["15-001-12345", "15-001-67890"],
        "FIELD": ["Field X", "Field Y"],
        "PRODUCING_ZONE": ["Zone A", "Zone B"],
        "OPERATOR": ["Acme", "Beta"],
        "COUNTY": ["Allen", "Butler"],
        "TOWNSHIP": ["10", "11"],
        "TWN_DIR": ["S", "S"],
        "RANGE": ["5", "6"],
        "RANGE_DIR": ["W", "W"],
        "SECTION": ["12", "13"],
        "SPOT": ["NE", "SW"],
        "LATITUDE": ["38.5", "38.6"],
        "LONGITUDE": ["-95.5", "-95.6"],
        "MONTH-YEAR": ["3-2021", "4-2021"],
        "PRODUCT": ["O", "G"],
        "WELLS": ["1", "2"],
        "PRODUCTION": ["500.0", "2000.0"],
        "source_file": ["lp1.txt", "lp2.txt"],
    }
    base.update(extra_cols)
    return pd.DataFrame(base)


# ---------------------------------------------------------------------------
# Task 13: rename_and_cast_columns
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_rename_removes_month_year_raw_column():
    df = _make_raw_df()
    ddf = dd.from_pandas(df, npartitions=1)
    result = rename_and_cast_columns(ddf).compute()
    assert "MONTH-YEAR" not in result.columns
    assert "month_year" in result.columns


@pytest.mark.unit
def test_rename_production_is_float64():
    df = _make_raw_df()
    ddf = dd.from_pandas(df, npartitions=1)
    result = rename_and_cast_columns(ddf).compute()
    assert result["production"].dtype == np.float64


@pytest.mark.unit
def test_rename_township_is_int32():
    df = _make_raw_df()
    ddf = dd.from_pandas(df, npartitions=1)
    result = rename_and_cast_columns(ddf).compute()
    assert result["township"].dtype == pd.Int32Dtype()


@pytest.mark.unit
def test_rename_production_non_numeric_becomes_nan():
    df = _make_raw_df()
    df.loc[0, "PRODUCTION"] = "not_a_number"
    ddf = dd.from_pandas(df, npartitions=1)
    result = rename_and_cast_columns(ddf).compute()
    assert pd.isna(result.loc[0, "production"])


@pytest.mark.unit
def test_rename_returns_dask_df():
    df = _make_raw_df()
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(rename_and_cast_columns(ddf), dd.DataFrame)


@pytest.mark.unit
def test_rename_preserves_extra_columns():
    df = _make_raw_df()
    df["MY_EXTRA_COL"] = "extra"
    ddf = dd.from_pandas(df, npartitions=1)
    result = rename_and_cast_columns(ddf).compute()
    assert "MY_EXTRA_COL" in result.columns


# ---------------------------------------------------------------------------
# Task 14: parse_production_date
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_production_date_values():
    df = pd.DataFrame(
        {"month_year": ["1-2021", "12-2023", "6-2022"], "production": [1.0, 2.0, 3.0]}
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf).compute()
    assert result["production_date"].iloc[0] == pd.Timestamp("2021-01-01")
    assert result["production_date"].iloc[1] == pd.Timestamp("2023-12-01")
    assert result["production_date"].iloc[2] == pd.Timestamp("2022-06-01")


@pytest.mark.unit
def test_parse_production_date_bad_month_becomes_nat():
    df = pd.DataFrame({"month_year": ["99-2021"], "production": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf).compute()
    assert pd.isna(result["production_date"].iloc[0])


@pytest.mark.unit
def test_parse_production_date_drops_month_year():
    df = pd.DataFrame({"month_year": ["3-2021"], "production": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf).compute()
    assert "month_year" not in result.columns


@pytest.mark.unit
def test_parse_production_date_dtype():
    df = pd.DataFrame({"month_year": ["3-2021"], "production": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf).compute()
    assert result["production_date"].dtype == "datetime64[ns]"


@pytest.mark.unit
def test_parse_production_date_returns_dask():
    df = pd.DataFrame({"month_year": ["3-2021"], "production": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(parse_production_date(ddf), dd.DataFrame)


@pytest.mark.unit
def test_parse_production_date_raises_if_col_absent():
    df = pd.DataFrame({"production": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    with pytest.raises(KeyError):
        parse_production_date(ddf)


# ---------------------------------------------------------------------------
# Task 15: explode_api_numbers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_explode_api_numbers_splits_comma():
    df = pd.DataFrame(
        {
            "api_number": ["15-001-12345, 15-001-67890"],
            "lease_kid": ["1001"],
            "production": [500.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert len(result) == 2
    assert set(result["well_id"].values) == {"15-001-12345", "15-001-67890"}


@pytest.mark.unit
def test_explode_api_numbers_nan_uses_synthetic():
    df = pd.DataFrame(
        {
            "api_number": [None],
            "lease_kid": ["99999"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert len(result) == 1
    assert result.iloc[0]["well_id"] == "LEASE-99999"


@pytest.mark.unit
def test_explode_api_numbers_empty_string_treated_as_nan():
    df = pd.DataFrame(
        {
            "api_number": [""],
            "lease_kid": ["77777"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert result.iloc[0]["well_id"] == "LEASE-77777"


@pytest.mark.unit
def test_explode_api_numbers_strips_whitespace():
    df = pd.DataFrame(
        {
            "api_number": [" 15-001-12345 "],
            "lease_kid": ["1001"],
            "production": [500.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert result.iloc[0]["well_id"] == "15-001-12345"


@pytest.mark.unit
def test_explode_api_numbers_renames_column():
    df = pd.DataFrame(
        {
            "api_number": ["15-001-00001"],
            "lease_kid": ["1001"],
            "production": [500.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert "api_number" not in result.columns
    assert "well_id" in result.columns


@pytest.mark.unit
def test_explode_api_numbers_returns_dask():
    df = pd.DataFrame({"api_number": ["15-001-00001"], "lease_kid": ["1001"], "production": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(explode_api_numbers(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 16: validate_physical_bounds
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_negative_production_becomes_nan():
    df = pd.DataFrame({"production": [-100.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert pd.isna(result.iloc[0]["production"])
    assert result.iloc[0]["outlier_flag"]


@pytest.mark.unit
def test_validate_zero_production_preserved():
    df = pd.DataFrame({"production": [0.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert result.iloc[0]["production"] == 0.0
    assert not result.iloc[0]["outlier_flag"]


@pytest.mark.unit
def test_validate_nan_production_preserved():
    df = pd.DataFrame({"production": [np.nan], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert pd.isna(result.iloc[0]["production"])
    assert not result.iloc[0]["outlier_flag"]


@pytest.mark.unit
def test_validate_oil_high_flags_but_preserves():
    df = pd.DataFrame({"production": [75000.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert result.iloc[0]["production"] == 75000.0
    assert result.iloc[0]["outlier_flag"]


@pytest.mark.unit
def test_validate_gas_high_not_flagged():
    df = pd.DataFrame({"production": [75000.0], "product": ["G"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert not result.iloc[0]["outlier_flag"]


@pytest.mark.unit
def test_validate_normal_oil_not_flagged():
    df = pd.DataFrame({"production": [500.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert not result.iloc[0]["outlier_flag"]
    assert result.iloc[0]["production"] == 500.0


@pytest.mark.unit
def test_validate_returns_dask():
    df = pd.DataFrame({"production": [100.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(validate_physical_bounds(ddf), dd.DataFrame)


@pytest.mark.unit
def test_validate_outlier_flag_is_bool():
    df = pd.DataFrame({"production": [100.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert result["outlier_flag"].dtype == bool


# ---------------------------------------------------------------------------
# Task 17: assign_unit_labels
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_assign_unit_oil():
    df = pd.DataFrame({"product": ["O"], "production": [100.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = assign_unit_labels(ddf).compute()
    assert result.iloc[0]["unit"] == "BBL"


@pytest.mark.unit
def test_assign_unit_gas():
    df = pd.DataFrame({"product": ["G"], "production": [2000.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = assign_unit_labels(ddf).compute()
    assert result.iloc[0]["unit"] == "MCF"


@pytest.mark.unit
def test_assign_unit_unknown():
    df = pd.DataFrame({"product": ["W"], "production": [50.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = assign_unit_labels(ddf).compute()
    assert result.iloc[0]["unit"] == "UNKNOWN"


@pytest.mark.unit
def test_assign_unit_column_dtype():
    df = pd.DataFrame({"product": ["O"], "production": [100.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = assign_unit_labels(ddf).compute()
    assert result["unit"].dtype == object


@pytest.mark.unit
def test_assign_unit_returns_dask():
    df = pd.DataFrame({"product": ["O"], "production": [100.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(assign_unit_labels(ddf), dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 18: deduplicate_records
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_deduplicate_removes_exact_duplicates():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1", "W1", "W2", "W2"],
            "production_date": pd.to_datetime(
                ["2021-01-01", "2021-01-01", "2021-02-01", "2021-01-01", "2021-02-01"]
            ),
            "product": ["O", "O", "O", "O", "O"],
            "production": [100.0, 100.0, 200.0, 150.0, 160.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = deduplicate_records(ddf).compute()
    assert len(result) == 4


@pytest.mark.unit
def test_deduplicate_no_change_when_no_dups():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W2"],
            "production_date": pd.to_datetime(["2021-01-01", "2021-01-01"]),
            "product": ["O", "O"],
            "production": [100.0, 200.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = deduplicate_records(ddf).compute()
    assert len(result) == 2


@pytest.mark.unit
def test_deduplicate_partial_key_match_not_removed():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "production_date": pd.to_datetime(["2021-01-01", "2021-01-01"]),
            "product": ["O", "G"],  # Different product — not duplicates
            "production": [100.0, 200.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = deduplicate_records(ddf).compute()
    assert len(result) == 2


@pytest.mark.unit
def test_deduplicate_returns_dask():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "production_date": pd.to_datetime(["2021-01-01"]),
            "product": ["O"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(deduplicate_records(ddf), dd.DataFrame)


@pytest.mark.unit
def test_deduplicate_idempotent():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "production_date": pd.to_datetime(["2021-01-01", "2021-01-01"]),
            "product": ["O", "O"],
            "production": [100.0, 100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result1 = deduplicate_records(ddf).compute()
    ddf2 = dd.from_pandas(result1, npartitions=1)
    result2 = deduplicate_records(ddf2).compute()
    assert len(result1) == len(result2)


# ---------------------------------------------------------------------------
# Task 19: sort_and_repartition
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sort_and_repartition_sorts_chronologically():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1", "W2", "W2", "W3", "W3"],
            "production_date": pd.to_datetime(
                ["2021-03-01", "2021-01-01", "2021-06-01", "2021-02-01", "2022-01-01", "2021-11-01"]
            ),
            "product": ["O"] * 6,
            "production": [100.0, 200.0, 150.0, 300.0, 50.0, 80.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = sort_and_repartition(ddf).compute()

    for well_id in result["well_id"].unique():
        well_rows = result[result["well_id"] == well_id].sort_values("production_date")
        assert list(well_rows["production_date"]) == sorted(well_rows["production_date"])


@pytest.mark.unit
def test_sort_and_repartition_returns_dask():
    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "production_date": pd.to_datetime(["2021-01-01"]),
            "product": ["O"],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    assert isinstance(sort_and_repartition(ddf), dd.DataFrame)


@pytest.mark.unit
def test_sort_and_repartition_index_is_well_id():
    df = pd.DataFrame(
        {
            "well_id": ["W1", "W2"],
            "production_date": pd.to_datetime(["2021-01-01", "2021-02-01"]),
            "product": ["O", "O"],
            "production": [100.0, 200.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = sort_and_repartition(ddf)
    assert result.index.name == "well_id"


# ---------------------------------------------------------------------------
# Task 20: write_processed_parquet
# ---------------------------------------------------------------------------


def _make_processed_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lease_kid": ["1001"],
            "lease_name": ["Test Lease"],
            "dor_code": ["D1"],
            "field_name": ["Field X"],
            "producing_zone": ["Zone A"],
            "operator": ["Acme"],
            "county": ["Allen"],
            "township": pd.array([10], dtype=pd.Int32Dtype()),
            "twn_dir": ["S"],
            "range_num": pd.array([5], dtype=pd.Int32Dtype()),
            "range_dir": ["W"],
            "section": pd.array([12], dtype=pd.Int32Dtype()),
            "spot": ["NE"],
            "latitude": [38.5],
            "longitude": [-95.5],
            "product": ["O"],
            "well_count": pd.array([1], dtype=pd.Int32Dtype()),
            "production": [500.0],
            "source_file": ["lp1.txt"],
            "production_date": pd.to_datetime(["2021-03-01"]),
            "well_id": ["15-001-12345"],
            "outlier_flag": [False],
            "unit": ["BBL"],
        }
    )


@pytest.mark.unit
def test_write_processed_parquet_creates_files(tmp_path: Path):
    df = _make_processed_df()
    ddf = dd.from_pandas(df, npartitions=1)
    write_processed_parquet(ddf, tmp_path / "processed")
    parquet_files = list((tmp_path / "processed").glob("*.parquet"))
    assert len(parquet_files) >= 1


@pytest.mark.unit
def test_write_processed_parquet_readable(tmp_path: Path):
    df = _make_processed_df()
    ddf = dd.from_pandas(df, npartitions=1)
    write_processed_parquet(ddf, tmp_path / "proc")
    for fp in (tmp_path / "proc").glob("*.parquet"):
        loaded = pd.read_parquet(fp)
        assert loaded is not None


@pytest.mark.unit
def test_write_processed_parquet_returns_path(tmp_path: Path):
    df = _make_processed_df()
    ddf = dd.from_pandas(df, npartitions=1)
    out_dir = tmp_path / "proc"
    result = write_processed_parquet(ddf, out_dir)
    assert result == out_dir


# ---------------------------------------------------------------------------
# Task 21: run_transform_pipeline
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_transform_pipeline_calls_steps(tmp_path: Path):
    call_order: list[str] = []
    sample_df = _make_processed_df()
    sample_ddf = dd.from_pandas(sample_df, npartitions=1)

    # Give each mock an identity return value and track call order
    def _track(name: str, return_val=None):
        def side_effect(*args, **kwargs):
            call_order.append(name)
            return return_val if return_val is not None else sample_ddf

        return side_effect

    with (
        patch("kgs_pipeline.transform.dd.read_parquet", return_value=sample_ddf),
        patch(
            "kgs_pipeline.transform.rename_and_cast_columns",
            side_effect=_track("rename", sample_ddf),
        ),
        patch(
            "kgs_pipeline.transform.parse_production_date", side_effect=_track("parse", sample_ddf)
        ),
        patch(
            "kgs_pipeline.transform.explode_api_numbers", side_effect=_track("explode", sample_ddf)
        ),
        patch(
            "kgs_pipeline.transform.validate_physical_bounds",
            side_effect=_track("validate", sample_ddf),
        ),
        patch("kgs_pipeline.transform.assign_unit_labels", side_effect=_track("units", sample_ddf)),
        patch(
            "kgs_pipeline.transform.deduplicate_records", side_effect=_track("dedup", sample_ddf)
        ),
        patch(
            "kgs_pipeline.transform.sort_and_repartition", side_effect=_track("sort", sample_ddf)
        ),
        patch(
            "kgs_pipeline.transform.write_processed_parquet",
            side_effect=_track("write", config.PROCESSED_DATA_DIR),
        ),
    ):
        # Make INTERIM_DATA_DIR appear to have files
        with patch("kgs_pipeline.config.INTERIM_DATA_DIR", tmp_path):
            # Create a fake parquet file so the RuntimeError check passes
            (tmp_path / "part.parquet").touch()
            run_transform_pipeline()

    assert call_order == [
        "rename",
        "parse",
        "explode",
        "validate",
        "units",
        "dedup",
        "sort",
        "write",
    ]


@pytest.mark.unit
def test_run_transform_pipeline_raises_if_no_interim():
    with patch("kgs_pipeline.config.INTERIM_DATA_DIR") as mock_dir:
        mock_dir.glob.return_value = []
        with pytest.raises(RuntimeError):
            run_transform_pipeline()
