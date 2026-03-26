"""Tests for kgs_pipeline/transform.py (Tasks 12–21)."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import dask.dataframe as dd
import pandas as pd  # type: ignore[import-untyped]
import pytest


# ---------------------------------------------------------------------------
# Task 12: load_interim_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_interim_parquet_returns_dask(tmp_path: Path, sample_interim_df):
    from kgs_pipeline.transform import load_interim_parquet

    sample_interim_df.to_parquet(tmp_path / "part.0.parquet", index=False)
    result = load_interim_parquet(tmp_path)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_load_interim_parquet_missing_dir():
    from kgs_pipeline.transform import load_interim_parquet

    with pytest.raises(FileNotFoundError):
        load_interim_parquet(Path("/nonexistent/path/abc"))


@pytest.mark.unit
def test_load_interim_parquet_empty_dir(tmp_path: Path):
    from kgs_pipeline.transform import load_interim_parquet

    with pytest.raises(RuntimeError, match="No Parquet files found"):
        load_interim_parquet(tmp_path)


@pytest.mark.unit
def test_load_interim_parquet_not_pandas(tmp_path: Path, sample_interim_df):
    from kgs_pipeline.transform import load_interim_parquet

    sample_interim_df.to_parquet(tmp_path / "part.parquet", index=False)
    result = load_interim_parquet(tmp_path)
    assert not isinstance(result, pd.DataFrame)


# ---------------------------------------------------------------------------
# Task 13: parse_production_date
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_production_date_jan_2020():
    from kgs_pipeline.transform import parse_production_date

    df = pd.DataFrame({"MONTH_YEAR": ["1-2020"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf).compute()
    assert result["production_date"].iloc[0] == pd.Timestamp("2020-01-01")


@pytest.mark.unit
def test_parse_production_date_dec_2021():
    from kgs_pipeline.transform import parse_production_date

    df = pd.DataFrame({"MONTH_YEAR": ["12-2021"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf).compute()
    assert result["production_date"].iloc[0] == pd.Timestamp("2021-12-01")


@pytest.mark.unit
def test_parse_production_date_invalid_becomes_nat():
    from kgs_pipeline.transform import parse_production_date

    df = pd.DataFrame({"MONTH_YEAR": ["bad-value"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf).compute()
    assert pd.isna(result["production_date"].iloc[0])


@pytest.mark.unit
def test_parse_production_date_drops_month_year():
    from kgs_pipeline.transform import parse_production_date

    df = pd.DataFrame({"MONTH_YEAR": ["1-2020"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf)
    assert "MONTH_YEAR" not in result.columns


@pytest.mark.unit
def test_parse_production_date_returns_dask():
    from kgs_pipeline.transform import parse_production_date

    df = pd.DataFrame({"MONTH_YEAR": ["1-2020"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = parse_production_date(ddf)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 14: normalize_column_names
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_normalize_column_names_lowercase():
    from kgs_pipeline.transform import normalize_column_names

    df = pd.DataFrame(columns=["LEASE_KID", "MONTH_YEAR", "PRODUCT", "PRODUCTION", "API_NUMBER"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = normalize_column_names(ddf)
    assert "lease_kid" in result.columns
    assert "product" in result.columns
    assert "production" in result.columns
    assert "api_number" in result.columns


@pytest.mark.unit
def test_normalize_column_names_range_renamed():
    from kgs_pipeline.transform import normalize_column_names

    df = pd.DataFrame(columns=["RANGE"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = normalize_column_names(ddf)
    assert "range_" in result.columns


@pytest.mark.unit
def test_normalize_column_names_returns_dask():
    from kgs_pipeline.transform import normalize_column_names

    df = pd.DataFrame(columns=["LEASE_KID"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = normalize_column_names(ddf)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_normalize_column_names_extra_column_preserved():
    from kgs_pipeline.transform import normalize_column_names

    df = pd.DataFrame(columns=["LEASE_KID", "CUSTOM_EXTRA"])
    ddf = dd.from_pandas(df, npartitions=1)
    result = normalize_column_names(ddf)
    assert "CUSTOM_EXTRA" in result.columns


# ---------------------------------------------------------------------------
# Task 15: explode_api_numbers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_explode_api_numbers_two_apis():
    from kgs_pipeline.transform import explode_api_numbers

    df = pd.DataFrame(
        {
            "api_number": ["1500112345, 1500167890"],
            "lease_kid": [999],
            "production": [100.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert len(result) == 2
    well_ids = set(result["well_id"].tolist())
    assert "1500112345" in well_ids
    assert "1500167890" in well_ids


@pytest.mark.unit
def test_explode_api_numbers_single_api():
    from kgs_pipeline.transform import explode_api_numbers

    df = pd.DataFrame({"api_number": ["1500112345"], "lease_kid": [999], "production": [100.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert len(result) == 1


@pytest.mark.unit
def test_explode_api_numbers_null_becomes_unknown():
    from kgs_pipeline.transform import explode_api_numbers

    df = pd.DataFrame({"api_number": [None], "lease_kid": [42], "production": [100.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert result["well_id"].iloc[0] == "UNKNOWN_42"


@pytest.mark.unit
def test_explode_api_numbers_api_column_dropped():
    from kgs_pipeline.transform import explode_api_numbers

    df = pd.DataFrame({"api_number": ["A001"], "lease_kid": [1], "production": [1.0]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf)
    assert "api_number" not in result.columns


@pytest.mark.unit
def test_explode_api_numbers_no_null_well_ids():
    from kgs_pipeline.transform import explode_api_numbers

    df = pd.DataFrame(
        {
            "api_number": ["A001", None, "B001, C001"],
            "lease_kid": [1, 2, 3],
            "production": [10.0, 20.0, 30.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf).compute()
    assert result["well_id"].notna().all()
    assert (result["well_id"] != "").all()


@pytest.mark.unit
def test_explode_api_numbers_returns_dask():
    from kgs_pipeline.transform import explode_api_numbers

    df = pd.DataFrame({"api_number": ["A001"], "lease_kid": [1]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = explode_api_numbers(ddf)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 16: validate_physical_bounds
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_negative_production_becomes_nan():
    from kgs_pipeline.transform import validate_physical_bounds

    df = pd.DataFrame({"production": [-5.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert pd.isna(result["production"].iloc[0])


@pytest.mark.unit
def test_validate_zero_production_preserved():
    from kgs_pipeline.transform import validate_physical_bounds

    df = pd.DataFrame({"production": [0.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert result["production"].iloc[0] == 0.0


@pytest.mark.unit
def test_validate_oil_outlier_flagged():
    from kgs_pipeline.transform import validate_physical_bounds

    df = pd.DataFrame({"production": [60000.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert result["outlier_flag"].iloc[0] is True
    assert len(result) == 1  # row not dropped


@pytest.mark.unit
def test_validate_oil_normal_not_flagged():
    from kgs_pipeline.transform import validate_physical_bounds

    df = pd.DataFrame({"production": [100.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert result["outlier_flag"].iloc[0] is False


@pytest.mark.unit
def test_validate_gas_never_flagged():
    from kgs_pipeline.transform import validate_physical_bounds

    df = pd.DataFrame({"production": [999999.0], "product": ["G"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert result["outlier_flag"].iloc[0] is False


@pytest.mark.unit
def test_validate_unit_column():
    from kgs_pipeline.transform import validate_physical_bounds

    df = pd.DataFrame({"production": [100.0, 500.0], "product": ["O", "G"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf).compute()
    assert result[result["product"] == "O"]["unit"].iloc[0] == "BBL"
    assert result[result["product"] == "G"]["unit"].iloc[0] == "MCF"


@pytest.mark.unit
def test_validate_returns_dask():
    from kgs_pipeline.transform import validate_physical_bounds

    df = pd.DataFrame({"production": [1.0], "product": ["O"]})
    ddf = dd.from_pandas(df, npartitions=1)
    result = validate_physical_bounds(ddf)
    assert isinstance(result, dd.DataFrame)


# ---------------------------------------------------------------------------
# Task 17: deduplicate_records
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_deduplicate_keeps_higher_production():
    from kgs_pipeline.transform import deduplicate_records

    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1", "W2"],
            "production_date": pd.to_datetime(["2020-01-01", "2020-01-01", "2020-02-01"]),
            "product": ["O", "O", "O"],
            "production": [100.0, 50.0, 200.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = deduplicate_records(ddf).compute()
    assert len(result) == 2
    dup_row = result[
        (result["well_id"] == "W1") & (result["production_date"] == pd.Timestamp("2020-01-01"))
    ]
    assert dup_row["production"].iloc[0] == 100.0


@pytest.mark.unit
def test_deduplicate_no_change_when_no_dups():
    from kgs_pipeline.transform import deduplicate_records

    df = pd.DataFrame(
        {
            "well_id": ["W1", "W2"],
            "production_date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
            "product": ["O", "O"],
            "production": [100.0, 200.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = deduplicate_records(ddf).compute()
    assert len(result) == 2


@pytest.mark.unit
def test_deduplicate_null_production_loses_to_real():
    from kgs_pipeline.transform import deduplicate_records

    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1"],
            "production_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            "product": ["O", "O"],
            "production": [float("nan"), 75.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = deduplicate_records(ddf).compute()
    assert len(result) == 1
    assert result["production"].iloc[0] == 75.0


@pytest.mark.unit
def test_deduplicate_returns_dask():
    from kgs_pipeline.transform import deduplicate_records

    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "production_date": pd.to_datetime(["2020-01-01"]),
            "product": ["O"],
            "production": [1.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = deduplicate_records(ddf)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_deduplicate_idempotent():
    from kgs_pipeline.transform import deduplicate_records

    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1", "W2"],
            "production_date": pd.to_datetime(["2020-01-01", "2020-01-01", "2020-01-01"]),
            "product": ["O", "O", "O"],
            "production": [100.0, 50.0, 200.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    r1 = deduplicate_records(ddf).compute()
    r2 = deduplicate_records(dd.from_pandas(r1, npartitions=1)).compute()
    assert len(r1) == len(r2)


# ---------------------------------------------------------------------------
# Task 18: sort_and_repartition
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sort_and_repartition_ascending_dates():
    from kgs_pipeline.transform import sort_and_repartition

    df = pd.DataFrame(
        {
            "well_id": ["W1", "W1", "W1"],
            "production_date": pd.to_datetime(["2020-03-01", "2020-01-01", "2020-02-01"]),
            "production": [3.0, 1.0, 2.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = sort_and_repartition(ddf).compute()
    dates = result[result["well_id"] == "W1"]["production_date"].tolist()
    assert dates == sorted(dates)


@pytest.mark.unit
def test_sort_and_repartition_multiple_wells():
    from kgs_pipeline.transform import sort_and_repartition

    df = pd.DataFrame(
        {
            "well_id": ["W2", "W1", "W2", "W1"],
            "production_date": pd.to_datetime(
                ["2020-02-01", "2020-03-01", "2020-01-01", "2020-01-01"]
            ),
            "production": [2.0, 3.0, 1.0, 1.0],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = sort_and_repartition(ddf).compute()
    for well in ["W1", "W2"]:
        dates = result[result["well_id"] == well]["production_date"].tolist()
        assert dates == sorted(dates)


@pytest.mark.unit
def test_sort_and_repartition_returns_dask():
    from kgs_pipeline.transform import sort_and_repartition

    df = pd.DataFrame(
        {
            "well_id": ["W1"],
            "production_date": pd.to_datetime(["2020-01-01"]),
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    result = sort_and_repartition(ddf)
    assert isinstance(result, dd.DataFrame)


@pytest.mark.unit
def test_sort_and_repartition_stability_across_partitions():
    from kgs_pipeline.transform import sort_and_repartition

    dates = pd.date_range("2020-01-01", periods=24, freq="MS")
    df = pd.DataFrame(
        {
            "well_id": ["W1"] * 24,
            "production_date": dates[::-1],  # reverse order
            "production": range(24),
        }
    )
    ddf = dd.from_pandas(df, npartitions=4)
    result = sort_and_repartition(ddf).compute()
    w1 = result[result["well_id"] == "W1"]["production_date"].tolist()
    assert w1 == sorted(w1)


# ---------------------------------------------------------------------------
# Task 19: write_processed_parquet
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_processed_parquet_creates_files(tmp_path: Path, sample_processed_ddf):
    from kgs_pipeline.transform import write_processed_parquet

    written = write_processed_parquet(sample_processed_ddf, tmp_path)
    assert len(written) >= 1
    assert all(isinstance(p, Path) for p in written)
    assert all(p.exists() and p.stat().st_size > 0 for p in written)


@pytest.mark.unit
def test_write_processed_parquet_schema_spot_check(tmp_path: Path, sample_processed_ddf):
    from kgs_pipeline.transform import write_processed_parquet

    written = write_processed_parquet(sample_processed_ddf, tmp_path)
    df = pd.read_parquet(written[0])
    assert "well_id" in df.columns
    assert "production_date" in df.columns
    assert "production" in df.columns
    assert "outlier_flag" in df.columns
    assert "unit" in df.columns


@pytest.mark.unit
def test_write_processed_parquet_all_paths_exist(tmp_path: Path, sample_processed_ddf):
    from kgs_pipeline.transform import write_processed_parquet

    written = write_processed_parquet(sample_processed_ddf, tmp_path)
    assert all(p.exists() for p in written)
    assert all(p.stat().st_size > 0 for p in written)


@pytest.mark.unit
def test_write_processed_parquet_dask_readable(tmp_path: Path, sample_processed_ddf):
    from kgs_pipeline.transform import write_processed_parquet

    write_processed_parquet(sample_processed_ddf, tmp_path)
    reloaded = dd.read_parquet(str(tmp_path), engine="pyarrow")
    assert isinstance(reloaded, dd.DataFrame)


@pytest.mark.unit
def test_write_processed_parquet_schema_stability(tmp_path: Path):
    from kgs_pipeline.transform import write_processed_parquet

    df1 = pd.DataFrame(
        {
            "well_id": ["W1"],
            "lease_kid": pd.array([1], dtype="int64"),
            "lease": ["L1"],
            "dor_code": ["D1"],
            "field": ["F1"],
            "producing_zone": ["Z1"],
            "operator": ["O1"],
            "county": ["C1"],
            "township": ["1"],
            "twn_dir": ["S"],
            "range_": ["1"],
            "range_dir": ["W"],
            "section": ["1"],
            "spot": ["NE"],
            "latitude": [38.0],
            "longitude": [-98.0],
            "production_date": pd.to_datetime(["2020-01-01"]),
            "product": ["O"],
            "wells": pd.array([1], dtype="Int64"),
            "production": [100.0],
            "unit": ["BBL"],
            "outlier_flag": [False],
            "source_file": ["lp1"],
        }
    )
    df2 = df1.copy()
    df2["well_id"] = "W2"
    df2["latitude"] = float("nan")

    ddf = dd.from_pandas(pd.concat([df1, df2], ignore_index=True), npartitions=1)
    written = write_processed_parquet(ddf, tmp_path)

    schemas = [set(pd.read_parquet(p).dtypes.items()) for p in written]
    if len(schemas) >= 2:
        assert schemas[0] == schemas[1]


# ---------------------------------------------------------------------------
# Task 20: generate_cleaning_report
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_generate_cleaning_report_creates_file(tmp_path: Path):
    from kgs_pipeline.transform import generate_cleaning_report

    stats = {
        "input_row_count": 100,
        "rows_after_filter": 95,
        "negative_production_set_nan": 2,
        "outliers_flagged": 1,
        "duplicates_removed": 3,
        "null_production_date_dropped": 0,
        "unknown_well_id_assigned": 1,
        "output_row_count": 91,
        "output_parquet_files": 5,
        "pipeline_run_timestamp": "2024-01-01T00:00:00+00:00",
    }
    path = generate_cleaning_report(stats, tmp_path)
    assert path.exists()
    assert path.name == "cleaning_report.json"


@pytest.mark.unit
def test_generate_cleaning_report_valid_json(tmp_path: Path):
    from kgs_pipeline.transform import generate_cleaning_report

    stats = {
        "input_row_count": 100,
        "rows_after_filter": 95,
        "negative_production_set_nan": 2,
        "outliers_flagged": 1,
        "duplicates_removed": 3,
        "null_production_date_dropped": 0,
        "unknown_well_id_assigned": 1,
        "output_row_count": 91,
        "output_parquet_files": 5,
        "pipeline_run_timestamp": "2024-01-01T00:00:00+00:00",
    }
    path = generate_cleaning_report(stats, tmp_path)
    data = json.loads(path.read_text())
    required_keys = [
        "input_row_count",
        "rows_after_filter",
        "negative_production_set_nan",
        "outliers_flagged",
        "duplicates_removed",
        "null_production_date_dropped",
        "unknown_well_id_assigned",
        "output_row_count",
        "output_parquet_files",
        "pipeline_run_timestamp",
    ]
    for key in required_keys:
        assert key in data


@pytest.mark.unit
def test_generate_cleaning_report_timestamp_parseable(tmp_path: Path):
    from kgs_pipeline.transform import generate_cleaning_report

    stats = {
        "input_row_count": 0,
        "rows_after_filter": 0,
        "negative_production_set_nan": 0,
        "outliers_flagged": 0,
        "duplicates_removed": 0,
        "null_production_date_dropped": 0,
        "unknown_well_id_assigned": 0,
        "output_row_count": 0,
        "output_parquet_files": 0,
    }
    path = generate_cleaning_report(stats, tmp_path)
    data = json.loads(path.read_text())
    ts = data["pipeline_run_timestamp"]
    # Should parse without exception
    datetime.fromisoformat(ts.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Task 21: run_transform_pipeline
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_transform_pipeline_propagates_runtime_error():
    from kgs_pipeline.transform import run_transform_pipeline

    with patch("kgs_pipeline.transform.load_interim_parquet", side_effect=RuntimeError("no data")):
        with pytest.raises(RuntimeError):
            run_transform_pipeline()


@pytest.mark.unit
def test_zero_production_preservation(tmp_path: Path):
    """Zero production stays 0.0; null production stays NaN."""
    from kgs_pipeline.transform import (
        deduplicate_records,
        explode_api_numbers,
        normalize_column_names,
        parse_production_date,
        sort_and_repartition,
        validate_physical_bounds,
        write_processed_parquet,
    )

    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001, 1002], dtype="int64"),
            "LEASE": ["L1", "L2"],
            "DOR_CODE": ["D1", "D2"],
            "API_NUMBER": ["W001", "W002"],
            "FIELD": ["F", "F"],
            "PRODUCING_ZONE": ["Z", "Z"],
            "OPERATOR": ["O", "O"],
            "COUNTY": ["C", "C"],
            "TOWNSHIP": ["1", "1"],
            "TWN_DIR": ["S", "S"],
            "RANGE": ["1", "1"],
            "RANGE_DIR": ["W", "W"],
            "SECTION": ["1", "1"],
            "SPOT": ["NE", "NE"],
            "LATITUDE": [38.0, 38.0],
            "LONGITUDE": [-98.0, -98.0],
            "MONTH_YEAR": ["1-2020", "1-2020"],
            "PRODUCT": ["O", "O"],
            "WELLS": pd.array([1, 1], dtype="Int64"),
            "PRODUCTION": [0.0, float("nan")],
            "source_file": ["lp1001", "lp1002"],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = parse_production_date(ddf)
    ddf = normalize_column_names(ddf)
    ddf = explode_api_numbers(ddf)
    ddf = validate_physical_bounds(ddf)
    ddf = deduplicate_records(ddf)
    ddf = sort_and_repartition(ddf)

    processed_dir = tmp_path / "processed"
    write_processed_parquet(ddf, processed_dir)
    result = dd.read_parquet(str(processed_dir), engine="pyarrow").compute()

    w1 = result[result["well_id"] == "W001"]
    w2 = result[result["well_id"] == "W002"]
    assert w1["production"].iloc[0] == 0.0
    assert pd.isna(w2["production"].iloc[0])


@pytest.mark.unit
def test_data_integrity_spot_check(tmp_path: Path):
    """Known production values survive the transform pipeline."""
    from kgs_pipeline.transform import (
        deduplicate_records,
        explode_api_numbers,
        normalize_column_names,
        parse_production_date,
        sort_and_repartition,
        validate_physical_bounds,
        write_processed_parquet,
    )

    known_values = [100.0, 200.0, 150.0, 300.0, 250.0]
    df = pd.DataFrame(
        {
            "LEASE_KID": pd.array([1001] * 5, dtype="int64"),
            "LEASE": ["L"] * 5,
            "DOR_CODE": ["D"] * 5,
            "API_NUMBER": [f"W{i:03d}" for i in range(5)],
            "FIELD": ["F"] * 5,
            "PRODUCING_ZONE": ["Z"] * 5,
            "OPERATOR": ["O"] * 5,
            "COUNTY": ["C"] * 5,
            "TOWNSHIP": ["1"] * 5,
            "TWN_DIR": ["S"] * 5,
            "RANGE": ["1"] * 5,
            "RANGE_DIR": ["W"] * 5,
            "SECTION": ["1"] * 5,
            "SPOT": ["NE"] * 5,
            "LATITUDE": [38.0] * 5,
            "LONGITUDE": [-98.0] * 5,
            "MONTH_YEAR": [f"{i + 1}-2020" for i in range(5)],
            "PRODUCT": ["O"] * 5,
            "WELLS": pd.array([1] * 5, dtype="Int64"),
            "PRODUCTION": known_values,
            "source_file": [f"lp100{i}" for i in range(5)],
        }
    )
    ddf = dd.from_pandas(df, npartitions=1)
    ddf = parse_production_date(ddf)
    ddf = normalize_column_names(ddf)
    ddf = explode_api_numbers(ddf)
    ddf = validate_physical_bounds(ddf)
    ddf = deduplicate_records(ddf)
    ddf = sort_and_repartition(ddf)

    out_dir = tmp_path / "processed"
    write_processed_parquet(ddf, out_dir)
    result = dd.read_parquet(str(out_dir)).compute()

    result_values = set(result["production"].dropna().tolist())
    matched = sum(1 for v in known_values if v in result_values)
    assert matched >= 3
