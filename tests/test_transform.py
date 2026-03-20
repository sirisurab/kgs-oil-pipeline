import pytest
from pathlib import Path
import pandas as pd
import dask.dataframe as dd
from unittest.mock import patch, MagicMock
import time
from kgs_pipeline import config
from kgs_pipeline.transform import (
    read_interim_parquet,
    cast_column_types,
    deduplicate,
    validate_physical_bounds,
    pivot_product_columns,
    fill_date_gaps,
    compute_cumulative_production,
    write_processed_parquet,
    run_transform_pipeline,
)


class TestReadInterimParquet:
    """Tests for read_interim_parquet function."""

    @pytest.mark.unit
    def test_read_interim_parquet_returns_dask_dataframe(self, tmp_path):
        """Given a small Parquet file, assert returns dask.dataframe.DataFrame."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        parquet_path = tmp_path / "kgs_monthly_raw.parquet"
        df.to_parquet(parquet_path)

        result = read_interim_parquet(tmp_path)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_read_interim_parquet_no_compute(self, tmp_path):
        """Assert .compute() is not called; return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        parquet_path = tmp_path / "kgs_monthly_raw.parquet"
        df.to_parquet(parquet_path)

        result = read_interim_parquet(tmp_path)

        assert isinstance(result, dd.DataFrame)
        # Verify it hasn't been computed to pandas
        assert not isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    def test_read_interim_parquet_file_not_found(self):
        """Given non-existent path, assert FileNotFoundError is raised."""
        with pytest.raises(FileNotFoundError):
            read_interim_parquet(Path("/nonexistent/path"))

    @pytest.mark.unit
    def test_read_interim_parquet_logs_partition_count(self, tmp_path, caplog):
        """Assert the function logs the partition count at INFO level."""
        df = pd.DataFrame({"col1": range(100)})
        parquet_path = tmp_path / "kgs_monthly_raw.parquet"
        df.to_parquet(parquet_path)

        with caplog.at_level("INFO"):
            result = read_interim_parquet(tmp_path)

        assert "partitions" in caplog.text.lower()


class TestCastColumnTypes:
    """Tests for cast_column_types function."""

    @pytest.mark.unit
    def test_cast_column_types_latitude_to_float(self):
        """Given latitude as string, assert casts to float64."""
        df = pd.DataFrame({"latitude": ["39.99"], "other": ["x"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = cast_column_types(ddf).compute()

        assert result["latitude"].dtype == "float64"
        assert result["latitude"].iloc[0] == 39.99

    @pytest.mark.unit
    def test_cast_column_types_invalid_numeric_to_nan(self):
        """Given latitude as 'N/A', assert casts to NaN (not exception)."""
        df = pd.DataFrame({"latitude": ["N/A"], "other": ["x"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = cast_column_types(ddf).compute()

        assert pd.isna(result["latitude"].iloc[0])

    @pytest.mark.unit
    def test_cast_column_types_production_float(self):
        """Given production as string, assert casts to float64."""
        df = pd.DataFrame({"production": ["161.8"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = cast_column_types(ddf).compute()

        assert result["production"].dtype == "float64"
        assert result["production"].iloc[0] == 161.8

    @pytest.mark.unit
    def test_cast_column_types_production_date_datetime(self):
        """Assert production_date is datetime64[ns]."""
        df = pd.DataFrame({"production_date": ["2020-01-01"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = cast_column_types(ddf).compute()

        assert result["production_date"].dtype == "datetime64[ns]"

    @pytest.mark.unit
    def test_cast_column_types_strips_string_whitespace(self):
        """Assert string columns have no leading/trailing whitespace."""
        df = pd.DataFrame({
            "well_id": ["  123  "],
            "county": ["  SEDGWICK  "],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = cast_column_types(ddf).compute()

        assert result["well_id"].iloc[0] == "123"
        assert result["county"].iloc[0] == "SEDGWICK"

    @pytest.mark.unit
    def test_cast_column_types_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({"col1": ["1"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = cast_column_types(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_cast_column_types_renames_wells_to_wells_count(self):
        """Assert 'wells' column is renamed to 'wells_count'."""
        df = pd.DataFrame({"wells": ["5"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = cast_column_types(ddf).compute()

        assert "wells_count" in result.columns
        assert "wells" not in result.columns


class TestDeduplicate:
    """Tests for deduplicate function."""

    @pytest.mark.unit
    def test_deduplicate_removes_duplicates(self):
        """Given 4 rows with 2 duplicates, assert result has 3 rows."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W3"],
            "production_date": ["2020-01-01", "2020-01-01", "2020-02-01", "2020-03-01"],
            "product": ["O", "O", "G", "O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = deduplicate(ddf).compute()

        assert len(result) == 3

    @pytest.mark.unit
    def test_deduplicate_no_duplicates_unchanged(self):
        """Given DataFrame with no duplicates, assert row count unchanged."""
        df = pd.DataFrame({
            "well_id": ["W1", "W2", "W3"],
            "production_date": ["2020-01-01", "2020-02-01", "2020-03-01"],
            "product": ["O", "G", "O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = deduplicate(ddf).compute()

        assert len(result) == 3

    @pytest.mark.unit
    def test_deduplicate_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": ["2020-01-01"],
            "product": ["O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = deduplicate(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_deduplicate_missing_column_raises(self):
        """Given DataFrame missing 'product' column, assert KeyError is raised."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": ["2020-01-01"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            deduplicate(ddf)

    @pytest.mark.unit
    def test_deduplicate_idempotent(self):
        """Assert running deduplicate twice produces the same result as once."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2"],
            "production_date": ["2020-01-01", "2020-01-01", "2020-02-01"],
            "product": ["O", "O", "G"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result1 = deduplicate(ddf).compute()
        result2 = deduplicate(deduplicate(ddf)).compute()

        assert len(result1) == len(result2)
        assert result1.sort_values("well_id").reset_index(drop=True).equals(
            result2.sort_values("well_id").reset_index(drop=True)
        )


class TestValidatePhysicalBounds:
    """Tests for validate_physical_bounds function."""

    @pytest.mark.unit
    def test_validate_negative_production_set_to_nan(self):
        """Given negative production, assert set to NaN."""
        df = pd.DataFrame({
            "production": [-50.0],
            "product": ["O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = validate_physical_bounds(ddf).compute()

        assert pd.isna(result["production"].iloc[0])

    @pytest.mark.unit
    def test_validate_oil_rate_ceiling_flagged(self):
        """Given oil > 50000, assert is_suspect_rate = True."""
        df = pd.DataFrame({
            "production": [75000.0],
            "product": ["O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = validate_physical_bounds(ddf).compute()

        assert result["is_suspect_rate"].iloc[0] is True

    @pytest.mark.unit
    def test_validate_oil_rate_normal_not_flagged(self):
        """Given oil <= 50000, assert is_suspect_rate = False."""
        df = pd.DataFrame({
            "production": [100.0],
            "product": ["O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = validate_physical_bounds(ddf).compute()

        assert result["is_suspect_rate"].iloc[0] is False

    @pytest.mark.unit
    def test_validate_latitude_out_of_bounds_to_nan(self):
        """Given latitude outside Kansas bounds, assert set to NaN."""
        df = pd.DataFrame({
            "latitude": [50.0],
            "production": [100.0],
            "product": ["O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = validate_physical_bounds(ddf).compute()

        assert pd.isna(result["latitude"].iloc[0])

    @pytest.mark.unit
    def test_validate_longitude_out_of_bounds_to_nan(self):
        """Given longitude outside Kansas bounds, assert set to NaN."""
        df = pd.DataFrame({
            "longitude": [-80.0],
            "production": [100.0],
            "product": ["O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = validate_physical_bounds(ddf).compute()

        assert pd.isna(result["longitude"].iloc[0])

    @pytest.mark.unit
    def test_validate_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({
            "production": [100.0],
            "product": ["O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = validate_physical_bounds(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_validate_missing_production_raises(self):
        """Given DataFrame missing 'production', assert KeyError is raised."""
        df = pd.DataFrame({
            "product": ["O"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            validate_physical_bounds(ddf)


class TestPivotProductColumns:
    """Tests for pivot_product_columns function."""

    @pytest.mark.unit
    def test_pivot_oil_and_gas_same_month(self):
        """Given O and G rows for same month, assert pivoted to single row."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "production_date": ["2020-01-01", "2020-01-01"],
            "product": ["O", "G"],
            "production": [100.0, 50.0],
            "lease_kid": ["L1", "L1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = pivot_product_columns(ddf).compute()

        assert len(result) == 1
        assert result["oil_bbl"].iloc[0] == 100.0
        assert result["gas_mcf"].iloc[0] == 50.0

    @pytest.mark.unit
    def test_pivot_only_oil_recorded(self):
        """Given only oil, assert gas_mcf = NaN."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": ["2020-01-01"],
            "product": ["O"],
            "production": [100.0],
            "lease_kid": ["L1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = pivot_product_columns(ddf).compute()

        assert result["oil_bbl"].iloc[0] == 100.0
        assert pd.isna(result["gas_mcf"].iloc[0])

    @pytest.mark.unit
    def test_pivot_zero_production_preserved(self):
        """Given production = 0.0, assert pivoted value is 0.0 (not NaN)."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": ["2020-01-01"],
            "product": ["O"],
            "production": [0.0],
            "lease_kid": ["L1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = pivot_product_columns(ddf).compute()

        assert result["oil_bbl"].iloc[0] == 0.0

    @pytest.mark.unit
    def test_pivot_has_required_columns(self):
        """Assert output has oil_bbl and gas_mcf columns."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": ["2020-01-01"],
            "product": ["O"],
            "production": [100.0],
            "lease_kid": ["L1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = pivot_product_columns(ddf).compute()

        assert "oil_bbl" in result.columns
        assert "gas_mcf" in result.columns

    @pytest.mark.unit
    def test_pivot_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": ["2020-01-01"],
            "product": ["O"],
            "production": [100.0],
            "lease_kid": ["L1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = pivot_product_columns(ddf)

        assert isinstance(result, dd.DataFrame)


class TestFillDateGaps:
    """Tests for fill_date_gaps function."""

    @pytest.mark.unit
    def test_fill_date_gaps_inserts_missing_months(self):
        """Given well with gap (Jan, Mar), assert Feb is inserted with NaN production."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-03-01"),
            ],
            "oil_bbl": [100.0, 200.0],
            "gas_mcf": [10.0, 20.0],
            "county": ["SEDGWICK", "SEDGWICK"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = fill_date_gaps(ddf).compute()

        assert len(result) == 3
        feb_row = result[result["production_date"] == pd.Timestamp("2020-02-01")]
        assert pd.isna(feb_row["oil_bbl"].iloc[0])
        assert pd.isna(feb_row["gas_mcf"].iloc[0])

    @pytest.mark.unit
    def test_fill_date_gaps_preserves_zero_production(self):
        """Given explicit zero production, assert preserved as 0.0 after gap-filling."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
            ],
            "oil_bbl": [0.0, 100.0],
            "gas_mcf": [None, 20.0],
            "county": ["SEDGWICK", "SEDGWICK"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = fill_date_gaps(ddf).compute()

        assert result[result["production_date"] == pd.Timestamp("2020-01-01")][
            "oil_bbl"
        ].iloc[0] == 0.0

    @pytest.mark.unit
    def test_fill_date_gaps_forward_fill_metadata(self):
        """Assert metadata (county, operator) is forward-filled for gap rows."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-03-01"),
            ],
            "oil_bbl": [100.0, 200.0],
            "gas_mcf": [10.0, 20.0],
            "county": ["SEDGWICK", "SEDGWICK"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = fill_date_gaps(ddf).compute()

        feb_row = result[result["production_date"] == pd.Timestamp("2020-02-01")]
        assert feb_row["county"].iloc[0] == "SEDGWICK"

    @pytest.mark.unit
    def test_fill_date_gaps_single_record_unchanged(self):
        """Given well with single record, assert output has exactly 1 row."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = fill_date_gaps(ddf).compute()

        assert len(result) == 1

    @pytest.mark.unit
    def test_fill_date_gaps_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = fill_date_gaps(ddf)

        assert isinstance(result, dd.DataFrame)


class TestComputeCumulativeProduction:
    """Tests for compute_cumulative_production function."""

    @pytest.mark.unit
    def test_compute_cumulative_basic_sequence(self):
        """Given oil [100, 150, 200], assert cumulative [100, 250, 450]."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
                pd.Timestamp("2020-03-01"),
            ],
            "oil_bbl": [100.0, 150.0, 200.0],
            "gas_mcf": [10.0, 15.0, 20.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_cumulative_production(ddf).compute()

        assert list(result["cumulative_oil_bbl"]) == [100.0, 250.0, 450.0]

    @pytest.mark.unit
    def test_compute_cumulative_with_nan_carries_forward(self):
        """Given NaN in oil_bbl, assert cumulative carries forward previous value."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
                pd.Timestamp("2020-03-01"),
            ],
            "oil_bbl": [100.0, None, 200.0],
            "gas_mcf": [10.0, None, 20.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_cumulative_production(ddf).compute()

        # NaN month should carry forward: [100, 100, 300]
        assert result["cumulative_oil_bbl"].iloc[0] == 100.0
        assert result["cumulative_oil_bbl"].iloc[1] == 100.0
        assert result["cumulative_oil_bbl"].iloc[2] == 300.0

    @pytest.mark.unit
    def test_compute_cumulative_monotonic_nondecreasing(self):
        """Assert cumulative sums are monotonically non-decreasing."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
                pd.Timestamp("2020-03-01"),
            ],
            "oil_bbl": [100.0, 50.0, 150.0],
            "gas_mcf": [10.0, 5.0, 20.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_cumulative_production(ddf).compute()

        # Check monotonicity
        oil_cum = result["cumulative_oil_bbl"].values
        assert all(oil_cum[i] <= oil_cum[i + 1] for i in range(len(oil_cum) - 1))

    @pytest.mark.unit
    def test_compute_cumulative_independent_per_well(self):
        """Given two wells, assert cumulative sums are computed independently."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W2"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
            ],
            "oil_bbl": [100.0, 50.0, 200.0, 100.0],
            "gas_mcf": [10.0, 5.0, 20.0, 10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_cumulative_production(ddf).compute()

        w1_cum = result[result["well_id"] == "W1"]["cumulative_oil_bbl"].values
        w2_cum = result[result["well_id"] == "W2"]["cumulative_oil_bbl"].values

        assert list(w1_cum) == [100.0, 150.0]
        assert list(w2_cum) == [200.0, 300.0]

    @pytest.mark.unit
    def test_compute_cumulative_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_cumulative_production(ddf)

        assert isinstance(result, dd.DataFrame)


class TestWriteProcessedParquet:
    """Tests for write_processed_parquet function."""

    @pytest.mark.unit
    def test_write_processed_parquet_returns_path(self, tmp_path):
        """Given DataFrame, assert returns Path ending in 'wells'."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W2"],
            "col1": [1, 2, 3, 4],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = write_processed_parquet(ddf, tmp_path)

        assert isinstance(result, Path)
        assert result.name == "wells"

    @pytest.mark.integration
    def test_write_processed_parquet_creates_output_dir(self, tmp_path):
        """Assert output_dir is created if doesn't exist."""
        df = pd.DataFrame({
            "well_id": ["W1", "W2"],
            "col1": [1, 2],
        })
        ddf = dd.from_pandas(df, npartitions=1)
        output_dir = tmp_path / "new" / "nested"

        assert not output_dir.exists()
        write_processed_parquet(ddf, output_dir)
        assert output_dir.exists()

    @pytest.mark.integration
    def test_write_processed_parquet_partitions_by_well_id(self, tmp_path):
        """Assert output contains subdirectories (one per well_id)."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W2"],
            "col1": [1, 2, 3, 4],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        wells_dir = write_processed_parquet(ddf, tmp_path)

        # Check for well_id subdirectories
        subdirs = list(wells_dir.glob("well_id=*"))
        assert len(subdirs) == 2

    @pytest.mark.integration
    def test_write_processed_parquet_readable(self, tmp_path):
        """Assert written files are readable by read_parquet."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "col1": [1, 2],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        wells_dir = write_processed_parquet(ddf, tmp_path)
        read_back = dd.read_parquet(str(wells_dir))

        assert len(read_back.compute()) == 2

    @pytest.mark.integration
    def test_write_processed_parquet_partition_correctness(self, tmp_path):
        """Assert each partition contains rows for only one well_id."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W2"],
            "col1": [1, 2, 3, 4],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        wells_dir = write_processed_parquet(ddf, tmp_path)
        read_back = dd.read_parquet(str(wells_dir)).compute()

        # Verify partitioning
        for well_id in ["W1", "W2"]:
            well_data = read_back[read_back["well_id"] == well_id]
            assert len(well_data) == 2


class TestRunTransformPipeline:
    """Tests for run_transform_pipeline function."""

    @pytest.mark.unit
    def test_run_transform_pipeline_calls_in_order(self, tmp_path):
        """Assert all sub-functions are called in correct sequence."""
        interim_dir = tmp_path / "interim"
        interim_dir.mkdir()
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.transform.read_interim_parquet") as mock_read:
            with patch("kgs_pipeline.transform.cast_column_types") as mock_cast:
                with patch("kgs_pipeline.transform.deduplicate") as mock_dedup:
                    with patch("kgs_pipeline.transform.validate_physical_bounds") as mock_val:
                        with patch("kgs_pipeline.transform.pivot_product_columns") as mock_piv:
                            with patch("kgs_pipeline.transform.fill_date_gaps") as mock_gap:
                                with patch("kgs_pipeline.transform.compute_cumulative_production") as mock_cum:
                                    with patch("kgs_pipeline.transform.write_processed_parquet") as mock_write:
                                        # Setup mock returns
                                        mock_ddf = MagicMock(spec=dd.DataFrame)
                                        mock_read.return_value = mock_ddf
                                        mock_cast.return_value = mock_ddf
                                        mock_dedup.return_value = mock_ddf
                                        mock_val.return_value = mock_ddf
                                        mock_piv.return_value = mock_ddf
                                        mock_gap.return_value = mock_ddf
                                        mock_cum.return_value = mock_ddf

                                        mock_output = output_dir / "wells"
                                        mock_write.return_value = mock_output

                                        result = run_transform_pipeline(interim_dir, output_dir)

                                        # Verify all were called in order
                                        assert mock_read.called
                                        assert mock_cast.called
                                        assert mock_dedup.called
                                        assert mock_val.called
                                        assert mock_piv.called
                                        assert mock_gap.called
                                        assert mock_cum.called
                                        assert mock_write.called

                                        # Verify call order (mock.call_count increases)
                                        assert result == mock_output

    @pytest.mark.unit
    def test_run_transform_pipeline_no_intermediate_compute(self, tmp_path):
        """Assert .compute() is not called on intermediate DataFrames."""
        interim_dir = tmp_path / "interim"
        interim_dir.mkdir()
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.transform.read_interim_parquet") as mock_read:
            with patch("kgs_pipeline.transform.cast_column_types") as mock_cast:
                with patch("kgs_pipeline.transform.deduplicate") as mock_dedup:
                    with patch("kgs_pipeline.transform.validate_physical_bounds") as mock_val:
                        with patch("kgs_pipeline.transform.pivot_product_columns") as mock_piv:
                            with patch("kgs_pipeline.transform.fill_date_gaps") as mock_gap:
                                with patch("kgs_pipeline.transform.compute_cumulative_production") as mock_cum:
                                    with patch("kgs_pipeline.transform.write_processed_parquet") as mock_write:
                                        # Setup mocks
                                        mock_ddf = MagicMock(spec=dd.DataFrame)
                                        mock_ddf.compute = MagicMock()
                                        mock_read.return_value = mock_ddf
                                        mock_cast.return_value = mock_ddf
                                        mock_dedup.return_value = mock_ddf
                                        mock_val.return_value = mock_ddf
                                        mock_piv.return_value = mock_ddf
                                        mock_gap.return_value = mock_ddf
                                        mock_cum.return_value = mock_ddf

                                        mock_write.return_value = output_dir / "wells"

                                        run_transform_pipeline(interim_dir, output_dir)

                                        # Verify .compute() was not called
                                        mock_ddf.compute.assert_not_called()
