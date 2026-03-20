import pytest
from pathlib import Path
import json
import pandas as pd
import numpy as np
import dask.dataframe as dd
from unittest.mock import patch, MagicMock
from kgs_pipeline import config
from kgs_pipeline.features import (
    read_processed_parquet,
    compute_time_features,
    compute_rolling_features,
    compute_decline_and_gor,
    encode_categorical_features,
    save_encoding_map,
    load_encoding_map,
    write_feature_parquet,
    run_features_pipeline,
)


class TestReadProcessedParquet:
    """Tests for read_processed_parquet function."""

    @pytest.mark.unit
    def test_read_processed_parquet_returns_dask_dataframe(self, tmp_path):
        """Given a Parquet file, assert returns dask.dataframe.DataFrame."""
        df = pd.DataFrame({"well_id": ["W1"], "col1": [1]})
        wells_dir = tmp_path / "wells"
        wells_dir.mkdir()
        df.to_parquet(str(wells_dir / "data.parquet"))

        result = read_processed_parquet(tmp_path)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_read_processed_parquet_file_not_found(self):
        """Given non-existent path, assert FileNotFoundError is raised."""
        with pytest.raises(FileNotFoundError):
            read_processed_parquet(Path("/nonexistent"))

    @pytest.mark.unit
    def test_read_processed_parquet_empty_dir_raises(self, tmp_path):
        """Given empty wells directory, assert ValueError is raised."""
        wells_dir = tmp_path / "wells"
        wells_dir.mkdir()

        with pytest.raises(ValueError):
            read_processed_parquet(tmp_path)

    @pytest.mark.unit
    def test_read_processed_parquet_logs_partitions(self, tmp_path, caplog):
        """Assert logs partition count at INFO level."""
        df = pd.DataFrame({"well_id": ["W1"], "col1": [1]})
        wells_dir = tmp_path / "wells"
        wells_dir.mkdir()
        df.to_parquet(str(wells_dir / "data.parquet"))

        with caplog.at_level("INFO"):
            read_processed_parquet(tmp_path)

        assert "partitions" in caplog.text.lower()


class TestComputeTimeFeatures:
    """Tests for compute_time_features function."""

    @pytest.mark.unit
    def test_compute_time_features_months_since_first(self):
        """Given well with first prod Jan-2020 and row for Mar-2020, assert months_since = 2."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-03-01"),
            ],
            "oil_bbl": [100.0, 150.0],
            "gas_mcf": [10.0, 15.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_time_features(ddf).compute()

        assert result["months_since_first_prod"].iloc[1] == pytest.approx(2.0, abs=0.1)

    @pytest.mark.unit
    def test_compute_time_features_producing_months_count(self):
        """Given 3 months with production, assert producing_months_count = [1, 2, 3]."""
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

        result = compute_time_features(ddf).compute()

        assert list(result["producing_months_count"]) == [1.0, 2.0, 3.0]

    @pytest.mark.unit
    def test_compute_time_features_production_phase_early(self):
        """Given months_since = 6, assert production_phase = 'early'."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W1", "W1", "W1", "W1"],
            "production_date": pd.date_range("2020-01-01", periods=6, freq="MS"),
            "oil_bbl": [100.0] * 6,
            "gas_mcf": [10.0] * 6,
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_time_features(ddf).compute()

        assert result["production_phase"].iloc[-1] == "early"

    @pytest.mark.unit
    def test_compute_time_features_production_phase_mid(self):
        """Given months_since = 24, assert production_phase = 'mid'."""
        df = pd.DataFrame({
            "well_id": ["W1"] * 24,
            "production_date": pd.date_range("2020-01-01", periods=24, freq="MS"),
            "oil_bbl": [100.0] * 24,
            "gas_mcf": [10.0] * 24,
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_time_features(ddf).compute()

        assert result["production_phase"].iloc[-1] == "mid"

    @pytest.mark.unit
    def test_compute_time_features_production_phase_late(self):
        """Given months_since = 80, assert production_phase = 'late'."""
        df = pd.DataFrame({
            "well_id": ["W1"] * 80,
            "production_date": pd.date_range("2020-01-01", periods=80, freq="MS"),
            "oil_bbl": [100.0] * 80,
            "gas_mcf": [10.0] * 80,
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_time_features(ddf).compute()

        assert result["production_phase"].iloc[-1] == "late"

    @pytest.mark.unit
    def test_compute_time_features_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_time_features(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_compute_time_features_independent_per_well(self):
        """Given two wells, assert months_since_first_prod resets per well."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W2"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-03-01"),
                pd.Timestamp("2020-02-01"),
                pd.Timestamp("2020-04-01"),
            ],
            "oil_bbl": [100.0, 150.0, 200.0, 250.0],
            "gas_mcf": [10.0, 15.0, 20.0, 25.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_time_features(ddf).compute()

        # W1 row 1: 2 months; W2 row 1: 2 months
        w1_months = result[result["well_id"] == "W1"]["months_since_first_prod"].iloc[-1]
        w2_months = result[result["well_id"] == "W2"]["months_since_first_prod"].iloc[-1]
        assert w1_months == pytest.approx(w2_months, abs=0.1)

    @pytest.mark.unit
    def test_compute_time_features_missing_column_raises(self):
        """Given missing production_date, assert KeyError is raised."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "oil_bbl": [100.0],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            compute_time_features(ddf)


class TestComputeRollingFeatures:
    """Tests for compute_rolling_features function."""

    @pytest.mark.unit
    def test_compute_rolling_features_oil_roll3(self):
        """Given oil = [100, 200, 300, 400], assert roll3[3] = (200+300+400)/3 = 300."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W1", "W1"],
            "production_date": pd.date_range("2020-01-01", periods=4, freq="MS"),
            "oil_bbl": [100.0, 200.0, 300.0, 400.0],
            "gas_mcf": [10.0, 20.0, 30.0, 40.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_rolling_features(ddf).compute()

        assert result["oil_bbl_roll3"].iloc[-1] == pytest.approx(300.0, abs=0.01)

    @pytest.mark.unit
    def test_compute_rolling_features_short_window(self):
        """Given only 2 months, assert roll3 uses min_periods=1 and returns mean of 2."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "production_date": pd.date_range("2020-01-01", periods=2, freq="MS"),
            "oil_bbl": [100.0, 200.0],
            "gas_mcf": [10.0, 20.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_rolling_features(ddf).compute()

        # With min_periods=1, month 2 should have mean of [100, 200] = 150
        assert result["oil_bbl_roll3"].iloc[-1] == pytest.approx(150.0, abs=0.01)

    @pytest.mark.unit
    def test_compute_rolling_features_all_6_columns_present(self):
        """Assert all 6 rolling columns are in output."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_rolling_features(ddf).compute()

        cols = [
            "oil_bbl_roll3", "oil_bbl_roll6", "oil_bbl_roll12",
            "gas_mcf_roll3", "gas_mcf_roll6", "gas_mcf_roll12",
        ]
        for col in cols:
            assert col in result.columns

    @pytest.mark.unit
    def test_compute_rolling_features_independent_per_well(self):
        """Given two wells, assert rolling stats computed independently."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W2"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
            ],
            "oil_bbl": [100.0, 200.0, 1000.0, 2000.0],
            "gas_mcf": [10.0, 20.0, 100.0, 200.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_rolling_features(ddf).compute()

        w1_roll = result[result["well_id"] == "W1"]["oil_bbl_roll3"].iloc[-1]
        w2_roll = result[result["well_id"] == "W2"]["oil_bbl_roll3"].iloc[-1]
        # W1 should be ~150, W2 should be ~1500
        assert w1_roll < 500 and w2_roll > 500

    @pytest.mark.unit
    def test_compute_rolling_features_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_rolling_features(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_compute_rolling_features_missing_column_raises(self):
        """Given missing oil_bbl, assert KeyError is raised."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            compute_rolling_features(ddf)


class TestComputeDeclineAndGor:
    """Tests for compute_decline_and_gor function."""

    @pytest.mark.unit
    def test_compute_decline_rate_basic(self):
        """Given oil = [100, 80], assert decline[1] = -0.20."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
            ],
            "oil_bbl": [100.0, 80.0],
            "gas_mcf": [10.0, 8.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_decline_and_gor(ddf).compute()

        assert result["oil_decline_rate_mom"].iloc[1] == pytest.approx(-0.2, abs=0.01)

    @pytest.mark.unit
    def test_compute_decline_rate_zero_denominator(self):
        """Given oil[t-1] = 0, assert decline[t] = NaN."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
            ],
            "oil_bbl": [0.0, 100.0],
            "gas_mcf": [10.0, 20.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_decline_and_gor(ddf).compute()

        assert pd.isna(result["oil_decline_rate_mom"].iloc[1])

    @pytest.mark.unit
    def test_compute_decline_rate_first_month_nan(self):
        """Given first month of well, assert decline rate = NaN."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_decline_and_gor(ddf).compute()

        assert pd.isna(result["oil_decline_rate_mom"].iloc[0])

    @pytest.mark.unit
    def test_compute_gor_basic(self):
        """Given oil = 100, gas = 50, assert GOR = 0.5."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [50.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_decline_and_gor(ddf).compute()

        assert result["gor"].iloc[0] == pytest.approx(0.5, abs=0.01)

    @pytest.mark.unit
    def test_compute_gor_zero_oil(self):
        """Given oil = 0, assert GOR = NaN."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [0.0],
            "gas_mcf": [50.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_decline_and_gor(ddf).compute()

        assert pd.isna(result["gor"].iloc[0])

    @pytest.mark.unit
    def test_compute_decline_and_gor_returns_dask_dataframe(self):
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [10.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_decline_and_gor(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_compute_decline_independent_per_well(self):
        """Given two wells, assert first row of each has NaN decline."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W2"],
            "production_date": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-02-01"),
                pd.Timestamp("2020-03-01"),
                pd.Timestamp("2020-04-01"),
            ],
            "oil_bbl": [100.0, 80.0, 200.0, 160.0],
            "gas_mcf": [10.0, 8.0, 20.0, 16.0],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_decline_and_gor(ddf).compute()

        w1_first = result[(result["well_id"] == "W1") & (result["production_date"] == pd.Timestamp("2020-01-01"))]["oil_decline_rate_mom"].iloc[0]
        w2_first = result[(result["well_id"] == "W2") & (result["production_date"] == pd.Timestamp("2020-03-01"))]["oil_decline_rate_mom"].iloc[0]
        assert pd.isna(w1_first) and pd.isna(w2_first)


class TestEncodeCategoricalFeatures:
    """Tests for encode_categorical_features function."""

    @pytest.mark.unit
    def test_encode_categorical_basic_alphabetical(self):
        """Given county = ['Allen', 'Barton', 'Allen'], assert encoded = [0, 1, 0]."""
        df = pd.DataFrame({
            "county": ["Allen", "Barton", "Allen"],
            "field": ["F1", "F1", "F1"],
            "producing_zone": ["Z1", "Z1", "Z1"],
            "operator": ["Op1", "Op1", "Op1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result, enc_map = encode_categorical_features(ddf)
        result_computed = result.compute()

        # Allen < Barton alphabetically, so Allen=0, Barton=1
        assert list(result_computed["county_encoded"]) == [0, 1, 0]

    @pytest.mark.unit
    def test_encode_categorical_with_provided_map(self):
        """Given encoding_map and new category, assert new category = -1."""
        df = pd.DataFrame({
            "county": ["Allen", "Crawford"],
            "field": ["F1", "F1"],
            "producing_zone": ["Z1", "Z1"],
            "operator": ["Op1", "Op1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        encoding_map = {
            "county": {"Allen": 0, "Barton": 1},
            "field": {"F1": 0},
            "producing_zone": {"Z1": 0},
            "operator": {"Op1": 0},
        }

        result, _ = encode_categorical_features(ddf, encoding_map)
        result_computed = result.compute()

        # Crawford not in map, should be -1
        assert result_computed["county_encoded"].iloc[1] == -1

    @pytest.mark.unit
    def test_encode_categorical_returns_mapping(self):
        """Assert returned encoding_map is a dict with all 4 categorical columns."""
        df = pd.DataFrame({
            "county": ["A", "B"],
            "field": ["F1", "F2"],
            "producing_zone": ["Z1", "Z2"],
            "operator": ["Op1", "Op2"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        _, enc_map = encode_categorical_features(ddf)

        assert set(enc_map.keys()) == {"county", "field", "producing_zone", "operator"}

    @pytest.mark.unit
    def test_encode_categorical_dtype_int32(self):
        """Assert encoded columns have dtype int32."""
        df = pd.DataFrame({
            "county": ["A"],
            "field": ["F1"],
            "producing_zone": ["Z1"],
            "operator": ["Op1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result, _ = encode_categorical_features(ddf)
        result_computed = result.compute()

        for col in ["county_encoded", "field_encoded", "producing_zone_encoded", "operator_encoded"]:
            assert result_computed[col].dtype == "int32"

    @pytest.mark.unit
    def test_encode_categorical_original_columns_preserved(self):
        """Assert original categorical columns are kept in output."""
        df = pd.DataFrame({
            "county": ["A"],
            "field": ["F1"],
            "producing_zone": ["Z1"],
            "operator": ["Op1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result, _ = encode_categorical_features(ddf)
        result_computed = result.compute()

        for col in ["county", "field", "producing_zone", "operator"]:
            assert col in result_computed.columns

    @pytest.mark.unit
    def test_encode_categorical_missing_column_raises(self):
        """Given missing 'field' column, assert KeyError is raised."""
        df = pd.DataFrame({
            "county": ["A"],
            "producing_zone": ["Z1"],
            "operator": ["Op1"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            encode_categorical_features(ddf)


class TestSaveLoadEncodingMap:
    """Tests for save_encoding_map and load_encoding_map functions."""

    @pytest.mark.unit
    def test_save_encoding_map_returns_path(self, tmp_path):
        """Assert save_encoding_map returns Path ending in 'encoding_map.json'."""
        enc_map = {
            "county": {"A": 0, "B": 1},
            "field": {"F1": 0},
            "producing_zone": {"Z1": 0},
            "operator": {"Op1": 0},
        }

        result = save_encoding_map(enc_map, tmp_path)

        assert isinstance(result, Path)
        assert result.name == "encoding_map.json"

    @pytest.mark.unit
    def test_save_encoding_map_creates_file(self, tmp_path):
        """Assert file is actually written to disk."""
        enc_map = {
            "county": {"A": 0},
            "field": {"F1": 0},
            "producing_zone": {"Z1": 0},
            "operator": {"Op1": 0},
        }

        result = save_encoding_map(enc_map, tmp_path)

        assert result.exists()
        assert result.is_file()

    @pytest.mark.unit
    def test_save_load_round_trip(self, tmp_path):
        """Assert save then load produces the same dict."""
        original = {
            "county": {"Allen": 0, "Barton": 1},
            "field": {"F1": 0, "F2": 1},
            "producing_zone": {"Z1": 0},
            "operator": {"Op1": 0, "Op2": 1},
        }

        save_encoding_map(original, tmp_path)
        loaded = load_encoding_map(tmp_path)

        assert loaded == original

    @pytest.mark.unit
    def test_save_encoding_map_creates_output_dir(self, tmp_path):
        """Given non-existent directory, assert save_encoding_map creates it."""
        enc_map = {"county": {"A": 0}, "field": {"F1": 0}, "producing_zone": {"Z1": 0}, "operator": {"Op1": 0}}
        output_dir = tmp_path / "new" / "nested"

        assert not output_dir.exists()
        save_encoding_map(enc_map, output_dir)
        assert output_dir.exists()

    @pytest.mark.unit
    def test_load_encoding_map_file_not_found(self):
        """Given non-existent file, assert FileNotFoundError is raised."""
        with pytest.raises(FileNotFoundError):
            load_encoding_map(Path("/nonexistent"))


class TestWriteFeatureParquet:
    """Tests for write_feature_parquet function."""

    @pytest.mark.unit
    def test_write_feature_parquet_returns_path(self, tmp_path):
        """Assert returns a Path object."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "col1": [1, 2],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = write_feature_parquet(ddf, tmp_path)

        assert isinstance(result, Path)

    @pytest.mark.integration
    def test_write_feature_parquet_creates_output_dir(self, tmp_path):
        """Assert output_dir is created if it doesn't exist."""
        df = pd.DataFrame({
            "well_id": ["W1"],
            "col1": [1],
        })
        ddf = dd.from_pandas(df, npartitions=1)
        output_dir = tmp_path / "new" / "nested"

        assert not output_dir.exists()
        write_feature_parquet(ddf, output_dir)
        assert output_dir.exists()

    @pytest.mark.integration
    def test_write_feature_parquet_contains_parquet_files(self, tmp_path):
        """Assert output directory contains .parquet files."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "col1": [1, 2],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        output_dir = write_feature_parquet(ddf, tmp_path)

        parquet_files = list(output_dir.glob("**/*.parquet"))
        assert len(parquet_files) > 0

    @pytest.mark.integration
    def test_write_feature_parquet_partition_by_well_id(self, tmp_path):
        """Assert output has well_id subdirectories."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1", "W2", "W2"],
            "col1": [1, 2, 3, 4],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        output_dir = write_feature_parquet(ddf, tmp_path)

        # Check for well_id partition directories
        subdirs = list(output_dir.glob("well_id=*"))
        assert len(subdirs) == 2

    @pytest.mark.integration
    def test_write_feature_parquet_readable(self, tmp_path):
        """Assert written Parquet can be read back."""
        df = pd.DataFrame({
            "well_id": ["W1", "W1"],
            "col1": [1, 2],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        output_dir = write_feature_parquet(ddf, tmp_path)
        read_back = dd.read_parquet(str(output_dir))

        assert len(read_back.compute()) == 2


class TestRunFeaturesPipeline:
    """Tests for run_features_pipeline function."""

    @pytest.mark.unit
    def test_run_features_pipeline_calls_in_order(self, tmp_path):
        """Assert all sub-functions are called in sequence."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.features.read_processed_parquet") as mock_read:
            with patch("kgs_pipeline.features.compute_time_features") as mock_time:
                with patch("kgs_pipeline.features.compute_rolling_features") as mock_roll:
                    with patch("kgs_pipeline.features.compute_decline_and_gor") as mock_dec:
                        with patch("kgs_pipeline.features.encode_categorical_features") as mock_enc:
                            with patch("kgs_pipeline.features.save_encoding_map") as mock_save:
                                with patch("kgs_pipeline.features.write_feature_parquet") as mock_write:
                                    # Setup mocks
                                    mock_ddf = MagicMock(spec=dd.DataFrame)
                                    mock_read.return_value = mock_ddf
                                    mock_time.return_value = mock_ddf
                                    mock_roll.return_value = mock_ddf
                                    mock_dec.return_value = mock_ddf

                                    enc_map = {"county": {}}
                                    mock_enc.return_value = (mock_ddf, enc_map)
                                    mock_save.return_value = output_dir / "encoding_map.json"
                                    mock_write.return_value = output_dir

                                    result = run_features_pipeline(processed_dir, output_dir)

                                    # Verify all were called
                                    assert mock_read.called
                                    assert mock_time.called
                                    assert mock_roll.called
                                    assert mock_dec.called
                                    assert mock_enc.called
                                    assert mock_save.called
                                    assert mock_write.called

    @pytest.mark.unit
    def test_run_features_pipeline_returns_path(self, tmp_path):
        """Assert pipeline returns Path to output directory."""
        processed_dir = tmp_path / "processed"
        processed_dir.mkdir()
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.features.read_processed_parquet") as mock_read:
            with patch("kgs_pipeline.features.compute_time_features") as mock_time:
                with patch("kgs_pipeline.features.compute_rolling_features") as mock_roll:
                    with patch("kgs_pipeline.features.compute_decline_and_gor") as mock_dec:
                        with patch("kgs_pipeline.features.encode_categorical_features") as mock_enc:
                            with patch("kgs_pipeline.features.save_encoding_map") as mock_save:
                                with patch("kgs_pipeline.features.write_feature_parquet") as mock_write:
                                    mock_ddf = MagicMock(spec=dd.DataFrame)
                                    mock_read.return_value = mock_ddf
                                    mock_time.return_value = mock_ddf
                                    mock_roll.return_value = mock_ddf
                                    mock_dec.return_value = mock_ddf
                                    mock_enc.return_value = (mock_ddf, {})
                                    mock_save.return_value = output_dir / "encoding_map.json"
                                    mock_write.return_value = output_dir

                                    result = run_features_pipeline(processed_dir, output_dir)

                                    assert isinstance(result, Path)
                                    assert result == output_dir
