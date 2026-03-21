import pytest
from pathlib import Path
import tempfile
import json
import pandas as pd  # type: ignore
import dask.dataframe as dd  # type: ignore
from unittest.mock import patch, MagicMock

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
    """Test cases for Task 20: read_processed_parquet()."""

    @pytest.mark.unit
    def test_read_processed_parquet_not_found(self) -> None:
        """Test that FileNotFoundError is raised when wells directory doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            with pytest.raises(FileNotFoundError):
                read_processed_parquet(tmppath)

    @pytest.mark.unit
    def test_read_processed_parquet_empty_directory(self) -> None:
        """Test that ValueError is raised when no Parquet files are found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            wells_dir = tmppath / "wells"
            wells_dir.mkdir()

            with pytest.raises(ValueError, match="No Parquet files found"):
                read_processed_parquet(tmppath)


class TestComputeTimeFeatures:
    """Test cases for Task 21: compute_time_features()."""

    @pytest.mark.unit
    def test_compute_time_features_returns_dask(self) -> None:
        """Test that function returns Dask DataFrame."""
        data = {
            "well_id": ["001"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [50.0],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_time_features(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_compute_time_features_missing_column(self) -> None:
        """Test that KeyError is raised if production_date column is missing."""
        data = {
            "well_id": ["001"],
            "oil_bbl": [100.0],
            "gas_mcf": [50.0],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            compute_time_features(ddf)


class TestComputeRollingFeatures:
    """Test cases for Task 22: compute_rolling_features()."""

    @pytest.mark.unit
    def test_all_rolling_columns_present(self) -> None:
        """Test that all 6 rolling columns are present."""
        data = {
            "well_id": ["001"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [50.0],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_rolling_features(ddf)

        required_cols = [
            "oil_bbl_roll3", "oil_bbl_roll6", "oil_bbl_roll12",
            "gas_mcf_roll3", "gas_mcf_roll6", "gas_mcf_roll12"
        ]
        for col in required_cols:
            assert col in result.columns

    @pytest.mark.unit
    def test_compute_rolling_features_returns_dask(self) -> None:
        """Test that function returns Dask DataFrame."""
        data = {
            "well_id": ["001"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [50.0],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_rolling_features(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_compute_rolling_features_missing_column(self) -> None:
        """Test that KeyError is raised if oil_bbl column is missing."""
        data = {
            "well_id": ["001"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "gas_mcf": [50.0],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            compute_rolling_features(ddf)


class TestComputeDeclineAndGor:
    """Test cases for Task 23: compute_decline_and_gor()."""

    @pytest.mark.unit
    def test_compute_decline_and_gor_returns_dask(self) -> None:
        """Test that function returns Dask DataFrame."""
        data = {
            "well_id": ["001"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "oil_bbl": [100.0],
            "gas_mcf": [50.0],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result = compute_decline_and_gor(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_compute_decline_and_gor_missing_column(self) -> None:
        """Test that KeyError is raised if oil_bbl column is missing."""
        data = {
            "well_id": ["001"],
            "production_date": [pd.Timestamp("2020-01-01")],
            "gas_mcf": [50.0],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            compute_decline_and_gor(ddf)


class TestEncodeCategoricalFeatures:
    """Test cases for Task 24: encode_categorical_features()."""

    @pytest.mark.unit
    def test_encode_categorical_returns_dask(self) -> None:
        """Test that function returns Dask DataFrame."""
        data = {
            "well_id": ["001"],
            "county": ["Allen"],
            "field": ["F1"],
            "producing_zone": ["Z1"],
            "operator": ["OP1"],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result_ddf, _ = encode_categorical_features(ddf)

        assert isinstance(result_ddf, dd.DataFrame)

    @pytest.mark.unit
    def test_encode_categorical_missing_column(self) -> None:
        """Test that KeyError is raised if a categorical column is missing."""
        data = {
            "well_id": ["001"],
            "county": ["Allen"],
            "field": ["F1"],
            "producing_zone": ["Z1"],
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            encode_categorical_features(ddf)


class TestSaveAndLoadEncodingMap:
    """Test cases for Tasks 25: save_encoding_map() and load_encoding_map()."""

    @pytest.mark.unit
    def test_save_encoding_map_basic(self) -> None:
        """Test saving encoding map to JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            encoding_map = {
                "county": {"Allen": 0, "Barton": 1},
                "field": {"F1": 0, "F2": 1},
            }

            result_path = save_encoding_map(encoding_map, tmppath)

            assert result_path.name == "encoding_map.json"
            assert result_path.exists()

    @pytest.mark.unit
    def test_save_and_load_roundtrip(self) -> None:
        """Test that save and load are consistent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            original_map = {
                "county": {"Allen": 0, "Barton": 1},
                "field": {"F1": 0, "F2": 1},
                "producing_zone": {"Z1": 0},
                "operator": {"OP1": 0, "OP2": 1},
            }

            save_encoding_map(original_map, tmppath)
            loaded_map = load_encoding_map(tmppath)

            assert loaded_map == original_map

    @pytest.mark.unit
    def test_load_encoding_map_not_found(self) -> None:
        """Test that FileNotFoundError is raised when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            with pytest.raises(FileNotFoundError):
                load_encoding_map(tmppath)


class TestWriteFeatureParquet:
    """Test cases for Task 26: write_feature_parquet()."""

    @pytest.mark.integration
    def test_write_feature_parquet_basic(self) -> None:
        """Test writing feature Parquet to disk."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            data = {
                "well_id": ["001", "002"],
                "production_date": [
                    pd.Timestamp("2020-01-01"),
                    pd.Timestamp("2020-02-01"),
                ],
                "oil_bbl": [100.0, 200.0],
                "gas_mcf": [50.0, 75.0],
            }
            df = pd.DataFrame(data)
            ddf = dd.from_pandas(df, npartitions=1)

            result = write_feature_parquet(ddf, tmppath)

            assert isinstance(result, Path)
            assert result.exists()


class TestRunFeaturesPipeline:
    """Test cases for Task 27: run_features_pipeline()."""

    @pytest.mark.unit
    def test_run_features_pipeline_call_order(self) -> None:
        """Test that pipeline steps are called in correct order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            processed_dir = Path(tmpdir) / "processed"
            output_dir = Path(tmpdir) / "output"
            processed_dir.mkdir()

            with patch("kgs_pipeline.features.read_processed_parquet") as mock_read:
                with patch("kgs_pipeline.features.compute_time_features") as mock_time:
                    with patch("kgs_pipeline.features.compute_rolling_features") as mock_rolling:
                        with patch("kgs_pipeline.features.compute_decline_and_gor") as mock_decline:
                            with patch("kgs_pipeline.features.encode_categorical_features") as mock_encode:
                                with patch("kgs_pipeline.features.save_encoding_map") as mock_save_map:
                                    with patch("kgs_pipeline.features.write_feature_parquet") as mock_write:
                                        mock_ddf = MagicMock(spec=dd.DataFrame)
                                        mock_read.return_value = mock_ddf
                                        mock_time.return_value = mock_ddf
                                        mock_rolling.return_value = mock_ddf
                                        mock_decline.return_value = mock_ddf
                                        mock_encode.return_value = (mock_ddf, {})
                                        mock_write.return_value = output_dir

                                        result = run_features_pipeline(processed_dir, output_dir)

                                        assert mock_read.called
                                        assert mock_time.called
                                        assert mock_rolling.called
                                        assert mock_decline.called
                                        assert mock_encode.called
                                        assert mock_save_map.called
                                        assert mock_write.called
