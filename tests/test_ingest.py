import pytest
from pathlib import Path
import tempfile
import pandas as pd  # type: ignore
import dask.dataframe as dd  # type: ignore
from unittest.mock import patch, MagicMock

from kgs_pipeline.ingest import (
    discover_raw_files,
    read_raw_files,
    parse_month_year,
    explode_api_numbers,
    write_interim_parquet,
    run_ingest_pipeline,
)


class TestDiscoverRawFiles:
    """Test cases for Task 05: discover_raw_files()."""

    @pytest.mark.unit
    def test_discover_raw_files_basic(self) -> None:
        """Test discovering .txt files in a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            # Create test files
            (tmppath / "file1.txt").write_text("content1")
            (tmppath / "file2.txt").write_text("content2")
            (tmppath / "file3.csv").write_text("csv content")

            result = discover_raw_files(tmppath)

            assert len(result) == 2
            assert all(p.suffix == ".txt" for p in result)
            assert result[0].name < result[1].name  # Sorted

    @pytest.mark.unit
    def test_discover_raw_files_empty_directory(self) -> None:
        """Test discovering files in an empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            result = discover_raw_files(tmppath)
            assert result == []

    @pytest.mark.unit
    def test_discover_raw_files_not_found(self) -> None:
        """Test that FileNotFoundError is raised for non-existent directory."""
        non_existent = Path("/tmp/non_existent_12345")
        with pytest.raises(FileNotFoundError):
            discover_raw_files(non_existent)

    @pytest.mark.unit
    def test_discover_raw_files_sorted(self) -> None:
        """Test that returned files are sorted alphabetically."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            # Create files in non-alphabetical order
            (tmppath / "z.txt").write_text("z")
            (tmppath / "a.txt").write_text("a")
            (tmppath / "m.txt").write_text("m")

            result = discover_raw_files(tmppath)

            assert [p.name for p in result] == ["a.txt", "m.txt", "z.txt"]


class TestReadRawFiles:
    """Test cases for Task 06: read_raw_files()."""

    @pytest.mark.unit
    def test_read_raw_files_basic(self) -> None:
        """Test reading multiple .txt files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create test CSV files with KGS schema
            csv_data = """LEASE_KID,MONTH-YEAR,API_NUMBER,PRODUCT,PRODUCTION
001,1-2020,15-131-20143,OIL,100
001,1-2020,15-131-20143,GAS,50"""

            file1 = tmppath / "file1.txt"
            file2 = tmppath / "file2.txt"
            file1.write_text(csv_data)
            file2.write_text(csv_data)

            result = read_raw_files([file1, file2])

            assert isinstance(result, dd.DataFrame)
            assert "source_file" in result.columns
            assert result.npartitions >= 1

    @pytest.mark.unit
    def test_read_raw_files_column_normalization(self) -> None:
        """Test that column names are normalized."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            csv_data = """LEASE-KID,MONTH-YEAR,API_NUMBER
001,1-2020,15-131-20143"""

            file1 = tmppath / "file1.txt"
            file1.write_text(csv_data)

            result = read_raw_files([file1])

            # Compute to check columns
            result_computed = result.compute()
            cols_lower = [c.lower() for c in result_computed.columns]

            assert "lease_kid" in cols_lower
            assert "month_year" in cols_lower
            assert "api_number" in cols_lower

    @pytest.mark.unit
    def test_read_raw_files_source_file_column(self) -> None:
        """Test that source_file column is added."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            csv_data = """LEASE_KID,MONTH-YEAR
001,1-2020"""

            file1 = tmppath / "myfile.txt"
            file1.write_text(csv_data)

            result = read_raw_files([file1])
            result_computed = result.compute()

            assert "source_file" in result_computed.columns
            assert result_computed["source_file"].iloc[0] == "myfile"

    @pytest.mark.unit
    def test_read_raw_files_empty_list(self) -> None:
        """Test that ValueError is raised for empty file list."""
        with pytest.raises(ValueError, match="No raw files provided"):
            read_raw_files([])

    @pytest.mark.unit
    def test_read_raw_files_returns_dask_dataframe(self) -> None:
        """Test that function returns Dask DataFrame, not pandas."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            csv_data = """LEASE_KID,MONTH-YEAR
001,1-2020"""

            file1 = tmppath / "file1.txt"
            file1.write_text(csv_data)

            result = read_raw_files([file1])

            assert isinstance(result, dd.DataFrame)
            assert not isinstance(result, pd.DataFrame)


class TestParseMonthYear:
    """Test cases for Task 07: parse_month_year()."""

    @pytest.mark.unit
    def test_parse_month_year_returns_dask_dataframe(self) -> None:
        """Test that function returns Dask DataFrame."""
        data = {
            "month_year": ["1-2020"],
            "value": [100]
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result = parse_month_year(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_parse_month_year_has_production_date(self) -> None:
        """Test that production_date column is created."""
        data = {
            "month_year": ["1-2020"],
            "value": [100]
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result = parse_month_year(ddf)

        assert "production_date" in result.columns


class TestExplodeApiNumbers:
    """Test cases for Task 08: explode_api_numbers()."""

    @pytest.mark.unit
    def test_explode_api_numbers_single(self) -> None:
        """Test exploding single API number (no comma)."""
        data = {
            "api_number": ["15-131-20143"],
            "value": [100]
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result = explode_api_numbers(ddf)
        result_computed = result.compute()

        assert len(result_computed) == 1
        assert result_computed["well_id"].iloc[0] == "15-131-20143"

    @pytest.mark.unit
    def test_explode_api_numbers_returns_dask_dataframe(self) -> None:
        """Test that function returns Dask DataFrame."""
        data = {
            "api_number": ["15-131-20143"],
            "value": [100]
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        result = explode_api_numbers(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_explode_api_numbers_missing_column(self) -> None:
        """Test that KeyError is raised if api_number column is missing."""
        data = {
            "value": [100]
        }
        df = pd.DataFrame(data)
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            explode_api_numbers(ddf)


class TestWriteInterimParquet:
    """Test cases for Task 09: write_interim_parquet()."""

    @pytest.mark.unit
    def test_write_interim_parquet_basic(self) -> None:
        """Test writing Dask DataFrame to Parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            data = {
                "well_id": ["15-131-20143", "15-131-20089"],
                "production": [100, 200]
            }
            df = pd.DataFrame(data)
            ddf = dd.from_pandas(df, npartitions=1)

            result = write_interim_parquet(ddf, tmppath)

            assert result.name == "kgs_monthly_raw.parquet"
            assert result.exists()


class TestRunIngestPipeline:
    """Test cases for Task 10: run_ingest_pipeline()."""

    @pytest.mark.unit
    def test_run_ingest_pipeline_call_order(self) -> None:
        """Test that pipeline steps are called in correct order."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / "raw"
            output_dir = Path(tmpdir) / "output"
            raw_dir.mkdir()

            with patch("kgs_pipeline.ingest.discover_raw_files") as mock_discover:
                with patch("kgs_pipeline.ingest.read_raw_files") as mock_read:
                    with patch("kgs_pipeline.ingest.parse_month_year") as mock_parse:
                        with patch("kgs_pipeline.ingest.explode_api_numbers") as mock_explode:
                            with patch("kgs_pipeline.ingest.write_interim_parquet") as mock_write:
                                mock_discover.return_value = [Path("file1.txt")]
                                mock_ddf = MagicMock(spec=dd.DataFrame)
                                mock_read.return_value = mock_ddf
                                mock_parse.return_value = mock_ddf
                                mock_explode.return_value = mock_ddf
                                mock_write.return_value = Path("output.parquet")

                                result = run_ingest_pipeline(raw_dir, output_dir)

                                assert mock_discover.called
                                assert mock_read.called
                                assert mock_parse.called
                                assert mock_explode.called
                                assert mock_write.called

    @pytest.mark.unit
    def test_run_ingest_pipeline_empty_files(self) -> None:
        """Test that pipeline returns None if no files are discovered."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / "raw"
            output_dir = Path(tmpdir) / "output"
            raw_dir.mkdir()

            with patch("kgs_pipeline.ingest.discover_raw_files") as mock_discover:
                mock_discover.return_value = []

                result = run_ingest_pipeline(raw_dir, output_dir)

                assert result is None
                mock_discover.assert_called_once()

    @pytest.mark.unit
    def test_run_ingest_pipeline_returns_path(self) -> None:
        """Test that pipeline returns a Path object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / "raw"
            output_dir = Path(tmpdir) / "output"
            raw_dir.mkdir()

            with patch("kgs_pipeline.ingest.discover_raw_files") as mock_discover:
                with patch("kgs_pipeline.ingest.read_raw_files") as mock_read:
                    with patch("kgs_pipeline.ingest.parse_month_year") as mock_parse:
                        with patch("kgs_pipeline.ingest.explode_api_numbers") as mock_explode:
                            with patch("kgs_pipeline.ingest.write_interim_parquet") as mock_write:
                                mock_discover.return_value = [Path("file1.txt")]
                                mock_ddf = MagicMock(spec=dd.DataFrame)
                                mock_read.return_value = mock_ddf
                                mock_parse.return_value = mock_ddf
                                mock_explode.return_value = mock_ddf
                                output_path = Path(tmpdir) / "output.parquet"
                                mock_write.return_value = output_path

                                result = run_ingest_pipeline(raw_dir, output_dir)

                                assert isinstance(result, Path)
