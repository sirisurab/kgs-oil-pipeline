import pytest
from pathlib import Path
import pandas as pd  # type: ignore[import-untyped]
import dask.dataframe as dd
from unittest.mock import patch, MagicMock
from kgs_pipeline import config
from kgs_pipeline.ingest import (
    discover_raw_files,
    read_raw_files,
    parse_month_year,
    explode_api_numbers,
    write_interim_parquet,
    run_ingest_pipeline,
)


class TestDiscoverRawFiles:
    """Tests for discover_raw_files function."""

    @pytest.mark.unit
    def test_discover_raw_files_finds_txt_files(self, tmp_path: Path) -> None:
        """Given a temp dir with 3 .txt and 1 .csv, assert returns 3 Path objects."""
        # Create test files
        txt_files = [tmp_path / f"lease_{i}.txt" for i in range(3)]
        for txt_file in txt_files:
            txt_file.write_text("test data")

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("test csv")

        result = discover_raw_files(tmp_path)

        assert len(result) == 3
        assert all(f.suffix == ".txt" for f in result)

    @pytest.mark.unit
    def test_discover_raw_files_sorted(self, tmp_path: Path) -> None:
        """Assert returned list is sorted alphabetically."""
        txt_files = ["c.txt", "a.txt", "b.txt"]
        for fname in txt_files:
            (tmp_path / fname).write_text("data")

        result = discover_raw_files(tmp_path)

        assert [f.name for f in result] == ["a.txt", "b.txt", "c.txt"]

    @pytest.mark.unit
    def test_discover_raw_files_empty_dir(self, tmp_path: Path) -> None:
        """Given an empty directory, assert returns [] and does not raise."""
        result = discover_raw_files(tmp_path)

        assert result == []

    @pytest.mark.unit
    def test_discover_raw_files_not_found(self) -> None:
        """Given non-existent path, assert FileNotFoundError is raised."""
        with pytest.raises(FileNotFoundError):
            discover_raw_files(Path("/nonexistent/path"))

    @pytest.mark.integration
    def test_discover_raw_files_real_dir(self) -> None:
        """Given RAW_DATA_DIR, if populated, assert non-empty list; else skip."""
        if not config.RAW_DATA_DIR.exists():
            pytest.skip(f"RAW_DATA_DIR does not exist: {config.RAW_DATA_DIR}")

        result = discover_raw_files(config.RAW_DATA_DIR)

        if not list(config.RAW_DATA_DIR.glob("*.txt")):
            pytest.skip("No .txt files in RAW_DATA_DIR")

        assert len(result) > 0


class TestReadRawFiles:
    """Tests for read_raw_files function."""

    @pytest.mark.unit
    def test_read_raw_files_basic(self, tmp_path: Path) -> None:
        """Given 2 small .txt files, assert returns dask.dataframe.DataFrame."""
        # Create test CSV files
        csv_content_1 = "LEASE_KID,MONTH-YEAR,PRODUCTION\nL001,1-2020,100\n"
        csv_content_2 = "LEASE_KID,MONTH-YEAR,PRODUCTION\nL002,2-2020,200\n"

        f1 = tmp_path / "lease_1.txt"
        f2 = tmp_path / "lease_2.txt"
        f1.write_text(csv_content_1)
        f2.write_text(csv_content_2)

        result = read_raw_files([f1, f2])

        assert isinstance(result, dd.DataFrame)
        assert len(result.compute()) == 2

    @pytest.mark.unit
    def test_read_raw_files_has_source_file_column(self, tmp_path: Path) -> None:
        """Assert the returned DataFrame has source_file column."""
        csv_content = "LEASE_KID,MONTH-YEAR\nL001,1-2020\n"
        f1 = tmp_path / "test_lease.txt"
        f1.write_text(csv_content)

        result = read_raw_files([f1])
        computed = result.compute()

        assert "source_file" in computed.columns
        assert computed["source_file"].iloc[0] == "test_lease"

    @pytest.mark.unit
    def test_read_raw_files_lowercase_columns(self, tmp_path: Path) -> None:
        """Assert all column names are lowercase with no hyphens or spaces."""
        csv_content = "LEASE-KID,MONTH YEAR,API_NUMBER\nL001,1-2020,123\n"
        f1 = tmp_path / "test.txt"
        f1.write_text(csv_content)

        result = read_raw_files([f1])
        computed = result.compute()

        col_names = list(computed.columns)
        assert all(c.islower() for c in col_names)
        assert all("-" not in c and " " not in c for c in col_names)

    @pytest.mark.unit
    def test_read_raw_files_empty_list_raises(self) -> None:
        """Given empty file_paths list, assert ValueError is raised."""
        with pytest.raises(ValueError):
            read_raw_files([])

    @pytest.mark.unit
    def test_read_raw_files_returns_dask_not_pandas(self, tmp_path: Path) -> None:
        """Assert return type is dask.dataframe.DataFrame, not pandas."""
        csv_content = "LEASE_KID,MONTH-YEAR\nL001,1-2020\n"
        f1 = tmp_path / "test.txt"
        f1.write_text(csv_content)

        result = read_raw_files([f1])

        assert isinstance(result, dd.DataFrame)
        assert not isinstance(result, pd.DataFrame)

    @pytest.mark.unit
    def test_read_raw_files_skips_corrupt_file(self, tmp_path: Path, caplog) -> None:  # type: ignore[name-defined]
        """Given one corrupt and one valid file, assert warns and reads the valid one."""
        csv_content_1 = "LEASE_KID,MONTH-YEAR\nL001,1-2020\n"
        csv_content_2 = "invalid\x00binary\x00content"  # Corrupt

        f1 = tmp_path / "valid.txt"
        f2 = tmp_path / "corrupt.txt"
        f1.write_text(csv_content_1)
        f2.write_bytes(csv_content_2.encode())

        with caplog.at_level("WARNING"):  # type: ignore[attr-defined]
            result = read_raw_files([f1, f2])

        # Should still have read the valid file
        assert len(result.compute()) >= 1


class TestParseMonthYear:
    """Tests for parse_month_year function."""

    @pytest.mark.unit
    def test_parse_month_year_filters_yearly_and_cumulative(self) -> None:
        """Given rows with 0 and -1 month codes, assert they are filtered out."""
        df = pd.DataFrame({
            "month_year": ["1-2020", "0-2020", "-1-2020", "12-2023"],
            "data": ["a", "b", "c", "d"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = parse_month_year(ddf).compute()

        assert len(result) == 2
        assert list(result["production_date"]) == [
            pd.Timestamp("2020-01-01"),
            pd.Timestamp("2023-12-01"),
        ]

    @pytest.mark.unit
    def test_parse_month_year_dtype_is_datetime(self) -> None:
        """Assert production_date column dtype is datetime64[ns]."""
        df = pd.DataFrame({"month_year": ["1-2020", "2-2020"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = parse_month_year(ddf)

        # Dask may use datetime64[s] or datetime64[ns]
        assert "datetime64" in str(result["production_date"].dtype)

    @pytest.mark.unit
    def test_parse_month_year_parsing_examples(self) -> None:
        """Assert specific month-year strings parse correctly."""
        df = pd.DataFrame({"month_year": ["1-2020", "10-2023"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = parse_month_year(ddf).compute()

        assert result["production_date"].iloc[0] == pd.Timestamp("2020-01-01")
        assert result["production_date"].iloc[1] == pd.Timestamp("2023-10-01")

    @pytest.mark.unit
    def test_parse_month_year_invalid_value_produces_nat(self) -> None:
        """Given invalid month_year, assert it produces NaT without exception."""
        df = pd.DataFrame({"month_year": ["1-2020", "99-9999", "13-2020"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = parse_month_year(ddf).compute()

        # Invalid dates should be NaT, not raise
        assert pd.isna(result["production_date"].iloc[1])

    @pytest.mark.unit
    def test_parse_month_year_returns_dask_dataframe(self) -> None:
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({"month_year": ["1-2020", "2-2020"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = parse_month_year(ddf)

        assert isinstance(result, dd.DataFrame)


class TestExplodeApiNumbers:
    """Tests for explode_api_numbers function."""

    @pytest.mark.unit
    def test_explode_api_numbers_basic(self) -> None:
        """Given row with comma-separated API numbers, assert exploded correctly."""
        df = pd.DataFrame({
            "api_number": ["15-131-20143, 15-131-20089"],
            "lease_kid": ["L001"],
        })
        ddf = dd.from_pandas(df, npartitions=1)

        result = explode_api_numbers(ddf).compute()

        assert len(result) == 2
        assert "well_id" in result.columns
        assert "api_number" not in result.columns
        assert set(result["well_id"]) == {"15-131-20143", "15-131-20089"}

    @pytest.mark.unit
    def test_explode_api_numbers_single_api(self) -> None:
        """Given single API number (no comma), assert produces 1 row."""
        df = pd.DataFrame({"api_number": ["15-131-20143"], "data": ["x"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = explode_api_numbers(ddf).compute()

        assert len(result) == 1
        assert result["well_id"].iloc[0] == "15-131-20143"

    @pytest.mark.unit
    def test_explode_api_numbers_null_api(self) -> None:
        """Given row with null api_number, assert produces 1 row with well_id=None."""
        df = pd.DataFrame({"api_number": [None], "data": ["x"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = explode_api_numbers(ddf).compute()

        assert len(result) == 1
        assert pd.isna(result["well_id"].iloc[0])

    @pytest.mark.unit
    def test_explode_api_numbers_returns_dask_dataframe(self) -> None:
        """Assert return type is dask.dataframe.DataFrame."""
        df = pd.DataFrame({"api_number": ["15-131-20143, 15-131-20089"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = explode_api_numbers(ddf)

        assert isinstance(result, dd.DataFrame)

    @pytest.mark.unit
    def test_explode_api_numbers_column_renamed(self) -> None:
        """Assert api_number is renamed to well_id and api_number is removed."""
        df = pd.DataFrame({"api_number": ["15-131-20143, 15-131-20089"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = explode_api_numbers(ddf).compute()

        assert "well_id" in result.columns
        assert "api_number" not in result.columns

    @pytest.mark.unit
    def test_explode_api_numbers_missing_column_raises(self) -> None:
        """Given DataFrame missing api_number column, assert KeyError is raised."""
        df = pd.DataFrame({"other_col": ["value"]})
        ddf = dd.from_pandas(df, npartitions=1)

        with pytest.raises(KeyError):
            explode_api_numbers(ddf)


class TestWriteInterimParquet:
    """Tests for write_interim_parquet function."""

    @pytest.mark.unit
    def test_write_interim_parquet_returns_path(self, tmp_path: Path) -> None:
        """Given a small DataFrame, assert returns Path ending with 'kgs_monthly_raw.parquet'."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        ddf = dd.from_pandas(df, npartitions=1)

        result = write_interim_parquet(ddf, tmp_path)

        assert isinstance(result, Path)
        assert result.name == "kgs_monthly_raw.parquet"

    @pytest.mark.unit
    def test_write_interim_parquet_creates_output_dir(self, tmp_path: Path) -> None:
        """Assert output_dir is created if it doesn't exist."""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        ddf = dd.from_pandas(df, npartitions=1)
        output_dir = tmp_path / "new" / "nested" / "dir"

        assert not output_dir.exists()
        write_interim_parquet(ddf, output_dir)
        assert output_dir.exists()

    @pytest.mark.integration
    def test_write_interim_parquet_readable(self, tmp_path: Path) -> None:
        """After writing, assert the parquet directory contains readable data."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        ddf = dd.from_pandas(df, npartitions=1)

        output_path = write_interim_parquet(ddf, tmp_path)

        # Should be readable by read_parquet
        read_back = dd.read_parquet(str(output_path))
        assert len(read_back.compute()) == len(df)

    @pytest.mark.integration
    def test_write_interim_parquet_preserves_rows(self, tmp_path: Path) -> None:
        """Assert row count is preserved after write and read."""
        original_df = pd.DataFrame({"col1": [1, 2, 3, 4, 5]})
        ddf = dd.from_pandas(original_df, npartitions=2)

        output_path = write_interim_parquet(ddf, tmp_path)
        read_back = dd.read_parquet(str(output_path)).compute()

        assert len(read_back) == len(original_df)


class TestRunIngestPipeline:
    """Tests for run_ingest_pipeline function."""

    @pytest.mark.unit
    def test_run_ingest_pipeline_calls_functions_in_order(self, tmp_path: Path) -> None:
        """Mock sub-functions and assert they are called in correct order."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.ingest.discover_raw_files") as mock_discover:
            with patch("kgs_pipeline.ingest.read_raw_files") as mock_read:
                with patch("kgs_pipeline.ingest.parse_month_year") as mock_parse:
                    with patch("kgs_pipeline.ingest.explode_api_numbers") as mock_explode:
                        with patch("kgs_pipeline.ingest.write_interim_parquet") as mock_write:
                            # Setup mock returns
                            test_file = Path("test.txt")
                            mock_discover.return_value = [test_file]
                            
                            mock_ddf = MagicMock(spec=dd.DataFrame)
                            mock_read.return_value = mock_ddf
                            mock_parse.return_value = mock_ddf
                            mock_explode.return_value = mock_ddf

                            output_parquet = output_dir / "kgs_monthly_raw.parquet"
                            mock_write.return_value = output_parquet

                            result = run_ingest_pipeline(raw_dir, output_dir)

                            # Verify call order
                            assert mock_discover.called
                            assert mock_read.called
                            assert mock_parse.called
                            assert mock_explode.called
                            assert mock_write.called
                            assert result == output_parquet

    @pytest.mark.unit
    def test_run_ingest_pipeline_empty_files_returns_none(self, tmp_path: Path) -> None:
        """Given empty file list, assert returns None and doesn't call read."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.ingest.discover_raw_files") as mock_discover:
            with patch("kgs_pipeline.ingest.read_raw_files") as mock_read:
                mock_discover.return_value = []

                result = run_ingest_pipeline(raw_dir, output_dir)

                assert result is None
                assert not mock_read.called

    @pytest.mark.unit
    def test_run_ingest_pipeline_no_compute_on_intermediates(self, tmp_path: Path) -> None:
        """Assert intermediate Dask DataFrames are not computed (lazy evaluation)."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.ingest.discover_raw_files") as mock_discover:
            with patch("kgs_pipeline.ingest.read_raw_files") as mock_read:
                with patch("kgs_pipeline.ingest.parse_month_year") as mock_parse:
                    with patch("kgs_pipeline.ingest.explode_api_numbers") as mock_explode:
                        with patch("kgs_pipeline.ingest.write_interim_parquet") as mock_write:
                            test_file = Path("test.txt")
                            mock_discover.return_value = [test_file]

                            # Return Dask DataFrame mocks
                            mock_ddf = MagicMock(spec=dd.DataFrame)
                            mock_ddf.npartitions = 1
                            mock_read.return_value = mock_ddf
                            mock_parse.return_value = mock_ddf
                            mock_explode.return_value = mock_ddf

                            mock_write.return_value = output_dir / "kgs_monthly_raw.parquet"

                            run_ingest_pipeline(raw_dir, output_dir)

                            # Verify that .compute() was not called on intermediate DataFrames
                            mock_ddf.compute.assert_not_called()

    @pytest.mark.integration
    def test_run_ingest_pipeline_real_raw_dir(self) -> None:
        """Given actual RAW_DATA_DIR, if populated, assert output is created."""
        if not config.RAW_DATA_DIR.exists():
            pytest.skip(f"RAW_DATA_DIR not found: {config.RAW_DATA_DIR}")

        if not list(config.RAW_DATA_DIR.glob("*.txt")):
            pytest.skip("No .txt files in RAW_DATA_DIR")

        result = run_ingest_pipeline(config.RAW_DATA_DIR, config.INTERIM_DATA_DIR)

        if result is not None:
            assert result.exists()
            assert result.name == "kgs_monthly_raw.parquet"
