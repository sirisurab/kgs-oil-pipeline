import pytest
from pathlib import Path
from io import StringIO
import pandas as pd
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from dask import delayed
from kgs_pipeline import config
from kgs_pipeline.acquire import (
    extract_lease_urls,
    scrape_lease_page,
    run_scrape_pipeline,
    ScrapeError,
)


class TestProjectScaffolding:
    """Tests for project scaffolding and config.py."""

    @pytest.mark.unit
    def test_project_root_exists(self):
        """Assert PROJECT_ROOT resolves to an existing directory."""
        assert config.PROJECT_ROOT.exists()
        assert config.PROJECT_ROOT.is_dir()

    @pytest.mark.unit
    def test_all_path_constants_are_path_instances(self):
        """Assert all Path constants are instances of pathlib.Path."""
        path_constants = [
            config.PROJECT_ROOT,
            config.RAW_DATA_DIR,
            config.INTERIM_DATA_DIR,
            config.PROCESSED_DATA_DIR,
            config.FEATURES_DATA_DIR,
            config.EXTERNAL_DATA_DIR,
            config.REFERENCES_DIR,
            config.OIL_LEASES_FILE,
        ]
        for path_const in path_constants:
            assert isinstance(path_const, Path)

    @pytest.mark.unit
    def test_importing_config_is_idempotent(self):
        """Assert importing config twice does not raise any exception."""
        import importlib
        try:
            importlib.reload(config)
        except Exception as e:
            pytest.fail(f"Reloading config raised {type(e).__name__}: {e}")

    @pytest.mark.unit
    def test_data_directories_created(self):
        """Assert that data directories exist after import."""
        assert config.RAW_DATA_DIR.exists()
        assert config.INTERIM_DATA_DIR.exists()
        assert config.PROCESSED_DATA_DIR.exists()
        assert config.FEATURES_DATA_DIR.exists()


class TestExtractLeaseUrls:
    """Tests for extract_lease_urls function."""

    @pytest.mark.unit
    def test_extract_lease_urls_basic(self, tmp_path):
        """Given a small CSV with 3 rows and 2 unique lease IDs, assert correct output."""
        csv_content = """LEASE_KID,URL
lease_001,https://example.com/lease1
lease_002,https://example.com/lease2
lease_001,https://example.com/lease1_dup
"""
        csv_file = tmp_path / "test_leases.csv"
        csv_file.write_text(csv_content)

        result = extract_lease_urls(csv_file)

        assert len(result) == 2
        assert list(result.columns) == ["lease_kid", "url"]
        assert result["lease_kid"].tolist() == ["lease_001", "lease_002"]

    @pytest.mark.unit
    def test_extract_lease_urls_deduplicates(self, tmp_path):
        """Given duplicates, assert deduplication produces one row per unique lease."""
        csv_content = """LEASE_KID,URL
A,https://a.com
B,https://b.com
A,https://a.com/alt
C,https://c.com
B,https://b.com/alt
"""
        csv_file = tmp_path / "test_leases.csv"
        csv_file.write_text(csv_content)

        result = extract_lease_urls(csv_file)

        assert len(result) == 3
        assert set(result["lease_kid"].unique()) == {"A", "B", "C"}

    @pytest.mark.unit
    def test_extract_lease_urls_file_not_found(self):
        """Given a non-existent file path, assert FileNotFoundError is raised."""
        with pytest.raises(FileNotFoundError):
            extract_lease_urls(Path("/nonexistent/file.csv"))

    @pytest.mark.unit
    def test_extract_lease_urls_missing_url_column(self, tmp_path):
        """Given a CSV missing the URL column, assert KeyError is raised."""
        csv_content = """LEASE_KID,OTHER_COLUMN
lease_001,value1
lease_002,value2
"""
        csv_file = tmp_path / "test_leases.csv"
        csv_file.write_text(csv_content)

        with pytest.raises(KeyError):
            extract_lease_urls(csv_file)

    @pytest.mark.unit
    def test_extract_lease_urls_filters_null_urls(self, tmp_path):
        """Given rows with null URLs, assert they are dropped."""
        csv_content = """LEASE_KID,URL
lease_001,https://example.com/lease1
lease_002,
lease_003,https://example.com/lease3
"""
        csv_file = tmp_path / "test_leases.csv"
        csv_file.write_text(csv_content)

        result = extract_lease_urls(csv_file)

        assert len(result) == 2
        assert set(result["lease_kid"].unique()) == {"lease_001", "lease_003"}

    @pytest.mark.integration
    def test_extract_lease_urls_real_file(self):
        """Given the real OIL_LEASES_FILE, assert valid output."""
        if not config.OIL_LEASES_FILE.exists():
            pytest.skip(f"OIL_LEASES_FILE not found at {config.OIL_LEASES_FILE}")

        result = extract_lease_urls(config.OIL_LEASES_FILE)

        assert len(result) > 0
        assert all(url.startswith("https://") for url in result["url"])
        assert list(result.columns) == ["lease_kid", "url"]


class TestScrapeLeasePage:
    """Tests for scrape_lease_page async function."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_scrape_lease_page_success(self, tmp_path):
        """Mock a successful page scrape and assert returns Path to .txt file."""
        lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
        
        # Mock page
        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.locator = MagicMock()
        mock_page.close = AsyncMock()

        # Mock button click
        mock_button_locator = MagicMock()
        mock_button_locator.first = MagicMock()
        mock_button_locator.first.click = AsyncMock()

        # Mock download link
        mock_link_locator = MagicMock()
        mock_link_locator.first = MagicMock()
        mock_link_locator.first.get_attribute = AsyncMock(
            return_value="/ords/oil.ogl5.download?f_lc=lp564.txt"
        )

        mock_page.locator.side_effect = [mock_button_locator, mock_link_locator]

        # Mock playwright instance
        mock_playwright = AsyncMock()
        mock_playwright.new_page = AsyncMock(return_value=mock_page)

        # Mock API request
        mock_response = AsyncMock()
        mock_response.ok = True
        mock_response.body = AsyncMock(return_value=b"test data")
        mock_api_request = AsyncMock()
        mock_api_request.get = AsyncMock()
        mock_api_request.get.return_value.__aenter__ = AsyncMock(
            return_value=mock_response
        )
        mock_api_request.get.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_playwright.api_request = mock_api_request

        semaphore = asyncio.Semaphore(1)
        result = await scrape_lease_page(lease_url, tmp_path, semaphore, mock_playwright)

        assert result is not None
        assert result.suffix == ".txt"
        assert result.parent == tmp_path

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_scrape_lease_page_missing_button(self, tmp_path):
        """Mock missing 'Save Monthly Data to File' button and assert ScrapeError."""
        lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
        
        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.locator = MagicMock()
        mock_page.close = AsyncMock()

        # Make button lookup raise an exception
        mock_button_locator = MagicMock()
        mock_button_locator.first.click = AsyncMock(side_effect=Exception("Button not found"))
        mock_page.locator.side_effect = [mock_button_locator]

        mock_playwright = AsyncMock()
        mock_playwright.new_page = AsyncMock(return_value=mock_page)

        semaphore = asyncio.Semaphore(1)
        with pytest.raises(ScrapeError):
            await scrape_lease_page(lease_url, tmp_path, semaphore, mock_playwright)

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_scrape_lease_page_missing_download_link(self, tmp_path):
        """Mock missing .txt link and assert returns None and logs warning."""
        lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
        
        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.locator = MagicMock()
        mock_page.close = AsyncMock()

        # Mock button click succeeds
        mock_button_locator = MagicMock()
        mock_button_locator.first = MagicMock()
        mock_button_locator.first.click = AsyncMock()

        # Mock download link not found
        mock_link_locator = MagicMock()
        mock_link_locator.first = MagicMock()
        mock_link_locator.first.get_attribute = AsyncMock(side_effect=Exception("Not found"))

        mock_page.locator.side_effect = [mock_button_locator, mock_link_locator]

        mock_playwright = AsyncMock()
        mock_playwright.new_page = AsyncMock(return_value=mock_page)

        semaphore = asyncio.Semaphore(1)
        result = await scrape_lease_page(lease_url, tmp_path, semaphore, mock_playwright)

        assert result is None

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_scrape_lease_page_file_exists_idempotent(self, tmp_path):
        """Assert that if file already exists, download is skipped."""
        lease_url = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc=1001135839"
        
        # Pre-create the output file
        existing_file = tmp_path / "lp564.txt"
        existing_file.write_text("existing data")

        mock_page = AsyncMock()
        mock_page.goto = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        mock_page.locator = MagicMock()
        mock_page.close = AsyncMock()

        # Mock button and link
        mock_button_locator = MagicMock()
        mock_button_locator.first = MagicMock()
        mock_button_locator.first.click = AsyncMock()

        mock_link_locator = MagicMock()
        mock_link_locator.first = MagicMock()
        mock_link_locator.first.get_attribute = AsyncMock(
            return_value="/ords/oil.ogl5.download?f_lc=lp564.txt"
        )

        mock_page.locator.side_effect = [mock_button_locator, mock_link_locator]

        mock_playwright = AsyncMock()
        mock_playwright.new_page = AsyncMock(return_value=mock_page)

        semaphore = asyncio.Semaphore(1)
        result = await scrape_lease_page(lease_url, tmp_path, semaphore, mock_playwright)

        assert result == existing_file
        # Verify existing file was not overwritten
        assert existing_file.read_text() == "existing data"


class TestRunScrapePipeline:
    """Tests for run_scrape_pipeline function."""

    @pytest.mark.unit
    def test_run_scrape_pipeline_success(self, tmp_path):
        """Mock extract_lease_urls and scrape functions, assert returns correct dict."""
        leases_file = tmp_path / "leases.csv"
        leases_file.write_text(
            "LEASE_KID,URL\n"
            "lease_001,https://example.com/1\n"
            "lease_002,https://example.com/2\n"
            "lease_003,https://example.com/3\n"
            "lease_004,https://example.com/4\n"
        )
        
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.acquire.extract_lease_urls") as mock_extract:
            with patch("kgs_pipeline.acquire._scrape_batch_wrapper") as mock_batch:
                # Setup extract_lease_urls mock
                mock_df = pd.DataFrame({
                    "lease_kid": ["lease_001", "lease_002", "lease_003", "lease_004"],
                    "url": [
                        "https://example.com/1",
                        "https://example.com/2",
                        "https://example.com/3",
                        "https://example.com/4",
                    ],
                })
                mock_extract.return_value = mock_df

                # Setup batch mock to return successful results
                def batch_side_effect(*args, **kwargs):
                    return {
                        "downloaded": [output_dir / f"lp{i}.txt" for i in range(1, 3)],
                        "failed": [],
                        "skipped": [],
                    }

                mock_batch.side_effect = batch_side_effect

                result = run_scrape_pipeline(leases_file, output_dir)

                assert "downloaded" in result
                assert "failed" in result
                assert "skipped" in result
                assert "total_leases" in result
                assert result["total_leases"] == 4

    @pytest.mark.unit
    def test_run_scrape_pipeline_some_failures(self, tmp_path):
        """Mock with some failed leases, assert dict tracks failures."""
        leases_file = tmp_path / "leases.csv"
        leases_file.write_text(
            "LEASE_KID,URL\n"
            "lease_001,https://example.com/1\n"
            "lease_002,https://example.com/2\n"
            "lease_003,https://example.com/3\n"
            "lease_004,https://example.com/4\n"
        )
        
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.acquire.extract_lease_urls") as mock_extract:
            with patch("kgs_pipeline.acquire._scrape_batch_wrapper") as mock_batch:
                mock_df = pd.DataFrame({
                    "lease_kid": ["lease_001", "lease_002", "lease_003", "lease_004"],
                    "url": [
                        "https://example.com/1",
                        "https://example.com/2",
                        "https://example.com/3",
                        "https://example.com/4",
                    ],
                })
                mock_extract.return_value = mock_df

                def batch_side_effect(*args, **kwargs):
                    return {
                        "downloaded": [output_dir / "lp1.txt", output_dir / "lp3.txt"],
                        "failed": ["https://example.com/2", "https://example.com/4"],
                        "skipped": [],
                    }

                mock_batch.side_effect = batch_side_effect

                result = run_scrape_pipeline(leases_file, output_dir)

                assert len(result["failed"]) == 2
                assert result["total_leases"] == 4

    @pytest.mark.unit
    def test_run_scrape_pipeline_delayed_objects(self, tmp_path):
        """Assert that Dask delayed tasks are created (lazy evaluation)."""
        leases_file = tmp_path / "leases.csv"
        leases_file.write_text(
            "LEASE_KID,URL\n"
            "lease_001,https://example.com/1\n"
            "lease_002,https://example.com/2\n"
        )
        
        output_dir = tmp_path / "output"

        with patch("kgs_pipeline.acquire.extract_lease_urls") as mock_extract:
            with patch("kgs_pipeline.acquire.dask.compute") as mock_compute:
                mock_df = pd.DataFrame({
                    "lease_kid": ["lease_001", "lease_002"],
                    "url": ["https://example.com/1", "https://example.com/2"],
                })
                mock_extract.return_value = mock_df
                
                mock_compute.return_value = [{
                    "downloaded": [],
                    "failed": [],
                    "skipped": [],
                }]

                run_scrape_pipeline(leases_file, output_dir)

                # Assert dask.compute was called
                mock_compute.assert_called_once()

    @pytest.mark.unit
    def test_run_scrape_pipeline_file_not_found(self, tmp_path):
        """Given non-existent leases file, assert FileNotFoundError is raised."""
        leases_file = tmp_path / "nonexistent.csv"
        output_dir = tmp_path / "output"

        with pytest.raises(FileNotFoundError):
            run_scrape_pipeline(leases_file, output_dir)
