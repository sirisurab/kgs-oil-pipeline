"""Test cases for acquire component."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from kgs_pipeline.acquire import (
    ScrapingError,
    load_lease_urls,
)


@pytest.fixture
def temp_lease_index():
    """Create a temporary lease index CSV for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        index_file = Path(tmpdir) / "leases.csv"
        df = pd.DataFrame(
            {
                "LEASE_KID": ["L001", "L002", "L003", "L001"],  # L001 duplicated
                "URL": [
                    "https://example.com/lease/L001",
                    "https://example.com/lease/L002",
                    None,  # L003 has null URL
                    "https://example.com/lease/L001_alt",
                ],
            }
        )
        df.to_csv(index_file, index=False)
        yield index_file


def test_load_lease_urls_success(temp_lease_index):
    """Given valid lease CSV, return list of lease dicts with deduplication."""
    result = load_lease_urls(temp_lease_index)

    # Should deduplicate on LEASE_KID and drop null URLs
    assert len(result) == 2  # L001 and L002 (L003 has null URL)
    lease_kids = {r["lease_kid"] for r in result}
    assert lease_kids == {"L001", "L002"}
    assert all("url" in r and "lease_kid" in r for r in result)


def test_load_lease_urls_file_not_found():
    """Given missing file, raise FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_lease_urls(Path("/nonexistent/path/leases.csv"))


def test_load_lease_urls_missing_lease_kid_column():
    """Given missing LEASE_KID column, raise KeyError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        index_file = Path(tmpdir) / "bad_leases.csv"
        df = pd.DataFrame({"URL": ["https://example.com/L001"]})
        df.to_csv(index_file, index=False)

        with pytest.raises(KeyError, match="LEASE_KID"):
            load_lease_urls(index_file)


def test_load_lease_urls_missing_url_column():
    """Given missing URL column, raise KeyError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        index_file = Path(tmpdir) / "bad_leases.csv"
        df = pd.DataFrame({"LEASE_KID": ["L001"]})
        df.to_csv(index_file, index=False)

        with pytest.raises(KeyError, match="URL"):
            load_lease_urls(index_file)


def test_load_lease_urls_empty_after_dedup():
    """Given all null URLs, return empty list."""
    with tempfile.TemporaryDirectory() as tmpdir:
        index_file = Path(tmpdir) / "empty_leases.csv"
        df = pd.DataFrame(
            {
                "LEASE_KID": ["L001", "L002"],
                "URL": [None, None],
            }
        )
        df.to_csv(index_file, index=False)

        result = load_lease_urls(index_file)
        assert result == []
