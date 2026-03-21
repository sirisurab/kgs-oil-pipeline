"""Central configuration for the KGS oil & gas pipeline."""

from pathlib import Path

# Project root: parent of kgs_pipeline directory
PROJECT_ROOT = (Path(__file__).parent.parent).resolve()

# Data directories
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
INTERIM_DATA_DIR = PROJECT_ROOT / "data" / "interim"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
EXTERNAL_DATA_DIR = PROJECT_ROOT / "data" / "external"

# Input files
LEASE_INDEX_FILE = EXTERNAL_DATA_DIR / "oil_leases_2020_present.txt"

# KGS URLs
KGS_BASE_URL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease?f_lc="
KGS_MONTH_SAVE_URL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave?f_lc="

# Scraping config
SCRAPE_CONCURRENCY = 5
SCRAPE_TIMEOUT_MS = 30000

# Production units
OIL_UNIT = "BBL"
GAS_UNIT = "MCF"
WATER_UNIT = "BBL"

# Domain constraints
MAX_REALISTIC_OIL_BBL_PER_MONTH = 50000.0

# Features output directory
FEATURES_DIR = PROCESSED_DATA_DIR / "features"
