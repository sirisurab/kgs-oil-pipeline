"""Pipeline configuration constants and defaults."""

from pathlib import Path

# Project root
ROOT_DIR = Path(__file__).parent.parent

# Data directories
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
CLEAN_DIR = DATA_DIR / "processed" / "clean"
FEATURES_DIR = DATA_DIR / "processed" / "features"
EXTERNAL_DIR = DATA_DIR / "external"
LOGS_DIR = ROOT_DIR / "logs"

# Default index file (filtered 2024-present)
LEASE_INDEX_FILE = EXTERNAL_DIR / "oil_leases_2024_present.txt"

# KGS URLs
KGS_MAIN_LEASE_URL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease"
KGS_MONTH_SAVE_URL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave"

# Acquire settings
MIN_YEAR = 2024
MAX_WORKERS = 5
RETRY_MAX = 3
RETRY_BASE_DELAY = 1.0
RETRY_FACTOR = 2.0

# Transform settings
IQR_MULTIPLIER = 1.5
PRODUCTION_UNIT_ERROR_THRESHOLD = 50_000.0

# Partition settings
ROWS_PER_PARTITION = 500_000
MAX_PARTITIONS = 200
MAX_READ_PARTITIONS = 50

# Required schema columns
REQUIRED_COLUMNS = [
    "LEASE_KID",
    "LEASE",
    "API_NUMBER",
    "MONTH-YEAR",
    "PRODUCT",
    "PRODUCTION",
    "WELLS",
    "OPERATOR",
    "COUNTY",
    "source_file",
]

# String columns to standardise
STRING_COLS = [
    "LEASE",
    "OPERATOR",
    "COUNTY",
    "PRODUCT",
    "PRODUCING_ZONE",
    "FIELD",
    "API_NUMBER",
    "LEASE_KID",
    "SPOT",
    "TWN_DIR",
    "RANGE_DIR",
]
