"""Pipeline configuration constants — single source of truth for all components."""

from pathlib import Path

# ---------------------------------------------------------------------------
# Root and data directories
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).parent.parent

RAW_DATA_DIR = _PROJECT_ROOT / "data" / "raw"
INTERIM_DATA_DIR = _PROJECT_ROOT / "data" / "interim"
PROCESSED_DATA_DIR = _PROJECT_ROOT / "data" / "processed"
FEATURES_DATA_DIR = _PROJECT_ROOT / "data" / "processed" / "features"
EXTERNAL_DATA_DIR = _PROJECT_ROOT / "data" / "external"

# Auto-create all data directories on import
for _dir in (
    RAW_DATA_DIR,
    INTERIM_DATA_DIR,
    PROCESSED_DATA_DIR,
    FEATURES_DATA_DIR,
    EXTERNAL_DATA_DIR,
):
    _dir.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Reference files
# ---------------------------------------------------------------------------
LEASE_INDEX_FILE = _PROJECT_ROOT / "references" / "oil_leases_2020_present.txt"

# ---------------------------------------------------------------------------
# KGS URLs
# ---------------------------------------------------------------------------
KGS_BASE_URL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MainLease"
KGS_MONTH_SAVE_URL = "https://chasm.kgs.ku.edu/ords/oil.ogl5.MonthSave"

# ---------------------------------------------------------------------------
# Concurrency and Dask settings
# ---------------------------------------------------------------------------
MAX_CONCURRENT_REQUESTS: int = 5
DASK_N_WORKERS: int = 4

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL: str = "INFO"

# ---------------------------------------------------------------------------
# Transform-stage constants (Task 12)
# ---------------------------------------------------------------------------
OIL_OUTLIER_THRESHOLD_BBL: int = 50000
PARTITION_COLUMN_PROCESSED: str = "well_id"
INTERIM_PARTITION_COLUMN: str = "LEASE_KID"

COLUMN_RENAME_MAP: dict[str, str] = {
    "LEASE KID": "lease_kid",
    "LEASE_KID": "lease_kid",
    "LEASE": "lease_name",
    "DOR_CODE": "dor_code",
    "API_NUMBER": "api_number",
    "FIELD": "field_name",
    "PRODUCING_ZONE": "producing_zone",
    "OPERATOR": "operator",
    "COUNTY": "county",
    "TOWNSHIP": "township",
    "TWN_DIR": "twn_dir",
    "RANGE": "range_num",
    "RANGE_DIR": "range_dir",
    "SECTION": "section",
    "SPOT": "spot",
    "LATITUDE": "latitude",
    "LONGITUDE": "longitude",
    "MONTH-YEAR": "month_year",
    "PRODUCT": "product",
    "WELLS": "well_count",
    "PRODUCTION": "production",
    "source_file": "source_file",
    "URL": "url",
}

# ---------------------------------------------------------------------------
# Features-stage constants (Task 22)
# ---------------------------------------------------------------------------
DECLINE_RATE_CLIP_MIN: float = -1.0
DECLINE_RATE_CLIP_MAX: float = 10.0
ROLLING_WINDOW_SHORT: int = 3
ROLLING_WINDOW_LONG: int = 6
CATEGORICAL_COLUMNS: list[str] = ["county", "operator", "producing_zone", "field_name", "product"]
