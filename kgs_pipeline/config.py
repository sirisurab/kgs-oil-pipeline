from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
INTERIM_DATA_DIR = PROJECT_ROOT / "data" / "interim"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
FEATURES_DATA_DIR = PROCESSED_DATA_DIR / "features"
EXTERNAL_DATA_DIR = PROJECT_ROOT / "data" / "external"
REFERENCES_DIR = PROJECT_ROOT / "references"

OIL_LEASES_FILE = EXTERNAL_DATA_DIR / "oil_leases_2020_present.txt"

MAX_CONCURRENT_SCRAPES = 5

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
FEATURES_DATA_DIR.mkdir(parents=True, exist_ok=True)
