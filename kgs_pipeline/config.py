from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

RAW_DATA_DIR: Path = PROJECT_ROOT / "data" / "raw"
INTERIM_DATA_DIR: Path = PROJECT_ROOT / "data" / "interim"
PROCESSED_DATA_DIR: Path = PROJECT_ROOT / "data" / "processed"
FEATURES_DATA_DIR: Path = PROJECT_ROOT / "data" / "processed" / "features"
EXTERNAL_DATA_DIR: Path = PROJECT_ROOT / "data" / "external"
REFERENCES_DIR: Path = PROJECT_ROOT / "references"

OIL_LEASES_FILE: Path = EXTERNAL_DATA_DIR / "oil_leases_2020_present.txt"

MAX_CONCURRENT_SCRAPES: int = 5

# Create directories
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
INTERIM_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
FEATURES_DATA_DIR.mkdir(parents=True, exist_ok=True)
EXTERNAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
REFERENCES_DIR.mkdir(parents=True, exist_ok=True)
