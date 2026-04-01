"""Pipeline orchestrator — runs the full KGS pipeline from acquire to features."""

import argparse
import logging
import time
from pathlib import Path

from kgs_pipeline.config import (
    CLEAN_DIR,
    FEATURES_DIR,
    INTERIM_DIR,
    IQR_MULTIPLIER,
    LEASE_INDEX_FILE,
    MAX_WORKERS,
    RAW_DIR,
)
from kgs_pipeline.utils import setup_logging

logger = setup_logging(__name__, level=logging.INFO)


def run_pipeline(
    index_path: str | None = None,
    raw_dir: str | None = None,
    interim_dir: str | None = None,
    clean_dir: str | None = None,
    features_dir: str | None = None,
    run_acquire: bool = True,
    run_ingest: bool = True,
    run_transform: bool = True,
    run_features_stage: bool = True,
    max_workers: int = MAX_WORKERS,
    iqr_multiplier: float = IQR_MULTIPLIER,
) -> None:
    """Run the full KGS pipeline or individual stages.

    Args:
        index_path: Path to lease index file.
        raw_dir: Raw data directory.
        interim_dir: Interim Parquet directory.
        clean_dir: Cleaned Parquet directory.
        features_dir: Features Parquet directory.
        run_acquire: Whether to run the acquire stage.
        run_ingest: Whether to run the ingest stage.
        run_transform: Whether to run the transform stage.
        run_features_stage: Whether to run the features stage.
        max_workers: Thread workers for acquire.
        iqr_multiplier: IQR multiplier for outlier capping.
    """
    index_path = index_path or str(LEASE_INDEX_FILE)
    raw_dir = raw_dir or str(RAW_DIR)
    interim_dir = interim_dir or str(INTERIM_DIR)
    clean_dir = clean_dir or str(CLEAN_DIR)
    features_dir = features_dir or str(FEATURES_DIR)

    for d in [raw_dir, interim_dir, clean_dir, features_dir]:
        Path(d).mkdir(parents=True, exist_ok=True)

    if run_acquire:
        from kgs_pipeline.acquire import run_acquire as _run_acquire
        t0 = time.perf_counter()
        logger.info("=== Stage: ACQUIRE ===")
        paths = _run_acquire(index_path, raw_dir, max_workers)
        logger.info("Acquire: %d files in %.2f s", len(paths), time.perf_counter() - t0)

    if run_ingest:
        from kgs_pipeline.ingest import run_ingest as _run_ingest
        t0 = time.perf_counter()
        logger.info("=== Stage: INGEST ===")
        _run_ingest(raw_dir, interim_dir)
        logger.info("Ingest complete in %.2f s", time.perf_counter() - t0)

    if run_transform:
        from kgs_pipeline.transform import run_transform as _run_transform
        t0 = time.perf_counter()
        logger.info("=== Stage: TRANSFORM ===")
        _run_transform(interim_dir, clean_dir, raw_dir, iqr_multiplier)
        logger.info("Transform complete in %.2f s", time.perf_counter() - t0)

    if run_features_stage:
        from kgs_pipeline.features import run_features as _run_features
        t0 = time.perf_counter()
        logger.info("=== Stage: FEATURES ===")
        _run_features(clean_dir, features_dir)
        logger.info("Features complete in %.2f s", time.perf_counter() - t0)

    logger.info("Pipeline complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KGS pipeline orchestrator")
    parser.add_argument("--index", default=str(LEASE_INDEX_FILE))
    parser.add_argument("--raw-dir", default=str(RAW_DIR))
    parser.add_argument("--interim-dir", default=str(INTERIM_DIR))
    parser.add_argument("--clean-dir", default=str(CLEAN_DIR))
    parser.add_argument("--features-dir", default=str(FEATURES_DIR))
    parser.add_argument("--acquire", action="store_true")
    parser.add_argument("--ingest", action="store_true")
    parser.add_argument("--transform", action="store_true")
    parser.add_argument("--features", action="store_true")
    parser.add_argument("--all", action="store_true", help="Run all stages")
    parser.add_argument("--max-workers", type=int, default=MAX_WORKERS)
    parser.add_argument("--iqr-multiplier", type=float, default=IQR_MULTIPLIER)
    args = parser.parse_args()

    run_all = args.all or not any([args.acquire, args.ingest, args.transform, args.features])

    run_pipeline(
        index_path=args.index,
        raw_dir=args.raw_dir,
        interim_dir=args.interim_dir,
        clean_dir=args.clean_dir,
        features_dir=args.features_dir,
        run_acquire=args.acquire or run_all,
        run_ingest=args.ingest or run_all,
        run_transform=args.transform or run_all,
        run_features_stage=args.features or run_all,
        max_workers=args.max_workers,
        iqr_multiplier=args.iqr_multiplier,
    )
