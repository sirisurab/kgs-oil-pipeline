"""Top-level pipeline orchestrator chaining all four stages."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from kgs_pipeline.acquire import run_acquire
from kgs_pipeline.config import config as _cfg
from kgs_pipeline.features import run_features
from kgs_pipeline.ingest import run_ingest
from kgs_pipeline.logging_utils import get_logger
from kgs_pipeline.transform import run_transform

logger = get_logger(__name__)


def run_pipeline(
    index_path: str,
    raw_dir: str,
    interim_dir: str,
    processed_dir: str,
    features_dir: str,
    min_year: int = 2024,
    workers: int = 5,
) -> dict:
    """Run the full KGS pipeline: acquire → ingest → transform → features.

    Returns a summary dict with keys: acquired, interim_files, processed_files, manifest.
    """
    logger.info("Pipeline starting", extra={"min_year": min_year, "workers": workers})

    logger.info("Stage: acquire")
    paths = run_acquire(index_path, raw_dir, min_year, workers)
    logger.info("Stage acquire complete", extra={"acquired": len(paths)})

    logger.info("Stage: ingest")
    interim_files = run_ingest(raw_dir, interim_dir, min_year)
    logger.info("Stage ingest complete", extra={"interim_files": interim_files})

    logger.info("Stage: transform")
    processed_files = run_transform(interim_dir, processed_dir)
    logger.info("Stage transform complete", extra={"processed_files": processed_files})

    logger.info("Stage: features")
    manifest_path = run_features(processed_dir, features_dir)
    logger.info("Stage features complete", extra={"manifest": str(manifest_path)})

    summary = {
        "acquired": len(paths),
        "interim_files": interim_files,
        "processed_files": processed_files,
        "manifest": str(manifest_path),
    }
    logger.info("Pipeline complete", extra=summary)
    return summary


def main() -> None:
    """CLI entry point for the full pipeline."""
    parser = argparse.ArgumentParser(description="KGS end-to-end pipeline runner")
    parser.add_argument("--index-path", default=_cfg.LEASE_INDEX_PATH)
    parser.add_argument("--raw-dir", default=_cfg.RAW_DATA_DIR)
    parser.add_argument("--interim-dir", default=_cfg.INTERIM_DATA_DIR)
    parser.add_argument("--processed-dir", default=_cfg.PROCESSED_DATA_DIR)
    parser.add_argument("--features-dir", default=_cfg.FEATURES_DATA_DIR)
    parser.add_argument("--min-year", "--start-year", type=int, default=_cfg.MIN_YEAR, dest="min_year")
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--workers", type=int, default=_cfg.MAX_WORKERS)
    args = parser.parse_args()

    if args.end_year is not None:
        logger.warning(
            "--end-year is reserved for future use and is currently ignored",
            extra={"end_year": args.end_year},
        )

    try:
        summary = run_pipeline(
            index_path=args.index_path,
            raw_dir=args.raw_dir,
            interim_dir=args.interim_dir,
            processed_dir=args.processed_dir,
            features_dir=args.features_dir,
            min_year=args.min_year,
            workers=args.workers,
        )
        print(summary)
        sys.exit(0)
    except Exception as exc:
        logger.error("Pipeline failed", extra={"error": str(exc)})
        sys.exit(1)


if __name__ == "__main__":
    main()
