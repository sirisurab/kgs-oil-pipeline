#!/usr/bin/env python3
"""Verify pipeline structure and imports."""

import sys
from pathlib import Path

# Add kgs to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    print("Verifying pipeline imports...")

    # Import core modules
    from kgs_pipeline import config
    print("✓ config module imported")

    from kgs_pipeline import acquire
    print("✓ acquire module imported")

    from kgs_pipeline import ingest
    print("✓ ingest module imported")

    from kgs_pipeline import transform
    print("✓ transform module imported")

    from kgs_pipeline import features
    print("✓ features module imported")

    # Verify key functions exist
    assert hasattr(acquire, "run_acquire_pipeline")
    print("✓ acquire.run_acquire_pipeline exists")

    assert hasattr(acquire, "load_lease_urls")
    print("✓ acquire.load_lease_urls exists")

    assert hasattr(ingest, "run_ingest_pipeline")
    print("✓ ingest.run_ingest_pipeline exists")

    assert hasattr(transform, "run_transform_pipeline")
    print("✓ transform.run_transform_pipeline exists")

    assert hasattr(features, "run_features_pipeline")
    print("✓ features.run_features_pipeline exists")

    # Verify config values
    assert config.RAW_DATA_DIR
    print(f"✓ RAW_DATA_DIR: {config.RAW_DATA_DIR}")

    assert config.INTERIM_DATA_DIR
    print(f"✓ INTERIM_DATA_DIR: {config.INTERIM_DATA_DIR}")

    assert config.PROCESSED_DATA_DIR
    print(f"✓ PROCESSED_DATA_DIR: {config.PROCESSED_DATA_DIR}")

    assert config.FEATURES_DIR
    print(f"✓ FEATURES_DIR: {config.FEATURES_DIR}")

    print("\n✅ All imports and basic structure verification passed!")
    print("\nPipeline architecture:")
    print("  Acquire   → loads lease URLs and scrapes KGS pages")
    print("  Ingest    → reads raw .txt, merges metadata")
    print("  Transform → cleans, standardizes, explodes wells")
    print("  Features  → engineers time-series features for ML")

except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)
except AssertionError as e:
    print(f"❌ Assertion error: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    sys.exit(1)
