#!/usr/bin/env python3
"""Quick test to verify all modules can be imported."""

try:
    from kgs_pipeline import config
    print("✓ kgs_pipeline.config")
except Exception as e:
    print(f"✗ kgs_pipeline.config: {e}")

try:
    from kgs_pipeline import acquire
    print("✓ kgs_pipeline.acquire")
except Exception as e:
    print(f"✗ kgs_pipeline.acquire: {e}")

try:
    from kgs_pipeline import ingest
    print("✓ kgs_pipeline.ingest")
except Exception as e:
    print(f"✗ kgs_pipeline.ingest: {e}")

try:
    from kgs_pipeline import transform
    print("✓ kgs_pipeline.transform")
except Exception as e:
    print(f"✗ kgs_pipeline.transform: {e}")

try:
    from kgs_pipeline import features
    print("✓ kgs_pipeline.features")
except Exception as e:
    print(f"✗ kgs_pipeline.features: {e}")

print("\nAll imports successful!")
