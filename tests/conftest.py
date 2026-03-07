"""pytest configuration for metaflow-flyte tests."""

import sys
from pathlib import Path


# Ensure the package root is importable when running tests directly.
sys.path.insert(0, str(Path(__file__).parent.parent))

# Use local datastore and metadata for all tests — no cloud credentials needed.
import metaflow
metaflow.metadata("local")

FLOWS_DIR = Path(__file__).parent / "flows"
