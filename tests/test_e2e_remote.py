"""
Remote e2e tests — flows run as real Flyte tasks inside the cluster.

Each test compiles a flow to a Flyte workflow file (with FLOW_FILE pointing
to a path that exists in the task container), then submits it via
``pyflyte run --remote`` and waits for execution to succeed.

Requirements
------------
- A running Flyte sandbox (``flytectl demo start``)
- ``FLYTE_TEST_IMAGE`` env var: Docker image for task containers.  The image
  must have metaflow-flyte installed and flows at ``/app/tests/flows/``.
- ``E2E_FLOWS_DIR`` env var: path to flows on the **host** that matches the
  path inside the container.  Defaults to ``tests/flows/`` (local dev).
- Metaflow S3 datastore env vars pointing at the sandbox MinIO.

Run:
    pytest tests/test_e2e_remote.py -v --timeout=300
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# When running in CI, flows are copied to a stable path that matches what's
# baked into the container image (COPY tests/flows/ /app/tests/flows/).
FLOWS_DIR = Path(os.environ.get("E2E_FLOWS_DIR", Path(__file__).parent / "flows"))

IMAGE = os.environ.get("FLYTE_TEST_IMAGE")
if not IMAGE:
    pytest.skip("FLYTE_TEST_IMAGE not set — skipping remote e2e tests", allow_module_level=True)

FLYTE_PROJECT = "flytesnacks"
FLYTE_DOMAIN = "development"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compile(flow_path: Path, out_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(flow_path),
            "--no-pylint",
            "--metadata=local",
            "--datastore=s3",
            "flyte",
            "create",
            "--image", IMAGE,
            "--project", FLYTE_PROJECT,
            "--domain", FLYTE_DOMAIN,
            str(out_path),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "flyte create failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
    )


def _run_remote(out_path: Path, wf_name: str, params: dict | None = None) -> None:
    """Submit the workflow to the Flyte cluster and wait for completion."""
    cmd = [
        "pyflyte", "run", "--remote",
        "--project", FLYTE_PROJECT,
        "--domain", FLYTE_DOMAIN,
        str(out_path), wf_name,
    ]
    if params:
        for k, v in params.items():
            cmd += ["--%s" % k, str(v)]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, (
        "Remote execution failed for %s:\nSTDOUT: %s\nSTDERR: %s"
        % (wf_name, result.stdout, result.stderr)
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.e2e


def test_e2e_linear_flow(tmp_path):
    """Linear 3-step flow runs successfully in the cluster."""
    out = tmp_path / "workflow.py"
    _compile(FLOWS_DIR / "linear_flow.py", out)
    _run_remote(out, "linear_flow")


def test_e2e_branch_flow(tmp_path):
    """Parallel split/join flow: both branches run and join successfully."""
    out = tmp_path / "workflow.py"
    _compile(FLOWS_DIR / "branch_flow.py", out)
    _run_remote(out, "branch_flow")


def test_e2e_foreach_flow(tmp_path):
    """Dynamic fan-out foreach flow: 3 body tasks run in parallel."""
    out = tmp_path / "workflow.py"
    _compile(FLOWS_DIR / "foreach_flow.py", out)
    _run_remote(out, "foreach_flow")


def test_e2e_param_flow(tmp_path):
    """Parameters are forwarded correctly from pyflyte run to Metaflow init."""
    out = tmp_path / "workflow.py"
    _compile(FLOWS_DIR / "param_flow.py", out)
    _run_remote(out, "param_flow", {"greeting": "Flyte"})


def test_e2e_conditional_flow(tmp_path):
    """Conditional (split-switch) flow: correct branch selected at runtime."""
    out = tmp_path / "workflow.py"
    _compile(FLOWS_DIR / "condition_flow.py", out)
    _run_remote(out, "conditional_flow")
