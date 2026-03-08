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

FLYTE_PROJECT = os.environ.get("FLYTE_PROJECT", "flytesnacks")
FLYTE_DOMAIN = os.environ.get("FLYTE_DOMAIN", "development")


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
            "--output-file", str(out_path),
            "--image", IMAGE,
            "--project", FLYTE_PROJECT,
            "--domain", FLYTE_DOMAIN,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"flyte create failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
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
            cmd += [f"--{k}", str(v)]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, (
        f"Remote execution failed for {wf_name}:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.e2e

_FLOWS = [
    pytest.param("linear_flow.py",    "linear_flow",    None,                   id="linear"),
    pytest.param("branch_flow.py",    "branch_flow",    None,                   id="branch"),
    pytest.param("foreach_flow.py",   "foreach_flow",   None,                   id="foreach"),
    pytest.param("param_flow.py",     "param_flow",     {"greeting": "Flyte"},  id="params"),
    pytest.param("condition_flow.py", "conditional_flow", None,                 id="conditional"),
]


@pytest.mark.parametrize("flow_file,wf_name,params", _FLOWS)
def test_e2e_flow(tmp_path, flow_file, wf_name, params):
    """Flow compiles and runs successfully as real Flyte tasks in the cluster."""
    out = tmp_path / "workflow.py"
    _compile(FLOWS_DIR / flow_file, out)
    _run_remote(out, wf_name, params)
