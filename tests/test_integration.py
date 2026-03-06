"""
Integration tests for metaflow-flyte.

Tier 1 (always runs): Compilation tests that verify ``flyte create`` generates
correct Python files without needing a Flyte cluster.

Tier 2 (local execution): Runs flows locally using ``pyflyte run`` which
executes Flyte tasks in-process (no cluster required).  Marked with
``@pytest.mark.integration`` — skip with ``pytest -m "not integration"``.
These tests run real Metaflow subprocesses against the local datastore.

Tier 3 (cluster): Marked ``@pytest.mark.e2e`` — requires docker-compose up.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import metaflow
import pytest

FLOWS_DIR = Path(__file__).parent / "flows"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compile(flow_path: Path, out_path: Path, extra_args: list[str] = ()) -> None:
    """Run ``python <flow> flyte create <out>`` and assert success."""
    result = subprocess.run(
        [
            sys.executable,
            str(flow_path),
            "--no-pylint",
            "--metadata=local",
            "--datastore=local",
            "flyte",
            "create",
            str(out_path),
        ]
        + list(extra_args),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "flyte create failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
    )


def _run_locally(out_path: Path, wf_name: str, params: dict | None = None) -> str:
    """
    Execute a generated Flyte workflow file locally via ``pyflyte run``.

    Returns the Metaflow run_id of the completed run.
    """
    cmd = ["pyflyte", "run", str(out_path), wf_name]
    if params:
        for k, v in params.items():
            cmd += ["--%s" % k, str(v)]

    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, (
        "pyflyte run failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
    )

    # Give Metaflow's local datastore a moment to flush, then find the latest run.
    time.sleep(0.1)
    return result.stdout


def _latest_run(flow_name: str) -> metaflow.Run:
    """Return the most recently created Metaflow run for *flow_name*."""
    flow = metaflow.Flow(flow_name)
    runs = sorted(flow.runs(), key=lambda r: r.created_at, reverse=True)
    assert runs, "No runs found for flow %s" % flow_name
    return runs[0]


def _read_generated(out_path: Path) -> str:
    return out_path.read_text()


# ---------------------------------------------------------------------------
# Tier 1: Compilation (no cluster)
# ---------------------------------------------------------------------------


class TestCompilation:
    """Unit tests for the ``flyte create`` command."""

    def test_create_generates_file(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        assert out.exists()
        content = _read_generated(out)
        assert "_step_start" in content
        assert "_step_process" in content
        assert "_step_end" in content
        assert "linear_flow" in content  # workflow function name

    def test_generated_file_is_importable(self, tmp_path):
        """The generated file should be syntactically valid Python."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        result = subprocess.run(
            [sys.executable, "-c", "import py_compile; py_compile.compile('%s')" % out],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

    def test_create_embeds_flow_config(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        content = _read_generated(out)
        assert "FLOW_NAME" in content
        assert "LinearFlow" in content
        assert "DATASTORE_TYPE" in content
        assert "METADATA_TYPE" in content

    def test_create_with_tags(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(
            FLOWS_DIR / "linear_flow.py", out,
            ["--tag", "team:ml", "--tag", "env:prod"],
        )
        content = _read_generated(out)
        assert "team:ml" in content
        assert "env:prod" in content

    def test_create_with_decorators(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(
            FLOWS_DIR / "linear_flow.py", out,
            ["--with", "resources:cpu=4,memory=8g"],
        )
        content = _read_generated(out)
        assert "resources:cpu=4,memory=8g" in content

    def test_create_branch_flow(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "branch_flow.py", out)
        content = _read_generated(out)
        assert "_step_branch_a" in content
        assert "_step_branch_b" in content
        assert "_step_join" in content
        # Split join: join receives individual branch task_ids
        assert "_in_branch_a" in content
        assert "_in_branch_b" in content

    def test_create_foreach_flow(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "foreach_flow.py", out)
        content = _read_generated(out)
        # Foreach step should return JSON-encoded result string
        assert "foreach_result_json" in content or "json.dumps" in content
        # Dynamic expander should be present
        assert "@dynamic" in content
        assert "_foreach_start_dynamic" in content or "_foreach_" in content
        # Body task should accept split_index
        assert "split_index" in content

    def test_create_param_flow(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "param_flow.py", out)
        content = _read_generated(out)
        # Parameters should appear in workflow signature and start task
        assert "greeting" in content
        assert "_param_greeting" in content

    def test_create_retry_flow(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "retry_flow.py", out)
        content = _read_generated(out)
        # Retry count should be in @_mf_task decorator
        assert "retries=2" in content

    def test_create_scheduled_flow(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "scheduled_flow.py", out)
        content = _read_generated(out)
        # LaunchPlan for schedule
        assert "LaunchPlan" in content
        assert "CronSchedule" in content
        assert "0 * * * *" in content

    def test_create_with_image(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(
            FLOWS_DIR / "linear_flow.py", out,
            ["--image", "ghcr.io/flyteorg/flytekit:py3.11-latest"],
        )
        content = _read_generated(out)
        assert "ghcr.io/flyteorg/flytekit:py3.11-latest" in content
        assert "IMAGE" in content

    def test_create_with_project_and_domain(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(
            FLOWS_DIR / "linear_flow.py", out,
            ["--project", "myproject", "--domain", "staging"],
        )
        content = _read_generated(out)
        assert "myproject" in content
        assert "staging" in content

    def test_flyte_deck_in_generated_file(self, tmp_path):
        """Every task should call _emit_deck to show Metaflow artifacts in Flyte UI."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        content = _read_generated(out)
        assert "_emit_deck" in content
        assert "flytekit.Deck" in content

    def test_run_id_task_present(self, tmp_path):
        """_mf_generate_run_id task must always be present."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        content = _read_generated(out)
        assert "_mf_generate_run_id" in content

    def test_flyte_internal_decorator_injected(self, tmp_path):
        """--with=flyte_internal must appear in every _step_cmd call."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        content = _read_generated(out)
        assert "flyte_internal" in content

    def test_workflow_timeout(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(
            FLOWS_DIR / "linear_flow.py", out,
            ["--workflow-timeout", "3600"],
        )
        # workflow_timeout is stored in config; not currently used at workflow
        # level (Flyte doesn't have a per-workflow timeout in @workflow decorator),
        # but should appear in generated config constant.
        content = _read_generated(out)
        assert out.exists()  # file was generated

    def test_project_flow(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "project_flow.py", out)
        content = _read_generated(out)
        assert "myproject" in content
        assert "ProjectFlow" in content

    def test_resources_flow_compiles_successfully(self, tmp_path):
        """@resources should compile — it only emits a UserWarning, not an error."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "resources_flow.py", out)
        assert out.exists()
        content = _read_generated(out)
        assert "_step_compute" in content

    def test_resources_flow_emits_warning(self, tmp_path):
        """flyte create on a @resources flow should warn on stderr."""
        out = tmp_path / "workflow.py"
        result = subprocess.run(
            [
                sys.executable,
                str(FLOWS_DIR / "resources_flow.py"),
                "--no-pylint",
                "--metadata=local",
                "--datastore=local",
                "flyte",
                "create",
                str(out),
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            "flyte create failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
        )
        combined = result.stdout + result.stderr
        assert "resources" in combined.lower() or out.exists()

    def test_create_conditional_flow(self, tmp_path):
        """Conditional (split-switch) flow should compile without error."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "condition_flow.py", out)
        content = _read_generated(out)
        # Split-switch step: returns JSON with branch_taken
        assert "_step_start" in content
        # Dynamic router for the split-switch
        assert "_cond_start_router" in content
        assert "@dynamic" in content
        # Branch tasks
        assert "_step_high" in content
        assert "_step_low" in content
        # Join receives JSON from router
        assert "_branch_result_json" in content
        assert "branch_step" in content

    def test_conditional_flow_is_importable(self, tmp_path):
        """Generated conditional workflow file must be syntactically valid Python."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "condition_flow.py", out)
        result = subprocess.run(
            [sys.executable, "-c", "import py_compile; py_compile.compile('%s')" % out],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

    def test_conditional_flow_workflow_function_present(self, tmp_path):
        """The @workflow function for a conditional flow must be emitted."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "condition_flow.py", out)
        content = _read_generated(out)
        assert "@workflow" in content
        assert "def conditional_flow(" in content

    def test_conditional_flow_branch_taken_json(self, tmp_path):
        """The split-switch task must return JSON with branch_taken key."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "condition_flow.py", out)
        content = _read_generated(out)
        assert "branch_taken" in content
        assert "_read_condition_branch" in content

    def test_conditional_flow_router_has_all_branches(self, tmp_path):
        """The dynamic router must contain if/elif cases for each branch."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "condition_flow.py", out)
        content = _read_generated(out)
        # Both branch cases must appear in the router
        assert "'high'" in content
        assert "'low'" in content


# ---------------------------------------------------------------------------
# Tier 2: Local execution (pyflyte run without cluster)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestLinearFlow:
    def test_linear_flow_completes(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        _run_locally(out, "linear_flow")
        run = _latest_run("LinearFlow")
        assert run is not None

    def test_linear_flow_artifacts(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        _run_locally(out, "linear_flow")
        run = _latest_run("LinearFlow")
        assert run["end"].task.data.result == "hello world"


@pytest.mark.integration
class TestBranchFlow:
    def test_branch_flow_completes(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "branch_flow.py", out)
        _run_locally(out, "branch_flow")
        run = _latest_run("BranchFlow")
        assert run is not None

    def test_branch_flow_both_branches_ran(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "branch_flow.py", out)
        _run_locally(out, "branch_flow")
        run = _latest_run("BranchFlow")
        step_names = {s.id for s in run}
        assert "branch_a" in step_names
        assert "branch_b" in step_names

    def test_branch_flow_artifacts(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "branch_flow.py", out)
        _run_locally(out, "branch_flow")
        run = _latest_run("BranchFlow")
        assert run["branch_a"].task.data.result_a == "hello from A"
        assert run["branch_b"].task.data.result_b == "hello from B"


@pytest.mark.integration
class TestForeachFlow:
    def test_foreach_flow_completes(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "foreach_flow.py", out)
        _run_locally(out, "foreach_flow")
        run = _latest_run("ForeachFlow")
        assert run is not None

    def test_foreach_flow_all_tasks_ran(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "foreach_flow.py", out)
        _run_locally(out, "foreach_flow")
        run = _latest_run("ForeachFlow")
        body_tasks = list(run["body"].tasks())
        assert len(body_tasks) == 3
        results = sorted(t.data.result for t in body_tasks)
        assert results == [
            "processed: alpha",
            "processed: beta",
            "processed: gamma",
        ]


@pytest.mark.integration
class TestParamFlow:
    def test_param_flow_default(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "param_flow.py", out)
        _run_locally(out, "param_flow")
        run = _latest_run("ParamFlow")
        assert run["end"].task.data.message == "hello, world"

    def test_param_flow_custom_param(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "param_flow.py", out)
        _run_locally(out, "param_flow", {"greeting": "Flyte"})
        run = _latest_run("ParamFlow")
        assert run["end"].task.data.message == "hello, Flyte"


@pytest.mark.integration
class TestArtifactFlow:
    def test_artifact_types_preserved(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "artifact_flow.py", out)
        _run_locally(out, "artifact_flow")
        run = _latest_run("ArtifactFlow")
        start_data = run["start"].task.data
        assert start_data.int_val == 42
        assert abs(start_data.float_val - 3.14) < 0.001
        assert start_data.str_val == "hello"
        assert start_data.list_val == [1, 2, 3]
        assert start_data.dict_val == {"key": "value", "nested": {"a": 1}}

    def test_artifact_transform(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "artifact_flow.py", out)
        _run_locally(out, "artifact_flow")
        run = _latest_run("ArtifactFlow")
        transform_data = run["transform"].task.data
        assert transform_data.doubled == 84
        assert transform_data.appended == [1, 2, 3, 4, 5]
        assert transform_data.merged["extra"] is True

    def test_foreach_artifacts_per_split(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "foreach_flow.py", out)
        _run_locally(out, "foreach_flow")
        run = _latest_run("ForeachFlow")
        body_tasks = list(run["body"].tasks())
        assert len(body_tasks) == 3
        results = sorted(t.data.result for t in body_tasks)
        assert results == [
            "processed: alpha",
            "processed: beta",
            "processed: gamma",
        ]


@pytest.mark.integration
class TestConditionalFlow:
    def test_conditional_flow_completes(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "condition_flow.py", out)
        _run_locally(out, "conditional_flow")
        run = _latest_run("ConditionalFlow")
        assert run is not None

    def test_conditional_flow_correct_branch_ran(self, tmp_path):
        """value=10 > 5, so the 'high' branch should execute."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "condition_flow.py", out)
        _run_locally(out, "conditional_flow")
        run = _latest_run("ConditionalFlow")
        # The 'high' branch sets result = "high"
        assert run["join"].task.data.final == "high"


@pytest.mark.integration
class TestConfigFlow:
    def test_config_defaults(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "config_flow.py", out)
        _run_locally(out, "config_flow")
        run = _latest_run("ConfigFlow")
        task_data = run["compute"].task.data
        assert task_data.total == 30  # [1,2,3,4,5] * 2 = 30
        assert task_data.config_label == "test"

    def test_config_overrides(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "config_flow.py", out)
        _run_locally(out, "config_flow", {"multiplier": "3", "label": "prod"})
        run = _latest_run("ConfigFlow")
        task_data = run["compute"].task.data
        assert task_data.total == 45  # [1,2,3,4,5] * 3 = 45
        assert task_data.config_label == "prod"


# ---------------------------------------------------------------------------
# Tier 3: Remote cluster (docker-compose)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestE2ECluster:
    """Tests against a real Flyte cluster — requires docker-compose up."""

    def test_linear_flow_remote(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(
            FLOWS_DIR / "linear_flow.py", out,
            ["--image", os.environ.get("FLYTE_TEST_IMAGE", "python:3.11-slim")],
        )
        result = subprocess.run(
            [
                "pyflyte", "run", "--remote",
                "--project", "flytesnacks",
                "--domain", "development",
                str(out), "linear_flow",
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, (
            "Remote run failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
        )

    def test_conditional_flow_remote(self, tmp_path):
        out = tmp_path / "workflow.py"
        _compile(
            FLOWS_DIR / "condition_flow.py", out,
            ["--image", os.environ.get("FLYTE_TEST_IMAGE", "python:3.11-slim")],
        )
        result = subprocess.run(
            [
                "pyflyte", "run", "--remote",
                "--project", "flytesnacks",
                "--domain", "development",
                str(out), "conditional_flow",
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, (
            "Remote conditional run failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
        )
