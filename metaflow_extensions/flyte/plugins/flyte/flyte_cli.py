"""Metaflow CLI extension: ``python myflow.py flyte <command>``.

Commands
--------
create    Compile the flow to a Flyte workflow Python file.
run       Compile and immediately run the flow locally via pyflyte.
register  Register the workflow with a Flyte cluster.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import uuid

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.util import get_username

from metaflow_extensions.flyte.plugins.flyte._codegen import _wf_fn
from metaflow_extensions.flyte.plugins.flyte.exception import FlyteException
from metaflow_extensions.flyte.plugins.flyte.flyte_compiler import FlyteCompiler

# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
def cli() -> None:
    pass


@cli.group(help="Commands for deploying Metaflow flows to Flyte.")
@click.pass_obj
def flyte(obj: object) -> None:
    pass


# ---------------------------------------------------------------------------
# flyte create
# ---------------------------------------------------------------------------


@flyte.command(help="Compile this flow to a Flyte workflow Python file.")
@click.option("--output-file", "output_file", default=None, help="Output file path.")
@click.option(
    "--tag",
    "tags",
    multiple=True,
    default=None,
    help="Annotate Metaflow run objects with this tag (repeatable).",
)
@click.option("--namespace", "user_namespace", default=None)
@click.option(
    "--with",
    "with_decorators",
    multiple=True,
    default=None,
    help="Inject a decorator on every step (repeatable), e.g. --with=resources:cpu=4.",
)
@click.option("--workflow-timeout", default=None, type=int, help="Flow-level timeout in seconds.")
@click.option(
    "--project",
    "flyte_project",
    default="flytesnacks",
    show_default=True,
    help="Flyte project name.",
)
@click.option(
    "--domain",
    "flyte_domain",
    default="development",
    show_default=True,
    help="Flyte domain (development/staging/production).",
)
@click.option("--image", default=None, help="Docker image URI for Flyte task containers.")
@click.option(
    "--max-parallelism",
    default=None,
    type=int,
    help="Max parallel tasks for foreach expansions.",
)
@click.option("--branch", default=None, help="@project branch name (for project flows).")
@click.option("--production", is_flag=True, default=False, help="Deploy to the production branch.")
@click.option(
    "--deployer-attribute-file", default=None, hidden=True,
    help="Write deployment info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def create(
    obj: object,
    output_file: str | None,
    tags: tuple[str, ...],
    user_namespace: str | None,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    flyte_project: str,
    flyte_domain: str,
    image: str | None,
    max_parallelism: int | None,
    branch: str | None,
    production: bool,
    deployer_attribute_file: str | None,
) -> None:
    flow_name = obj.flow.name  # type: ignore[attr-defined]
    if output_file is None:
        output_file = f"{flow_name.lower()}_flyte.py"
    if os.path.abspath(sys.argv[0]) == os.path.abspath(output_file):
        raise MetaflowException(
            "Output file name cannot be the same as the flow file name."
        )

    _make_compiler_and_write(
        obj, output_file, tags, user_namespace, with_decorators,
        workflow_timeout, flyte_project, flyte_domain, image, max_parallelism,
        branch=branch, production=production,
    )

    if deployer_attribute_file:
        # Capture env vars needed for local execution so from_deployment can restore them.
        _env_keys = ("METAFLOW_DEFAULT_METADATA", "METAFLOW_DEFAULT_DATASTORE",
                     "METAFLOW_DEFAULT_ENVIRONMENT", "METAFLOW_DATASTORE_SYSROOT_LOCAL")
        _saved_env = {k: os.environ[k] for k in _env_keys if k in os.environ}
        # Also capture METAFLOW_FLOW_CONFIG_VALUE so that the trigger subprocess
        # (which re-imports the flow but may lack the --config-value CLI flag)
        # uses the same compile-time config baked into the generated workflow file.
        # Read it from the generated file rather than os.environ so we always get
        # the value that was actually written (avoids stale env from a prior run).
        try:
            import re as _re
            with open(os.path.abspath(output_file)) as _wf:
                _wf_src = _wf.read()
            _m = _re.search(r"^FLOW_CONFIG_VALUE: str \| None = (.+)$", _wf_src, _re.MULTILINE)
            if _m:
                import ast as _ast
                _fcv = _ast.literal_eval(_m.group(1))
                if _fcv:
                    _saved_env["METAFLOW_FLOW_CONFIG_VALUE"] = _fcv
        except Exception:
            pass
        _additional_info = {
            "workflow_file": os.path.abspath(output_file),
            "flyte_project": flyte_project,
            "flyte_domain": flyte_domain,
            "saved_env": _saved_env,
        }
        # The "name" field becomes deployer.name which is used as the identifier
        # passed to FlyteDeployedFlow.from_deployment().  Encode everything needed
        # to recover the deployment so that from_deployment() can rebuild the
        # deployer without access to the original Deployer() instance.
        _recovery_id = json.dumps({
            "name": flow_name,
            "flow_name": flow_name,
            "flow_file": os.path.abspath(sys.argv[0]),
            **_additional_info,
        })
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": _recovery_id,
                    "flow_name": flow_name,
                    "metadata": "{}",
                    "additional_info": _additional_info,
                },
                f,
            )

    obj.echo(  # type: ignore[attr-defined]
        f"Flyte workflow file written to *{output_file}*.\n"
        f"Run locally: pyflyte run {output_file} {_wf_fn(flow_name)}\n"
        f"Register:    python {sys.argv[0]} flyte register {output_file}",
        bold=True,
    )


# ---------------------------------------------------------------------------
# flyte run  (local execution via pyflyte)
# ---------------------------------------------------------------------------


@flyte.command(help="Compile and immediately run the flow locally via pyflyte.")
@click.option("--tag", "tags", multiple=True, default=None)
@click.option("--namespace", "user_namespace", default=None)
@click.option("--with", "with_decorators", multiple=True, default=None)
@click.option("--workflow-timeout", default=None, type=int)
@click.option("--project", "flyte_project", default="flytesnacks", show_default=True)
@click.option("--domain", "flyte_domain", default="development", show_default=True)
@click.option("--image", default=None, help="Docker image URI.")
@click.option("--max-parallelism", default=None, type=int)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--remote",
    is_flag=True,
    default=False,
    help="Execute on a remote Flyte cluster instead of locally.",
)
@click.pass_obj
def run(
    obj: object,
    tags: tuple[str, ...],
    user_namespace: str | None,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    flyte_project: str,
    flyte_domain: str,
    image: str | None,
    max_parallelism: int | None,
    branch: str | None,
    production: bool,
    remote: bool,
) -> None:
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
        tmp_path = tmp.name

    try:
        _make_compiler_and_write(
            obj, tmp_path, tags, user_namespace, with_decorators,
            workflow_timeout, flyte_project, flyte_domain, image, max_parallelism,
            branch=branch, production=production,
        )

        wf_name = _wf_fn(obj.flow.name)  # type: ignore[attr-defined]

        if remote:
            cmd = [
                "pyflyte", "run", "--remote",
                "--project", flyte_project,
                "--domain", flyte_domain,
                tmp_path, wf_name,
            ]
        else:
            cmd = ["pyflyte", "run", tmp_path, wf_name]

        obj.echo("Running: {}".format(" ".join(cmd)), bold=True)  # type: ignore[attr-defined]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise FlyteException(f"pyflyte run failed with exit code {result.returncode:d}")
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# flyte register
# ---------------------------------------------------------------------------


@flyte.command(help="Register the Flyte workflow with a Flyte cluster.")
@click.argument("output_file", required=True)
@click.option("--tag", "tags", multiple=True, default=None)
@click.option("--namespace", "user_namespace", default=None)
@click.option("--with", "with_decorators", multiple=True, default=None)
@click.option("--workflow-timeout", default=None, type=int)
@click.option("--project", "flyte_project", default="flytesnacks", show_default=True)
@click.option("--domain", "flyte_domain", default="development", show_default=True)
@click.option("--image", default=None, required=True, help="Docker image URI (required for registration).")
@click.option("--max-parallelism", default=None, type=int)
@click.option(
    "--host",
    default="localhost:30080",
    show_default=True,
    help="Flyte Admin endpoint (host:port).",
)
@click.option("--insecure", is_flag=True, default=False, help="Use insecure gRPC (no TLS).")
@click.option(
    "--branch", default=None, help="@project branch name (for project flows).",
)
@click.option("--production", is_flag=True, default=False, help="Deploy to the production branch.")
@click.option(
    "--deployer-attribute-file", default=None, hidden=True,
    help="Write deployment info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def register(
    obj: object,
    output_file: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    flyte_project: str,
    flyte_domain: str,
    image: str | None,
    max_parallelism: int | None,
    host: str,
    insecure: bool,
    branch: str | None,
    production: bool,
    deployer_attribute_file: str | None,
) -> None:
    if os.path.abspath(sys.argv[0]) == os.path.abspath(output_file):
        raise MetaflowException("Output file cannot be the same as the flow file.")

    _make_compiler_and_write(
        obj, output_file, tags, user_namespace, with_decorators,
        workflow_timeout, flyte_project, flyte_domain, image, max_parallelism,
        branch=branch, production=production,
    )

    register_cmd = _pyflyte_register_cmd(flyte_project, flyte_domain, insecure, output_file)
    obj.echo("Registering: {}".format(" ".join(register_cmd)), bold=True)  # type: ignore[attr-defined]
    result = subprocess.run(register_cmd)
    if result.returncode != 0:
        raise FlyteException(f"pyflyte register failed with exit code {result.returncode:d}")

    flow_name = obj.flow.name  # type: ignore[attr-defined]
    wf_name = _wf_fn(flow_name)

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": wf_name,
                    "flow_name": flow_name,
                    "metadata": "{}",
                },
                f,
            )

    obj.echo(  # type: ignore[attr-defined]
        "Workflow registered. Run it with:\n"
        f"  pyflyte run --remote --project {flyte_project} --domain {flyte_domain} {output_file} {wf_name}",
        bold=True,
    )


# ---------------------------------------------------------------------------
# flyte deploy  (alias for register, used by the Deployer API)
# ---------------------------------------------------------------------------


@flyte.command(help="Compile and register this flow as a Flyte workflow (Deployer API).")
@click.argument("output_file", required=True)
@click.option("--tag", "tags", multiple=True, default=None)
@click.option("--namespace", "user_namespace", default=None)
@click.option("--with", "with_decorators", multiple=True, default=None)
@click.option("--workflow-timeout", default=None, type=int)
@click.option("--project", "flyte_project", default="flytesnacks", show_default=True)
@click.option("--domain", "flyte_domain", default="development", show_default=True)
@click.option("--image", default=None, help="Docker image URI.")
@click.option("--max-parallelism", default=None, type=int)
@click.option(
    "--host",
    default="localhost:30080",
    show_default=True,
    help="Flyte Admin endpoint (host:port).",
)
@click.option("--insecure", is_flag=True, default=False, help="Use insecure gRPC (no TLS).")
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--deployer-attribute-file", default=None, hidden=True,
    help="Write deployment info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def deploy(
    obj: object,
    output_file: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    flyte_project: str,
    flyte_domain: str,
    image: str | None,
    max_parallelism: int | None,
    host: str,
    insecure: bool,
    branch: str | None,
    production: bool,
    deployer_attribute_file: str | None,
) -> None:
    if os.path.abspath(sys.argv[0]) == os.path.abspath(output_file):
        raise MetaflowException("Output file cannot be the same as the flow file.")

    _make_compiler_and_write(
        obj, output_file, tags, user_namespace, with_decorators,
        workflow_timeout, flyte_project, flyte_domain, image, max_parallelism,
        branch=branch, production=production,
    )

    if image:
        register_cmd = _pyflyte_register_cmd(flyte_project, flyte_domain, insecure, output_file)
        obj.echo("Registering: {}".format(" ".join(register_cmd)), bold=True)  # type: ignore[attr-defined]
        result = subprocess.run(register_cmd)
        if result.returncode != 0:
            raise FlyteException(f"pyflyte register failed with exit code {result.returncode:d}")

    flow_name = obj.flow.name  # type: ignore[attr-defined]
    wf_name = _wf_fn(flow_name)

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": wf_name,
                    "flow_name": flow_name,
                    "metadata": "{}",
                },
                f,
            )

    obj.echo(  # type: ignore[attr-defined]
        f"Flyte workflow file written to *{output_file}* and registered.\n"
        f"Trigger a run: python {sys.argv[0]} flyte trigger",
        bold=True,
    )


# ---------------------------------------------------------------------------
# flyte trigger
# ---------------------------------------------------------------------------


@flyte.command(help="Trigger a Flyte workflow execution via pyflyte run.")
@click.option("--name", default=None, hidden=True, help="Flow name override (Deployer API).")
@click.option("--tag", "tags", multiple=True, default=None)
@click.option("--namespace", "user_namespace", default=None)
@click.option("--with", "with_decorators", multiple=True, default=None)
@click.option("--workflow-timeout", default=None, type=int)
@click.option("--project", "flyte_project", default="flytesnacks", show_default=True)
@click.option("--domain", "flyte_domain", default="development", show_default=True)
@click.option("--image", default=None, help="Docker image URI.")
@click.option("--max-parallelism", default=None, type=int)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--deployer-attribute-file", default=None, hidden=True,
    help="Write triggered-run info JSON here (used by Metaflow Deployer API).",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (repeatable).",
)
@click.pass_obj
def trigger(
    obj: object,
    name: str | None,
    tags: tuple[str, ...],
    user_namespace: str | None,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    flyte_project: str,
    flyte_domain: str,
    image: str | None,
    max_parallelism: int | None,
    branch: str | None,
    production: bool,
    deployer_attribute_file: str | None,
    run_params: tuple[str, ...],
) -> None:
    # Parse run_params into CLI args for pyflyte run.
    params: dict[str, str] = {}
    for kv in run_params:
        k, _, v = kv.partition("=")
        params[k.strip()] = v.strip()

    flow_name = name or obj.flow.name  # type: ignore[attr-defined]
    run_id = "flyte-local-" + uuid.uuid4().hex[:12]
    pathspec = f"{flow_name}/{run_id}"

    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
        tmp_path = tmp.name

    try:
        _make_compiler_and_write(
            obj, tmp_path, tags, user_namespace, with_decorators,
            workflow_timeout, flyte_project, flyte_domain, image, max_parallelism,
            branch=branch, production=production,
        )

        wf_name = _wf_fn(flow_name)

        # Write the deployer attribute file BEFORE execution so handle_timeout
        # can read it immediately (pyflyte run is synchronous and blocking).
        if deployer_attribute_file:
            with open(deployer_attribute_file, "w") as f:
                json.dump(
                    {
                        "pathspec": pathspec,
                        "name": flow_name,
                        "metadata": "{}",
                    },
                    f,
                )

        # Run locally (no --remote): pyflyte executes tasks as local Python
        # functions so Metaflow metadata is written to ~/.metaflow/.
        # Use pyflyte from the same env as the current Python interpreter.
        _pyflyte = os.path.join(os.path.dirname(sys.executable), "pyflyte")
        if not os.path.isfile(_pyflyte):
            _pyflyte = "pyflyte"
        cmd = [_pyflyte, "run", tmp_path, wf_name]
        for k, v in params.items():
            cmd += [f"--{k}", v]

        env = {**os.environ, "METAFLOW_FLYTE_LOCAL_RUN_ID": run_id}
        obj.echo("Triggering (local): {}".format(" ".join(cmd)), bold=True)  # type: ignore[attr-defined]
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            raise FlyteException(
                f"pyflyte run failed with exit code {result.returncode:d}:\n{result.stderr}"
            )

        obj.echo(  # type: ignore[attr-defined]
            f"Triggered Flyte execution (pathspec: *{pathspec}*).",
            bold=True,
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# flyte resume
# ---------------------------------------------------------------------------


@flyte.command(help="Re-run a failed flow, reusing outputs from steps that already succeeded.")
@click.option(
    "--run-id",
    "clone_run_id",
    required=True,
    help="Metaflow run ID of the failed run to resume (e.g. flyte-abc123).",
)
@click.option("--tag", "tags", multiple=True, default=None)
@click.option("--namespace", "user_namespace", default=None)
@click.option("--with", "with_decorators", multiple=True, default=None)
@click.option("--workflow-timeout", default=None, type=int)
@click.option("--project", "flyte_project", default="flytesnacks", show_default=True)
@click.option("--domain", "flyte_domain", default="development", show_default=True)
@click.option("--image", default=None, help="Docker image URI.")
@click.option("--max-parallelism", default=None, type=int)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--deployer-attribute-file", default=None, hidden=True,
    help="Write resumed-run info JSON here (used by Metaflow Deployer API).",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (repeatable).",
)
@click.pass_obj
def resume(
    obj: object,
    clone_run_id: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    flyte_project: str,
    flyte_domain: str,
    image: str | None,
    max_parallelism: int | None,
    branch: str | None,
    production: bool,
    deployer_attribute_file: str | None,
    run_params: tuple[str, ...],
) -> None:
    """Re-run the flow, passing ``origin_run_id`` so steps whose outputs
    already exist in the Metaflow datastore are skipped (clone-run-id pattern).
    """
    params: dict[str, str] = {}
    for kv in run_params:
        k, _, v = kv.partition("=")
        params[k.strip()] = v.strip()

    flow_name = obj.flow.name  # type: ignore[attr-defined]
    run_id = "flyte-resume-" + uuid.uuid4().hex[:12]
    pathspec = f"{flow_name}/{run_id}"

    with tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w") as tmp:
        tmp_path = tmp.name

    try:
        _make_compiler_and_write(
            obj, tmp_path, tags, user_namespace, with_decorators,
            workflow_timeout, flyte_project, flyte_domain, image, max_parallelism,
            branch=branch, production=production,
        )

        wf_name = _wf_fn(flow_name)

        if deployer_attribute_file:
            with open(deployer_attribute_file, "w") as f:
                json.dump(
                    {
                        "pathspec": pathspec,
                        "name": flow_name,
                        "metadata": "{}",
                    },
                    f,
                )

        _pyflyte = os.path.join(os.path.dirname(sys.executable), "pyflyte")
        if not os.path.isfile(_pyflyte):
            _pyflyte = "pyflyte"
        # Pass origin_run_id so _mf_generate_run_id reuses the failed run's ID,
        # allowing Metaflow to skip steps whose artifacts already exist.
        cmd = [_pyflyte, "run", tmp_path, wf_name, "--origin_run_id", clone_run_id]
        for k, v in params.items():
            cmd += [f"--{k}", v]

        env = {**os.environ, "METAFLOW_FLYTE_LOCAL_RUN_ID": run_id}
        obj.echo("Resuming from run {}: {}".format(clone_run_id, " ".join(cmd)), bold=True)  # type: ignore[attr-defined]
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            raise FlyteException(
                f"pyflyte run failed with exit code {result.returncode:d}:\n{result.stderr}"
            )

        obj.echo(  # type: ignore[attr-defined]
            f"Resumed Flyte execution (pathspec: *{pathspec}*).",
            bold=True,
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pyflyte_register_cmd(flyte_project: str, flyte_domain: str, insecure: bool, output_file: str) -> list[str]:
    cmd = ["pyflyte", "register", "--project", flyte_project, "--domain", flyte_domain]
    if insecure:
        cmd.append("--insecure")
    cmd.append(output_file)
    return cmd


def _make_compiler_and_write(
    obj: object,
    output_file: str,
    tags: tuple[str, ...],
    user_namespace: str | None,
    with_decorators: tuple[str, ...],
    workflow_timeout: int | None,
    flyte_project: str,
    flyte_domain: str,
    image: str | None,
    max_parallelism: int | None,
    branch: str | None = None,
    production: bool = False,
) -> None:
    compiler = FlyteCompiler(
        name=_resolve_name(obj),  # type: ignore[arg-type]
        graph=obj.graph,           # type: ignore[attr-defined]
        flow=obj.flow,             # type: ignore[attr-defined]
        metadata=obj.metadata,     # type: ignore[attr-defined]
        flow_datastore=obj.flow_datastore,  # type: ignore[attr-defined]
        environment=obj.environment,        # type: ignore[attr-defined]
        event_logger=obj.event_logger,      # type: ignore[attr-defined]
        monitor=obj.monitor,                # type: ignore[attr-defined]
        tags=list(tags),
        namespace=user_namespace,
        username=get_username(),
        description=obj.flow.__doc__,       # type: ignore[attr-defined]
        flow_file=os.path.abspath(sys.argv[0]),
        with_decorators=list(with_decorators),
        flyte_project=flyte_project,
        flyte_domain=flyte_domain,
        image=image,
        workflow_timeout=workflow_timeout,
        max_parallelism=max_parallelism,
        branch=branch,
        production=production,
    )
    source = compiler.compile()
    with open(output_file, "w") as f:
        f.write(source)


def _resolve_name(obj: object) -> str:
    name = obj.flow.name  # type: ignore[attr-defined]

    try:
        project_decos = obj.flow._flow_decorators.get("project")  # type: ignore[attr-defined]
        if project_decos:
            project_name = project_decos[0].attributes.get("name", "")
            if project_name:
                name = f"{project_name}.{name}"
    except Exception:
        pass

    return name
