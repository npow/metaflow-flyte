"""Metaflow CLI extension: ``python myflow.py flyte <command>``.

Commands
--------
create    Compile the flow to a Flyte workflow Python file.
run       Compile and immediately run the flow locally via pyflyte.
register  Register the workflow with a Flyte cluster.
"""

from __future__ import annotations

import os
import subprocess
import sys

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.util import get_username

from metaflow_extensions.flyte.plugins.flyte.exception import (
    FlyteException,
    NotSupportedException,
)
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
@click.argument("output_file", required=True)
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
@click.pass_obj
def create(
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
    branch: str | None,
    production: bool,
) -> None:
    if os.path.abspath(sys.argv[0]) == os.path.abspath(output_file):
        raise MetaflowException(
            "Output file name cannot be the same as the flow file name."
        )

    _make_compiler_and_write(
        obj, output_file, tags, user_namespace, with_decorators,
        workflow_timeout, flyte_project, flyte_domain, image, max_parallelism,
        branch=branch, production=production,
    )

    obj.echo(  # type: ignore[attr-defined]
        "Flyte workflow file written to *{out}*.\n"
        "Run locally: pyflyte run {out} {wf}\n"
        "Register:    python {flow} flyte register {out}".format(
            out=output_file,
            wf=_wf_fn(obj.flow.name),  # type: ignore[attr-defined]
            flow=sys.argv[0],
        ),
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
    import tempfile

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

        obj.echo("Running: %s" % " ".join(cmd), bold=True)  # type: ignore[attr-defined]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise FlyteException("pyflyte run failed with exit code %d" % result.returncode)
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
) -> None:
    if os.path.abspath(sys.argv[0]) == os.path.abspath(output_file):
        raise MetaflowException("Output file cannot be the same as the flow file.")

    _make_compiler_and_write(
        obj, output_file, tags, user_namespace, with_decorators,
        workflow_timeout, flyte_project, flyte_domain, image, max_parallelism,
    )

    register_cmd = [
        "pyflyte", "register",
        "--project", flyte_project,
        "--domain", flyte_domain,
    ]
    if insecure:
        register_cmd.append("--insecure")
    register_cmd.append(output_file)

    obj.echo("Registering: %s" % " ".join(register_cmd), bold=True)  # type: ignore[attr-defined]
    result = subprocess.run(register_cmd)
    if result.returncode != 0:
        raise FlyteException("pyflyte register failed with exit code %d" % result.returncode)

    obj.echo(  # type: ignore[attr-defined]
        "Workflow registered. Run it with:\n"
        "  pyflyte run --remote --project {p} --domain {d} {f} {wf}".format(
            p=flyte_project, d=flyte_domain, f=output_file,
            wf=_wf_fn(obj.flow.name),  # type: ignore[attr-defined]
        ),
        bold=True,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


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
    import re
    name = obj.flow.name  # type: ignore[attr-defined]

    try:
        project_decos = obj.flow._flow_decorators.get("project")  # type: ignore[attr-defined]
        if project_decos:
            project_name = project_decos[0].attributes.get("name", "")
            if project_name:
                name = "%s.%s" % (project_name, name)
    except Exception:
        pass

    return name


def _wf_fn(flow_name: str) -> str:
    result: list[str] = []
    for i, ch in enumerate(flow_name):
        if ch.isupper() and i > 0:
            result.append("_")
        result.append(ch.lower())
    return "".join(result)
