"""Python source-code generator for the Metaflow-Flyte integration.

``generate_flyte_file`` is the single public function.  It takes a
``FlowSpec`` plus a ``FlyteFlowConfig`` and returns the complete text of a
self-contained Python file that, when executed with ``pyflyte run``, runs
the Metaflow flow as a Flyte workflow.

Design constraints
------------------
- The generated file must be self-contained — no imports from this package.
- It uses only stdlib + metaflow + flytekit.
- Linear, split/join, foreach, and conditional (split-switch) graph shapes
  are all supported.
- Foreach body tasks run in parallel via Flyte's ``@dynamic`` sub-workflows.
- Conditional branches use a ``@dynamic`` router that reads the Metaflow
  ``_transition`` artifact at runtime and calls only the selected branch task.
- Each Flyte task produces a ``flytekit.Deck`` showing Metaflow artifact
  retrieval snippets so users can copy-paste code from the Flyte UI.

Graph shape → generated code mapping
-------------------------------------
linear / split / end          plain ``@task`` functions wired sequentially
foreach                       ``@task`` + ``@dynamic`` fan-out expander
split-switch (conditional)    ``@task`` returning JSON + ``@dynamic`` router
join (foreach)                ``@task`` accepting ``List[str]`` of body task_ids
join (split)                  ``@task`` accepting one ``str`` per branch
join (condition)              ``@task`` accepting JSON from the dynamic router
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import datetime

from metaflow_extensions.flyte.plugins.flyte._types import (
    FlowSpec,
    FlyteFlowConfig,
    NodeType,
    ParameterSpec,
    StepSpec,
    TriggerOnFinishSpec,
    TriggerSpec,
)

# ---------------------------------------------------------------------------
# Static sections — embedded verbatim in every generated file
# ---------------------------------------------------------------------------

_HELPERS = '''\
# ---------------------------------------------------------------------------
# Runtime helpers
# ---------------------------------------------------------------------------


def _foreach_info_path(run_id: str, step_name: str) -> str:
    safe = run_id.replace('/', '_').replace(':', '_')
    return os.path.join(tempfile.gettempdir(), f'mf_flyte_foreach_{safe}_{step_name}.json')


def _read_foreach_info(path: str) -> int:
    try:
        with open(path) as _f:
            return int(json.load(_f)['num_splits'])
    except Exception:
        return 0


def _run_cmd(cmd: list[str], extra_env: dict[str, str] | None = None) -> None:
    env = os.environ.copy()
    env.setdefault('METAFLOW_DATASTORE_SYSROOT_LOCAL', os.path.expanduser('~'))
    if extra_env:
        env.update(extra_env)
    proc = subprocess.Popen(cmd, env=env)
    def _forward(sig, _frame):
        proc.send_signal(sig)
    _prev_term = signal.signal(signal.SIGTERM, _forward)
    _prev_int  = signal.signal(signal.SIGINT,  _forward)
    try:
        proc.wait()
    finally:
        signal.signal(signal.SIGTERM, _prev_term)
        signal.signal(signal.SIGINT,  _prev_int)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)


def _mf_artifact_names(run_id: str, step_name: str, task_id: str) -> list[str]:
    """Return user-defined artifact names (no values loaded)."""
    try:
        from metaflow.datastore import FlowDataStore
        from metaflow.plugins import DATASTORES
        _impl = next(d for d in DATASTORES if d.TYPE == DATASTORE_TYPE)
        _root = _impl.get_datastore_root_from_config(lambda *a: None)
        _fds = FlowDataStore(FLOW_NAME, None, storage_impl=_impl, ds_root=_root)
        _tds = _fds.get_task_datastore(run_id, step_name, task_id, attempt=0, mode='r')
        _SKIP = {'name', 'input'}
        return [n for n in _tds if not n.startswith('_') and n not in _SKIP]
    except Exception:
        return []


def _step_cmd(
    step_name: str,
    run_id: str,
    task_id: str,
    input_paths: str,
    retry_count: int = 0,
    max_user_code_retries: int = 0,
    split_index: int | None = None,
) -> list[str]:
    cmd: list[str] = [
        sys.executable, FLOW_FILE,
        "--datastore", DATASTORE_TYPE,
        "--metadata", METADATA_TYPE,
        "--no-pylint",
        "--with=flyte_internal",
    ]
    # Propagate the execution environment so that environment-aware decorators
    # (e.g. @conda) activate the correct environment manager at task runtime.
    if ENVIRONMENT_TYPE and ENVIRONMENT_TYPE != "local":
        cmd += ["--environment", ENVIRONMENT_TYPE]
    # Propagate the @project branch so the step subprocess resolves the same
    # project branch name that was used at compile time (create / trigger).
    if PROJECT_BRANCH:
        cmd += ["--branch", PROJECT_BRANCH]
    cmd += [
        "step", step_name,
        "--run-id", run_id,
        "--task-id", task_id,
        "--retry-count", str(retry_count),
        "--max-user-code-retries", str(max_user_code_retries),
        "--input-paths", input_paths,
    ]
    for _tag in TAGS:
        cmd += ["--tag", _tag]
    for _deco in WITH_DECORATORS:
        cmd += [f"--with={_deco}"]
    if NAMESPACE:
        cmd += ["--namespace", NAMESPACE]
    if split_index is not None:
        cmd += ["--split-index", str(split_index)]
    return cmd


def _flyte_env() -> dict[str, str]:
    """Capture Flyte execution context for the flyte_internal decorator."""
    env: dict[str, str] = {}
    try:
        ctx = current_context()
        env['METAFLOW_FLYTE_EXECUTION_ID'] = str(ctx.execution_id.name)
        env['METAFLOW_FLYTE_EXECUTION_PROJECT'] = str(ctx.execution_id.project)
        env['METAFLOW_FLYTE_EXECUTION_DOMAIN'] = str(ctx.execution_id.domain)
    except Exception:
        pass
    return env


def _read_condition_branch(run_id: str, step_name: str, task_id: str) -> str:
    """Read the _transition artifact to determine which branch was taken."""
    try:
        from metaflow.datastore import FlowDataStore
        from metaflow.plugins import DATASTORES
        _impl = next(d for d in DATASTORES if d.TYPE == DATASTORE_TYPE)
        _root = _impl.get_datastore_root_from_config(lambda *a: None)
        _fds = FlowDataStore(FLOW_NAME, None, storage_impl=_impl, ds_root=_root)
        _tds = _fds.get_task_datastore(run_id, step_name, task_id, attempt=0, mode='r')
        _transition = _tds['_transition']
        # _transition is a list of step names; return the first one as the branch taken
        if isinstance(_transition, (list, tuple)) and _transition:
            return str(_transition[0])
        return str(_transition)
    except Exception:
        return ''


def _emit_deck(run_id: str, step_name: str, task_id: str) -> None:
    """Render Metaflow artifact retrieval snippets in the Flyte UI Deck."""
    try:
        artifact_names = _mf_artifact_names(run_id, step_name, task_id)
        pathspec = f'{FLOW_NAME}/{run_id}/{step_name}/{task_id}'
        html = f'<h3>Metaflow Artifacts &mdash; <code>{pathspec}</code></h3>'
        if artifact_names:
            html += '<pre style="background:#f5f5f5;padding:12px;border-radius:4px">'
            html += 'from metaflow import Task\\n'
            html += f"task = Task('{pathspec}')\\n\\n"
            for _n in artifact_names:
                html += f'# {_n}\\n'
                html += f'task.data.{_n}\\n\\n'
            html += '</pre>'
        else:
            html += '<p><em>No user artifacts produced.</em></p>'
        flytekit.Deck('metaflow', html)
    except Exception:
        pass'''


_RUN_ID_TASK = '''\
# ---------------------------------------------------------------------------
# Run-ID generation — derives a stable Metaflow run_id from the Flyte
# execution ID so every step in the same execution shares a run.
# ---------------------------------------------------------------------------


@_mf_task()
def _mf_generate_run_id(origin_run_id: str = '') -> str:
    # If a prior run_id is supplied (e.g. via Flyte --recover), reuse it so
    # Metaflow treats this execution as a resume of the original run.
    if origin_run_id:
        return origin_run_id
    try:
        ctx = current_context()
        name = ctx.execution_id.name
        # Check for a pre-seeded run ID (set by local trigger for deterministic pathspec).
        seeded = os.environ.get('METAFLOW_FLYTE_LOCAL_RUN_ID')
        if seeded:
            return seeded
        # 'local' is the placeholder used by pyflyte run without --remote;
        # generate a unique id so each local run gets its own Metaflow run.
        if not name or name == 'local':
            return 'flyte-local-' + uuid.uuid4().hex[:12]
        return 'flyte-' + name
    except Exception:
        return 'flyte-' + uuid.uuid4().hex[:16]'''


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def _find_foreach_join_step(spec: FlowSpec, foreach_step_name: str) -> StepSpec | None:
    """Return the foreach-join StepSpec that closes *foreach_step_name*'s fan-out."""
    for s in spec.steps:
        if s.is_foreach_join and s.split_parents and s.split_parents[-1] == foreach_step_name:
            return s
    return None


def generate_flyte_file(spec: FlowSpec, cfg: FlyteFlowConfig) -> str:
    """Return the full Python source of a runnable Flyte workflow file."""
    # Pre-compute foreach and conditional (split-switch) info in one pass.
    foreach_body: dict[str, str] = {}           # foreach_step  -> body_step name
    condition_branches: dict[str, frozenset[str]] = {}  # switch_step -> branch step names
    step_by_name: dict[str, StepSpec] = {}
    for s in spec.steps:
        step_by_name[s.name] = s
        if s.node_type == NodeType.FOREACH and s.out_funcs:
            foreach_body[s.name] = s.out_funcs[0]
        elif s.node_type == NodeType.SPLIT_SWITCH:
            condition_branches[s.name] = frozenset(s.out_funcs)
    foreach_body_set = set(foreach_body.values())
    condition_branch_set: set[str] = set().union(*condition_branches.values()) if condition_branches else set()

    # Detect nested foreach: a foreach body step that is itself a foreach.
    # For each such outer foreach step, record the inner join step that will be
    # consumed inside the outer @dynamic expander rather than as a standalone
    # workflow step.
    #
    # nested_foreach_joins: outer_foreach_step_name -> inner_join_step_name
    # steps_consumed_in_dynamic: set of step names executed inside an outer dynamic
    nested_foreach_joins: dict[str, str] = {}
    steps_consumed_in_dynamic: set[str] = set()
    for s in spec.steps:
        if s.node_type == NodeType.FOREACH:
            body_name = foreach_body.get(s.name)
            if body_name and body_name in foreach_body:
                # body step is itself a foreach — find its join
                inner_join = _find_foreach_join_step(spec, body_name)
                if inner_join:
                    nested_foreach_joins[s.name] = inner_join.name
                    steps_consumed_in_dynamic.add(inner_join.name)

    parts: list[str] = [
        _render_header(spec, cfg),
        _HELPERS,
        _RUN_ID_TASK,
    ]

    for step in spec.steps:
        parts.append(_render_task(step, spec, foreach_body, foreach_body_set, condition_branch_set, nested_foreach_joins))
        if step.node_type == NodeType.FOREACH:
            body_step = step_by_name[foreach_body[step.name]]
            inner_join_name = nested_foreach_joins.get(step.name)
            inner_join_step = step_by_name[inner_join_name] if inner_join_name else None
            parts.append(_render_foreach_dynamic(step, body_step, inner_join_step))
        elif step.node_type == NodeType.SPLIT_SWITCH:
            branch_steps = [s for s in spec.steps if s.name in condition_branches[step.name]]
            parts.append(_render_condition_dynamic(step, branch_steps))

    parts.append(_render_workflow(
        spec, cfg, foreach_body, foreach_body_set,
        condition_branches, condition_branch_set,
        nested_foreach_joins, steps_consumed_in_dynamic,
    ))

    if spec.schedule_cron:
        parts.append(_render_launch_plan(spec))

    for trigger in spec.triggers:
        parts.append(_render_event_trigger_launch_plan(spec, trigger))

    for tof in spec.trigger_on_finishes:
        parts.append(_render_trigger_on_finish_launch_plan(spec, tof))

    parts.append(f"if __name__ == '__main__':\n    {_wf_fn(spec.name)}()")

    return "\n\n\n".join(parts)


# ---------------------------------------------------------------------------
# Header + configuration block
# ---------------------------------------------------------------------------


def _render_header(spec: FlowSpec, cfg: FlyteFlowConfig) -> str:
    effective_flow_name = cfg.flow_name if cfg.flow_name else spec.name
    spec_tags_set = set(spec.tags)
    all_tags = list(spec.tags) + [t for t in cfg.tags if t not in spec_tags_set]
    return f'''\
# Generated by metaflow-flyte on {datetime.now().isoformat(timespec="seconds")}
# Metaflow flow: {spec.name}
# Do not edit — regenerate with: python {cfg.flow_file} flyte create <file>

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import uuid
from datetime import timedelta
from typing import List

import flytekit
from flytekit import Resources, current_context, dynamic, task, workflow
from flytekit.core.launch_plan import LaunchPlan

# ---------------------------------------------------------------------------
# Compile-time configuration
# ---------------------------------------------------------------------------
FLOW_FILE: str = os.environ.get("METAFLOW_FLYTE_FLOW_FILE", {cfg.flow_file!r})
FLOW_NAME: str = {effective_flow_name!r}
DATASTORE_TYPE: str = {cfg.datastore_type!r}
METADATA_TYPE: str = {cfg.metadata_type!r}
USERNAME: str = {cfg.username!r}
TAGS: List[str] = {all_tags!r}
NAMESPACE: str | None = {spec.namespace!r}
FLYTE_PROJECT: str = {cfg.flyte_project!r}
FLYTE_DOMAIN: str = {cfg.flyte_domain!r}
IMAGE: str | None = {cfg.image!r}
WITH_DECORATORS: List[str] = {list(cfg.with_decorators)!r}
# Raw @project branch passed via --branch (without "test."/"user." prefix).
# Empty string means no explicit branch — @project defaults to user.<username>.
PROJECT_BRANCH: str = {(cfg.project_info or {}).get("branch_raw", "")!r}
# Compile-time config values serialised to JSON; injected as
# METAFLOW_FLOW_CONFIG_VALUE into every step subprocess so that config_expr
# and @project decorators evaluate correctly at task runtime.
FLOW_CONFIG_VALUE: str | None = {cfg.flow_config_value!r}
# Execution environment type passed via --environment at compile time.
# Propagated to every step subprocess so that environment-aware decorators
# (e.g. @conda) activate the correct environment manager at task runtime.
ENVIRONMENT_TYPE: str = {cfg.environment_type!r}

# Task decorator factory — applies IMAGE and enables Deck when set.
_TASK_KWARGS = {{'container_image': IMAGE, 'enable_deck': True}} if IMAGE else {{'enable_deck': True}}


def _mf_task(**extra):
    """Apply IMAGE + extra kwargs to @task."""
    return task(**{{**_TASK_KWARGS, **extra}})'''


# ---------------------------------------------------------------------------
# Per-step @task functions
# ---------------------------------------------------------------------------


def _resources_expr(step: StepSpec) -> str | None:
    """Return a ``Resources(...)`` expression for ``@resources`` attributes, or None."""
    if step.resource_cpu is None and step.resource_gpu is None and step.resource_memory is None:
        return None
    args = []
    if step.resource_cpu is not None:
        args.append(f"cpu={str(step.resource_cpu)!r}")
    if step.resource_memory is not None:
        # Metaflow memory is in MB; Flyte uses string like "1024Mi"
        args.append(f"mem={f'{step.resource_memory}Mi'!r}")
    if step.resource_gpu is not None:
        args.append(f"gpu={str(step.resource_gpu)!r}")
    return "Resources({})".format(", ".join(args))


def _task_decorator(step: StepSpec) -> str:
    """Build the ``@_mf_task(...)`` decorator line for a step."""
    parts = [f"retries={step.max_user_code_retries:d}"]
    if step.timeout_seconds is not None:
        parts.append(f"timeout=timedelta(seconds={step.timeout_seconds:d})")
    res = _resources_expr(step)
    if res is not None:
        # Translate @resources to native Flyte resource requests/limits.
        parts.append(f"requests={res}")
        parts.append(f"limits={res}")
    return "@_mf_task({})".format(", ".join(parts))


def _task_signature(step: StepSpec, spec: FlowSpec, is_start: bool, is_foreach_body: bool) -> str:
    """Build the ``def _step_<name>(...)`` signature line."""
    fn = _task_fn(step.name)
    if is_start:
        param_args = "".join(
            f", _param_{p.name}: {p.type_name} = {p.default!r}"
            for p in spec.parameters
        )
        return f"def {fn}(run_id: str{param_args}) -> str:"
    if step.is_foreach_join:
        return f"def {fn}(run_id: str, _body_task_ids: List[str]) -> str:"
    if step.is_split_join:
        in_args = "".join(f", _in_{p}: str" for p in step.in_funcs)
        return f"def {fn}(run_id: str{in_args}) -> str:"
    if step.is_condition_join:
        return f"def {fn}(run_id: str, _branch_result_json: str) -> str:"
    # A step that is both a foreach fan-out AND a foreach body (nested foreach)
    # must accept ``split_index`` so the outer @dynamic can invoke it per-split.
    if step.node_type == NodeType.FOREACH and is_foreach_body:
        return f"def {fn}(run_id: str, prev_task_id: str, split_index: int = 0) -> str:"
    if step.node_type in (NodeType.SPLIT_SWITCH, NodeType.FOREACH):
        return f"def {fn}(run_id: str, prev_task_id: str) -> str:"
    if is_foreach_body:
        return f"def {fn}(run_id: str, prev_task_id: str, split_index: int = 0) -> str:"
    # linear, split, end, condition branch
    return f"def {fn}(run_id: str, prev_task_id: str) -> str:"


def _input_paths_lines(
    step: StepSpec,
    spec: FlowSpec,
    foreach_body: dict[str, str],
    is_start: bool,
    nested_foreach_joins: dict[str, str] | None = None,
) -> list[str]:
    """Lines that compute ``input_paths`` for a step (no leading indent)."""
    if is_start:
        return _start_init_lines(spec) + [
            'input_paths: str = f"{run_id}/_parameters/{param_task_id}"',
        ]
    if step.is_foreach_join:
        foreach_step_name = step.split_parents[-1]
        # For outer joins in a nested-foreach, the body task IDs that arrive via
        # ``_body_task_ids`` are actually the inner-join task IDs (produced by
        # the outer @dynamic expander).  Use the inner-join step name so that
        # Metaflow reads the correct artifacts from the inner-join tasks.
        nfj = nested_foreach_joins or {}
        if foreach_step_name in nfj:
            body_name = nfj[foreach_step_name]
        else:
            body_name = foreach_body.get(foreach_step_name) or step.in_funcs[0]
        return [
            'input_paths: str = ",".join('
            f'f"{{run_id}}/{body_name}/{{tid}}" for tid in _body_task_ids)'
        ]
    if step.is_split_join:
        joined = ", ".join(
            f'f"{{run_id}}/{p}/{{_in_{p}}}"' for p in step.in_funcs
        )
        return [f"input_paths: str = \",\".join([{joined}])"]
    if step.is_condition_join:
        return [
            "_branch_info = json.loads(_branch_result_json)",
            "_branch_step_name: str = _branch_info['branch_step']",
            "_branch_tid: str = _branch_info['task_id']",
            'input_paths: str = f"{run_id}/{_branch_step_name}/{_branch_tid}"',
        ]
    parent = step.in_funcs[0] if step.in_funcs else "start"
    return [f'input_paths: str = f"{{run_id}}/{parent}/{{prev_task_id}}"']


def _start_init_lines(spec: FlowSpec) -> list[str]:
    """Lines that run ``metaflow init`` before the start step (no leading indent)."""
    indent = "    "
    lines = [
        "param_task_id: str = uuid.uuid4().hex[:16]",
        "_init_cmd: list[str] = [",
        indent + "sys.executable, FLOW_FILE,",
        indent + '"--datastore", DATASTORE_TYPE,',
        indent + '"--metadata", METADATA_TYPE,',
        indent + '"--no-pylint",',
        "]",
        # Propagate the execution environment so that environment-aware decorators
        # (e.g. @conda_base) validate correctly during init.
        'if ENVIRONMENT_TYPE and ENVIRONMENT_TYPE != "local":',
        indent + '_init_cmd += ["--environment", ENVIRONMENT_TYPE]',
        "_init_cmd += [",
        indent + '"init",',
        indent + '"--run-id", run_id,',
        indent + '"--task-id", param_task_id,',
        "]",
        "for _tag in TAGS:",
        indent + '_init_cmd += ["--tag", _tag]',
    ]
    for p in spec.parameters:
        lines.append(f'_init_cmd += ["--{p.name}", str(_param_{p.name})]')
    lines.append("_run_cmd(_init_cmd)")
    return lines


def _extra_env_lines(step: StepSpec) -> list[str]:
    """Lines that set up ``_extra_env`` for a step (no leading indent)."""
    lines = ["_extra_env: dict[str, str] = _flyte_env()"]
    if step.node_type == NodeType.FOREACH:
        lines += [
            f"foreach_path: str = _foreach_info_path(run_id, {step.name!r})",
            "_extra_env['METAFLOW_FLYTE_FOREACH_INFO_PATH'] = foreach_path",
        ]
    # Propagate compile-time config values so that config_expr / @project
    # decorators evaluate correctly at task runtime (mirrors Airflow/Prefect).
    lines += [
        "if FLOW_CONFIG_VALUE:",
        "    _extra_env['METAFLOW_FLOW_CONFIG_VALUE'] = FLOW_CONFIG_VALUE",
    ]
    if step.env_vars:
        lines.append(f"_extra_env.update({dict(step.env_vars)!r})")
    return lines


def _run_step_lines(step: StepSpec, is_foreach_body: bool) -> list[str]:
    """Lines that invoke ``_run_cmd(_step_cmd(...))`` (no leading indent)."""
    indent = "    "
    max_retries = step.max_user_code_retries

    def _step_cmd_args(retry_var: str) -> list[str]:
        args = [
            f"{step.name!r}, run_id, task_id, input_paths,",
            f"retry_count={retry_var},",
            f"max_user_code_retries={max_retries:d},",
        ]
        if is_foreach_body:
            args.append("split_index=split_index,")
        return args

    if max_retries == 0:
        lines = ["_run_cmd(_step_cmd("]
        for arg in _step_cmd_args("0"):
            lines.append(indent + arg)
        lines.append("), extra_env=_extra_env)")
        return lines

    # Derive retry_count from Flyte's native retry mechanism.
    # Flyte's @task(retries=N) handles the retry loop natively; we just
    # need to pass the current attempt number to the step subprocess.
    lines = [
        "try:",
        indent + "_retry_count = current_context().retry_count",
        "except Exception:",
        indent + "_retry_count = 0",
        "_run_cmd(_step_cmd(",
    ]
    for arg in _step_cmd_args("_retry_count"):
        lines.append(indent + arg)
    lines.append("), extra_env=_extra_env)")
    return lines


def _return_lines(step: StepSpec, is_condition_branch: bool) -> list[str]:
    """Lines that form the return statement of a step task (no leading indent)."""
    if step.node_type == NodeType.FOREACH:
        return [
            "_num_splits: int = _read_foreach_info(foreach_path)",
            "return json.dumps({'task_id': task_id, 'num_splits': _num_splits})",
        ]
    if step.node_type == NodeType.SPLIT_SWITCH:
        return [
            f"_branch_taken: str = _read_condition_branch(run_id, {step.name!r}, task_id)",
            "return json.dumps({'task_id': task_id, 'branch_taken': _branch_taken})",
        ]
    if is_condition_branch:
        return [f"return json.dumps({{'branch_step': {step.name!r}, 'task_id': task_id}})"]
    return ["return task_id"]


def _render_task(
    step: StepSpec,
    spec: FlowSpec,
    foreach_body: dict[str, str],
    foreach_body_set: set[str],
    condition_branch_set: set[str],
    nested_foreach_joins: dict[str, str] | None = None,
) -> str:
    """Return the full ``@_mf_task(...)`` function source for *step*."""
    is_start = step.name == "start"
    is_foreach_body = step.name in foreach_body_set
    is_condition_branch = step.name in condition_branch_set
    indent = "    "

    body_lines = (
        ["task_id: str = uuid.uuid4().hex[:16]"]
        + _input_paths_lines(step, spec, foreach_body, is_start, nested_foreach_joins)
        + _extra_env_lines(step)
        + _run_step_lines(step, is_foreach_body)
        + [f"_emit_deck(run_id, {step.name!r}, task_id)"]
        + _return_lines(step, is_condition_branch)
    )

    lines = [
        _task_decorator(step),
        _task_signature(step, spec, is_start, is_foreach_body),
    ] + [indent + ln for ln in body_lines]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Foreach @dynamic fan-out expander
# ---------------------------------------------------------------------------


def _render_foreach_dynamic(
    foreach_step: StepSpec,
    body_step: StepSpec,
    inner_join_step: StepSpec | None = None,
) -> str:
    """Return a ``@dynamic`` function that fans out the foreach body tasks.

    If *inner_join_step* is supplied, the body step is itself a foreach.  In
    that case the expander also calls the inner-level ``@dynamic`` expander and
    runs the inner join step per outer iteration, returning the inner-join task
    IDs so that the outer join step can read from them.
    """
    fn_name = f"_foreach_{foreach_step.name}_dynamic"
    body_task = _task_fn(body_step.name)
    step_repr = repr(foreach_step.name)

    if inner_join_step is None:
        # Simple (non-nested) foreach: fan out body tasks and return their IDs.
        return f'''\
@dynamic(**_TASK_KWARGS)
def {fn_name}(run_id: str, foreach_result_json: str) -> List[str]:
    """Fan out {step_repr} body tasks in parallel."""
    _info = json.loads(foreach_result_json)
    _foreach_task_id: str = _info['task_id']
    _num_splits: int = int(_info['num_splits'])
    _task_ids: List[str] = []
    for _i in range(_num_splits):
        _tid = {body_task}(run_id=run_id, prev_task_id=_foreach_task_id, split_index=_i)
        _task_ids.append(_tid)
    return _task_ids'''

    # Nested foreach: body step is itself a foreach.
    # For each outer split, run the body task (which is a foreach step), fan out
    # its inner body tasks via the inner @dynamic, then run the inner join step.
    # Return the inner-join task IDs so the outer join can read from them.
    inner_dynamic_fn = f"_foreach_{body_step.name}_dynamic"
    inner_join_task = _task_fn(inner_join_step.name)
    return f'''\
@dynamic(**_TASK_KWARGS)
def {fn_name}(run_id: str, foreach_result_json: str) -> List[str]:
    """Fan out {step_repr} body tasks (nested foreach) in parallel."""
    _info = json.loads(foreach_result_json)
    _foreach_task_id: str = _info['task_id']
    _num_splits: int = int(_info['num_splits'])
    _inner_join_tids: List[str] = []
    for _i in range(_num_splits):
        # Run the body step (itself a foreach): returns foreach_result_json for
        # the inner level.
        _inner_foreach_json = {body_task}(
            run_id=run_id, prev_task_id=_foreach_task_id, split_index=_i
        )
        # Fan out the inner body tasks and collect their IDs.
        _inner_body_tids = {inner_dynamic_fn}(
            run_id=run_id, foreach_result_json=_inner_foreach_json
        )
        # Run the inner join for this outer iteration.
        _inner_join_tid = {inner_join_task}(
            run_id=run_id, _body_task_ids=_inner_body_tids
        )
        _inner_join_tids.append(_inner_join_tid)
    return _inner_join_tids'''


# ---------------------------------------------------------------------------
# Conditional (split-switch) @dynamic router
# ---------------------------------------------------------------------------


def _render_condition_dynamic(switch_step: StepSpec, branch_steps: list[StepSpec]) -> str:
    """Return a ``@dynamic`` function that routes to exactly one branch task.

    The split-switch task returns JSON ``{"task_id": ..., "branch_taken": ...}``.
    This router inspects ``branch_taken`` at runtime and calls only the matching
    branch task, returning its JSON so the join step can locate the artifact.
    """
    fn_name = f"_cond_{switch_step.name}_router"
    step_repr = repr(switch_step.name)
    lines = [
        "@dynamic(**_TASK_KWARGS)",
        f"def {fn_name}(run_id: str, switch_result_json: str) -> str:",
        f'    """Route to the branch task selected at runtime for step {step_repr}."""',
        "    _info = json.loads(switch_result_json)",
        "    _switch_task_id: str = _info['task_id']",
        "    _branch_taken: str = _info['branch_taken']",
    ]
    branch_names = [bs.name for bs in branch_steps]
    for i, name in enumerate(branch_names):
        keyword = "if" if i == 0 else "elif"
        lines.append(f"    {keyword} _branch_taken == {name!r}:")
        lines.append(f"        return {_task_fn(name)}(run_id=run_id, prev_task_id=_switch_task_id)")
    if branch_names:
        lines += [
            "    else:",
            f"        return {_task_fn(branch_names[0])}(run_id=run_id, prev_task_id=_switch_task_id)",
        ]
    else:
        lines.append("    return json.dumps({'branch_step': '', 'task_id': _switch_task_id})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Top-level @workflow function
# ---------------------------------------------------------------------------


def _render_workflow(
    spec: FlowSpec,
    cfg: FlyteFlowConfig,
    foreach_body: dict[str, str],
    foreach_body_set: set[str],
    condition_branches: dict[str, frozenset[str]],
    condition_branch_set: set[str],
    nested_foreach_joins: dict[str, str] | None = None,
    steps_consumed_in_dynamic: set[str] | None = None,
) -> str:
    """Return the ``@workflow`` function source."""
    nfj = nested_foreach_joins or {}
    consumed = steps_consumed_in_dynamic or set()

    sig = _workflow_signature(spec.parameters)
    lines = [
        "@workflow",
        f"def {_wf_fn(spec.name)}({sig}) -> None:",
    ]
    if spec.description:
        lines.append(f"    {spec.description!r}")
    lines.append("    run_id = _mf_generate_run_id(origin_run_id=origin_run_id)")

    task_id_vars: dict[str, str] = {}  # step_name -> Python variable holding task_id

    for step in spec.steps:
        var = f"_tid_{step.name}"
        is_start = step.name == "start"

        if step.name in foreach_body_set:
            continue  # already registered / executed inside the outer foreach block

        if step.name in consumed:
            continue  # executed inside an outer @dynamic expander (e.g. inner join)

        if step.node_type == NodeType.FOREACH:
            foreach_result_var = f"_foreach_result_json_{step.name}"
            if is_start:
                pkw = "".join(f", _param_{p.name}={p.name}" for p in spec.parameters)
                lines.append(f"    {foreach_result_var} = {_task_fn(step.name)}(run_id=run_id{pkw})")
            else:
                parent = step.in_funcs[0]
                lines.append(
                    f"    {foreach_result_var} = {_task_fn(step.name)}(run_id=run_id, prev_task_id={task_id_vars[parent]})"
                )
            # For nested foreach, the outer @dynamic returns the inner-join task IDs.
            # Name the variable after the inner-join step so the outer join's
            # body_var lookup resolves correctly.
            if step.name in nfj:
                inner_join_name = nfj[step.name]
                body_tids_var = f"_body_tids_{inner_join_name}"
                task_id_vars[inner_join_name] = body_tids_var
            else:
                body_name = foreach_body[step.name]
                body_tids_var = f"_body_tids_{body_name}"
                task_id_vars[body_name] = body_tids_var
            lines.append(
                f"    {body_tids_var} = _foreach_{step.name}_dynamic(run_id=run_id, foreach_result_json={foreach_result_var})"
            )
            task_id_vars[step.name] = foreach_result_var
            continue

        if step.node_type == NodeType.SPLIT_SWITCH:
            switch_result_var = f"_cond_result_json_{step.name}"
            if is_start:
                pkw = "".join(f", _param_{p.name}={p.name}" for p in spec.parameters)
                lines.append(f"    {switch_result_var} = {_task_fn(step.name)}(run_id=run_id{pkw})")
            else:
                parent = step.in_funcs[0]
                lines.append(
                    f"    {switch_result_var} = {_task_fn(step.name)}(run_id=run_id, prev_task_id={task_id_vars[parent]})"
                )
            routed_tid_var = f"_cond_routed_tid_{step.name}"
            lines.append(
                f"    {routed_tid_var} = _cond_{step.name}_router(run_id=run_id, switch_result_json={switch_result_var})"
            )
            task_id_vars[step.name] = switch_result_var
            for branch_name in condition_branches.get(step.name, set()):
                task_id_vars[branch_name] = routed_tid_var
            continue

        if step.name in condition_branch_set:
            continue  # handled inside the split-switch block above

        if is_start:
            pkw = "".join(f", _param_{p.name}={p.name}" for p in spec.parameters)
            lines.append(f"    {var} = {_task_fn(step.name)}(run_id=run_id{pkw})")

        elif step.is_foreach_join:
            foreach_step_name = step.split_parents[-1]
            # For nested foreach: the outer @dynamic returns inner-join task IDs,
            # so use the inner-join step name as the tids-var key.
            if foreach_step_name in nfj:
                body_var = f"_body_tids_{nfj[foreach_step_name]}"
            else:
                body_var = "_body_tids_{}".format(foreach_body.get(foreach_step_name, "body"))
            lines.append(
                f"    {var} = {_task_fn(step.name)}(run_id=run_id, _body_task_ids={body_var})"
            )

        elif step.is_split_join:
            in_kwargs = "".join(f", _in_{p}={task_id_vars[p]}" for p in step.in_funcs)
            lines.append(f"    {var} = {_task_fn(step.name)}(run_id=run_id{in_kwargs})")

        elif step.is_condition_join:
            switch_step_name = _find_switch_step_for_join(step, condition_branches)
            branch_result_var = f"_cond_routed_tid_{switch_step_name}"
            lines.append(
                f"    {var} = {_task_fn(step.name)}(run_id=run_id, _branch_result_json={branch_result_var})"
            )

        else:
            # linear, split (not foreach/conditional), end
            parent = step.in_funcs[0] if step.in_funcs else "start"
            lines.append(
                f"    {var} = {_task_fn(step.name)}(run_id=run_id, prev_task_id={task_id_vars[parent]})"
            )

        task_id_vars[step.name] = var

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LaunchPlan for @schedule support
# ---------------------------------------------------------------------------


def _render_launch_plan(spec: FlowSpec) -> str:
    """Return the ``LaunchPlan`` source for a scheduled workflow."""
    wf = _wf_fn(spec.name)
    schedule_name = repr(wf + "_schedule")
    return f'''\
# Schedule: register a LaunchPlan that triggers on the given cron.
_schedule_launch_plan = LaunchPlan.get_or_create(
    workflow={wf},
    name={schedule_name},
    schedule=flytekit.CronSchedule(schedule={spec.schedule_cron!r}),
)'''


# ---------------------------------------------------------------------------
# LaunchPlans for @trigger and @trigger_on_finish support
# ---------------------------------------------------------------------------


def _render_event_trigger_launch_plan(spec: FlowSpec, trigger: TriggerSpec) -> str:
    """Return a LaunchPlan comment block for @trigger(event=...).

    Flyte does not have a built-in named-event trigger equivalent to Metaflow's
    @trigger decorator.  The closest Flyte primitive is a LaunchPlan that is
    activated manually (or via an external orchestration layer that watches for
    the named event and calls LaunchPlan.execute()).  We therefore emit an
    *inactive* LaunchPlan stub with the event name encoded in the plan name so
    it is discoverable in the Flyte UI, and include a descriptive comment
    explaining the mapping.
    """
    wf = _wf_fn(spec.name)
    safe_event = re.sub(r"[^a-zA-Z0-9_]", "_", trigger.event_name)
    plan_name = repr(f"{wf}_on_event_{safe_event}")
    fixed_inputs_parts = []
    for flow_param, event_field in trigger.parameter_map:
        # Emit as a comment — fixed_inputs requires concrete values at
        # registration time, which we don't have for dynamic event fields.
        fixed_inputs_parts.append(f"# parameter {flow_param} <- event field {event_field!r}")
    fixed_inputs_comment = "\n" + "\n".join(fixed_inputs_parts) if fixed_inputs_parts else ""
    return f'''\
# ---------------------------------------------------------------------------
# @trigger(event={trigger.event_name!r}) → Flyte LaunchPlan stub
#
# Flyte does not have a native named-event trigger.  This inactive LaunchPlan
# is registered with a name that encodes the event so it can be discovered in
# the Flyte UI and activated programmatically when the event fires.
# To trigger on the event, call:
#   lp = LaunchPlan.get_or_create(workflow={wf}, name={plan_name})
#   lp()
#{fixed_inputs_comment}
# ---------------------------------------------------------------------------
_event_trigger_{safe_event} = LaunchPlan.get_or_create(
    workflow={wf},
    name={plan_name},
)'''


def _render_trigger_on_finish_launch_plan(spec: FlowSpec, tof: TriggerOnFinishSpec) -> str:
    """Return a LaunchPlan for @trigger_on_finish(flow=UpstreamFlow).

    Flyte supports chaining workflows via LaunchPlan fixed_inputs and
    on_completion triggers (Flyte Admin notifications).  The most portable
    approach is to emit a comment-documented LaunchPlan named after the
    upstream flow so operators can wire it up via the Flyte Admin API or
    flytectl.  A code comment explains the recommended wiring.
    """
    wf = _wf_fn(spec.name)
    safe_upstream = re.sub(r"[^a-zA-Z0-9_]", "_", tof.flow_name).lower()
    plan_name = repr(f"{wf}_after_{safe_upstream}")
    return f'''\
# ---------------------------------------------------------------------------
# @trigger_on_finish(flow={tof.flow_name!r}) → Flyte LaunchPlan stub
#
# Register this LaunchPlan and configure it as a success notification target
# on the upstream workflow's LaunchPlan in Flyte Admin.  The upstream
# LaunchPlan name follows the same naming convention: <upstream_wf>_schedule
# or similar.  Use flytectl or the Flyte Admin UI to set the
# on_workflow_completion notification to call this plan.
#
# Programmatic wiring example (flyte-remote SDK):
#   remote.execute(lp_downstream, inputs={{}})
# ---------------------------------------------------------------------------
_trigger_on_finish_{safe_upstream} = LaunchPlan.get_or_create(
    workflow={wf},
    name={plan_name},
)'''


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _task_fn(step_name: str) -> str:
    return f"_step_{step_name}"


def _wf_fn(flow_name: str) -> str:
    """CamelCase flow name → snake_case Python identifier."""
    return re.sub(r"(?<!^)([A-Z])", r"_\1", flow_name).lower()


def _workflow_signature(params: Sequence[ParameterSpec]) -> str:
    """Build the Python function parameter string from flow parameters.

    Always appends ``origin_run_id: str = ''`` so callers can resume a prior
    Metaflow run by passing the original run_id (e.g. via Flyte ``--recover``).
    """
    parts: list[str] = [
        "{}: {} = {!r}".format(p.name, p.type_name, p.default if p.default is not None else "")
        for p in params
    ]
    parts.append("origin_run_id: str = ''")
    return ", ".join(parts)


def _find_switch_step_for_join(
    join_step: StepSpec,
    condition_branches: dict[str, frozenset[str]],
) -> str:
    """Return the split-switch step name whose branches converge at *join_step*.

    Walks the join's in_funcs (direct branch predecessors) and finds which
    split-switch step feeds one of those branches.
    """
    for switch_name, branches in condition_branches.items():
        for branch_name in join_step.in_funcs:
            if branch_name in branches:
                return switch_name
    # Fallback: use the first in_func's in_funcs (walk one level up)
    if join_step.in_funcs:
        return join_step.in_funcs[0]
    return "start"
