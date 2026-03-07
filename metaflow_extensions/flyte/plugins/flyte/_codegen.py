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

from datetime import datetime
from typing import Sequence

from metaflow_extensions.flyte.plugins.flyte._types import (
    FlowSpec,
    FlyteFlowConfig,
    NodeType,
    ParameterSpec,
    StepSpec,
)

# ---------------------------------------------------------------------------
# Tiny code-builder
# ---------------------------------------------------------------------------

_INDENT = "    "


class _CB:
    """Incrementally builds Python source, tracking indentation level."""

    def __init__(self) -> None:
        self._lines: list[str] = []
        self._level: int = 0

    def emit(self, line: str = "") -> "_CB":
        pad = _INDENT * self._level
        self._lines.append(pad + line if line else "")
        return self

    def indent(self) -> "_CB":
        self._level += 1
        return self

    def dedent(self) -> "_CB":
        self._level = max(0, self._level - 1)
        return self

    def build(self) -> str:
        return "\n".join(self._lines)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_flyte_file(spec: FlowSpec, cfg: FlyteFlowConfig) -> str:
    """Return the full Python source of a runnable Flyte workflow file."""
    cb = _CB()
    _emit_header(cb, spec, cfg)
    _emit_helpers(cb, cfg)
    _emit_run_id_task(cb)

    # Pre-compute foreach info for the full graph
    foreach_body: dict[str, str] = {}  # foreach_step -> body_step name
    for s in spec.steps:
        if s.node_type == NodeType.FOREACH and s.out_funcs:
            foreach_body[s.name] = s.out_funcs[0]
    foreach_body_set = set(foreach_body.values())

    # Pre-compute conditional (split-switch) info:
    #   condition_branches: split_switch_step -> frozenset of branch step names
    condition_branches: dict[str, frozenset[str]] = {}
    for s in spec.steps:
        if s.node_type == NodeType.SPLIT_SWITCH:
            condition_branches[s.name] = frozenset(s.out_funcs)
    condition_branch_set: set[str] = set().union(*condition_branches.values()) if condition_branches else set()

    for step in spec.steps:
        cb.emit()
        cb.emit()
        _emit_task(cb, step, spec, cfg, foreach_body, foreach_body_set, condition_branch_set)
        # After a foreach step, emit the dynamic fan-out expander
        if step.node_type == NodeType.FOREACH:
            body_name = foreach_body[step.name]
            body_step = next(s for s in spec.steps if s.name == body_name)
            cb.emit()
            cb.emit()
            _emit_foreach_dynamic(cb, step, body_step)
        # After a split-switch step, emit the dynamic conditional router
        elif step.node_type == NodeType.SPLIT_SWITCH:
            branch_steps = [s for s in spec.steps if s.name in condition_branches[step.name]]
            cb.emit()
            cb.emit()
            _emit_condition_dynamic(cb, step, branch_steps)

    cb.emit()
    cb.emit()
    _emit_workflow(cb, spec, cfg, foreach_body, foreach_body_set, condition_branches, condition_branch_set)

    if spec.schedule_cron:
        cb.emit()
        cb.emit()
        _emit_launch_plan(cb, spec)

    cb.emit()
    cb.emit()
    cb.emit("if __name__ == '__main__':")
    cb.indent()
    cb.emit("%s()" % _wf_fn(spec.name))
    cb.dedent()

    return cb.build()


# ---------------------------------------------------------------------------
# Header + configuration block
# ---------------------------------------------------------------------------


def _emit_header(cb: _CB, spec: FlowSpec, cfg: FlyteFlowConfig) -> None:
    cb.emit("# Generated by metaflow-flyte on %s" % datetime.now().isoformat(timespec="seconds"))
    cb.emit("# Metaflow flow: %s" % spec.name)
    cb.emit("# Do not edit — regenerate with: python %s flyte create <file>" % cfg.flow_file)
    cb.emit()
    cb.emit("from __future__ import annotations")
    cb.emit()
    cb.emit("import json")
    cb.emit("import os")
    cb.emit("import signal")
    cb.emit("import subprocess")
    cb.emit("import sys")
    cb.emit("import tempfile")
    cb.emit("import uuid")
    cb.emit("from typing import List")
    cb.emit()
    cb.emit("import flytekit")
    cb.emit("from flytekit import current_context, dynamic, task, workflow")
    cb.emit("from flytekit.core.launch_plan import LaunchPlan")
    cb.emit("from flytekit.models.core.execution import WorkflowExecutionPhase")
    cb.emit()
    cb.emit("# ---------------------------------------------------------------------------")
    cb.emit("# Compile-time configuration")
    cb.emit("# ---------------------------------------------------------------------------")
    cb.emit("FLOW_FILE: str = %r" % cfg.flow_file)
    # Use project-aware flow name when @project is present
    effective_flow_name = cfg.flow_name if cfg.flow_name else spec.name
    cb.emit("FLOW_NAME: str = %r" % effective_flow_name)
    cb.emit("DATASTORE_TYPE: str = %r" % cfg.datastore_type)
    cb.emit("METADATA_TYPE: str = %r" % cfg.metadata_type)
    cb.emit("USERNAME: str = %r" % cfg.username)
    # Merge flow-level tags (from @tags) with CLI-supplied tags
    all_tags = list(spec.tags) + [t for t in cfg.tags if t not in set(spec.tags)]
    cb.emit("TAGS: List[str] = %r" % all_tags)
    cb.emit("NAMESPACE: str | None = %r" % spec.namespace)
    cb.emit("FLYTE_PROJECT: str = %r" % cfg.flyte_project)
    cb.emit("FLYTE_DOMAIN: str = %r" % cfg.flyte_domain)
    cb.emit("IMAGE: str | None = %r" % cfg.image)
    cb.emit("WITH_DECORATORS: List[str] = %r" % list(cfg.with_decorators))
    cb.emit()
    cb.emit("# Task decorator factory — applies IMAGE and enables Deck when set.")
    cb.emit("_TASK_KWARGS = {'container_image': IMAGE, 'enable_deck': True} if IMAGE else {'enable_deck': True}")
    cb.emit()
    cb.emit()
    cb.emit("def _mf_task(**extra):")
    cb.indent()
    cb.emit('"""Apply IMAGE + extra kwargs to @task."""')
    cb.emit("return task(**{**_TASK_KWARGS, **extra})")
    cb.dedent()


# ---------------------------------------------------------------------------
# Runtime helpers (embedded in generated file)
# ---------------------------------------------------------------------------


def _emit_helpers(cb: _CB, cfg: FlyteFlowConfig) -> None:
    cb.emit()
    cb.emit()
    cb.emit("# ---------------------------------------------------------------------------")
    cb.emit("# Runtime helpers")
    cb.emit("# ---------------------------------------------------------------------------")
    cb.emit()

    # foreach side-car path
    cb.emit("def _foreach_info_path(run_id: str, step_name: str) -> str:")
    cb.indent()
    cb.emit("safe = run_id.replace('/', '_').replace(':', '_')")
    cb.emit("return os.path.join(tempfile.gettempdir(), f'mf_flyte_foreach_{safe}_{step_name}.json')")
    cb.dedent()
    cb.emit()
    cb.emit()
    cb.emit("def _read_foreach_info(path: str) -> int:")
    cb.indent()
    cb.emit("try:")
    cb.indent()
    cb.emit("with open(path) as _f:")
    cb.indent()
    cb.emit("return int(json.load(_f)['num_splits'])")
    cb.dedent()
    cb.dedent()
    cb.emit("except Exception:")
    cb.indent()
    cb.emit("return 0")
    cb.dedent()
    cb.dedent()
    cb.emit()
    cb.emit()
    cb.emit("def _run_cmd(cmd: list[str], extra_env: dict[str, str] | None = None) -> None:")
    cb.indent()
    cb.emit("env = os.environ.copy()")
    cb.emit("env.setdefault('METAFLOW_DATASTORE_SYSROOT_LOCAL', os.path.expanduser('~'))")
    cb.emit("if extra_env:")
    cb.indent()
    cb.emit("env.update(extra_env)")
    cb.dedent()
    cb.emit("proc = subprocess.Popen(cmd, env=env)")
    cb.emit("def _forward(sig, _frame):")
    cb.indent()
    cb.emit("proc.send_signal(sig)")
    cb.dedent()
    cb.emit("_prev_term = signal.signal(signal.SIGTERM, _forward)")
    cb.emit("_prev_int  = signal.signal(signal.SIGINT,  _forward)")
    cb.emit("try:")
    cb.indent()
    cb.emit("proc.wait()")
    cb.dedent()
    cb.emit("finally:")
    cb.indent()
    cb.emit("signal.signal(signal.SIGTERM, _prev_term)")
    cb.emit("signal.signal(signal.SIGINT,  _prev_int)")
    cb.dedent()
    cb.emit("if proc.returncode != 0:")
    cb.indent()
    cb.emit("raise subprocess.CalledProcessError(proc.returncode, cmd)")
    cb.dedent()
    cb.dedent()
    cb.emit()
    cb.emit()
    cb.emit("def _mf_artifact_names(run_id: str, step_name: str, task_id: str) -> list[str]:")
    cb.indent()
    cb.emit('"""Return user-defined artifact names (no values loaded)."""')
    cb.emit("try:")
    cb.indent()
    cb.emit("from metaflow.datastore import FlowDataStore")
    cb.emit("from metaflow.plugins import DATASTORES")
    cb.emit("_impl = next(d for d in DATASTORES if d.TYPE == DATASTORE_TYPE)")
    cb.emit("_root = _impl.get_datastore_root_from_config(lambda *a: None)")
    cb.emit("_fds = FlowDataStore(FLOW_NAME, None, storage_impl=_impl, ds_root=_root)")
    cb.emit("_tds = _fds.get_task_datastore(run_id, step_name, task_id, attempt=0, mode='r')")
    cb.emit("_SKIP = {'name', 'input'}")
    cb.emit("return [n for n in _tds if not n.startswith('_') and n not in _SKIP]")
    cb.dedent()
    cb.emit("except Exception:")
    cb.indent()
    cb.emit("return []")
    cb.dedent()
    cb.dedent()
    cb.emit()
    cb.emit()
    cb.emit("def _step_cmd(")
    cb.indent()
    cb.emit("step_name: str,")
    cb.emit("run_id: str,")
    cb.emit("task_id: str,")
    cb.emit("input_paths: str,")
    cb.emit("retry_count: int = 0,")
    cb.emit("max_user_code_retries: int = 0,")
    cb.emit("split_index: int | None = None,")
    cb.dedent()
    cb.emit(") -> list[str]:")
    cb.indent()
    cb.emit("cmd: list[str] = [")
    cb.indent()
    cb.emit("sys.executable, FLOW_FILE,")
    cb.emit('"--datastore", DATASTORE_TYPE,')
    cb.emit('"--metadata", METADATA_TYPE,')
    cb.emit('"--no-pylint",')
    cb.emit('"--with=flyte_internal",')
    cb.emit('"step", step_name,')
    cb.emit('"--run-id", run_id,')
    cb.emit('"--task-id", task_id,')
    cb.emit('"--retry-count", str(retry_count),')
    cb.emit('"--max-user-code-retries", str(max_user_code_retries),')
    cb.emit('"--input-paths", input_paths,')
    cb.dedent()
    cb.emit("]")
    cb.emit("for _tag in TAGS:")
    cb.indent()
    cb.emit('cmd += ["--tag", _tag]')
    cb.dedent()
    cb.emit("for _deco in WITH_DECORATORS:")
    cb.indent()
    cb.emit('cmd += [f"--with={_deco}"]')
    cb.dedent()
    cb.emit("if NAMESPACE:")
    cb.indent()
    cb.emit('cmd += ["--namespace", NAMESPACE]')
    cb.dedent()
    cb.emit("if split_index is not None:")
    cb.indent()
    cb.emit('cmd += ["--split-index", str(split_index)]')
    cb.dedent()
    cb.emit("return cmd")
    cb.dedent()
    cb.emit()
    cb.emit()
    cb.emit("def _flyte_env() -> dict[str, str]:")
    cb.indent()
    cb.emit('"""Capture Flyte execution context for the flyte_internal decorator."""')
    cb.emit("env: dict[str, str] = {}")
    cb.emit("try:")
    cb.indent()
    cb.emit("ctx = current_context()")
    cb.emit("env['METAFLOW_FLYTE_EXECUTION_ID'] = str(ctx.execution_id.name)")
    cb.emit("env['METAFLOW_FLYTE_EXECUTION_PROJECT'] = str(ctx.execution_id.project)")
    cb.emit("env['METAFLOW_FLYTE_EXECUTION_DOMAIN'] = str(ctx.execution_id.domain)")
    cb.dedent()
    cb.emit("except Exception:")
    cb.indent()
    cb.emit("pass")
    cb.dedent()
    cb.emit("return env")
    cb.dedent()
    cb.emit()
    cb.emit()
    cb.emit("def _read_condition_branch(run_id: str, step_name: str, task_id: str) -> str:")
    cb.indent()
    cb.emit('"""Read the _transition artifact to determine which branch was taken."""')
    cb.emit("try:")
    cb.indent()
    cb.emit("from metaflow.datastore import FlowDataStore")
    cb.emit("from metaflow.plugins import DATASTORES")
    cb.emit("_impl = next(d for d in DATASTORES if d.TYPE == DATASTORE_TYPE)")
    cb.emit("_root = _impl.get_datastore_root_from_config(lambda *a: None)")
    cb.emit("_fds = FlowDataStore(FLOW_NAME, None, storage_impl=_impl, ds_root=_root)")
    cb.emit("_tds = _fds.get_task_datastore(run_id, step_name, task_id, attempt=0, mode='r')")
    cb.emit("_transition = _tds['_transition']")
    cb.emit("# _transition is a list of step names; return the first one as the branch taken")
    cb.emit("if isinstance(_transition, (list, tuple)) and _transition:")
    cb.indent()
    cb.emit("return str(_transition[0])")
    cb.dedent()
    cb.emit("return str(_transition)")
    cb.dedent()
    cb.emit("except Exception:")
    cb.indent()
    cb.emit("return ''")
    cb.dedent()
    cb.dedent()
    cb.emit()
    cb.emit()
    cb.emit("def _emit_deck(run_id: str, step_name: str, task_id: str) -> None:")
    cb.indent()
    cb.emit('"""Render Metaflow artifact retrieval snippets in the Flyte UI Deck."""')
    cb.emit("try:")
    cb.indent()
    cb.emit("artifact_names = _mf_artifact_names(run_id, step_name, task_id)")
    cb.emit("pathspec = f'{FLOW_NAME}/{run_id}/{step_name}/{task_id}'")
    cb.emit("html = f'<h3>Metaflow Artifacts &mdash; <code>{pathspec}</code></h3>'")
    cb.emit("if artifact_names:")
    cb.indent()
    cb.emit("html += '<pre style=\"background:#f5f5f5;padding:12px;border-radius:4px\">'")
    cb.emit("html += 'from metaflow import Task\\n'")
    cb.emit("html += f\"task = Task('{pathspec}')\\n\\n\"")
    cb.emit("for _n in artifact_names:")
    cb.indent()
    cb.emit("html += f'# {_n}\\n'")
    cb.emit("html += f'task.data.{_n}\\n\\n'")
    cb.dedent()
    cb.emit("html += '</pre>'")
    cb.dedent()
    cb.emit("else:")
    cb.indent()
    cb.emit("html += '<p><em>No user artifacts produced.</em></p>'")
    cb.dedent()
    cb.emit("flytekit.Deck('metaflow', html)")
    cb.dedent()
    cb.emit("except Exception:")
    cb.indent()
    cb.emit("pass")
    cb.dedent()
    cb.dedent()


# ---------------------------------------------------------------------------
# Run-ID generation task
# ---------------------------------------------------------------------------


def _emit_run_id_task(cb: _CB) -> None:
    cb.emit()
    cb.emit()
    cb.emit("# ---------------------------------------------------------------------------")
    cb.emit("# Run-ID generation — derives a stable Metaflow run_id from the Flyte")
    cb.emit("# execution ID so every step in the same execution shares a run.")
    cb.emit("# ---------------------------------------------------------------------------")
    cb.emit()
    cb.emit()
    cb.emit("@_mf_task()")
    cb.emit("def _mf_generate_run_id(origin_run_id: str = '') -> str:")
    cb.indent()
    cb.emit("# If a prior run_id is supplied (e.g. via Flyte --recover), reuse it so")
    cb.emit("# Metaflow treats this execution as a resume of the original run.")
    cb.emit("if origin_run_id:")
    cb.indent()
    cb.emit("return origin_run_id")
    cb.dedent()
    cb.emit("try:")
    cb.indent()
    cb.emit("ctx = current_context()")
    cb.emit("name = ctx.execution_id.name")
    cb.emit("# Check for a pre-seeded run ID (set by local trigger for deterministic pathspec).")
    cb.emit("seeded = os.environ.get('METAFLOW_FLYTE_LOCAL_RUN_ID')")
    cb.emit("if seeded:")
    cb.indent()
    cb.emit("return seeded")
    cb.dedent()
    cb.emit("# 'local' is the placeholder used by pyflyte run without --remote;")
    cb.emit("# generate a unique id so each local run gets its own Metaflow run.")
    cb.emit("if not name or name == 'local':")
    cb.indent()
    cb.emit("return 'flyte-local-' + uuid.uuid4().hex[:12]")
    cb.dedent()
    cb.emit("return 'flyte-' + name")
    cb.dedent()
    cb.emit("except Exception:")
    cb.indent()
    cb.emit("return 'flyte-' + uuid.uuid4().hex[:16]")
    cb.dedent()
    cb.dedent()


# ---------------------------------------------------------------------------
# Named-tuple for foreach results
# ---------------------------------------------------------------------------


def _emit_foreach_named_tuple(cb: _CB, step: StepSpec) -> None:
    nt_name = _foreach_result_type(step.name)
    cb.emit("%s = NamedTuple(%r, task_id=str, num_splits=int)" % (nt_name, nt_name))


def _foreach_result_type(step_name: str) -> str:
    return "_%s_Result" % step_name.title().replace("_", "")


# ---------------------------------------------------------------------------
# Per-step @task functions
# ---------------------------------------------------------------------------


def _task_decorator(step: StepSpec) -> str:
    """Build the ``@_mf_task(...)`` decorator line for a step.

    Includes retry count and, when set, a ``timeout=timedelta(...)`` kwarg.
    """
    parts = ["retries=%d" % step.max_user_code_retries]
    if step.timeout_seconds is not None:
        parts.append("timeout=timedelta(seconds=%d)" % step.timeout_seconds)
    return "@_mf_task(%s)" % ", ".join(parts)


def _emit_task(
    cb: _CB,
    step: StepSpec,
    spec: FlowSpec,
    cfg: FlyteFlowConfig,
    foreach_body: dict[str, str],
    foreach_body_set: set[str],
    condition_branch_set: set[str],
) -> None:
    """Emit the ``@_mf_task(...)`` function for *step*.

    Function signature and body vary by step role:

    - **start**: accepts flow parameters; runs ``metaflow init`` before the step.
    - **split-switch**: runs the step; returns JSON ``{task_id, branch_taken}``
      for the downstream ``@dynamic`` router.
    - **foreach**: runs the step; returns JSON ``{task_id, num_splits}``
      for the downstream ``@dynamic`` fan-out expander.
    - **foreach body**: accepts ``split_index`` for parallel execution.
    - **foreach join**: accepts ``_body_task_ids: List[str]`` from the expander.
    - **split join**: accepts one ``_in_<step>: str`` per upstream branch.
    - **condition join**: accepts ``_branch_result_json: str`` from the router.
    - **linear / end / branch**: accepts ``prev_task_id: str``.
    """
    is_start = step.name == "start"
    is_foreach_body = step.name in foreach_body_set
    is_condition_branch = step.name in condition_branch_set

    cb.emit(_task_decorator(step))

    # --- function signature ---
    if is_start:
        # start step: accepts flow parameters individually
        param_args = "".join(
            ", _param_%s: %s = %r" % (p.name, p.type_name, p.default)
            for p in spec.parameters
        )
        cb.emit("def %s(run_id: str%s) -> str:" % (_task_fn(step.name), param_args))
    elif step.is_foreach_join:
        cb.emit("def %s(run_id: str, _body_task_ids: List[str]) -> str:" % _task_fn(step.name))
    elif step.is_split_join:
        in_args = "".join(", _in_%s: str" % p for p in step.in_funcs)
        cb.emit("def %s(run_id: str%s) -> str:" % (_task_fn(step.name), in_args))
    elif step.is_condition_join:
        # Receives JSON {"branch_step": ..., "task_id": ...} from the dynamic router
        cb.emit("def %s(run_id: str, _branch_result_json: str) -> str:" % _task_fn(step.name))
    elif step.node_type == NodeType.SPLIT_SWITCH:
        # The split-switch step returns JSON carrying the task_id and which branch was taken
        cb.emit("def %s(run_id: str, prev_task_id: str) -> str:" % _task_fn(step.name))
    elif step.node_type == NodeType.FOREACH:
        cb.emit("def %s(run_id: str, prev_task_id: str) -> str:" % _task_fn(step.name))
    elif is_foreach_body:
        cb.emit("def %s(run_id: str, prev_task_id: str, split_index: int = 0) -> str:" % _task_fn(step.name))
    elif is_condition_branch:
        # A conditional branch task: receives the split-switch task_id as prev
        cb.emit("def %s(run_id: str, prev_task_id: str) -> str:" % _task_fn(step.name))
    else:
        # linear, split, end
        cb.emit("def %s(run_id: str, prev_task_id: str) -> str:" % _task_fn(step.name))

    cb.indent()

    cb.emit("task_id: str = uuid.uuid4().hex[:16]")

    # --- build input_paths ---
    if is_start:
        _emit_start_init(cb, spec)
        cb.emit("input_paths: str = f\"{run_id}/_parameters/{param_task_id}\"")
    elif step.is_foreach_join:
        body_name = _foreach_body_for_join(spec, step)
        cb.emit(
            "input_paths: str = \",\".join("
            "f\"{run_id}/%s/{tid}\" for tid in _body_task_ids)" % body_name
        )
    elif step.is_split_join:
        parts = ", ".join(
            'f"{run_id}/%s/{_in_%s}"' % (p, p) for p in step.in_funcs
        )
        cb.emit("input_paths: str = \",\".join([%s])" % parts)
    elif step.is_condition_join:
        # Unpack the router JSON to get the branch step name and its task_id
        cb.emit("_branch_info = json.loads(_branch_result_json)")
        cb.emit("_branch_step_name: str = _branch_info['branch_step']")
        cb.emit("_branch_tid: str = _branch_info['task_id']")
        cb.emit("input_paths: str = f\"{run_id}/{_branch_step_name}/{_branch_tid}\"")
        # Override task_id var reference used in _step_cmd below

    elif step.node_type == NodeType.SPLIT_SWITCH:
        parent = step.in_funcs[0] if step.in_funcs else "start"
        cb.emit("input_paths: str = f\"{run_id}/%s/{prev_task_id}\"" % parent)
    elif step.node_type == NodeType.FOREACH:
        parent = step.in_funcs[0] if step.in_funcs else "start"
        cb.emit("input_paths: str = f\"{run_id}/%s/{prev_task_id}\"" % parent)
    elif is_foreach_body:
        parent = step.in_funcs[0] if step.in_funcs else "start"
        cb.emit("input_paths: str = f\"{run_id}/%s/{prev_task_id}\"" % parent)
    else:
        parent = step.in_funcs[0] if step.in_funcs else "start"
        cb.emit("input_paths: str = f\"{run_id}/%s/{prev_task_id}\"" % parent)

    # --- foreach side-car setup ---
    if step.node_type == NodeType.FOREACH:
        cb.emit("foreach_path: str = _foreach_info_path(run_id, %r)" % step.name)
        cb.emit("_extra_env: dict[str, str] = _flyte_env()")
        cb.emit("_extra_env['METAFLOW_FLYTE_FOREACH_INFO_PATH'] = foreach_path")
    else:
        cb.emit("_extra_env: dict[str, str] = _flyte_env()")

    # --- @environment vars ---
    if step.env_vars:
        cb.emit("_extra_env.update(%r)" % dict(step.env_vars))

    # --- run the step ---
    cb.emit("_run_cmd(_step_cmd(")
    cb.indent()
    cb.emit("%r, run_id, task_id, input_paths," % step.name)
    cb.emit("max_user_code_retries=%d," % step.max_user_code_retries)
    if is_foreach_body:
        cb.emit("split_index=split_index,")
    cb.dedent()
    cb.emit("), extra_env=_extra_env)")

    # --- Flyte Deck: Metaflow artifact snippets ---
    cb.emit("_emit_deck(run_id, %r, task_id)" % step.name)

    # --- return ---
    if step.node_type == NodeType.FOREACH:
        cb.emit("_num_splits: int = _read_foreach_info(foreach_path)")
        cb.emit("return json.dumps({'task_id': task_id, 'num_splits': _num_splits})")
    elif step.node_type == NodeType.SPLIT_SWITCH:
        # Return JSON so the dynamic router knows which branch to execute
        cb.emit("_branch_taken: str = _read_condition_branch(run_id, %r, task_id)" % step.name)
        cb.emit("return json.dumps({'task_id': task_id, 'branch_taken': _branch_taken})")
    elif is_condition_branch:
        # Return JSON with branch identity so the join step can build input_paths.
        # The router returns this Promise[str] directly (avoids Promise serialization).
        cb.emit("return json.dumps({'branch_step': %r, 'task_id': task_id})" % step.name)
    else:
        cb.emit("return task_id")

    cb.dedent()


def _emit_start_init(cb: _CB, spec: FlowSpec) -> None:
    cb.emit("param_task_id: str = uuid.uuid4().hex[:16]")
    cb.emit("_init_cmd: list[str] = [")
    cb.indent()
    cb.emit("sys.executable, FLOW_FILE,")
    cb.emit('"--datastore", DATASTORE_TYPE,')
    cb.emit('"--metadata", METADATA_TYPE,')
    cb.emit('"--no-pylint",')
    cb.emit('"init",')
    cb.emit('"--run-id", run_id,')
    cb.emit('"--task-id", param_task_id,')
    cb.dedent()
    cb.emit("]")
    cb.emit("for _tag in TAGS:")
    cb.indent()
    cb.emit('_init_cmd += ["--tag", _tag]')
    cb.dedent()
    # Pass each flow parameter as a CLI argument (--<name> <value>).
    # Metaflow's 'init' command reads parameters via @add_custom_parameters
    # which maps them to click options, not env vars.
    if spec.parameters:
        for p in spec.parameters:
            cb.emit('_init_cmd += ["--%s", str(_param_%s)]' % (p.name, p.name))
    cb.emit("_run_cmd(_init_cmd)")


# ---------------------------------------------------------------------------
# Foreach dynamic expander
# ---------------------------------------------------------------------------


def _emit_foreach_dynamic(cb: _CB, foreach_step: StepSpec, body_step: StepSpec) -> None:
    """Emit a ``@dynamic`` function that fans out the foreach body tasks."""
    fn_name = "_foreach_%s_dynamic" % foreach_step.name
    cb.emit("@dynamic(**_TASK_KWARGS)")
    cb.emit("def %s(run_id: str, foreach_result_json: str) -> List[str]:" % fn_name)
    cb.indent()
    cb.emit('"""Fan out %r body tasks in parallel."""' % foreach_step.name)
    cb.emit("_info = json.loads(foreach_result_json)")
    cb.emit("_foreach_task_id: str = _info['task_id']")
    cb.emit("_num_splits: int = int(_info['num_splits'])")
    cb.emit("_task_ids: List[str] = []")
    cb.emit("for _i in range(_num_splits):")
    cb.indent()
    cb.emit(
        "_tid = %s(run_id=run_id, prev_task_id=_foreach_task_id, split_index=_i)"
        % _task_fn(body_step.name)
    )
    cb.emit("_task_ids.append(_tid)")
    cb.dedent()
    cb.emit("return _task_ids")
    cb.dedent()


# ---------------------------------------------------------------------------
# Conditional (split-switch) dynamic router
# ---------------------------------------------------------------------------


def _emit_condition_dynamic(
    cb: _CB,
    switch_step: StepSpec,
    branch_steps: list[StepSpec],
) -> None:
    """Emit a ``@dynamic`` function that routes to exactly one branch task.

    The split-switch task returns JSON ``{"task_id": ..., "branch_taken": ...}``.
    This dynamic function inspects ``branch_taken`` at runtime and calls only
    the matching branch task, returning JSON ``{"branch_step": ..., "task_id": ...}``
    so the join step receives both the branch step name and its task_id.
    """
    fn_name = "_cond_%s_router" % switch_step.name
    cb.emit("@dynamic(**_TASK_KWARGS)")
    cb.emit("def %s(run_id: str, switch_result_json: str) -> str:" % fn_name)
    cb.indent()
    cb.emit('"""Route to the branch task selected at runtime for step %r."""' % switch_step.name)
    cb.emit("_info = json.loads(switch_result_json)")
    cb.emit("_switch_task_id: str = _info['task_id']")
    cb.emit("_branch_taken: str = _info['branch_taken']")

    branch_step_names = [bs.name for bs in branch_steps]
    first = True
    for branch_name in branch_step_names:
        keyword = "if" if first else "elif"
        cb.emit("%s _branch_taken == %r:" % (keyword, branch_name))
        cb.indent()
        # Return the Promise[str] directly — branch tasks now return the full JSON
        # {'branch_step': ..., 'task_id': ...}, so the join step can parse it.
        # We cannot json.dumps() a Promise inside a @dynamic function.
        cb.emit(
            "return %s(run_id=run_id, prev_task_id=_switch_task_id)"
            % _task_fn(branch_name)
        )
        cb.dedent()
        first = False

    # Fallback: run the first branch if branch resolution fails
    if branch_step_names:
        cb.emit("else:")
        cb.indent()
        cb.emit(
            "return %s(run_id=run_id, prev_task_id=_switch_task_id)"
            % _task_fn(branch_step_names[0])
        )
        cb.dedent()
    else:
        cb.emit("return json.dumps({'branch_step': '', 'task_id': _switch_task_id})")

    cb.dedent()


# ---------------------------------------------------------------------------
# Top-level @workflow function
# ---------------------------------------------------------------------------


def _emit_workflow(
    cb: _CB,
    spec: FlowSpec,
    cfg: FlyteFlowConfig,
    foreach_body: dict[str, str],
    foreach_body_set: set[str],
    condition_branches: dict[str, frozenset[str]],
    condition_branch_set: set[str],
) -> None:
    sig = _workflow_signature(spec.parameters)
    cb.emit("@workflow")
    cb.emit("def %s(%s) -> None:" % (_wf_fn(spec.name), sig))
    cb.indent()
    if spec.description:
        cb.emit("%r" % spec.description)

    # First task: generate (or reuse) the Metaflow run_id
    cb.emit("run_id = _mf_generate_run_id(origin_run_id=origin_run_id)")

    task_id_vars: dict[str, str] = {}  # step_name -> Python variable holding task_id

    for step in spec.steps:
        var = "_tid_%s" % step.name
        is_start = step.name == "start"

        if step.node_type == NodeType.FOREACH:
            # Foreach step: emit task call then dynamic fan-out expander.
            # A foreach step can also be the 'start' step.
            foreach_result_var = "_foreach_result_json_%s" % step.name
            if is_start:
                param_kwargs = "".join(
                    ", _param_%s=%s" % (p.name, p.name) for p in spec.parameters
                )
                cb.emit(
                    "%s = %s(run_id=run_id%s)"
                    % (foreach_result_var, _task_fn(step.name), param_kwargs)
                )
            else:
                parent = step.in_funcs[0]
                cb.emit(
                    "%s = %s(run_id=run_id, prev_task_id=%s)"
                    % (foreach_result_var, _task_fn(step.name), task_id_vars[parent])
                )
            body_name = foreach_body[step.name]
            body_tids_var = "_body_tids_%s" % body_name
            cb.emit(
                "%s = _foreach_%s_dynamic(run_id=run_id, foreach_result_json=%s)"
                % (body_tids_var, step.name, foreach_result_var)
            )
            task_id_vars[step.name] = foreach_result_var
            task_id_vars[body_name] = body_tids_var
            continue

        elif step.name in foreach_body_set:
            # Body step was already registered inside the foreach block above
            continue

        elif step.node_type == NodeType.SPLIT_SWITCH:
            # Conditional branch: run the split-switch task, then the dynamic router
            switch_result_var = "_cond_result_json_%s" % step.name
            if is_start:
                param_kwargs = "".join(
                    ", _param_%s=%s" % (p.name, p.name) for p in spec.parameters
                )
                cb.emit(
                    "%s = %s(run_id=run_id%s)"
                    % (switch_result_var, _task_fn(step.name), param_kwargs)
                )
            else:
                parent = step.in_funcs[0]
                cb.emit(
                    "%s = %s(run_id=run_id, prev_task_id=%s)"
                    % (switch_result_var, _task_fn(step.name), task_id_vars[parent])
                )
            routed_tid_var = "_cond_routed_tid_%s" % step.name
            cb.emit(
                "%s = _cond_%s_router(run_id=run_id, switch_result_json=%s)"
                % (routed_tid_var, step.name, switch_result_var)
            )
            task_id_vars[step.name] = switch_result_var
            # All branch steps share the same routed task_id variable
            for branch_name in condition_branches.get(step.name, set()):
                task_id_vars[branch_name] = routed_tid_var
            continue

        elif step.name in condition_branch_set:
            # Branch steps are handled inside the split-switch block above
            continue

        elif is_start:
            param_kwargs = "".join(
                ", _param_%s=%s" % (p.name, p.name) for p in spec.parameters
            )
            cb.emit("%s = %s(run_id=run_id%s)" % (var, _task_fn(step.name), param_kwargs))

        elif step.is_foreach_join:
            foreach_step_name = step.split_parents[-1]
            body_var = "_body_tids_%s" % foreach_body.get(foreach_step_name, "body")
            cb.emit(
                "%s = %s(run_id=run_id, _body_task_ids=%s)"
                % (var, _task_fn(step.name), body_var)
            )

        elif step.is_split_join:
            in_kwargs = "".join(
                ", _in_%s=%s" % (p, task_id_vars[p]) for p in step.in_funcs
            )
            cb.emit("%s = %s(run_id=run_id%s)" % (var, _task_fn(step.name), in_kwargs))

        elif step.is_condition_join:
            # The join after a conditional: receives the router JSON output
            switch_step_name = _find_switch_step_for_join(spec, step, condition_branches)
            branch_result_var = "_cond_routed_tid_%s" % switch_step_name
            cb.emit(
                "%s = %s(run_id=run_id, _branch_result_json=%s)"
                % (var, _task_fn(step.name), branch_result_var)
            )

        else:
            # linear, split (not foreach/conditional), end
            parent = step.in_funcs[0] if step.in_funcs else "start"
            cb.emit(
                "%s = %s(run_id=run_id, prev_task_id=%s)"
                % (var, _task_fn(step.name), task_id_vars[parent])
            )

        task_id_vars[step.name] = var

    cb.dedent()


# ---------------------------------------------------------------------------
# LaunchPlan for @schedule support
# ---------------------------------------------------------------------------


def _emit_launch_plan(cb: _CB, spec: FlowSpec) -> None:
    cb.emit("# Schedule: register a LaunchPlan that triggers on the given cron.")
    cb.emit("_schedule_launch_plan = LaunchPlan.get_or_create(")
    cb.indent()
    cb.emit("workflow=%s," % _wf_fn(spec.name))
    cb.emit("name=%r," % ("%s_schedule" % _wf_fn(spec.name)))
    cb.emit("schedule=flytekit.CronSchedule(schedule=%r)," % spec.schedule_cron)
    cb.dedent()
    cb.emit(")")


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _task_fn(step_name: str) -> str:
    return "_step_%s" % step_name


def _wf_fn(flow_name: str) -> str:
    """CamelCase flow name → snake_case Python identifier."""
    result: list[str] = []
    for i, ch in enumerate(flow_name):
        if ch.isupper() and i > 0:
            result.append("_")
        result.append(ch.lower())
    return "".join(result)


def _workflow_signature(params: Sequence[ParameterSpec]) -> str:
    """Build the Python function parameter string from flow parameters.

    Always appends ``origin_run_id: str = ''`` so callers can resume a prior
    Metaflow run by passing the original run_id (e.g. via Flyte ``--recover``).
    """
    parts: list[str] = []
    for p in params:
        if isinstance(p.default, str):
            parts.append("%s: str = %r" % (p.name, p.default))
        elif isinstance(p.default, bool):
            parts.append("%s: bool = %r" % (p.name, p.default))
        elif isinstance(p.default, int):
            parts.append("%s: int = %r" % (p.name, p.default))
        elif isinstance(p.default, float):
            parts.append("%s: float = %r" % (p.name, p.default))
        else:
            parts.append("%s: str = %r" % (p.name, str(p.default) if p.default is not None else ""))
    parts.append("origin_run_id: str = ''")
    return ", ".join(parts)


def _foreach_body_for_join(spec: FlowSpec, join_step: StepSpec) -> str:
    """Return the body step name for a foreach join step."""
    foreach_step_name = join_step.split_parents[-1]
    for s in spec.steps:
        if s.name == foreach_step_name and s.out_funcs:
            return s.out_funcs[0]
    return join_step.in_funcs[0]


def _find_switch_step_for_join(
    spec: FlowSpec,
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
