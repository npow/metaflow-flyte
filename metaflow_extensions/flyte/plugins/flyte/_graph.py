"""Graph analysis utilities for the Metaflow-Flyte integration.

``analyze_graph`` is the only public function.  It walks the Metaflow DAG
and returns a ``FlowSpec`` containing an ordered list of ``StepSpec`` objects
that ``_codegen.py`` later turns into Python source.

Supported Metaflow graph shapes
--------------------------------
- linear          straight sequence of steps
- split / join    static parallel branches that all execute
- foreach         dynamic fan-out over an iterable
- split-switch    conditional branching — exactly one branch executes at runtime
                  (Metaflow ``self.next({...}, condition=...)``)
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any

from metaflow.parameters import deploy_time_eval

from metaflow_extensions.flyte.plugins.flyte._types import (
    FlowSpec,
    NodeType,
    ParameterSpec,
    StepSpec,
)
from metaflow_extensions.flyte.plugins.flyte.exception import (
    NotSupportedException,
    FlyteException,
)

if TYPE_CHECKING:
    from metaflow.flowgraph import FlowGraph


def analyze_graph(
    graph: Any,  # metaflow.flowgraph.FlowGraph
    flow: Any,   # metaflow.FlowSpec subclass instance
) -> FlowSpec:
    """Convert a Metaflow ``FlowGraph`` into a ``FlowSpec``."""
    _validate(graph, flow)

    steps = _topological_order(graph)
    parameters = _extract_parameters(flow)
    schedule_cron = _extract_schedule(flow)
    tags_raw = getattr(flow, "_tags", None) or []
    project_name = _extract_project(flow)

    return FlowSpec(
        name=flow.name,
        steps=tuple(steps),
        parameters=tuple(parameters),
        description=(flow.__doc__ or "").strip(),
        schedule_cron=schedule_cron,
        tags=tuple(tags_raw),
        namespace=None,
        project_name=project_name,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _validate(graph: Any, flow: Any) -> None:
    """Raise NotSupportedException for features incompatible with Flyte."""
    import warnings

    for node in graph:
        if node.parallel_foreach:
            raise NotSupportedException(
                "Deploying flows with @parallel to Flyte is not yet supported."
            )
        for deco in node.decorators:
            if deco.name == "batch":
                raise NotSupportedException(
                    "Step *%s* uses @batch which is not supported with Flyte. "
                    "Remove @batch or use --with=batch on the Flyte CLI instead." % node.name
                )
            if deco.name == "slurm":
                raise NotSupportedException(
                    "Step *%s* uses @slurm which is not supported with Flyte." % node.name
                )
            if deco.name == "resources":
                warnings.warn(
                    "Step *%s* uses @resources. Resource requirements are not enforced "
                    "by this integration — configure matching resources on your Flyte "
                    "task/workflow directly." % node.name,
                    UserWarning,
                    stacklevel=2,
                )

    for bad_deco in ("trigger", "trigger_on_finish", "exit_hook"):
        try:
            decos = flow._flow_decorators.get(bad_deco)
        except Exception:
            decos = None
        if decos:
            warnings.warn(
                "@%s is not supported by this integration and will be ignored." % bad_deco,
                UserWarning,
                stacklevel=2,
            )


def _max_user_code_retries(node: Any) -> int:
    max_retries = 0
    for deco in node.decorators:
        user_retries, _ = deco.step_task_retry_count()
        max_retries = max(max_retries, user_retries)
    return max_retries


def _step_retry_delay_seconds(node: Any) -> int | None:
    for deco in node.decorators:
        if deco.name == "retry":
            mins = deco.attributes.get("minutes_between_retries", 0)
            if mins:
                return int(mins) * 60
    return None


def _step_timeout_seconds(node: Any) -> int | None:
    for deco in node.decorators:
        if deco.name == "timeout":
            secs = deco.attributes.get("seconds", 0) or 0
            mins = deco.attributes.get("minutes", 0) or 0
            hours = deco.attributes.get("hours", 0) or 0
            total = int(secs) + int(mins) * 60 + int(hours) * 3600
            if total > 0:
                return total
    return None


def _step_env_vars(node: Any) -> tuple[tuple[str, str], ...]:
    for deco in node.decorators:
        if deco.name == "environment":
            raw = deco.attributes.get("vars") or {}
            return tuple(sorted(raw.items()))
    return ()


def _is_foreach_join(graph: Any, node: Any) -> bool:
    if node.type != "join":
        return False
    if not node.split_parents:
        return False
    return graph[node.split_parents[-1]].type == "foreach"


def _is_split_join(graph: Any, node: Any) -> bool:
    if node.type != "join":
        return False
    if not node.split_parents:
        return False
    return graph[node.split_parents[-1]].type == "split"


def _is_condition_join(graph: Any, node: Any) -> bool:
    """Return True if *node* is the merge point after a split-switch (conditional).

    A condition-join step has multiple in_funcs that all originate from a
    split-switch parent. The step may be type "join" (defined with ``inputs``)
    or "linear" (defined without it) — both are valid.
    """
    if len(node.in_funcs) < 2:
        return False
    return any(
        graph[grandparent].type == "split-switch"
        for parent in node.in_funcs
        for grandparent in graph[parent].in_funcs
    )


def _topological_order(graph: Any) -> list[StepSpec]:
    """BFS from *start* yielding ``StepSpec`` objects in topological order."""
    visited: set[str] = set()
    result: list[StepSpec] = []
    queue: deque[str] = deque(["start"])

    while queue:
        name = queue.popleft()
        if name in visited:
            continue

        node = graph[name]

        # Only process once all predecessors are done.
        if any(p not in visited for p in node.in_funcs):
            queue.append(name)
            continue

        visited.add(name)

        # Map Metaflow graph node type to our NodeType enum.
        # The start step can have type None (plain start), "start", "split", etc.
        raw_type = node.type
        if raw_type is None:
            # Metaflow assigns None to the start node when it has no explicit type
            raw_type = "start"
        try:
            node_type = NodeType(raw_type)
        except ValueError:
            # Unknown type — treat as linear so codegen can still proceed
            node_type = NodeType.LINEAR

        # Collect switch_cases for split-switch steps.
        raw_switch = getattr(node, "switch_cases", None) or {}
        switch_cases = tuple(sorted(raw_switch.items()))

        spec = StepSpec(
            name=node.name,
            node_type=node_type,
            in_funcs=tuple(node.in_funcs),
            out_funcs=tuple(node.out_funcs),
            split_parents=tuple(node.split_parents),
            max_user_code_retries=_max_user_code_retries(node),
            is_foreach_join=_is_foreach_join(graph, node),
            is_split_join=_is_split_join(graph, node),
            is_condition_join=_is_condition_join(graph, node),
            switch_cases=switch_cases,
            timeout_seconds=_step_timeout_seconds(node),
            retry_delay_seconds=_step_retry_delay_seconds(node),
            env_vars=_step_env_vars(node),
        )
        result.append(spec)

        for child in node.out_funcs:
            if child not in visited:
                queue.append(child)

    return result


def _param_kwarg(param: Any, key: str) -> Any:
    """Read a kwarg from a Parameter, handling both stock and override-based subclasses."""
    value = param.kwargs.get(key)
    if value is None:
        value = getattr(param, "_override_kwargs", {}).get(key)
    return value


def _extract_parameters(flow: Any) -> list[ParameterSpec]:
    _type_map = {
        "int": "int",
        "float": "float",
        "bool": "bool",
        "str": "str",
        "NoneType": "str",
    }
    params: list[ParameterSpec] = []
    for _, param in flow._get_parameters():
        raw_default = _param_kwarg(param, "default")
        default = deploy_time_eval(raw_default)
        type_name = _type_map.get(type(default).__name__, "str")
        params.append(
            ParameterSpec(
                name=param.name,
                default=default,
                description=_param_kwarg(param, "help") or "",
                type_name=type_name,
            )
        )
    return params


def _extract_schedule(flow: Any) -> str | None:
    schedules = flow._flow_decorators.get("schedule")
    if not schedules:
        return None
    s = schedules[0]
    if s.attributes.get("cron"):
        return s.attributes["cron"]
    if s.attributes.get("weekly"):
        return "0 0 * * 0"
    if s.attributes.get("hourly"):
        return "0 * * * *"
    if s.attributes.get("daily"):
        return "0 0 * * *"
    return None


def _extract_project(flow: Any) -> str | None:
    try:
        project_decos = flow._flow_decorators.get("project")
    except Exception:
        return None
    if not project_decos:
        return None
    return project_decos[0].attributes.get("name") or None
