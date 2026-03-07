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

import warnings
from collections import deque
from typing import Any

from metaflow.parameters import deploy_time_eval

from metaflow_extensions.flyte.plugins.flyte._types import (
    FlowSpec,
    NodeType,
    ParameterSpec,
    StepSpec,
    TriggerOnFinishSpec,
    TriggerSpec,
)
from metaflow_extensions.flyte.plugins.flyte.exception import NotSupportedException


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
    triggers = _extract_triggers(flow)
    trigger_on_finishes = _extract_trigger_on_finishes(flow)

    return FlowSpec(
        name=flow.name,
        steps=tuple(steps),
        parameters=tuple(parameters),
        description=(flow.__doc__ or "").strip(),
        schedule_cron=schedule_cron,
        tags=tuple(tags_raw),
        namespace=None,
        project_name=project_name,
        triggers=tuple(triggers),
        trigger_on_finishes=tuple(trigger_on_finishes),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _validate(graph: Any, flow: Any) -> None:
    """Raise NotSupportedException for features incompatible with Flyte."""
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
            # @resources is translated to native Flyte Resources — no warning needed.

    # @trigger and @trigger_on_finish are extracted and wired as Flyte LaunchPlan
    # triggers during code generation — no warning needed.

    try:
        exit_hook_decos = flow._flow_decorators.get("exit_hook")
    except Exception:
        exit_hook_decos = None
    if exit_hook_decos:
        warnings.warn(
            "@exit_hook is not supported by this integration and will be ignored.",
            UserWarning,
            stacklevel=2,
        )


def _max_user_code_retries(node: Any) -> int:
    max_retries = 0
    for deco in node.decorators:
        user_retries, _ = deco.step_task_retry_count()
        max_retries = max(max_retries, user_retries)
    return max_retries


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


def _step_resources(node: Any) -> tuple[int | None, int | None, int | None]:
    """Return (cpu, gpu, memory_mb) from @resources, or (None, None, None)."""
    for deco in node.decorators:
        if deco.name == "resources":
            cpu = deco.attributes.get("cpu")
            gpu = deco.attributes.get("gpu")
            memory = deco.attributes.get("memory")
            return (
                int(cpu) if cpu is not None else None,
                int(gpu) if gpu is not None else None,
                int(memory) if memory is not None else None,
            )
    return (None, None, None)


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

        resource_cpu, resource_gpu, resource_memory = _step_resources(node)
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
            env_vars=_step_env_vars(node),
            resource_cpu=resource_cpu,
            resource_gpu=resource_gpu,
            resource_memory=resource_memory,
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


_PARAM_TYPE_MAP = {
    "int": "int",
    "float": "float",
    "bool": "bool",
    "str": "str",
    "NoneType": "str",
}


def _extract_parameters(flow: Any) -> list[ParameterSpec]:
    params: list[ParameterSpec] = []
    for _, param in flow._get_parameters():
        raw_default = _param_kwarg(param, "default")
        default = deploy_time_eval(raw_default)
        type_name = _PARAM_TYPE_MAP.get(type(default).__name__, "str")
        params.append(
            ParameterSpec(
                name=param.name,
                default=default,
                description=_param_kwarg(param, "help") or "",
                type_name=type_name,
            )
        )
    return params


_SCHEDULE_CRONS = {
    "weekly": "0 0 * * 0",
    "hourly": "0 * * * *",
    "daily":  "0 0 * * *",
}


def _extract_schedule(flow: Any) -> str | None:
    schedules = flow._flow_decorators.get("schedule")
    if not schedules:
        return None
    attrs = schedules[0].attributes
    if attrs.get("cron"):
        return attrs["cron"]
    for key, cron in _SCHEDULE_CRONS.items():
        if attrs.get(key):
            return cron
    return None


def _extract_project(flow: Any) -> str | None:
    try:
        project_decos = flow._flow_decorators.get("project")
    except Exception:
        return None
    if not project_decos:
        return None
    return project_decos[0].attributes.get("name") or None


def _extract_triggers(flow: Any) -> list[TriggerSpec]:
    """Return TriggerSpec entries from @trigger(event=...) or @trigger(events=[...])."""
    try:
        decos = flow._flow_decorators.get("trigger")
    except Exception:
        return []
    if not decos:
        return []

    raw_triggers = getattr(decos[0], "triggers", None) or []
    result: list[TriggerSpec] = []
    for t in raw_triggers:
        if not isinstance(t, dict):
            continue
        name = t.get("name")
        if not name or not isinstance(name, str):
            warnings.warn(
                "@trigger entry has a non-string or deploy-time event name %r — "
                "skipping this trigger. Evaluate the event name before deploying." % (name,),
                UserWarning,
                stacklevel=2,
            )
            continue
        raw_params = t.get("parameters") or {}
        param_map = tuple(sorted(raw_params.items())) if isinstance(raw_params, dict) else ()
        result.append(TriggerSpec(event_name=name, parameter_map=param_map))
    return result


def _extract_trigger_on_finishes(flow: Any) -> list[TriggerOnFinishSpec]:
    """Return TriggerOnFinishSpec entries from @trigger_on_finish(flow=...) or flows=[...]."""
    try:
        decos = flow._flow_decorators.get("trigger_on_finish")
    except Exception:
        return []
    if not decos:
        return []

    raw_triggers = getattr(decos[0], "triggers", None) or []
    result: list[TriggerOnFinishSpec] = []
    for t in raw_triggers:
        if not isinstance(t, dict):
            continue
        flow_name = t.get("flow") or t.get("fq_name")
        if not flow_name or not isinstance(flow_name, str):
            warnings.warn(
                "@trigger_on_finish entry has a non-string or missing flow name %r — "
                "skipping this trigger." % (flow_name,),
                UserWarning,
                stacklevel=2,
            )
            continue
        result.append(TriggerOnFinishSpec(flow_name=flow_name))
    return result
