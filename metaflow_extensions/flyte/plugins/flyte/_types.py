"""Shared dataclasses and enums for the Metaflow-Flyte integration.

These types are the internal data model that ``_graph.py`` produces and
``_codegen.py`` consumes.  They carry no business logic — they are plain,
immutable value objects.

``NodeType`` mirrors Metaflow's internal graph-node type strings, including
``SPLIT_SWITCH`` for conditional branches created with
``self.next({...}, condition=...)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class NodeType(str, Enum):
    """Mirror of Metaflow's internal graph-node types."""

    START = "start"
    LINEAR = "linear"
    SPLIT = "split"
    SPLIT_SWITCH = "split-switch"  # conditional branching via self.next({...}, condition=...)
    JOIN = "join"
    FOREACH = "foreach"
    END = "end"


@dataclass(frozen=True)
class StepSpec:
    """Compiled specification for a single Metaflow step."""

    name: str
    node_type: NodeType
    in_funcs: tuple[str, ...]       # upstream step names
    out_funcs: tuple[str, ...]      # downstream step names
    split_parents: tuple[str, ...]  # ancestors that opened the current fork
    max_user_code_retries: int = 0
    is_foreach_join: bool = False    # join that closes a foreach
    is_split_join: bool = False      # join that closes a static split
    is_condition_join: bool = False  # join that closes a split-switch (conditional)
    # For split-switch steps: maps case label -> branch step name
    switch_cases: tuple[tuple[str, str], ...] = ()
    timeout_seconds: int | None = None
    env_vars: tuple[tuple[str, str], ...] = ()  # from @environment(vars={...})
    resource_cpu: int | None = None    # from @resources(cpu=N)
    resource_gpu: int | None = None    # from @resources(gpu=N)
    resource_memory: int | None = None  # from @resources(memory=N) in MB


@dataclass(frozen=True)
class ParameterSpec:
    """A single Metaflow flow parameter as seen at deploy time."""

    name: str
    default: object
    description: str = ""
    type_name: str = "str"  # Python type name: str, int, float, bool


@dataclass(frozen=True)
class TriggerSpec:
    """A custom-event trigger from @trigger(event=...)."""

    event_name: str
    parameter_map: tuple[tuple[str, str], ...] = ()  # (flow_param, event_field) pairs


@dataclass(frozen=True)
class TriggerOnFinishSpec:
    """A flow-completion trigger from @trigger_on_finish(flow=...)."""

    flow_name: str  # upstream Metaflow flow name whose completion triggers this flow


@dataclass(frozen=True)
class FlowSpec:
    """Fully-analysed description of a Metaflow flow, ready for code generation."""

    name: str
    steps: tuple[StepSpec, ...]
    parameters: tuple[ParameterSpec, ...]
    description: str = ""
    schedule_cron: str | None = None
    tags: tuple[str, ...] = ()
    namespace: str | None = None
    project_name: str | None = None
    triggers: tuple[TriggerSpec, ...] = ()           # from @trigger
    trigger_on_finishes: tuple[TriggerOnFinishSpec, ...] = ()  # from @trigger_on_finish


@dataclass(frozen=True)
class FlyteFlowConfig:
    """User-supplied options for the generated Flyte workflow."""

    flow_file: str                     # absolute path to the Metaflow .py file
    datastore_type: str = "local"
    metadata_type: str = "local"
    username: str = ""
    flyte_project: str = "flytesnacks"
    flyte_domain: str = "development"
    image: str | None = None           # Docker image URI (None = local Python)
    with_decorators: tuple[str, ...] = ()
    workflow_timeout: int | None = None
    max_parallelism: int | None = None  # for foreach map tasks
    # CLI-supplied tags (merged with flow @tags at codegen time)
    tags: tuple[str, ...] = ()
    # Project-aware flow name (e.g. "myproject.prod.MyFlow") or plain name
    flow_name: str = ""
    project_info: dict[str, str] | None = None  # {"name": ..., "flow_name": ..., "branch": ...}
    flow_config_value: str | None = None  # JSON-serialized METAFLOW_FLOW_CONFIG_VALUE
    environment_type: str = "local"  # value of --environment top-level arg (e.g. "conda")
