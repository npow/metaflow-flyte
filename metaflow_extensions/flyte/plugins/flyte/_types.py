"""Shared dataclasses and enums for the Metaflow-Flyte integration.

These types are the internal data model that ``_graph.py`` produces and
``_codegen.py`` consumes.  They carry no business logic — they are plain,
immutable value objects.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class NodeType(str, Enum):
    """Mirror of Metaflow's internal graph-node types."""

    START = "start"
    LINEAR = "linear"
    SPLIT = "split"
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
    is_foreach_join: bool = False   # join that closes a foreach
    is_split_join: bool = False     # join that closes a static split
    timeout_seconds: int | None = None
    retry_delay_seconds: int | None = None
    env_vars: tuple[tuple[str, str], ...] = ()  # from @environment(vars={...})


@dataclass(frozen=True)
class ParameterSpec:
    """A single Metaflow flow parameter as seen at deploy time."""

    name: str
    default: object
    description: str = ""
    type_name: str = "str"  # Python type name: str, int, float, bool


@dataclass(frozen=True)
class FlowSpec:
    """Fully-analysed description of a Metaflow flow, ready for code generation."""

    name: str
    steps: tuple[StepSpec, ...]
    parameters: tuple[ParameterSpec, ...]
    description: str = ""
    schedule_cron: str | None = None
    tags: tuple[str, ...] = field(default_factory=tuple)
    namespace: str | None = None
    project_name: str | None = None


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
    project_info: dict | None = None   # {"name": ..., "flow_name": ..., "branch": ...}
