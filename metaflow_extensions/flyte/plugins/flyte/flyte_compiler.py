"""FlyteCompiler: orchestrates graph analysis and code generation."""

from __future__ import annotations

from typing import Any

from metaflow_extensions.flyte.plugins.flyte._codegen import generate_flyte_file
from metaflow_extensions.flyte.plugins.flyte._graph import analyze_graph
from metaflow_extensions.flyte.plugins.flyte._types import FlyteFlowConfig


class FlyteCompiler:
    """Compile a Metaflow flow graph into a Flyte workflow Python file."""

    def __init__(
        self,
        name: str,
        graph: Any,
        flow: Any,
        metadata: Any,
        flow_datastore: Any,
        environment: Any,
        event_logger: Any,
        monitor: Any,
        tags: list[str],
        namespace: str | None,
        username: str,
        description: str | None,
        flow_file: str,
        with_decorators: list[str],
        flyte_project: str,
        flyte_domain: str,
        image: str | None,
        workflow_timeout: int | None,
        max_parallelism: int | None,
        branch: str | None = None,
        production: bool = False,
    ) -> None:
        self._name = name
        self._graph = graph
        self._flow = flow
        self._username = username
        self._branch = branch
        self._production = production

        # Compute @project info and project-aware flow name
        project_info = self._get_project()
        flow_name = project_info["flow_name"] if project_info else name

        # Merge user CLI tags with project-specific tags
        all_tags = list(tags)
        if project_info:
            all_tags += [
                "project:{}".format(project_info["name"]),
                "project_branch:{}".format(project_info["branch"]),
            ]

        self._cfg = FlyteFlowConfig(
            flow_file=flow_file,
            datastore_type=getattr(flow_datastore, "TYPE", "local"),
            metadata_type="local",  # always use local metadata in containers
            username=username or "",
            flyte_project=flyte_project,
            flyte_domain=flyte_domain,
            image=image,
            with_decorators=tuple(with_decorators),
            workflow_timeout=workflow_timeout,
            max_parallelism=max_parallelism,
            tags=tuple(all_tags),
            flow_name=flow_name,
            project_info=project_info,
            flow_config_value=self._extract_flow_config_value(flow),
        )

    def compile(self) -> str:
        """Return the full source of the generated Flyte workflow Python file."""
        spec = analyze_graph(self._graph, self._flow)
        return generate_flyte_file(spec, self._cfg)

    @staticmethod
    def _extract_flow_config_value(flow: Any) -> str | None:
        """Extract compile-time config values from the flow as a JSON string.

        Mirrors what the Airflow and Prefect deployers do: reads
        ``flow._flow_state[FlowStateItems.CONFIGS]`` at compile time and
        returns a JSON string so it can be embedded as
        ``METAFLOW_FLOW_CONFIG_VALUE`` in the generated file, propagating
        config_expr / @project config to every step subprocess at runtime.
        """
        import json

        try:
            from metaflow.flowspec import FlowStateItems

            flow_configs = flow._flow_state[FlowStateItems.CONFIGS]
            config_env = {
                name: value
                for name, (value, _is_plain) in flow_configs.items()
                if value is not None
            }
            if config_env:
                return json.dumps(config_env)
        except Exception:
            pass
        return None

    def _get_project(self) -> dict[str, str] | None:
        """Extract @project decorator info and compute the project-aware flow name."""
        try:
            from metaflow.plugins.project_decorator import format_name

            flow_decos = getattr(self._flow, "_flow_decorators", {})
            project_list = flow_decos.get("project", [])
            if not project_list:
                return None
            d = project_list[0]
            project_name = d.attributes.get("name")
            if not project_name:
                return None
            project_flow_name, branch_name = format_name(
                self._name,
                project_name,
                self._production,
                self._branch,
                self._username or "",
            )
            return {
                "name": project_name,
                "flow_name": project_flow_name,
                "branch": branch_name,
                # raw branch as passed by the user via --branch (without the
                # "test."/"user." prefix added by format_name).  Stored so that
                # _step_cmd can pass --branch <raw> to Metaflow subprocesses and
                # have @project resolve the same branch name.
                "branch_raw": self._branch or "",
            }
        except Exception:
            return None
