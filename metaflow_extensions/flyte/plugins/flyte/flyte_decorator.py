"""flyte_internal step decorator.

Auto-attached via ``--with=flyte_internal`` when Metaflow runs inside a
Flyte task.  Responsibilities:
  - Records Flyte execution ID, project, and domain in Metaflow metadata
    so runs are cross-referenced between the two systems.
  - Writes the foreach split-count to a side-car JSON file so the Flyte
    dynamic task can fan out the body steps.
"""

from __future__ import annotations

import json
import os
from typing import Any

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum

# Environment variables set by the Flyte task before launching each step subprocess.
ENV_EXECUTION_ID = "METAFLOW_FLYTE_EXECUTION_ID"
ENV_EXECUTION_PROJECT = "METAFLOW_FLYTE_EXECUTION_PROJECT"
ENV_EXECUTION_DOMAIN = "METAFLOW_FLYTE_EXECUTION_DOMAIN"

# Path where foreach cardinality is written so the @dynamic task can fan out.
ENV_FOREACH_INFO_PATH = "METAFLOW_FLYTE_FOREACH_INFO_PATH"


class FlyteInternalDecorator(StepDecorator):
    """Internal decorator attached to every step running inside Flyte."""

    name = "flyte_internal"

    def task_pre_step(
        self,
        step_name: str,
        task_datastore: Any,
        metadata: Any,
        run_id: str,
        task_id: str,
        flow: Any,
        graph: Any,
        retry_count: int,
        max_user_code_retries: int,
        ubf_context: Any,
        inputs: Any,
    ) -> None:
        meta: dict[str, str] = {}

        execution_id = os.environ.get(ENV_EXECUTION_ID)
        project = os.environ.get(ENV_EXECUTION_PROJECT)
        domain = os.environ.get(ENV_EXECUTION_DOMAIN)

        if execution_id:
            meta["flyte-execution-id"] = execution_id
        if project:
            meta["flyte-execution-project"] = project
        if domain:
            meta["flyte-execution-domain"] = domain

        entries = [
            MetaDatum(
                field=k,
                value=v,
                type=k,
                tags=["attempt_id:{0}".format(retry_count)],
            )
            for k, v in meta.items()
        ]
        if entries:
            metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self,
        step_name: str,
        flow: Any,
        graph: Any,
        is_task_ok: bool,
        retry_count: int,
        max_user_code_retries: int,
    ) -> None:
        if not is_task_ok:
            return

        if graph[step_name].type == "foreach":
            info_path = os.environ.get(ENV_FOREACH_INFO_PATH)
            if info_path:
                num_splits = getattr(flow, "_foreach_num_splits", None)
                if num_splits is not None:
                    with open(info_path, "w") as f:
                        json.dump({"num_splits": int(num_splits)}, f)
