"""DeployedFlow and TriggeredRun objects for the Flyte Deployer plugin."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar, Optional

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    import metaflow
    import metaflow.runner.deployer_impl


class FlyteTriggeredRun(TriggeredRun):
    """A Flyte workflow execution that was triggered via the Deployer API.

    Inherits ``.run`` from :class:`~metaflow.runner.deployer.TriggeredRun`, which polls
    Metaflow until the run with ``pathspec`` (``FlowName/flyte-<execution_id>``) appears.
    """

    @property
    def flyte_ui(self) -> Optional[str]:
        """URL to the Flyte UI for this workflow execution, if available."""
        # The pathspec is "FlowName/flyte-<execution_id>"; extract the execution id.
        try:
            _, run_id = self.pathspec.split("/")
            if run_id.startswith("flyte-"):
                execution_id = run_id[len("flyte-"):]
                return "http://localhost:30080/console/projects/flytesnacks/domains/development/executions/%s" % execution_id
        except Exception:
            pass
        return None

    @property
    def status(self) -> Optional[str]:
        """Return a simple status string based on the underlying Metaflow run."""
        run = self.run
        if run is None:
            return "PENDING"
        if run.successful:
            return "SUCCEEDED"
        if run.finished:
            return "FAILED"
        return "RUNNING"


class FlyteDeployedFlow(DeployedFlow):
    """A Metaflow flow deployed as a registered Flyte workflow."""

    TYPE: ClassVar[Optional[str]] = "flyte"

    def run(self, **kwargs) -> FlyteTriggeredRun:
        """Trigger a new execution of this deployed Flyte workflow.

        Parameters
        ----------
        **kwargs : Any
            Flow parameters as keyword arguments (e.g. ``greeting="hello"``).

        Returns
        -------
        FlyteTriggeredRun
        """
        # Convert kwargs to "key=value" strings for --run-param.
        run_params = tuple("%s=%s" % (k, v) for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(
                name=self.name,
                deployer_attribute_file=attribute_file_path,
                run_params=run_params,
            )

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )
            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return FlyteTriggeredRun(deployer=self.deployer, content=content)

        raise RuntimeError(
            "Error triggering Flyte execution for flow %r"
            % self.deployer.flow_file
        )
