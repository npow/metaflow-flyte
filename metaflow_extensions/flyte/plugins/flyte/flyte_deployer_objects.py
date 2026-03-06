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

    For local execution (no remote Flyte cluster), the Metaflow run is written
    to ``~/.metaflow/`` and polled using local metadata.
    """

    @property
    def run(self):
        """Retrieve the Run object, applying deployer env vars so local metadata works."""
        import os
        import metaflow
        from metaflow.exception import MetaflowNotFound

        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA", "local")
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")

        old_meta = os.environ.get("METAFLOW_DEFAULT_METADATA")
        old_sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        try:
            os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
            metaflow.metadata(meta_type)
            if meta_type == "local" and sysroot is None:
                sysroot = os.path.expanduser("~")
            if sysroot:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
            return metaflow.Run(self.pathspec, _namespace_check=False)
        except MetaflowNotFound:
            return None
        except Exception:
            return None
        finally:
            if old_meta is None:
                os.environ.pop("METAFLOW_DEFAULT_METADATA", None)
            else:
                os.environ["METAFLOW_DEFAULT_METADATA"] = old_meta
            if old_sysroot is None:
                os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
            else:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old_sysroot

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

    @property
    def id(self) -> str:
        """Deployment identifier encoding all info needed for ``from_deployment``."""
        import json
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        return json.dumps({
            "name": self.name,
            "flow_name": self.flow_name,
            "flow_file": getattr(self.deployer, "flow_file", None),
            **additional_info,
        })

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None) -> "FlyteDeployedFlow":
        """Recover a FlyteDeployedFlow from a deployment identifier."""
        import json
        from .flyte_deployer import FlyteDeployer

        info = json.loads(identifier)
        deployer = FlyteDeployer(flow_file=info["flow_file"], deployer_kwargs={})
        deployer.name = info["name"]
        deployer.flow_name = info["flow_name"]
        deployer.metadata = metadata or "{}"
        deployer.additional_info = {
            k: v for k, v in info.items()
            if k not in ("name", "flow_name", "flow_file")
        }
        # Restore saved env vars so the second trigger uses the same metadata config.
        saved_env = (deployer.additional_info or {}).get("saved_env", {})
        if saved_env:
            deployer.env_vars = dict(deployer.env_vars)
            deployer.env_vars.update(saved_env)
        return cls(deployer=deployer)

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
            trigger_kwargs = dict(name=self.name, deployer_attribute_file=attribute_file_path)
            if run_params:
                trigger_kwargs["run_params"] = run_params
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(**trigger_kwargs)

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

    trigger = run
