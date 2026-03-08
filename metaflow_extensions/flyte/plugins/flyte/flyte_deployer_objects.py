"""DeployedFlow and TriggeredRun objects for the Flyte Deployer plugin."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    pass


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
            if meta_type == "local" and sysroot is None:
                sysroot = os.path.expanduser("~")
            if meta_type == "local" and sysroot:
                # Use "local@<sysroot>" to force compute_info() to set
                # LocalStorage.datastore_root to the provided path.  This is
                # necessary because LocalStorage.SYSROOT_VAR is set at module
                # import time and cannot be changed via os.environ after import.
                metaflow.metadata(f"local@{sysroot}")
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot
            else:
                metaflow.metadata(meta_type)
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
    def flyte_ui(self) -> str | None:
        """URL to the Flyte UI for this workflow execution, if available."""
        # The pathspec is "FlowName/flyte-<execution_id>"; extract the execution id.
        try:
            _, run_id = self.pathspec.split("/")
            if run_id.startswith("flyte-"):
                execution_id = run_id[len("flyte-"):]
                return f"http://localhost:30080/console/projects/flytesnacks/domains/development/executions/{execution_id}"
        except Exception:
            pass
        return None

    @property
    def status(self) -> str | None:
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

    TYPE: ClassVar[str | None] = "flyte"

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
    def from_deployment(cls, identifier: str, metadata: str | None = None) -> FlyteDeployedFlow:
        """Recover a FlyteDeployedFlow from a deployment identifier.

        The identifier is expected to be the JSON string written as ``deployer.name``
        by the ``flyte create --deployer-attribute-file`` command.  It encodes the
        flow file path and other deployment metadata so that the deployer can be
        fully reconstructed.

        A plain flow-name string is also accepted for backwards compatibility,
        though ``trigger()`` will fail without a valid ``flow_file``.
        """
        import json

        from .flyte_deployer import FlyteDeployer

        # Try to parse as JSON; fall back to treating as a plain flow name.
        try:
            info = json.loads(identifier)
        except (json.JSONDecodeError, ValueError):
            info = {"name": identifier, "flow_name": identifier, "flow_file": None}

        deployer = FlyteDeployer(flow_file=info.get("flow_file"), deployer_kwargs={})
        deployer.name = identifier  # preserve the original identifier for chained calls
        deployer.flow_name = info.get("flow_name", info.get("name"))
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
        # Use a list (not a tuple) because the MetaflowAPI click_api type-checks
        # against Union[List[str], Tuple[str]] where Tuple[str] means a 1-element
        # tuple; a multi-element tuple fails the check.
        run_params = [f"{k}={v}" for k, v in kwargs.items()]

        # Apply saved_env from create (includes METAFLOW_FLOW_CONFIG_VALUE) so
        # the trigger subprocess recompiles the flow with the correct config.
        # This is needed for the direct deploy→trigger path (not only from_deployment).
        _additional_info = getattr(self.deployer, "additional_info", {}) or {}
        _saved_env = _additional_info.get("saved_env", {})
        if _saved_env:
            import copy as _copy
            import json as _json
            _env = _copy.copy(dict(self.deployer.env_vars or {}))
            _merged = dict(_saved_env)
            # If METAFLOW_FLOW_CONFIG_VALUE is in saved_env, remove any config
            # names that are already covered by --config (file-based) top-level
            # kwargs so we don't trigger "both a value and a file" conflicts.
            if "METAFLOW_FLOW_CONFIG_VALUE" in _merged:
                _file_config_names = set()
                for _cfg_pair in (self.deployer.top_level_kwargs.get("config") or []):
                    try:
                        _file_config_names.add(_cfg_pair[0])
                    except (IndexError, TypeError):
                        pass
                if _file_config_names:
                    try:
                        _fcv = _json.loads(_merged["METAFLOW_FLOW_CONFIG_VALUE"])
                        for _k in _file_config_names:
                            _fcv.pop(_k, None)
                        if _fcv:
                            _merged["METAFLOW_FLOW_CONFIG_VALUE"] = _json.dumps(_fcv)
                        else:
                            del _merged["METAFLOW_FLOW_CONFIG_VALUE"]
                    except Exception:
                        pass
            _env.update(_merged)
            self.deployer.env_vars = _env

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            # Pass the plain flow class name (e.g. "HelloFlow"), not the JSON
            # recovery identifier stored in self.name, so that the trigger CLI
            # can pass it to _wf_fn() to derive the pyflyte workflow function name.
            trigger_kwargs: dict = {"name": self.flow_name, "deployer_attribute_file": attribute_file_path}
            if run_params:
                trigger_kwargs["run_params"] = run_params
            # Propagate --branch so @project flows use the same branch in the
            # re-compiled trigger as they did in the original create().
            branch = self.deployer.top_level_kwargs.get("branch")
            if branch:
                trigger_kwargs["branch"] = branch
            # Propagate remote-mode settings from additional_info (set by create()).
            _additional_info = getattr(self.deployer, "additional_info", {}) or {}
            if _additional_info.get("flyte_endpoint"):
                trigger_kwargs["flyte_endpoint"] = _additional_info["flyte_endpoint"]
            if _additional_info.get("image"):
                trigger_kwargs["image"] = _additional_info["image"]
            if _additional_info.get("container_flows_dir"):
                trigger_kwargs["container_flows_dir"] = _additional_info["container_flows_dir"]
            if _additional_info.get("container_sysroot"):
                trigger_kwargs["container_sysroot"] = _additional_info["container_sysroot"]
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
            f"Error triggering Flyte execution for flow {self.deployer.flow_file!r}"
        )

    trigger = run
