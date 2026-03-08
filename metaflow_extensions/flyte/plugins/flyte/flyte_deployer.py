"""Metaflow Deployer plugin for Flyte.

Registers ``TYPE = "flyte"`` so that ``Deployer(flow_file).flyte(...)``
is available and the UX test suite can parametrise ``--scheduler-type=flyte``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    from metaflow_extensions.flyte.plugins.flyte.flyte_deployer_objects import (
        FlyteDeployedFlow,
    )


class FlyteDeployer(DeployerImpl):
    """Deployer implementation for Flyte.

    Parameters
    ----------
    flyte_project : str, optional
        Flyte project name (default: ``flytesnacks``).
    flyte_domain : str, optional
        Flyte domain (default: ``development``).
    image : str, optional
        Docker image URI for the Flyte task containers.
    """

    TYPE: ClassVar[str | None] = "flyte"

    def __init__(self, deployer_kwargs: dict[str, str], **kwargs) -> None:
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> dict[str, str]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> type[FlyteDeployedFlow]:
        from .flyte_deployer_objects import FlyteDeployedFlow

        return FlyteDeployedFlow

    def create(self, **kwargs) -> FlyteDeployedFlow:
        """Register this flow as a Flyte workflow and return a deployed-flow handle.

        Parameters
        ----------
        output_file : str
            Path for the generated Flyte workflow Python file.
        tags : list[str], optional
            Tags to attach to Metaflow runs.
        flyte_project : str, optional
            Flyte project name.
        flyte_domain : str, optional
            Flyte domain.
        image : str, optional
            Docker image URI (required for remote registration).
        deployer_attribute_file : str, optional
            Write deployment info JSON here (Metaflow Deployer API internal).

        Returns
        -------
        FlyteDeployedFlow
        """
        from .flyte_deployer_objects import FlyteDeployedFlow

        return self._create(FlyteDeployedFlow, **kwargs)
