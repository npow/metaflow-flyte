"""Metaflow extension registration for the Flyte integration.

Metaflow discovers this file via the ``metaflow_extensions`` namespace package
mechanism.  The two descriptor lists tell Metaflow to:
  - add ``python flow.py flyte …`` CLI commands, and
  - make the ``--with=flyte_internal`` step decorator available so it can
    be auto-attached when a step runs inside a Flyte task.
"""

CLIS_DESC = [
    ("flyte", ".flyte.flyte_cli.cli"),
]

STEP_DECORATORS_DESC = [
    ("flyte_internal", ".flyte.flyte_decorator.FlyteInternalDecorator"),
]

DEPLOYER_IMPL_PROVIDERS_DESC = [
    ("flyte", ".flyte.flyte_deployer.FlyteDeployer"),
]
