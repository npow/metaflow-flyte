"""Exception classes for the Metaflow-Flyte integration."""

from metaflow.exception import MetaflowException


class FlyteException(MetaflowException):
    """Base exception for Metaflow-Flyte integration errors."""

    headline = "Flyte error"


class NotSupportedException(FlyteException):
    """Raised when a Metaflow feature is not supported by the Flyte integration."""

    headline = "Not supported in Flyte"
