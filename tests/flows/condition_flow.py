"""Flow using conditional branching (split-switch) via self.next({...}, condition=...)."""
from metaflow import FlowSpec, step


class ConditionalFlow(FlowSpec):
    """A flow that uses conditional (split-switch) branching.

    The start step selects exactly one branch at runtime based on
    ``self.value > 5``.  Only the chosen branch step runs; both branches
    merge at the ``join`` step.
    """

    @step
    def start(self):
        self.value = 10
        self.next({"high": self.high, "low": self.low}, condition="self.value > 5")

    @step
    def high(self):
        self.result = "high"
        self.next(self.join)

    @step
    def low(self):
        self.result = "low"
        self.next(self.join)

    @step
    def join(self, inputs):
        self.final = inputs[0].result
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConditionalFlow()
