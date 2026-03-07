"""Flow where the split-switch step is NOT the start step."""
from metaflow import FlowSpec, step


class MidConditionFlow(FlowSpec):
    """Conditional on a non-start step: start → decide → [high, low] → join → end."""

    @step
    def start(self):
        self.value = 10
        self.next(self.decide)

    @step
    def decide(self):
        self.branch = "high" if self.value > 5 else "low"
        self.next({"high": self.high, "low": self.low}, condition="branch")

    @step
    def high(self):
        self.result = "high path"
        self.next(self.join)

    @step
    def low(self):
        self.result = "low path"
        self.next(self.join)

    @step
    def join(self):
        self.final = self.result
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MidConditionFlow()
