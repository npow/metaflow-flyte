"""Flow using @condition decorator — should fail validation."""
from metaflow import FlowSpec, condition, step


class ConditionFlow(FlowSpec):
    """A flow that uses @condition for branching."""

    @step
    def start(self):
        self.value = 1
        self.next(self.a, self.b, condition="self.value > 0")

    @condition(lambda self: self.value > 0)
    @step
    def a(self):
        self.result = "a"
        self.next(self.end)

    @condition(lambda self: self.value <= 0)
    @step
    def b(self):
        self.result = "b"
        self.next(self.end)

    @step
    def end(self):
        print(self.result)


if __name__ == "__main__":
    ConditionFlow()
