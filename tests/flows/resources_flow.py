"""Flow using @resources decorator — should compile but emit a UserWarning."""
from metaflow import FlowSpec, resources, step


class ResourcesFlow(FlowSpec):
    """A flow that uses @resources on a step."""

    @step
    def start(self):
        self.message = "hello"
        self.next(self.compute)

    @resources(cpu=4, memory=8000)
    @step
    def compute(self):
        self.result = self.message + " world"
        self.next(self.end)

    @step
    def end(self):
        print(self.result)


if __name__ == "__main__":
    ResourcesFlow()
