"""Flow with @timeout and @environment decorators on steps."""
from metaflow import FlowSpec, environment, step, timeout


class TimeoutEnvFlow(FlowSpec):
    """A flow where steps have @timeout and @environment."""

    @step
    def start(self):
        self.next(self.compute)

    @timeout(hours=1)
    @environment(vars={"MY_VAR": "hello", "REGION": "us-east-1"})
    @step
    def compute(self):
        self.result = 42
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TimeoutEnvFlow()
