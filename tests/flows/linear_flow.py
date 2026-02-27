from metaflow import FlowSpec, step


class LinearFlow(FlowSpec):
    """A simple 3-step linear flow: start → process → end."""

    @step
    def start(self):
        self.message = "hello"
        self.next(self.process)

    @step
    def process(self):
        self.result = self.message + " world"
        self.next(self.end)

    @step
    def end(self):
        print("Result:", self.result)


if __name__ == "__main__":
    LinearFlow()
