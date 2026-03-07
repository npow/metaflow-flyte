"""Flow where the foreach step is NOT the start step."""
from metaflow import FlowSpec, step


class MidForeachFlow(FlowSpec):
    """Foreach on a non-start step: start → expand → [body] → join → end."""

    @step
    def start(self):
        self.items = ["x", "y", "z"]
        self.next(self.expand)

    @step
    def expand(self):
        self.next(self.body, foreach="items")

    @step
    def body(self):
        self.result = self.input.upper()
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [inp.result for inp in inputs]
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MidForeachFlow()
