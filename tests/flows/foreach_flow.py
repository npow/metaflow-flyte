from metaflow import FlowSpec, step


class ForeachFlow(FlowSpec):
    """A foreach flow: start → for each item → body → join → end."""

    @step
    def start(self):
        self.items = ["alpha", "beta", "gamma"]
        self.next(self.body, foreach="items")

    @step
    def body(self):
        self.result = "processed: %s" % self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [inp.result for inp in inputs]
        self.next(self.end)

    @step
    def end(self):
        print("Results:", self.results)


if __name__ == "__main__":
    ForeachFlow()
