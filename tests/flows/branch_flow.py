from metaflow import FlowSpec, step


class BranchFlow(FlowSpec):
    """A split/join flow: start → [branch_a, branch_b] → join → end."""

    @step
    def start(self):
        self.message = "hello"
        self.next(self.branch_a, self.branch_b)

    @step
    def branch_a(self):
        self.result_a = self.message + " from A"
        self.next(self.join)

    @step
    def branch_b(self):
        self.result_b = self.message + " from B"
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [
            inp.result_a if hasattr(inp, "result_a") else inp.result_b
            for inp in inputs
        ]
        self.merge_artifacts(inputs, exclude=["result_a", "result_b"])
        self.next(self.end)

    @step
    def end(self):
        print("Results:", self.results)


if __name__ == "__main__":
    BranchFlow()
