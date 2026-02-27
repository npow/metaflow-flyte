from metaflow import FlowSpec, Parameter, step


class ConfigFlow(FlowSpec):
    """A flow that tests multiple parameters and artifact types."""

    multiplier = Parameter("multiplier", default=2, type=int, help="Multiplier value")
    label = Parameter("label", default="test", help="Label string")

    @step
    def start(self):
        self.numbers = [1, 2, 3, 4, 5]
        self.config_label = self.label
        self.next(self.compute)

    @step
    def compute(self):
        self.doubled = [x * self.multiplier for x in self.numbers]
        self.total = sum(self.doubled)
        self.metadata_dict = {"label": self.config_label, "total": self.total}
        self.next(self.end)

    @step
    def end(self):
        print("Total:", self.total)
        print("Label:", self.config_label)


if __name__ == "__main__":
    ConfigFlow()
