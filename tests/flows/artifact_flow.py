from metaflow import FlowSpec, step


class ArtifactFlow(FlowSpec):
    """A flow that produces various types of artifacts."""

    @step
    def start(self):
        self.int_val = 42
        self.float_val = 3.14
        self.str_val = "hello"
        self.list_val = [1, 2, 3]
        self.dict_val = {"key": "value", "nested": {"a": 1}}
        self.next(self.transform)

    @step
    def transform(self):
        self.doubled = self.int_val * 2
        self.appended = self.list_val + [4, 5]
        self.merged = {**self.dict_val, "extra": True}
        self.next(self.end)

    @step
    def end(self):
        print("doubled:", self.doubled)
        print("appended:", self.appended)


if __name__ == "__main__":
    ArtifactFlow()
