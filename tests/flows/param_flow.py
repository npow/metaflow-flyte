from metaflow import FlowSpec, Parameter, step


class ParamFlow(FlowSpec):
    """A flow that accepts a Parameter."""

    greeting = Parameter("greeting", default="world", help="Name to greet")

    @step
    def start(self):
        self.message = "hello, %s" % self.greeting
        self.next(self.end)

    @step
    def end(self):
        print("Message:", self.message)


if __name__ == "__main__":
    ParamFlow()
