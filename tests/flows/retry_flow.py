from metaflow import FlowSpec, retry, step


class RetryFlow(FlowSpec):
    """A flow with a retry decorator to validate retry config extraction."""

    @step
    def start(self):
        self.next(self.flaky)

    @retry(times=2, minutes_between_retries=1)
    @step
    def flaky(self):
        self.result = "ok"
        self.next(self.end)

    @step
    def end(self):
        print("Result:", self.result)


if __name__ == "__main__":
    RetryFlow()
