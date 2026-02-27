from metaflow import FlowSpec, schedule, step


@schedule(cron="0 * * * *")  # hourly (5-field cron for Flyte)
class ScheduledFlow(FlowSpec):
    """A flow with @schedule — validates LaunchPlan generation."""

    @step
    def start(self):
        self.message = "scheduled run"
        self.next(self.end)

    @step
    def end(self):
        print("Message:", self.message)


if __name__ == "__main__":
    ScheduledFlow()
