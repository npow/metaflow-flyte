from metaflow import FlowSpec, project, step


@project(name="myproject")
class ProjectFlow(FlowSpec):
    """A flow decorated with @project to test namespace isolation."""

    @step
    def start(self):
        self.message = "project run"
        self.next(self.end)

    @step
    def end(self):
        print("Message:", self.message)


if __name__ == "__main__":
    ProjectFlow()
