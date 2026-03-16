"""Conditional flow with multi-step arms — for Netflix/metaflow UX test suite.

Verifies that the Flyte backend correctly handles conditional branches where
each arm has more than one step.

Graph: start (switch) → high_a → high_b → join → end
                      ↘ low  ────────────↗
"""
import os

from metaflow import FlowSpec, step, project


@project(name="condition_multi_step_flow")
class ConditionalMultiStepFlow(FlowSpec):
    """Conditional flow where the 'high' arm has two steps."""

    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.value = 10
        self.branch = "high" if self.value > 5 else "low"
        self.next({"high": self.high_a, "low": self.low}, condition="branch")

    @step
    def high_a(self):
        self.label = "high_a"
        self.next(self.high_b)

    @step
    def high_b(self):
        self.result = "high"
        self.next(self.join)

    @step
    def low(self):
        self.result = "low"
        self.next(self.join)

    @step
    def join(self):
        self.final = self.result
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConditionalMultiStepFlow()
