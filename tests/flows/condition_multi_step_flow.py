"""Conditional flow with multi-step arms.

Used to verify that the Flyte codegen correctly handles conditional branches
that contain more than one step — the bug where only immediate branch children
were tracked in condition_branch_set, causing KeyError/NameError for interior
arm steps.

Graph: start (switch) → high_a → high_b → join → end
                      ↘ low  ────────────↗
"""
from metaflow import FlowSpec, step


class ConditionalMultiStepFlow(FlowSpec):
    """Conditional flow where the 'high' arm has two steps."""

    @step
    def start(self):
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
