"""Unit tests for _graph.py — focusing on _validate() checks."""

from __future__ import annotations

import warnings

import pytest

from metaflow_extensions.flyte.plugins.flyte._graph import (
    _is_condition_join,
    _topological_order,
    _validate,
)
from metaflow_extensions.flyte.plugins.flyte._types import NodeType
from metaflow_extensions.flyte.plugins.flyte.exception import NotSupportedException

# ---------------------------------------------------------------------------
# Minimal stubs for graph nodes and flow objects
# ---------------------------------------------------------------------------


class _Deco:
    """Minimal stub for a Metaflow step decorator."""

    def __init__(self, name: str):
        self.name = name

    def step_task_retry_count(self):
        return 0, 0


class _Node:
    """Minimal stub for a Metaflow graph node."""

    def __init__(self, name: str, decos: list[str] | None = None, parallel_foreach: bool = False):
        self.name = name
        self.parallel_foreach = parallel_foreach
        self.decorators = [_Deco(d) for d in (decos or [])]
        self.type = "linear"
        self.in_funcs: list[str] = []
        self.out_funcs: list[str] = []
        self.split_parents: list[str] = []
        self.switch_cases: dict = {}


class _Graph:
    """Minimal stub for a Metaflow FlowGraph."""

    def __init__(self, nodes: list[_Node]):
        self._nodes = {n.name: n for n in nodes}

    def __iter__(self):
        return iter(self._nodes.values())

    def __getitem__(self, name: str) -> _Node:
        return self._nodes[name]


class _Flow:
    """Minimal stub for a Metaflow FlowSpec instance."""

    def _flow_decorators_get(self):
        return {}

    _flow_decorators = _FakeFlowDecos = None

    class _FD:
        def get(self, key):
            return None

    def __init__(self):
        self._flow_decorators = self._FD()


# ---------------------------------------------------------------------------
# Tests for @condition (split-switch) — now supported, should NOT raise
# ---------------------------------------------------------------------------


class TestConditionValidation:
    def _make_graph_with_deco(self, deco_name: str) -> tuple[_Graph, _Flow]:
        node = _Node("start", decos=[deco_name])
        return _Graph([node]), _Flow()

    def test_condition_decorator_does_not_raise(self):
        """@condition is now supported — validation must not raise."""
        graph, flow = self._make_graph_with_deco("condition")
        # Should not raise NotSupportedException
        _validate(graph, flow)

    def test_no_condition_does_not_raise(self):
        node = _Node("start", decos=["retry"])
        graph = _Graph([node])
        flow = _Flow()
        _validate(graph, flow)

    def test_batch_still_raises(self):
        node = _Node("start", decos=["batch"])
        graph = _Graph([node])
        flow = _Flow()
        with pytest.raises(NotSupportedException, match="batch"):
            _validate(graph, flow)


# ---------------------------------------------------------------------------
# Tests for @resources warning
# ---------------------------------------------------------------------------


class TestResourcesWarning:
    def test_resources_emits_no_warning(self):
        """@resources is translated to native Flyte Resources — no warning should be emitted."""
        node = _Node("compute", decos=["resources"])
        graph = _Graph([node])
        flow = _Flow()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _validate(graph, flow)

        resource_warnings = [w for w in caught if issubclass(w.category, UserWarning) and "resources" in str(w.message).lower()]
        assert len(resource_warnings) == 0

    def test_resources_does_not_raise(self):
        node = _Node("step_a", decos=["resources"])
        graph = _Graph([node])
        flow = _Flow()
        # Must not raise
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            _validate(graph, flow)

    def test_multiple_resources_steps_no_warnings(self):
        """Multiple @resources steps should not emit any warnings."""
        nodes = [
            _Node("step_a", decos=["resources"]),
            _Node("step_b", decos=["resources"]),
        ]
        graph = _Graph(nodes)
        flow = _Flow()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _validate(graph, flow)

        resource_warnings = [w for w in caught if issubclass(w.category, UserWarning) and "resources" in str(w.message).lower()]
        assert len(resource_warnings) == 0


# ---------------------------------------------------------------------------
# Tests for _is_condition_join and split-switch graph analysis
# ---------------------------------------------------------------------------


class TestIsConditionJoin:
    """Tests for the _is_condition_join helper."""

    def _make_switch_graph(self) -> _Graph:
        """Build a minimal split-switch graph: start --(switch)--> high, low --> join --> end."""
        start = _Node("start")
        start.type = "split-switch"
        start.out_funcs = ["high", "low"]
        start.switch_cases = {"high": "high", "low": "low"}

        high = _Node("high")
        high.type = "linear"
        high.in_funcs = ["start"]
        high.out_funcs = ["join"]

        low = _Node("low")
        low.type = "linear"
        low.in_funcs = ["start"]
        low.out_funcs = ["join"]

        join = _Node("join")
        join.type = "join"
        join.in_funcs = ["high", "low"]
        join.out_funcs = ["end"]

        end = _Node("end")
        end.type = "end"
        end.in_funcs = ["join"]

        return _Graph([start, high, low, join, end])

    def test_join_after_split_switch_is_detected(self):
        graph = self._make_switch_graph()
        join_node = graph["join"]
        assert _is_condition_join(graph, join_node) is True

    def test_non_join_node_is_not_condition_join(self):
        graph = self._make_switch_graph()
        assert _is_condition_join(graph, graph["high"]) is False
        assert _is_condition_join(graph, graph["start"]) is False
        assert _is_condition_join(graph, graph["end"]) is False

    def test_regular_split_join_is_not_condition_join(self):
        """A join after a regular split (not split-switch) should return False."""
        start = _Node("start")
        start.type = "split"
        start.out_funcs = ["a", "b"]

        a = _Node("a")
        a.type = "linear"
        a.in_funcs = ["start"]
        a.out_funcs = ["join"]

        b = _Node("b")
        b.type = "linear"
        b.in_funcs = ["start"]
        b.out_funcs = ["join"]

        join = _Node("join")
        join.type = "join"
        join.in_funcs = ["a", "b"]
        join.split_parents = ["start"]

        graph = _Graph([start, a, b, join])
        assert _is_condition_join(graph, join) is False

    def test_linear_join_with_no_switch_parent_is_false(self):
        """A join whose parents come from linear nodes is not a condition join."""
        prev = _Node("prev")
        prev.type = "linear"
        prev.out_funcs = ["join"]

        join = _Node("join")
        join.type = "join"
        join.in_funcs = ["prev"]

        prev2 = _Node("prev2")
        prev2.type = "linear"
        prev2.out_funcs = ["join"]
        join.in_funcs = ["prev", "prev2"]

        prev.in_funcs = ["start_node"]
        prev2.in_funcs = ["start_node"]

        start_node = _Node("start_node")
        start_node.type = "linear"
        start_node.out_funcs = ["prev", "prev2"]

        graph = _Graph([start_node, prev, prev2, join])
        assert _is_condition_join(graph, join) is False


class TestSplitSwitchNodeType:
    """Tests for NodeType.SPLIT_SWITCH enum value."""

    def test_split_switch_enum_value(self):
        assert NodeType.SPLIT_SWITCH == "split-switch"
        assert NodeType("split-switch") == NodeType.SPLIT_SWITCH

    def test_split_switch_is_str(self):
        assert isinstance(NodeType.SPLIT_SWITCH, str)

    def test_split_switch_distinct_from_split(self):
        assert NodeType.SPLIT_SWITCH != NodeType.SPLIT
        assert NodeType.SPLIT_SWITCH != NodeType.JOIN


class TestTopologicalOrderStartEndAnnotations:
    """Regression tests for @step(start=True)/@step(end=True) annotation support.

    Metaflow PR Netflix/metaflow#3120 lets a flow's entry/terminal step have
    any name, exposing them via graph.start_step / graph.end_step. Before
    this fix, _topological_order hard-coded the entry name as "start" and
    crashed with KeyError on any flow that used the new annotations.
    """

    @staticmethod
    def _annotated_graph() -> _Graph:
        """entry --(linear)--> done; entry is start, done is end (custom names)."""
        entry = _Node("entry")
        entry.out_funcs = ["done"]

        done = _Node("done")
        done.type = "end"
        done.in_funcs = ["entry"]

        graph = _Graph([entry, done])
        graph.start_step = "entry"  # type: ignore[attr-defined]
        graph.end_step = "done"  # type: ignore[attr-defined]
        return graph

    def test_topological_order_uses_graph_start_step(self):
        graph = self._annotated_graph()
        steps = _topological_order(graph)
        assert [s.name for s in steps] == ["entry", "done"]

    def test_is_start_and_is_end_flagged_correctly(self):
        graph = self._annotated_graph()
        steps = {s.name: s for s in _topological_order(graph)}
        assert steps["entry"].is_start is True
        assert steps["entry"].is_end is False
        assert steps["done"].is_start is False
        assert steps["done"].is_end is True

    def test_legacy_named_steps_still_work(self):
        """Backward compat: graphs without start_step attr fall back to literal names."""
        start = _Node("start")
        start.out_funcs = ["end"]

        end = _Node("end")
        end.type = "end"
        end.in_funcs = ["start"]

        graph = _Graph([start, end])
        # No start_step / end_step attrs — emulates older metaflow.
        steps = {s.name: s for s in _topological_order(graph)}
        assert steps["start"].is_start is True
        assert steps["end"].is_end is True
