"""Unit tests for _graph.py — focusing on _validate() checks."""

from __future__ import annotations

import types
import warnings

import pytest

from metaflow_extensions.flyte.plugins.flyte._graph import _validate
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
# Tests for @condition
# ---------------------------------------------------------------------------


class TestConditionValidation:
    def _make_graph_with_deco(self, deco_name: str) -> tuple[_Graph, _Flow]:
        node = _Node("start", decos=[deco_name])
        return _Graph([node]), _Flow()

    def test_condition_raises_not_supported(self):
        graph, flow = self._make_graph_with_deco("condition")
        with pytest.raises(NotSupportedException, match="condition"):
            _validate(graph, flow)

    def test_condition_error_message_mentions_step(self):
        node = _Node("my_step", decos=["condition"])
        graph = _Graph([node])
        flow = _Flow()
        with pytest.raises(NotSupportedException, match="my_step"):
            _validate(graph, flow)

    def test_no_condition_does_not_raise(self):
        node = _Node("start", decos=["retry"])
        graph = _Graph([node])
        flow = _Flow()
        # Should not raise
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
    def test_resources_emits_user_warning(self):
        node = _Node("compute", decos=["resources"])
        graph = _Graph([node])
        flow = _Flow()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _validate(graph, flow)

        resource_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(resource_warnings) == 1
        assert "resources" in str(resource_warnings[0].message).lower()

    def test_resources_warning_mentions_step_name(self):
        node = _Node("my_gpu_step", decos=["resources"])
        graph = _Graph([node])
        flow = _Flow()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _validate(graph, flow)

        msgs = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
        assert any("my_gpu_step" in m for m in msgs), "Warning should mention step name"

    def test_resources_warning_is_user_warning(self):
        node = _Node("step_a", decos=["resources"])
        graph = _Graph([node])
        flow = _Flow()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _validate(graph, flow)

        assert all(issubclass(w.category, UserWarning) for w in caught if "resources" in str(w.message).lower())

    def test_resources_does_not_raise(self):
        node = _Node("step_a", decos=["resources"])
        graph = _Graph([node])
        flow = _Flow()
        # Must not raise
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            _validate(graph, flow)

    def test_multiple_resources_steps_each_warn(self):
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
        assert len(resource_warnings) == 2
