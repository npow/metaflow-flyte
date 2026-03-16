"""Adversarial tests: error paths, edge cases, and branches not hit by happy-path tests.

These tests target the uncovered lines identified by the coverage report and
exercise behaviours that go wrong in realistic misuse scenarios.
"""
from __future__ import annotations

import subprocess
import sys
import warnings
from pathlib import Path

import pytest

from metaflow_extensions.flyte.plugins.flyte._codegen import (
    _compute_condition_arm_chains,
    _find_switch_step_for_join,
    _task_decorator,
    _wf_fn,
)
from metaflow_extensions.flyte.plugins.flyte._graph import (
    _extract_schedule,
    _is_foreach_join,
    _is_split_join,
    _param_kwarg,
    _step_env_vars,
    _step_timeout_seconds,
    _validate,
)
from metaflow_extensions.flyte.plugins.flyte._types import NodeType, StepSpec
from metaflow_extensions.flyte.plugins.flyte.exception import NotSupportedException

FLOWS_DIR = Path(__file__).parent / "flows"


# ---------------------------------------------------------------------------
# Helpers shared with test_graph.py
# ---------------------------------------------------------------------------


class _Deco:
    def __init__(self, name: str, **attrs):
        self.name = name
        self.attributes = attrs

    def step_task_retry_count(self):
        return 0, 0


class _Node:
    def __init__(self, name: str, decos: list | None = None, parallel_foreach: bool = False):
        self.name = name
        self.parallel_foreach = parallel_foreach
        self.decorators = decos or []
        self.type = "linear"
        self.in_funcs: list[str] = []
        self.out_funcs: list[str] = []
        self.split_parents: list[str] = []
        self.switch_cases: dict = {}


class _Graph:
    def __init__(self, nodes: list[_Node]):
        self._nodes = {n.name: n for n in nodes}

    def __iter__(self):
        return iter(self._nodes.values())

    def __getitem__(self, name: str) -> _Node:
        return self._nodes[name]


class _Flow:
    class _FD:
        def __init__(self, data: dict | None = None):
            self._data = data or {}

        def get(self, key):
            return self._data.get(key)

    def __init__(self, flow_decos: dict | None = None, raise_on_get: bool = False):
        if raise_on_get:
            class _RaisingFD:
                def get(self, key):
                    raise RuntimeError("simulated error")
            self._flow_decorators = _RaisingFD()
        else:
            self._flow_decorators = self._FD(flow_decos)


# ---------------------------------------------------------------------------
# _validate: @parallel raises
# ---------------------------------------------------------------------------


class TestValidateParallel:
    def test_parallel_foreach_raises(self):
        node = _Node("train")
        node.parallel_foreach = True
        graph = _Graph([node])
        with pytest.raises(NotSupportedException, match="parallel"):
            _validate(graph, _Flow())

    def test_parallel_only_one_step_raises(self):
        """Only the affected step needs @parallel to trigger the error."""
        a = _Node("a")
        b = _Node("b")
        b.parallel_foreach = True
        graph = _Graph([a, b])
        with pytest.raises(NotSupportedException):
            _validate(graph, _Flow())


# ---------------------------------------------------------------------------
# _validate: @slurm raises
# ---------------------------------------------------------------------------


class TestValidateSlurm:
    def test_slurm_raises(self):
        node = _Node("train", decos=[_Deco("slurm")])
        graph = _Graph([node])
        with pytest.raises(NotSupportedException, match="slurm"):
            _validate(graph, _Flow())

    def test_slurm_error_mentions_step_name(self):
        node = _Node("gpu_step", decos=[_Deco("slurm")])
        graph = _Graph([node])
        with pytest.raises(NotSupportedException, match="gpu_step"):
            _validate(graph, _Flow())


# ---------------------------------------------------------------------------
# _validate: unsupported flow decorators warn
# ---------------------------------------------------------------------------


class TestValidateFlowDecos:
    def test_trigger_does_not_warn(self):
        """@trigger is now supported — _validate must not emit a warning for it."""
        graph = _Graph([_Node("start")])

        class _FakeDeco:
            pass

        flow = _Flow(flow_decos={"trigger": [_FakeDeco()]})
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _validate(graph, flow)
        msgs = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
        assert not any("trigger" in m and "ignored" in m for m in msgs)

    def test_exit_hook_emits_warning(self):
        graph = _Graph([_Node("start")])

        class _FakeDeco:
            pass

        flow = _Flow(flow_decos={"exit_hook": [_FakeDeco()]})
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _validate(graph, flow)
        msgs = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
        assert any("exit_hook" in m for m in msgs)

    def test_exception_in_flow_decorators_get_is_swallowed(self):
        """If _flow_decorators.get() raises, _validate should not propagate it."""
        graph = _Graph([_Node("start")])
        flow = _Flow(raise_on_get=True)
        # Must not raise
        _validate(graph, flow)


# ---------------------------------------------------------------------------
# _step_timeout_seconds: hours / minutes / seconds
# ---------------------------------------------------------------------------


class TestStepTimeoutSeconds:
    def _node_with_timeout(self, **kwargs) -> _Node:
        node = _Node("step")
        node.decorators = [_Deco("timeout", **kwargs)]
        return node

    def test_timeout_seconds(self):
        assert _step_timeout_seconds(self._node_with_timeout(seconds=90)) == 90

    def test_timeout_minutes(self):
        assert _step_timeout_seconds(self._node_with_timeout(minutes=2)) == 120

    def test_timeout_hours(self):
        assert _step_timeout_seconds(self._node_with_timeout(hours=1)) == 3600

    def test_timeout_combined(self):
        assert _step_timeout_seconds(self._node_with_timeout(hours=1, minutes=30, seconds=5)) == 5405

    def test_no_timeout_returns_none(self):
        assert _step_timeout_seconds(_Node("step")) is None

    def test_timeout_zero_returns_none(self):
        """A timeout of 0 should be treated as absent."""
        assert _step_timeout_seconds(self._node_with_timeout(seconds=0)) is None


# ---------------------------------------------------------------------------
# _step_env_vars
# ---------------------------------------------------------------------------


class TestStepEnvVars:
    def test_environment_vars_extracted(self):
        node = _Node("step")
        node.decorators = [_Deco("environment", vars={"KEY": "val", "OTHER": "x"})]
        result = _step_env_vars(node)
        assert result == (("KEY", "val"), ("OTHER", "x"))  # sorted

    def test_environment_vars_sorted(self):
        node = _Node("step")
        node.decorators = [_Deco("environment", vars={"Z": "1", "A": "2"})]
        result = _step_env_vars(node)
        assert result[0][0] == "A"
        assert result[1][0] == "Z"

    def test_no_environment_returns_empty(self):
        assert _step_env_vars(_Node("step")) == ()


# ---------------------------------------------------------------------------
# _is_foreach_join and _is_split_join: early-return branches
# ---------------------------------------------------------------------------


class TestJoinClassifiers:
    def _join_without_split_parents(self) -> _Node:
        node = _Node("join")
        node.type = "join"
        node.split_parents = []  # empty → early return
        return node

    def test_foreach_join_no_split_parents_returns_false(self):
        node = self._join_without_split_parents()
        graph = _Graph([node])
        assert _is_foreach_join(graph, node) is False

    def test_split_join_no_split_parents_returns_false(self):
        node = self._join_without_split_parents()
        graph = _Graph([node])
        assert _is_split_join(graph, node) is False

    def test_foreach_join_wrong_parent_type_returns_false(self):
        """A join whose split parent is a 'split', not 'foreach', is not a foreach join."""
        parent = _Node("s")
        parent.type = "split"
        join = _Node("j")
        join.type = "join"
        join.split_parents = ["s"]
        graph = _Graph([parent, join])
        assert _is_foreach_join(graph, join) is False

    def test_split_join_wrong_parent_type_returns_false(self):
        """A join whose split parent is 'foreach', not 'split', is not a split join."""
        parent = _Node("s")
        parent.type = "foreach"
        join = _Node("j")
        join.type = "join"
        join.split_parents = ["s"]
        graph = _Graph([parent, join])
        assert _is_split_join(graph, join) is False


# ---------------------------------------------------------------------------
# _param_kwarg: _override_kwargs fallback
# ---------------------------------------------------------------------------


class TestParamKwarg:
    def test_reads_from_kwargs(self):
        class _P:
            kwargs = {"default": 42, "help": "desc"}
        assert _param_kwarg(_P(), "default") == 42

    def test_falls_back_to_override_kwargs(self):
        class _P:
            kwargs = {"default": None}
            _override_kwargs = {"default": "override_value"}
        assert _param_kwarg(_P(), "default") == "override_value"

    def test_returns_none_when_absent_everywhere(self):
        class _P:
            kwargs = {}
        assert _param_kwarg(_P(), "missing") is None

    def test_kwargs_takes_priority_over_override(self):
        class _P:
            kwargs = {"default": "primary"}
            _override_kwargs = {"default": "fallback"}
        assert _param_kwarg(_P(), "default") == "primary"


# ---------------------------------------------------------------------------
# _extract_schedule: keyword schedules (weekly/daily/hourly)
# ---------------------------------------------------------------------------


class TestExtractSchedule:
    class _SchedDeco:
        def __init__(self, **attrs):
            self.attributes = attrs

    def _flow_with_schedule(self, **attrs):
        flow = _Flow()
        flow._flow_decorators = _Flow._FD({"schedule": [self._SchedDeco(**attrs)]})
        return flow

    def test_cron_string_returned_verbatim(self):
        flow = self._flow_with_schedule(cron="*/5 * * * *")
        assert _extract_schedule(flow) == "*/5 * * * *"

    def test_daily_keyword(self):
        flow = self._flow_with_schedule(daily=True)
        assert _extract_schedule(flow) == "0 0 * * *"

    def test_weekly_keyword(self):
        flow = self._flow_with_schedule(weekly=True)
        assert _extract_schedule(flow) == "0 0 * * 0"

    def test_hourly_keyword(self):
        flow = self._flow_with_schedule(hourly=True)
        assert _extract_schedule(flow) == "0 * * * *"

    def test_no_schedule_returns_none(self):
        flow = _Flow()
        assert _extract_schedule(flow) is None

    def test_schedule_with_no_matching_key_returns_none(self):
        """A schedule decorator with no recognised keys returns None."""
        flow = self._flow_with_schedule(unknown_key=True)
        assert _extract_schedule(flow) is None


# ---------------------------------------------------------------------------
# _task_decorator: timeout included when set
# ---------------------------------------------------------------------------


class TestTaskDecorator:
    def _step(self, **kwargs) -> StepSpec:
        return StepSpec(
            name="s",
            node_type=NodeType.LINEAR,
            in_funcs=(),
            out_funcs=(),
            split_parents=(),
            max_user_code_retries=kwargs.get("retries", 0),
            is_foreach_join=False,
            is_split_join=False,
            is_condition_join=False,
            switch_cases=(),
            timeout_seconds=kwargs.get("timeout_seconds"),
            env_vars=(),
        )

    def test_no_timeout_no_timedelta(self):
        dec = _task_decorator(self._step())
        assert "timedelta" not in dec
        assert "retries=0" in dec

    def test_timeout_seconds_included(self):
        dec = _task_decorator(self._step(timeout_seconds=3600))
        assert "timedelta(seconds=3600)" in dec

    def test_retries_included(self):
        dec = _task_decorator(self._step(retries=3))
        assert "retries=3" in dec


# ---------------------------------------------------------------------------
# _find_switch_step_for_join: fallback paths
# ---------------------------------------------------------------------------


class TestFindSwitchStepForJoin:
    def _step(self, name: str, in_funcs: tuple = ()) -> StepSpec:
        return StepSpec(
            name=name,
            node_type=NodeType.JOIN,
            in_funcs=in_funcs,
            out_funcs=(),
            split_parents=(),
            max_user_code_retries=0,
            is_foreach_join=False,
            is_split_join=False,
            is_condition_join=True,
            switch_cases=(),
            timeout_seconds=None,
            env_vars=(),
        )

    def test_returns_switch_name_when_branch_found(self):
        join = self._step("join", in_funcs=("branch_a",))
        branches = {"decide": frozenset({"branch_a", "branch_b"})}
        assert _find_switch_step_for_join(join, branches) == "decide"

    def test_fallback_returns_first_in_func(self):
        """No matching branch in condition_branches → fall back to first in_func."""
        join = self._step("join", in_funcs=("some_other_step",))
        branches = {"decide": frozenset({"unrelated_a", "unrelated_b"})}
        assert _find_switch_step_for_join(join, branches) == "some_other_step"

    def test_fallback_returns_start_when_no_in_funcs(self):
        """No in_funcs at all → return 'start'."""
        join = self._step("join", in_funcs=())
        assert _find_switch_step_for_join(join, {}) == "start"

    def test_empty_condition_branches_uses_fallback(self):
        join = self._step("join", in_funcs=("prev",))
        assert _find_switch_step_for_join(join, {}) == "prev"

    def test_deep_arm_step_resolved_via_switch_for_arm_step(self):
        """Multi-step arm: join.in_funcs = [last_arm_step], not an immediate branch child.
        _find_switch_step_for_join must find the switch via switch_for_arm_step."""
        join = self._step("join", in_funcs=("high_b",))
        branches = {"start": frozenset({"high_a", "low"})}
        # high_b is a deeper interior arm step, not an immediate branch child
        switch_for_arm_step = {"high_a": "start", "high_b": "start", "low": "start"}
        result = _find_switch_step_for_join(join, branches, switch_for_arm_step)
        assert result == "start"

    def test_direct_match_takes_priority_over_deep_arm(self):
        """Direct branch child match wins over switch_for_arm_step lookup."""
        join = self._step("join", in_funcs=("high_a",))
        branches = {"start": frozenset({"high_a", "low"})}
        switch_for_arm_step = {"high_a": "start", "low": "start"}
        assert _find_switch_step_for_join(join, branches, switch_for_arm_step) == "start"


# ---------------------------------------------------------------------------
# _compute_condition_arm_chains: multi-step arm chain computation
# ---------------------------------------------------------------------------


class TestComputeConditionArmChains:
    def _make_step(self, name, is_condition_join=False, out_funcs=()):
        return StepSpec(
            name=name,
            node_type=NodeType.LINEAR,
            in_funcs=(),
            out_funcs=out_funcs,
            split_parents=(),
            max_user_code_retries=0,
            is_foreach_join=False,
            is_split_join=False,
            is_condition_join=is_condition_join,
            switch_cases=(),
            timeout_seconds=None,
            env_vars=(),
        )

    def test_single_step_arm_has_chain_length_one(self):
        high = self._make_step("high", out_funcs=("join",))
        low = self._make_step("low", out_funcs=("join",))
        join = self._make_step("join", is_condition_join=True)
        step_by_name = {"high": high, "low": low, "join": join}
        branches = {"start": frozenset({"high", "low"})}
        chains = _compute_condition_arm_chains(branches, step_by_name)
        assert len(chains["start"]["high"]) == 1
        assert chains["start"]["high"][0].name == "high"
        assert len(chains["start"]["low"]) == 1

    def test_multi_step_arm_has_full_chain(self):
        """high_a → high_b → join: chain for 'high_a' must be [high_a, high_b]."""
        high_a = self._make_step("high_a", out_funcs=("high_b",))
        high_b = self._make_step("high_b", out_funcs=("join",))
        low = self._make_step("low", out_funcs=("join",))
        join = self._make_step("join", is_condition_join=True)
        step_by_name = {"high_a": high_a, "high_b": high_b, "low": low, "join": join}
        branches = {"start": frozenset({"high_a", "low"})}
        chains = _compute_condition_arm_chains(branches, step_by_name)
        high_chain = chains["start"]["high_a"]
        assert [s.name for s in high_chain] == ["high_a", "high_b"]

    def test_join_step_not_included_in_chain(self):
        """The condition-join step itself must not be in any arm chain."""
        high = self._make_step("high", out_funcs=("join",))
        join = self._make_step("join", is_condition_join=True)
        step_by_name = {"high": high, "join": join}
        branches = {"start": frozenset({"high"})}
        chains = _compute_condition_arm_chains(branches, step_by_name)
        assert all(s.name != "join" for s in chains["start"]["high"])


# ---------------------------------------------------------------------------
# _wf_fn: CamelCase → snake_case edge cases
# ---------------------------------------------------------------------------


class TestWfFn:
    def test_simple(self):
        assert _wf_fn("MyFlow") == "my_flow"

    def test_acronym(self):
        assert _wf_fn("MLFlow") == "m_l_flow"

    def test_already_lower(self):
        assert _wf_fn("myflow") == "myflow"

    def test_single_word(self):
        assert _wf_fn("Flow") == "flow"

    def test_numbers_in_name(self):
        """Numbers should not introduce spurious underscores."""
        assert _wf_fn("Flow2023") == "flow2023"

    def test_leading_capital_only(self):
        assert _wf_fn("Flow") == "flow"

    def test_consecutive_caps_produce_underscores(self):
        """MLPipeline → m_l_pipeline (each cap-after-cap gets an underscore)."""
        result = _wf_fn("MLPipeline")
        # The regex inserts _ before every capital that follows another char,
        # so M-L-Pipeline → m_l_pipeline
        assert result == "m_l_pipeline"


# ---------------------------------------------------------------------------
# Compilation tests for new flow shapes (covering codegen branches)
# ---------------------------------------------------------------------------


def _compile(flow_path: Path, out: Path, extra_args: list[str] | None = None) -> None:
    cmd = [
        sys.executable,
        str(flow_path),
        "--no-pylint",
        "--metadata=local",
        "--datastore=local",
        "flyte",
        "create",
        "--output-file", str(out),
        *(extra_args or []),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, (
        f"flyte create failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )


def _read(path: Path) -> str:
    return path.read_text()


class TestTripleNestedForeachRaises:
    """3-level nested foreach must raise NotSupportedException at codegen time."""

    def _make_step(self, name, node_type=NodeType.LINEAR, out_funcs=(), in_funcs=(),
                   is_foreach_join=False, split_parents=()):
        return StepSpec(
            name=name, node_type=node_type, in_funcs=in_funcs, out_funcs=out_funcs,
            split_parents=split_parents, max_user_code_retries=0,
            is_foreach_join=is_foreach_join, is_split_join=False, is_condition_join=False,
            switch_cases=(), timeout_seconds=None, env_vars=(),
        )

    def test_three_level_foreach_raises(self):
        """A flow with outer→mid→inner foreach nesting (3 levels) must raise."""
        from metaflow_extensions.flyte.plugins.flyte._codegen import generate_flyte_file
        from metaflow_extensions.flyte.plugins.flyte._types import FlowSpec, FlyteFlowConfig
        outer = self._make_step("outer", NodeType.FOREACH, out_funcs=("mid",), in_funcs=("start",))
        mid = self._make_step("mid", NodeType.FOREACH, out_funcs=("inner",), in_funcs=("outer",))
        inner = self._make_step("inner", NodeType.LINEAR, out_funcs=("inner_join",), in_funcs=("mid",))
        inner_join = self._make_step("inner_join", NodeType.JOIN, out_funcs=("mid_join",),
                                     in_funcs=("inner",), is_foreach_join=True, split_parents=("mid",))
        mid_join = self._make_step("mid_join", NodeType.JOIN, out_funcs=("outer_join",),
                                   in_funcs=("inner_join",), is_foreach_join=True, split_parents=("outer",))
        start = self._make_step("start", NodeType.FOREACH, out_funcs=("outer",))
        outer_join = self._make_step("outer_join", NodeType.JOIN, out_funcs=("end",),
                                     in_funcs=("mid_join",), is_foreach_join=True, split_parents=("start",))
        end = self._make_step("end", NodeType.END, in_funcs=("outer_join",))
        spec = FlowSpec(
            name="TripleFlow",
            steps=(start, outer, mid, inner, inner_join, mid_join, outer_join, end),
            parameters=(),
        )
        cfg = FlyteFlowConfig(flow_file="/tmp/flow.py", datastore_type="local")
        with pytest.raises(NotSupportedException, match="3\\+"):
            generate_flyte_file(spec, cfg)


# ---------------------------------------------------------------------------
# Retry-count passed to _emit_deck and _read_condition_branch
# ---------------------------------------------------------------------------


class TestRetryCountPropagation:
    def test_emit_deck_receives_retry_count_for_retried_step(self, tmp_path):
        """Steps with @retry must call _emit_deck with attempt=_retry_count."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "retry_flow.py", out)
        content = _read(out)
        # The flaky step has retries > 0, so _retry_count is read from context
        assert "attempt=_retry_count" in content
        # _retry_count must be defined even for non-retried steps
        assert "_retry_count = 0" in content or "_retry_count = current_context" in content

    def test_no_retry_step_still_defines_retry_count(self, tmp_path):
        """Non-retried steps must define _retry_count=0 so _emit_deck compiles."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        content = _read(out)
        assert "_retry_count = 0" in content
        assert "attempt=_retry_count" in content

    def test_condition_branch_passes_retry_count(self, tmp_path):
        """_read_condition_branch must receive attempt=_retry_count."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "condition_flow.py", out)
        content = _read(out)
        assert "_read_condition_branch(" in content
        assert "attempt=_retry_count" in content


# ---------------------------------------------------------------------------
# Resume: --clone-run-id in _step_cmd + METAFLOW_FLYTE_LOCAL_RUN_ID priority
# ---------------------------------------------------------------------------


class TestResumeGeneration:
    def test_step_cmd_includes_clone_run_id(self, tmp_path):
        """Generated _step_cmd must append --clone-run-id when METAFLOW_FLYTE_CLONE_RUN_ID is set."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        content = _read(out)
        assert "METAFLOW_FLYTE_CLONE_RUN_ID" in content
        assert "--clone-run-id" in content

    def test_run_id_task_does_not_return_origin_run_id_directly(self, tmp_path):
        """_mf_generate_run_id must NOT return origin_run_id directly as the run_id.
        Previously `if origin_run_id: return origin_run_id` caused the resume
        pathspec to point to the OLD run rather than the new one."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "linear_flow.py", out)
        content = _read(out)
        # The function must not have `if origin_run_id: return origin_run_id`
        assert "if origin_run_id:" not in content or "return origin_run_id" not in content
        # METAFLOW_FLYTE_LOCAL_RUN_ID must be read and returned when set
        assert "METAFLOW_FLYTE_LOCAL_RUN_ID" in content
        assert "if seeded:" in content


class TestAdversarialCompilation:
    def test_branch_flow_split_join_signature(self, tmp_path):
        """Split/join step must accept one _in_<step> arg per upstream branch."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "branch_flow.py", out)
        content = _read(out)
        # The join step receives inputs from branch_a and branch_b
        assert "_in_branch_a" in content
        assert "_in_branch_b" in content

    def test_mid_foreach_non_start(self, tmp_path):
        """Foreach on a non-start step must emit _foreach_expand_dynamic (not start variant)."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "mid_foreach_flow.py", out)
        content = _read(out)
        assert "_foreach_expand_dynamic" in content
        assert "_step_expand" in content
        # The foreach-as-non-start workflow wires start before expand
        assert "prev_task_id=_tid_start" in content

    def test_mid_condition_non_start(self, tmp_path):
        """Split-switch on a non-start step must route via a non-start conditional router."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "mid_condition_flow.py", out)
        content = _read(out)
        assert "_cond_decide_router" in content
        assert "_step_decide" in content
        # Non-start: the switch call receives prev_task_id from start
        assert "prev_task_id=_tid_start" in content

    def test_timeout_appears_in_decorator(self, tmp_path):
        """A step with @timeout must produce timedelta(...) in the @_mf_task decorator."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "timeout_flow.py", out)
        content = _read(out)
        assert "timedelta" in content

    def test_environment_vars_appear_in_task_body(self, tmp_path):
        """A step with @environment must call _extra_env.update(...) in the task body."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "timeout_flow.py", out)
        content = _read(out)
        assert "_extra_env.update" in content
        assert "MY_VAR" in content

    def test_flow_with_description_includes_docstring(self, tmp_path):
        """Flow docstring must appear as the @workflow function docstring."""
        out = tmp_path / "workflow.py"
        _compile(FLOWS_DIR / "mid_foreach_flow.py", out)
        content = _read(out)
        assert "Foreach on a non-start step" in content

    def test_generated_file_is_valid_python(self, tmp_path):
        """Every generated file must be parseable as Python."""
        import ast

        for flow_file in [
            "branch_flow.py",
            "mid_foreach_flow.py",
            "mid_condition_flow.py",
            "timeout_flow.py",
        ]:
            out = tmp_path / f"{flow_file}_workflow.py"
            _compile(FLOWS_DIR / flow_file, out)
            src = out.read_text()
            try:
                ast.parse(src)
            except SyntaxError as e:
                pytest.fail(f"Generated code for {flow_file} has a syntax error: {e}")
