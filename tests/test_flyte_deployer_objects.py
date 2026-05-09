"""Unit tests for flyte_deployer_objects — the synthetic Run/Step/Task objects
returned to deployer test code when Metaflow metadata is not accessible from
the test runner (typical for remote Flyte execution in CI)."""

from __future__ import annotations

from metaflow_extensions.flyte.plugins.flyte.flyte_deployer_objects import (
    _RemoteFlowRun,
    _RemoteStep,
)


class _StubItem:
    def __init__(self, path: str) -> None:
        self.path = path


class _StubStorage:
    """Minimal stand-in for a Metaflow storage_impl. Returns canned listings."""

    def __init__(self, items: list[str]) -> None:
        self._items = [_StubItem(p) for p in items]

    @staticmethod
    def path_join(*parts: str) -> str:
        return "/".join(p.strip("/") for p in parts if p)

    def list_content(self, prefixes):
        return list(self._items)


class _StubFDS:
    def __init__(self, storage_impl) -> None:
        self._storage_impl = storage_impl


class TestRemoteStepId:
    def test_id_returns_step_name(self):
        step = _RemoteStep(
            flow_datastore=_StubFDS(_StubStorage([])),
            flow_name="MyFlow",
            run_id="flyte-abc",
            step_name="entry",
        )
        assert step.id == "entry"


class TestRemoteFlowRunIter:
    """Discovery of step names from S3 prefixes. Required for assertions like
    ``{step.id for step in run}`` against Flyte-executed runs."""

    @staticmethod
    def _build_run(items: list[str]) -> _RemoteFlowRun:
        run = _RemoteFlowRun(
            pathspec="MyFlow/flyte-abc",
            env_vars={"METAFLOW_DATASTORE_SYSROOT_S3": "s3://bucket/meta"},
        )
        # Inject a stub FDS so __iter__/_get_fds skips the real S3 connect.
        run._fds = _StubFDS(_StubStorage(items))  # type: ignore[attr-defined]
        return run

    def test_iter_yields_unique_step_names(self):
        items = [
            "MyFlow/flyte-abc/entry/task-1/_task_ok",
            "MyFlow/flyte-abc/entry/task-1/result.pkl",
            "MyFlow/flyte-abc/left/task-2/_task_ok",
            "MyFlow/flyte-abc/right/task-3/_task_ok",
            "MyFlow/flyte-abc/merge/task-4/_task_ok",
            "MyFlow/flyte-abc/done/task-5/_task_ok",
        ]
        run = self._build_run(items)
        step_names = {step.id for step in run}
        assert step_names == {"entry", "left", "right", "merge", "done"}

    def test_iter_skips_parameters_pseudo_step(self):
        items = [
            "MyFlow/flyte-abc/_parameters/task-0/data",
            "MyFlow/flyte-abc/start/task-1/_task_ok",
            "MyFlow/flyte-abc/end/task-2/_task_ok",
        ]
        run = self._build_run(items)
        step_names = {step.id for step in run}
        assert step_names == {"start", "end"}

    def test_iter_returns_empty_when_fds_unavailable(self):
        run = _RemoteFlowRun(
            pathspec="MyFlow/flyte-abc",
            env_vars={},  # no METAFLOW_DATASTORE_SYSROOT_S3
        )
        assert list(run) == []
