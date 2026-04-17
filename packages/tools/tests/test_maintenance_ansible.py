"""Tests for slopbox_temporal.maintenance._shared.ansible.run_playbook.

Fakes ``ansible_runner.interface.run_async`` with a ``_FakeRunner`` whose
``.events`` iterator yields a scripted event sequence and whose ``.status``
is set to the terminal value at the end of the run.  Verifies:

* private_data_dir is built correctly (inventory/hosts, project symlink)
* ``check=True`` is passed through on dry-run
* ``failed_tasks`` are parsed from the event stream
* terminal statuses map to the expected outcomes (return / raise)
* activity cancellation propagates to ``runner.canceled = True``
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from temporalio.exceptions import ApplicationError

from slopbox_temporal.maintenance._shared import ansible as ansible_mod
from slopbox_temporal.maintenance._shared.ansible import AnsibleResult, run_playbook


# ---------------------------------------------------------------------------
# Fake ansible-runner
# ---------------------------------------------------------------------------


class _FakeRunner:
    def __init__(self, events: list[dict], terminal_status: str, rc: int = 0) -> None:
        self._events = iter(events)
        self.status = "running"
        self._terminal_status = terminal_status
        self.rc = rc
        self.canceled = False

    @property
    def events(self):  # consumed by run_playbook's `_next_event`
        return self

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._events)
        except StopIteration:
            # Mark the run terminal once the event stream is drained.
            self.status = self._terminal_status
            raise


class _FakeThread:
    def __init__(self) -> None:
        self._joined = False

    def join(self) -> None:
        self._joined = True


def _fake_run_async_factory(
    events: list[dict],
    terminal_status: str,
    rc: int = 0,
    recorder: dict | None = None,
):
    def _run_async(**kwargs: Any):
        if recorder is not None:
            recorder.update(kwargs)
        return _FakeThread(), _FakeRunner(events, terminal_status, rc=rc)

    return _run_async


# ---------------------------------------------------------------------------
# Shared fixture: writable playbook repo with one playbook
# ---------------------------------------------------------------------------


@pytest.fixture
def playbook_repo(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    repo = tmp_path / "playbooks"
    repo.mkdir()
    (repo / "site.yml").write_text("- hosts: target\n  tasks: []\n")
    monkeypatch.setenv("MAINTENANCE_PLAYBOOK_REPO_PATH", str(repo))
    return repo


INVENTORY = "[target]\nnode-01 ansible_host=10.0.0.5\n"


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_playbook_successful_run(playbook_repo: Path) -> None:
    events = [
        {"uuid": "e1", "event": "playbook_on_start", "event_data": {}},
        {"uuid": "e2", "event": "runner_on_ok", "event_data": {"host": "node-01", "task": "Gather facts"}},
    ]
    recorder: dict = {}
    with patch(
        "ansible_runner.interface.run_async",
        new=_fake_run_async_factory(events, terminal_status="successful", recorder=recorder),
    ):
        result = await run_playbook(
            playbook="site.yml",
            inventory=INVENTORY,
            extra_vars={"node_name": "node-01"},
            dry_run=False,
        )
    assert isinstance(result, AnsibleResult)
    assert result.status == "successful"
    assert result.rc == 0
    assert result.failed_tasks == []
    # private_data_dir has the expected shape
    pdd = Path(recorder["private_data_dir"])
    assert (pdd / "inventory" / "hosts").read_text() == INVENTORY
    assert (pdd / "project").resolve() == playbook_repo.resolve()
    assert result.artifact_dir == pdd
    # check mode is off on real runs
    assert recorder["check"] is False
    assert recorder["extravars"] == {"node_name": "node-01"}


@pytest.mark.asyncio
async def test_run_playbook_dry_run_sets_check_true(playbook_repo: Path) -> None:
    recorder: dict = {}
    with patch(
        "ansible_runner.interface.run_async",
        new=_fake_run_async_factory([], terminal_status="successful", recorder=recorder),
    ):
        await run_playbook(
            playbook="site.yml",
            inventory=INVENTORY,
            dry_run=True,
        )
    assert recorder["check"] is True


# ---------------------------------------------------------------------------
# Failure parsing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_playbook_failed_status_raises_with_parsed_tasks(
    playbook_repo: Path,
) -> None:
    events = [
        {"uuid": "e1", "event": "playbook_on_start", "event_data": {}},
        {
            "uuid": "e2",
            "event": "runner_on_failed",
            "event_data": {
                "host": "node-01",
                "task": "Cordon node",
                "res": {"msg": "kubectl: command not found"},
            },
        },
        {
            "uuid": "e3",
            "event": "runner_on_unreachable",
            "event_data": {"host": "node-02", "task": "Ping"},
        },
    ]
    with patch(
        "ansible_runner.interface.run_async",
        new=_fake_run_async_factory(events, terminal_status="failed", rc=2),
    ):
        with pytest.raises(ApplicationError) as exc_info:
            await run_playbook(
                playbook="site.yml",
                inventory=INVENTORY,
                dry_run=False,
            )
    msg = str(exc_info.value)
    assert "site.yml" in msg
    assert "Cordon node" in msg
    assert "kubectl: command not found" in msg
    assert "node-02" in msg
    # failed playbook is retryable by default — don't mark non_retryable
    assert exc_info.value.non_retryable is False


@pytest.mark.asyncio
async def test_run_playbook_timeout_status_raises(playbook_repo: Path) -> None:
    with patch(
        "ansible_runner.interface.run_async",
        new=_fake_run_async_factory([], terminal_status="timeout", rc=124),
    ):
        with pytest.raises(ApplicationError) as exc_info:
            await run_playbook(
                playbook="site.yml",
                inventory=INVENTORY,
                dry_run=False,
            )
    assert "timed out" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Missing playbook — non-retryable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_playbook_missing_file_is_non_retryable(playbook_repo: Path) -> None:
    with pytest.raises(ApplicationError) as exc_info:
        await run_playbook(
            playbook="does-not-exist.yml",
            inventory=INVENTORY,
            dry_run=False,
        )
    assert exc_info.value.non_retryable is True
    assert "does-not-exist.yml" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


class _BlockingRunner(_FakeRunner):
    """Never finishes until ``release()`` is called."""

    def __init__(self, terminal_status: str = "canceled") -> None:
        super().__init__(events=[], terminal_status=terminal_status)
        self._released = threading.Event()

    def __next__(self):
        # Block until released; then behave like the base (terminal on exhaustion).
        self._released.wait(timeout=5)
        return super().__next__()

    def release(self) -> None:
        self._released.set()


@pytest.mark.asyncio
async def test_run_playbook_cancellation_sets_canceled_flag(
    playbook_repo: Path,
) -> None:
    blocking = _BlockingRunner()

    def _run_async(**_kwargs):
        return _FakeThread(), blocking

    with patch("ansible_runner.interface.run_async", new=_run_async):
        task = asyncio.create_task(
            run_playbook(
                playbook="site.yml",
                inventory=INVENTORY,
                dry_run=False,
            )
        )
        # Let the coroutine start and enter the events loop.
        await asyncio.sleep(0.05)
        task.cancel()
        blocking.release()  # let the thread-off next() return so cleanup can complete
        with pytest.raises(asyncio.CancelledError):
            await task

    assert blocking.canceled is True


# ---------------------------------------------------------------------------
# Heartbeat floor cancels cleanly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_floor_constant_is_reasonable() -> None:
    # Guard against someone accidentally setting this to 0 (busy loop) or a
    # giant value (defeats the purpose).
    assert 1 <= ansible_mod._HEARTBEAT_FLOOR_SECONDS <= 60
