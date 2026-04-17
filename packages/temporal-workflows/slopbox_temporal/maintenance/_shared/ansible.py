"""Run Ansible playbooks from Temporal activities via ``ansible-runner``.

Why ``ansible-runner`` instead of a hand-rolled subprocess: it already solves
the non-trivial parts — structured per-task event stream, an artifact
directory (stdout, events, status, rc persisted to disk), cancellation
(``runner.canceled = True``), and clean separation of run state from the
worker's own working directory.  Re-implementing any of that would be wasted
effort, and we get the same machinery AWX and Ansible Automation Platform
use at runtime.

Execution model
---------------
Each call to :func:`run_playbook` builds a self-contained *private data
directory* in ``$TMPDIR`` with this shape::

    /tmp/slopbox-ansible-XXXX/
    ├── inventory/
    │   └── hosts            # rendered from the `inventory` string argument
    └── project              # symlink to config.playbook_repo_path()

``ansible_runner.interface.run_async`` forks ``ansible-playbook`` in a
daemon thread and returns immediately.  We consume ``runner.events`` on a
worker thread (``asyncio.to_thread``) so the asyncio event loop stays
responsive, and we heartbeat Temporal on every event plus every 15 s as a
floor — so a playbook with long-running tasks still keeps the activity
alive.

Cancellation
------------
``asyncio.CancelledError`` (which Temporal raises when the activity is
cancelled or the worker is shutting down) sets ``runner.canceled = True``
and awaits the runner thread before re-raising.  Ansible-runner translates
this into a SIGTERM to the child ``ansible-playbook`` process.

Artifact directory
------------------
The private data dir is not cleaned up automatically — its contents are the
primary forensic artefact when a playbook fails.  Operators should rotate
``$TMPDIR`` (or set ``TMPDIR`` to a dedicated mount with rotation) per the
worker deployment's policy.  The path is returned on the :class:`AnsibleResult`
and logged on every run.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .config import playbook_repo_path

logger = logging.getLogger("slopbox_temporal.maintenance.ansible")

# Heartbeat floor: even if ansible-runner emits no events for a while
# (e.g. a long-running ``wait_for`` task), Temporal still sees the activity
# as alive.  Callers should set ``heartbeat_timeout`` to something comfortably
# larger than this — 60 s is a sensible default.
_HEARTBEAT_FLOOR_SECONDS = 15

# Event names that indicate a task-level failure — tracked so the workflow
# can surface which task(s) failed without reading log files.
_FAILURE_EVENTS = frozenset({"runner_on_failed", "runner_on_unreachable"})


@dataclass
class AnsibleResult:
    rc: int
    status: str            # successful / failed / timeout / canceled / ...
    duration_seconds: float
    artifact_dir: Path     # private_data_dir, for postmortem inspection
    failed_tasks: list[str] = field(default_factory=list)


async def run_playbook(
    playbook: str,
    inventory: str,
    extra_vars: dict[str, Any] | None = None,
    *,
    dry_run: bool,
) -> AnsibleResult:
    """Run ``playbook`` against the single-host ``inventory``.

    ``dry_run`` maps to Ansible ``--check`` mode so tasks run but make no
    changes.  Non-retryable exceptions are raised for problems retries can't
    fix: the playbook repo is missing, the playbook file doesn't exist, or
    the env var pointing at the repo is unset.  Everything else propagates
    through :class:`ApplicationError` and is subject to the activity's
    retry policy.
    """
    # Import lazily so unit tests that don't need ansible-runner can still
    # import this module (e.g. to check that the activity symbol exists).
    import ansible_runner

    repo = playbook_repo_path()
    playbook_abs = repo / playbook
    if not playbook_abs.is_file():
        raise ApplicationError(
            f"playbook not found: {playbook_abs}",
            non_retryable=True,
        )

    private_data_dir = Path(tempfile.mkdtemp(prefix="slopbox-ansible-"))
    (private_data_dir / "inventory").mkdir()
    (private_data_dir / "inventory" / "hosts").write_text(inventory)
    (private_data_dir / "project").symlink_to(repo, target_is_directory=True)

    label = "[dry-run] " if dry_run else ""
    logger.info(
        "%srunning ansible-playbook %s (private_data_dir=%s)",
        label,
        playbook,
        private_data_dir,
    )

    started = time.monotonic()
    thread, runner = await asyncio.to_thread(
        ansible_runner.interface.run_async,
        private_data_dir=str(private_data_dir),
        playbook=playbook,
        extravars=extra_vars or {},
        check=dry_run,
        quiet=True,
    )

    failed_tasks: list[str] = []
    heartbeat_task = asyncio.create_task(_heartbeat_floor(runner))
    try:
        while True:
            event = await asyncio.to_thread(_next_event, runner.events)
            if event is None:
                break
            _heartbeat(runner, event)
            if event.get("event") in _FAILURE_EVENTS:
                failed_tasks.append(_format_failed_task(event))
        # The events generator returns once ansible-runner marks the run as
        # finished — join the runner thread so we observe the final status.
        await asyncio.to_thread(thread.join)
    except asyncio.CancelledError:
        logger.warning(
            "activity cancelled; signalling ansible-runner to stop (playbook=%s)",
            playbook,
        )
        runner.canceled = True
        await asyncio.to_thread(thread.join)
        raise
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

    duration = time.monotonic() - started
    result = AnsibleResult(
        rc=runner.rc or 0,
        status=runner.status,
        duration_seconds=duration,
        artifact_dir=private_data_dir,
        failed_tasks=failed_tasks,
    )

    if runner.status == "successful":
        logger.info(
            "playbook %s succeeded in %.1fs (rc=%d)",
            playbook,
            duration,
            result.rc,
        )
        return result
    if runner.status == "canceled":
        # Cancellation path already re-raised CancelledError above; reaching
        # this branch means ansible-runner reported "canceled" without an
        # asyncio cancellation — treat it as a retryable failure.
        raise ApplicationError(
            f"playbook {playbook} reported canceled status: {failed_tasks}"
        )
    if runner.status == "timeout":
        raise ApplicationError(
            f"playbook {playbook} timed out after {duration:.0f}s"
        )
    raise ApplicationError(
        f"playbook {playbook} failed (status={runner.status}, rc={result.rc}): "
        f"{failed_tasks or '<no task-level failures parsed from event stream>'}"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _next_event(events_iter: Any) -> dict | None:
    """Pull the next event off the runner's iterator; return ``None`` at end."""
    try:
        return next(events_iter)
    except StopIteration:
        return None


def _heartbeat(runner: Any, event: dict) -> None:
    """Heartbeat with the latest event uuid + runner status.

    Wrapped in ``try/except`` so tests that call ``run_playbook`` outside
    an activity context don't blow up — ``activity.heartbeat`` raises
    outside an activity.
    """
    try:
        activity.heartbeat(
            {
                "last_event_uuid": event.get("uuid"),
                "event": event.get("event"),
                "status": runner.status,
            }
        )
    except Exception:  # pragma: no cover - non-activity context in tests
        pass


async def _heartbeat_floor(runner: Any) -> None:
    """Periodically heartbeat with runner status even if no events fire."""
    while True:
        await asyncio.sleep(_HEARTBEAT_FLOOR_SECONDS)
        try:
            activity.heartbeat({"status": runner.status})
        except Exception:  # pragma: no cover - non-activity context in tests
            return


def _format_failed_task(event: dict) -> str:
    ed = event.get("event_data", {}) or {}
    host = ed.get("host", "<unknown-host>")
    task = ed.get("task", ed.get("task_action", "<unknown-task>"))
    reason = ed.get("res", {}).get("msg") if isinstance(ed.get("res"), dict) else None
    if reason:
        return f"{host}: {task} — {reason}"
    return f"{host}: {task}"
