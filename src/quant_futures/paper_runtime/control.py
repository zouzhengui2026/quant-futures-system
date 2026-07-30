"""Filesystem-facing lifecycle controls and non-authoritative status projection."""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from pathlib import Path

from .lifecycle import Lifecycle, LifecycleError, LifecycleRecord, LifecycleState
from .journal import JournalError, TransitionJournal
from .lock import RunDirectoryLock


def _atomic_projection(path: Path, value: dict[str, object]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def project_status(run_directory: str | Path) -> dict[str, object]:
    """Rebuild a lock-consistent snapshot from both persisted authorities."""
    directory = Path(run_directory)
    with RunDirectoryLock(directory):
        return _project_status_held(Lifecycle(directory))


def _project_status_held(lifecycle: Lifecycle) -> dict[str, object]:
    record = lifecycle.current()
    journal_tail = TransitionJournal(lifecycle.run_directory).tail()
    if journal_tail is not None and journal_tail.run_id != record.run_id:
        raise JournalError("journal run ID does not match lifecycle authority")
    return _status_for_record(record, journal_tail)


def write_status(run_directory: str | Path) -> dict[str, object]:
    with RunDirectoryLock(run_directory):
        return _write_status_held(Lifecycle(run_directory))


def _write_status_held(lifecycle: Lifecycle) -> dict[str, object]:
    status = _project_status_held(lifecycle)
    _atomic_projection(lifecycle.run_directory / "status.json", status)
    return status


def _status_for_record(record: LifecycleRecord, journal_tail: object | None = None) -> dict[str, object]:
    return {"schema_version": 1, "authoritative": False, "authority": Lifecycle.filename,
            "run_id": record.run_id, "lifecycle": record.state.value,
            "lifecycle_sequence": record.sequence, "last_reason": record.reason,
            "input_cursor": getattr(journal_tail, "input_cursor", 0),
            "journal_sequence": getattr(journal_tail, "sequence", 0),
            "journal_tail_digest": getattr(journal_tail, "digest", None),
            "pending_order": None,
            "positions": [], "equity": None, "risk_outcome": None,
            "counters": {"inputs": 0, "orders": 0, "fills": 0}}


def start(root: str | Path) -> Path:
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    directory = root / run_id
    directory.mkdir(mode=0o700)
    lifecycle = Lifecycle(directory)
    with RunDirectoryLock(directory):
        lifecycle._initialize_held(run_id)
        lifecycle._transition_held(LifecycleState.STARTING, "start requested")
        lifecycle._transition_held(LifecycleState.RUNNING, "checkpoint-one control plane initialized")
        _write_status_held(lifecycle)
    return directory


def transition(run_directory: str | Path, target: LifecycleState, reason: str) -> LifecycleRecord:
    lifecycle = Lifecycle(run_directory)
    with RunDirectoryLock(run_directory):
        record = lifecycle._transition_held(target, reason)
        _write_status_held(lifecycle)
        return record


def stop(run_directory: str | Path) -> LifecycleRecord:
    lifecycle = Lifecycle(run_directory)
    with RunDirectoryLock(run_directory):
        lifecycle._transition_held(LifecycleState.STOPPING, "stop requested")
        record = lifecycle._transition_held(
            LifecycleState.COMPLETED, "checkpoint-one control plane stopped"
        )
        _write_status_held(lifecycle)
        return record


def recover(run_directory: str | Path) -> LifecycleRecord:
    lifecycle = Lifecycle(run_directory)
    with RunDirectoryLock(run_directory):
        journal = TransitionJournal(run_directory)
        journal_tail = journal._repair_tail_held()
        current = lifecycle.current()
        if journal_tail is not None and journal_tail.run_id != current.run_id:
            raise JournalError("journal run ID does not match lifecycle authority")
        lifecycle._transition_held(LifecycleState.RECOVERING, "recovery requested")
        record = lifecycle._transition_held(LifecycleState.RUNNING, "lifecycle authority validated")
        _write_status_held(lifecycle)
        return record


def audit(run_directory: str | Path) -> bool:
    """Validate lifecycle authority and require the projection to match it exactly."""
    try:
        with RunDirectoryLock(run_directory):
            expected = _project_status_held(Lifecycle(run_directory))
            actual = json.loads((Path(run_directory) / "status.json").read_text(encoding="utf-8"))
    except (LifecycleError, JournalError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return actual == expected
