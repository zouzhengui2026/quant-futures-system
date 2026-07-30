"""Filesystem-facing lifecycle controls and non-authoritative status projection."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

from .lifecycle import Lifecycle, LifecycleError, LifecycleRecord, LifecycleState
from .lock import RunDirectoryLock


def _atomic_projection(path: Path, value: dict[str, object]) -> None:
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def project_status(run_directory: str | Path) -> dict[str, object]:
    """Rebuild status exclusively from authoritative persisted lifecycle state."""
    record = Lifecycle(run_directory).current()
    return {"schema_version": 1, "authoritative": False, "authority": Lifecycle.filename,
            "run_id": record.run_id, "lifecycle": record.state.value,
            "lifecycle_sequence": record.sequence, "last_reason": record.reason,
            "input_cursor": 0, "journal_sequence": 0, "pending_order": None,
            "positions": [], "equity": None, "risk_outcome": None,
            "counters": {"inputs": 0, "orders": 0, "fills": 0}}


def write_status(run_directory: str | Path) -> dict[str, object]:
    status = project_status(run_directory)
    _atomic_projection(Path(run_directory) / "status.json", status)
    return status


def start(root: str | Path) -> Path:
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    directory = root / run_id
    directory.mkdir(mode=0o700)
    with RunDirectoryLock(directory):
        lifecycle = Lifecycle(directory)
        lifecycle.initialize(run_id)
        lifecycle.transition(LifecycleState.STARTING, "start requested")
        lifecycle.transition(LifecycleState.RUNNING, "checkpoint-one control plane initialized")
        write_status(directory)
    return directory


def transition(run_directory: str | Path, target: LifecycleState, reason: str) -> LifecycleRecord:
    with RunDirectoryLock(run_directory):
        record = Lifecycle(run_directory).transition(target, reason)
        write_status(run_directory)
        return record


def stop(run_directory: str | Path) -> LifecycleRecord:
    with RunDirectoryLock(run_directory):
        lifecycle = Lifecycle(run_directory)
        lifecycle.transition(LifecycleState.STOPPING, "stop requested")
        record = lifecycle.transition(LifecycleState.COMPLETED, "checkpoint-one control plane stopped")
        write_status(run_directory)
        return record


def recover(run_directory: str | Path) -> LifecycleRecord:
    with RunDirectoryLock(run_directory):
        lifecycle = Lifecycle(run_directory)
        lifecycle.transition(LifecycleState.RECOVERING, "recovery requested")
        record = lifecycle.transition(LifecycleState.RUNNING, "lifecycle authority validated")
        write_status(run_directory)
        return record


def audit(run_directory: str | Path) -> bool:
    """Validate lifecycle authority and require the projection to match it exactly."""
    expected = project_status(run_directory)
    try:
        actual = json.loads((Path(run_directory) / "status.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    return actual == expected
