"""Validated, persisted Paper Runtime lifecycle authority.

The lifecycle log is the checkpoint-one authority.  ``status.json`` is only a
replaceable projection and is never consulted when deciding a transition.
Checkpoint two will replace this deliberately small JSON-lines log with the
framed transition journal.
"""

from __future__ import annotations

import errno
import json
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Iterator

from .lock import RunDirectoryLock


class LifecycleState(StrEnum):
    CREATED = "CREATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    COMPLETED = "COMPLETED"
    FAILED_RECOVERABLE = "FAILED_RECOVERABLE"
    RECOVERING = "RECOVERING"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class LifecycleError(ValueError):
    """The persisted lifecycle is malformed or a transition is illegal."""


LEGAL_TRANSITIONS: frozenset[tuple[LifecycleState, LifecycleState]] = frozenset({
    (LifecycleState.CREATED, LifecycleState.STARTING),
    (LifecycleState.STARTING, LifecycleState.RUNNING),
    (LifecycleState.STARTING, LifecycleState.FAILED_RECOVERABLE),
    (LifecycleState.RUNNING, LifecycleState.PAUSED),
    (LifecycleState.RUNNING, LifecycleState.STOPPING),
    (LifecycleState.RUNNING, LifecycleState.FAILED_RECOVERABLE),
    (LifecycleState.PAUSED, LifecycleState.RUNNING),
    (LifecycleState.PAUSED, LifecycleState.STOPPING),
    (LifecycleState.PAUSED, LifecycleState.FAILED_RECOVERABLE),
    (LifecycleState.STOPPING, LifecycleState.COMPLETED),
    (LifecycleState.STOPPING, LifecycleState.FAILED_RECOVERABLE),
    (LifecycleState.FAILED_RECOVERABLE, LifecycleState.RECOVERING),
    (LifecycleState.FAILED_RECOVERABLE, LifecycleState.FAILED_TERMINAL),
    (LifecycleState.RECOVERING, LifecycleState.RUNNING),
    (LifecycleState.RECOVERING, LifecycleState.FAILED_RECOVERABLE),
    (LifecycleState.RECOVERING, LifecycleState.FAILED_TERMINAL),
})


@dataclass(frozen=True)
class LifecycleRecord:
    schema_version: int
    run_id: str
    sequence: int
    previous_state: LifecycleState | None
    state: LifecycleState
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {"schema_version": self.schema_version, "run_id": self.run_id,
                "sequence": self.sequence,
                "previous_state": self.previous_state.value if self.previous_state else None,
                "state": self.state.value, "reason": self.reason}


class Lifecycle:
    """Read and append validated lifecycle records in one run directory."""

    filename = "lifecycle.jsonl"

    def __init__(self, run_directory: str | Path) -> None:
        self.run_directory = Path(run_directory)
        self.path = self.run_directory / self.filename

    def initialize(self, run_id: str) -> LifecycleRecord:
        with RunDirectoryLock(self.run_directory):
            return self._initialize_held(run_id)

    def _initialize_held(self, run_id: str) -> LifecycleRecord:
        """Initialize authority while the caller holds the run-directory lock."""
        if not run_id or not self.run_directory.is_dir():
            raise LifecycleError("a non-empty run ID and existing run directory are required")
        if self.path.exists():
            raise LifecycleError("lifecycle authority already exists")
        record = LifecycleRecord(1, run_id, 1, None, LifecycleState.CREATED, "run created")
        self._append(record, exclusive=True)
        return record

    def records(self) -> tuple[LifecycleRecord, ...]:
        return tuple(self._read_records())

    def current(self) -> LifecycleRecord:
        records = self.records()
        if not records:
            raise LifecycleError("lifecycle authority is missing or empty")
        return records[-1]

    def transition(self, target: LifecycleState, reason: str) -> LifecycleRecord:
        with RunDirectoryLock(self.run_directory):
            return self._transition_held(target, reason)

    def _transition_held(self, target: LifecycleState, reason: str) -> LifecycleRecord:
        """Append a transition while the caller holds the run-directory lock."""
        current = self.current()
        if (current.state, target) not in LEGAL_TRANSITIONS:
            raise LifecycleError(f"illegal lifecycle transition: {current.state.value} -> {target.value}")
        if not reason.strip():
            raise LifecycleError("transition reason must not be empty")
        record = LifecycleRecord(1, current.run_id, current.sequence + 1,
                                 current.state, target, reason.strip())
        self._append(record)
        return record

    def _read_records(self) -> Iterator[LifecycleRecord]:
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeDecodeError) as exc:
            raise LifecycleError(f"cannot read lifecycle authority: {exc}") from exc
        previous: LifecycleRecord | None = None
        for index, line in enumerate(lines, 1):
            try:
                raw = json.loads(line)
                if set(raw) != {"schema_version", "run_id", "sequence", "previous_state", "state", "reason"}:
                    raise ValueError("unexpected lifecycle fields")
                record = LifecycleRecord(raw["schema_version"], raw["run_id"], raw["sequence"],
                    LifecycleState(raw["previous_state"]) if raw["previous_state"] else None,
                    LifecycleState(raw["state"]), raw["reason"])
            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                raise LifecycleError(f"invalid lifecycle record at line {index}: {exc}") from exc
            if (record.schema_version != 1 or not isinstance(record.run_id, str) or not record.run_id
                    or not isinstance(record.sequence, int) or isinstance(record.sequence, bool)
                    or not isinstance(record.reason, str) or not record.reason):
                raise LifecycleError(f"invalid lifecycle record at line {index}")
            if previous is None:
                valid = record.sequence == 1 and record.previous_state is None and record.state is LifecycleState.CREATED
            else:
                valid = (record.run_id == previous.run_id and record.sequence == previous.sequence + 1
                         and record.previous_state is previous.state
                         and (previous.state, record.state) in LEGAL_TRANSITIONS)
            if not valid:
                raise LifecycleError(f"invalid lifecycle lineage at line {index}")
            previous = record
            yield record

    def _append(self, record: LifecycleRecord, *, exclusive: bool = False) -> None:
        flags = os.O_WRONLY | os.O_CREAT | (os.O_EXCL if exclusive else os.O_APPEND)
        try:
            descriptor = os.open(self.path, flags, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(json.dumps(record.as_dict(), sort_keys=True, separators=(",", ":")) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            if exclusive:
                _fsync_directory(self.run_directory)
        except OSError as exc:
            raise LifecycleError(f"cannot persist lifecycle transition: {exc}") from exc


def _fsync_directory(directory: Path) -> bool:
    """Make a newly created directory entry durable when the platform permits.

    Some platforms cannot open or fsync directory descriptors.  Those explicit
    unsupported-operation cases safely fall back to the already-fsynced file;
    other I/O failures remain fatal rather than claiming durability.
    """
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(directory, flags)
        os.fsync(descriptor)
    except OSError as exc:
        unsupported = {errno.EACCES, errno.EINVAL, errno.ENOTSUP}
        if exc.errno not in unsupported:
            raise
        return False
    finally:
        if descriptor is not None:
            os.close(descriptor)
    return True
