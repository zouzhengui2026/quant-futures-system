"""Boundary-safe operational controls for the finite Paper runtime.

Operational requests are deliberately separate from trading authority.  A
writer records a request atomically; only the runtime loop, between product
transitions, is allowed to translate it into lifecycle authority.
"""
from __future__ import annotations

import hashlib
import json
import os
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Iterable

from quant_futures.product.data import Bar

from .control import _atomic_projection, _write_status_held
from .lifecycle import Lifecycle, LifecycleState, _fsync_directory
from .lock import RunDirectoryLock
from .transition import PaperTransitionCoordinator, PaperTransitionState


class OperationalError(RuntimeError):
    """An operational request or runtime state is invalid."""


@dataclass(frozen=True, slots=True)
class RuntimeResult:
    processed: int
    stopped: bool
    state: PaperTransitionState


class StopFlag:
    """Signal-safe process-local stop flag.

    The handler intentionally performs no I/O, locking, logging, or lifecycle
    mutation.  The runtime consumes the flag at its next committed boundary.
    """

    def __init__(self) -> None:
        self.event = Event()

    def handler(self, _signum: int, _frame: object) -> None:
        self.event.set()

    def install(self) -> None:
        signal.signal(signal.SIGINT, self.handler)
        signal.signal(signal.SIGTERM, self.handler)


class OperationalRequests:
    filename = "control-request.json"

    def __init__(self, run_directory: str | Path) -> None:
        self.run_directory = Path(run_directory)
        self.path = self.run_directory / self.filename

    def request(self, command: str) -> dict[str, object]:
        if command not in {"pause", "resume", "stop"}:
            raise OperationalError(f"unsupported operational request: {command}")
        with RunDirectoryLock(self.run_directory):
            current = self._read_held()
            sequence = int(current.get("sequence", 0)) + 1 if current else 1
            value = {"schema_version": 1, "sequence": sequence, "command": command}
            _atomic_projection(self.path, value)
            _fsync_directory(self.run_directory)
            return value

    def _read_held(self) -> dict[str, object] | None:
        if not self.path.exists():
            return None
        try:
            value = json.loads(self.path.read_bytes())
        except (OSError, json.JSONDecodeError) as exc:
            raise OperationalError(f"invalid operational request: {exc}") from exc
        if (not isinstance(value, dict) or set(value) != {"schema_version", "sequence", "command"}
                or value["schema_version"] != 1 or type(value["sequence"]) is not int
                or value["sequence"] < 1 or value["command"] not in {"pause", "resume", "stop"}):
            raise OperationalError("invalid operational request authority")
        return value


class PaperRuntime:
    """Finite one-event-at-a-time service around the domain coordinator."""

    def __init__(self, coordinator: PaperTransitionCoordinator, *, pace_seconds: float = 0,
                 stop_flag: StopFlag | None = None) -> None:
        if not isinstance(pace_seconds, (int, float)) or pace_seconds < 0:
            raise OperationalError("pace must be non-negative")
        self.coordinator = coordinator
        self.pace_seconds = float(pace_seconds)
        self.stop_flag = stop_flag or StopFlag()
        self.requests = OperationalRequests(coordinator.journal.run_directory)
        self._seen_request = 0

    def run(self, bars: Iterable[Bar]) -> RuntimeResult:
        processed = 0
        for bar in bars:
            if self._at_boundary():
                return RuntimeResult(processed, True, self.coordinator.state)
            self.coordinator.transition(bar)
            processed += 1
            if self.pace_seconds:
                time.sleep(self.pace_seconds)
        stopped = self._at_boundary(final=True)
        return RuntimeResult(processed, stopped, self.coordinator.state)

    def _at_boundary(self, *, final: bool = False) -> bool:
        directory = self.coordinator.journal.run_directory
        while True:
            with RunDirectoryLock(directory):
                lifecycle = Lifecycle(directory)
                record = lifecycle.current()
                request = self.requests._read_held()
                command = None
                if request and int(request["sequence"]) > self._seen_request:
                    self._seen_request = int(request["sequence"])
                    command = str(request["command"])
                if self.stop_flag.event.is_set():
                    command = "stop"
                if command == "stop" or final:
                    if record.state in {LifecycleState.RUNNING, LifecycleState.PAUSED}:
                        lifecycle._transition_held(LifecycleState.STOPPING, "boundary-safe stop requested")
                        checkpoint_exists = (directory / "checkpoint.json").is_file()
                        lifecycle._transition_held(
                            LifecycleState.COMPLETED,
                            "final checkpoint committed" if checkpoint_exists
                            else "input completed without a committed product checkpoint")
                    _write_status_held(lifecycle, self.coordinator.state.journal_snapshot)
                    return True
                if command == "pause" and record.state is LifecycleState.RUNNING:
                    lifecycle._transition_held(LifecycleState.PAUSED, "boundary-safe pause requested")
                    _write_status_held(lifecycle, self.coordinator.state.journal_snapshot)
                elif command == "resume" and record.state is LifecycleState.PAUSED:
                    lifecycle._transition_held(LifecycleState.RUNNING, "resume requested")
                    _write_status_held(lifecycle, self.coordinator.state.journal_snapshot)
                paused = lifecycle.current().state is LifecycleState.PAUSED
            if not paused:
                return False
            time.sleep(min(self.pace_seconds or .01, .1))


class RecoveryAttempts:
    """Small hash-chained authority for monotonic recovery accounting."""

    filename = "recovery-attempts.jsonl"

    def __init__(self, run_directory: str | Path) -> None:
        self.run_directory = Path(run_directory)
        self.path = self.run_directory / self.filename

    def append(self, run_id: str, outcome: str = "started") -> int:
        """Record one attempt through the public lock-enforced API."""
        with RunDirectoryLock(self.run_directory):
            return self.append_held(run_id, outcome)

    def append_held(self, run_id: str, outcome: str) -> int:
        records = self.read()
        previous = records[-1] if records else None
        sequence = len(records) + 1
        unsigned = {"schema_version": 1, "run_id": run_id, "sequence": sequence,
                    "outcome": outcome, "previous_digest": previous["digest"] if previous else None}
        unsigned["digest"] = hashlib.sha256(json.dumps(unsigned, sort_keys=True,
            separators=(",", ":")).encode("ascii")).hexdigest()
        with self.path.open("a", encoding="ascii") as stream:
            stream.write(json.dumps(unsigned, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush(); os.fsync(stream.fileno())
        _fsync_directory(self.path.parent)
        return sequence

    def attempt_count(self) -> int:
        """Return attempts, rather than the number of outcome records."""
        return sum(record["outcome"] == "started" for record in self.read())

    def read(self) -> list[dict[str, object]]:
        if not self.path.exists():
            return []
        result: list[dict[str, object]] = []
        previous = None
        for number, line in enumerate(self.path.read_text(encoding="ascii").splitlines(), 1):
            try: value = json.loads(line)
            except json.JSONDecodeError as exc: raise OperationalError("corrupt recovery authority") from exc
            digest = value.pop("digest", None)
            expected = hashlib.sha256(json.dumps(value, sort_keys=True,
                separators=(",", ":")).encode("ascii")).hexdigest()
            value["digest"] = digest
            if (digest != expected or value.get("sequence") != number
                    or value.get("previous_digest") != previous):
                raise OperationalError("corrupt recovery authority")
            previous = digest; result.append(value)
        return result
