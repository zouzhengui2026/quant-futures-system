"""Durable, framed, append-only transition journal.

Frames are self-delimiting and independently authenticated.  The digest chain makes
record removal, reordering, and modification detectable; a short header or payload is
treated as corruption rather than silently ignored as a recoverable tail.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Mapping

from .lifecycle import _fsync_directory
from .lock import RunDirectoryLock


_MAGIC = b"QFTJ"
_HEADER = struct.Struct(">4sI")
_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
_FIELDS = frozenset({
    "schema_version", "run_id", "sequence", "previous_digest",
    "transition_type", "input_cursor", "payload", "digest",
})


class JournalError(ValueError):
    """The transition journal is unavailable, corrupt, or cannot be persisted."""


@dataclass(frozen=True)
class TransitionRecord:
    schema_version: int
    run_id: str
    sequence: int
    previous_digest: str | None
    transition_type: str
    input_cursor: int
    payload: dict[str, object]
    digest: str

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "sequence": self.sequence,
            "previous_digest": self.previous_digest,
            "transition_type": self.transition_type,
            "input_cursor": self.input_cursor,
            "payload": self.payload,
            "digest": self.digest,
        }


class TransitionJournal:
    """Validate and durably append state transitions for one run directory."""

    filename = "transitions.journal"

    def __init__(self, run_directory: str | Path) -> None:
        self.run_directory = Path(run_directory)
        self.path = self.run_directory / self.filename

    def records(self) -> tuple[TransitionRecord, ...]:
        return tuple(self._read_records())

    def append(
        self,
        run_id: str,
        transition_type: str,
        input_cursor: int,
        payload: Mapping[str, object],
    ) -> TransitionRecord:
        """Append one transition while enforcing the run's single-writer lock."""
        with RunDirectoryLock(self.run_directory):
            return self._append_held(run_id, transition_type, input_cursor, payload)

    def _append_held(
        self,
        run_id: str,
        transition_type: str,
        input_cursor: int,
        payload: Mapping[str, object],
    ) -> TransitionRecord:
        """Append while a command-scoped caller holds the run-directory lock."""
        if not isinstance(run_id, str) or not run_id:
            raise JournalError("run ID must be a non-empty string")
        if not isinstance(transition_type, str) or not transition_type.strip():
            raise JournalError("transition type must be a non-empty string")
        if not isinstance(input_cursor, int) or isinstance(input_cursor, bool) or input_cursor < 0:
            raise JournalError("input cursor must be a non-negative integer")
        if not isinstance(payload, Mapping):
            raise JournalError("transition payload must be an object")

        prior = self.records()
        previous = prior[-1] if prior else None
        if previous is not None and previous.run_id != run_id:
            raise JournalError("run ID does not match journal lineage")
        unsigned: dict[str, object] = {
            "schema_version": 1,
            "run_id": run_id,
            "sequence": 1 if previous is None else previous.sequence + 1,
            "previous_digest": None if previous is None else previous.digest,
            "transition_type": transition_type.strip(),
            "input_cursor": input_cursor,
            "payload": dict(payload),
        }
        try:
            digest = _digest(unsigned)
            record = TransitionRecord(**unsigned, digest=digest)  # type: ignore[arg-type]
            encoded = _canonical(record.as_dict())
        except (TypeError, ValueError) as exc:
            raise JournalError(f"transition payload is not canonical JSON: {exc}") from exc
        if len(encoded) > _MAX_PAYLOAD_BYTES:
            raise JournalError("transition record exceeds maximum frame size")
        self._persist(_HEADER.pack(_MAGIC, len(encoded)) + encoded)
        return record

    def _persist(self, frame: bytes) -> None:
        created = not self.path.exists()
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        try:
            descriptor = os.open(self.path, flags, 0o600)
            try:
                view = memoryview(frame)
                while view:
                    written = os.write(descriptor, view)
                    if written == 0:
                        raise OSError("zero-byte journal write")
                    view = view[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if created:
                _fsync_directory(self.run_directory)
        except OSError as exc:
            raise JournalError(f"cannot persist transition journal: {exc}") from exc

    def _read_records(self) -> Iterator[TransitionRecord]:
        try:
            stream = self.path.open("rb")
        except OSError as exc:
            if isinstance(exc, FileNotFoundError):
                return
            raise JournalError(f"cannot read transition journal: {exc}") from exc
        previous: TransitionRecord | None = None
        with stream:
            frame_number = 0
            while True:
                header = stream.read(_HEADER.size)
                if not header:
                    return
                frame_number += 1
                if len(header) != _HEADER.size:
                    raise JournalError(f"truncated journal header at frame {frame_number}")
                magic, length = _HEADER.unpack(header)
                if magic != _MAGIC or length > _MAX_PAYLOAD_BYTES:
                    raise JournalError(f"invalid journal header at frame {frame_number}")
                encoded = stream.read(length)
                if len(encoded) != length:
                    raise JournalError(f"truncated journal payload at frame {frame_number}")
                record = _decode(encoded, frame_number)
                valid = (
                    (previous is None and record.sequence == 1 and record.previous_digest is None)
                    or (
                        previous is not None
                        and record.run_id == previous.run_id
                        and record.sequence == previous.sequence + 1
                        and record.previous_digest == previous.digest
                    )
                )
                if not valid:
                    raise JournalError(f"invalid journal lineage at frame {frame_number}")
                previous = record
                yield record


def _canonical(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _digest(unsigned: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical(unsigned)).hexdigest()


def _decode(encoded: bytes, frame_number: int) -> TransitionRecord:
    try:
        raw = json.loads(encoded.decode("utf-8"))
        if not isinstance(raw, dict) or frozenset(raw) != _FIELDS:
            raise ValueError("unexpected journal fields")
        record = TransitionRecord(**raw)
        unsigned = record.as_dict()
        unsigned.pop("digest")
        valid = (
            record.schema_version == 1
            and isinstance(record.run_id, str) and bool(record.run_id)
            and isinstance(record.sequence, int) and not isinstance(record.sequence, bool)
            and record.sequence > 0
            and (record.previous_digest is None or _is_digest(record.previous_digest))
            and isinstance(record.transition_type, str) and bool(record.transition_type.strip())
            and isinstance(record.input_cursor, int) and not isinstance(record.input_cursor, bool)
            and record.input_cursor >= 0
            and isinstance(record.payload, dict)
            and _is_digest(record.digest)
            and record.digest == _digest(unsigned)
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise JournalError(f"invalid journal record at frame {frame_number}: {exc}") from exc
    if not valid:
        raise JournalError(f"invalid journal record at frame {frame_number}")
    return record


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
