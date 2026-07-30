"""Durable, framed, append-only transition journal."""

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
    "schema_version", "run_id", "sequence", "journal_event_id",
    "product_transition_id", "stage", "event_type", "effective_market_timestamp",
    "input_cursor", "payload", "previous_digest", "digest",
})


class JournalError(ValueError):
    """The transition journal is unavailable, corrupt, or cannot be persisted."""


@dataclass(frozen=True)
class TransitionRecord:
    schema_version: int
    run_id: str
    sequence: int
    journal_event_id: str
    product_transition_id: str
    stage: str
    event_type: str
    effective_market_timestamp: str
    input_cursor: int
    payload: dict[str, object]
    previous_digest: str | None
    digest: str

    def as_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


@dataclass(frozen=True)
class _Tail:
    run_id: str | None
    sequence: int
    digest: str | None
    boundary: int
    signature: tuple[int, int] | None


class TransitionJournal:
    """Validate on open and retain only constant-sized durable tail metadata."""

    filename = "transitions.journal"

    def __init__(self, run_directory: str | Path) -> None:
        self.run_directory = Path(run_directory)
        self.path = self.run_directory / self.filename
        self._tail: _Tail | None = None

    def records(self) -> tuple[TransitionRecord, ...]:
        return tuple(self._scan(repair_tail=False, collect=True)[0])

    def append(
        self, run_id: str, product_transition_id: str, stage: str, event_type: str,
        effective_market_timestamp: str, input_cursor: int, payload: Mapping[str, object],
    ) -> TransitionRecord:
        with RunDirectoryLock(self.run_directory):
            return self._append_held(run_id, product_transition_id, stage, event_type,
                                     effective_market_timestamp, input_cursor, payload)

    def _append_held(
        self, run_id: str, product_transition_id: str, stage: str, event_type: str,
        effective_market_timestamp: str, input_cursor: int, payload: Mapping[str, object],
    ) -> TransitionRecord:
        values = (run_id, product_transition_id, stage, event_type, effective_market_timestamp)
        if any(not isinstance(value, str) or not value.strip() for value in values):
            raise JournalError("journal identity, stage, event type, and timestamp must be non-empty strings")
        if not isinstance(input_cursor, int) or isinstance(input_cursor, bool) or input_cursor < 0:
            raise JournalError("input cursor must be a non-negative integer")
        normalized = _normalize_json(payload)
        if not isinstance(normalized, dict):
            raise JournalError("transition payload must be an object")

        tail = self._validated_tail_held()
        if tail.run_id is not None and tail.run_id != run_id:
            raise JournalError("run ID does not match journal lineage")
        unsigned: dict[str, object] = {
            "schema_version": 1, "run_id": run_id, "sequence": tail.sequence + 1,
            "product_transition_id": product_transition_id,
            "stage": stage, "event_type": event_type,
            "effective_market_timestamp": effective_market_timestamp,
            "input_cursor": input_cursor, "payload": normalized,
            "previous_digest": tail.digest,
        }
        unsigned["journal_event_id"] = _digest(unsigned)
        digest = _digest(unsigned)
        record = TransitionRecord(**unsigned, digest=digest)  # type: ignore[arg-type]
        encoded = _canonical(record.as_dict())
        if len(encoded) > _MAX_PAYLOAD_BYTES:
            raise JournalError("transition record exceeds maximum frame size")
        self._persist(_HEADER.pack(_MAGIC, len(encoded)) + encoded)
        stat = self.path.stat()
        self._tail = _Tail(run_id, record.sequence, digest, stat.st_size,
                           (stat.st_size, stat.st_mtime_ns))
        return record

    def _validated_tail_held(self) -> _Tail:
        signature = _signature(self.path)
        if self._tail is not None and self._tail.signature == signature:
            return self._tail
        _, tail = self._scan(repair_tail=True, collect=False)
        self._tail = tail
        return tail

    def _persist(self, frame: bytes) -> None:
        created = not self.path.exists()
        try:
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                view = memoryview(frame)
                while view:
                    written = os.write(descriptor, view)
                    if not written:
                        raise OSError("zero-byte journal write")
                    view = view[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if created:
                _fsync_directory(self.run_directory)
        except OSError as exc:
            raise JournalError(f"cannot persist transition journal: {exc}") from exc

    def _scan(self, repair_tail: bool, collect: bool) -> tuple[Iterator[TransitionRecord], _Tail]:
        records: list[TransitionRecord] = []
        if not self.path.exists():
            return iter(records), _Tail(None, 0, None, 0, None)
        try:
            stream = self.path.open("r+b" if repair_tail else "rb")
            with stream:
                previous: TransitionRecord | None = None
                frame_number = 0
                boundary = 0
                while True:
                    header = stream.read(_HEADER.size)
                    if not header:
                        break
                    frame_number += 1
                    if len(header) != _HEADER.size:
                        if repair_tail:
                            _truncate_tail(stream, boundary)
                            break
                        raise JournalError(f"truncated journal header at frame {frame_number}")
                    magic, length = _HEADER.unpack(header)
                    if magic != _MAGIC or length > _MAX_PAYLOAD_BYTES:
                        raise JournalError(f"invalid journal header at frame {frame_number}")
                    encoded = stream.read(length)
                    if len(encoded) != length:
                        if repair_tail:
                            _truncate_tail(stream, boundary)
                            break
                        raise JournalError(f"truncated journal payload at frame {frame_number}")
                    record = _decode(encoded, frame_number)
                    if not ((previous is None and record.sequence == 1 and record.previous_digest is None)
                            or (previous is not None and record.run_id == previous.run_id
                                and record.sequence == previous.sequence + 1
                                and record.previous_digest == previous.digest)):
                        raise JournalError(f"invalid journal lineage at frame {frame_number}")
                    previous = record
                    if collect:
                        records.append(record)
                    boundary = stream.tell()
            stat = self.path.stat()
        except OSError as exc:
            raise JournalError(f"cannot read transition journal: {exc}") from exc
        tail = _Tail(previous.run_id if previous else None, previous.sequence if previous else 0,
                     previous.digest if previous else None, boundary,
                     (stat.st_size, stat.st_mtime_ns))
        return iter(records), tail


def _truncate_tail(stream: object, boundary: int) -> None:
    stream.truncate(boundary)  # type: ignore[attr-defined]
    stream.flush()  # type: ignore[attr-defined]
    os.fsync(stream.fileno())  # type: ignore[attr-defined]


def _signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_size, stat.st_mtime_ns


def _normalize_json(value: object) -> object:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise JournalError("transition payload object keys must be strings")
        return {key: _normalize_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json(item) for item in value]
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and value == value and abs(value) != float("inf"):
        return value
    raise JournalError("transition payload is not canonical JSON")


def _canonical(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def _digest(unsigned: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical(unsigned)).hexdigest()


def _decode(encoded: bytes, frame_number: int) -> TransitionRecord:
    try:
        raw = json.loads(encoded.decode("utf-8"))
        if not isinstance(raw, dict) or frozenset(raw) != _FIELDS:
            raise ValueError("unexpected journal fields")
        record = TransitionRecord(**raw)
        unsigned = record.as_dict(); unsigned.pop("digest")
        event_unsigned = dict(unsigned); event_id = event_unsigned.pop("journal_event_id")
        valid = (
            record.schema_version == 1 and isinstance(record.run_id, str) and bool(record.run_id)
            and isinstance(record.sequence, int) and not isinstance(record.sequence, bool) and record.sequence > 0
            and all(isinstance(value, str) and bool(value.strip()) for value in
                    (record.product_transition_id, record.stage, record.event_type,
                     record.effective_market_timestamp))
            and isinstance(record.input_cursor, int) and not isinstance(record.input_cursor, bool)
            and record.input_cursor >= 0 and isinstance(record.payload, dict)
            and _normalize_json(record.payload) == record.payload
            and _is_digest(event_id) and event_id == _digest(event_unsigned)
            and (record.previous_digest is None or _is_digest(record.previous_digest))
            and _is_digest(record.digest) and record.digest == _digest(unsigned)
            and encoded == _canonical(raw)
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError, JournalError) as exc:
        raise JournalError(f"invalid journal record at frame {frame_number}: {exc}") from exc
    if not valid:
        raise JournalError(f"invalid journal record at frame {frame_number}")
    return record


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
