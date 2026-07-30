from __future__ import annotations

import json
import struct
from pathlib import Path

import pytest

from quant_futures.paper_runtime import journal as journal_module
from quant_futures.paper_runtime.journal import JournalError, TransitionJournal


def append(journal: TransitionJournal, event: str = "INPUT_ACCEPTED", cursor: int = 0,
           payload: dict[object, object] | None = None):
    return journal.append("run-1", f"transition-{cursor}", "INPUT", event,
                          "2026-01-01T00:00:00Z", cursor, payload or {})


def test_append_round_trip_has_complete_envelope_and_normalized_payload(tmp_path: Path) -> None:
    journal = TransitionJournal(tmp_path)
    first = append(journal, payload={"symbols": ("BTCUSDT",)})
    second = append(journal, "ORDER_SUBMITTED", 1, {"quantity": 2})
    assert journal.records() == (first, second)
    assert first.payload == {"symbols": ["BTCUSDT"]}
    assert first.sequence == 1 and first.previous_digest is None
    assert second.sequence == 2 and second.previous_digest == first.digest
    assert len(first.journal_event_id) == len(first.digest) == 64


@pytest.mark.parametrize("payload", [{1: "bad"}, {"nested": [{2: "bad"}]},
                                      {"bad": object()}, {"bad": float("nan")}])
def test_rejects_malformed_nested_payload(tmp_path: Path, payload: dict[object, object]) -> None:
    with pytest.raises(JournalError, match="keys|canonical JSON"):
        append(TransitionJournal(tmp_path), payload=payload)


def test_incomplete_tail_is_repaired_at_every_header_and_payload_boundary(tmp_path: Path) -> None:
    source = TransitionJournal(tmp_path / "source")
    source.run_directory.mkdir()
    append(source)
    first = source.path.read_bytes()
    append(source, cursor=1)
    second = source.path.read_bytes()[len(first):]
    for cut in range(1, len(second)):
        directory = tmp_path / f"cut-{cut}"
        directory.mkdir()
        journal = TransitionJournal(directory)
        journal.path.write_bytes(first + second[:cut])
        recovered = append(journal, "RECOVERED", 1)
        assert recovered.sequence == 2
        assert len(journal.records()) == 2


def test_records_fail_closed_on_truncated_tail(tmp_path: Path) -> None:
    journal = TransitionJournal(tmp_path)
    append(journal)
    journal.path.write_bytes(journal.path.read_bytes()[:-1])
    with pytest.raises(JournalError, match="truncated"):
        journal.records()


def test_completed_prefix_corruption_is_never_repaired(tmp_path: Path) -> None:
    journal = TransitionJournal(tmp_path)
    append(journal); append(journal, cursor=1)
    damaged = bytearray(journal.path.read_bytes()); damaged[0] = ord("X")
    journal.path.write_bytes(damaged); before = journal.path.read_bytes()
    with pytest.raises(JournalError, match="header"):
        append(TransitionJournal(tmp_path), cursor=2)
    assert journal.path.read_bytes() == before


@pytest.mark.parametrize("mode", ["tamper", "duplicate", "reorder", "oversized"])
def test_corrupt_completed_frames_are_rejected(tmp_path: Path, mode: str) -> None:
    journal = TransitionJournal(tmp_path); append(journal); append(journal, cursor=1)
    data = journal.path.read_bytes()
    length = struct.unpack(">4sI", data[:8])[1]
    boundary = 8 + length
    if mode == "tamper":
        raw = json.loads(data[8:boundary]); raw["payload"] = {"valid": "json"}
        encoded = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
        data = struct.pack(">4sI", b"QFTJ", len(encoded)) + encoded + data[boundary:]
    elif mode == "duplicate": data += data[:boundary]
    elif mode == "reorder": data = data[boundary:] + data[:boundary]
    else: data = struct.pack(">4sI", b"QFTJ", journal_module._MAX_PAYLOAD_BYTES + 1)
    journal.path.write_bytes(data)
    with pytest.raises(JournalError): journal.records()


def test_steady_state_append_scans_existing_history_only_once(tmp_path: Path,
                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    journal = TransitionJournal(tmp_path); decoded = 0
    original = journal_module._decode
    def instrumented(encoded: bytes, frame: int):
        nonlocal decoded; decoded += 1
        return original(encoded, frame)
    monkeypatch.setattr(journal_module, "_decode", instrumented)
    for index in range(500): append(journal, cursor=index)
    assert decoded == 0  # Empty restart scan, then O(1) cached durable tail updates.
    restarted = TransitionJournal(tmp_path)
    append(restarted, cursor=500)
    assert decoded == 500  # One linear restart validation, not one scan per append.


def test_first_create_fsyncs_file_then_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(journal_module.os, "fsync", lambda descriptor: calls.append("file"))
    monkeypatch.setattr(journal_module, "_fsync_directory", lambda path: calls.append("directory"))
    append(TransitionJournal(tmp_path))
    assert calls == ["file", "directory"]
