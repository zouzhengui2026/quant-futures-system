from __future__ import annotations

import json
import struct
from pathlib import Path

import pytest

from quant_futures.paper_runtime.journal import JournalError, TransitionJournal


def test_append_round_trip_builds_digest_lineage(tmp_path: Path) -> None:
    journal = TransitionJournal(tmp_path)
    first = journal.append("run-1", "INPUT_ACCEPTED", 0, {"symbol": "BTCUSDT"})
    second = journal.append("run-1", "ORDER_SUBMITTED", 1, {"quantity": 2})

    records = journal.records()
    assert records == (first, second)
    assert first.sequence == 1 and first.previous_digest is None
    assert second.sequence == 2 and second.previous_digest == first.digest
    assert len(first.digest) == 64


@pytest.mark.parametrize("cut", [1, 7, -1])
def test_truncated_frame_fails_closed(tmp_path: Path, cut: int) -> None:
    journal = TransitionJournal(tmp_path)
    journal.append("run-1", "INPUT_ACCEPTED", 0, {})
    data = journal.path.read_bytes()
    journal.path.write_bytes(data[:cut] if cut > 0 else data[:cut])

    with pytest.raises(JournalError, match="truncated"):
        journal.records()


def test_payload_tampering_is_detected(tmp_path: Path) -> None:
    journal = TransitionJournal(tmp_path)
    journal.append("run-1", "INPUT_ACCEPTED", 0, {"accepted": True})
    data = bytearray(journal.path.read_bytes())
    data[data.index(b"true")] = ord("f")
    journal.path.write_bytes(data)

    with pytest.raises(JournalError, match="invalid journal record"):
        journal.records()


def test_rejects_wrong_run_and_non_json_payload_without_appending(tmp_path: Path) -> None:
    journal = TransitionJournal(tmp_path)
    journal.append("run-1", "INPUT_ACCEPTED", 0, {})
    original = journal.path.read_bytes()

    with pytest.raises(JournalError, match="run ID"):
        journal.append("run-2", "ORDER_SUBMITTED", 1, {})
    with pytest.raises(JournalError, match="canonical JSON"):
        journal.append("run-1", "ORDER_SUBMITTED", 1, {"bad": object()})
    assert journal.path.read_bytes() == original


def test_invalid_digest_lineage_is_rejected(tmp_path: Path) -> None:
    journal = TransitionJournal(tmp_path)
    journal.append("run-1", "INPUT_ACCEPTED", 0, {})
    raw = {
        "schema_version": 1, "run_id": "run-1", "sequence": 3,
        "previous_digest": "0" * 64, "transition_type": "ORDER_SUBMITTED",
        "input_cursor": 1, "payload": {}, "digest": "0" * 64,
    }
    encoded = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    with journal.path.open("ab") as stream:
        stream.write(struct.pack(">4sI", b"QFTJ", len(encoded)) + encoded)

    with pytest.raises(JournalError, match="invalid journal record|invalid journal lineage"):
        journal.records()
