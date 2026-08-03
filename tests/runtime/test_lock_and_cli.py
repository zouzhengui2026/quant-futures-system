from __future__ import annotations

import builtins
import json
import multiprocessing
import time
from pathlib import Path

import pytest

from quant_futures.paper_runtime.control import start
from quant_futures.paper_runtime import control as paper_control
from quant_futures.paper_runtime import journal as journal_module
from quant_futures.paper_runtime.lifecycle import Lifecycle, LifecycleState
from quant_futures.paper_runtime.lock import RunDirectoryLock, RunLockError
from quant_futures.paper_runtime.journal import JournalError, TransitionJournal
from quant_futures.product.cli import main


def _hold_lock(directory: str, ready: multiprocessing.synchronize.Event,
               release: multiprocessing.synchronize.Event) -> None:
    with RunDirectoryLock(directory):
        ready.set()
        release.wait(10)


def _run_control(directory: str, command: str, begin: multiprocessing.synchronize.Event,
                 results: multiprocessing.Queue) -> None:
    begin.wait(10)
    results.put((command, main(["paper", command, directory])))


def _slow_pause(directory: str, ready: multiprocessing.synchronize.Event,
                results: multiprocessing.Queue) -> None:
    original = paper_control._atomic_projection

    def slow_projection(path: Path, value: dict[str, object]) -> None:
        ready.set()
        time.sleep(0.25)
        original(path, value)

    paper_control._atomic_projection = slow_projection
    results.put(main(["paper", "pause", directory]))


def _slow_journal_append(directory: str, ready: multiprocessing.synchronize.Event) -> None:
    journal = TransitionJournal(directory)
    original = journal._persist

    def slow_persist(frame: bytes) -> None:
        descriptor = paper_control.os.open(journal.path, paper_control.os.O_WRONLY |
                                           paper_control.os.O_CREAT | paper_control.os.O_APPEND, 0o600)
        try:
            midpoint = len(frame) // 2
            paper_control.os.write(descriptor, frame[:midpoint])
            ready.set()
            time.sleep(0.25)
            paper_control.os.write(descriptor, frame[midpoint:])
            paper_control.os.fsync(descriptor)
        finally:
            paper_control.os.close(descriptor)

    journal._persist = slow_persist  # type: ignore[method-assign]
    run_id = Lifecycle(directory).current().run_id
    journal.append(run_id, "slow-1", "INPUT", "INPUT_ACCEPTED",
                   "2026-01-01T00:00:00Z", 0, {})


def test_second_process_writer_is_rejected(tmp_path: Path) -> None:
    ready = multiprocessing.Event()
    release = multiprocessing.Event()
    process = multiprocessing.Process(target=_hold_lock, args=(str(tmp_path), ready, release))
    process.start()
    assert ready.wait(5)
    try:
        with pytest.raises(RunLockError, match="another writer"):
            RunDirectoryLock(tmp_path).acquire()
    finally:
        release.set()
        process.join(5)
    assert process.exitcode == 0


def test_direct_lifecycle_mutation_cannot_bypass_process_lock(tmp_path: Path) -> None:
    lifecycle = Lifecycle(tmp_path)
    lifecycle.initialize("run-1")
    before = lifecycle.path.read_bytes()
    ready = multiprocessing.Event()
    release = multiprocessing.Event()
    process = multiprocessing.Process(target=_hold_lock, args=(str(tmp_path), ready, release))
    process.start()
    assert ready.wait(5)
    try:
        with pytest.raises(RunLockError, match="another writer"):
            lifecycle.transition(LifecycleState.STARTING, "direct mutation")
        assert lifecycle.path.read_bytes() == before
    finally:
        release.set()
        process.join(5)
    assert process.exitcode == 0


def test_missing_fcntl_isolated_until_paper_lock_use(tmp_path: Path,
                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__

    def without_fcntl(name: str, *args: object, **kwargs: object) -> object:
        if name == "fcntl":
            raise ImportError("simulated unsupported platform")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_fcntl)
    # Existing CLI modules and parsing remain usable without the Paper backend.
    assert main(["validate-config", "--config", str(tmp_path / "missing.yaml")]) == 2
    with pytest.raises(RunLockError, match="unsupported on this platform"):
        RunDirectoryLock(tmp_path).acquire()


def test_scriptable_control_commands_and_exit_codes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    directory = start(tmp_path)
    assert main(["paper", "status", str(directory)]) == 0
    assert '"authoritative": false' in capsys.readouterr().out
    assert main(["paper", "pause", str(directory)]) == 0
    assert main(["paper", "pause", str(directory)]) == 2
    assert "illegal lifecycle transition" in capsys.readouterr().err
    assert main(["paper", "resume", str(directory)]) == 0
    assert main(["paper", "audit", str(directory)]) == 0
    assert main(["paper", "stop", str(directory)]) == 0
    assert main(["paper", "resume", str(directory)]) == 2


def test_concurrent_controls_preserve_lineage_and_exact_projection(tmp_path: Path) -> None:
    directory = start(tmp_path)
    begin = multiprocessing.Event()
    results: multiprocessing.Queue = multiprocessing.Queue()
    processes = [multiprocessing.Process(
        target=_run_control, args=(str(directory), command, begin, results)
    ) for command in ("pause", "resume", "stop")]
    for process in processes:
        process.start()
    begin.set()
    outcomes = dict(results.get(timeout=10) for _ in processes)
    for process in processes:
        process.join(10)
        assert process.exitcode == 0

    assert set(outcomes) == {"pause", "resume", "stop"}
    assert all(code in {0, 2} for code in outcomes.values())
    lifecycle = Lifecycle(directory)
    records = lifecycle.records()  # Reading all records validates complete lineage.
    assert records[-1] == lifecycle.current()
    status = json.loads((directory / "status.json").read_text(encoding="utf-8"))
    assert status == paper_control.project_status(directory)


def test_audit_during_control_transaction_never_reports_projection_mismatch(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    directory = start(tmp_path)
    ready = multiprocessing.Event()
    results: multiprocessing.Queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=_slow_pause, args=(str(directory), ready, results))
    process.start()
    assert ready.wait(5)

    # Lock contention is a reported command error, not a false audit mismatch.
    assert main(["paper", "audit", str(directory)]) == 2
    captured = capsys.readouterr()
    assert "another writer" in captured.err
    assert "mismatch" not in captured.out

    assert results.get(timeout=5) == 0
    process.join(5)
    assert process.exitcode == 0
    assert paper_control.audit(directory)
    assert json.loads((directory / "status.json").read_text(encoding="utf-8")) == \
        paper_control.project_status(directory)


@pytest.mark.parametrize("authority", [
    None,
    b'{"schema_version":1',
    b"not-json\n",
    b"null\n",
    b"[]\n",
    b'{"schema_version":1,"run_id":7,"sequence":1,"previous_state":null,'
    b'"state":"CREATED","reason":"run created"}\n',
    b'{"schema_version":1,"run_id":"other","sequence":2,"previous_state":"CREATED",'
    b'"state":"STARTING","reason":"bad lineage"}\n',
])
def test_paper_audit_corrupt_authority_returns_mismatch(
    tmp_path: Path, authority: bytes | None, capsys: pytest.CaptureFixture[str],
) -> None:
    directory = tmp_path / "run"
    directory.mkdir()
    if authority is not None:
        (directory / "lifecycle.jsonl").write_bytes(authority)
    (directory / "status.json").write_text("{}\n", encoding="utf-8")
    assert main(["paper", "audit", str(directory)]) == 3
    assert "paper runtime audit: mismatch" in capsys.readouterr().out


def test_paper_audit_accepts_missing_or_empty_checkpoint_two_journal(tmp_path: Path) -> None:
    directory = start(tmp_path)
    assert paper_control.audit(directory)
    (directory / TransitionJournal.filename).touch()
    assert paper_control.audit(directory)


@pytest.mark.parametrize("name", [
    ".checkpoint.json.crash.tmp",
    ".status.json.crash.tmp",
    ".runtime.json.crash.tmp",
    ".control-request.json.crash.tmp",
])
def test_paper_audit_rejects_protocol_temporary_artifacts(
    tmp_path: Path, name: str, capsys: pytest.CaptureFixture[str],
) -> None:
    directory = start(tmp_path)
    (directory / name).write_bytes(b"forged-or-leftover")
    assert main(["paper", "audit", str(directory)]) == 3
    assert "paper runtime audit: mismatch" in capsys.readouterr().out


def test_paper_audit_does_not_reject_similarly_named_operator_file(tmp_path: Path) -> None:
    directory = start(tmp_path)
    (directory / ".checkpoint.json.operator-note").write_text("not protocol", encoding="utf-8")
    assert paper_control.audit(directory)


@pytest.mark.parametrize("entry_kind", ["directory", "broken-symlink"])
def test_paper_audit_rejects_non_file_protocol_temporary_entries(
    tmp_path: Path, entry_kind: str,
) -> None:
    directory = start(tmp_path)
    artifact = directory / ".checkpoint.json.crash.tmp"
    if entry_kind == "directory":
        artifact.mkdir()
    else:
        try:
            artifact.symlink_to(directory / "missing-target")
        except (NotImplementedError, OSError):
            pytest.skip("symbolic links are unavailable on this platform")
    assert not paper_control.audit(directory)


@pytest.mark.parametrize("corruption", ["tamper", "truncate", "duplicate", "reorder"])
def test_paper_audit_journal_corruption_returns_exit_three(
    tmp_path: Path, corruption: str, capsys: pytest.CaptureFixture[str],
) -> None:
    directory = start(tmp_path)
    journal = TransitionJournal(directory)
    args = ("run-1", "product-1", "INPUT", "INPUT_ACCEPTED",
            "2026-01-01T00:00:00Z", 0, {"accepted": True})
    # Use the lifecycle run ID required by the journal authority.
    args = (Lifecycle(directory).current().run_id, *args[1:])
    journal.append(*args)
    journal.append(args[0], "product-2", "INPUT", "INPUT_ACCEPTED",
                   "2026-01-01T00:00:01Z", 1, {})
    paper_control.write_status(directory)
    assert paper_control.audit(directory)
    data = journal.path.read_bytes()
    first_length = int.from_bytes(data[4:8], "big")
    # Header and committed footer are part of the frame boundary.
    first_length += journal_module._HEADER.size + journal_module._FOOTER.size
    if corruption == "tamper":
        damaged = bytearray(data); damaged[damaged.index(b"true")] = ord("f")
        data = bytes(damaged)
    elif corruption == "truncate": data = data[:-1]
    elif corruption == "duplicate": data += data[:first_length]
    else: data = data[first_length:] + data[:first_length]
    journal.path.write_bytes(data)
    assert main(["paper", "audit", str(directory)]) == 3
    assert "paper runtime audit: mismatch" in capsys.readouterr().out


def _failed_run_with_journal(tmp_path: Path) -> tuple[Path, TransitionJournal]:
    directory = start(tmp_path)
    paper_control.transition(directory, LifecycleState.FAILED_RECOVERABLE, "simulated failure")
    journal = TransitionJournal(directory)
    run_id = Lifecycle(directory).current().run_id
    journal.append(run_id, "product-1", "INPUT", "INPUT_ACCEPTED",
                   "2026-01-01T00:00:00Z", 0, {})
    paper_control.write_status(directory)
    return directory, journal


def test_recover_repairs_tail_before_lifecycle_mutation(tmp_path: Path) -> None:
    directory, journal = _failed_run_with_journal(tmp_path)
    committed = journal.path.read_bytes()
    journal.path.write_bytes(committed + b"QF")
    record = paper_control.recover(directory)
    assert record.state is LifecycleState.RUNNING
    assert journal.path.read_bytes() == committed
    assert journal.tail() is not None
    assert json.loads((directory / "status.json").read_text()) == paper_control.project_status(directory)


def test_recover_corrupt_journal_does_not_mutate_lifecycle(tmp_path: Path) -> None:
    directory, journal = _failed_run_with_journal(tmp_path)
    damaged = bytearray(journal.path.read_bytes()); damaged[-1] ^= 1
    journal.path.write_bytes(damaged)
    lifecycle_before = Lifecycle(directory).path.read_bytes()
    journal_before = journal.path.read_bytes()
    with pytest.raises(JournalError):
        paper_control.recover(directory)
    assert Lifecycle(directory).path.read_bytes() == lifecycle_before
    assert journal.path.read_bytes() == journal_before


def test_status_never_reads_partial_journal_frame(tmp_path: Path) -> None:
    directory = start(tmp_path)
    ready = multiprocessing.Event()
    process = multiprocessing.Process(target=_slow_journal_append, args=(str(directory), ready))
    process.start()
    assert ready.wait(5)
    with pytest.raises(RunLockError, match="another writer"):
        paper_control.project_status(directory)
    process.join(5)
    assert process.exitcode == 0
    status = paper_control.project_status(directory)
    assert status["journal_sequence"] == 1
