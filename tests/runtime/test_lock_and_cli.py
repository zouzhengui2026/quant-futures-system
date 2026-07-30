from __future__ import annotations

import builtins
import json
import multiprocessing
import time
from pathlib import Path

import pytest

from quant_futures.paper_runtime.control import start
from quant_futures.paper_runtime import control as paper_control
from quant_futures.paper_runtime.lifecycle import Lifecycle, LifecycleState
from quant_futures.paper_runtime.lock import RunDirectoryLock, RunLockError
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
