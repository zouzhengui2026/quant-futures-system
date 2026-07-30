from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest

from quant_futures.paper_runtime.control import start
from quant_futures.paper_runtime.lock import RunDirectoryLock, RunLockError
from quant_futures.product.cli import main


def _hold_lock(directory: str, ready: multiprocessing.synchronize.Event,
               release: multiprocessing.synchronize.Event) -> None:
    with RunDirectoryLock(directory):
        ready.set()
        release.wait(10)


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
