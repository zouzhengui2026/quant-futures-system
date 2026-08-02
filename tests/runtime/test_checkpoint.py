import json
import multiprocessing
import os
from dataclasses import replace

import pytest

from quant_futures.paper_runtime import (
    CheckpointError, CheckpointStore, PaperTransitionCoordinator, RunDirectoryLock,
    RunLockError, TransitionError,
)
from quant_futures.paper_runtime.journal import TransitionJournal
from quant_futures.product.strategy import FixedStrategy
from quant_futures.product.config import DataConfig

from test_transition import (VERIFIED_DATA, authorized_coordinator, bar, config,
                             running_lifecycle)


def test_current_close_restore_matches_uninterrupted(tmp_path):
    cfg = config(commission=2.0, slippage=1.0)
    uninterrupted = tmp_path / "all"; uninterrupted.mkdir()
    split = tmp_path / "split"; split.mkdir()
    all_run = authorized_coordinator("run", cfg, FixedStrategy(1.0), TransitionJournal(uninterrupted))
    split_run = authorized_coordinator("run", cfg, FixedStrategy(1.0), TransitionJournal(split))
    for item in (bar(0), bar(1, close=110, funding=0.001)):
        expected = all_run.transition(item)
    split_run.transition(bar(0))
    restored = PaperTransitionCoordinator("run", cfg, FixedStrategy(1.0), TransitionJournal(split), data_fingerprint=VERIFIED_DATA)
    actual = restored.transition(bar(1, close=110, funding=0.001))
    assert actual.input_cursor == expected.input_cursor
    assert actual.portfolio_snapshot == expected.portfolio_snapshot
    assert actual.account_snapshot == expected.account_snapshot
    assert actual.risk_snapshot == expected.risk_snapshot
    assert (split / "transitions.journal").read_bytes() == (uninterrupted / "transitions.journal").read_bytes()
    assert (split / "checkpoint.json").read_bytes() == (uninterrupted / "checkpoint.json").read_bytes()


def test_next_open_pending_order_restores_and_fills_once(tmp_path):
    cfg = config(timing="next_open", commission=3.0, slippage=2.0)
    journal = TransitionJournal(tmp_path)
    first = authorized_coordinator("pending", cfg, FixedStrategy(1.0), journal).transition(bar(0))
    order_id = first.pending_order.order.order_id
    restored = PaperTransitionCoordinator("pending", cfg, FixedStrategy(1.0), TransitionJournal(tmp_path), data_fingerprint=VERIFIED_DATA)
    assert restored.state.pending_order.order.order_id == order_id
    state = restored.transition(bar(1, open_=105, close=106))
    assert state.counters.fills == 1
    assert state.portfolio_snapshot.positions[0].average_entry_price == pytest.approx(105.021)


def test_checkpoint_bytes_are_canonical_and_stable(tmp_path):
    paths = (tmp_path / "a", tmp_path / "b")
    values = []
    for path in paths:
        path.mkdir()
        authorized_coordinator("same", config(), FixedStrategy(1.0), TransitionJournal(path)).transition(bar(0))
        values.append((path / "checkpoint.json").read_bytes())
    assert values[0] == values[1]
    assert values[0].endswith(b"\n")


@pytest.mark.parametrize("mutation", ["truncate", "extra", "wrong_run", "wrong_config"])
def test_corrupt_or_foreign_checkpoint_fails_closed(tmp_path, mutation):
    journal = TransitionJournal(tmp_path)
    authorized_coordinator("reject", config(), FixedStrategy(1.0), journal).transition(bar(0))
    path = tmp_path / "checkpoint.json"
    if mutation == "truncate":
        path.write_bytes(path.read_bytes()[:-4])
    else:
        value = json.loads(path.read_bytes())
        if mutation == "extra": value["extra"] = True
        if mutation == "wrong_run": value["run_id"] = "foreign"
        if mutation == "wrong_config": value["config_digest"] = "0" * 64
        path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(TransitionError, match="checkpoint restoration"):
        PaperTransitionCoordinator("reject", config(), FixedStrategy(1.0), TransitionJournal(tmp_path), data_fingerprint=VERIFIED_DATA)


def test_atomic_failure_preserves_previous_checkpoint_and_cleans_temp(tmp_path, monkeypatch):
    store = CheckpointStore(tmp_path)
    original = {"schema_version": 1, "value": "old"}
    store.write(original)
    before = store.path.read_bytes()
    monkeypatch.setattr(os, "replace", lambda *args: (_ for _ in ()).throw(OSError("cut")))
    with pytest.raises(CheckpointError):
        store.write({"schema_version": 1, "value": "new"})
    assert store.path.read_bytes() == before
    assert list(tmp_path.glob(".checkpoint.json.*.tmp")) == []


def _hold_run_lock(path, ready, release):
    with RunDirectoryLock(path):
        ready.set()
        release.wait(10)


def _hold_restore(path, ready, release, outcome):
    original = PaperTransitionCoordinator._install_bounded_state
    def controlled(self, value):
        ready.set()
        assert release.wait(10)
        return original(self, value)
    PaperTransitionCoordinator._install_bounded_state = controlled
    try:
        PaperTransitionCoordinator("restore-lock", config(), FixedStrategy(1.0),
                                   TransitionJournal(path), data_fingerprint=VERIFIED_DATA)
        outcome.put("restored")
    except BaseException as exc:
        outcome.put(type(exc).__name__)


def test_public_checkpoint_write_obeys_multiprocess_run_lock(tmp_path):
    store = CheckpointStore(tmp_path)
    store.write({"schema_version": 1, "value": "old"})
    before = store.path.read_bytes()
    context = multiprocessing.get_context("spawn")
    ready, release = context.Event(), context.Event()
    process = context.Process(target=_hold_run_lock, args=(tmp_path, ready, release))
    process.start()
    assert ready.wait(10)
    try:
        with pytest.raises(RunLockError):
            store.write({"schema_version": 1, "value": "new"})
        assert store.path.read_bytes() == before
    finally:
        release.set()
        process.join(10)
    assert process.exitcode == 0


def test_restore_excludes_independent_authority_contenders(tmp_path):
    coordinator = authorized_coordinator(
        "restore-lock", config(), FixedStrategy(1.0), TransitionJournal(tmp_path))
    coordinator.transition(bar(0))
    authority = {name: (tmp_path / name).read_bytes() for name in (
        "checkpoint.json", "transitions.journal", "lifecycle.jsonl")}
    context = multiprocessing.get_context("fork")
    ready, release, outcome = context.Event(), context.Event(), context.Queue()
    process = context.Process(target=_hold_restore,
                              args=(tmp_path, ready, release, outcome))
    process.start()
    assert ready.wait(10)
    try:
        with pytest.raises(RunLockError):
            CheckpointStore(tmp_path).write({"schema_version": 1})
        with pytest.raises(RunLockError):
            authorized_coordinator("restore-lock", config(), FixedStrategy(1.0),
                                   TransitionJournal(tmp_path))
        from quant_futures.paper_runtime import Lifecycle, LifecycleState
        with pytest.raises(RunLockError):
            Lifecycle(tmp_path).transition(LifecycleState.PAUSED, "contender")
        with pytest.raises(RunLockError):
            Lifecycle(tmp_path).transition(LifecycleState.STOPPING, "contender")
        assert {name: (tmp_path / name).read_bytes() for name in authority} == authority
    finally:
        release.set()
        process.join(10)
    assert process.exitcode == 0
    assert outcome.get(timeout=2) == "restored"


@pytest.mark.parametrize("section", [
    "strategy", "execution", "portfolio", "account", "risk", "counters",
])
def test_every_retained_authority_section_is_validated(tmp_path, section):
    journal = TransitionJournal(tmp_path)
    authorized_coordinator("authority", config(), FixedStrategy(1.0), journal).transition(bar(0))
    path = tmp_path / "checkpoint.json"
    value = json.loads(path.read_bytes())
    target = value[section]
    if isinstance(target, list):
        target[0][next(iter(target[0]))] = "corrupt"
    else:
        target[next(iter(target))] = "corrupt"
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(TransitionError, match="checkpoint restoration"):
        PaperTransitionCoordinator("authority", config(), FixedStrategy(1.0),
                                   TransitionJournal(tmp_path), data_fingerprint=VERIFIED_DATA)


def test_directory_fsync_failure_leaves_complete_new_authority(tmp_path, monkeypatch):
    store = CheckpointStore(tmp_path)
    store.write({"schema_version": 1, "value": "old"})
    new = {"schema_version": 1, "value": "new"}
    monkeypatch.setattr("quant_futures.paper_runtime.checkpoint._fsync_directory",
                        lambda path: (_ for _ in ()).throw(OSError("uncertain")))
    with pytest.raises(CheckpointError):
        store.write(new)
    assert store.path.read_bytes() == (json.dumps(new, sort_keys=True,
        separators=(",", ":")) + "\n").encode("ascii")
    assert list(tmp_path.glob(".checkpoint.json.*.tmp")) == []


def test_checkpoint_state_is_bounded_and_restore_never_replays_transitions(tmp_path,
                                                                           monkeypatch):
    coordinator = authorized_coordinator(
        "bounded", config(), FixedStrategy(1.0), TransitionJournal(tmp_path))
    sizes = []
    for index in range(120):
        coordinator.transition(bar(index, close=100 + index / 10))
        if index in {9, 119}:
            sizes.append((tmp_path / "checkpoint.json").stat().st_size)
    # Cursor digits are the only event-count-dependent representation.
    assert sizes[1] - sizes[0] < 32
    value = json.loads((tmp_path / "checkpoint.json").read_bytes())
    assert "history" not in value
    assert len(value["strategy"]["closes"]) == 1
    monkeypatch.setattr(PaperTransitionCoordinator, "transition",
                        lambda *args: (_ for _ in ()).throw(AssertionError("historical replay")))
    restored = PaperTransitionCoordinator(
        "bounded", config(), FixedStrategy(1.0), TransitionJournal(tmp_path),
        data_fingerprint=VERIFIED_DATA)
    assert restored.state.input_cursor == 120


def test_replay_content_identity_fails_closed_and_explicit_fingerprint_is_allowed(tmp_path):
    replay = tmp_path / "bars.csv"
    replay.write_bytes(b"first")
    cfg = replace(config(), data=DataConfig(str(replay)))
    running = tmp_path / "run"; running.mkdir()
    running_lifecycle(running, "content")
    coordinator = PaperTransitionCoordinator(
        "content", cfg, FixedStrategy(1.0), TransitionJournal(running))
    coordinator.transition(bar(0))
    replay.write_bytes(b"changed")
    with pytest.raises(TransitionError, match="data fingerprint"):
        PaperTransitionCoordinator("content", cfg, FixedStrategy(1.0),
                                   TransitionJournal(running))
    missing = replace(config(), data=DataConfig(str(tmp_path / "missing.csv")))
    empty = tmp_path / "empty"; empty.mkdir()
    with pytest.raises(TransitionError, match="readable regular file"):
        PaperTransitionCoordinator("missing", missing, FixedStrategy(0.0),
                                   TransitionJournal(empty))
    running_lifecycle(empty, "missing")
    PaperTransitionCoordinator("missing", missing, FixedStrategy(0.0),
                               TransitionJournal(empty), data_fingerprint=VERIFIED_DATA)
