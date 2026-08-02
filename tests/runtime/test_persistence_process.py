"""Physical persistence-boundary evidence using disposable child processes."""

from __future__ import annotations

import multiprocessing
import os
import json
from pathlib import Path

import pytest

from quant_futures.paper_runtime.checkpoint import CheckpointStore, canonical_checkpoint
from quant_futures.paper_runtime.journal import TransitionJournal


def _initialize_product_run(directory: str, config_path: str, replay: str) -> None:
    from quant_futures.paper_runtime import Lifecycle, LifecycleState
    from quant_futures.paper_runtime import control

    path = os.fspath(directory)
    os.makedirs(path)
    lifecycle = Lifecycle(path)
    lifecycle.initialize("process-matrix-run")
    lifecycle.transition(LifecycleState.STARTING, "start")
    lifecycle.transition(LifecycleState.RUNNING, "run")
    control.write_runtime_metadata(path, Path(config_path), Path(replay))


def _produce_product_cut(directory: str, config_path: str, replay: str,
                         cut_index: int, stage: str | None) -> None:
    from dataclasses import replace
    from quant_futures.paper_runtime import Lifecycle, PaperTransitionCoordinator
    from quant_futures.product.config import load_config
    from quant_futures.product.data import load_bars
    from quant_futures.product.strategy import build_strategy

    config = load_config(config_path)
    config = replace(config, data=replace(config.data, path=replay))
    bars, fingerprint = load_bars(replay, config.data.schema,
                                  timeframe=config.data.timeframe)
    armed = {"value": False}

    def cut(boundary: str) -> None:
        if armed["value"] and stage is not None and boundary == f"durable:{stage}":
            os._exit(93)

    coordinator = PaperTransitionCoordinator(
        Lifecycle(directory).current().run_id, config,
        build_strategy(config.strategy.name, config.strategy.parameters),
        TransitionJournal(directory), failure_injector=cut,
        data_fingerprint=f"sha256:{fingerprint}",
    )
    for index, item in enumerate(bars):
        armed["value"] = index == cut_index
        coordinator.transition(item)
    os._exit(0)


def _recover_product_process(directory: str) -> None:
    from quant_futures.paper_runtime import control

    control.recover(directory)
    os._exit(0)


def _continue_product_process(directory: str) -> None:
    from quant_futures.paper_runtime import control

    control.continue_runtime(directory)
    os._exit(0)


def _crash_public_recovery(directory: str, boundary: str) -> None:
    """Crash through the public recovery API, never a reconciliation helper."""
    from quant_futures.paper_runtime import control

    def cut(name: str) -> None:
        if name == boundary:
            os._exit(94)

    control.recover(directory, failure_injector=cut)
    os._exit(0)


def _recover_twice_then_continue(directory: Path) -> None:
    """Run each recovery attempt and continuation in a fresh OS process."""
    for target in (_recover_product_process, _recover_product_process,
                   _continue_product_process):
        process = multiprocessing.Process(target=target, args=(str(directory),))
        process.start(); process.join(30)
        assert process.exitcode == 0


def _write_product_fixture(root, timing):
    replay = root / f"{timing}.csv"
    replay.write_text(
        "timestamp,open,high,low,close,volume,funding_rate\n"
        "2025-01-01T00:00:00Z,100,101,99,100,1,0.001\n"
        "2025-01-01T01:00:00Z,100,111,99,110,1,0.002\n"
        "2025-01-01T02:00:00Z,90,91,79,80,1,0.003\n"
        "2025-01-01T03:00:00Z,85,96,84,95,1,0.004\n",
        encoding="utf-8",
    )
    config = root / f"{timing}.yml"
    config.write_text(
        "mode: paper\n"
        f"data:\n  path: {replay.name}\n  source: test\n  symbol: BTC\n  timeframe: 1h\n"
        "  schema:\n    timestamp: timestamp\n    open: open\n    high: high\n"
        "    low: low\n    close: close\n    volume: volume\n    funding_rate: funding_rate\n"
        "strategy:\n  name: moving_average_crossover\n  parameters:\n    fast: 1\n    slow: 2\n"
        "costs:\n  commission_bps: 7\n  slippage_bps: 5\n  funding_rate: 0\n"
        f"fill_timing: {timing}\noutput_directory: .\n",
        encoding="utf-8",
    )
    return config, replay


def _checkpoint_core(path):
    value = json.loads((path / "checkpoint.json").read_text(encoding="utf-8"))
    value.pop("recovery_digest", None)
    return value


def _product_core(path):
    """Return the complete Product authority, excluding recovery-only lineage."""
    value = _checkpoint_core(path)
    value.pop("lifecycle", None)
    value.pop("recovery_counter", None)
    return value


def _assert_unique_product_identities(records):
    selectors = {
        "journal": [record.journal_event_id for record in records],
        "input": [record.payload["input_event_id"] for record in records
                  if record.stage == "transition_started"],
        "transition": [record.product_transition_id for record in records
                       if record.stage == "transition_started"],
        "order": [record.payload["order_id"] for record in records
                  if record.stage == "order_submitted"],
        "fill": [record.payload["fill_id"] for record in records
                 if record.stage == "fill_committed"],
    }
    for name, identities in selectors.items():
        assert len(identities) == len(set(identities)), name


def _assert_recovery_chain(path, attempts=2):
    from quant_futures.paper_runtime import RecoveryAttempts

    records = RecoveryAttempts(path).read()
    starts = [record for record in records if record["outcome"] == "started"]
    assert len(starts) == attempts
    assert len(records) == attempts * 2
    assert all(records[index + 1]["previous_digest"] == records[index]["digest"]
               for index in range(len(records) - 1))
    checkpoint = json.loads((path / "checkpoint.json").read_text(encoding="utf-8"))
    assert checkpoint["recovery_counter"] == attempts
    assert checkpoint["recovery_digest"] == records[-1]["digest"]


def _crash_journal(directory: str, boundary: str) -> None:
    def cut(name: str) -> None:
        if name == boundary:
            os._exit(91)

    TransitionJournal(directory, cut).append(
        "run", "transition", "transition_started", "bar",
        "2025-01-01T00:00:00+00:00", 0, {"value": 1},
    )


@pytest.mark.parametrize("boundary", [
    "journal_partial_frame_written",
    "journal_frame_write_completed",
    "journal_before_flush",
    "journal_flush_completed",
    "journal_fsync_completed",
    "journal_post_fsync_published",
])
def test_child_exit_at_physical_journal_boundary_is_repairable_or_committed(
    tmp_path, boundary,
):
    process = multiprocessing.Process(
        target=_crash_journal, args=(str(tmp_path), boundary))
    process.start(); process.join(10)
    assert process.exitcode == 91

    journal = TransitionJournal(tmp_path)
    with open(tmp_path / ".paper-runtime.lock", "a+b"):
        # Public append performs the lock-scoped scan/repair before publication.
        journal.append("run", "continuation", "transition_started", "bar",
                       "2025-01-01T01:00:00+00:00", 1, {"value": 2})
    records = journal.records()
    assert records[-1].product_transition_id == "continuation"
    assert len(records) in {1, 2}
    assert [record.sequence for record in records] == list(range(1, len(records) + 1))


def _crash_checkpoint(directory: str, boundary: str) -> None:
    def cut(name: str) -> None:
        if name == boundary:
            os._exit(92)

    CheckpointStore(directory, cut).write({"schema_version": 1, "value": "new"})


@pytest.mark.parametrize("boundary", [
    "checkpoint_temporary_written",
    "checkpoint_file_flush_completed",
    "checkpoint_file_fsync_completed",
    "checkpoint_atomic_replace_completed",
    "checkpoint_directory_fsync_completed",
    "checkpoint_post_directory_fsync_published",
])
def test_child_exit_at_physical_checkpoint_boundary_leaves_exact_old_or_new(
    tmp_path, boundary,
):
    store = CheckpointStore(tmp_path)
    old = store.write({"schema_version": 1, "value": "old"})
    new = canonical_checkpoint({"schema_version": 1, "value": "new"})
    process = multiprocessing.Process(
        target=_crash_checkpoint, args=(str(tmp_path), boundary))
    process.start(); process.join(10)
    assert process.exitcode == 92
    assert store.path.read_bytes() in {old, new}
    assert store.read()["value"] in {"old", "new"}
    orphaned = list(tmp_path.glob(".checkpoint.json.*.tmp"))
    if boundary in {
        "checkpoint_temporary_written", "checkpoint_file_flush_completed",
        "checkpoint_file_fsync_completed",
    }:
        assert len(orphaned) == 1
    else:
        assert orphaned == []

    # The next public write is the deterministic reopen point: it treats no
    # temporary as authority, removes only exact checkpoint temporaries, and
    # leaves a complete canonical old-or-new checkpoint throughout.
    final = store.write({"schema_version": 1, "value": "continued"})
    assert store.path.read_bytes() == final
    assert list(tmp_path.glob(".checkpoint.json.*.tmp")) == []


def test_repeated_pre_replace_process_cuts_cannot_grow_checkpoint_temporaries(tmp_path):
    store = CheckpointStore(tmp_path)
    old = store.write({"schema_version": 1, "value": "old"})
    for _ in range(6):
        process = multiprocessing.Process(
            target=_crash_checkpoint,
            args=(str(tmp_path), "checkpoint_file_fsync_completed"),
        )
        process.start(); process.join(10)
        assert process.exitcode == 92
        assert store.path.read_bytes() == old
        assert len(list(tmp_path.glob(".checkpoint.json.*.tmp"))) == 1

    store.write({"schema_version": 1, "value": "reopened"})
    assert list(tmp_path.glob(".checkpoint.json.*.tmp")) == []


def test_checkpoint_cleanup_matches_only_exact_temporary_protocol_names(tmp_path):
    store = CheckpointStore(tmp_path)
    keep = [
        tmp_path / ".checkpoint.json.tmp",
        tmp_path / ".checkpoint.json.extra.tmp.more",
        tmp_path / "checkpoint.json.extra.tmp",
    ]
    for path in keep:
        path.write_text("not protocol state", encoding="utf-8")
    store.write({"schema_version": 1, "value": "authority"})
    assert all(path.exists() for path in keep)


@pytest.mark.parametrize("timing,cut_index", [
    ("current_close", 1),
    ("next_open", 1),  # pending-order creation
    ("next_open", 2),  # carried open fill and target replacement
])
def test_fresh_process_product_recovery_matrix(tmp_path, timing, cut_index):
    """Producer, recovery, and continuation never share Product process memory."""
    from quant_futures.paper_runtime import Lifecycle, LifecycleState

    config, replay = _write_product_fixture(tmp_path, timing)
    expected = tmp_path / f"expected-{timing}-{cut_index}"
    _initialize_product_run(str(expected), str(config), str(replay))
    producer = multiprocessing.Process(
        target=_produce_product_cut,
        args=(str(expected), str(config), str(replay), len(replay.read_text().splitlines()), None),
    )
    producer.start(); producer.join(20)
    assert producer.exitcode == 0
    _recover_twice_then_continue(expected)

    expected_records = TransitionJournal(expected).records()
    # Select by the one-based transition cursor rather than relying on stage names.
    transition_ids = []
    for record in expected_records:
        if record.product_transition_id not in transition_ids:
            transition_ids.append(record.product_transition_id)
    target_id = transition_ids[cut_index]
    stages = [record.stage for record in expected_records
              if record.product_transition_id == target_id]

    for stage in stages:
        recovered = tmp_path / f"recovered-{timing}-{cut_index}-{stage}"
        _initialize_product_run(str(recovered), str(config), str(replay))
        child = multiprocessing.Process(
            target=_produce_product_cut,
            args=(str(recovered), str(config), str(replay), cut_index, stage),
        )
        child.start(); child.join(20)
        assert child.exitcode == 93
        # The cut happened after the named record became durable.
        assert TransitionJournal(recovered).records()[-1].stage == stage

        _recover_twice_then_continue(recovered)

        actual_records = TransitionJournal(recovered).records()
        assert (recovered / "transitions.journal").read_bytes() == (
            expected / "transitions.journal").read_bytes()
        assert actual_records == expected_records
        assert _checkpoint_core(recovered) == _checkpoint_core(expected)
        _assert_recovery_chain(recovered)
        _assert_recovery_chain(expected)
        assert Lifecycle(recovered).current().state is LifecycleState.COMPLETED

        event_ids = [record.journal_event_id for record in actual_records]
        input_ids = [record.payload["input_event_id"] for record in actual_records
                     if record.stage == "transition_started"]
        order_ids = [record.payload["order_id"] for record in actual_records
                     if record.stage == "order_submitted"]
        fill_ids = [record.payload["fill_id"] for record in actual_records
                    if record.stage == "fill_committed"]
        assert len(event_ids) == len(set(event_ids))
        assert len(input_ids) == len(set(input_ids))
        assert len(order_ids) == len(set(order_ids))
        assert len(fill_ids) == len(set(fill_ids))
        transition_ids = [record.product_transition_id for record in actual_records
                          if record.stage == "transition_started"]
        assert len(transition_ids) == len(set(transition_ids))


@pytest.mark.parametrize("boundary", [
    "recovery_started_durable",
    "recovery_suffix_checkpoint_durable",
    "recovery_outcome_durable",
    "checkpoint_temporary_written",
    "checkpoint_file_flush_completed",
    "checkpoint_file_fsync_completed",
    "checkpoint_atomic_replace_completed",
    "checkpoint_directory_fsync_completed",
    "checkpoint_post_directory_fsync_published",
    "recovery_commitment_published",
])
def test_public_recovery_publication_cuts_converge_in_fresh_processes(
    tmp_path, boundary,
):
    """Every public recovery publication cut is restartable and lease-clean."""
    from quant_futures.paper_runtime import RecoveryAttempts, RuntimeConsumerLease

    config, replay = _write_product_fixture(tmp_path, "current_close")
    run = tmp_path / "recovery-publication"
    baseline = tmp_path / "recovery-publication-baseline"
    for candidate in (baseline, run):
        _initialize_product_run(str(candidate), str(config), str(replay))
        producer = multiprocessing.Process(
            target=_produce_product_cut,
            args=(str(candidate), str(config), str(replay), 1,
                  "strategy_committed"),
        )
        producer.start(); producer.join(20)
        assert producer.exitcode == 93

    # The baseline begins at the identical durable incomplete Product prefix,
    # then converges without a recovery-publication crash.
    _recover_twice_then_continue(baseline)

    crashed = multiprocessing.Process(
        target=_crash_public_recovery, args=(str(run), boundary))
    crashed.start(); crashed.join(30)
    assert crashed.exitcode == 94
    assert not RuntimeConsumerLease.is_owned(run)

    # A second crash while reconciling the prior invocation, followed by two
    # entirely fresh coherent attempts and a separate continuation process.
    second = multiprocessing.Process(
        target=_crash_public_recovery,
        args=(str(run), "recovery_started_durable"),
    )
    second.start(); second.join(30)
    assert second.exitcode == 94
    _recover_twice_then_continue(run)

    records = RecoveryAttempts(run).read()
    assert len(records) % 2 == 0
    assert [record["outcome"] for record in records[::2]] == [
        "started"
    ] * (len(records) // 2)
    assert all(record["outcome"] in {"recovered", "no-op", "failed"}
               for record in records[1::2])
    assert all(records[index + 1]["previous_digest"] == records[index]["digest"]
               for index in range(len(records) - 1))
    checkpoint = CheckpointStore(run).read()
    assert checkpoint["recovery_counter"] == len(records) // 2
    assert checkpoint["recovery_digest"] == records[-1]["digest"]
    assert list(run.glob(".checkpoint.json.*.tmp")) == []
    assert not RuntimeConsumerLease.is_owned(run)

    # Recovery-publication crashes may add attempts/lifecycle records, but may
    # never alter the protected Product journal or bounded Product authority.
    actual = TransitionJournal(run).records()
    expected = TransitionJournal(baseline).records()
    assert (run / "transitions.journal").read_bytes() == (
        baseline / "transitions.journal").read_bytes()
    assert actual == expected
    assert _product_core(run) == _product_core(baseline)
    _assert_unique_product_identities(actual)
    assert not RuntimeConsumerLease.is_owned(baseline)
    assert list(baseline.glob(".checkpoint.json.*.tmp")) == []
