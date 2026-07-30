from datetime import datetime, timedelta, timezone
from multiprocessing import Event, Process, Queue
from threading import Event as ThreadEvent, Thread
import time

import pytest

from quant_futures.paper_runtime import (
    Lifecycle, LifecycleState, PaperTransitionCoordinator, RunLockError, StageProtocol,
    TransitionError,
)
from quant_futures.paper_runtime.journal import TransitionJournal
from quant_futures.product.config import CostConfig, DataConfig, ProductConfig
from quant_futures.product.data import Bar
from quant_futures.product.engine import simulate
from quant_futures.product.strategy import FixedStrategy, HoldStrategy


class SequenceStrategy:
    name = "sequence"
    version = "1"

    def __init__(self, values):
        self.values = iter(values)

    def target(self, context):
        return next(self.values)


class SlowStrategy:
    name = "slow"
    version = "1"

    def __init__(self, entered, release):
        self.entered, self.release = entered, release

    def target(self, context):
        self.entered.set()
        assert self.release.wait(5)
        return 0.0


def config(*, timing="current_close", commission=0.0, slippage=0.0):
    return ProductConfig("paper", DataConfig("unused.csv"),
                         costs=CostConfig(commission, slippage, 0.0),
                         fill_timing=timing)


def bar(n, *, close=100.0, open_=100.0, funding=None):
    timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(hours=n)
    return Bar(timestamp, open_, max(open_, close), min(open_, close), close, 1.0, funding)


def stages(journal):
    return [record.stage for record in journal.iter_records()]

def running_lifecycle(path, run_id):
    lifecycle = Lifecycle(path)
    lifecycle.initialize(run_id)
    lifecycle.transition(LifecycleState.STARTING, "start")
    lifecycle.transition(LifecycleState.RUNNING, "run")
    return lifecycle


def authorized_coordinator(run_id, cfg, strategy, journal, failure_injector=None):
    """Focused-test harness that explicitly materializes production lifecycle authority."""
    lifecycle_path = journal.run_directory / Lifecycle.filename
    if not lifecycle_path.exists():
        running_lifecycle(journal.run_directory, run_id)
    return PaperTransitionCoordinator(run_id, cfg, strategy, journal, failure_injector)


def _run_slow_transition(path, entered, release, outcomes):
    try:
        coordinator = PaperTransitionCoordinator(
            "process-run", config(), SlowStrategy(entered, release), TransitionJournal(path))
        coordinator.transition(bar(0))
        outcomes.put(("transition", "committed"))
    except BaseException as exc:
        outcomes.put(("transition", type(exc).__name__))


def _attempt_transition(path, outcomes):
    try:
        coordinator = PaperTransitionCoordinator(
            "process-run", config(), HoldStrategy(), TransitionJournal(path))
        coordinator.transition(bar(1))
        outcomes.put(("second", "committed"))
    except BaseException as exc:
        outcomes.put(("second", type(exc).__name__))


def _attempt_control(path, target, outcomes):
    try:
        Lifecycle(path).transition(target, target.value.lower())
        outcomes.put((target.value, "committed"))
    except BaseException as exc:
        outcomes.put((target.value, type(exc).__name__))



def test_no_order_transition_and_status_use_canonical_state(tmp_path):
    journal = TransitionJournal(tmp_path)
    coordinator = authorized_coordinator("run-a", config(), HoldStrategy(), journal)
    state = coordinator.transition(bar(0))
    assert stages(journal) == [
        "transition_started", "input_committed", "strategy_committed",
        "portfolio_committed", "account_committed", "risk_committed",
        "transition_committed",
    ]
    assert state.input_cursor == 1
    assert coordinator.status_projection()["equity"] == state.account_snapshot.equity
    assert coordinator.status_projection()["positions"] == []
    assert state.last_committed_ordering_key == "2025-01-01T00:00:00Z"


@pytest.mark.parametrize("authority", ["missing", "corrupt", "paused", "wrong-run"])
def test_transition_requires_running_matching_lifecycle_authority(tmp_path, authority):
    if authority == "corrupt":
        (tmp_path / Lifecycle.filename).write_text("not-json\n", encoding="utf-8")
    elif authority == "paused":
        lifecycle = running_lifecycle(tmp_path, "authority-run")
        lifecycle.transition(LifecycleState.PAUSED, "pause")
    elif authority == "wrong-run":
        running_lifecycle(tmp_path, "different-run")
    coordinator = PaperTransitionCoordinator(
        "authority-run", config(), HoldStrategy(), TransitionJournal(tmp_path))
    before = (tmp_path / "transitions.journal").read_bytes() if (
        tmp_path / "transitions.journal").exists() else b""
    with pytest.raises(TransitionError, match="lifecycle"):
        coordinator.transition(bar(0))
    after = (tmp_path / "transitions.journal").read_bytes() if (
        tmp_path / "transitions.journal").exists() else b""
    assert after == before == b""


@pytest.mark.parametrize("candidate", [
    bar(0, close=101.0),
    bar(-1),
])
def test_committed_market_timestamp_is_a_strict_bounded_ordering_key(tmp_path, candidate):
    journal = TransitionJournal(tmp_path)
    coordinator = authorized_coordinator(
        "ordered-run", config(), HoldStrategy(), journal)
    first = coordinator.transition(bar(0))
    before = (tmp_path / "transitions.journal").read_bytes()
    with pytest.raises(TransitionError, match="strictly increasing"):
        coordinator.transition(candidate)
    assert (tmp_path / "transitions.journal").read_bytes() == before
    assert first.last_committed_ordering_key == "2025-01-01T00:00:00Z"


def test_rejects_nonadjacent_replay_before_journal_append(tmp_path):
    journal = TransitionJournal(tmp_path)
    coordinator = authorized_coordinator(
        "ordered-replay", config(), HoldStrategy(), journal)
    coordinator.transition(bar(0))
    coordinator.transition(bar(1))
    before = (tmp_path / "transitions.journal").read_bytes()
    with pytest.raises(TransitionError, match="strictly increasing"):
        coordinator.transition(bar(0))
    assert (tmp_path / "transitions.journal").read_bytes() == before


def test_current_close_costs_funding_and_canonical_outputs(tmp_path):
    coordinator = authorized_coordinator(
        "run-b", config(commission=10.0, slippage=10.0), FixedStrategy(1.0),
        TransitionJournal(tmp_path))
    first = coordinator.transition(bar(0))
    second = coordinator.transition(bar(1, close=110.0, funding=0.01))
    position = second.portfolio_snapshot.positions[0]
    assert position.signed_quantity == 1.0
    assert position.average_entry_price == pytest.approx(100.1)
    # Both fee and explicit funding flow through AccountEquityEngine cash_flow.
    assert second.account_snapshot.cash_flow == pytest.approx(-1.2001)
    assert second.risk_snapshot.account_snapshot is second.account_snapshot
    assert first.counters.fills == 1 and second.counters.fills == 1


def test_matches_product_v01_account_portfolio_and_risk_path(tmp_path):
    cfg = config(commission=2.0, slippage=1.0)
    bars = (bar(0), bar(1, close=110.0, funding=0.001))
    expected = simulate(cfg, bars, FixedStrategy(1.0))
    coordinator = authorized_coordinator(
        "canonical-comparison", cfg, FixedStrategy(1.0), TransitionJournal(tmp_path))
    states = tuple(coordinator.transition(item) for item in bars)
    for state, record in zip(states, expected):
        assert state.account_snapshot.equity == record.equity
        assert state.portfolio_snapshot.positions[0].signed_quantity == record.quantity
        assert state.risk_snapshot.drawdown_ratio == record.drawdown


def test_next_open_creates_then_fills_pending_order(tmp_path):
    journal = TransitionJournal(tmp_path)
    coordinator = authorized_coordinator(
        "run-c", config(timing="next_open"), FixedStrategy(1.0), journal)
    first = coordinator.transition(bar(0))
    assert first.pending_order is not None and first.counters.fills == 0
    second = coordinator.transition(bar(1, open_=105.0, close=106.0))
    assert second.pending_order is None and second.counters.fills == 1
    assert second.portfolio_snapshot.positions[0].average_entry_price == 105.0


def test_next_open_target_change_records_real_causal_order_and_payloads(tmp_path):
    journal = TransitionJournal(tmp_path)
    coordinator = authorized_coordinator(
        "changing", config(timing="next_open", commission=10.0, slippage=10.0),
        SequenceStrategy((1.0, -1.0)), journal)
    first = coordinator.transition(bar(0))
    prior_order = first.pending_order.order.order_id
    second = coordinator.transition(bar(1, open_=105.0, close=106.0, funding=0.01))
    records = [r for r in journal.iter_records() if r.product_transition_id == journal.tail().product_transition_id]
    assert [r.stage for r in records] == [
        "transition_started", "input_committed", "fill_prepared", "fill_committed",
        "portfolio_committed", "strategy_committed", "order_submitted",
        "account_committed", "risk_committed", "transition_committed",
    ]
    prepared = next(r for r in records if r.stage == "fill_prepared")
    committed = next(r for r in records if r.stage == "fill_committed")
    account = next(r for r in records if r.stage == "account_committed")
    assert prepared.payload["order_id"] == committed.payload["order_id"] == prior_order
    assert prepared.payload["fill_price"] == pytest.approx(105.105)
    assert account.payload["commission"] == pytest.approx(0.105105)
    assert account.payload["funding"] == 0.0  # position was flat at the funding boundary
    assert second.pending_order is not None
    assert second.pending_order.order.order_id != prior_order


def test_stage_protocol_rejects_illegal_and_duplicate_stages():
    protocol = StageProtocol()
    with pytest.raises(TransitionError):
        protocol.accept("strategy_committed")
    protocol.accept("transition_started")
    with pytest.raises(TransitionError):
        protocol.accept("transition_started")
    protocol = StageProtocol()
    protocol.accept("transition_started")
    protocol.accept("input_committed")
    with pytest.raises(TransitionError, match="fill_prepared"):
        protocol.accept("fill_committed")


def test_deterministic_byte_stable_journal(tmp_path):
    paths = [tmp_path / "one", tmp_path / "two"]
    payloads = []
    for path in paths:
        path.mkdir()
        coordinator = authorized_coordinator(
            "same-run", config(), FixedStrategy(1.0), TransitionJournal(path))
        coordinator.transition(bar(0))
        payloads.append((path / "transitions.journal").read_bytes())
    assert payloads[0] == payloads[1]


@pytest.mark.parametrize("failed_stage", [
    "transition_started", "input_committed", "strategy_committed", "order_submitted",
    "fill_prepared", "fill_committed", "portfolio_committed", "account_committed",
    "risk_committed", "transition_committed",
])
def test_any_journal_boundary_failure_poisons_coordinator(
    tmp_path, monkeypatch, failed_stage,
):
    journal = TransitionJournal(tmp_path)
    coordinator = authorized_coordinator("run-d", config(), FixedStrategy(1.0), journal)
    original = journal._append_held

    def fail_commit(*args, **kwargs):
        if args[2] == failed_stage:
            raise OSError("durability failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(journal, "_append_held", fail_commit)
    with pytest.raises(OSError):
        coordinator.transition(bar(0))
    assert coordinator.state.input_cursor == 0
    with pytest.raises(TransitionError, match="unusable"):
        coordinator.transition(bar(1))


def test_reopening_completed_or_partial_journal_fails_closed(tmp_path, monkeypatch):
    journal = TransitionJournal(tmp_path)
    coordinator = authorized_coordinator("run-e", config(), HoldStrategy(), journal)
    coordinator.transition(bar(0))
    with pytest.raises(TransitionError, match="strictly increasing"):
        coordinator.transition(bar(0))
    with pytest.raises(TransitionError, match="unusable"):
        coordinator.transition(bar(1))
    with pytest.raises(TransitionError, match="Checkpoint 4"):
        authorized_coordinator("run-e", config(), HoldStrategy(), TransitionJournal(tmp_path))

    partial = tmp_path / "partial"
    partial.mkdir()
    journal = TransitionJournal(partial)
    coordinator = authorized_coordinator("run-f", config(), HoldStrategy(), journal)
    original = journal._append_held
    monkeypatch.setattr(journal, "_append_held", lambda *a, **k: (
        (_ for _ in ()).throw(OSError("cut")) if a[2] == "strategy_committed" else original(*a, **k)
    ))
    with pytest.raises(OSError):
        coordinator.transition(bar(0))
    with pytest.raises(TransitionError, match="Checkpoint 4"):
        authorized_coordinator("run-f", config(), HoldStrategy(), TransitionJournal(partial))


@pytest.mark.parametrize("boundary", [
    "strategy", "submit", "fill", "portfolio", "account", "risk",
])
def test_canonical_authority_failure_poisons_coordinator(tmp_path, boundary):
    def inject(candidate):
        if candidate == boundary:
            raise RuntimeError(f"injected {boundary}")

    coordinator = authorized_coordinator(
        "authority-cut", config(), FixedStrategy(1.0), TransitionJournal(tmp_path), inject)
    with pytest.raises(RuntimeError, match=boundary):
        coordinator.transition(bar(0))
    with pytest.raises(TransitionError, match="unusable"):
        coordinator.transition(bar(1))


def test_whole_transition_excludes_second_coordinator_and_control(tmp_path):
    lifecycle = Lifecycle(tmp_path)
    lifecycle.initialize("locked-run")
    lifecycle.transition(LifecycleState.STARTING, "start")
    lifecycle.transition(LifecycleState.RUNNING, "run")
    entered, release = ThreadEvent(), ThreadEvent()
    first = authorized_coordinator(
        "locked-run", config(), SlowStrategy(entered, release), TransitionJournal(tmp_path))
    second = authorized_coordinator(
        "locked-run", config(), HoldStrategy(), TransitionJournal(tmp_path))
    outcomes = []

    def run_first():
        outcomes.append(("first", first.transition(bar(0))))

    def run_second():
        try:
            second.transition(bar(1))
        except (TransitionError, RunLockError) as exc:
            outcomes.append(("second", str(exc)))

    def pause():
        try:
            lifecycle.transition(LifecycleState.PAUSED, "pause")
        except RunLockError as exc:
            outcomes.append(("pause", str(exc)))

    workers = [Thread(target=run_first), Thread(target=run_second), Thread(target=pause)]
    workers[0].start()
    assert entered.wait(5)
    workers[1].start()
    workers[2].start()
    time.sleep(0.05)
    assert sorted(name for name, _ in outcomes) == ["pause", "second"]
    release.set()
    for worker in workers:
        worker.join(5)
        assert not worker.is_alive()
    records = tuple(TransitionJournal(tmp_path).iter_records())
    transition_ids = [record.product_transition_id for record in records]
    assert len(set(transition_ids)) == 1
    assert transition_ids == [transition_ids[0]] * len(records)
    assert [name for name, _ in outcomes].count("first") == 1
    assert [name for name, _ in outcomes].count("pause") == 1
    assert [name for name, _ in outcomes].count("second") == 1
    lifecycle.transition(LifecycleState.PAUSED, "pause after product commit")


def test_multiprocess_transition_excludes_product_and_controls(tmp_path):
    lifecycle = running_lifecycle(tmp_path, "process-run")
    lifecycle_before = (tmp_path / Lifecycle.filename).read_bytes()
    entered, release, outcomes = Event(), Event(), Queue()
    first = Process(target=_run_slow_transition,
                    args=(tmp_path, entered, release, outcomes))
    first.start()
    assert entered.wait(5)

    contenders = [
        Process(target=_attempt_transition, args=(tmp_path, outcomes)),
        Process(target=_attempt_control,
                args=(tmp_path, LifecycleState.PAUSED, outcomes)),
        Process(target=_attempt_control,
                args=(tmp_path, LifecycleState.STOPPING, outcomes)),
    ]
    for process in contenders:
        process.start()
    for process in contenders:
        process.join(5)
        assert process.exitcode == 0

    blocked = dict(outcomes.get(timeout=2) for _ in contenders)
    assert blocked == {
        "second": "RunLockError",
        "PAUSED": "RunLockError",
        "STOPPING": "RunLockError",
    }
    assert (tmp_path / Lifecycle.filename).read_bytes() == lifecycle_before

    release.set()
    first.join(5)
    assert first.exitcode == 0
    assert outcomes.get(timeout=2) == ("transition", "committed")
    records = tuple(TransitionJournal(tmp_path).iter_records())
    assert records
    assert len({record.product_transition_id for record in records}) == 1
    assert [record.sequence for record in records] == list(range(1, len(records) + 1))
    assert lifecycle.current().state is LifecycleState.RUNNING
