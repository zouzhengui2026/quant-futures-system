from copy import deepcopy
from contextvars import Context
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone, tzinfo
import gc
from threading import Barrier, Event as ThreadEvent, RLock, Thread
import weakref

import pytest

from quant_futures.alpha import AlphaDirection
from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioLedgerError
from quant_futures.domain.order import OrderStatus
from quant_futures.execution import FixedQuantityExecutionPolicy, PaperExecutionEngine
from quant_futures.portfolio import (
    PortfolioLedger, PortfolioSnapshot, PositionSide, PositionSnapshot, PositionUpdate,
)
from quant_futures.portfolio.ledger import _LEDGER_ANCHORS

from test_paper_execution_engine import NOW, intent
from test_execution_engine import risk_for


def filled(order_id: str, price: float, *, direction=None, when=NOW, quantity=2.0):
    engine = PaperExecutionEngine(EventBus(), lambda: when)
    risk = risk_for() if direction is None else risk_for(direction)
    value = FixedQuantityExecutionPolicy(
        quantity=quantity, clock=lambda: intent(order_id=order_id).created_at,
        order_id_factory=lambda: order_id,
    ).create_intent(risk)
    engine.submit(value)
    return engine.fill(order_id, price)


def filled_for(order_id: str, price: float, source: str, symbol: str, *, when=NOW):
    """Build a valid report for an arbitrary accounting key."""
    base = risk_for()
    observation = replace(base.decision_intent.alpha_candidate.observation,
                          source=source, symbol=symbol)
    timing = replace(base.decision_intent.alpha_candidate.timing_assessment,
                     observation=observation)
    alpha = replace(base.decision_intent.alpha_candidate, source=source, symbol=symbol,
                    observation=observation, timing_assessment=timing)
    decision = replace(base.decision_intent, source=source, symbol=symbol,
                       alpha_candidate=alpha)
    risk = replace(base, source=source, symbol=symbol, decision_intent=decision)
    value = FixedQuantityExecutionPolicy(
        quantity=2.0, clock=lambda: intent(order_id=order_id).created_at,
        order_id_factory=lambda: order_id,
    ).create_intent(risk)
    engine = PaperExecutionEngine(EventBus(), lambda: when)
    engine.submit(value)
    return engine.fill(order_id, price)


def state(ledger):
    return (dict(ledger._positions), {key: tuple(value) for key, value in ledger._history.items()},
            dict(ledger._processed))


def test_fresh_context_cannot_bypass_same_thread_reentry_guard() -> None:
    bus, events = EventBus(), []
    ledger = PortfolioLedger(bus)
    nested = filled("fresh-context-nested", 101.0)

    def subscriber(event) -> None:
        events.append(event)
        with pytest.raises(PortfolioLedgerError, match="re-entered"):
            Context().run(ledger.apply, nested)

    bus.subscribe(EventType.PORTFOLIO_UPDATED, subscriber)
    outer = ledger.apply(filled("fresh-context-outer", 100.0))

    assert ledger.processed("fresh-context-outer") is outer
    with pytest.raises(PortfolioLedgerError, match="unknown order_id"):
        ledger.processed("fresh-context-nested")
    assert ledger.history(outer.current_position.source, outer.current_position.symbol) == (outer,)
    assert len(events) == 1


def test_replacing_exposed_lock_cannot_overlap_transition_or_reorder_events() -> None:
    bus, published = EventBus(), []
    ledger = PortfolioLedger(bus)
    original_lock = ledger._lock
    publication_entered, release_publication = ThreadEvent(), ThreadEvent()
    second_started, second_finished = ThreadEvent(), ThreadEvent()

    def subscriber(event) -> None:
        published.append(event.payload["execution_report"].order.order_id)
        if len(published) == 1:
            ledger._lock = RLock()
            publication_entered.set()
            assert release_publication.wait(2)

    bus.subscribe(EventType.PORTFOLIO_UPDATED, subscriber)

    def apply_second() -> None:
        assert publication_entered.wait(2)
        second_started.set()
        ledger.apply(filled_for("lock-second", 101.0, "other", "OTHER"))
        second_finished.set()

    thread = Thread(target=apply_second)
    thread.start()
    outer_done = ThreadEvent()
    outer = Thread(target=lambda: (ledger.apply(filled("lock-first", 100.0)), outer_done.set()))
    outer.start()
    assert publication_entered.wait(2)
    assert second_started.wait(2)
    assert not second_finished.is_set()
    release_publication.set()
    outer.join(2)
    thread.join(2)
    assert outer_done.is_set() and second_finished.is_set()
    assert published == ["lock-first", "lock-second"]
    assert ledger._lock is original_lock


def test_callback_infrastructure_replacement_is_repaired_after_publication() -> None:
    original, replacement = EventBus(), EventBus()
    original_events, replacement_events = [], []
    ledger = PortfolioLedger(original)

    def corrupt(event) -> None:
        original_events.append(event)
        ledger._lock = object()
        ledger.event_bus = replacement
        raise RuntimeError("subscriber failure")

    unsubscribe = original.subscribe(EventType.PORTFOLIO_UPDATED, corrupt)
    replacement.subscribe(EventType.PORTFOLIO_UPDATED, replacement_events.append)
    with pytest.raises(RuntimeError, match="subscriber failure"):
        ledger.apply(filled("infrastructure-first", 100.0))
    assert ledger.event_bus is original
    assert hasattr(ledger._lock, "acquire")
    unsubscribe()
    ledger.apply(filled("infrastructure-second", 101.0))
    assert len(original_events) == 1
    assert replacement_events == []


def test_instance_dictionary_rewrite_cannot_forge_audit_anchor() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.PORTFOLIO_UPDATED, events.append)
    ledger = PortfolioLedger(bus)
    first = ledger.apply(filled("anchor-first", 100.0))
    ledger.apply(filled("anchor-second", 101.0))
    forged = replace(first)
    key = (first.current_position.source, first.current_position.symbol)
    ledger._history[key][0] = forged
    ledger._processed["anchor-first"] = forged
    ledger._committed_identity = dict(ledger._committed_identity)
    ledger._committed_identity["anchor-first"] = replace(
        ledger._committed_identity["anchor-first"], update=forged)

    with pytest.raises(PortfolioLedgerError, match="anchor was replaced"):
        ledger.apply(filled("anchor-third", 102.0))
    assert "anchor-third" not in ledger._processed
    assert len(ledger._history[key]) == 2
    assert len(events) == 2


def test_in_place_commitment_rewrite_cannot_forge_audit_anchor() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.PORTFOLIO_UPDATED, events.append)
    ledger = PortfolioLedger(bus)
    first = ledger.apply(filled("forge-one", 100.0))
    ledger.apply(filled("forge-two", 101.0, when=NOW + timedelta(seconds=1)))
    forged = replace(first)
    key = (first.current_position.source, first.current_position.symbol)
    order_id = first.execution_report.order.order_id
    positions = dict(ledger._positions)
    history_length = len(ledger._history[key])
    processed_keys = set(ledger._processed)

    ledger._history[key][0] = forged
    ledger._processed[order_id] = forged
    ledger._committed_identity[order_id] = replace(
        ledger._committed_identity[order_id], update=forged,
    )

    with pytest.raises(PortfolioLedgerError, match="commitment entry was replaced"):
        ledger.apply(filled("forge-three", 102.0, when=NOW + timedelta(seconds=2)))
    assert ledger._positions == positions
    assert len(ledger._history[key]) == history_length
    assert set(ledger._processed) == processed_keys
    assert "forge-three" not in ledger._processed
    assert len(events) == 2


@pytest.mark.parametrize("mutation", ["add", "remove", "wrong"])
def test_in_place_commitment_mapping_mutation_is_detected(mutation) -> None:
    ledger = PortfolioLedger(EventBus())
    first = ledger.apply(filled("commitment-one", 100.0))
    order_id = first.execution_report.order.order_id
    if mutation == "add":
        ledger._committed_identity["unexpected"] = ledger._committed_identity[order_id]
    elif mutation == "remove":
        del ledger._committed_identity[order_id]
    else:
        ledger._committed_identity[order_id] = replace(
            ledger._committed_identity[order_id])

    with pytest.raises(PortfolioLedgerError, match="commitment"):
        ledger.apply(filled("commitment-two", 101.0, when=NOW + timedelta(seconds=1)))
    assert "commitment-two" not in ledger._processed


def test_ledger_authority_registry_uses_weak_ownership() -> None:
    gc.collect()
    baseline = len(_LEDGER_ANCHORS)
    ledger = PortfolioLedger(EventBus())
    assert ledger in _LEDGER_ANCHORS
    assert len(_LEDGER_ANCHORS) == baseline + 1
    ledger_ref = weakref.ref(ledger)

    del ledger
    gc.collect()

    assert ledger_ref() is None
    assert len(_LEDGER_ANCHORS) == baseline


def test_long_average_cost_reduce_close_and_flat_history() -> None:
    ledger = PortfolioLedger(EventBus())
    first = ledger.apply(filled("one", 100.0))
    second = ledger.apply(filled("two", 120.0))
    reduced = ledger.apply(filled("three", 130.0, direction=AlphaDirection.SHORT))
    closed = ledger.apply(filled("four", 90.0, direction=AlphaDirection.SHORT))

    assert first.current_position.side is PositionSide.LONG
    assert second.current_position.average_entry_price == 110.0
    assert reduced.realized_pnl_delta == 40.0
    assert closed.current_position.side is PositionSide.FLAT
    assert closed.current_position.average_entry_price is None
    assert closed.current_position.realized_pnl == 0.0
    key = (closed.current_position.source, closed.current_position.symbol)
    assert ledger.get(*key) is closed.current_position
    assert ledger.history(*key) == (first, second, reduced, closed)


def test_short_average_cost_profit_and_reversal() -> None:
    ledger = PortfolioLedger(EventBus())
    ledger.apply(filled("short-one", 120.0, direction=AlphaDirection.SHORT))
    added = ledger.apply(filled("short-two", 100.0, direction=AlphaDirection.SHORT))
    assert added.current_position.average_entry_price == 110.0
    reduced = ledger.apply(filled("buy-one", 90.0))
    assert reduced.realized_pnl_delta == 40.0

    # The standard helper creates quantity two; tamper-free larger fills are
    # exercised by first reducing the short and then opening long from flat.
    closed = ledger.apply(filled("buy-two", 130.0))
    opened = ledger.apply(filled("buy-three", 80.0))
    assert closed.current_position.side is PositionSide.FLAT
    assert opened.current_position.side is PositionSide.LONG
    assert opened.current_position.realized_pnl == 0.0


def test_event_payload_identity_snapshot_sorting_and_duplicate_rejection() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.PORTFOLIO_UPDATED, events.append)
    ledger = PortfolioLedger(bus)
    report = filled("original", 100.0)
    update = ledger.apply(report)

    assert set(events[0].payload) == {
        "position_update", "execution_report", "previous_position",
        "current_position", "portfolio_snapshot", "execution_intent",
        "risk_assessment", "decision_intent", "alpha_candidate",
        "timing_assessment", "observation",
    }
    assert events[0].payload["position_update"] is update
    assert events[0].payload["execution_report"] is report
    assert ledger.processed("original") is update
    with pytest.raises(PortfolioLedgerError):
        ledger.apply(report)
    assert len(events) == 1


def test_out_of_order_unknown_queries_and_immutable_results() -> None:
    ledger = PortfolioLedger(EventBus())
    later = filled("later", 100.0, when=NOW + timedelta(seconds=1))
    ledger.apply(later)
    with pytest.raises(PortfolioLedgerError):
        ledger.apply(filled("earlier", 100.0, when=NOW))
    with pytest.raises(PortfolioLedgerError):
        ledger.get("test", "missing")
    with pytest.raises(PortfolioLedgerError):
        ledger.processed("missing")
    assert isinstance(ledger.positions(), tuple)
    position = ledger.positions()[0]
    assert isinstance(ledger.history(position.source, position.symbol), tuple)


def test_models_are_frozen_repeatably_validated_and_fail_closed() -> None:
    position = PortfolioLedger(EventBus()).apply(filled("frozen", 100.0)).current_position
    with pytest.raises((FrozenInstanceError, AttributeError)):
        position.realized_pnl = 1.0  # type: ignore[misc]
    position.validate()
    object.__setattr__(position, "side", PositionSide.SHORT)
    with pytest.raises(DomainValidationError):
        position.validate()


def test_subscriber_reads_committed_state_reentry_and_failure_semantics() -> None:
    bus = EventBus()
    ledger = PortfolioLedger(bus)
    seen = []

    def subscriber(event) -> None:
        update = event.payload["position_update"]
        seen.append(ledger.processed(update.execution_report.order.order_id))
        ledger._transition_active = False
        with pytest.raises(PortfolioLedgerError, match="re-entered"):
            ledger.apply(filled("nested", 1.0))
        raise RuntimeError("subscriber failed")

    bus.subscribe(EventType.PORTFOLIO_UPDATED, subscriber)
    with pytest.raises(RuntimeError, match="subscriber failed"):
        ledger.apply(filled("committed", 100.0))
    assert seen == [ledger.processed("committed")]


@pytest.mark.parametrize(
    "direction,first_qty,first_price,second_qty,second_price,side,quantity,average,delta",
    [
        (None, 2, 100, 3, 120, PositionSide.LONG, 5, 112, 0),
        (None, 5, 100, 2, 130, PositionSide.LONG, 3, 100, 60),
        (None, 5, 100, 2, 80, PositionSide.LONG, 3, 100, -40),
        (None, 2, 100, 2, 120, PositionSide.FLAT, 0, None, 40),
        (None, 2, 100, 5, 120, PositionSide.SHORT, -3, 120, 40),
        (AlphaDirection.SHORT, 2, 120, 3, 100, PositionSide.SHORT, -5, 108, 0),
        (AlphaDirection.SHORT, 5, 100, 2, 80, PositionSide.SHORT, -3, 100, 40),
        (AlphaDirection.SHORT, 5, 100, 2, 120, PositionSide.SHORT, -3, 100, -40),
        (AlphaDirection.SHORT, 2, 100, 2, 80, PositionSide.FLAT, 0, None, 40),
        (AlphaDirection.SHORT, 2, 100, 5, 80, PositionSide.LONG, 3, 80, 40),
    ],
)
def test_accounting_transition_matrix(direction, first_qty, first_price, second_qty,
                                      second_price, side, quantity, average, delta):
    ledger = PortfolioLedger(EventBus())
    ledger.apply(filled("matrix-1", first_price, direction=direction, quantity=first_qty))
    second_direction = direction if delta == 0 else (
        AlphaDirection.SHORT if direction is None else None)
    update = ledger.apply(filled("matrix-2", second_price, direction=second_direction,
                                 quantity=second_qty))
    assert (update.current_position.side, update.current_position.signed_quantity,
            update.current_position.average_entry_price, update.realized_pnl_delta) == (
                side, quantity, average, delta)


def test_cumulative_realized_and_flat_position_remains_queryable():
    ledger = PortfolioLedger(EventBus())
    ledger.apply(filled("cum-1", 100, quantity=4))
    ledger.apply(filled("cum-2", 120, direction=AlphaDirection.SHORT, quantity=2))
    closed = ledger.apply(filled("cum-3", 80, direction=AlphaDirection.SHORT, quantity=2))
    assert closed.current_position.realized_pnl == 0.0
    assert ledger.get("replay", "BTCUSDT") is closed.current_position


def test_empty_snapshot_and_stable_cancellation_total():
    ledger = PortfolioLedger(EventBus())
    empty = ledger.snapshot()
    assert empty.positions == () and empty.total_realized_pnl == 0.0
    template = ledger.apply(filled("stable", 100)).current_position
    positions = {}
    for symbol, pnl in (("A", 1e16), ("B", 1.0), ("C", -1e16)):
        position = replace(template, symbol=symbol, realized_pnl=pnl)
        positions[(position.source, symbol)] = position
    snapshot = ledger._make_snapshot(positions)
    snapshot.validate()
    assert snapshot.total_realized_pnl == 1.0
    assert [position.symbol for position in snapshot.positions] == ["A", "B", "C"]


@pytest.mark.parametrize("field,value", [
    ("signed_quantity", -2.0), ("side", PositionSide.SHORT),
    ("average_entry_price", 101.0), ("realized_pnl", 1.0),
    ("updated_at", NOW + timedelta(seconds=1)), ("last_order_id", "wrong"),
])
def test_tampered_stored_previous_fails_atomically(field, value):
    bus, events = EventBus(), []
    bus.subscribe(EventType.PORTFOLIO_UPDATED, events.append)
    ledger = PortfolioLedger(bus)
    previous = ledger.apply(filled(f"tamper-first-{field}", 100)).current_position
    positions_dict, history_dict, processed_dict = (
        ledger._positions, ledger._history, ledger._processed)
    key = (previous.source, previous.symbol)
    history_list = ledger._history[key]
    original_updates = tuple(history_list)
    before_values = deepcopy(state(ledger))
    object.__setattr__(previous, field, value)
    # Detection is fail-closed, not restoration of adversarial pre-existing edits.
    tampered_values = deepcopy(state(ledger))
    with pytest.raises(DomainValidationError):
        ledger.apply(filled(f"tamper-next-{field}", 110, when=NOW + timedelta(seconds=2)))
    assert ledger._positions is positions_dict
    assert ledger._history is history_dict
    assert ledger._processed is processed_dict
    assert ledger._history[key] is history_list
    assert ledger._positions[key] is previous
    assert tuple(ledger._history[key]) == original_updates
    assert all(actual is expected for actual, expected in zip(history_list, original_updates))
    assert ledger._processed[original_updates[0].execution_report.order.order_id] is original_updates[0]
    assert len(ledger._positions) == len(positions_dict) == 1
    assert len(ledger._processed) == len(processed_dict) == 1
    assert state(ledger) == tampered_values
    assert state(ledger) != before_values
    assert len(events) == 1


@pytest.mark.parametrize("field,value", [
    ("signed_quantity", 999.0), ("side", PositionSide.SHORT),
    ("average_entry_price", 5.0), ("realized_pnl", 3.0),
    ("updated_at", NOW + timedelta(seconds=1)), ("last_order_id", "wrong"),
])
def test_position_update_rejects_incorrect_accounting(field, value):
    update = PortfolioLedger(EventBus()).apply(filled(f"invalid-{field}", 100))
    changes = {field: value}
    if field == "side":
        changes["signed_quantity"] = -update.current_position.signed_quantity
    current = replace(update.current_position, **changes)
    portfolio = PortfolioSnapshot((current,), current.realized_pnl, current.updated_at)
    with pytest.raises(DomainValidationError):
        replace(update, current_position=current, portfolio_snapshot=portfolio)


@pytest.mark.parametrize("status", [OrderStatus.SUBMITTED, OrderStatus.CANCELLED, OrderStatus.REJECTED])
def test_rejects_every_non_filled_report_status(status):
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    value = intent(order_id=f"status-{status.value}")
    if status is OrderStatus.SUBMITTED:
        report = engine.submit(value)
    elif status is OrderStatus.REJECTED:
        report = engine.reject(value, "no")
    else:
        engine.submit(value)
        report = engine.cancel(value.order.order_id, "no")
    with pytest.raises(PortfolioLedgerError):
        PortfolioLedger(EventBus()).apply(report)


def test_concurrent_same_report_has_exactly_one_success():
    ledger, barrier, outcomes = PortfolioLedger(EventBus()), Barrier(3), []
    report = filled("race", 100)
    def run():
        barrier.wait()
        try:
            ledger.apply(report)
            outcomes.append("ok")
        except PortfolioLedgerError:
            outcomes.append("duplicate")
    threads = [Thread(target=run), Thread(target=run)]
    for thread in threads: thread.start()
    barrier.wait()
    for thread in threads: thread.join()
    assert sorted(outcomes) == ["duplicate", "ok"]
    assert ledger.history("replay", "BTCUSDT") == (ledger.processed("race"),)


def test_all_models_are_frozen_slotted_and_repeatably_validate():
    update = PortfolioLedger(EventBus()).apply(filled("all-frozen", 100))
    for model in (update.current_position, update.portfolio_snapshot, update):
        assert not hasattr(model, "__dict__")
        model.validate(); model.validate()
        with pytest.raises((FrozenInstanceError, AttributeError)):
            setattr(model, next(iter(model.__dataclass_fields__)), None)


def test_identical_symbols_from_different_sources_are_independent():
    ledger = PortfolioLedger(EventBus())
    first = ledger.apply(filled_for("source-a", 100, "a", "BTCUSDT"))
    second = ledger.apply(filled_for("source-b", 200, "b", "BTCUSDT"))
    assert ledger.positions() == (first.current_position, second.current_position)
    assert ledger.history("a", "BTCUSDT") == (first,)
    assert ledger.history("b", "BTCUSDT") == (second,)


@pytest.mark.parametrize("operation,args", [
    ("get", ("", "BTCUSDT")), ("get", (" ", "BTCUSDT")),
    ("get", (1, "BTCUSDT")), ("get", ("replay", "")),
    ("get", ("replay", "\t")), ("get", ("replay", object())),
    ("history", ("", "BTCUSDT")), ("history", ("replay", " ")),
    ("processed", ("",)), ("processed", (" \n",)), ("processed", (False,)),
])
def test_query_strings_are_strictly_validated(operation, args):
    with pytest.raises(DomainValidationError):
        getattr(PortfolioLedger(EventBus()), operation)(*args)


def test_non_report_and_unknown_history_are_rejected():
    ledger = PortfolioLedger(EventBus())
    with pytest.raises(DomainValidationError):
        ledger.apply(object())
    with pytest.raises(PortfolioLedgerError, match="unknown position"):
        ledger.history("replay", "missing")


@pytest.mark.parametrize("quantity,side,average", [
    (2.0, PositionSide.LONG, 10.0),
    (-2.0, PositionSide.SHORT, 10.0),
    (0.0, PositionSide.FLAT, None),
])
def test_position_snapshot_accepts_all_valid_sides(quantity, side, average):
    PositionSnapshot("source", "symbol", quantity, side, average, 0.0, NOW, "order").validate()


@pytest.mark.parametrize("field,value", [
    ("source", ""), ("source", "  "), ("source", 1),
    ("symbol", ""), ("symbol", None), ("last_order_id", "\t"),
    ("signed_quantity", True), ("signed_quantity", float("nan")),
    ("signed_quantity", float("inf")), ("realized_pnl", float("-inf")),
    ("updated_at", datetime(2026, 1, 1)),
])
def test_position_snapshot_rejects_invalid_scalars(field, value):
    values = dict(source="s", symbol="x", signed_quantity=1.0,
                  side=PositionSide.LONG, average_entry_price=10.0,
                  realized_pnl=0.0, updated_at=NOW, last_order_id="o")
    values[field] = value
    with pytest.raises(DomainValidationError):
        PositionSnapshot(**values)


class _BrokenTimezone(tzinfo):
    def utcoffset(self, dt):
        raise ValueError("broken timezone")


def test_position_snapshot_rejects_timezone_that_cannot_compute_offset():
    with pytest.raises(DomainValidationError):
        PositionSnapshot("s", "x", 1, PositionSide.LONG, 10, 0,
                         datetime(2026, 1, 1, tzinfo=_BrokenTimezone()), "o")


@pytest.mark.parametrize("quantity,side,average", [
    (1, PositionSide.SHORT, 10), (-1, PositionSide.LONG, 10),
    (0, PositionSide.LONG, 10), (0, PositionSide.SHORT, 10),
    (0, PositionSide.FLAT, 10), (1, PositionSide.LONG, None),
    (-1, PositionSide.SHORT, None), (1, PositionSide.LONG, 0),
    (-1, PositionSide.SHORT, -1), (1, PositionSide.LONG, float("nan")),
    (-1, PositionSide.SHORT, float("inf")),
])
def test_position_snapshot_rejects_every_side_and_average_mismatch(quantity, side, average):
    with pytest.raises(DomainValidationError):
        PositionSnapshot("s", "x", quantity, side, average, 0, NOW, "o")


def test_portfolio_snapshot_rejects_duplicates_sorting_total_and_timestamp():
    base = PortfolioLedger(EventBus()).apply(filled("portfolio-invalid", 100)).current_position
    earlier = replace(base, source="a", symbol="a")
    later = replace(base, source="b", symbol="b", updated_at=NOW + timedelta(seconds=1))
    invalid = [
        ((earlier, earlier), earlier.realized_pnl * 2, earlier.updated_at),
        ((later, earlier), 0.0, later.updated_at),
        ((earlier,), 1.0, earlier.updated_at),
        ((earlier,), 0.0, later.updated_at),
    ]
    for positions, total, updated_at in invalid:
        with pytest.raises(DomainValidationError):
            PortfolioSnapshot(positions, total, updated_at)


@pytest.mark.parametrize("model_name,field,value", [
    ("position", "side", PositionSide.SHORT),
    ("portfolio", "total_realized_pnl", 99.0),
    ("update", "realized_pnl_delta", 99.0),
])
def test_all_models_validate_fail_closed_after_adversarial_tampering(model_name, field, value):
    update = PortfolioLedger(EventBus()).apply(filled(f"tamper-model-{model_name}", 100))
    model = {"position": update.current_position,
             "portfolio": update.portfolio_snapshot, "update": update}[model_name]
    object.__setattr__(model, field, value)
    with pytest.raises(DomainValidationError):
        model.validate()


def test_success_preserves_exact_complete_lineage_identity():
    bus, events = EventBus(), []
    bus.subscribe(EventType.PORTFOLIO_UPDATED, events.append)
    ledger = PortfolioLedger(bus)
    report = filled("lineage", 100)
    update = ledger.apply(report)
    payload = events[0].payload
    intent_value = report.execution_intent
    risk = intent_value.risk_assessment
    decision = risk.decision_intent
    alpha = decision.alpha_candidate
    expected = {
        "position_update": update, "execution_report": report,
        "previous_position": update.previous_position,
        "current_position": update.current_position,
        "portfolio_snapshot": update.portfolio_snapshot,
        "execution_intent": intent_value, "risk_assessment": risk,
        "decision_intent": decision, "alpha_candidate": alpha,
        "timing_assessment": alpha.timing_assessment,
        "observation": alpha.observation,
    }
    assert set(payload) == set(expected)
    assert all(payload[name] is value for name, value in expected.items())
    assert ledger.history("replay", "BTCUSDT")[0] is update
    assert ledger.processed("lineage") is update


def test_subscriber_observes_every_committed_query_before_delivery():
    bus, observed = EventBus(), []
    ledger = PortfolioLedger(bus)
    def subscriber(event):
        update = event.payload["position_update"]
        position = update.current_position
        observed.append((ledger.get(position.source, position.symbol), ledger.positions(),
                         ledger.history(position.source, position.symbol),
                         ledger.processed(position.last_order_id), ledger.snapshot()))
    bus.subscribe(EventType.PORTFOLIO_UPDATED, subscriber)
    update = ledger.apply(filled("visible", 100))
    assert observed == [(update.current_position, (update.current_position,), (update,),
                         update, update.portfolio_snapshot)]


def test_concurrent_different_keys_are_serialized_without_lost_updates():
    bus, events = EventBus(), []
    bus.subscribe(EventType.PORTFOLIO_UPDATED, events.append)
    ledger = PortfolioLedger(bus)
    barrier, release = Barrier(3), ThreadEvent()
    reports = [filled_for("parallel-a", 100, "a", "BTC"),
               filled_for("parallel-b", 200, "b", "ETH")]
    outcomes = []
    def run(report):
        barrier.wait()
        release.wait()
        outcomes.append(ledger.apply(report))
    workers = [Thread(target=run, args=(report,)) for report in reports]
    for worker in workers: worker.start()
    barrier.wait(); release.set()
    for worker in workers: worker.join()
    committed = [event.payload["position_update"] for event in events]
    assert len(outcomes) == len(committed) == 2
    assert {id(update) for update in outcomes} == {id(update) for update in committed}
    assert [len(update.portfolio_snapshot.positions) for update in committed] == [1, 2]
    assert ledger.positions() == tuple(sorted(
        (update.current_position for update in committed),
        key=lambda position: (position.source, position.symbol)))
    for update in committed:
        position = update.current_position
        assert ledger.history(position.source, position.symbol) == (update,)
        assert ledger.processed(position.last_order_id) is update


def _two_fill_ledger():
    bus, events = EventBus(), []
    bus.subscribe(EventType.PORTFOLIO_UPDATED, events.append)
    ledger = PortfolioLedger(bus)
    first = ledger.apply(filled("audit-one", 100, when=NOW))
    second = ledger.apply(filled("audit-two", 110, when=NOW + timedelta(seconds=1)))
    return ledger, events, first, second


def _assert_audit_corruption_rejects_third(ledger, events):
    key = ("replay", "BTCUSDT")
    positions, history, processed = ledger._positions, ledger._history, ledger._processed
    history_list = ledger._history[key]
    position_count, history_count, processed_count = (
        len(positions), len(history_list), len(processed))
    with pytest.raises((DomainValidationError, PortfolioLedgerError)):
        ledger.apply(filled("audit-three", 120, when=NOW + timedelta(seconds=2)))
    assert ledger._positions is positions
    assert ledger._history is history
    assert ledger._processed is processed
    assert ledger._history[key] is history_list
    assert len(positions) == position_count
    assert len(history_list) == history_count
    assert len(processed) == processed_count
    assert "audit-three" not in processed
    assert len(events) == 2


@pytest.mark.parametrize("field,replacement", [
    ("realized_pnl_delta", lambda update: 999.0),
    ("execution_report", lambda update: replace(update.execution_report)),
    ("current_position", lambda update: replace(update.current_position)),
    ("portfolio_snapshot", lambda update: replace(update.portfolio_snapshot)),
])
def test_older_update_tampering_is_detected_before_third_commit(field, replacement):
    ledger, events, first, _ = _two_fill_ledger()
    object.__setattr__(first, field, replacement(first))
    _assert_audit_corruption_rejects_third(ledger, events)


def test_replacing_history_entry_with_equal_clone_is_detected():
    ledger, events, first, _ = _two_fill_ledger()
    ledger._history[("replay", "BTCUSDT")][0] = replace(first)
    _assert_audit_corruption_rejects_third(ledger, events)


def test_broken_previous_current_identity_chain_is_detected():
    ledger, events, first, second = _two_fill_ledger()
    object.__setattr__(second, "previous_position", replace(first.current_position))
    _assert_audit_corruption_rejects_third(ledger, events)


@pytest.mark.parametrize("mutation", ["remove", "add"])
def test_removed_or_added_history_entry_is_detected(mutation):
    ledger, events, first, _ = _two_fill_ledger()
    history = ledger._history[("replay", "BTCUSDT")]
    if mutation == "remove":
        history.pop(0)
    else:
        history.append(first)
    _assert_audit_corruption_rejects_third(ledger, events)


@pytest.mark.parametrize("mutation", ["clone", "wrong", "remove", "add"])
def test_processed_mapping_corruption_is_detected(mutation):
    ledger, events, first, second = _two_fill_ledger()
    if mutation == "clone":
        ledger._processed["audit-one"] = replace(first)
    elif mutation == "wrong":
        ledger._processed["audit-one"] = second
    elif mutation == "remove":
        del ledger._processed["audit-one"]
    else:
        ledger._processed["unexpected"] = first
    _assert_audit_corruption_rejects_third(ledger, events)


def test_position_not_identical_to_last_history_position_is_detected():
    ledger, events, _, second = _two_fill_ledger()
    ledger._positions[("replay", "BTCUSDT")] = replace(second.current_position)
    _assert_audit_corruption_rejects_third(ledger, events)


def test_valid_complete_multi_fill_audit_state_continues_to_apply():
    ledger, events, first, second = _two_fill_ledger()
    third = ledger.apply(filled("audit-three", 120, when=NOW + timedelta(seconds=2)))
    assert ledger.history("replay", "BTCUSDT") == (first, second, third)
    assert ledger.processed("audit-three") is third
    assert len(events) == 3


@pytest.mark.parametrize("lineage_field", [
    "order",
    "execution_intent",
    "risk_assessment",
    "decision_intent",
    "alpha_candidate",
    "timing_assessment",
    "observation",
])
def test_equal_lineage_clone_is_rejected_before_subsequent_commit(lineage_field):
    ledger, events, first, _ = _two_fill_ledger()
    report = first.execution_report
    execution_intent = report.execution_intent
    risk = execution_intent.risk_assessment
    decision = risk.decision_intent
    alpha = decision.alpha_candidate
    owners = {
        "order": (report, report.order),
        "execution_intent": (report, execution_intent),
        "risk_assessment": (execution_intent, risk),
        "decision_intent": (risk, decision),
        "alpha_candidate": (decision, alpha),
        "timing_assessment": (alpha, alpha.timing_assessment),
        "observation": (alpha, alpha.observation),
    }
    owner, original = owners[lineage_field]
    committed_identity = ledger._committed_identity
    committed_records = dict(committed_identity)
    object.__setattr__(owner, lineage_field, replace(original))

    _assert_audit_corruption_rejects_third(ledger, events)
    assert ledger._committed_identity is committed_identity
    assert ledger._committed_identity == committed_records


@pytest.mark.parametrize("lineage_path", [
    "execution_intent.order",
    "timing_assessment.observation",
])
def test_equal_duplicate_lineage_child_clone_is_rejected(lineage_path):
    ledger, events, first, _ = _two_fill_ledger()
    intent = first.execution_report.execution_intent
    alpha = intent.risk_assessment.decision_intent.alpha_candidate
    if lineage_path == "execution_intent.order":
        owner, field, original = intent, "order", intent.order
    else:
        owner, field, original = (
            alpha.timing_assessment, "observation",
            alpha.timing_assessment.observation,
        )
    positions = ledger._positions
    history = ledger._history
    processed = ledger._processed
    commitments = ledger._committed_identity
    history_list = ledger._history[("replay", "BTCUSDT")]
    updates = tuple(history_list)
    object.__setattr__(owner, field, replace(original))

    _assert_audit_corruption_rejects_third(ledger, events)
    assert ledger._positions is positions
    assert ledger._history is history
    assert ledger._processed is processed
    assert ledger._committed_identity is commitments
    assert ledger._history[("replay", "BTCUSDT")] is history_list
    assert tuple(history_list) == updates


def test_equal_non_current_portfolio_position_clone_is_rejected():
    bus, events = EventBus(), []
    bus.subscribe(EventType.PORTFOLIO_UPDATED, events.append)
    ledger = PortfolioLedger(bus)
    first = ledger.apply(filled_for("multi-a", 100, "alpha", "BTCUSDT"))
    second = ledger.apply(filled_for("multi-b", 200, "zeta", "ETHUSDT"))
    portfolio = second.portfolio_snapshot
    assert portfolio.positions == (first.current_position, second.current_position)
    object.__setattr__(portfolio, "positions", (
        replace(first.current_position), second.current_position,
    ))
    positions = ledger._positions
    history = ledger._history
    processed = ledger._processed
    committed_identity = ledger._committed_identity

    with pytest.raises((DomainValidationError, PortfolioLedgerError)):
        ledger.apply(filled_for(
            "multi-c", 210, "zeta", "ETHUSDT", when=NOW + timedelta(seconds=1)))
    assert ledger._positions is positions
    assert ledger._history is history
    assert ledger._processed is processed
    assert ledger._committed_identity is committed_identity
    assert "multi-c" not in processed
    assert len(events) == 2
