from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from threading import Barrier, Thread

import pytest

from quant_futures.alpha import AlphaDirection
from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioLedgerError
from quant_futures.domain.order import OrderStatus
from quant_futures.execution import FixedQuantityExecutionPolicy, PaperExecutionEngine
from quant_futures.portfolio import (
    PortfolioLedger, PortfolioSnapshot, PositionSide, PositionSnapshot, PositionUpdate,
)

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


def state(ledger):
    return (dict(ledger._positions), {key: tuple(value) for key, value in ledger._history.items()},
            dict(ledger._processed))


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
    before = state(ledger)
    object.__setattr__(previous, field, value)
    with pytest.raises(DomainValidationError):
        ledger.apply(filled(f"tamper-next-{field}", 110, when=NOW + timedelta(seconds=2)))
    assert state(ledger) == before
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


def test_concurrent_duplicate_has_one_success_and_different_fills_are_serialized():
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
