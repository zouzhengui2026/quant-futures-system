from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from threading import Barrier, Thread

import pytest

from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, OrderLifecycleError
from quant_futures.domain.order import OrderStatus
from quant_futures.execution import FixedQuantityExecutionPolicy, PaperExecutionEngine

from test_execution_engine import CREATED, risk_for

NOW = CREATED + timedelta(minutes=1)


def intent(*, order_id: str = "paper-1", price: float | None = None, direction=None):
    risk = risk_for() if direction is None else risk_for(direction)
    return FixedQuantityExecutionPolicy(
        quantity=2.0,
        price=price,
        clock=lambda: CREATED,
        order_id_factory=lambda: order_id,
    ).create_intent(risk)


def test_submit_fill_history_and_canonical_events_preserve_identity() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.EXECUTION_UPDATED, events.append)
    original = intent()
    engine = PaperExecutionEngine(bus, lambda: NOW)

    submitted = engine.submit(original)
    filled = engine.fill("paper-1", 60_000.0)

    assert submitted.order.status is OrderStatus.SUBMITTED
    assert filled.order.status is OrderStatus.FILLED
    assert filled.execution_intent is original
    assert original.order.status is OrderStatus.CREATED
    assert engine.get("paper-1") is filled
    assert engine.history("paper-1") == (submitted, filled)
    assert isinstance(engine.history("paper-1"), tuple)
    assert len(events) == 2
    assert set(events[-1].payload) == {
        "paper_execution_report", "execution_intent", "order", "previous_status",
        "risk_assessment", "decision_intent", "alpha_candidate",
        "timing_assessment", "observation",
    }
    assert events[-1].payload["paper_execution_report"] is filled
    assert events[-1].payload["execution_intent"] is original


def test_reject_and_cancel_are_terminal_and_require_reasons() -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    rejected = engine.reject(intent(order_id="rejected"), "risk gate")
    assert rejected.order.status is OrderStatus.REJECTED
    with pytest.raises(OrderLifecycleError):
        engine.fill("rejected", 1.0)
    submitted = engine.submit(intent(order_id="cancelled"))
    cancelled = engine.cancel("cancelled", "operator request")
    assert submitted.previous_status is OrderStatus.CREATED
    assert cancelled.previous_status is OrderStatus.SUBMITTED
    with pytest.raises(OrderLifecycleError):
        engine.cancel("cancelled", "again")
    with pytest.raises(DomainValidationError):
        engine.reject(intent(order_id="bad-reason"), " ")


def test_limit_prices_and_invalid_fill_are_rejected_without_state_change() -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    engine.submit(intent(order_id="buy", price=100.0))
    with pytest.raises(DomainValidationError):
        engine.fill("buy", 100.01)
    assert engine.get("buy").order.status is OrderStatus.SUBMITTED
    assert engine.fill("buy", 99.0).average_fill_price == 99.0

    from quant_futures.alpha import AlphaDirection
    engine.submit(intent(order_id="sell", price=100.0, direction=AlphaDirection.SHORT))
    with pytest.raises(DomainValidationError):
        engine.fill("sell", 99.99)
    assert engine.fill("sell", 101.0).order.status is OrderStatus.FILLED


def test_unknown_duplicate_invalid_ids_and_frozen_report() -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    value = intent()
    report = engine.submit(value)
    with pytest.raises((FrozenInstanceError, AttributeError)):
        report.reason = "changed"  # type: ignore[misc]
    report.validate()
    with pytest.raises(OrderLifecycleError):
        engine.submit(value)
    for operation in (engine.get, engine.history):
        with pytest.raises(OrderLifecycleError):
            operation("missing")
        with pytest.raises(OrderLifecycleError):
            operation(" ")


def test_clock_failure_and_mutation_are_atomic_and_publish_nothing() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.EXECUTION_UPDATED, events.append)
    original = intent()
    holder = {}

    def mutating_clock() -> datetime:
        object.__setattr__(original.order, "quantity", 3.0)
        return NOW

    engine = PaperExecutionEngine(bus, mutating_clock)
    holder["engine"] = engine
    with pytest.raises(DomainValidationError):
        engine.submit(original)
    assert events == []
    with pytest.raises(OrderLifecycleError):
        engine.get("paper-1")


def test_subscriber_failure_does_not_roll_back_committed_state() -> None:
    bus = EventBus()

    def fail(_event) -> None:
        raise RuntimeError("subscriber failed")

    bus.subscribe(EventType.EXECUTION_UPDATED, fail)
    engine = PaperExecutionEngine(bus, lambda: NOW)
    with pytest.raises(RuntimeError, match="subscriber failed"):
        engine.submit(intent())
    assert engine.get("paper-1").order.status is OrderStatus.SUBMITTED


def test_concurrent_fill_cancel_has_exactly_one_winner() -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    engine.submit(intent())
    barrier = Barrier(3)
    outcomes = []

    def run(operation) -> None:
        barrier.wait()
        try:
            operation()
            outcomes.append("ok")
        except OrderLifecycleError:
            outcomes.append("lifecycle")

    workers = [
        Thread(target=run, args=(lambda: engine.fill("paper-1", 1.0),)),
        Thread(target=run, args=(lambda: engine.cancel("paper-1", "race"),)),
    ]
    for worker in workers:
        worker.start()
    barrier.wait()
    for worker in workers:
        worker.join()
    assert sorted(outcomes) == ["lifecycle", "ok"]
    assert len(engine.history("paper-1")) == 2
