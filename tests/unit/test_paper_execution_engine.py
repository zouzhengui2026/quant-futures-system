from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from threading import Barrier, Event as ThreadEvent, RLock, Thread

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


@pytest.mark.parametrize("operation", ["submit", "reject", "fill", "cancel"])
def test_clock_reentrant_transitions_are_rejected_and_outer_transition_is_atomic(
    operation: str,
) -> None:
    original = intent()
    holder = {}

    def clock() -> datetime:
        engine = holder["engine"]
        calls = {
            "submit": lambda: engine.submit(intent(order_id="nested")),
            "reject": lambda: engine.reject(intent(order_id="nested"), "nested"),
            "fill": lambda: engine.fill("missing", 1.0),
            "cancel": lambda: engine.cancel("missing", "nested"),
        }
        calls[operation]()
        return NOW

    engine = PaperExecutionEngine(EventBus(), clock)
    holder["engine"] = engine
    with pytest.raises(OrderLifecycleError, match="re-entered"):
        engine.submit(original)
    with pytest.raises(OrderLifecycleError, match="unknown"):
        engine.get("paper-1")


@pytest.mark.parametrize("operation", ["submit", "reject", "fill", "cancel"])
def test_subscriber_reentrant_transitions_are_rejected_until_publish_finishes(
    operation: str,
) -> None:
    bus = EventBus()
    engine = PaperExecutionEngine(bus, lambda: NOW)
    nested_errors = []

    def subscriber(_event) -> None:
        calls = {
            "submit": lambda: engine.submit(intent(order_id="nested")),
            "reject": lambda: engine.reject(intent(order_id="nested"), "nested"),
            "fill": lambda: engine.fill("paper-1", 1.0),
            "cancel": lambda: engine.cancel("paper-1", "nested"),
        }
        try:
            calls[operation]()
        except OrderLifecycleError as exc:
            nested_errors.append(exc)

    bus.subscribe(EventType.EXECUTION_UPDATED, subscriber)
    report = engine.submit(intent())
    assert report.order.status is OrderStatus.SUBMITTED
    assert len(nested_errors) == 1
    assert "re-entered" in str(nested_errors[0])
    assert engine.history("paper-1") == (report,)


def test_event_publication_is_serialized_in_history_order() -> None:
    bus = EventBus()
    first_event_entered = ThreadEvent()
    release_first_event = ThreadEvent()
    published = []

    def subscriber(event) -> None:
        published.append(event.payload["paper_execution_report"])
        if len(published) == 1:
            first_event_entered.set()
            assert release_first_event.wait(timeout=2)

    bus.subscribe(EventType.EXECUTION_UPDATED, subscriber)
    engine = PaperExecutionEngine(bus, lambda: NOW)
    first = Thread(target=lambda: engine.submit(intent()))
    second = Thread(target=lambda: engine.fill("paper-1", 1.0))
    first.start()
    assert first_event_entered.wait(timeout=2)
    second.start()
    assert len(published) == 1
    assert second.is_alive()
    release_first_event.set()
    first.join(timeout=2)
    second.join(timeout=2)
    assert published == list(engine.history("paper-1"))


@pytest.mark.parametrize("replacement", ["list", "report"])
def test_clock_cannot_replace_history_list_or_report_with_equal_clone(replacement: str) -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    submitted = engine.submit(intent())
    original_history_dict = engine._history
    original_history = engine._history["paper-1"]

    def clock() -> datetime:
        if replacement == "list":
            engine._history["paper-1"] = list(original_history)
        else:
            original_history[0] = replace(submitted)
        return NOW

    engine.clock = clock
    with pytest.raises(DomainValidationError, match="replace"):
        engine.fill("paper-1", 1.0)
    assert engine.get("paper-1") is submitted
    assert engine._history is original_history_dict
    assert engine._history["paper-1"] is original_history
    assert engine._history["paper-1"][0] is submitted
    assert engine.history("paper-1") == (submitted,)


def test_clock_cannot_replace_current_execution_intent_with_equal_clone() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.EXECUTION_UPDATED, events.append)
    original_intent = intent()
    engine = PaperExecutionEngine(bus, lambda: NOW)
    submitted = engine.submit(original_intent)
    clone = None

    def clock() -> datetime:
        nonlocal clone
        clone = replace(submitted.execution_intent)
        object.__setattr__(submitted, "execution_intent", clone)
        return NOW

    engine.clock = clock
    with pytest.raises(DomainValidationError, match="execution intent"):
        engine.fill("paper-1", 1.0)
    assert clone is not None and clone is not original_intent
    assert engine.get("paper-1") is submitted
    assert engine.history("paper-1") == (submitted,)
    assert engine.history("paper-1")[0] is submitted
    assert submitted.execution_intent is original_intent
    assert len(events) == 1
    assert all(event.payload["order"].status is not OrderStatus.FILLED for event in events)


def test_clock_cannot_replace_current_order_with_equal_clone() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.EXECUTION_UPDATED, events.append)
    engine = PaperExecutionEngine(bus, lambda: NOW)
    submitted = engine.submit(intent())
    original_submitted_order = submitted.order
    clone = None

    def clock() -> datetime:
        nonlocal clone
        clone = replace(submitted.order)
        object.__setattr__(submitted, "order", clone)
        return NOW

    engine.clock = clock
    with pytest.raises(DomainValidationError, match="current report order"):
        engine.fill("paper-1", 1.0)
    assert clone is not None and clone is not original_submitted_order
    assert engine.get("paper-1") is submitted
    assert engine.history("paper-1") == (submitted,)
    assert engine.history("paper-1")[0] is submitted
    assert submitted.order is original_submitted_order
    assert len(events) == 1


def test_clock_exception_releases_guard_without_committing_or_publishing() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.EXECUTION_UPDATED, events.append)

    def broken_clock() -> datetime:
        raise RuntimeError("clock failed")

    engine = PaperExecutionEngine(bus, broken_clock)
    with pytest.raises(RuntimeError, match="clock failed"):
        engine.submit(intent())
    assert events == []
    engine.clock = lambda: NOW
    assert engine.submit(intent()).order.status is OrderStatus.SUBMITTED


@pytest.mark.parametrize("terminal", ["filled", "cancelled", "rejected"])
def test_all_terminal_states_reject_further_transitions(terminal: str) -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    original = intent()
    if terminal == "rejected":
        engine.reject(original, "terminal")
    else:
        engine.submit(original)
        if terminal == "filled":
            engine.fill("paper-1", 1.0)
        else:
            engine.cancel("paper-1", "terminal")
    for transition in (
        lambda: engine.submit(original),
        lambda: engine.reject(original, "again"),
        lambda: engine.fill("paper-1", 1.0),
        lambda: engine.cancel("paper-1", "again"),
    ):
        with pytest.raises(OrderLifecycleError):
            transition()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda report: object.__setattr__(report, "occurred_at", NOW + timedelta(days=1)),
        lambda report: object.__setattr__(report, "reason", "tampered"),
        lambda report: object.__setattr__(report, "previous_status", OrderStatus.REJECTED),
        lambda report: object.__setattr__(report.order, "quantity", 99.0),
        lambda report: object.__setattr__(report.order, "status", OrderStatus.FILLED),
        lambda report: object.__setattr__(
            report.execution_intent, "created_at", NOW + timedelta(days=1)
        ),
        lambda report: object.__setattr__(report.execution_intent.order, "quantity", 99.0),
    ],
)
def test_clock_in_place_current_report_tampering_is_fully_rolled_back(mutate) -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.EXECUTION_UPDATED, events.append)
    engine = PaperExecutionEngine(bus, lambda: NOW)
    submitted = engine.submit(intent())
    value_snapshot = deepcopy(submitted)
    original_order = submitted.order
    original_intent = submitted.execution_intent

    def clock() -> datetime:
        mutate(submitted)
        return NOW

    engine.clock = clock
    with pytest.raises(DomainValidationError):
        engine.fill("paper-1", 1.0)
    assert engine.get("paper-1") is submitted
    assert engine.history("paper-1")[0] is submitted
    assert submitted == value_snapshot
    assert submitted.order is original_order
    assert submitted.execution_intent is original_intent
    submitted.validate()
    assert len(events) == 1


@pytest.mark.parametrize(
    "selector,field,value",
    [
        (lambda report: report.execution_intent.risk_assessment, "policy_name", "tampered"),
        (
            lambda report: report.execution_intent.risk_assessment.decision_intent,
            "policy_name",
            "tampered",
        ),
        (
            lambda report: report.execution_intent.risk_assessment.decision_intent.alpha_candidate,
            "model_name",
            "tampered",
        ),
        (
            lambda report: report.execution_intent.risk_assessment.decision_intent.alpha_candidate.timing_assessment,
            "confidence",
            0.1,
        ),
        (
            lambda report: report.execution_intent.risk_assessment.decision_intent.alpha_candidate.observation,
            "price",
            1.0,
        ),
    ],
)
def test_clock_lineage_tampering_restores_values_and_identities(selector, field, value) -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    submitted = engine.submit(intent())
    lineage = (
        submitted.execution_intent.risk_assessment,
        submitted.execution_intent.risk_assessment.decision_intent,
        submitted.execution_intent.risk_assessment.decision_intent.alpha_candidate,
        submitted.execution_intent.risk_assessment.decision_intent.alpha_candidate.timing_assessment,
        submitted.execution_intent.risk_assessment.decision_intent.alpha_candidate.observation,
    )
    snapshots = tuple(deepcopy(item) for item in lineage)

    def clock() -> datetime:
        object.__setattr__(selector(submitted), field, value)
        return NOW

    engine.clock = clock
    with pytest.raises(DomainValidationError):
        engine.cancel("paper-1", "cancel")
    restored = (
        submitted.execution_intent.risk_assessment,
        submitted.execution_intent.risk_assessment.decision_intent,
        submitted.execution_intent.risk_assessment.decision_intent.alpha_candidate,
        submitted.execution_intent.risk_assessment.decision_intent.alpha_candidate.timing_assessment,
        submitted.execution_intent.risk_assessment.decision_intent.alpha_candidate.observation,
    )
    assert all(actual is original for actual, original in zip(restored, lineage, strict=True))
    assert restored == snapshots
    submitted.validate()


@pytest.mark.parametrize(
    "mutation",
    ["history_dict", "history_list", "report_clone", "report_time", "order_quantity",
     "intent_clone", "insert_report", "delete_report"],
)
def test_every_history_tampering_mode_is_transactionally_restored(mutation: str) -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    submitted = engine.submit(intent())
    history_dict = engine._history
    history_list = history_dict["paper-1"]
    value_snapshot = deepcopy(submitted)

    def clock() -> datetime:
        if mutation == "history_dict":
            engine._history = dict(engine._history)
        elif mutation == "history_list":
            engine._history["paper-1"] = list(history_list)
        elif mutation == "report_clone":
            history_list[0] = replace(submitted)
        elif mutation == "report_time":
            object.__setattr__(submitted, "occurred_at", NOW + timedelta(days=1))
        elif mutation == "order_quantity":
            object.__setattr__(submitted.order, "quantity", 99.0)
        elif mutation == "intent_clone":
            object.__setattr__(submitted, "execution_intent", replace(submitted.execution_intent))
        elif mutation == "insert_report":
            history_list.append(replace(submitted))
        else:
            history_list.clear()
        return NOW

    engine.clock = clock
    with pytest.raises((DomainValidationError, OrderLifecycleError)):
        engine.fill("paper-1", 1.0)
    assert engine._history is history_dict
    assert engine._history["paper-1"] is history_list
    assert history_list == [submitted]
    assert history_list[0] is submitted
    assert submitted == value_snapshot


@pytest.mark.parametrize("phase", ["register", "advance"])
@pytest.mark.parametrize("mutation", ["insert", "delete", "current", "list", "report"])
def test_unrelated_ledger_tampering_is_transactionally_restored(
    phase: str, mutation: str
) -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    unrelated = engine.submit(intent(order_id="unrelated"))
    target = engine.submit(intent(order_id="target")) if phase == "advance" else None
    current_dict, history_dict = engine._current, engine._history
    unrelated_list = history_dict["unrelated"]

    def clock() -> datetime:
        if mutation == "insert":
            engine._current["intruder"] = unrelated
            engine._history["intruder"] = [unrelated]
        elif mutation == "delete":
            del engine._current["unrelated"]
            del engine._history["unrelated"]
        elif mutation == "current":
            engine._current["unrelated"] = replace(unrelated)
        elif mutation == "list":
            engine._history["unrelated"] = list(unrelated_list)
        else:
            object.__setattr__(unrelated, "reason", "tampered")
        return NOW

    engine.clock = clock
    with pytest.raises(DomainValidationError):
        if phase == "register":
            engine.submit(intent(order_id="target"))
        else:
            engine.fill("target", 1.0)
    assert engine._current is current_dict
    assert engine._history is history_dict
    expected_keys = {"unrelated", "target"} if phase == "advance" else {"unrelated"}
    assert set(current_dict) == expected_keys
    assert set(history_dict) == expected_keys
    assert current_dict["unrelated"] is unrelated
    assert history_dict["unrelated"] is unrelated_list
    assert unrelated_list == [unrelated]
    assert unrelated.reason is None
    if target is not None:
        assert current_dict["target"] is target


def test_clock_cannot_bypass_reentry_guard_by_resetting_observable_bool() -> None:
    bus, events = EventBus(), []
    bus.subscribe(EventType.EXECUTION_UPDATED, events.append)
    calls = 0
    engine = None

    def clock() -> datetime:
        nonlocal calls
        calls += 1
        assert engine is not None
        engine._transition_active = False
        engine.submit(intent(order_id="nested"))
        return NOW

    engine = PaperExecutionEngine(bus, clock)
    with pytest.raises(OrderLifecycleError, match="re-entered"):
        engine.submit(intent(order_id="outer"))
    assert calls == 1
    assert engine._current == {}
    assert engine._history == {}
    assert events == []
    assert engine._transition_active is False
    engine.clock = lambda: NOW
    assert engine.submit(intent(order_id="recovered")).order.status is OrderStatus.SUBMITTED


def test_clock_event_bus_replacement_is_rolled_back_without_publication() -> None:
    original_bus, original_events = EventBus(), []
    replacement_bus, replacement_events = EventBus(), []
    original_bus.subscribe(EventType.EXECUTION_UPDATED, original_events.append)
    replacement_bus.subscribe(EventType.EXECUTION_UPDATED, replacement_events.append)
    engine = PaperExecutionEngine(original_bus, lambda: NOW)

    def clock() -> datetime:
        engine.event_bus = replacement_bus
        return NOW

    engine.clock = clock
    with pytest.raises(DomainValidationError, match="event bus"):
        engine.submit(intent())
    assert engine.event_bus is original_bus
    assert engine._current == {} and engine._history == {}
    assert original_events == [] and replacement_events == []
    assert engine._transition_active is False


def test_clock_lock_replacement_is_rolled_back_and_engine_remains_serialized() -> None:
    engine = PaperExecutionEngine(EventBus(), lambda: NOW)
    original_lock = engine._lock

    def clock() -> datetime:
        engine._lock = RLock()
        return NOW

    engine.clock = clock
    with pytest.raises(DomainValidationError, match="lifecycle lock"):
        engine.submit(intent(order_id="failed"))
    assert engine._lock is original_lock
    assert engine._current == {} and engine._history == {}
    assert engine._transition_active is False

    engine.clock = lambda: NOW
    barrier, outcomes = Barrier(3), []

    def submit_concurrently(order_id: str) -> None:
        barrier.wait()
        outcomes.append(engine.submit(intent(order_id=order_id)).order.order_id)

    workers = [
        Thread(target=submit_concurrently, args=("one",)),
        Thread(target=submit_concurrently, args=("two",)),
    ]
    for worker in workers:
        worker.start()
    barrier.wait()
    for worker in workers:
        worker.join()
    assert sorted(outcomes) == ["one", "two"]
    assert set(engine._current) == {"one", "two"}


def test_subscriber_cannot_bypass_reentry_guard_by_resetting_observable_bool() -> None:
    bus, events = EventBus(), []
    engine = PaperExecutionEngine(bus, lambda: NOW)
    nested_errors = []

    def subscriber(event) -> None:
        events.append(event)
        engine._transition_active = False
        try:
            engine.fill("paper-1", 1.0)
        except OrderLifecycleError as exc:
            nested_errors.append(exc)

    bus.subscribe(EventType.EXECUTION_UPDATED, subscriber)
    submitted = engine.submit(intent())
    assert engine.history("paper-1") == (submitted,)
    assert len(events) == 1 and len(nested_errors) == 1
    assert "re-entered" in str(nested_errors[0])
    assert engine._transition_active is False
