from dataclasses import FrozenInstanceError
from datetime import timedelta

import pytest

from quant_futures.alpha import AlphaDirection
from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioLedgerError
from quant_futures.execution import PaperExecutionEngine
from quant_futures.portfolio import PortfolioLedger, PositionSide

from test_paper_execution_engine import NOW, intent


def filled(order_id: str, price: float, *, direction=None, when=NOW):
    engine = PaperExecutionEngine(EventBus(), lambda: when)
    engine.submit(intent(order_id=order_id, direction=direction))
    return engine.fill(order_id, price)


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
