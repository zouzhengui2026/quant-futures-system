"""Phase 11 deterministic account-equity coverage."""

from concurrent.futures import ThreadPoolExecutor
from contextvars import Context
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import gc
import math
import weakref

import pytest

from quant_futures.account import AccountEquityEngine, AccountSnapshot, PositionValuation
from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import AccountValuationError, DomainValidationError
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.portfolio.models import PortfolioSnapshot, PositionSide, PositionSnapshot

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def position(symbol="BTC", quantity=2.0, average=100.0, realized=3.0, at=NOW):
    side = PositionSide.FLAT if quantity == 0 else (
        PositionSide.LONG if quantity > 0 else PositionSide.SHORT)
    return PositionSnapshot("sim", symbol, quantity, side,
                            None if quantity == 0 else average, realized, at, f"order-{symbol}")


def portfolio(*positions):
    ordered = tuple(sorted(positions, key=lambda p: (p.source, p.symbol)))
    return PortfolioSnapshot(ordered, math.fsum(p.realized_pnl for p in ordered),
                             max((p.updated_at for p in ordered),
                                 default=datetime(1970, 1, 1, tzinfo=timezone.utc)))


def mark(p, price, at=NOW):
    return MarketDataRecord(MarketDataKind.MARK_PRICE, p.symbol, p.source, at, {"price": price})


@pytest.mark.parametrize(("quantity", "price", "expected"), [
    (2.0, 110.0, 20.0), (2.0, 90.0, -20.0),
    (-2.0, 90.0, 20.0), (-2.0, 110.0, -20.0),
])
def test_long_and_short_mark_to_market(quantity, price, expected):
    p = position(quantity=quantity)
    record = mark(p, price)
    result = AccountEquityEngine(EventBus(), 1_000).value(portfolio(p), (record,))
    assert result.valuations[0].position is p
    assert result.valuations[0].mark_record is record
    assert result.total_unrealized_pnl == expected
    assert result.equity == 1_000 + p.realized_pnl + expected


def test_empty_and_flat_positions_need_no_marks():
    engine = AccountEquityEngine(EventBus(), 100)
    empty = engine.value(portfolio(), ())
    assert empty.valuations == () and empty.equity == 100
    flat = position(quantity=0, average=None)
    valued = engine.value(portfolio(flat), ())
    assert valued.valuations[0].mark_record is None
    assert valued.valuations[0].unrealized_pnl == 0.0


def test_event_payload_and_committed_reads_preserve_identity():
    bus = EventBus()
    engine = AccountEquityEngine(bus, 100)
    p, seen = position(), []
    records = (mark(p, 101),)
    def subscriber(event):
        seen.append((event, engine.latest(), engine.history()))
    bus.subscribe(EventType.ACCOUNT_UPDATED, subscriber)
    result = engine.value(portfolio(p), records)
    event, latest, history = seen[0]
    assert set(event.payload) == {"account_snapshot", "portfolio_snapshot", "valuations", "mark_records"}
    assert event.payload["account_snapshot"] is result is latest is history[0]
    assert event.payload["mark_records"] is records


@pytest.mark.parametrize("records", [[], None, {}])
def test_marks_must_be_tuple(records):
    p = position()
    with pytest.raises(AccountValuationError):
        AccountEquityEngine(EventBus(), 0).value(portfolio(p), records)


def test_marks_must_be_exact_complete_set():
    p, extra = position(), position("ETH")
    engine = AccountEquityEngine(EventBus(), 0)
    for records in ((), (mark(p, 100), mark(p, 101)), (mark(p, 100), mark(extra, 10))):
        with pytest.raises(AccountValuationError):
            engine.value(portfolio(p), records)
    wrong = MarketDataRecord(MarketDataKind.INDEX_PRICE, p.symbol, p.source, NOW, {"price": 100})
    with pytest.raises(AccountValuationError):
        engine.value(portfolio(p), (wrong,))


@pytest.mark.parametrize("price", [True, 0, -1, math.nan, math.inf])
def test_invalid_mark_prices_fail_without_commit_or_event(price):
    bus, p, seen = EventBus(), position(), []
    bus.subscribe(EventType.ACCOUNT_UPDATED, seen.append)
    engine = AccountEquityEngine(bus, 100)
    try:
        record = mark(p, price)
    except Exception:
        record = None
    if record is not None:
        with pytest.raises((AccountValuationError, DomainValidationError)):
            engine.value(portfolio(p), (record,))
    assert engine.history() == () and seen == []


def test_stale_and_out_of_order_valuations_are_rejected():
    p = position(at=NOW)
    engine = AccountEquityEngine(EventBus(), 100)
    with pytest.raises((AccountValuationError, DomainValidationError)):
        engine.value(portfolio(p), (mark(p, 100, NOW - timedelta(seconds=1)),))
    engine.value(portfolio(p), (mark(p, 100, NOW + timedelta(seconds=2)),))
    with pytest.raises(AccountValuationError):
        engine.value(portfolio(p), (mark(p, 100, NOW + timedelta(seconds=1)),))


def test_subscriber_exception_propagates_after_commit():
    bus, p = EventBus(), position()
    engine = AccountEquityEngine(bus, 100)
    bus.subscribe(EventType.ACCOUNT_UPDATED, lambda event: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError, match="boom"):
        engine.value(portfolio(p), (mark(p, 100),))
    assert len(engine.history()) == 1


def test_reentry_with_fresh_context_is_rejected_but_reads_work():
    bus, p = EventBus(), position()
    engine = AccountEquityEngine(bus, 100)
    errors = []
    def subscriber(event):
        assert engine.latest() is event.payload["account_snapshot"]
        try:
            Context().run(engine.value, portfolio(p), (mark(p, 100),))
        except Exception as exc:
            errors.append(exc)
    bus.subscribe(EventType.ACCOUNT_UPDATED, subscriber)
    engine.value(portfolio(p), (mark(p, 100),))
    assert isinstance(errors[0], AccountValuationError)
    assert len(engine.history()) == 1


def test_lock_and_bus_replacement_is_repaired():
    bus, replacement, p = EventBus(), EventBus(), position()
    engine, seen = AccountEquityEngine(bus, 100), []
    bus.subscribe(EventType.ACCOUNT_UPDATED, lambda event: (
        setattr(engine, "_lock", object()), setattr(engine, "event_bus", replacement)))
    replacement.subscribe(EventType.ACCOUNT_UPDATED, seen.append)
    engine.value(portfolio(p), (mark(p, 100),))
    engine.value(portfolio(p), (mark(p, 101),))
    assert engine.event_bus is bus and seen == [] and len(engine.history()) == 2


def test_concurrent_values_are_serialized_and_ordered():
    bus, p, events = EventBus(), position(), []
    engine = AccountEquityEngine(bus, 100)
    bus.subscribe(EventType.ACCOUNT_UPDATED, lambda event: events.append(event.payload["account_snapshot"]))
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = tuple(pool.map(lambda _: engine.value(portfolio(p), (mark(p, 101),)), range(20)))
    assert len(results) == 20
    assert tuple(events) == engine.history()


def test_models_are_frozen_slotted_repeatable_and_detect_tampering():
    p, record = position(), None
    record = mark(p, 101)
    valuation = PositionValuation(p, record, 101, 2.0, 5.0, NOW)
    snapshot = AccountSnapshot(portfolio(p), (valuation,), 100, 3, 2, 5, 105, NOW)
    valuation.validate(); snapshot.validate()
    with pytest.raises((FrozenInstanceError, AttributeError)):
        snapshot.equity = 1
    assert not hasattr(snapshot, "__dict__")
    object.__setattr__(valuation, "unrealized_pnl", 99)
    with pytest.raises(DomainValidationError):
        valuation.validate()
    with pytest.raises(DomainValidationError):
        snapshot.validate()


def test_engine_can_be_collected_when_subscriber_closure_captures_it():
    bus = EventBus()
    engine = AccountEquityEngine(bus, 100)
    reference = weakref.ref(engine)
    bus.subscribe(EventType.ACCOUNT_UPDATED, lambda event, engine=engine: engine.history())
    del engine
    gc.collect()
    assert reference() is not None  # subscriber intentionally owns it
    bus._subscribers.clear()
    gc.collect()
    assert reference() is None
