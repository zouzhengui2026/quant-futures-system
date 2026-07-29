"""Phase 11 deterministic account-equity coverage."""

from concurrent.futures import ThreadPoolExecutor
from contextvars import Context
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import gc
import math
import threading
from types import MappingProxyType
import weakref

import pytest

from quant_futures.account import AccountEquityEngine, AccountSnapshot, PositionValuation
from quant_futures.account import engine as account_engine_module
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


@pytest.mark.parametrize("read", ["latest", "history", "value"])
def test_coherent_committed_graph_forgery_is_rejected(read):
    bus, p, events = EventBus(), position(), []
    engine = AccountEquityEngine(bus, 100)
    bus.subscribe(EventType.ACCOUNT_UPDATED, events.append)
    record = mark(p, 101)
    snapshot = engine.value(portfolio(p), (record,))
    valuation = snapshot.valuations[0]
    object.__setattr__(record, "values", MappingProxyType({"price": 150.0}))
    object.__setattr__(valuation, "mark_price", 150.0)
    object.__setattr__(valuation, "unrealized_pnl", 100.0)
    object.__setattr__(valuation, "total_pnl", 103.0)
    object.__setattr__(snapshot, "total_unrealized_pnl", 100.0)
    object.__setattr__(snapshot, "total_pnl", 103.0)
    object.__setattr__(snapshot, "equity", 203.0)

    with pytest.raises(AccountValuationError, match="mutated"):
        if read == "value":
            engine.value(portfolio(p), (mark(p, 150),))
        else:
            getattr(engine, read)()
    assert len(events) == 1


def test_engine_and_bus_subscriber_cycle_is_collectible():
    def make_cycle():
        bus = EventBus()
        engine = AccountEquityEngine(bus, 100)
        bus.subscribe(EventType.ACCOUNT_UPDATED, lambda event: engine.history())
        return weakref.ref(engine), weakref.ref(bus)

    engine_ref, bus_ref = make_cycle()
    gc.collect()
    assert engine_ref() is None
    assert bus_ref() is None


def test_latest_before_first_valuation_fails_closed():
    with pytest.raises(AccountValuationError, match="empty"):
        AccountEquityEngine(EventBus(), 100).latest()


def test_starting_equity_callback_mutation_is_repaired():
    bus, p = EventBus(), position()
    engine = AccountEquityEngine(bus, 100)
    bus.subscribe(EventType.ACCOUNT_UPDATED,
                  lambda event: setattr(engine, "starting_equity", 9_999))
    first = engine.value(portfolio(p), (mark(p, 100),))
    second = engine.value(portfolio(p), (mark(p, 101),))
    assert engine.starting_equity == 100
    assert first.starting_equity == second.starting_equity == 100
    assert second.equity == 105


def test_reentry_guard_is_cleaned_after_subscriber_exception():
    bus, p = EventBus(), position()
    engine = AccountEquityEngine(bus, 100)
    def fail_once(event):
        bus.unsubscribe(EventType.ACCOUNT_UPDATED, fail_once)
        raise RuntimeError("subscriber failed")
    bus.subscribe(EventType.ACCOUNT_UPDATED, fail_once)
    with pytest.raises(RuntimeError, match="subscriber failed"):
        engine.value(portfolio(p), (mark(p, 100),))
    assert engine.value(portfolio(p), (mark(p, 101),)).equity == 105


def test_mixed_positions_are_sorted_and_stably_aggregated_to_negative_equity():
    flat = position("MID", 0, None, -5)
    long = position("AAA", 1e16, 100, 7)
    short = position("ZZZ", -1e16, 100, -3)
    snapshot = AccountEquityEngine(EventBus(), 1).value(
        portfolio(short, flat, long), (mark(short, 101), mark(long, 99)))
    assert tuple(v.position for v in snapshot.valuations) == (long, flat, short)
    assert snapshot.valuations[0].mark_record.symbol == "AAA"
    assert snapshot.valuations[1].mark_record is None
    assert snapshot.valuations[2].mark_record.symbol == "ZZZ"
    assert snapshot.total_unrealized_pnl == -2e16
    assert snapshot.equity == -2e16


def test_commitment_strongly_anchors_exact_graph_and_rejects_equal_replacement():
    p = position()
    portfolio_snapshot = portfolio(p)
    record = mark(p, 101)
    engine = AccountEquityEngine(EventBus(), 100)
    snapshot = engine.value(portfolio_snapshot, (record,))
    valuation = snapshot.valuations[0]
    commitment = account_engine_module._ANCHORS[engine].commitments[0]

    replacement_position = position()
    replacement_portfolio = portfolio(replacement_position)
    replacement_record = mark(replacement_position, 101)
    replacement_valuation = PositionValuation(
        replacement_position, replacement_record, 101, 2, 5, NOW)
    replacement_snapshot = AccountSnapshot(
        replacement_portfolio, (replacement_valuation,), 100, 3, 2, 5, 105, NOW)
    account_engine_module._ANCHORS[engine].history[0] = replacement_snapshot
    del record, valuation, portfolio_snapshot, p
    gc.collect()

    assert commitment.snapshot is snapshot and commitment.snapshot is not replacement_snapshot
    assert commitment.portfolio.portfolio is snapshot.portfolio_snapshot
    assert commitment.portfolio.portfolio is not replacement_portfolio
    assert commitment.portfolio.positions_tuple is snapshot.portfolio_snapshot.positions
    assert commitment.portfolio.positions[0].position is not replacement_position
    assert commitment.valuations[0].valuation is not replacement_valuation
    assert commitment.valuations[0].mark.record is not replacement_record
    with pytest.raises(AccountValuationError, match="differs"):
        engine.latest()


def test_split_lock_transitions_never_overlap_and_preserve_order():
    bus, p = EventBus(), position()
    engine = AccountEquityEngine(bus, 100)
    first_publishing = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    state_lock = threading.Lock()
    active = 0
    maximum = 0
    events = []

    def subscriber(event):
        nonlocal active, maximum
        with state_lock:
            active += 1
            maximum = max(maximum, active)
        events.append(event.payload["account_snapshot"])
        if len(events) == 1:
            engine._lock = threading.RLock()
            first_publishing.set()
            assert release_first.wait(2)
        with state_lock:
            active -= 1

    bus.subscribe(EventType.ACCOUNT_UPDATED, subscriber)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(engine.value, portfolio(p), (mark(p, 101),))
        assert first_publishing.wait(2)
        second = pool.submit(lambda: (second_started.set(), engine.value(
            portfolio(p), (mark(p, 102, NOW + timedelta(seconds=1)),)))[1])
        assert second_started.wait(2)
        assert len(events) == 1
        release_first.set()
        results = (first.result(), second.result())
    assert maximum == 1
    assert tuple(events) == engine.history() == results


def test_transition_strongly_retains_and_restores_original_event_bus():
    def build():
        original = EventBus()
        replacement = EventBus()
        engine = AccountEquityEngine(original, 100)
        original_ref = weakref.ref(original)
        original.subscribe(EventType.ACCOUNT_UPDATED,
                           lambda event: setattr(engine, "event_bus", replacement))
        return engine, original_ref

    engine, original_ref = build()
    gc.collect()
    assert original_ref() is not None
    p = position()
    assert engine.value(portfolio(p), (mark(p, 101),)).equity == 105
    assert engine.event_bus is original_ref()
    seen = []
    original_ref().subscribe(EventType.ACCOUNT_UPDATED, seen.append)
    engine.value(portfolio(p), (mark(p, 102, NOW + timedelta(seconds=1)),))
    assert len(seen) == 1


def test_different_concurrent_inputs_have_exact_event_history_order():
    bus, p, events = EventBus(), position(), []
    engine = AccountEquityEngine(bus, 100)
    bus.subscribe(EventType.ACCOUNT_UPDATED,
                  lambda event: events.append(event.payload["account_snapshot"]))
    barrier = threading.Barrier(3)
    def run(price):
        barrier.wait()
        return engine.value(portfolio(p), (mark(p, price),))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, 101), pool.submit(run, 102)]
        barrier.wait()
        results = tuple(f.result() for f in futures)
    assert set(map(id, results)) == set(map(id, events))
    assert tuple(events) == engine.history()


def test_fsum_preserves_genuine_stable_cancellation():
    positive = position("AAA", 1, 1, 0)
    unit = position("BBB", 1, 1, 0)
    negative = position("CCC", -1, 1, 0)
    snapshot = AccountEquityEngine(EventBus(), 10).value(
        portfolio(positive, unit, negative),
        (mark(positive, 1e16), mark(unit, 2), mark(negative, 1e16)),
    )
    assert [v.unrealized_pnl for v in snapshot.valuations] == [1e16, 1.0, -1e16]
    assert snapshot.total_unrealized_pnl == math.fsum((1e16, 1.0, -1e16)) == 1.0
    assert snapshot.equity == 11.0


def test_account_model_rejects_missing_extra_duplicate_and_bad_aggregates():
    p = position()
    record = mark(p, 101)
    valuation = PositionValuation(p, record, 101, 2, 5, NOW)
    port = portfolio(p)
    valid = (port, (valuation,), 100, 3, 2, 5, 105, NOW)
    invalid = [
        (port, (), 100, 3, 0, 3, 103, NOW),
        (port, (valuation, valuation), 100, 3, 4, 7, 107, NOW),
        (port, (valuation,), 100, 3, 2, 6, 106, NOW),
        (port, (valuation,), 100, 3, 2, 5, 106, NOW),
    ]
    assert AccountSnapshot(*valid).equity == 105
    for args in invalid:
        with pytest.raises(DomainValidationError):
            AccountSnapshot(*args)


@pytest.mark.parametrize("change", [
    {"source": "other"}, {"symbol": "ETH"},
    {"kind": MarketDataKind.INDEX_PRICE}, {"values": {"price": 101, "extra": 1}},
    {"values": {"other": 101}},
    {"timestamp": datetime(2026, 1, 1)},
])
def test_valuation_model_rejects_wrong_mark_lineage(change):
    p = position()
    values = dict(kind=MarketDataKind.MARK_PRICE, symbol=p.symbol, source=p.source,
                  timestamp=NOW, values={"price": 101})
    values.update(change)
    with pytest.raises((DomainValidationError, Exception)):
        record = MarketDataRecord(**values)
        PositionValuation(p, record, record.values.get("price"), 2, 5, NOW)
