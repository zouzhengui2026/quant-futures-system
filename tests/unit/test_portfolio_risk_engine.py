"""Phase 12 account-aware portfolio risk coverage."""

from contextvars import Context
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
import gc
import math
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event as ThreadEvent, Lock
import weakref

import pytest

from quant_futures.account.models import AccountSnapshot, PositionValuation
from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioRiskError
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.portfolio.models import PortfolioSnapshot, PositionSide, PositionSnapshot
from quant_futures.risk import (
    PortfolioRiskEngine, PortfolioRiskLimits, PortfolioRiskOutcome, PortfolioRiskSnapshot,
    PositionExposure, RiskLimitCode, RiskLimitBreach,
)
import quant_futures.risk.portfolio_engine as portfolio_engine_module

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def limits(**changes):
    values = dict(require_positive_equity=True, max_gross_notional=10_000,
                  max_abs_net_notional=10_000, max_position_notional=10_000,
                  max_concentration_ratio=1, max_gross_exposure_multiple=100)
    values.update(changes)
    return PortfolioRiskLimits(**values)


def account(*specs, equity=1_000):
    positions, valuations = [], []
    for symbol, quantity, price in sorted(specs):
        side = PositionSide.FLAT if quantity == 0 else (
            PositionSide.LONG if quantity > 0 else PositionSide.SHORT)
        position = PositionSnapshot("sim", symbol, quantity, side,
                                    None if quantity == 0 else price, 0, NOW, "order-" + symbol)
        positions.append(position)
        if quantity == 0:
            valuations.append(PositionValuation(position, None, None, 0, 0, NOW))
        else:
            mark = MarketDataRecord(MarketDataKind.MARK_PRICE, symbol, "sim", NOW,
                                    {"price": price})
            valuations.append(PositionValuation(position, mark, price, 0, 0, NOW))
    portfolio = PortfolioSnapshot(tuple(positions), 0, NOW if positions else datetime(
        1970, 1, 1, tzinfo=timezone.utc))
    valued_at = NOW if positions else portfolio.updated_at
    return AccountSnapshot(portfolio, tuple(valuations), equity, 0, 0, 0, equity, valued_at)


def test_empty_flat_and_mixed_formulas_and_exact_lineage():
    engine = PortfolioRiskEngine(EventBus(), limits())
    empty = engine.evaluate(account())
    assert (empty.long_notional, empty.short_notional, empty.gross_notional,
            empty.net_notional, empty.concentration_ratio) == (0, 0, 0, 0, 0)
    source = account(("BTC", 2, 100), ("ETH", -3, 50), ("XRP", 0, 1))
    result = engine.evaluate(source)
    assert (result.long_notional, result.short_notional, result.gross_notional,
            result.net_notional, result.largest_position_notional,
            result.concentration_ratio) == (200, 150, 350, 50, 200, 200 / 350)
    assert result.account_snapshot is source
    assert all(e.position_valuation is v for e, v in zip(result.position_exposures,
                                                          source.valuations))


def test_stable_large_notional_cancellation():
    result = PortfolioRiskEngine(EventBus(), limits(max_gross_notional=1e22,
        max_abs_net_notional=1e22, max_position_notional=1e22)).evaluate(
            account(("A", 1e15, 1), ("B", -1e15, 1), ("C", 1, 1)))
    assert result.net_notional == 1


def test_equity_and_every_ordered_breach_with_position_attribution():
    configured = limits(max_gross_notional=10, max_abs_net_notional=5,
                        max_position_notional=6, max_concentration_ratio=.4,
                        max_gross_exposure_multiple=.1)
    result = PortfolioRiskEngine(EventBus(), configured).evaluate(
        account(("BTC", 2, 10), equity=0))
    assert result.gross_exposure_multiple is None
    assert [b.code for b in result.breaches] == [
        RiskLimitCode.NON_POSITIVE_EQUITY, RiskLimitCode.MAX_GROSS_NOTIONAL,
        RiskLimitCode.MAX_ABS_NET_NOTIONAL, RiskLimitCode.MAX_POSITION_NOTIONAL,
        RiskLimitCode.MAX_CONCENTRATION,
    ]
    position_breach = result.breaches[3]
    assert (position_breach.source, position_breach.symbol) == ("sim", "BTC")
    positive = PortfolioRiskEngine(EventBus(), configured).evaluate(
        account(("BTC", 2, 10), equity=100))
    assert positive.breaches[-1].code is RiskLimitCode.MAX_GROSS_EXPOSURE_MULTIPLE
    assert positive.outcome is PortfolioRiskOutcome.BREACHED


@pytest.mark.parametrize("field,value", [
    ("max_gross_notional", True), ("max_abs_net_notional", 0),
    ("max_position_notional", -1), ("max_concentration_ratio", 1.1),
    ("max_gross_exposure_multiple", math.inf), ("max_gross_notional", math.nan),
])
def test_invalid_limits(field, value):
    with pytest.raises(DomainValidationError):
        limits(**{field: value})


def test_event_exact_identities_and_subscriber_reads_after_commit():
    bus, seen = EventBus(), []
    engine = PortfolioRiskEngine(bus, limits())
    bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED,
                  lambda event: seen.append((event, engine.latest(), engine.history())))
    source = account(("BTC", 1, 10))
    result = engine.evaluate(source)
    event, latest, history = seen[0]
    assert set(event.payload) == {"portfolio_risk_snapshot", "account_snapshot",
                                  "position_exposures", "limits", "breaches"}
    assert event.payload["portfolio_risk_snapshot"] is result is latest is history[0]
    assert event.payload["account_snapshot"] is source
    assert event.payload["limits"] is engine.limits


def test_subscriber_failure_commits_and_fresh_context_reentry_is_rejected():
    bus = EventBus()
    engine = PortfolioRiskEngine(bus, limits())
    source, errors = account(("BTC", 1, 10)), []
    def callback(event):
        try:
            Context().run(engine.evaluate, source)
        except Exception as exc:
            errors.append(exc)
        raise RuntimeError("subscriber failed")
    bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED, callback)
    with pytest.raises(RuntimeError, match="subscriber failed"):
        engine.evaluate(source)
    assert len(engine.history()) == 1
    assert isinstance(errors[0], PortfolioRiskError)


def test_coherent_tampering_and_equal_replacement_are_detected():
    engine = PortfolioRiskEngine(EventBus(), limits())
    result = engine.evaluate(account(("BTC", 1, 10)))
    replacement = tuple(list(result.position_exposures))
    object.__setattr__(result, "position_exposures", replacement)
    with pytest.raises(PortfolioRiskError):
        engine.history()


def _breached_engine_and_result():
    configured = limits(max_gross_notional=5, max_abs_net_notional=6,
                        max_position_notional=7, max_concentration_ratio=.4,
                        max_gross_exposure_multiple=.05)
    engine = PortfolioRiskEngine(EventBus(), configured)
    return engine, engine.evaluate(account(("BTC", 1, 10), equity=100))


@pytest.mark.parametrize("mutation", [
    lambda breaches: (),
    lambda breaches: breaches[1:],
    lambda breaches: tuple(reversed(breaches)),
    lambda breaches: breaches + (breaches[0],),
    lambda breaches: (replace(breaches[0], actual=breaches[0].actual + 1),) + breaches[1:],
    lambda breaches: (replace(breaches[0], limit=breaches[0].limit + 1),) + breaches[1:],
    lambda breaches: breaches[:2] + (
        replace(breaches[2], source="other", symbol="ETH"),) + breaches[3:],
])
def test_exact_canonical_breach_tuple_rejects_coherent_tampering(mutation):
    engine, result = _breached_engine_and_result()
    object.__setattr__(result, "breaches", mutation(result.breaches))
    object.__setattr__(result, "outcome", (PortfolioRiskOutcome.BREACHED
                                           if result.breaches else PortfolioRiskOutcome.HEALTHY))
    with pytest.raises(DomainValidationError):
        result.validate()
    with pytest.raises((DomainValidationError, PortfolioRiskError)):
        engine.history()


@pytest.mark.parametrize("breach", [
    (RiskLimitCode.NON_POSITIVE_EQUITY, 1, None, None, None),
    (RiskLimitCode.NON_POSITIVE_EQUITY, 0, 1, None, None),
    (RiskLimitCode.MAX_GROSS_NOTIONAL, 10, 10, None, None),
    (RiskLimitCode.MAX_ABS_NET_NOTIONAL, 9, 10, None, None),
    (RiskLimitCode.MAX_POSITION_NOTIONAL, 11, 10, None, "BTC"),
    (RiskLimitCode.MAX_POSITION_NOTIONAL, 11, 10, "sim", None),
    (RiskLimitCode.MAX_CONCENTRATION, 2, 1, "sim", "BTC"),
])
def test_breach_code_specific_semantics_are_fail_closed(breach):
    with pytest.raises(DomainValidationError):
        RiskLimitBreach(*breach)


@pytest.mark.parametrize("field", [
    "max_gross_notional", "max_abs_net_notional", "max_position_notional",
    "max_concentration_ratio", "max_gross_exposure_multiple",
])
@pytest.mark.parametrize("value", [True, 0, -1, math.nan, math.inf, -math.inf])
def test_complete_numeric_limit_validation(field, value):
    with pytest.raises(DomainValidationError):
        limits(**{field: value})


@pytest.mark.parametrize("value", [0, 1, None, "yes"])
def test_require_positive_equity_must_be_an_actual_bool(value):
    with pytest.raises(DomainValidationError):
        limits(require_positive_equity=value)


def test_threshold_equality_is_healthy_and_positive_equity_can_be_optional():
    exact = PortfolioRiskEngine(EventBus(), limits(
        max_gross_notional=10, max_abs_net_notional=10,
        max_position_notional=10, max_concentration_ratio=1,
        max_gross_exposure_multiple=1)).evaluate(account(("BTC", 1, 10), equity=10))
    assert exact.breaches == ()
    zero = account(equity=0)
    position = PositionSnapshot("sim", "LOSS", 1, PositionSide.LONG, 1, -2, NOW, "loss")
    portfolio = PortfolioSnapshot((position,), -2, NOW)
    valuation = PositionValuation(position, MarketDataRecord(
        MarketDataKind.MARK_PRICE, "LOSS", "sim", NOW, {"price": 1}), 1, 0, -2, NOW)
    negative = AccountSnapshot(portfolio, (valuation,), 1, -2, 0, -2, -1, NOW)
    for source in (zero, negative):
        result = PortfolioRiskEngine(EventBus(), limits(
            require_positive_equity=False)).evaluate(source)
        assert result.breaches == ()
        assert result.outcome is PortfolioRiskOutcome.HEALTHY


def test_concurrent_evaluations_have_one_exact_history_and_event_order():
    bus, events = EventBus(), []
    engine = PortfolioRiskEngine(bus, limits())
    bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED,
                  lambda event: events.append(event.payload["portfolio_risk_snapshot"]))
    inputs = [account((f"S{i}", i + 1, 1)) for i in range(8)]
    barrier = Barrier(len(inputs))

    def evaluate(source):
        barrier.wait()
        return engine.evaluate(source)

    with ThreadPoolExecutor(max_workers=len(inputs)) as executor:
        returned = list(executor.map(evaluate, inputs))
    history = engine.history()
    assert len(history) == len(inputs)
    assert events == list(history)
    assert {id(item) for item in returned} == {id(item) for item in history}
    assert engine.latest() is history[-1]


def test_subscriber_replacements_are_restored_even_after_exception():
    original_bus, replacement_bus = EventBus(), EventBus()
    original_limits, replacement_limits = limits(), limits(max_gross_notional=1)
    engine = PortfolioRiskEngine(original_bus, original_limits)

    def attack(_event):
        engine.event_bus = replacement_bus
        engine.limits = replacement_limits
        engine._lock = object()
        raise RuntimeError("subscriber failed")

    unsubscribe = original_bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED, attack)
    with pytest.raises(RuntimeError, match="subscriber failed"):
        engine.evaluate(account(("BTC", 1, 10)))
    assert engine.event_bus is original_bus
    assert engine.limits is original_limits
    unsubscribe()
    assert engine.evaluate(account(("BTC", 1, 10))).outcome is PortfolioRiskOutcome.HEALTHY


@pytest.mark.parametrize("raises", [False, True])
def test_subscriber_limits_field_mutation_is_restored(raises):
    bus = EventBus()
    configured = limits()
    canonical_max_gross = configured.max_gross_notional
    engine = PortfolioRiskEngine(bus, configured)

    def attack(event):
        object.__setattr__(event.payload["limits"], "max_gross_notional", 1)
        if raises:
            raise RuntimeError("subscriber failed")

    unsubscribe = bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED, attack)
    if raises:
        with pytest.raises(RuntimeError, match="subscriber failed"):
            engine.evaluate(account(("BTC", 1, 10)))
    else:
        engine.evaluate(account(("BTC", 1, 10)))

    assert configured.max_gross_notional == canonical_max_gross
    assert engine.history()[0].limits is configured
    unsubscribe()
    assert engine.evaluate(account(("BTC", 1, 10))).outcome is PortfolioRiskOutcome.HEALTHY


def test_engine_subscriber_cycle_is_collectable():
    bus = EventBus()
    engine = PortfolioRiskEngine(bus, limits())
    bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED,
                  lambda _event, owner=engine: owner.history())
    engine_ref, bus_ref = weakref.ref(engine), weakref.ref(bus)
    del engine, bus
    gc.collect()
    assert engine_ref() is None
    assert bus_ref() is None


def test_equal_limits_replacement_and_corrupt_zero_state_fail_closed():
    engine = PortfolioRiskEngine(EventBus(), limits())
    engine.limits = replace(engine.limits)
    with pytest.raises(PortfolioRiskError):
        engine.history()
    engine = PortfolioRiskEngine(EventBus(), limits())
    engine._latest = object()
    with pytest.raises(PortfolioRiskError):
        engine.history()


def test_split_lock_replacement_cannot_split_the_authoritative_transition():
    original_bus = EventBus()
    engine = PortfolioRiskEngine(original_bus, limits())
    first_source = account(("FIRST", 1, 1))
    second_source = account(("SECOND", 2, 1))
    callback_entered = ThreadEvent()
    release_callback = ThreadEvent()
    second_started = ThreadEvent()
    second_finished = ThreadEvent()
    events = []

    def attack(event):
        snapshot = event.payload["portfolio_risk_snapshot"]
        events.append(snapshot)
        if snapshot.account_snapshot is first_source:
            engine._lock = Lock()
            callback_entered.set()
            assert release_callback.wait(5)

    original_bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED, attack)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(engine.evaluate, first_source)
        assert callback_entered.wait(5)

        def second_evaluation():
            second_started.set()
            result = engine.evaluate(second_source)
            second_finished.set()
            return result

        second_future = executor.submit(second_evaluation)
        assert second_started.wait(5)
        assert not second_finished.wait(.1)
        assert len(engine._history) == 1
        release_callback.set()
        first, second = first_future.result(5), second_future.result(5)

    history = engine.history()
    assert history == (first, second)
    assert events == [first, second]
    assert history[0] is first and history[1] is second
    assert engine.latest() is second


def test_original_event_bus_is_retained_locally_during_replacement_without_owner():
    replacement = EventBus()
    replacement_events = []
    replacement.subscribe(EventType.PORTFOLIO_RISK_UPDATED,
                          lambda event: replacement_events.append(event))
    engine = PortfolioRiskEngine(EventBus(), limits())
    original_ref = weakref.ref(engine.event_bus)
    original_events = []

    def attack(event):
        original_events.append(event.payload["portfolio_risk_snapshot"])
        engine.event_bus = replacement
        gc.collect()
        assert original_ref() is not None

    engine.event_bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED, attack)
    first = engine.evaluate(account(("FIRST", 1, 1)))
    assert engine.event_bus is original_ref()
    second = engine.evaluate(account(("SECOND", 1, 1)))
    assert original_events == [first, second]
    assert replacement_events == []


def test_commitment_construction_failure_has_zero_state_and_later_recovers(monkeypatch):
    bus, events = EventBus(), []
    engine = PortfolioRiskEngine(bus, limits())
    bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED, events.append)
    real_fingerprint = portfolio_engine_module._fingerprint

    def fail_snapshot_fingerprint(value):
        if isinstance(value, PortfolioRiskSnapshot):
            raise RuntimeError("fingerprint failed")
        return real_fingerprint(value)

    monkeypatch.setattr(portfolio_engine_module, "_fingerprint", fail_snapshot_fingerprint)
    with pytest.raises(RuntimeError, match="fingerprint failed"):
        engine.evaluate(account(("BTC", 1, 10)))
    assert engine.history() == ()
    with pytest.raises(PortfolioRiskError, match="history is empty"):
        engine.latest()
    assert events == []

    monkeypatch.setattr(portfolio_engine_module, "_fingerprint", real_fingerprint)
    result = engine.evaluate(account(("BTC", 1, 10)))
    assert engine.history() == (result,)
    assert events[0].payload["portfolio_risk_snapshot"] is result


def test_same_input_concurrency_preserves_exact_commit_and_event_identities():
    bus, events = EventBus(), []
    engine = PortfolioRiskEngine(bus, limits())
    source = account(("BTC", 1, 10))
    bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED,
                  lambda event: events.append(event.payload["portfolio_risk_snapshot"]))
    workers = 8
    barrier = Barrier(workers)

    def evaluate():
        barrier.wait()
        return engine.evaluate(source)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        returned = [future.result() for future in
                    [executor.submit(evaluate) for _ in range(workers)]]
    history = engine.history()
    assert len({id(snapshot) for snapshot in returned}) == workers
    assert {id(snapshot) for snapshot in returned} == {id(snapshot) for snapshot in history}
    assert len(events) == workers
    assert all(event_snapshot is history[index]
               for index, event_snapshot in enumerate(events))
    assert all(snapshot.account_snapshot is source for snapshot in history)
    assert engine.latest() is history[-1]


def test_fresh_context_and_thread_reentry_state_are_cleaned_after_exception():
    bus = EventBus()
    engine = PortfolioRiskEngine(bus, limits())
    source = account(("BTC", 1, 10))
    errors = []

    def callback(_event):
        for invoke in (lambda: engine.evaluate(source),
                       lambda: Context().run(engine.evaluate, source)):
            with pytest.raises(PortfolioRiskError) as raised:
                invoke()
            errors.append(raised.value)
        raise RuntimeError("stop publication")

    unsubscribe = bus.subscribe(EventType.PORTFOLIO_RISK_UPDATED, callback)
    with pytest.raises(RuntimeError, match="stop publication"):
        engine.evaluate(source)
    unsubscribe()
    later = engine.evaluate(source)
    assert len(errors) == 2
    assert engine.history() == (engine.history()[0], later)
    assert engine.latest() is later


def test_history_is_an_immutable_tuple_detached_from_internal_storage():
    engine = PortfolioRiskEngine(EventBus(), limits())
    first = engine.evaluate(account(("BTC", 1, 10)))
    history = engine.history()
    assert isinstance(history, tuple) and history == (first,)
    with pytest.raises(TypeError):
        history[0] = first
    engine.evaluate(account(("ETH", 1, 10)))
    assert history == (first,)


@pytest.mark.parametrize("target", [
    "account", "valuation", "exposure", "limits", "breach", "history", "latest",
])
def test_exact_or_equal_public_graph_replacements_fail_closed(target):
    engine, snapshot = _breached_engine_and_result()
    if target == "account":
        object.__setattr__(snapshot, "account_snapshot", replace(snapshot.account_snapshot))
    elif target == "valuation":
        replacement_account = replace(
            snapshot.account_snapshot,
            valuations=(replace(snapshot.account_snapshot.valuations[0]),),
        )
        object.__setattr__(snapshot, "account_snapshot", replacement_account)
    elif target == "exposure":
        object.__setattr__(snapshot, "position_exposures", tuple(
            replace(exposure) for exposure in snapshot.position_exposures))
    elif target == "limits":
        object.__setattr__(snapshot, "limits", replace(snapshot.limits))
    elif target == "breach":
        object.__setattr__(snapshot, "breaches", tuple(
            replace(breach) for breach in snapshot.breaches))
    elif target == "history":
        engine._history = list(engine._history)
    else:
        engine._latest = replace(snapshot)
    with pytest.raises((DomainValidationError, PortfolioRiskError)):
        engine.history()


@pytest.mark.parametrize("factory", [
    lambda: PositionExposure(
        account(("BTC", 1, 10)).valuations[0], 10, 10),
    lambda: limits(),
    lambda: RiskLimitBreach(RiskLimitCode.MAX_GROSS_NOTIONAL, 11, 10),
    lambda: PortfolioRiskEngine(EventBus(), limits()).evaluate(account(("BTC", 1, 10))),
])
def test_every_public_record_is_frozen_slotted_and_repeatably_validated(factory):
    model = factory()
    assert not hasattr(model, "__dict__")
    model.validate()
    model.validate()
    first_field = next(iter(model.__dataclass_fields__))
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        setattr(model, first_field, getattr(model, first_field))


def test_multi_position_breaches_follow_deterministic_exposure_order():
    result = PortfolioRiskEngine(EventBus(), limits(
        max_position_notional=5)).evaluate(account(
            ("ZETA", 2, 10), ("ALPHA", -3, 10), ("MU", 4, 10)))
    position_breaches = tuple(
        breach for breach in result.breaches
        if breach.code is RiskLimitCode.MAX_POSITION_NOTIONAL)
    assert [(breach.source, breach.symbol, breach.actual) for breach in position_breaches] == [
        ("sim", "ALPHA", 30), ("sim", "MU", 40), ("sim", "ZETA", 20),
    ]
