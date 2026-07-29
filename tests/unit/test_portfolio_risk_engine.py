"""Phase 12 account-aware portfolio risk coverage."""

from contextvars import Context
from dataclasses import replace
from datetime import datetime, timezone
import gc
import math
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import weakref

import pytest

from quant_futures.account.models import AccountSnapshot, PositionValuation
from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioRiskError
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.portfolio.models import PortfolioSnapshot, PositionSide, PositionSnapshot
from quant_futures.risk import (
    PortfolioRiskEngine, PortfolioRiskLimits, PortfolioRiskOutcome, RiskLimitCode,
    RiskLimitBreach,
)

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
