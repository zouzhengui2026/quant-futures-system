"""Phase 12 account-aware portfolio risk coverage."""

from contextvars import Context
from datetime import datetime, timezone
import math

import pytest

from quant_futures.account.models import AccountSnapshot, PositionValuation
from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioRiskError
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.portfolio.models import PortfolioSnapshot, PositionSide, PositionSnapshot
from quant_futures.risk import (
    PortfolioRiskEngine, PortfolioRiskLimits, PortfolioRiskOutcome, RiskLimitCode,
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
