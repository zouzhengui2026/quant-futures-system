"""Product orchestration over the repository's authoritative trading engines.

The product layer deliberately owns no position or equity calculator.  It only
translates the strategy's normalized target into a directional proposal and
then retains the exact objects committed by execution, portfolio, account, and
portfolio-risk authorities.
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from math import isfinite

from quant_futures.account import AccountEquityEngine
from quant_futures.alpha import AlphaCandidate, AlphaDirection
from quant_futures.core.events import EventBus
from quant_futures.decision import DecisionEngine
from quant_futures.decision.policies import ThresholdDecisionPolicy
from quant_futures.execution import ExecutionEngine, FixedQuantityExecutionPolicy, PaperExecutionEngine
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.models import LiquidityRegime, MarketObservation, TrendRegime, VolatilityRegime
from quant_futures.portfolio import PortfolioLedger, PositionSide
from quant_futures.risk import RiskEngine
from quant_futures.risk.policies import ThresholdRiskPolicy
from quant_futures.risk.portfolio_engine import PortfolioRiskEngine
from quant_futures.risk.portfolio_models import PortfolioRiskLimits
from quant_futures.timing import RuleEvaluation, RuleOutcome, TimingAssessment, TimingStatus

from .config import ProductConfig
from .data import Bar
from .strategy import Strategy, StrategyContext


@dataclass(frozen=True, slots=True)
class Record:
    transition_id: int; timestamp: str; price: float; target: float; quantity: float
    fill_quantity: float; fill_price: float | None; commission: float; slippage: float
    funding: float; cash: float; equity: float; drawdown: float; risk_breach: bool
    order_id: str | None = None
    final_pending_cancelled: bool = False
    # Exact authoritative identities; artifact serialization intentionally emits
    # their stable identifiers/values rather than making parallel copies.
    execution_report: object | None = None
    position_update: object | None = None
    account_snapshot: object | None = None
    portfolio_risk_snapshot: object | None = None


_ARTIFACT_FIELDS = tuple(f.name for f in fields(Record) if f.name not in {
    "execution_report", "position_update", "account_snapshot", "portfolio_risk_snapshot"})


def _candidate(config: ProductConfig, bar: Bar, direction: AlphaDirection) -> AlphaCandidate:
    observation = MarketObservation(
        config.data.symbol, config.data.source, bar.timestamp, bar.close,
        TrendRegime.RANGE, VolatilityRegime.NORMAL, LiquidityRegime.LIQUID,
        MarketFeatures(trend_strength=1.0),
    )
    timing = TimingAssessment(
        observation, TimingStatus.FAVORABLE,
        (RuleEvaluation("product_target", RuleOutcome.PASS, "normalized target delta"),),
        bar.timestamp, 1.0,
    )
    return AlphaCandidate(
        config.data.symbol, config.data.source, bar.timestamp, bar.timestamp,
        "product_target", direction, 1.0, 1.0, ("normalized target delta",),
        observation, timing,
    )


def _position(ledger: PortfolioLedger, source: str, symbol: str) -> float:
    for item in ledger.snapshot().positions:
        if (item.source, item.symbol) == (source, symbol):
            return item.signed_quantity
    return 0.0


def simulate(config: ProductConfig, bars: tuple[Bar, ...], strategy: Strategy) -> tuple[Record, ...]:
    """Run the canonical transition order.

    At every open the previously submitted intent is filled and committed
    *before* the next target delta is calculated.  Targets are normalized
    exposure fractions in ``[-1, 1]``; ``max_position`` is applied exactly once.
    An intent left after the final decision is explicitly cancelled.
    """
    bus = EventBus()
    clock = {"value": bars[0].timestamp}
    paper, ledger = PaperExecutionEngine(bus, lambda: clock["value"]), PortfolioLedger(bus)
    account = AccountEquityEngine(bus, config.starting_equity)
    limit = max(config.starting_equity * 1000.0, config.risk.max_position * max(b.close for b in bars) * 10.0)
    portfolio_risk = PortfolioRiskEngine(bus, PortfolioRiskLimits(True, limit, limit, limit, 1.0, 1000.0))
    closes: list[float] = []
    records: list[Record] = []
    pending = None
    peak = config.starting_equity

    for index, bar in enumerate(bars):
        clock["value"] = bar.timestamp
        report = update = None
        fill_qty = commission = slippage = 0.0
        fill_price = None
        # Commit prior next-open work first; this is the position authority used
        # by the strategy and target-delta calculation below.
        if pending is not None:
            fill_qty = pending.order.quantity * (1 if pending.order.side.value == "buy" else -1)
            reference = bar.open
            fill_price = reference * (1 + (1 if fill_qty > 0 else -1) * config.costs.slippage_bps / 10000)
            slippage = abs(fill_qty) * abs(fill_price - reference)
            commission = abs(fill_qty * fill_price) * config.costs.commission_bps / 10000
            report = paper.fill(pending.order.order_id, fill_price)
            update = ledger.apply(report)
            pending = None

        current = _position(ledger, config.data.source, config.data.symbol)
        closes.append(bar.close)
        normalized = strategy.target(StrategyContext(bar, tuple(closes), current / config.risk.max_position))
        if isinstance(normalized, bool) or not isinstance(normalized, (int, float)) or not isfinite(normalized) or not -1 <= normalized <= 1:
            raise ValueError("strategy target must be a finite normalized exposure in [-1, 1]")
        target = float(normalized) * config.risk.max_position
        requested = target - current
        if requested:
            decision_engine = DecisionEngine(bus, ThresholdDecisionPolicy(clock=lambda timestamp=bar.timestamp: timestamp))
            risk_engine = RiskEngine(bus, ThresholdRiskPolicy(clock=lambda timestamp=bar.timestamp: timestamp))
            direction = AlphaDirection.LONG if requested > 0 else AlphaDirection.SHORT
            decision = decision_engine.decide(_candidate(config, bar, direction))
            assessment = risk_engine.assess(decision)
            execution = ExecutionEngine(bus, FixedQuantityExecutionPolicy(
                quantity=abs(requested), clock=lambda timestamp=bar.timestamp: timestamp,
                order_id_factory=lambda n=index + 1: f"transition-{n:08d}",
            ))
            intent = execution.create_intent(assessment)
            paper.submit(intent)
            if config.fill_timing == "next_open":
                pending = intent
            else:
                fill_qty = requested
                reference = bar.close
                fill_price = reference * (1 + (1 if fill_qty > 0 else -1) * config.costs.slippage_bps / 10000)
                slippage = abs(fill_qty) * abs(fill_price - reference)
                commission = abs(fill_qty * fill_price) * config.costs.commission_bps / 10000
                report = paper.fill(intent.order.order_id, fill_price)
                update = ledger.apply(report)

        snapshot = ledger.snapshot()
        marks = tuple(MarketDataRecord(MarketDataKind.MARK_PRICE, p.symbol, p.source, bar.timestamp,
                                       {"price": bar.close}) for p in snapshot.positions if p.side is not PositionSide.FLAT)
        account_snapshot = account.value(snapshot, marks)
        risk_snapshot = portfolio_risk.evaluate(account_snapshot)
        equity = account_snapshot.equity
        peak = max(peak, equity)
        drawdown = 0.0 if peak <= 0 else (peak - equity) / peak
        quantity = _position(ledger, config.data.source, config.data.symbol)
        records.append(Record(
            index + 1, bar.timestamp.isoformat().replace("+00:00", "Z"), bar.close,
            target, quantity, fill_qty, fill_price, commission, slippage, 0.0,
            equity - quantity * bar.close, equity, drawdown,
            bool(risk_snapshot.breaches) or drawdown > config.risk.max_drawdown,
            report.order.order_id if report else None, False, report, update,
            account_snapshot, risk_snapshot,
        ))

    if pending is not None:
        paper.cancel(pending.order.order_id, "end of replay: no following open")
        if records:
            last = records[-1]
            records[-1] = Record(**{
                **record_dict(last),
                "final_pending_cancelled": True,
                "execution_report": last.execution_report,
                "position_update": last.position_update,
                "account_snapshot": last.account_snapshot,
                "portfolio_risk_snapshot": last.portfolio_risk_snapshot,
            })
    return tuple(records)


def record_dict(record: Record) -> dict:
    return {name: getattr(record, name) for name in _ARTIFACT_FIELDS}
