"""Incremental, authoritative Paper market-event transitions.

This module is deliberately a domain coordinator: it owns neither filesystem
discovery nor a CLI/runtime loop.  Accounting and risk results are retained
from the Product v0.1 authorities rather than recalculated here.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from math import isfinite

from quant_futures.account import AccountEquityEngine
from quant_futures.alpha import AlphaDirection
from quant_futures.core.events import EventBus
from quant_futures.decision import DecisionEngine
from quant_futures.decision.policies import ThresholdDecisionPolicy
from quant_futures.execution import ExecutionEngine, FixedQuantityExecutionPolicy, PaperExecutionEngine
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.portfolio import PortfolioLedger, PositionSide
from quant_futures.product.config import ProductConfig
from quant_futures.product.data import Bar
from quant_futures.product.engine import _candidate, _position
from quant_futures.product.strategy import Strategy, StrategyContext
from quant_futures.risk import RiskEngine
from quant_futures.risk.policies import ThresholdRiskPolicy
from quant_futures.risk.portfolio_engine import PortfolioRiskEngine
from quant_futures.risk.portfolio_models import PortfolioRiskLimits

from .journal import JournalError, JournalSnapshot, TransitionJournal

STAGE_ORDER = (
    "transition_started", "input_committed", "strategy_committed",
    "order_submitted", "fill_prepared", "fill_committed",
    "portfolio_committed", "account_committed", "risk_committed",
    "transition_committed",
)
_OPTIONAL = {"order_submitted", "fill_prepared", "fill_committed"}


class TransitionError(RuntimeError):
    """The incremental transition is inconsistent and must fail closed."""


class StageProtocol:
    """Validate a single input's ordered, unique stage sequence."""

    def __init__(self) -> None:
        self._last = -1
        self._seen: set[str] = set()

    def accept(self, stage: str) -> None:
        if stage not in STAGE_ORDER or stage in self._seen:
            raise TransitionError(f"illegal or duplicate transition stage: {stage}")
        index = STAGE_ORDER.index(stage)
        if index <= self._last or any(
            required not in self._seen
            for required in STAGE_ORDER[self._last + 1:index]
            if required not in _OPTIONAL
        ):
            raise TransitionError(f"illegal transition stage order: {stage}")
        self._seen.add(stage)
        self._last = index

    def complete(self) -> None:
        if self._last != len(STAGE_ORDER) - 1:
            raise TransitionError("transition did not commit")


@dataclass(frozen=True, slots=True)
class TransitionCounters:
    inputs: int = 0
    orders: int = 0
    fills: int = 0
    journal_events: int = 0


@dataclass(frozen=True, slots=True)
class PaperTransitionState:
    """Non-restorable canonical state exposed to status projection."""

    input_cursor: int
    pending_order: object | None
    portfolio_snapshot: object
    account_snapshot: object
    risk_snapshot: object
    counters: TransitionCounters
    journal_snapshot: JournalSnapshot


def deterministic_id(run_id: str, input_identity: str, kind: str, domain: object) -> str:
    encoded = json.dumps(
        {"run_id": run_id, "input": input_identity, "kind": kind, "domain": domain},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("ascii")
    return f"{kind}-{hashlib.sha256(encoded).hexdigest()[:24]}"


class PaperTransitionCoordinator:
    """Consume exactly one normalized bar through the canonical authorities."""

    def __init__(self, run_id: str, config: ProductConfig, strategy: Strategy,
                 journal: TransitionJournal) -> None:
        if not run_id.strip():
            raise TransitionError("run ID must be non-empty")
        self.run_id, self.config, self.strategy, self.journal = run_id, config, strategy, journal
        self._bus = EventBus()
        self._clock = {"value": None}
        self._paper = PaperExecutionEngine(self._bus, lambda: self._clock["value"])
        self._ledger = PortfolioLedger(self._bus)
        self._account = AccountEquityEngine(self._bus, config.starting_equity)
        self._portfolio_risk = PortfolioRiskEngine(self._bus, PortfolioRiskLimits(
            config.risk.require_positive_equity, config.risk.max_gross_notional,
            config.risk.max_abs_net_notional, config.risk.max_position_notional,
            config.risk.max_concentration_ratio, config.risk.max_gross_exposure_multiple,
            config.risk.max_drawdown))
        self._closes: list[float] = []
        self._pending = None
        self._cash_flow = 0.0
        self._cursor = 0
        self._orders = self._fills = self._events = 0
        self._identities: set[str] = set()
        snapshot = journal.snapshot()
        if snapshot.tail is not None and snapshot.tail.run_id != run_id:
            raise TransitionError("journal/lifecycle run ID mismatch")
        self._journal_snapshot = snapshot
        self._portfolio_snapshot = self._ledger.snapshot()
        self._account_snapshot = self._account.value(self._portfolio_snapshot, ())
        self._risk_snapshot = self._portfolio_risk.evaluate(self._account_snapshot)

    @property
    def state(self) -> PaperTransitionState:
        return PaperTransitionState(
            self._cursor, self._pending, self._portfolio_snapshot,
            self._account_snapshot, self._risk_snapshot,
            TransitionCounters(self._cursor, self._orders, self._fills, self._events),
            self._journal_snapshot,
        )

    def status_projection(self) -> dict[str, object]:
        """Build a non-authoritative view directly from the canonical state."""
        state = self.state
        return {
            "authoritative": False,
            "run_id": self.run_id,
            "input_cursor": state.input_cursor,
            "pending_order": (
                state.pending_order.order.order_id if state.pending_order is not None else None
            ),
            "positions": [
                {"source": p.source, "symbol": p.symbol,
                 "quantity": p.signed_quantity, "average_entry_price": p.average_entry_price}
                for p in state.portfolio_snapshot.positions
            ],
            "equity": state.account_snapshot.equity,
            "risk_outcome": state.risk_snapshot.outcome.value,
            "counters": {
                "inputs": state.counters.inputs, "orders": state.counters.orders,
                "fills": state.counters.fills,
                "journal_events": state.counters.journal_events,
            },
        }

    def transition(self, bar: Bar) -> PaperTransitionState:
        if not isinstance(bar, Bar):
            raise TransitionError("input must be one normalized Bar")
        cursor = self._cursor + 1
        timestamp = bar.timestamp.isoformat().replace("+00:00", "Z")
        input_domain = {"cursor": cursor, "timestamp": timestamp, "open": bar.open,
                        "high": bar.high, "low": bar.low, "close": bar.close,
                        "volume": bar.volume, "funding_rate": bar.funding_rate}
        input_id = deterministic_id(self.run_id, timestamp, "input", input_domain)
        transition_id = deterministic_id(self.run_id, input_id, "transition", cursor)
        if input_id in self._identities or transition_id in self._identities:
            raise TransitionError("duplicate deterministic identity in uninterrupted run")
        protocol = StageProtocol()

        def emit(stage: str, payload: dict[str, object]) -> None:
            protocol.accept(stage)
            record = self.journal.append(
                self.run_id, transition_id, stage, "paper_market_transition", timestamp,
                cursor if stage == "transition_committed" else self._cursor,
                {"input_event_id": input_id, **payload},
            )
            if record.journal_event_id in self._identities:
                raise TransitionError("duplicate journal event identity")
            self._identities.add(record.journal_event_id)
            self._events += 1
            self._journal_snapshot = self.journal.snapshot()

        emit("transition_started", {})
        emit("input_committed", input_domain)
        self._clock["value"] = bar.timestamp
        carried = _position(self._ledger, self.config.data.source, self.config.data.symbol)
        funding = -carried * bar.close * bar.funding_rate if bar.funding_rate is not None else 0.0
        commission = 0.0

        pending_fill: tuple[str, str, float] | None = None
        if self._pending is not None:
            intent = self._pending
            signed = intent.order.quantity * (1 if intent.order.side.value == "buy" else -1)
            fill_price = bar.open * (1 + (1 if signed > 0 else -1) * self.config.costs.slippage_bps / 10000)
            fill_id = deterministic_id(self.run_id, input_id, "fill", intent.order.order_id)
            report = self._paper.fill(intent.order.order_id, fill_price)
            self._ledger.apply(report)
            pending_fill = (fill_id, report.order.order_id, fill_price)
            commission = abs(signed * fill_price) * self.config.costs.commission_bps / 10000
            self._fills += 1
            self._pending = None

        current = _position(self._ledger, self.config.data.source, self.config.data.symbol)
        self._closes.append(bar.close)
        normalized = self.strategy.target(StrategyContext(bar, tuple(self._closes),
                                                           current / self.config.risk.max_position))
        if (isinstance(normalized, bool) or not isinstance(normalized, (int, float))
                or not isfinite(normalized) or not -1 <= normalized <= 1):
            raise TransitionError("strategy target must be finite and normalized")
        target, requested = float(normalized) * self.config.risk.max_position, 0.0
        requested = target - current
        emit("strategy_committed", {"normalized_target": float(normalized), "target": target})

        if requested:
            direction = AlphaDirection.LONG if requested > 0 else AlphaDirection.SHORT
            decision = DecisionEngine(self._bus, ThresholdDecisionPolicy(
                clock=lambda timestamp=bar.timestamp: timestamp)).decide(
                    _candidate(self.config, bar, direction))
            assessment = RiskEngine(self._bus, ThresholdRiskPolicy(
                clock=lambda timestamp=bar.timestamp: timestamp)).assess(decision)
            order_id = deterministic_id(self.run_id, input_id, "order", {"quantity": requested})
            execution = ExecutionEngine(self._bus, FixedQuantityExecutionPolicy(
                abs(requested), clock=lambda timestamp=bar.timestamp: timestamp,
                order_id_factory=lambda: order_id))
            intent = execution.create_intent(assessment)
            self._paper.submit(intent)
            self._orders += 1
            emit("order_submitted", {"order_id": order_id, "quantity": requested})
            if pending_fill is not None:
                fill_id, pending_order_id, pending_price = pending_fill
                emit("fill_prepared", {"fill_id": fill_id, "order_id": pending_order_id,
                                       "fill_price": pending_price})
                emit("fill_committed", {"fill_id": fill_id, "order_id": pending_order_id})
            if self.config.fill_timing == "next_open":
                self._pending = intent
            else:
                fill_price = bar.close * (1 + (1 if requested > 0 else -1) * self.config.costs.slippage_bps / 10000)
                fill_id = deterministic_id(self.run_id, input_id, "fill", order_id)
                emit("fill_prepared", {"fill_id": fill_id, "order_id": order_id,
                                       "fill_price": fill_price})
                report = self._paper.fill(order_id, fill_price)
                emit("fill_committed", {"fill_id": fill_id, "order_id": order_id})
                self._ledger.apply(report)
                commission += abs(requested * fill_price) * self.config.costs.commission_bps / 10000
                self._fills += 1
        elif pending_fill is not None:
            # The authority committed this next-open fill before strategy.  Its
            # facts are ordered here solely to satisfy the durable protocol.
            fill_id, pending_order_id, pending_price = pending_fill
            emit("fill_prepared", {"fill_id": fill_id, "order_id": pending_order_id,
                                   "fill_price": pending_price})
            emit("fill_committed", {"fill_id": fill_id, "order_id": pending_order_id})

        self._portfolio_snapshot = self._ledger.snapshot()
        emit("portfolio_committed", {"positions": len(self._portfolio_snapshot.positions)})
        marks = tuple(MarketDataRecord(
            MarketDataKind.MARK_PRICE, p.symbol, p.source, bar.timestamp, {"price": bar.close})
            for p in self._portfolio_snapshot.positions if p.side is not PositionSide.FLAT)
        self._cash_flow += funding - commission
        self._account_snapshot = self._account.value(
            self._portfolio_snapshot, marks, cash_flow=self._cash_flow)
        emit("account_committed", {"equity": self._account_snapshot.equity,
                                   "cash_flow": self._cash_flow, "commission": commission,
                                   "funding": funding})
        self._risk_snapshot = self._portfolio_risk.evaluate(self._account_snapshot)
        emit("risk_committed", {"outcome": self._risk_snapshot.outcome.value,
                                "drawdown": self._risk_snapshot.drawdown_ratio,
                                "breaches": [b.code.value for b in self._risk_snapshot.breaches]})
        emit("transition_committed", {"orders": self._orders, "fills": self._fills})
        protocol.complete()
        self._cursor = cursor
        self._identities.update((input_id, transition_id))
        return self.state
