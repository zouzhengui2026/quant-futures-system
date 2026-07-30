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
from typing import Callable

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

from .journal import JournalSnapshot, TransitionJournal
from .lifecycle import Lifecycle, LifecycleState
from .lock import RunDirectoryLock

VALID_STAGES = frozenset({
    "transition_started", "input_committed", "strategy_committed",
    "order_submitted", "fill_prepared", "fill_committed",
    "portfolio_committed", "account_committed", "risk_committed",
    "transition_committed",
})
class TransitionError(RuntimeError):
    """The incremental transition is inconsistent and must fail closed."""


class StageProtocol:
    """Validate a single input's ordered, unique stage sequence."""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def accept(self, stage: str) -> None:
        if stage not in VALID_STAGES or stage in self._seen:
            raise TransitionError(f"illegal or duplicate transition stage: {stage}")
        requirements = {
            "transition_started": set(),
            "input_committed": {"transition_started"},
            "fill_prepared": {"input_committed"},
            "fill_committed": {"fill_prepared"},
            "portfolio_committed": {"input_committed"},
            "strategy_committed": {"input_committed"},
            "order_submitted": {"strategy_committed"},
            "account_committed": {"strategy_committed", "portfolio_committed"},
            "risk_committed": {"account_committed"},
            "transition_committed": {"risk_committed"},
        }
        if not requirements[stage].issubset(self._seen):
            missing = sorted(requirements[stage] - self._seen)
            raise TransitionError(
                f"illegal transition stage order: {stage} requires {', '.join(missing)}"
            )
        if stage == "portfolio_committed" and "fill_prepared" in self._seen and "fill_committed" not in self._seen:
            raise TransitionError("portfolio commit requires the prepared fill to be committed")
        if stage == "strategy_committed" and "fill_committed" in self._seen and "portfolio_committed" not in self._seen:
            raise TransitionError("strategy cannot precede a carried fill's portfolio effect")
        self._seen.add(stage)

    def complete(self) -> None:
        if "transition_committed" not in self._seen:
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
                 journal: TransitionJournal,
                 failure_injector: Callable[[str], None] | None = None) -> None:
        if not run_id.strip():
            raise TransitionError("run ID must be non-empty")
        self.run_id, self.config, self.strategy, self.journal = run_id, config, strategy, journal
        self._failure_injector = failure_injector or (lambda boundary: None)
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
        self._poisoned = False
        self._last_input_identity: str | None = None
        snapshot = journal.snapshot()
        if snapshot.tail is not None and snapshot.tail.run_id != run_id:
            raise TransitionError("journal/lifecycle run ID mismatch")
        if snapshot.tail is not None:
            raise TransitionError(
                "non-empty journal requires Checkpoint 4 authoritative restoration"
            )
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
        """Commit one event while holding the run-directory writer lock throughout."""
        if not isinstance(bar, Bar):
            raise TransitionError("input must be one normalized Bar")
        if self._poisoned:
            raise TransitionError("coordinator is unusable after a partial transition")
        with RunDirectoryLock(self.journal.run_directory):
            try:
                return self._transition_held(bar)
            except BaseException:
                # Checkpoint 4 will restore authorities.  Until then, none of
                # the canonical engines may be reused after an uncertain cut.
                self._poisoned = True
                raise

    def _transition_held(self, bar: Bar) -> PaperTransitionState:
        persisted = self.journal._snapshot_held()
        if persisted != self._journal_snapshot:
            raise TransitionError("journal advanced outside this coordinator")
        lifecycle_path = self.journal.run_directory / Lifecycle.filename
        if lifecycle_path.exists():
            lifecycle = Lifecycle(self.journal.run_directory).current()
            if lifecycle.run_id != self.run_id or lifecycle.state is not LifecycleState.RUNNING:
                raise TransitionError("lifecycle is not eligible for a market transition")
        cursor = self._cursor + 1
        timestamp = bar.timestamp.isoformat().replace("+00:00", "Z")
        input_domain = {"cursor": cursor, "timestamp": timestamp, "open": bar.open,
                        "high": bar.high, "low": bar.low, "close": bar.close,
                        "volume": bar.volume, "funding_rate": bar.funding_rate}
        input_id = deterministic_id(
            self.run_id, timestamp, "input",
            {key: value for key, value in input_domain.items() if key != "cursor"},
        )
        transition_id = deterministic_id(self.run_id, input_id, "transition", cursor)
        if input_id == self._last_input_identity:
            raise TransitionError("duplicate deterministic identity in uninterrupted run")
        protocol = StageProtocol()

        def emit(stage: str, payload: dict[str, object]) -> None:
            protocol.accept(stage)
            self._failure_injector(f"journal:{stage}")
            record = self.journal._append_held(
                self.run_id, transition_id, stage, "paper_market_transition", timestamp,
                cursor if stage == "transition_committed" else self._cursor,
                {"input_event_id": input_id, **payload},
            )
            self._events += 1

        emit("transition_started", {})
        emit("input_committed", input_domain)
        self._clock["value"] = bar.timestamp
        carried = _position(self._ledger, self.config.data.source, self.config.data.symbol)
        funding = -carried * bar.close * bar.funding_rate if bar.funding_rate is not None else 0.0
        commission = 0.0

        pending_fill: tuple[str, str, float, float] | None = None
        if self._pending is not None:
            intent = self._pending
            signed = intent.order.quantity * (1 if intent.order.side.value == "buy" else -1)
            fill_price = bar.open * (1 + (1 if signed > 0 else -1) * self.config.costs.slippage_bps / 10000)
            fill_id = deterministic_id(self.run_id, input_id, "fill", intent.order.order_id)
            emit("fill_prepared", {"fill_id": fill_id, "order_id": intent.order.order_id,
                                   "fill_price": fill_price})
            self._failure_injector("fill")
            report = self._paper.fill(intent.order.order_id, fill_price)
            emit("fill_committed", {"fill_id": fill_id, "order_id": report.order.order_id,
                                    "quantity": signed, "fill_price": fill_price})
            self._failure_injector("portfolio")
            self._ledger.apply(report)
            commission = abs(signed * fill_price) * self.config.costs.commission_bps / 10000
            pending_fill = (fill_id, report.order.order_id, fill_price, commission)
            self._fills += 1
            self._pending = None
            self._portfolio_snapshot = self._ledger.snapshot()
            emit("portfolio_committed", {"positions": _positions_payload(self._portfolio_snapshot)})

        current = _position(self._ledger, self.config.data.source, self.config.data.symbol)
        self._closes.append(bar.close)
        self._failure_injector("strategy")
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
            self._failure_injector("submit")
            self._paper.submit(intent)
            self._orders += 1
            emit("order_submitted", {"order_id": order_id, "quantity": requested})
            if self.config.fill_timing == "next_open":
                self._pending = intent
            else:
                fill_price = bar.close * (1 + (1 if requested > 0 else -1) * self.config.costs.slippage_bps / 10000)
                fill_id = deterministic_id(self.run_id, input_id, "fill", order_id)
                emit("fill_prepared", {"fill_id": fill_id, "order_id": order_id,
                                       "fill_price": fill_price})
                self._failure_injector("fill")
                report = self._paper.fill(order_id, fill_price)
                emit("fill_committed", {"fill_id": fill_id, "order_id": order_id,
                                        "quantity": requested, "fill_price": fill_price})
                self._failure_injector("portfolio")
                self._ledger.apply(report)
                commission += abs(requested * fill_price) * self.config.costs.commission_bps / 10000
                self._fills += 1
        if pending_fill is None:
            self._portfolio_snapshot = self._ledger.snapshot()
            emit("portfolio_committed", {"positions": _positions_payload(self._portfolio_snapshot)})
        marks = tuple(MarketDataRecord(
            MarketDataKind.MARK_PRICE, p.symbol, p.source, bar.timestamp, {"price": bar.close})
            for p in self._portfolio_snapshot.positions if p.side is not PositionSide.FLAT)
        self._cash_flow += funding - commission
        self._failure_injector("account")
        self._account_snapshot = self._account.value(
            self._portfolio_snapshot, marks, cash_flow=self._cash_flow)
        emit("account_committed", {"equity": self._account_snapshot.equity,
                                   "cash_flow": self._cash_flow, "commission": commission,
                                   "funding": funding,
                                   "unrealized_pnl": self._account_snapshot.total_unrealized_pnl})
        self._failure_injector("risk")
        self._risk_snapshot = self._portfolio_risk.evaluate(self._account_snapshot)
        emit("risk_committed", {"outcome": self._risk_snapshot.outcome.value,
                                "drawdown": self._risk_snapshot.drawdown_ratio,
                                "breaches": [b.code.value for b in self._risk_snapshot.breaches]})
        emit("transition_committed", {"orders": self._orders, "fills": self._fills})
        protocol.complete()
        self._cursor = cursor
        self._last_input_identity = input_id
        self._journal_snapshot = self.journal._snapshot_held()
        return self.state


def _positions_payload(snapshot: object) -> list[dict[str, object]]:
    return [
        {"source": position.source, "symbol": position.symbol,
         "quantity": position.signed_quantity,
         "average_entry_price": position.average_entry_price}
        for position in snapshot.positions
    ]
