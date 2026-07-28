"""Thread-safe average-cost position and portfolio ledger."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from collections.abc import Iterator
from dataclasses import dataclass, field
from math import fsum
from threading import RLock

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioLedgerError
from quant_futures.domain.order import OrderSide, OrderStatus
from quant_futures.execution.paper.models import PaperExecutionReport
from quant_futures.portfolio.models import (
    EMPTY_PORTFOLIO_TIMESTAMP,
    ZERO_TOLERANCE,
    PortfolioSnapshot,
    PositionSide,
    PositionSnapshot,
    PositionUpdate,
)

_ACTIVE_PORTFOLIO_LEDGERS: ContextVar[frozenset[int]] = ContextVar(
    "_ACTIVE_PORTFOLIO_LEDGERS", default=frozenset()
)


@dataclass(slots=True)
class PortfolioLedger:
    event_bus: EventBus
    _positions: dict[tuple[str, str], PositionSnapshot] = field(init=False, repr=False)
    _history: dict[tuple[str, str], list[PositionUpdate]] = field(init=False, repr=False)
    _processed: dict[str, PositionUpdate] = field(init=False, repr=False)
    _lock: RLock = field(init=False, repr=False)
    _transition_active: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.event_bus, EventBus):
            raise DomainValidationError("event_bus must be an EventBus")
        self._positions = {}
        self._history = {}
        self._processed = {}
        self._lock = RLock()
        self._transition_active = False

    def apply(self, execution_report: PaperExecutionReport) -> PositionUpdate:
        with self._transition_guard():
            if not isinstance(execution_report, PaperExecutionReport):
                raise DomainValidationError("execution_report must be a PaperExecutionReport")
            execution_report.validate()
            if execution_report.order.status is not OrderStatus.FILLED:
                raise PortfolioLedgerError("only FILLED execution reports may be applied")
            order_id = execution_report.order.order_id
            if order_id in self._processed:
                raise PortfolioLedgerError(f"order_id already processed: {order_id}")
            intent = execution_report.execution_intent
            key = (intent.source, intent.symbol)
            previous = self._positions.get(key)
            if previous is not None and execution_report.occurred_at < previous.updated_at:
                raise PortfolioLedgerError("fill occurred before the current position update")
            current, delta = self._account(previous, execution_report)
            prospective = dict(self._positions)
            prospective[key] = current
            portfolio = self._make_snapshot(prospective)
            update = PositionUpdate(
                execution_report, previous, current, delta, portfolio,
                execution_report.occurred_at,
            )
            update.validate()
            self._positions[key] = current
            self._history.setdefault(key, []).append(update)
            self._processed[order_id] = update
            self._publish(update)
            return update

    def get(self, source: str, symbol: str) -> PositionSnapshot:
        key = self._validate_key(source, symbol)
        with self._lock:
            try:
                return self._positions[key]
            except KeyError as exc:
                raise PortfolioLedgerError(f"unknown position: {source}/{symbol}") from exc

    def positions(self) -> tuple[PositionSnapshot, ...]:
        with self._lock:
            return tuple(self._positions[key] for key in sorted(self._positions))

    def history(self, source: str, symbol: str) -> tuple[PositionUpdate, ...]:
        key = self._validate_key(source, symbol)
        with self._lock:
            try:
                return tuple(self._history[key])
            except KeyError as exc:
                raise PortfolioLedgerError(f"unknown position: {source}/{symbol}") from exc

    def processed(self, order_id: str) -> PositionUpdate:
        self._non_empty("order_id", order_id)
        with self._lock:
            try:
                return self._processed[order_id]
            except KeyError as exc:
                raise PortfolioLedgerError(f"unknown order_id: {order_id}") from exc

    def snapshot(self) -> PortfolioSnapshot:
        with self._lock:
            return self._make_snapshot(self._positions)

    @staticmethod
    def _account(previous: PositionSnapshot | None,
                 report: PaperExecutionReport) -> tuple[PositionSnapshot, float]:
        quantity = float(report.filled_quantity)
        price = float(report.average_fill_price)
        x = quantity if report.order.side is OrderSide.BUY else -quantity
        q = previous.signed_quantity if previous else 0.0
        old_average = previous.average_entry_price if previous else None
        cumulative = previous.realized_pnl if previous else 0.0
        delta = 0.0
        if q == 0.0:
            new_quantity, new_average = x, price
        elif q * x > 0:
            new_quantity = q + x
            new_average = (abs(q) * old_average + abs(x) * price) / abs(new_quantity)
        else:
            closed = min(abs(q), abs(x))
            delta = ((price - old_average) if q > 0 else (old_average - price)) * closed
            new_quantity = q + x
            if abs(new_quantity) <= ZERO_TOLERANCE:
                new_quantity, new_average = 0.0, None
            elif q * new_quantity > 0:
                new_average = old_average
            else:
                new_average = price
        if abs(delta) <= ZERO_TOLERANCE:
            delta = 0.0
        realized = cumulative + delta
        if abs(realized) <= ZERO_TOLERANCE:
            realized = 0.0
        side = PositionSide.FLAT if new_quantity == 0.0 else (
            PositionSide.LONG if new_quantity > 0 else PositionSide.SHORT
        )
        intent = report.execution_intent
        return PositionSnapshot(
            intent.source, intent.symbol, new_quantity, side, new_average, realized,
            report.occurred_at, report.order.order_id,
        ), delta

    @staticmethod
    def _make_snapshot(positions: dict[tuple[str, str], PositionSnapshot]) -> PortfolioSnapshot:
        ordered = tuple(positions[key] for key in sorted(positions))
        total = fsum(position.realized_pnl for position in ordered)
        if abs(total) <= ZERO_TOLERANCE:
            total = 0.0
        updated_at = max((position.updated_at for position in ordered),
                         default=EMPTY_PORTFOLIO_TIMESTAMP)
        return PortfolioSnapshot(ordered, total, updated_at)

    def _publish(self, update: PositionUpdate) -> None:
        report = update.execution_report
        intent = report.execution_intent
        risk = intent.risk_assessment
        decision = risk.decision_intent
        alpha = decision.alpha_candidate
        self.event_bus.publish(Event(EventType.PORTFOLIO_UPDATED, {
            "position_update": update,
            "execution_report": report,
            "previous_position": update.previous_position,
            "current_position": update.current_position,
            "portfolio_snapshot": update.portfolio_snapshot,
            "execution_intent": intent,
            "risk_assessment": risk,
            "decision_intent": decision,
            "alpha_candidate": alpha,
            "timing_assessment": alpha.timing_assessment,
            "observation": alpha.observation,
        }, occurred_at=update.applied_at))

    @contextmanager
    def _transition_guard(self) -> Iterator[None]:
        ledger_id = id(self)
        if ledger_id in _ACTIVE_PORTFOLIO_LEDGERS.get():
            raise PortfolioLedgerError("portfolio transitions must not be re-entered")
        with self._lock:
            active = _ACTIVE_PORTFOLIO_LEDGERS.get()
            if ledger_id in active:
                raise PortfolioLedgerError("portfolio transitions must not be re-entered")
            token = _ACTIVE_PORTFOLIO_LEDGERS.set(active | {ledger_id})
            self._transition_active = True
            try:
                yield
            finally:
                self._transition_active = False
                _ACTIVE_PORTFOLIO_LEDGERS.reset(token)

    @classmethod
    def _validate_key(cls, source: object, symbol: object) -> tuple[str, str]:
        cls._non_empty("source", source)
        cls._non_empty("symbol", symbol)
        return source, symbol

    @staticmethod
    def _non_empty(name: str, value: object) -> None:
        if not isinstance(value, str) or not value.strip():
            raise DomainValidationError(f"{name} must be a non-empty string")
