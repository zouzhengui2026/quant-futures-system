"""Thread-safe average-cost position and portfolio ledger."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from collections.abc import Iterator
from dataclasses import dataclass, field
from threading import RLock

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioLedgerError
from quant_futures.alpha.models import AlphaCandidate
from quant_futures.decision.models import DecisionIntent
from quant_futures.domain.order import Order, OrderSide, OrderStatus
from quant_futures.execution.models import ExecutionIntent
from quant_futures.execution.paper.models import PaperExecutionReport
from quant_futures.observation.models import MarketObservation
from quant_futures.portfolio.models import (
    EMPTY_PORTFOLIO_TIMESTAMP,
    PortfolioSnapshot,
    PositionSide,
    PositionSnapshot,
    PositionUpdate,
    _account_position,
    _realized_total,
)
from quant_futures.risk.models import RiskAssessment
from quant_futures.timing.models import TimingAssessment

_ACTIVE_PORTFOLIO_LEDGERS: ContextVar[frozenset[int]] = ContextVar(
    "_ACTIVE_PORTFOLIO_LEDGERS", default=frozenset()
)


@dataclass(frozen=True, slots=True)
class _CommittedIdentity:
    """Exact object references for one committed execution lineage."""

    update: PositionUpdate
    report: PaperExecutionReport
    order: Order
    execution_intent: ExecutionIntent
    risk_assessment: RiskAssessment
    decision_intent: DecisionIntent
    alpha_candidate: AlphaCandidate
    timing_assessment: TimingAssessment
    observation: MarketObservation
    current_position: PositionSnapshot
    portfolio_snapshot: PortfolioSnapshot
    portfolio_positions: tuple[PositionSnapshot, ...]


@dataclass(slots=True)
class PortfolioLedger:
    event_bus: EventBus
    _positions: dict[tuple[str, str], PositionSnapshot] = field(init=False, repr=False)
    _history: dict[tuple[str, str], list[PositionUpdate]] = field(init=False, repr=False)
    _processed: dict[str, PositionUpdate] = field(init=False, repr=False)
    _committed_identity: dict[str, _CommittedIdentity] = field(init=False, repr=False)
    _lock: RLock = field(init=False, repr=False)
    _transition_active: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.event_bus, EventBus):
            raise DomainValidationError("event_bus must be an EventBus")
        self._positions = {}
        self._history = {}
        self._processed = {}
        self._committed_identity = {}
        self._lock = RLock()
        self._transition_active = False

    def apply(self, execution_report: PaperExecutionReport) -> PositionUpdate:
        """Validate and atomically commit one fill.

        Existing audit objects are revalidated before any new accounting state
        is committed.  Adversarial ``object.__setattr__`` edits are therefore
        detected fail-closed; because callers hold the exact immutable audit
        objects, the ledger detects such pre-existing edits but does not claim
        to restore their former values.
        """
        with self._transition_guard():
            if not isinstance(execution_report, PaperExecutionReport):
                raise DomainValidationError("execution_report must be a PaperExecutionReport")
            execution_report.validate()
            if execution_report.order.status is not OrderStatus.FILLED:
                raise PortfolioLedgerError("only FILLED execution reports may be applied")
            self._validate_committed_state()
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
            risk = intent.risk_assessment
            decision = risk.decision_intent
            alpha = decision.alpha_candidate
            self._committed_identity[order_id] = _CommittedIdentity(
                update=update,
                report=execution_report,
                order=execution_report.order,
                execution_intent=intent,
                risk_assessment=risk,
                decision_intent=decision,
                alpha_candidate=alpha,
                timing_assessment=alpha.timing_assessment,
                observation=alpha.observation,
                current_position=current,
                portfolio_snapshot=portfolio,
                portfolio_positions=portfolio.positions,
            )
            self._publish(update)
            return update

    def _validate_committed_state(self) -> None:
        """Fail closed unless the complete committed audit graph is intact."""
        try:
            if set(self._positions) != set(self._history):
                raise PortfolioLedgerError("position and history keys must match")
            seen: dict[str, PositionUpdate] = {}
            for key, history in self._history.items():
                if not isinstance(history, list) or not history:
                    raise PortfolioLedgerError("every history must be a non-empty list")
                previous_update: PositionUpdate | None = None
                for update in history:
                    if not isinstance(update, PositionUpdate):
                        raise PortfolioLedgerError("history must contain PositionUpdate values")
                    update.validate()
                    current_key = (update.current_position.source,
                                   update.current_position.symbol)
                    if current_key != key:
                        raise PortfolioLedgerError("history update key does not match its key")
                    if previous_update is None:
                        if update.previous_position is not None:
                            raise PortfolioLedgerError("first update must not have a previous position")
                    else:
                        if update.previous_position is not previous_update.current_position:
                            raise PortfolioLedgerError("history position identity chain is broken")
                        if update.applied_at < previous_update.applied_at:
                            raise PortfolioLedgerError("history timestamps must be nondecreasing")
                    order_id = update.execution_report.order.order_id
                    if order_id in seen:
                        raise PortfolioLedgerError("history order IDs must be globally unique")
                    seen[order_id] = update
                    identity = self._committed_identity.get(order_id)
                    if identity is None or identity.update is not update:
                        raise PortfolioLedgerError("stored update identity was replaced")
                    report = update.execution_report
                    intent = report.execution_intent
                    risk = intent.risk_assessment
                    decision = risk.decision_intent
                    alpha = decision.alpha_candidate
                    portfolio = update.portfolio_snapshot
                    if (identity.report is not report
                            or identity.order is not report.order
                            or identity.execution_intent is not intent
                            or identity.risk_assessment is not risk
                            or identity.decision_intent is not decision
                            or identity.alpha_candidate is not alpha
                            or identity.timing_assessment is not alpha.timing_assessment
                            or identity.observation is not alpha.observation
                            or identity.current_position is not update.current_position
                            or identity.portfolio_snapshot is not portfolio):
                        raise PortfolioLedgerError("stored audit object identity was replaced")
                    if (len(portfolio.positions) != len(identity.portfolio_positions)
                            or any(actual is not committed for actual, committed in zip(
                                portfolio.positions, identity.portfolio_positions))):
                        raise PortfolioLedgerError(
                            "stored portfolio position identities were replaced")
                    previous_update = update
                if self._positions[key] is not history[-1].current_position:
                    raise PortfolioLedgerError("position must be the last history position")
            if set(self._processed) != set(seen):
                raise PortfolioLedgerError("processed order IDs must exactly match history")
            if set(self._committed_identity) != set(seen):
                raise PortfolioLedgerError("identity commitments must exactly match history")
            if any(self._processed[order_id] is not update
                   for order_id, update in seen.items()):
                raise PortfolioLedgerError("processed update identity does not match history")
        except (DomainValidationError, PortfolioLedgerError):
            raise
        except Exception as exc:
            raise PortfolioLedgerError("committed portfolio state is structurally corrupt") from exc

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
        return _account_position(previous, report)

    @staticmethod
    def _make_snapshot(positions: dict[tuple[str, str], PositionSnapshot]) -> PortfolioSnapshot:
        ordered = tuple(positions[key] for key in sorted(positions))
        total = _realized_total(ordered)
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
