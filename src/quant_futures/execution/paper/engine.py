"""Thread-safe, in-memory paper order lifecycle orchestration."""

from copy import deepcopy
from contextlib import contextmanager
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import isfinite
from numbers import Real
from threading import RLock
from typing import Callable

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, OrderLifecycleError
from quant_futures.domain.order import Order, OrderStatus
from quant_futures.execution.models import ExecutionIntent

from .models import PaperExecutionReport


@dataclass(slots=True)
class PaperExecutionEngine:
    """Apply only the four supported paper transitions and retain their audit trail."""

    event_bus: EventBus
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    _current: dict[str, PaperExecutionReport] = field(init=False, repr=False)
    _history: dict[str, list[PaperExecutionReport]] = field(init=False, repr=False)
    _lock: RLock = field(init=False, repr=False)
    _transition_active: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.event_bus, EventBus):
            raise DomainValidationError("event_bus must be an EventBus")
        if not callable(self.clock):
            raise DomainValidationError("clock must be callable")
        self._current = {}
        self._history = {}
        self._lock = RLock()
        self._transition_active = False

    def submit(self, execution_intent: ExecutionIntent) -> PaperExecutionReport:
        with self._transition_guard():
            report = self._register(execution_intent, OrderStatus.SUBMITTED)
            self._publish(report)
            return report

    def reject(self, execution_intent: ExecutionIntent, reason: str) -> PaperExecutionReport:
        with self._transition_guard():
            self._validate_reason(reason)
            report = self._register(execution_intent, OrderStatus.REJECTED, reason)
            self._publish(report)
            return report

    def fill(self, order_id: str, fill_price: float) -> PaperExecutionReport:
        with self._transition_guard():
            if (
                not isinstance(fill_price, Real)
                or isinstance(fill_price, bool)
                or not isfinite(fill_price)
                or fill_price <= 0
            ):
                raise DomainValidationError("fill_price must be a finite positive number")
            report = self._advance(order_id, OrderStatus.FILLED, fill_price=fill_price)
            self._publish(report)
            return report

    def cancel(self, order_id: str, reason: str) -> PaperExecutionReport:
        with self._transition_guard():
            self._validate_reason(reason)
            report = self._advance(order_id, OrderStatus.CANCELLED, reason=reason)
            self._publish(report)
            return report

    def get(self, order_id: str) -> PaperExecutionReport:
        self._validate_order_id(order_id)
        with self._lock:
            try:
                return self._current[order_id]
            except KeyError as exc:
                raise OrderLifecycleError(f"unknown order_id: {order_id}") from exc

    def history(self, order_id: str) -> tuple[PaperExecutionReport, ...]:
        self._validate_order_id(order_id)
        with self._lock:
            try:
                return tuple(self._history[order_id])
            except KeyError as exc:
                raise OrderLifecycleError(f"unknown order_id: {order_id}") from exc

    def _register(
        self,
        intent: ExecutionIntent,
        status: OrderStatus,
        reason: str | None = None,
    ) -> PaperExecutionReport:
        if not isinstance(intent, ExecutionIntent):
            raise DomainValidationError("execution_intent must be an ExecutionIntent")
        intent.validate()
        order_id = intent.order.order_id
        if order_id in self._current or order_id in self._history:
            raise OrderLifecycleError(f"order_id already exists: {order_id}")
        snapshot, identities = self._capture_integrity(intent)
        occurred_at = self._read_clock()
        self._verify_integrity(intent, snapshot, identities)
        if order_id in self._current or order_id in self._history:
            raise OrderLifecycleError("clock must not modify paper execution ledgers")
        report = PaperExecutionReport(
            intent,
            self._copy_order(intent.order, status),
            OrderStatus.CREATED,
            occurred_at,
            reason=reason,
        )
        report.validate()
        self._current[order_id] = report
        self._history[order_id] = [report]
        return report

    def _advance(
        self,
        order_id: str,
        status: OrderStatus,
        *,
        reason: str | None = None,
        fill_price: float | None = None,
    ) -> PaperExecutionReport:
        self._validate_order_id(order_id)
        current = self._lookup(order_id)
        if current.order.status is not OrderStatus.SUBMITTED:
            raise OrderLifecycleError("only a SUBMITTED order may be filled or cancelled")
        current_before = current
        intent_before = current.execution_intent
        order_before = current.order
        current_value_snapshot = deepcopy(current)
        history_before = self._history[order_id]
        reports_before = tuple(history_before)
        history_snapshot = deepcopy(history_before)
        snapshot, identities = self._capture_integrity(current.execution_intent)
        try:
            occurred_at = self._read_clock()
        except BaseException:
            self._restore_current_report(current, intent_before, order_before)
            raise
        if self._current.get(order_id) is not current_before:
            self._restore_current_report(current, intent_before, order_before)
            raise OrderLifecycleError("current order changed during transition")
        if current.execution_intent is not intent_before:
            self._restore_current_report(current, intent_before, order_before)
            raise DomainValidationError(
                "clock must not replace the current report execution intent"
            )
        if current.order is not order_before:
            self._restore_current_report(current, intent_before, order_before)
            raise DomainValidationError("clock must not replace the current report order")
        if current != current_value_snapshot:
            self._restore_current_report(current, intent_before, order_before)
            raise DomainValidationError("clock must not mutate the current paper execution report")
        self._verify_integrity(current.execution_intent, snapshot, identities)
        if self._history.get(order_id) is not history_before:
            raise DomainValidationError("clock must not replace the paper execution history list")
        if len(history_before) != len(reports_before) or any(
            actual is not expected
            for actual, expected in zip(history_before, reports_before, strict=True)
        ):
            raise DomainValidationError("clock must not replace paper execution history reports")
        if history_before != history_snapshot:
            raise DomainValidationError("clock must not mutate paper execution history")
        current.validate()
        if occurred_at < current.occurred_at:
            raise DomainValidationError("occurred_at must not precede the previous report")
        report = PaperExecutionReport(
            current.execution_intent,
            self._copy_order(current.order, status),
            OrderStatus.SUBMITTED,
            occurred_at,
            reason=reason,
            filled_quantity=current.order.quantity if status is OrderStatus.FILLED else None,
            average_fill_price=fill_price,
        )
        report.validate()
        self._current[order_id] = report
        history_before.append(report)
        return report

    @contextmanager
    def _transition_guard(self) -> Iterator[None]:
        """Serialize transitions and reject callback-driven lifecycle re-entry."""
        with self._lock:
            if self._transition_active:
                raise OrderLifecycleError("paper lifecycle transitions must not be re-entered")
            self._transition_active = True
            try:
                yield
            finally:
                self._transition_active = False

    def _read_clock(self) -> datetime:
        clock_before = self.clock
        if not callable(clock_before):
            raise DomainValidationError("clock must be callable")
        occurred_at = clock_before()
        if self.clock is not clock_before:
            raise DomainValidationError("clock must not be replaced during a transition")
        if not isinstance(occurred_at, datetime):
            raise DomainValidationError("clock must return a timezone-aware datetime")
        try:
            offset = occurred_at.utcoffset()
        except Exception as exc:
            raise DomainValidationError("clock must return a timezone-aware datetime") from exc
        if occurred_at.tzinfo is None or offset is None:
            raise DomainValidationError("clock must return a timezone-aware datetime")
        return occurred_at

    @staticmethod
    def _capture_integrity(intent: ExecutionIntent) -> tuple[ExecutionIntent, tuple[object, ...]]:
        risk = intent.risk_assessment
        decision = risk.decision_intent
        alpha = decision.alpha_candidate
        return deepcopy(intent), (
            intent,
            intent.order,
            risk,
            decision,
            alpha,
            alpha.timing_assessment,
            alpha.observation,
        )

    @staticmethod
    def _verify_integrity(
        intent: ExecutionIntent,
        snapshot: ExecutionIntent,
        identities: tuple[object, ...],
    ) -> None:
        intent.validate()
        risk = intent.risk_assessment
        decision = risk.decision_intent
        alpha = decision.alpha_candidate
        after = (
            intent,
            intent.order,
            risk,
            decision,
            alpha,
            alpha.timing_assessment,
            alpha.observation,
        )
        if intent != snapshot:
            raise DomainValidationError("clock must not mutate ExecutionIntent or its lineage")
        if any(left is not right for left, right in zip(after, identities, strict=True)):
            raise DomainValidationError("clock must not replace ExecutionIntent lineage objects")

    @staticmethod
    def _restore_current_report(
        current: PaperExecutionReport,
        intent: ExecutionIntent,
        order: Order,
    ) -> None:
        """Undo only illegal callback replacement of a current report's fields."""
        if current.execution_intent is not intent:
            object.__setattr__(current, "execution_intent", intent)
        if current.order is not order:
            object.__setattr__(current, "order", order)

    def _lookup(self, order_id: str) -> PaperExecutionReport:
        try:
            return self._current[order_id]
        except KeyError as exc:
            raise OrderLifecycleError(f"unknown order_id: {order_id}") from exc

    @staticmethod
    def _copy_order(order: Order, status: OrderStatus) -> Order:
        return Order(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=order.price,
            status=status,
            created_at=order.created_at,
        )

    def _publish(self, report: PaperExecutionReport) -> None:
        intent = report.execution_intent
        risk = intent.risk_assessment
        decision = risk.decision_intent
        alpha = decision.alpha_candidate
        self.event_bus.publish(Event(EventType.EXECUTION_UPDATED, {
            "paper_execution_report": report,
            "execution_intent": intent,
            "order": report.order,
            "previous_status": report.previous_status,
            "risk_assessment": risk,
            "decision_intent": decision,
            "alpha_candidate": alpha,
            "timing_assessment": alpha.timing_assessment,
            "observation": alpha.observation,
        }))

    @staticmethod
    def _validate_order_id(order_id: object) -> None:
        if not isinstance(order_id, str) or not order_id.strip():
            raise OrderLifecycleError("order_id must be a non-empty string")

    @staticmethod
    def _validate_reason(reason: object) -> None:
        if not isinstance(reason, str) or not reason.strip():
            raise DomainValidationError("reason must be a non-empty string")
