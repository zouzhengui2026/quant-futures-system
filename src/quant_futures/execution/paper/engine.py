"""Thread-safe, in-memory paper order lifecycle orchestration."""

from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
from collections.abc import Iterator
from dataclasses import dataclass, field, fields, is_dataclass
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

_ACTIVE_PAPER_ENGINES: ContextVar[frozenset[int]] = ContextVar(
    "_ACTIVE_PAPER_ENGINES",
    default=frozenset(),
)


@dataclass(slots=True)
class _ObjectSnapshot:
    value: object
    field_values: dict[str, object]
    value_snapshot: object


@dataclass(slots=True)
class _LedgerSnapshot:
    current_dict: dict[str, PaperExecutionReport]
    history_dict: dict[str, list[PaperExecutionReport]]
    current_items: dict[str, PaperExecutionReport]
    history_items: dict[str, list[PaperExecutionReport]]
    history_reports: dict[str, tuple[PaperExecutionReport, ...]]
    current_value_snapshot: dict[str, PaperExecutionReport]
    history_value_snapshot: dict[str, list[PaperExecutionReport]]
    object_snapshots: tuple[_ObjectSnapshot, ...]
    clock: Callable[[], datetime]
    event_bus: EventBus
    lock: object
    transition_active: bool


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
        callback_snapshot = self._capture_callback_state(intent)
        try:
            occurred_at = self._read_clock()
            self._validate_callback_state(callback_snapshot)
            report = PaperExecutionReport(
                intent,
                self._copy_order(intent.order, status),
                OrderStatus.CREATED,
                occurred_at,
                reason=reason,
            )
            report.validate()
        except BaseException:
            self._rollback_callback(callback_snapshot)
            raise
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
        callback_snapshot = self._capture_callback_state()
        try:
            occurred_at = self._read_clock()
            if self._current.get(order_id) is not current_before:
                raise OrderLifecycleError("current order changed during transition")
            if current.execution_intent is not intent_before:
                raise DomainValidationError(
                    "clock must not replace the current report execution intent"
                )
            if current.order is not order_before:
                raise DomainValidationError("clock must not replace the current report order")
            if current != current_value_snapshot:
                raise DomainValidationError(
                    "clock must not mutate the current paper execution report"
                )
            self._validate_callback_state(callback_snapshot)
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
        except BaseException:
            self._rollback_callback(callback_snapshot)
            raise
        history_before = self._history[order_id]
        self._current[order_id] = report
        history_before.append(report)
        return report

    @contextmanager
    def _transition_guard(self) -> Iterator[None]:
        """Serialize transitions and reject callback-driven lifecycle re-entry."""
        engine_key = id(self)
        active = _ACTIVE_PAPER_ENGINES.get()
        if engine_key in active:
            raise OrderLifecycleError("paper lifecycle transitions must not be re-entered")
        with self._lock:
            active = _ACTIVE_PAPER_ENGINES.get()
            if engine_key in active:
                raise OrderLifecycleError("paper lifecycle transitions must not be re-entered")
            token = _ACTIVE_PAPER_ENGINES.set(active | {engine_key})
            self._transition_active = True
            try:
                yield
            finally:
                self._transition_active = False
                _ACTIVE_PAPER_ENGINES.reset(token)

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

    def _capture_callback_state(self, *extra_roots: object) -> _LedgerSnapshot:
        object_snapshots: dict[int, _ObjectSnapshot] = {}

        def capture(value: object) -> None:
            if is_dataclass(value) and not isinstance(value, type):
                if id(value) in object_snapshots:
                    return
                values = {item.name: getattr(value, item.name) for item in fields(value)}
                object_snapshots[id(value)] = _ObjectSnapshot(value, values, deepcopy(value))
                for child in values.values():
                    capture(child)
            elif isinstance(value, dict):
                for key, child in value.items():
                    capture(key)
                    capture(child)
            elif isinstance(value, (tuple, list)):
                for child in value:
                    capture(child)

        for report in self._current.values():
            capture(report)
        for reports in self._history.values():
            capture(reports)
        for root in extra_roots:
            capture(root)
        return _LedgerSnapshot(
            current_dict=self._current,
            history_dict=self._history,
            current_items=dict(self._current),
            history_items=dict(self._history),
            history_reports={key: tuple(value) for key, value in self._history.items()},
            current_value_snapshot=deepcopy(self._current),
            history_value_snapshot=deepcopy(self._history),
            object_snapshots=tuple(object_snapshots.values()),
            clock=self.clock,
            event_bus=self.event_bus,
            lock=self._lock,
            transition_active=self._transition_active,
        )

    def _validate_callback_state(self, snapshot: _LedgerSnapshot) -> None:
        if self.clock is not snapshot.clock:
            raise DomainValidationError("clock must not be replaced during a transition")
        if self.event_bus is not snapshot.event_bus:
            raise DomainValidationError("clock must not replace the event bus")
        if self._lock is not snapshot.lock:
            raise DomainValidationError("clock must not replace the lifecycle lock")
        if self._transition_active is not True:
            raise DomainValidationError("clock must not modify the transition guard state")
        if self._current is not snapshot.current_dict:
            raise DomainValidationError("clock must not replace the current ledger dictionary")
        if self._history is not snapshot.history_dict:
            raise DomainValidationError("clock must not replace the paper execution history dictionary")
        if set(self._current) != set(snapshot.current_items):
            raise DomainValidationError("clock must not add or remove current ledger orders")
        if any(self._current[key] is not value for key, value in snapshot.current_items.items()):
            raise DomainValidationError("clock must not replace current ledger reports")
        if set(self._history) != set(snapshot.history_items):
            raise DomainValidationError("clock must not add or remove history ledger orders")
        for key, reports in snapshot.history_items.items():
            if self._history[key] is not reports:
                raise DomainValidationError("clock must not replace the paper execution history list")
            expected = snapshot.history_reports[key]
            if len(reports) != len(expected) or any(
                actual is not original
                for actual, original in zip(reports, expected, strict=True)
            ):
                raise DomainValidationError("clock must not replace paper execution history reports")
        if self._current != snapshot.current_value_snapshot:
            raise DomainValidationError("clock must not mutate current ledger reports")
        if self._history != snapshot.history_value_snapshot:
            raise DomainValidationError("clock must not mutate paper execution history")
        for item in snapshot.object_snapshots:
            if item.value != item.value_snapshot:
                raise DomainValidationError("clock must not mutate the paper execution object graph")
            for name, original in item.field_values.items():
                value = getattr(item.value, name)
                if self._identity_value(original) and value is not original:
                    raise DomainValidationError("clock must not replace paper execution object graph members")

    def _rollback_callback(self, snapshot: _LedgerSnapshot) -> None:
        self.clock = snapshot.clock
        self.event_bus = snapshot.event_bus
        self._lock = snapshot.lock
        self._transition_active = snapshot.transition_active
        self._current = snapshot.current_dict
        snapshot.current_dict.clear()
        snapshot.current_dict.update(snapshot.current_items)
        self._history = snapshot.history_dict
        snapshot.history_dict.clear()
        snapshot.history_dict.update(snapshot.history_items)
        for key, reports in snapshot.history_items.items():
            reports[:] = snapshot.history_reports[key]
        for item in snapshot.object_snapshots:
            for name, original in item.field_values.items():
                object.__setattr__(item.value, name, original)
        self._validate_callback_state(snapshot)
        for item in snapshot.object_snapshots:
            validate = getattr(item.value, "validate", None)
            if callable(validate):
                validate()
            else:
                post_init = getattr(item.value, "__post_init__", None)
                if callable(post_init):
                    post_init()

    @staticmethod
    def _identity_value(value: object) -> bool:
        return is_dataclass(value) and not isinstance(value, type) or isinstance(
            value, (tuple, list, dict)
        )

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
