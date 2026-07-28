"""Immutable reports for deterministic paper order transitions."""

from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from numbers import Real

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.domain.order import Order, OrderSide, OrderStatus
from quant_futures.execution.models import ExecutionIntent


@dataclass(frozen=True, slots=True)
class PaperExecutionReport:
    """An auditable result of one complete paper lifecycle transition."""

    execution_intent: ExecutionIntent
    order: Order
    previous_status: OrderStatus
    occurred_at: datetime
    reason: str | None = None
    filled_quantity: float | None = None
    average_fill_price: float | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Revalidate the report and its original intent, failing closed."""
        if not isinstance(self.execution_intent, ExecutionIntent):
            raise DomainValidationError("execution_intent must be an ExecutionIntent")
        self.execution_intent.validate()
        original = self.execution_intent.order
        if original.status is not OrderStatus.CREATED:
            raise DomainValidationError("execution intent order must remain CREATED")
        if not isinstance(self.order, Order):
            raise DomainValidationError("order must be an Order")
        self.order.validate()
        if self.order.status is OrderStatus.CREATED:
            raise DomainValidationError("report order status must not be CREATED")
        for name in ("order_id", "symbol", "side", "quantity", "price", "created_at"):
            if getattr(self.order, name) != getattr(original, name):
                raise DomainValidationError(f"report order must preserve {name}")
        if not isinstance(self.previous_status, OrderStatus):
            raise DomainValidationError("previous_status must be an OrderStatus")
        self._validate_time()

        transition = (self.previous_status, self.order.status)
        allowed = {
            (OrderStatus.CREATED, OrderStatus.SUBMITTED),
            (OrderStatus.CREATED, OrderStatus.REJECTED),
            (OrderStatus.SUBMITTED, OrderStatus.FILLED),
            (OrderStatus.SUBMITTED, OrderStatus.CANCELLED),
        }
        if transition not in allowed:
            raise DomainValidationError("invalid paper order status transition")

        if self.order.status is OrderStatus.SUBMITTED:
            self._require_empty_execution_details()
        elif self.order.status in (OrderStatus.REJECTED, OrderStatus.CANCELLED):
            if not isinstance(self.reason, str) or not self.reason.strip():
                raise DomainValidationError("reason must be a non-empty string")
            if self.filled_quantity is not None or self.average_fill_price is not None:
                raise DomainValidationError("non-filled report must not contain fill details")
        else:
            self._validate_fill()

    def _validate_time(self) -> None:
        if not isinstance(self.occurred_at, datetime):
            raise DomainValidationError("occurred_at must be a timezone-aware datetime")
        try:
            offset = self.occurred_at.utcoffset()
        except Exception as exc:
            raise DomainValidationError("occurred_at must be a timezone-aware datetime") from exc
        if self.occurred_at.tzinfo is None or offset is None:
            raise DomainValidationError("occurred_at must be timezone-aware")
        try:
            too_early = self.occurred_at < self.execution_intent.created_at
        except (TypeError, ValueError) as exc:
            raise DomainValidationError("occurred_at must be comparable to intent time") from exc
        if too_early:
            raise DomainValidationError("occurred_at must not precede execution intent")

    def _require_empty_execution_details(self) -> None:
        if self.reason is not None:
            raise DomainValidationError("submitted report reason must be None")
        if self.filled_quantity is not None or self.average_fill_price is not None:
            raise DomainValidationError("submitted report must not contain fill details")

    def _validate_fill(self) -> None:
        if self.reason is not None:
            raise DomainValidationError("filled report reason must be None")
        if (
            not isinstance(self.filled_quantity, Real)
            or isinstance(self.filled_quantity, bool)
            or not isfinite(self.filled_quantity)
            or self.filled_quantity != self.order.quantity
        ):
            raise DomainValidationError("filled_quantity must equal the full order quantity")
        price = self.average_fill_price
        if (
            not isinstance(price, Real)
            or isinstance(price, bool)
            or not isfinite(price)
            or price <= 0
        ):
            raise DomainValidationError("average_fill_price must be a finite positive number")
        limit = self.order.price
        if limit is not None:
            if self.order.side is OrderSide.BUY and price > limit:
                raise DomainValidationError("BUY fill price must not exceed limit price")
            if self.order.side is OrderSide.SELL and price < limit:
                raise DomainValidationError("SELL fill price must not be below limit price")
