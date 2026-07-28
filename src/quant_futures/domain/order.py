"""Order domain entities and lifecycle states.

This module models intent only; it does not submit orders or communicate with
an exchange.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from math import isfinite
from numbers import Real

from quant_futures.core.exceptions import DomainValidationError


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    CREATED = "created"
    SUBMITTED = "submitted"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class Order:
    """An immutable order record with an explicit lifecycle status."""

    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float | None
    status: OrderStatus = OrderStatus.CREATED
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Revalidate every order invariant at each boundary crossing."""
        if not isinstance(self.order_id, str) or not self.order_id.strip():
            raise DomainValidationError("order_id must be a non-empty string")
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise DomainValidationError("symbol must be a non-empty string")
        if not isinstance(self.side, OrderSide):
            raise DomainValidationError("side must be an OrderSide")
        if not isinstance(self.status, OrderStatus):
            raise DomainValidationError("status must be an OrderStatus")
        if (
            not isinstance(self.quantity, Real)
            or isinstance(self.quantity, bool)
            or not isfinite(self.quantity)
            or self.quantity <= 0
        ):
            raise DomainValidationError("quantity must be a finite positive number")
        if self.price is not None and (
            not isinstance(self.price, Real)
            or isinstance(self.price, bool)
            or not isfinite(self.price)
            or self.price <= 0
        ):
            raise DomainValidationError("price must be a finite positive number when provided")
        if not isinstance(self.created_at, datetime):
            raise DomainValidationError("created_at must be a timezone-aware datetime")
        try:
            offset = self.created_at.utcoffset()
        except Exception as exc:
            raise DomainValidationError("created_at must be a timezone-aware datetime") from exc
        if self.created_at.tzinfo is None or offset is None:
            raise DomainValidationError("created_at must be timezone-aware")
