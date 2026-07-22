"""Order domain entities and lifecycle states.

This module models intent only; it does not submit orders or communicate with
an exchange.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from math import isfinite

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
        if not self.order_id.strip():
            raise DomainValidationError("order_id must not be empty")
        if not self.symbol.strip():
            raise DomainValidationError("symbol must not be empty")
        if not isfinite(self.quantity) or self.quantity <= 0:
            raise DomainValidationError("quantity must be a finite positive number")
        if self.price is not None and (not isfinite(self.price) or self.price <= 0):
            raise DomainValidationError("price must be a finite positive number when provided")
        if self.created_at.tzinfo is None:
            raise DomainValidationError("created_at must be timezone-aware")
