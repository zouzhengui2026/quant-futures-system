"""Position domain entities and lifecycle states."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from math import isfinite

from quant_futures.core.exceptions import DomainValidationError


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"


class PositionStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class Position:
    """A point-in-time representation of one instrument position."""

    symbol: str
    side: PositionSide
    size: float
    entry_price: float
    unrealized_pnl: float = 0.0
    liquidation_price: float | None = None
    status: PositionStatus = PositionStatus.OPEN
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise DomainValidationError("symbol must not be empty")
        if not isfinite(self.size) or self.size < 0:
            raise DomainValidationError("size must be a finite non-negative number")
        if not isfinite(self.entry_price) or self.entry_price <= 0:
            raise DomainValidationError("entry_price must be a finite positive number")
        if not isfinite(self.unrealized_pnl):
            raise DomainValidationError("unrealized_pnl must be a finite number")
        if self.liquidation_price is not None and (
            not isfinite(self.liquidation_price) or self.liquidation_price <= 0
        ):
            raise DomainValidationError(
                "liquidation_price must be a finite positive number when provided"
            )
        if self.updated_at.tzinfo is None:
            raise DomainValidationError("updated_at must be timezone-aware")
        if self.status is PositionStatus.OPEN and self.size == 0:
            raise DomainValidationError("an open position must have a positive size")
