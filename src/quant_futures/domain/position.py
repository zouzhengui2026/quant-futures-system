"""Position domain entities."""

from dataclasses import dataclass
from enum import Enum


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"


@dataclass
class Position:
    symbol: str
    side: PositionSide
    size: float
    entry_price: float
    unrealized_pnl: float = 0.0
    liquidation_price: float | None = None
