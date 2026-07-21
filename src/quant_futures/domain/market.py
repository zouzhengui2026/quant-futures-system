"""Market domain entities."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketSnapshot:
    """A point-in-time market observation."""

    symbol: str
    price: float
    volume: float
    funding_rate: float
    open_interest: float
    timestamp: datetime
