"""Market domain entities."""

from dataclasses import dataclass
from datetime import datetime
from math import isfinite

from quant_futures.core.exceptions import DomainValidationError


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """Validated point-in-time market observation for a single instrument."""

    symbol: str
    price: float
    volume: float
    funding_rate: float
    open_interest: float
    timestamp: datetime

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise DomainValidationError("symbol must not be empty")
        if self.timestamp.tzinfo is None:
            raise DomainValidationError("timestamp must be timezone-aware")
        self._validate_finite("price", self.price, non_negative=True)
        self._validate_finite("volume", self.volume, non_negative=True)
        self._validate_finite("funding_rate", self.funding_rate)
        self._validate_finite("open_interest", self.open_interest, non_negative=True)

    @staticmethod
    def _validate_finite(name: str, value: float, *, non_negative: bool = False) -> None:
        if not isfinite(value) or (non_negative and value < 0):
            qualifier = " a finite non-negative" if non_negative else " finite"
            raise DomainValidationError(f"{name} must be{qualifier} number")
