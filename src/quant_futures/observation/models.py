"""Immutable models emitted by the market observation layer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from math import isfinite

from quant_futures.core.exceptions import DomainValidationError


class TrendRegime(str, Enum):
    """Describes directionality only; it is not a trading recommendation."""

    UPTREND = "trend_up"
    DOWNTREND = "trend_down"
    RANGE = "range"

    # Legacy aliases preserve callers that used the original short names.
    UP = UPTREND
    DOWN = DOWNTREND


class VolatilityRegime(str, Enum):
    """Describes the relative size of observed price movement."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class LiquidityRegime(str, Enum):
    """Describes available market depth or observed trading activity."""

    LIQUID = "liquid"
    THIN = "thin"
    STRESSED = "stressed"


@dataclass(frozen=True, slots=True)
class MarketObservation:
    """A point-in-time, non-directional description of a market's state."""

    symbol: str
    source: str
    timestamp: datetime
    price: float
    trend_regime: TrendRegime
    volatility_regime: VolatilityRegime
    liquidity_regime: LiquidityRegime
    features: "MarketFeatures"

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.source.strip():
            raise DomainValidationError("symbol and source must not be empty")
        if self.timestamp.tzinfo is None:
            raise DomainValidationError("timestamp must be timezone-aware")
        if not isfinite(self.price) or self.price <= 0:
            raise DomainValidationError("price must be a finite positive number")

    # Compatibility/readability aliases for consumers that use state terminology.
    @property
    def trend_state(self) -> str:
        return self.trend_regime.value

    @property
    def volatility_state(self) -> str:
        return self.volatility_regime.value

    @property
    def liquidity_state(self) -> str:
        return self.liquidity_regime.value


# Avoid a runtime import cycle while still exposing the type to static checkers.
from quant_futures.observation.features import MarketFeatures  # noqa: E402
