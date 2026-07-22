"""Validated, transport-neutral market data records.

These models deliberately contain observations only.  They do not expose order
or account concepts, which keeps the data layer safe to run independently of
trading capabilities.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from math import isfinite
from types import MappingProxyType
from typing import Mapping

from quant_futures.core.exceptions import MarketDataError


class MarketDataKind(str, Enum):
    """Market observations supported by the Phase 2 collection boundary."""

    OHLCV = "ohlcv"
    FUNDING_RATE = "funding_rate"
    OPEN_INTEREST = "open_interest"
    MARK_PRICE = "mark_price"
    INDEX_PRICE = "index_price"
    LIQUIDATION = "liquidation"
    INSTRUMENT_METADATA = "instrument_metadata"


@dataclass(frozen=True, slots=True)
class MarketDataRecord:
    """One normalized observation received from an exchange or historical feed."""

    kind: MarketDataKind
    symbol: str
    source: str
    timestamp: datetime
    values: Mapping[str, float | str | bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise MarketDataError("symbol must not be empty")
        if not self.source.strip():
            raise MarketDataError("source must not be empty")
        if self.timestamp.tzinfo is None:
            raise MarketDataError("timestamp must be timezone-aware")
        if not self.values:
            raise MarketDataError("values must not be empty")
        for name, value in self.values.items():
            if not name.strip():
                raise MarketDataError("value names must not be empty")
            if isinstance(value, float) and not isfinite(value):
                raise MarketDataError(f"{name} must be finite")
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))


@dataclass(frozen=True, slots=True)
class InstrumentMetadata:
    """Validated static contract information supplied by an exchange."""

    symbol: str
    source: str
    contract_size: float
    price_tick: float
    quantity_step: float
    timestamp: datetime

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.source.strip():
            raise MarketDataError("symbol and source must not be empty")
        if self.timestamp.tzinfo is None:
            raise MarketDataError("timestamp must be timezone-aware")
        for name, value in (
            ("contract_size", self.contract_size),
            ("price_tick", self.price_tick),
            ("quantity_step", self.quantity_step),
        ):
            if not isfinite(value) or value <= 0:
                raise MarketDataError(f"{name} must be a finite positive number")

    def as_record(self) -> MarketDataRecord:
        return MarketDataRecord(
            kind=MarketDataKind.INSTRUMENT_METADATA,
            symbol=self.symbol,
            source=self.source,
            timestamp=self.timestamp,
            values={
                "contract_size": self.contract_size,
                "price_tick": self.price_tick,
                "quantity_step": self.quantity_step,
            },
        )
